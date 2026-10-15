// Package config loads and validates storeboard's JSON configuration.
//
// The file is strict: unknown keys are an error, so a typo can never be
// silently ignored. Absent keys keep their defaults, which makes an empty
// object a valid configuration for a fresh install. The schema is versioned
// like every other JSON surface (configVersion); this build reads exactly
// version 1.
//
// The model: this machine is a peer (named by `peer`), other devices are
// `peers` with declared endpoints, `groups` bind peers together around
// `folders`, and `mounts` is this machine's local override of a group
// folder's default mount point. All daemon state lives under one dataDir.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// ConfigVersion is the only config schema version this build reads.
const ConfigVersion = 1

// ConfigEnvVar points at a configuration file, for deployments that install
// one system-wide (the NixOS module does). An explicit --config still wins,
// and the per-user XDG file is the default when neither is set.
const ConfigEnvVar = "STOREBOARD_CONFIG"

// appName is the directory storeboard keeps its files in under each XDG base.
const appName = "storeboard"

// Peer is a declared device: name lives in its map key, endpoints are the
// host:port addresses other peers dial.
type Peer struct {
	Endpoints []string `json:"endpoints"`
}

// Group is a set of peers sharing folders. Every group must include this
// peer; folder names are unique across the whole config so a local mount
// override can name a folder without qualifying its group.
type Group struct {
	Peers   []string               `json:"peers"`
	Folders map[string]GroupFolder `json:"folders"`
	// Permissions is the group's default ownership and mode for its
	// folders; a per-folder entry on a machine overrides it field by field.
	Permissions *Permissions `json:"permissions,omitempty"`
	// StorageDir overrides the machine's storageDir for this group's
	// folders. Absent falls back to the top-level one.
	StorageDir *string `json:"storageDir,omitempty"`
}

// Permissions is the ownership and mode the daemon gives the entries it
// creates for a folder. Every field is optional: an empty field falls
// through to the next level — machine-local entry, then group default, then
// the directory the mount lives in (its owner, its group, its mode). So a
// folder with nothing declared is permissioned exactly like the directory
// containing it, and inherits any change that directory sees.
type Permissions struct {
	User  string `json:"user,omitempty"`  // user name or numeric uid
	Group string `json:"group,omitempty"` // group name or numeric gid
	Mode  string `json:"mode,omitempty"`  // octal directory mode, e.g. "0755"
}

// Validate checks a Permissions block's syntax. User and group names are
// resolved at run time, not here: the build-time config check runs in a
// sandbox with no passwd database.
func (p *Permissions) Validate(where string) error {
	if p == nil {
		return nil
	}
	for _, f := range []struct{ label, v string }{{"user", p.User}, {"group", p.Group}} {
		if f.v == "" {
			continue
		}
		if strings.ContainsAny(f.v, ":,\n \t") {
			return fmt.Errorf("%s %s %q: a name or numeric id, not a colon list", where, f.label, f.v)
		}
	}
	if p.Mode != "" {
		if _, err := ParseMode(p.Mode); err != nil {
			return fmt.Errorf("%s mode: %w", where, err)
		}
	}
	return nil
}

// ParseMode parses an octal directory mode ("755", "0755").
func ParseMode(s string) (os.FileMode, error) {
	m, err := strconv.ParseUint(s, 8, 32)
	if err != nil || m > 0o777 {
		return 0, fmt.Errorf("%q: want octal 0000-0777", s)
	}
	return os.FileMode(m), nil
}

// Backup is a machine's version history: which folders keep one, and how far
// back it reaches.
type Backup struct {
	// Enable turns history keeping on for this machine's folders.
	Enable bool `json:"enable"`
	// Keep is how long a version may live, as a duration with h/d/w/y units
	// ("1y", "90d", "36h"). Empty means one year. Nothing older is ever
	// kept, whatever the thinning schedule would say.
	Keep string `json:"keep,omitempty"`
	// Folders opts individual folders out by name.
	Folders map[string]bool `json:"folders,omitempty"`
}

// ParseKeep parses a keep duration: the usual Go units plus d, w and y, so a
// retention window reads the way it is spoken ("1y", "90d").
func ParseKeep(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	unit := time.Duration(0)
	digits := s
	switch {
	case strings.HasSuffix(s, "y"):
		unit, digits = 365*24*time.Hour, strings.TrimSuffix(s, "y")
	case strings.HasSuffix(s, "w"):
		unit, digits = 7*24*time.Hour, strings.TrimSuffix(s, "w")
	case strings.HasSuffix(s, "d"):
		unit, digits = 24*time.Hour, strings.TrimSuffix(s, "d")
	}
	if unit == 0 {
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("%q: want a duration like 36h, 90d, 4w or 1y", s)
		}
		if d <= 0 {
			return 0, fmt.Errorf("%q: keep must be positive", s)
		}
		return d, nil
	}
	n, err := strconv.Atoi(digits)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%q: want a positive number before %q", s, s[len(s)-1:])
	}
	return time.Duration(n) * unit, nil
}

// BackupFor reports whether a folder keeps history on this machine and for
// how long (zero duration means the default year).
func (c *Config) BackupFor(folder string) (bool, time.Duration, error) {
	if c.Backup == nil || !c.Backup.Enable {
		return false, 0, nil
	}
	if off, ok := c.Backup.Folders[folder]; ok && !off {
		return false, 0, nil
	}
	keep, err := ParseKeep(c.Backup.Keep)
	if err != nil {
		return false, 0, fmt.Errorf("backup keep: %w", err)
	}
	return true, keep, nil
}

// GroupFolder is a folder's declaration inside a group. Mount is the
// default every peer sees; each peer overrides it locally in `mounts`.
type GroupFolder struct {
	Mount string `json:"mount,omitempty"`
}

// Config is the complete configuration, shared by the daemon and the CLI.
type Config struct {
	// ConfigVersion pins the schema. A file that declares another version is
	// an error, never a best-effort parse.
	ConfigVersion int `json:"configVersion"`

	// DataDir is the one storage directory: store, journal, index — every
	// piece of daemon state, including default mounts for folders nobody
	// gave a path.
	DataDir string `json:"dataDir"`

	// Socket is the unix socket the daemon serves /v1 on and the CLI dials.
	Socket string `json:"socket"`

	// Peer is this machine's name. Empty is allowed only while no groups
	// are declared; it then defaults to the hostname.
	Peer string `json:"peer"`

	// Peers are the declared devices, by name.
	Peers map[string]Peer `json:"peers"`

	// Groups bind peers together around folders.
	Groups map[string]Group `json:"groups"`

	// Mounts overrides a group folder's default mount on THIS machine.
	Mounts map[string]string `json:"mounts"`

	// StorageDir, when set, is where folder content lives on this machine:
	// each folder's root becomes <storageDir>/<folder> and the mount path is
	// a symlink to it, so content and mount point are independent — the
	// mount can sit anywhere, and no two folders can index the same bytes.
	// Absent or null takes the default, <dataDir>/storage; an explicit empty
	// string restores the original layout (a replica's content at the mount
	// path itself, a streamed folder's under the data directory). A group
	// may override it per group; a folder with content at its mount path is
	// migrated into the storage directory on the next transition.
	StorageDir *string `json:"storageDir,omitempty"`

	// Backup, per machine, keeps a thinned history of every file's versions
	// under the data directory: the newest version of each hour for a day,
	// each day for a week, each week for a month, each month for a year (and
	// each year beyond that), and nothing older than Keep. Folders opt out
	// per name; a version is never dropped while it is the file's current
	// one, and the retention policy is the only thing in the daemon that
	// ever deletes anything of its own accord.
	Backup *Backup `json:"backup,omitempty"`

	// Permissions overrides, per folder on THIS machine, the ownership and
	// mode the daemon gives the folder's entries. Fields left empty fall
	// back to the group's Permissions and then to the directory the mount
	// lives in, so a folder with nothing declared is permissioned exactly
	// like the directory containing it.
	Permissions map[string]Permissions `json:"permissions,omitempty"`

	// KeepOffline, per folder, decides what THIS machine does with a group
	// folder: true (the default for anything undeclared) keeps a full local
	// replica that works offline; false streams the folder from a holder —
	// mounted live while a holder is reachable, an empty dummy dir when
	// none is, remerged on reconnect.
	KeepOffline map[string]bool `json:"keepOffline"`
}

// Default returns the built-in configuration with the XDG defaults filled in.
func Default() *Config {
	cfg := &Config{
		ConfigVersion: ConfigVersion,
		DataDir:       DefaultDataDir(),
		Socket:        DefaultSocketPath(),
	}
	if cfg.DataDir != "" {
		def := DefaultStorageDir(cfg.DataDir)
		cfg.StorageDir = &def
	}
	return cfg
}

// DefaultPath is $XDG_CONFIG_HOME/storeboard/config.json. It is empty when no
// config directory can be determined.
func DefaultPath() string {
	dir, err := os.UserConfigDir()
	if err != nil || dir == "" {
		return ""
	}
	return filepath.Join(dir, appName, "config.json")
}

// DefaultDataDir is $XDG_DATA_HOME/storeboard. It is empty when no data
// directory can be determined.
func DefaultDataDir() string {
	dir := os.Getenv("XDG_DATA_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil || home == "" {
			return ""
		}
		dir = filepath.Join(home, ".local", "share")
	}
	return filepath.Join(dir, appName)
}

// DefaultSocketPath is $XDG_RUNTIME_DIR/storeboard.sock: per user, and gone
// on logout, so a crashed daemon never leaves a stale socket across
// sessions. With no runtime directory it falls back next to the data dir.
func DefaultSocketPath() string {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, appName+".sock")
	}
	if dir := DefaultDataDir(); dir != "" {
		return filepath.Join(dir, appName+".sock")
	}
	return ""
}

// DefaultMountDir is where a folder mounts when neither the group nor this
// machine picked a path: inside the one storage directory, so a fresh
// install keeps everything it owns in one place.
func (c *Config) DefaultMountDir(folder string) string {
	return filepath.Join(c.DataDir, "mounts", folder)
}

// SystemConfigPath is the system-wide config the NixOS module installs.
// The CLI uses it as a last resort when neither a per-user file nor the
// environment names one — an environment variable only reaches sessions
// that sourced /etc/set-environment, and a shell spawned without that
// (an agent shell, a terminal inside an editor) would otherwise resolve to
// a per-user default socket no system daemon is listening on.
var SystemConfigPath = "/etc/storeboard/config.json"

// ResolvePath decides which configuration file to load: the explicit flag,
// then an existing per-user file — the more specific declaration beats a
// system-wide default, so a home-manager configuration wins over the
// NixOS module's /etc file — then the environment, then the system file
// itself. An empty result means Load falls back to DefaultPath().
func ResolvePath(explicit string) string {
	if trimmed := strings.TrimSpace(explicit); trimmed != "" {
		return trimmed
	}
	if p := DefaultPath(); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	if env := strings.TrimSpace(os.Getenv(ConfigEnvVar)); env != "" {
		return env
	}
	if _, err := os.Stat(SystemConfigPath); err == nil {
		return SystemConfigPath
	}
	return ""
}

// Load reads the configuration from path. An empty path selects DefaultPath();
// a missing file at that location is not an error and yields Default(). A path
// given explicitly must exist.
func Load(path string) (*Config, error) {
	explicit := path != ""
	if !explicit {
		path = DefaultPath()
		if path == "" {
			return nil, fmt.Errorf("cannot determine config directory; pass --config")
		}
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) && !explicit {
			return Default(), nil
		}
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	raw, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg, err := Parse(raw)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// Parse reads a configuration from JSON text: the same strict decoding,
// defaulting and validation Load applies to a file, exposed so an embedder
// (the Android app, whose config arrives as a string) uses one code path.
func Parse(raw []byte) (*Config, error) {
	// Decode into an empty config, not Default(): every default that depends
	// on another field (storageDir under dataDir, above all) has to be
	// computed from what the file actually says, not from the XDG guess.
	cfg := &Config{ConfigVersion: ConfigVersion}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("parse config: trailing data after JSON value")
	}

	// An explicitly blanked string falls back to the default; JSON cannot
	// distinguish "absent" from "empty" when decoding into filled defaults.
	if cfg.DataDir == "" {
		cfg.DataDir = DefaultDataDir()
	}
	if cfg.Socket == "" {
		cfg.Socket = DefaultSocketPath()
	}
	// storageDir is a pointer so a file can distinguish three cases: absent
	// (the default under dataDir), empty (content at the mount path), and a
	// path.
	if cfg.StorageDir == nil && cfg.DataDir != "" {
		def := DefaultStorageDir(cfg.DataDir)
		cfg.StorageDir = &def
	}

	// Maps decode into whatever the file leaves behind (or nil when absent);
	// normalize so callers never branch on nil.
	if cfg.Peers == nil {
		cfg.Peers = map[string]Peer{}
	}
	if cfg.Groups == nil {
		cfg.Groups = map[string]Group{}
	}
	if cfg.Mounts == nil {
		cfg.Mounts = map[string]string{}
	}
	if cfg.KeepOffline == nil {
		cfg.KeepOffline = map[string]bool{}
	}
	if cfg.Permissions == nil {
		cfg.Permissions = map[string]Permissions{}
	}
	for name, g := range cfg.Groups {
		if g.Folders == nil {
			g.Folders = map[string]GroupFolder{}
			cfg.Groups[name] = g
		}
	}

	// A config that never says who it is gets the hostname: a fresh install
	// with no groups still needs a name for status and, later, the protocol.
	// Declaring any group makes the name explicit — Validate enforces it.
	if cfg.Peer == "" {
		if host, err := os.Hostname(); err == nil {
			cfg.Peer = host
		}
	}
	if len(cfg.Peers) == 0 && cfg.Peer != "" {
		cfg.Peers[cfg.Peer] = Peer{Endpoints: []string{}}
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate reports structural errors in the configuration.
func (c *Config) Validate() error {
	if c.ConfigVersion != ConfigVersion {
		return fmt.Errorf("configVersion %d: this build reads version %d", c.ConfigVersion, ConfigVersion)
	}
	if c.DataDir == "" {
		return fmt.Errorf("dataDir is empty; cannot determine the XDG data directory")
	}
	if !filepath.IsAbs(c.DataDir) {
		return fmt.Errorf("dataDir %q: path must be absolute", c.DataDir)
	}
	if c.Socket == "" {
		return fmt.Errorf("socket is empty; cannot determine the XDG runtime directory")
	}
	if !filepath.IsAbs(c.Socket) {
		return fmt.Errorf("socket %q: path must be absolute", c.Socket)
	}

	if c.Peer != "" {
		if err := checkName("peer", c.Peer); err != nil {
			return err
		}
		if _, ok := c.Peers[c.Peer]; !ok {
			return fmt.Errorf("peer %q is not declared in peers", c.Peer)
		}
	}

	for name, p := range c.Peers {
		if err := checkName("peer", name); err != nil {
			return err
		}
		for _, ep := range p.Endpoints {
			if err := checkEndpoint(ep); err != nil {
				return fmt.Errorf("peer %q endpoint %q: %w", name, ep, err)
			}
		}
	}

	// Folder name → owning group, for global-uniqueness and `mounts` lookups.
	folderGroup := map[string]string{}

	for name, g := range c.Groups {
		if err := checkName("group", name); err != nil {
			return err
		}
		if len(g.Peers) == 0 {
			return fmt.Errorf("group %q: no peers", name)
		}
		for _, member := range g.Peers {
			if _, ok := c.Peers[member]; !ok {
				return fmt.Errorf("group %q: unknown peer %q", name, member)
			}
		}
		if c.Peer == "" {
			return fmt.Errorf("groups are declared but this machine has no peer name; set peer")
		}
		if !slices.Contains(g.Peers, c.Peer) {
			return fmt.Errorf("group %q: this peer %q is not a member", name, c.Peer)
		}

		for folder, gf := range g.Folders {
			if err := checkName("folder", folder); err != nil {
				return err
			}
			if owner, dup := folderGroup[folder]; dup {
				return fmt.Errorf("folder %q: declared in groups %q and %q; folder names are global", folder, owner, name)
			}
			folderGroup[folder] = name
			if gf.Mount != "" && !filepath.IsAbs(gf.Mount) {
				return fmt.Errorf("group %q folder %q: mount %q must be absolute", name, folder, gf.Mount)
			}
		}
	}

	for folder, mount := range c.Mounts {
		if _, ok := folderGroup[folder]; !ok {
			return fmt.Errorf("mounts: unknown folder %q", folder)
		}
		if !filepath.IsAbs(mount) {
			return fmt.Errorf("mounts[%q]: path %q must be absolute", folder, mount)
		}
	}

	for folder := range c.KeepOffline {
		if _, ok := folderGroup[folder]; !ok {
			return fmt.Errorf("keepOffline: unknown folder %q", folder)
		}
	}

	for folder, p := range c.Permissions {
		if _, ok := folderGroup[folder]; !ok {
			return fmt.Errorf("permissions: unknown folder %q", folder)
		}
		if err := p.Validate(fmt.Sprintf("permissions %s", folder)); err != nil {
			return err
		}
	}

	for name, g := range c.Groups {
		if err := g.Permissions.Validate(fmt.Sprintf("group %s permissions", name)); err != nil {
			return err
		}
		if g.StorageDir != nil && !filepath.IsAbs(*g.StorageDir) {
			return fmt.Errorf("group %q storageDir %q must be absolute", name, *g.StorageDir)
		}
	}
	if c.StorageDir != nil && *c.StorageDir != "" && !filepath.IsAbs(*c.StorageDir) {
		return fmt.Errorf("storageDir %q must be absolute (or empty for content at the mount path)", *c.StorageDir)
	}

	if c.Backup != nil {
		if _, err := ParseKeep(c.Backup.Keep); err != nil {
			return fmt.Errorf("backup keep: %w", err)
		}
	}

	// Two folders on one identical path stay an error: there is no reading
	// of that shape where the outcome is defined. Nested mounts, in
	// contrast, are a deliberate tradeoff (a pool root plus the subtrees
	// other machines stream) — those are warnings, see Warnings.
	folders := c.Folders()
	for i, a := range folders {
		for _, b := range folders[i+1:] {
			if a.Mount == b.Mount {
				return fmt.Errorf("folders %q and %q both mount %s: one path cannot be two folders", a.Name, b.Name, a.Mount)
			}
		}
	}
	// A folder whose mount is its own storage root would ask for a symlink
	// pointing at itself.
	for _, f := range folders {
		if f.Storage != "" && f.Storage == f.Mount {
			return fmt.Errorf("folder %q: mount and storage are both %s; the mount is the storage directory's view and has to be a different path", f.Name, f.Mount)
		}
	}

	return nil
}

// Warnings reports shapes that are legal but cost something: today, folders
// whose scan roots nest. A nested mount converges — the holder's filesystem
// mediates, every write is temp-file-then-rename, content is
// content-addressed — but the machine that holds both indexes the same files
// twice (two journals, two scans, two broadcasts, two writers per path) and
// its content store takes a copy of the outer tree. Folders backed by a
// storage directory never warn: their roots are <storageDir>/<folder>,
// disjoint by construction.
func (c *Config) Warnings() []string {
	type root struct{ name, path string }
	var roots []root
	for groupName, g := range c.Groups {
		for folder := range g.Folders {
			roots = append(roots, root{folder, c.FolderRoot(folder, groupName)})
		}
	}
	sort.Slice(roots, func(i, j int) bool { return roots[i].name < roots[j].name })

	var out []string
	for i, a := range roots {
		for _, b := range roots[i+1:] {
			switch {
			case contains(a.path, b.path):
				out = append(out, fmt.Sprintf("folder %q is rooted at %s, which contains %s (folder %q): the machine holding both indexes those files twice, in two journals, and keeps a copy of the outer tree in its content store", a.name, a.path, b.path, b.name))
			case contains(b.path, a.path):
				out = append(out, fmt.Sprintf("folder %q is rooted at %s, which contains %s (folder %q): the machine holding both indexes those files twice, in two journals, and keeps a copy of the outer tree in its content store", b.name, b.path, a.path, a.name))
			}
		}
	}
	return out
}

// contains reports whether outer is a proper ancestor directory of inner.
// Path-boundary aware: /srv/photos does not contain /srv/photos-old.
func contains(outer, inner string) bool {
	if !strings.HasPrefix(inner, outer) {
		return false
	}
	return strings.HasPrefix(inner[len(outer):], "/")
}

// DefaultStorageDir is where folder content lives when nothing else says:
// inside the one storage directory, so a fresh install keeps everything it
// owns in one place. A config that wants the original layout (content at the
// mount path) sets storageDir to the empty string explicitly.
func DefaultStorageDir(dataDir string) string {
	return filepath.Join(dataDir, "storage")
}

// StorageFor is a folder's content directory when the config puts content in
// a storage directory: the group's storageDir if declared, else the
// machine's, else the default under the data directory. Empty means the
// original layout — content at the mount path (replicas) or under the data
// directory (streamed) — which is what an explicit empty storageDir asks for.
func (c *Config) StorageFor(folder, group string) string {
	if g, ok := c.Groups[group]; ok && g.StorageDir != nil {
		return *g.StorageDir
	}
	if c.StorageDir != nil {
		return *c.StorageDir
	}
	return DefaultStorageDir(c.DataDir)
}

// FolderRoot is where a folder's content lives on this machine: under its
// storage directory when one is configured, else at its effective mount.
func (c *Config) FolderRoot(folder, group string) string {
	if s := c.StorageFor(folder, group); s != "" {
		return filepath.Join(s, folder)
	}
	return c.FolderMount(folder, group)
}

// FolderMount is a folder's effective mount: this machine's override, else
// the group's default, else inside the data directory.
func (c *Config) FolderMount(folder, group string) string {
	mount := c.Mounts[folder]
	if mount == "" {
		mount = c.Groups[group].Folders[folder].Mount
	}
	if mount == "" {
		mount = c.DefaultMountDir(folder)
	}
	return mount
}

// PermissionsFor is the declared permissions for a folder: this machine's
// per-folder entry wins field by field over the group's default. Nil when
// neither declares anything — the daemon then inherits from the directory
// the mount lives in.
func (c *Config) PermissionsFor(folder, group string) *Permissions {
	var local *Permissions
	if p, ok := c.Permissions[folder]; ok {
		local = &p
	}
	def := c.Groups[group].Permissions
	if local == nil && def == nil {
		return nil
	}
	out := &Permissions{}
	lu, lg, lm := "", "", ""
	if local != nil {
		lu, lg, lm = local.User, local.Group, local.Mode
	}
	du, dg, dm := "", "", ""
	if def != nil {
		du, dg, dm = def.User, def.Group, def.Mode
	}
	for _, f := range []struct {
		dst        *string
		local, def string
	}{
		{&out.User, lu, du},
		{&out.Group, lg, dg},
		{&out.Mode, lm, dm},
	} {
		if f.local != "" {
			*f.dst = f.local
		} else {
			*f.dst = f.def
		}
	}
	if out.User == "" && out.Group == "" && out.Mode == "" {
		return nil
	}
	return out
}

// PeerList lists the declared peers, sorted by name, endpoints intact.
// Empty lists stay arrays — the contract says arrays, never null.
func (c *Config) PeerList() []api.Peer {
	out := make([]api.Peer, 0, len(c.Peers))
	for name, p := range c.Peers {
		endpoints := p.Endpoints
		if endpoints == nil {
			endpoints = []string{}
		}
		out = append(out, api.Peer{Name: name, Endpoints: endpoints})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// GroupList lists the declared groups, sorted by name; each group's folder
// names are sorted too.
func (c *Config) GroupList() []api.Group {
	out := make([]api.Group, 0, len(c.Groups))
	for name, g := range c.Groups {
		folders := make([]string, 0, len(g.Folders))
		for folder := range g.Folders {
			folders = append(folders, folder)
		}
		sort.Strings(folders)
		out = append(out, api.Group{Name: name, Peers: g.Peers, Folders: folders})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Folders lists every folder across all groups with its effective local
// mount point — local override, else the group's declared default, else
// inside the one storage directory. Sorted by (globally unique) name.
func (c *Config) Folders() []api.Folder {
	out := make([]api.Folder, 0, len(c.Mounts)+len(c.KeepOffline))
	for groupName, g := range c.Groups {
		for folder := range g.Folders {
			// Absent from the map means keep: a replica is the conservative
			// default, and streaming is the opt-out.
			keepOffline := true
			if v, ok := c.KeepOffline[folder]; ok {
				keepOffline = v
			}
			f := api.Folder{
				Name:        folder,
				Group:       groupName,
				Mount:       c.FolderMount(folder, groupName),
				KeepOffline: keepOffline,
			}
			if s := c.StorageFor(folder, groupName); s != "" {
				f.Storage = filepath.Join(s, folder)
			}
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// checkName is the identity rule for peers, groups and folders: a name ends
// up in JSON keys, journal entries and CLI output, so it is an identifier,
// never a path.
func checkName(kind, name string) error {
	if name == "" {
		return fmt.Errorf("%s with empty name", kind)
	}
	if name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("%s name %q: a name is an identifier, not a path", kind, name)
	}
	return nil
}

// checkEndpoint accepts host:port — the wire format every declared endpoint
// uses. Bare hostnames are rejected until the protocol picks a default port.
func checkEndpoint(ep string) error {
	host, port, err := net.SplitHostPort(ep)
	if err != nil {
		return fmt.Errorf("want host:port: %w", err)
	}
	if host == "" {
		return fmt.Errorf("empty host")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("port %q: want 1-65535", port)
	}
	return nil
}
