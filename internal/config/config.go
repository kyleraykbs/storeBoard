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
	return &Config{
		ConfigVersion: ConfigVersion,
		DataDir:       DefaultDataDir(),
		Socket:        DefaultSocketPath(),
	}
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

	cfg := Default()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("parse config %s: trailing data after JSON value", path)
	}

	// An explicitly blanked string falls back to the default; JSON cannot
	// distinguish "absent" from "empty" when decoding into filled defaults.
	if cfg.DataDir == "" {
		cfg.DataDir = DefaultDataDir()
	}
	if cfg.Socket == "" {
		cfg.Socket = DefaultSocketPath()
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
		return nil, fmt.Errorf("config %s: %w", path, err)
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
	}

	// Effective mounts must not overlap: a folder whose mount contains
	// another's makes the same bytes two folders' business on the machine
	// that holds both — double scans and broadcasts, two writers for one
	// path, and a replica scan that copies the whole outer tree into the
	// content store. Ambiguity about what syncs where is exactly what this
	// config model exists to prevent (DESIGN invariant 12).
	folders := c.Folders()
	for i, a := range folders {
		for _, b := range folders[i+1:] {
			switch {
			case a.Mount == b.Mount:
				return fmt.Errorf("folders %q and %q both mount %s: one path cannot be two folders", a.Name, b.Name, a.Mount)
			case contains(a.Mount, b.Mount):
				return fmt.Errorf("folder %q mounts %s, which contains %s (folder %q): nested mounts double every file — give one of them its own group, or drop the outer folder", a.Name, a.Mount, b.Mount, b.Name)
			case contains(b.Mount, a.Mount):
				return fmt.Errorf("folder %q mounts %s, which contains %s (folder %q): nested mounts double every file — give one of them its own group, or drop the outer folder", b.Name, b.Mount, a.Mount, a.Name)
			}
		}
	}

	return nil
}

// contains reports whether outer is a proper ancestor directory of inner.
// Path-boundary aware: /srv/photos does not contain /srv/photos-old.
func contains(outer, inner string) bool {
	if !strings.HasPrefix(inner, outer) {
		return false
	}
	return strings.HasPrefix(inner[len(outer):], "/")
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
		for folder, gf := range g.Folders {
			mount := c.Mounts[folder]
			if mount == "" {
				mount = gf.Mount
			}
			if mount == "" {
				mount = c.DefaultMountDir(folder)
			}
			// Absent from the map means keep: a replica is the conservative
			// default, and streaming is the opt-out.
			keepOffline := true
			if v, ok := c.KeepOffline[folder]; ok {
				keepOffline = v
			}
			out = append(out, api.Folder{
				Name:        folder,
				Group:       groupName,
				Mount:       mount,
				KeepOffline: keepOffline,
			})
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
