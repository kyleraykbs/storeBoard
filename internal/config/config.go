// Package config loads and validates storeboard's JSON configuration.
//
// The file is strict: unknown keys are an error, so a typo can never be
// silently ignored. Absent keys keep their defaults, which makes an empty
// object a valid configuration for a fresh install. The schema is versioned
// like every other JSON surface (configVersion); this build reads exactly
// version 2.
//
// The model: one `root` directory holds flat top-level stores. A store may
// reference any other store by name through `links`; each link is a relative
// presentation path inside the referencing store. Store identity and policy
// belong only to the referenced top-level declaration.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// ConfigVersion is the only config schema version this build reads.
const ConfigVersion = 2

// ConfigEnvVar points at a configuration file, for deployments that install
// one system-wide (the NixOS module does). An explicit --config still wins,
// and the per-user XDG file is the default when neither is set.
const ConfigEnvVar = "STOREBOARD_CONFIG"

// appName is the directory storeboard keeps its files in under each XDG base.
const appName = "storeboard"

// Sync types. Local holds a full replica that works offline; remote streams
// the store from a holder. Empty in a declaration means inherit.
const (
	SyncLocal  = "local"
	SyncRemote = "remote"
)

// Peer is a declared device: name lives in its map key, endpoints are the
// host:port addresses other peers dial.
type Peer struct {
	Endpoints []string `json:"endpoints"`
}

// Group is a set of peers that share stores. Every group must include this
// peer.
type Group struct {
	Peers []string `json:"peers"`
	// Permissions is the group default for its stores; a store's own
	// declaration overrides it field by field.
	Permissions *Permissions `json:"permissions,omitempty"`
}

// Permissions is the ownership and mode the daemon gives the entries it
// creates for a store. Every field is optional: an empty field falls through
// to the next level — the machine's per-store entry, the store's effective
// declaration (own, then nearest ancestor), the group default, then the
// directory the store's canonical dir lives in. So a store with nothing
// declared is permissioned exactly like its parent directory.
type Permissions struct {
	User  string `json:"user,omitempty"`
	Group string `json:"group,omitempty"`
	Mode  string `json:"mode,omitempty"`
}

// Validate checks a Permissions block's syntax. User and group names are
// resolved at run time, not here: the build-time config check runs in a
// sandbox with no passwd database.
func (p *Permissions) Validate(where string) error {
	if p == nil {
		return nil
	}
	if p.User != "" {
		if _, err := strconv.Atoi(p.User); err != nil && strings.ContainsAny(p.User, "/:\x00") {
			return fmt.Errorf("%s: user %q is not a name or uid", where, p.User)
		}
	}
	if p.Group != "" {
		if _, err := strconv.Atoi(p.Group); err != nil && strings.ContainsAny(p.Group, "/:\x00") {
			return fmt.Errorf("%s: group %q is not a name or gid", where, p.Group)
		}
	}
	if p.Mode != "" {
		if _, err := ParseMode(p.Mode); err != nil {
			return fmt.Errorf("%s: %w", where, err)
		}
	}
	return nil
}

// ParseMode parses an octal directory mode ("755", "0755").
func ParseMode(s string) (os.FileMode, error) {
	v, err := strconv.ParseUint(s, 8, 32)
	if err != nil || v > 0o777 {
		return 0, fmt.Errorf("mode %q is not an octal permission mask", s)
	}
	return os.FileMode(v), nil
}

// Backup is a machine's version history: which stores keep one, and how far
// back it reaches.
type Backup struct {
	// Enable turns history keeping on for this machine's stores.
	Enable bool `json:"enable"`
	// Keep is how long a version may live, as a duration with h/d/w/y units
	// ("1y", "90d", "36h"). Empty means one year.
	Keep string `json:"keep,omitempty"`
	// Stores opts individual stores out by id.
	Stores map[string]bool `json:"stores,omitempty"`
}

// ParseKeep parses a keep duration: the usual Go units plus d, w and y, so a
// retention window reads the way it is spoken ("1y", "90d").
func ParseKeep(s string) (time.Duration, error) {
	if s == "" {
		return DefaultKeep, nil
	}
	if strings.HasSuffix(s, "y") {
		n, err := strconv.ParseFloat(strings.TrimSuffix(s, "y"), 64)
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("keep %q: not a duration", s)
		}
		return time.Duration(n * float64(365*24*time.Hour)), nil
	}
	if strings.HasSuffix(s, "w") {
		n, err := strconv.ParseFloat(strings.TrimSuffix(s, "w"), 64)
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("keep %q: not a duration", s)
		}
		return time.Duration(n * float64(7*24*time.Hour)), nil
	}
	if strings.HasSuffix(s, "d") {
		n, err := strconv.ParseFloat(strings.TrimSuffix(s, "d"), 64)
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("keep %q: not a duration", s)
		}
		return time.Duration(n * float64(24*time.Hour)), nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("keep %q: %w", s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("keep %q: not a duration", s)
	}
	return d, nil
}

// DefaultKeep is the retention window when none is declared.
const DefaultKeep = 365 * 24 * time.Hour

// BackupFor reports whether a store keeps history on this machine and for how
// long (zero duration means the default year).
func (c *Config) BackupFor(store string) (bool, time.Duration, error) {
	if c.Backup == nil || !c.Backup.Enable {
		return false, 0, nil
	}
	if off, ok := c.Backup.Stores[store]; ok && !off {
		return false, 0, nil
	}
	keep, err := ParseKeep(c.Backup.Keep)
	if err != nil {
		return false, 0, fmt.Errorf("backup keep: %w", err)
	}
	return true, keep, nil
}

// StreamPolicy is how a `remote` store behaves when no holder answers.
type StreamPolicy string

const (
	// StreamQueueUnbounded accepts offline writes without a byte limit.
	StreamQueueUnbounded StreamPolicy = "unbounded"
	// StreamQueueCapped accepts offline writes only within MaxQueuedBytes.
	StreamQueueCapped StreamPolicy = "capped"
	// StreamQueueReadOnly refuses offline writes; the tree stays visible.
	StreamQueueReadOnly StreamPolicy = "readonly"
)

// OfflineQueue controls queued writes for a streamed store.
type OfflineQueue struct {
	Policy StreamPolicy `json:"policy,omitempty"`
	// MaxBytes bounds a capped queue. Unset means the engine default.
	MaxBytes int64 `json:"maxBytes,omitempty"`
}

// Store is one top-level store declaration. Changing its key creates a new
// store; old canonical data and durable state remain untouched for manual
// migration.
type Store struct {
	Group       string            `json:"group,omitempty"`
	Sync        string            `json:"sync,omitempty"`
	Preferred   string            `json:"preferred,omitempty"`
	Offline     *OfflineQueue     `json:"offline,omitempty"`
	Symlinks    []string          `json:"symlinks,omitempty"`
	Permissions *Permissions      `json:"permissions,omitempty"`
	Links       map[string]string `json:"links,omitempty"`
}

// Config is the complete configuration, shared by the daemon and the CLI.
type Config struct {
	// ConfigVersion pins the schema. A file that declares another version is
	// an error, never a best-effort parse.
	ConfigVersion int `json:"configVersion"`

	// Root is the one directory the bytes live in: every store's canonical
	// dir is <root>/<store>, and the tree below it is made of symlinks. It
	// is a plain container, not a store.
	Root string `json:"root"`

	// DataDir is the one storage directory for daemon state: blob store,
	// journals, index, trash, history.
	DataDir string `json:"dataDir"`

	// Socket is the unix socket the daemon serves /v1 on and the CLI dials.
	Socket string `json:"socket"`

	// Peer is this machine's name. Empty is allowed only while no groups
	// are declared; it then defaults to the hostname.
	Peer string `json:"peer"`

	// Peers are the declared devices, by name.
	Peers map[string]Peer `json:"peers"`

	// Groups bind peers together around stores.
	Groups map[string]Group `json:"groups"`

	// StoreTree is the store tree, by id. Ids are globally unique and name
	// the canonical directory under root.
	StoreTree map[string]Store `json:"stores"`

	// Backup, per machine, keeps a thinned history of every file's versions.
	// A store that is streamed on this machine keeps none.
	Backup *Backup `json:"backup,omitempty"`

	// Permissions overrides, per store on THIS machine, the ownership and
	// mode the daemon gives the store's entries.
	Permissions map[string]Permissions `json:"permissions,omitempty"`
}

type StoreView struct {
	ID          string
	Group       string
	Sync        string
	Preferred   string
	Offline     *OfflineQueue
	Symlinks    []string
	Links       map[string]string
	Permissions *Permissions
	Dir         string
}

// DefaultRoot is where the store tree lives when the config does not say.
func DefaultRoot() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		if dir := DefaultDataDir(); dir != "" {
			return filepath.Join(dir, "root")
		}
		return "Storeboard"
	}
	return filepath.Join(home, "Storeboard")
}

// Default returns the built-in configuration with the XDG defaults filled in.
func Default() *Config {
	return &Config{
		ConfigVersion: ConfigVersion,
		Root:          DefaultRoot(),
		DataDir:       DefaultDataDir(),
		Socket:        DefaultSocketPath(),
		Peers:         map[string]Peer{},
		Groups:        map[string]Group{},
		StoreTree:     map[string]Store{},
		Permissions:   map[string]Permissions{},
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
// StateDirName is the directory inside a store root that holds the daemon's
// state: blobs, journals, history and trash. It sits under the root so a
// store's data is entirely in one place. Store names may not begin with a
// dot, so it can never be mistaken for a store.
const StateDirName = ".storeboard"

// DefaultDataDir is the XDG state location, used when a config names neither
// root nor dataDir (the CLI only needs a socket).
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
	return Parse(raw)
}

// Parse reads a configuration from JSON text: the same strict decoding,
// defaulting and validation Load applies to a file, exposed so an embedder
// (the Android app, whose config arrives as a string) uses one code path.
func Parse(raw []byte) (*Config, error) {
	// Decode into an empty config, not Default(): every default that depends
	// on another field (root, dataDir) has to be computed from what the file
	// actually says, not from the XDG guess.
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
	if cfg.Root == "" {
		cfg.Root = DefaultRoot()
	}
	// Daemon state belongs with the stores: blobs, journals, history and trash
	// are store data, not user data, so an unset dataDir follows the root
	// instead of the XDG guess. A store root on a pool then keeps everything
	// on that pool, and no part of a store can land on another disk because a
	// separate default said so.
	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join(cfg.Root, StateDirName)
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
	if cfg.StoreTree == nil {
		cfg.StoreTree = map[string]Store{}
	}
	if cfg.Permissions == nil {
		cfg.Permissions = map[string]Permissions{}
	}
	for name, g := range cfg.Groups {
		if g.Peers == nil {
			g.Peers = []string{}
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
	if c.Root == "" {
		return fmt.Errorf("root is empty; cannot determine where the store tree lives")
	}
	if !filepath.IsAbs(c.Root) {
		return fmt.Errorf("root %q: path must be absolute", c.Root)
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
		if !containsString(g.Peers, c.Peer) {
			return fmt.Errorf("group %q: this peer %q is not a member", name, c.Peer)
		}
		if err := g.Permissions.Validate(fmt.Sprintf("group %s permissions", name)); err != nil {
			return err
		}
	}

	views := c.StoreViews()
	seenID := map[string]bool{}
	for id := range c.StoreTree {
		seenID[id] = true
		if err := checkName("store", id); err != nil {
			return err
		}
	}
	for _, v := range views {
		seenID[v.ID] = true
		if v.Group == "" {
			return fmt.Errorf("store %q: no group; a top-level store must declare one", v.ID)
		}
		if _, ok := c.Groups[v.Group]; !ok {
			return fmt.Errorf("store %q: unknown group %q", v.ID, v.Group)
		}
		switch v.Sync {
		case SyncLocal, SyncRemote:
		default:
			return fmt.Errorf("store %q: sync %q must be %q or %q", v.ID, v.Sync, SyncLocal, SyncRemote)
		}
		if v.Preferred != "" {
			if _, ok := c.Peers[v.Preferred]; !ok {
				return fmt.Errorf("store %q: preferred peer %q is not declared", v.ID, v.Preferred)
			}
			if !containsString(c.Groups[v.Group].Peers, v.Preferred) {
				return fmt.Errorf("store %q: preferred peer %q is not in group %q", v.ID, v.Preferred, v.Group)
			}
		}
		if v.Offline != nil {
			switch v.Offline.Policy {
			case "", StreamQueueUnbounded, StreamQueueCapped, StreamQueueReadOnly:
			default:
				return fmt.Errorf("store %q: offline policy %q must be %q, %q or %q", v.ID, v.Offline.Policy, StreamQueueUnbounded, StreamQueueCapped, StreamQueueReadOnly)
			}
			if v.Offline.Policy == StreamQueueCapped && v.Offline.MaxBytes <= 0 {
				return fmt.Errorf("store %q: capped offline queue needs a positive maxBytes", v.ID)
			}
			if v.Offline.Policy != StreamQueueCapped && v.Offline.MaxBytes != 0 {
				return fmt.Errorf("store %q: maxBytes needs the capped offline policy", v.ID)
			}
		}
		for target, path := range v.Links {
			if !seenID[target] {
				return fmt.Errorf("store %q link target %q: unknown store", v.ID, target)
			}
			if err := checkRelPath(path); err != nil {
				return fmt.Errorf("store %q link path: %w", v.ID, err)
			}
		}
	}
	var symlinks []struct{ path, store string }
	for _, v := range views {
		for _, sp := range v.Symlinks {
			if !filepath.IsAbs(sp) {
				return fmt.Errorf("store %q symlink %q: path must be absolute", v.ID, sp)
			}
			if filepath.Clean(sp) != sp {
				return fmt.Errorf("store %q symlink %q: path must be clean", v.ID, sp)
			}
			if sp == c.Root || containsPath(sp, c.Root) || containsPath(c.Root, sp) {
				return fmt.Errorf("store %q symlink %q overlaps root %s", v.ID, sp, c.Root)
			}
			symlinks = append(symlinks, struct{ path, store string }{sp, v.ID})
		}
	}
	for i, a := range symlinks {
		for _, b := range symlinks[i+1:] {
			if a.path == b.path {
				return fmt.Errorf("stores %q and %q both claim symlink %q", a.store, b.store, a.path)
			}
			if containsPath(a.path, b.path) || containsPath(b.path, a.path) {
				return fmt.Errorf("symlinks %q and %q nest: one path cannot be inside another", a.path, b.path)
			}
		}
	}
	for _, v := range views {
		seenPaths := map[string]string{}
		for target, path := range v.Links {
			if !seenID[target] {
				return fmt.Errorf("store %q link target %q: unknown store", v.ID, target)
			}
			if other, ok := seenPaths[path]; ok {
				return fmt.Errorf("store %q links %q and %q both appear as %q", v.ID, other, target, path)
			}
			seenPaths[path] = target
			if target == v.ID {
				return fmt.Errorf("store %q links to itself", v.ID)
			}
		}
	}
	if err := c.checkLinkCycles(); err != nil {
		return err
	}
	if c.Backup != nil {
		if _, err := ParseKeep(c.Backup.Keep); err != nil {
			return fmt.Errorf("backup keep: %w", err)
		}
		for store := range c.Backup.Stores {
			if !seenID[store] {
				return fmt.Errorf("backup stores: unknown store %q", store)
			}
		}
	}

	for store, p := range c.Permissions {
		if !seenID[store] {
			return fmt.Errorf("permissions: unknown store %q", store)
		}
		if err := p.Validate(fmt.Sprintf("permissions %s", store)); err != nil {
			return err
		}
	}
	return nil
}

// checkRelPath accepts a clean relative path inside a parent.
func checkRelPath(p string) error {
	if p == "" {
		return fmt.Errorf("path is empty")
	}
	if filepath.IsAbs(p) {
		return fmt.Errorf("path %q must be relative", p)
	}
	clean := filepath.Clean(p)
	if clean != p || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("path %q must be a clean relative path inside the parent", p)
	}
	return nil
}

func (c *Config) StoreViews() []StoreView {
	out := make([]StoreView, 0, len(c.StoreTree))
	for id, s := range c.StoreTree {
		links := map[string]string{}
		for target, path := range s.Links {
			links[target] = path
		}
		out = append(out, StoreView{
			ID:          id,
			Group:       s.Group,
			Sync:        firstNonEmpty(s.Sync, SyncLocal),
			Preferred:   s.Preferred,
			Offline:     s.Offline,
			Symlinks:    append([]string(nil), s.Symlinks...),
			Links:       links,
			Permissions: mergePerms(nil, s.Permissions),
			Dir:         filepath.Join(c.Root, id),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c *Config) checkLinkCycles() error {
	state := map[string]uint8{}
	var visit func(string) error
	visit = func(id string) error {
		if state[id] == 1 {
			return fmt.Errorf("store link cycle involving %q", id)
		}
		if state[id] == 2 {
			return nil
		}
		state[id] = 1
		for target := range c.StoreTree[id].Links {
			if err := visit(target); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}
	for id := range c.StoreTree {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

// StoreView returns one resolved top-level store.
func (c *Config) StoreView(id string) (StoreView, bool) {
	for _, v := range c.StoreViews() {
		if v.ID == id {
			return v, true
		}
	}
	return StoreView{}, false
}

// PermissionsFor is the declared permissions for a store: the store's own
// declaration wins over its group's default, then the machine override.
func (c *Config) PermissionsFor(store string) *Permissions {
	v, ok := c.StoreView(store)
	var declared, groupDefault *Permissions
	if ok {
		declared = v.Permissions
		groupDefault = c.Groups[v.Group].Permissions
	}
	out := mergePerms(groupDefault, declared)
	if local, exists := c.Permissions[store]; exists {
		out = mergePerms(out, &local)
	}
	return out
}

// PeerList lists the declared peers, sorted by name, endpoints intact.
// Empty lists stay arrays — the contract says arrays, never null.
func (c *Config) PeerList() []api.Peer {
	out := make([]api.Peer, 0, len(c.Peers))
	for name, p := range c.Peers {
		eps := append([]string(nil), p.Endpoints...)
		if eps == nil {
			eps = []string{}
		}
		out = append(out, api.Peer{Name: name, Endpoints: eps})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// GroupList lists the declared groups, sorted by name; each group's stores
// are sorted too.
func (c *Config) GroupList() []api.Group {
	views := c.StoreViews()
	out := make([]api.Group, 0, len(c.Groups))
	for name, g := range c.Groups {
		stores := []string{}
		for _, v := range views {
			if v.Group == name {
				stores = append(stores, v.ID)
			}
		}
		sort.Strings(stores)
		peers := append([]string(nil), g.Peers...)
		if peers == nil {
			peers = []string{}
		}
		out = append(out, api.Group{Name: name, Peers: peers, Stores: stores})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Stores lists every top-level store with its resolved view, sorted by id.
func (c *Config) Stores() []api.Store {
	views := c.StoreViews()
	out := make([]api.Store, 0, len(views))
	for _, v := range views {
		symlinks := append([]string(nil), v.Symlinks...)
		if symlinks == nil {
			symlinks = []string{}
		}
		out = append(out, api.Store{
			Name:     v.ID,
			Group:    v.Group,
			Sync:     v.Sync,
			Root:     v.Dir,
			Symlinks: symlinks,
			Links:    v.Links,
		})
	}
	return out
}

// checkName is the identity rule for peers, groups and stores: a name ends
// up in JSON keys, journal entries and CLI output, so it is an identifier,
// never a path.
func checkName(kind, name string) error {
	if name == "" {
		return fmt.Errorf("%s name is empty", kind)
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return fmt.Errorf("%s name %q: not an identifier; use letters, digits, '-', '_' or '.'", kind, name)
		}
	}
	if name == "." || name == ".." {
		return fmt.Errorf("%s name %q is a path element", kind, name)
	}
	if strings.HasPrefix(name, ".") {
		return fmt.Errorf("%s name %q: leading '.' is reserved", kind, name)
	}
	return nil
}

// checkEndpoint accepts host:port — the wire format every declared endpoint
// uses. Bare hostnames are rejected until the protocol picks a default port.
func checkEndpoint(ep string) error {
	host, port, err := cutHostPort(ep)
	if err != nil {
		return err
	}
	if host == "" || port == "" {
		return fmt.Errorf("want host:port")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("port %q is not a number in 1-65535", port)
	}
	return nil
}

// cutHostPort splits host:port, including bracketed IPv6 literals.
func cutHostPort(ep string) (string, string, error) {
	if strings.HasPrefix(ep, "[") {
		end := strings.Index(ep, "]")
		if end < 0 {
			return "", "", fmt.Errorf("unterminated IPv6 literal")
		}
		if end+1 >= len(ep) || ep[end+1] != ':' {
			return "", "", fmt.Errorf("want host:port")
		}
		return ep[1:end], ep[end+2:], nil
	}
	host, port, ok := strings.Cut(ep, ":")
	if !ok {
		return "", "", fmt.Errorf("want host:port")
	}
	if strings.Contains(port, ":") {
		return "", "", fmt.Errorf("IPv6 literals must be bracketed")
	}
	return host, port, nil
}

// firstNonEmpty returns the first non-empty string.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// mergePerms overlays own on inherited, field by field.
func mergePerms(inherited, own *Permissions) *Permissions {
	if inherited == nil && own == nil {
		return nil
	}
	out := &Permissions{}
	if inherited != nil {
		out.User, out.Group, out.Mode = inherited.User, inherited.Group, inherited.Mode
	}
	if own != nil {
		if own.User != "" {
			out.User = own.User
		}
		if own.Group != "" {
			out.Group = own.Group
		}
		if own.Mode != "" {
			out.Mode = own.Mode
		}
	}
	if out.User == "" && out.Group == "" && out.Mode == "" {
		return nil
	}
	return out
}

// containsPath reports whether outer is a proper ancestor of inner.
// Path-boundary aware: /srv/photos does not contain /srv/photos-old.
func containsPath(outer, inner string) bool {
	outer, inner = filepath.Clean(outer), filepath.Clean(inner)
	if outer == inner {
		return false
	}
	return strings.HasPrefix(inner, outer+string(filepath.Separator))
}

func containsString(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}
