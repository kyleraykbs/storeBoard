package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"
)

// Requested is a store a program asked for at runtime instead of one
// declared in the config (DESIGN: requested stores). The config always
// wins: a declared store of the same name is the declaration, and a
// request only fills in what nobody declared.
type Requested struct {
	Group       string       `json:"group"`
	Sync        string       `json:"sync,omitempty"`
	Permissions *Permissions `json:"permissions,omitempty"`
	// NoBackup keeps no version history for it, where the group would.
	NoBackup bool `json:"noBackup,omitempty"`
	// By is who asked: a systemd unit, or "root".
	By string    `json:"by"`
	At time.Time `json:"at"`
}

// RequestedFile is where requests are kept, in the data dir.
func RequestedFile(dataDir string) string { return filepath.Join(dataDir, "requested.json") }

// LoadRequested reads the requests; none is an empty map.
func LoadRequested(dataDir string) (map[string]Requested, error) {
	b, err := os.ReadFile(RequestedFile(dataDir))
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]Requested{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]Requested{}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("%s: %w", RequestedFile(dataDir), err)
	}
	return out, nil
}

// SaveRequested writes the requests atomically.
func SaveRequested(dataDir string, req map[string]Requested) error {
	b, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	tmp := RequestedFile(dataDir) + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, RequestedFile(dataDir))
}

// Declared reports whether the config itself declares a top-level store.
func (c *Config) Declared(name string) bool {
	_, ok := c.StoreTree[name]
	return ok
}

// WithRequested returns a copy of the config with every request merged in
// that the config does not declare and this machine can carry (its group
// exists and has this peer), and the names merged. RequestedBy on the copy
// records who asked for each.
func (c *Config) WithRequested(req map[string]Requested) (*Config, []string) {
	out := *c
	out.StoreTree = make(map[string]Store, len(c.StoreTree)+len(req))
	for k, v := range c.StoreTree {
		out.StoreTree[k] = v
	}
	if c.Backup != nil {
		b := *c.Backup
		b.Stores = make(map[string]bool, len(c.Backup.Stores))
		for k, v := range c.Backup.Stores {
			b.Stores[k] = v
		}
		out.Backup = &b
	}
	out.RequestedBy = map[string]string{}
	var merged []string
	names := make([]string, 0, len(req))
	for name := range req {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		r := req[name]
		if c.Declared(name) {
			continue
		}
		g, ok := c.Groups[r.Group]
		if !ok || !slices.Contains(g.Peers, c.Peer) {
			continue
		}
		out.StoreTree[name] = Store{Group: r.Group, Sync: r.Sync, Permissions: r.Permissions}
		if r.NoBackup && out.Backup != nil {
			if out.Backup.Stores == nil {
				out.Backup.Stores = map[string]bool{}
			}
			if _, set := out.Backup.Stores[name]; !set {
				out.Backup.Stores[name] = false
			}
		}
		out.RequestedBy[name] = r.By
		merged = append(merged, name)
	}
	return &out, merged
}
