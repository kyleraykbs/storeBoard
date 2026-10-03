// Package config loads and validates storeboard's JSON configuration.
//
// The file is strict: unknown keys are an error, so a typo can never be
// silently ignored. Absent keys keep their defaults, which makes an empty
// object a valid configuration. The schema is versioned like every other
// JSON surface (configVersion); this build reads exactly version 1.
package config

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// ConfigVersion is the only config schema version this build reads.
const ConfigVersion = 1

// ConfigEnvVar points at a configuration file, for deployments that install
// one system-wide (the NixOS CLI module does). An explicit --config still
// wins, and the per-user XDG file is the default when neither is set.
const ConfigEnvVar = "STOREBOARD_CONFIG"

// appName is the directory storeboard keeps its files in under each XDG base.
const appName = "storeboard"

// Config is the complete configuration, shared by the daemon and the CLI.
type Config struct {
	// ConfigVersion pins the schema. A file that declares another version is
	// an error, never a best-effort parse.
	ConfigVersion int `json:"configVersion"`

	// DataDir is where the daemon keeps its state: store, journal, index.
	DataDir string `json:"dataDir"`

	// Socket is the unix socket the daemon serves /v1 on and the CLI dials.
	Socket string `json:"socket"`

	// Folders are the folders this machine syncs.
	Folders []api.Folder `json:"folders"`
}

// Default returns the built-in configuration with the XDG defaults filled in.
func Default() *Config {
	return &Config{
		ConfigVersion: ConfigVersion,
		DataDir:       DefaultDataDir(),
		Socket:        DefaultSocketPath(),
		Folders:       []api.Folder{},
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

// ResolvePath decides which configuration file to load: the explicit flag,
// then an existing per-user file — the more specific declaration beats a
// system-wide default, so a home-manager configuration wins over the
// NixOS module's /etc file — then the environment. An empty result means
// Load falls back to DefaultPath().
func ResolvePath(explicit string) string {
	if trimmed := strings.TrimSpace(explicit); trimmed != "" {
		return trimmed
	}
	if p := DefaultPath(); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return strings.TrimSpace(os.Getenv(ConfigEnvVar))
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
	if cfg.Folders == nil {
		cfg.Folders = []api.Folder{}
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

	seen := make(map[string]bool, len(c.Folders))
	for _, f := range c.Folders {
		if f.Name == "" {
			return fmt.Errorf("folder with empty name")
		}
		if f.Name == "." || f.Name == ".." || strings.ContainsAny(f.Name, `/\`) {
			return fmt.Errorf("folder name %q: a name is an identifier, not a path", f.Name)
		}
		if seen[f.Name] {
			return fmt.Errorf("folder name %q: duplicated", f.Name)
		}
		seen[f.Name] = true
		if f.Path == "" {
			return fmt.Errorf("folder %q: path is empty", f.Name)
		}
		if !filepath.IsAbs(f.Path) {
			return fmt.Errorf("folder %q: path %q must be absolute", f.Name, f.Path)
		}
	}
	return nil
}
