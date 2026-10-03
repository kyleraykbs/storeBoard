package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// setXDG pins every directory the defaults read, so the test does not
// depend on the environment it runs in.
func setXDG(t *testing.T) (dataHome, runtimeDir string) {
	t.Helper()
	dataHome = t.TempDir()
	runtimeDir = t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_RUNTIME_DIR", runtimeDir)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	return dataHome, runtimeDir
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	path := writeConfig(t, `{"configVersion": 1, "folars": []}`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("unknown key accepted")
	}
	if !strings.Contains(err.Error(), "folars") {
		t.Fatalf("error does not name the bad key: %v", err)
	}
}

func TestLoadFillsDefaults(t *testing.T) {
	dataHome, runtimeDir := setXDG(t)
	path := writeConfig(t, `{}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConfigVersion != ConfigVersion {
		t.Errorf("configVersion = %d, want %d", cfg.ConfigVersion, ConfigVersion)
	}
	if want := filepath.Join(dataHome, "storeboard"); cfg.DataDir != want {
		t.Errorf("dataDir = %q, want %q", cfg.DataDir, want)
	}
	if want := filepath.Join(runtimeDir, "storeboard.sock"); cfg.Socket != want {
		t.Errorf("socket = %q, want %q", cfg.Socket, want)
	}
	if cfg.Folders == nil || len(cfg.Folders) != 0 {
		t.Errorf("folders = %#v, want a non-nil empty slice", cfg.Folders)
	}
}

func TestLoadExplicitPathMustExist(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.json")
	if _, err := Load(missing); err == nil {
		t.Fatal("missing explicit config accepted")
	}
}

func TestLoadMissingDefaultYieldsDefaults(t *testing.T) {
	setXDG(t) // XDG_CONFIG_HOME points at an empty directory

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("missing default config: %v", err)
	}
	if cfg.ConfigVersion != ConfigVersion {
		t.Errorf("configVersion = %d, want %d", cfg.ConfigVersion, ConfigVersion)
	}
}

func TestLoadRejectsTrailingData(t *testing.T) {
	path := writeConfig(t, `{} {}`)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing data: %v", err)
	}
}

func TestLoadFillsBlankStringsFromDefaults(t *testing.T) {
	_, runtimeDir := setXDG(t)
	path := writeConfig(t, `{"configVersion": 1, "socket": ""}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(runtimeDir, "storeboard.sock"); cfg.Socket != want {
		t.Errorf("socket = %q, want the default %q", cfg.Socket, want)
	}
}

func TestValidate(t *testing.T) {
	valid := func() *Config {
		return &Config{
			ConfigVersion: ConfigVersion,
			DataDir:       "/var/lib/storeboard",
			Socket:        "/run/storeboard.sock",
			Folders:       []api.Folder{{Name: "photos", Path: "/home/kyle/Pictures"}},
		}
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"valid", func(c *Config) {}, ""},
		{"wrong config version", func(c *Config) { c.ConfigVersion = 2 }, "configVersion"},
		{"empty data dir", func(c *Config) { c.DataDir = "" }, "dataDir"},
		{"relative data dir", func(c *Config) { c.DataDir = "var/lib" }, "absolute"},
		{"empty socket", func(c *Config) { c.Socket = "" }, "socket"},
		{"relative socket", func(c *Config) { c.Socket = "run/s.sock" }, "absolute"},
		{"empty folder name", func(c *Config) { c.Folders[0].Name = "" }, "empty name"},
		{"folder name with slash", func(c *Config) { c.Folders[0].Name = "a/b" }, "identifier"},
		{"duplicate folder name", func(c *Config) {
			c.Folders = append(c.Folders, api.Folder{Name: "photos", Path: "/other"})
		}, "duplicated"},
		{"relative folder path", func(c *Config) { c.Folders[0].Path = "Pictures" }, "absolute"},
		{"empty folder path", func(c *Config) { c.Folders[0].Path = "" }, "path is empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid()
			tt.mutate(cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestResolvePath(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir()) // no per-user file exists
	t.Setenv(ConfigEnvVar, "/etc/storeboard/config.json")

	if got := ResolvePath("/explicit.json"); got != "/explicit.json" {
		t.Errorf("explicit flag lost: %q", got)
	}
	if got := ResolvePath(""); got != "/etc/storeboard/config.json" {
		t.Errorf("environment lost: %q", got)
	}
	t.Setenv(ConfigEnvVar, "  ")
	if got := ResolvePath(""); got != "" {
		t.Errorf("blank environment = %q, want empty", got)
	}
}

func TestResolvePathUserFileBeatsEnvironment(t *testing.T) {
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv(ConfigEnvVar, "/etc/storeboard/config.json")

	userFile := filepath.Join(xdg, "storeboard", "config.json")
	if err := os.MkdirAll(filepath.Dir(userFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(userFile, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	if got := ResolvePath(""); got != userFile {
		t.Errorf("ResolvePath = %q, want the user file %q to beat the environment", got, userFile)
	}
}
