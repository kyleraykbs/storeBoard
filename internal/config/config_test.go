package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// valid returns the baseline model: this peer, two more devices, two groups,
// and three flat stores with a reusable bulk link.
func valid() *Config {
	return &Config{
		ConfigVersion: ConfigVersion,
		Root:          "/home/kyle/Storeboard",
		DataDir:       "/var/lib/storeboard",
		Socket:        "/run/storeboard.sock",
		Peer:          "laptop",
		Peers: map[string]Peer{
			"laptop":  {Endpoints: []string{"192.168.1.10:4242"}},
			"desktop": {Endpoints: []string{"192.168.1.11:4242", "[fd00::11]:4242"}},
			"phone":   {Endpoints: []string{}},
		},
		Groups: map[string]Group{
			"family": {Peers: []string{"laptop", "desktop", "phone"}},
			"media":  {Peers: []string{"laptop", "desktop"}},
		},
		StoreTree: map[string]Store{
			"pictures":  {Group: "family", Links: map[string]string{"bulk": "Bulk"}},
			"bulk":      {Group: "media", Sync: SyncRemote},
			"documents": {Group: "family", Symlinks: []string{"/home/kyle/Documents"}},
		},
		Permissions: map[string]Permissions{},
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	path := writeConfig(t, `{"configVersion": 2, "strs": {}}`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("unknown key accepted")
	}
	if !strings.Contains(err.Error(), "strs") {
		t.Fatalf("error does not name the bad key: %v", err)
	}
}

func TestLoadFillsDefaultsAndClaimsHostname(t *testing.T) {
	_, runtimeDir := setXDG(t)
	path := writeConfig(t, `{}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConfigVersion != ConfigVersion {
		t.Errorf("configVersion = %d, want %d", cfg.ConfigVersion, ConfigVersion)
	}
	home, _ := os.UserHomeDir()
	if want := filepath.Join(home, "Storeboard"); cfg.Root != want {
		t.Errorf("root = %q, want %q", cfg.Root, want)
	}
	// State follows the root: a store's blobs, journals and history belong
	// with the store, not on whatever disk the XDG default names.
	if want := filepath.Join(cfg.Root, StateDirName); cfg.DataDir != want {
		t.Errorf("dataDir = %q, want %q", cfg.DataDir, want)
	}
	if want := filepath.Join(runtimeDir, "storeboard.sock"); cfg.Socket != want {
		t.Errorf("socket = %q, want %q", cfg.Socket, want)
	}
	// A nameless fresh install claims the hostname and registers itself.
	if cfg.Peer == "" {
		t.Error("peer is empty; the hostname default did not apply")
	}
	if _, ok := cfg.Peers[cfg.Peer]; !ok {
		t.Errorf("peer %q not registered in peers: %#v", cfg.Peer, cfg.Peers)
	}
	if cfg.Groups == nil || cfg.StoreTree == nil || cfg.Permissions == nil {
		t.Error("maps must be non-nil after load")
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
	path := writeConfig(t, `{"configVersion": 2, "socket": ""}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(runtimeDir, "storeboard.sock"); cfg.Socket != want {
		t.Errorf("socket = %q, want the default %q", cfg.Socket, want)
	}
}

func TestLoadFullModel(t *testing.T) {
	path := writeConfig(t, `{
		"configVersion": 2,
		"root": "/srv/board",
		"peer": "laptop",
		"peers": {
			"laptop": { "endpoints": ["192.168.1.10:4242"] },
			"desktop": { "endpoints": ["192.168.1.11:4242"] }
		},
		"groups": {
			"family": { "peers": ["laptop", "desktop"] },
			"media": { "peers": ["laptop"] }
		},
		"stores": {
			"pictures": { "group": "family", "links": { "bulk": "Bulk" } },
			"bulk": { "group": "media", "sync": "remote" },
			"documents": { "group": "family", "symlinks": ["/home/alice/Documents"] }
		}
	}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	bulk, ok := cfg.StoreView("bulk")
	if !ok || bulk.Sync != SyncRemote || bulk.Dir != filepath.Join("/srv/board", "bulk") {
		t.Fatalf("bulk = %+v", bulk)
	}
	docs, _ := cfg.StoreView("documents")
	if len(docs.Symlinks) != 1 || docs.Symlinks[0] != "/home/alice/Documents" {
		t.Errorf("documents symlinks = %v", docs.Symlinks)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"valid", func(c *Config) {}, ""},
		{"wrong config version", func(c *Config) { c.ConfigVersion = 1 }, "configVersion"},
		{"empty root", func(c *Config) { c.Root = "" }, "root"},
		{"relative root", func(c *Config) { c.Root = "Storeboard" }, "absolute"},
		{"empty data dir", func(c *Config) { c.DataDir = "" }, "dataDir"},
		{"relative data dir", func(c *Config) { c.DataDir = "var/lib" }, "absolute"},
		{"empty socket", func(c *Config) { c.Socket = "" }, "socket"},
		{"relative socket", func(c *Config) { c.Socket = "run/s.sock" }, "absolute"},
		{"peer not declared", func(c *Config) { c.Peer = "ghost" }, "not declared in peers"},
		{"peer name is a path", func(c *Config) {
			c.Peer = "a/b"
			c.Peers["a/b"] = Peer{}
		}, "identifier"},
		{"peer key is a path", func(c *Config) { c.Peers["a/b"] = Peer{} }, "identifier"},
		{"endpoint without port", func(c *Config) {
			c.Peers["laptop"] = Peer{Endpoints: []string{"192.168.1.10"}}
		}, "host:port"},
		{"endpoint with dead port", func(c *Config) {
			c.Peers["laptop"] = Peer{Endpoints: []string{"192.168.1.10:0"}}
		}, "1-65535"},
		{"group without peers", func(c *Config) { c.Groups["family"] = Group{} }, "no peers"},
		{"group with unknown peer", func(c *Config) {
			c.Groups["family"] = Group{Peers: []string{"laptop", "stranger"}}
		}, "unknown peer"},
		{"group without this peer", func(c *Config) {
			c.Groups["family"] = Group{Peers: []string{"desktop", "phone"}}
		}, "not a member"},
		{"top-level store without group", func(c *Config) {
			c.StoreTree["loose"] = Store{}
		}, "no group"},
		{"store with unknown group", func(c *Config) {
			c.StoreTree["loose"] = Store{Group: "ghost"}
		}, "unknown group"},
		{"bad sync type", func(c *Config) {
			c.StoreTree["loose"] = Store{Group: "family", Sync: "sometimes"}
		}, "sync"},
		{"invalid link path", func(c *Config) {
			c.StoreTree["pictures"].Links["bulk"] = "/Bulk"
		}, "relative"},
		{"escaping link path", func(c *Config) {
			c.StoreTree["pictures"].Links["bulk"] = "../Bulk"
		}, "clean relative"},
		{"duplicate link paths", func(c *Config) {
			c.StoreTree["other"] = Store{Group: "family"}
			c.StoreTree["pictures"].Links["other"] = "Bulk"
		}, "both appear"},
		{"unknown link target", func(c *Config) {
			c.StoreTree["pictures"].Links["missing"] = "Missing"
		}, "unknown store"},
		{"link cycle", func(c *Config) {
			bulk := c.StoreTree["bulk"]
			bulk.Links = map[string]string{"pictures": "Pictures"}
			pictures := c.StoreTree["pictures"]
			pictures.Links["bulk"] = "Bulk"
			c.StoreTree["bulk"] = bulk
			c.StoreTree["pictures"] = pictures
		}, "cycle"},
		{"store name is a path", func(c *Config) {
			c.StoreTree["a/b"] = Store{Group: "family"}
		}, "identifier"},
		{"relative symlink", func(c *Config) {
			c.StoreTree["documents"] = Store{Group: "family", Symlinks: []string{"Documents"}}
		}, "absolute"},
		{"symlink inside root", func(c *Config) {
			c.StoreTree["documents"] = Store{Group: "family", Symlinks: []string{"/home/kyle/Storeboard/Documents"}}
		}, "overlaps root"},
		{"symlink above root", func(c *Config) {
			c.StoreTree["documents"] = Store{Group: "family", Symlinks: []string{"/home/kyle"}}
		}, "overlaps root"},
		{"duplicate symlink", func(c *Config) {
			c.StoreTree["pictures"] = Store{Group: "family", Symlinks: []string{"/home/kyle/Documents"}}
		}, "claim symlink"},
		{"nested symlinks", func(c *Config) {
			c.StoreTree["documents"] = Store{Group: "family", Symlinks: []string{"/home/kyle/Documents"}}
			c.StoreTree["pictures"] = Store{Group: "family", Symlinks: []string{"/home/kyle/Documents/Old"}}
		}, "nest"},
		{"backup for unknown store", func(c *Config) {
			c.Backup = &Backup{Enable: true, Stores: map[string]bool{"ghost": false}}
		}, "unknown store"},
		{"permissions for unknown store", func(c *Config) {
			c.Permissions["ghost"] = Permissions{User: "kyle"}
		}, "unknown store"},
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

// Store policy is declared on each top-level store; links only choose where
// another store is presented.
func TestFlatStorePolicies(t *testing.T) {
	cfg := valid()
	cfg.StoreTree["pictures"] = Store{Group: "family", Sync: SyncLocal, Permissions: &Permissions{Group: "users", Mode: "0770"}, Links: map[string]string{"bulk": "Bulk"}}
	cfg.StoreTree["bulk"] = Store{Group: "media", Sync: SyncRemote, Permissions: &Permissions{User: "kyle"}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	bulk, _ := cfg.StoreView("bulk")
	if bulk.Group != "media" || bulk.Sync != SyncRemote || bulk.Permissions.User != "kyle" {
		t.Errorf("bulk = %+v", bulk)
	}
}

func TestStoreViewsAreSortedAndNonNil(t *testing.T) {
	cfg := valid()

	views := cfg.StoreViews()
	if len(views) != 3 || views[0].ID != "bulk" || views[1].ID != "documents" || views[2].ID != "pictures" {
		t.Errorf("views not sorted: %#v", views)
	}
	stores := cfg.Stores()
	if len(stores) != 3 || stores[0].Name != "bulk" {
		t.Errorf("stores not sorted: %#v", stores)
	}
	for _, s := range stores {
		if s.Symlinks == nil {
			t.Errorf("store %q: nil symlinks; the contract says arrays", s.Name)
		}
	}

	empty := &Config{ConfigVersion: ConfigVersion, Root: "/r", DataDir: "/d", Socket: "/s", Peers: map[string]Peer{}, Groups: map[string]Group{}}
	if peers := empty.PeerList(); peers == nil {
		t.Error("PeerList nil on empty config")
	}
	if groups := empty.GroupList(); groups == nil {
		t.Error("GroupList nil on empty config")
	}
	if stores := empty.Stores(); stores == nil {
		t.Error("Stores nil on empty config")
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
	// Blank environment falls back to the system file — pointed at a path
	// that does not exist here, so the expectation stays deterministic.
	SystemConfigPath = "/no-such-storeboard-test/config.json"
	defer func() { SystemConfigPath = "/etc/storeboard/config.json" }()
	if got := ResolvePath(""); got != "" {
		t.Errorf("blank environment = %q, want empty", got)
	}
}

func TestResolvePathSystemFallback(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv(ConfigEnvVar, "")

	// A host-dependent /etc file would make this test non-deterministic;
	// point the candidate at the test's own fixtures.
	SystemConfigPath = filepath.Join(t.TempDir(), "absent.json")
	defer func() { SystemConfigPath = "/etc/storeboard/config.json" }()

	if got := ResolvePath(""); got != "" {
		t.Fatalf("ResolvePath = %q with no existing candidates, want empty", got)
	}

	// The system file is the last candidate: a deployed host has one, and a
	// shell that never saw STOREBOARD_CONFIG must still find the daemon.
	exists := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(exists, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	SystemConfigPath = exists

	if got := ResolvePath(""); got != exists {
		t.Errorf("ResolvePath = %q, want the system file %q", got, exists)
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

func TestPermissionsPrecedenceAndValidation(t *testing.T) {
	path := writeConfig(t, `{
  "configVersion": 2,
  "root": "/home/kyle/Storeboard",
  "peer": "laptop",
  "dataDir": "/var/lib/storeboard",
  "socket": "/run/storeboard.sock",
  "groups": {
    "family": {
      "peers": ["laptop"],
      "permissions": {"group": "users", "mode": "2770"}
    }
  },
  "stores": {"photos": {"group": "family"}}
}`)
	if _, err := Load(path); err == nil {
		t.Fatal("mode 2770: setgid is not a directory mode this config accepts")
	}

	path = writeConfig(t, `{
  "configVersion": 2,
  "root": "/home/kyle/Storeboard",
  "peer": "laptop",
  "dataDir": "/var/lib/storeboard",
  "socket": "/run/storeboard.sock",
  "groups": {"family": {"peers": ["laptop"], "permissions": {"group": "users", "mode": "0770"}}},
  "stores": {"photos": {"group": "family"}, "documents": {"group": "family", "permissions": {"mode": "0750"}}},
  "permissions": {"photos": {"user": "kyle"}}
}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.PermissionsFor("photos")
	if got == nil || got.User != "kyle" || got.Group != "users" || got.Mode != "0770" {
		t.Fatalf("photos permissions = %+v, want kyle/users/0770 (machine field over group default)", got)
	}
	if got := cfg.PermissionsFor("documents"); got == nil || got.User != "" || got.Mode != "0750" {
		t.Fatalf("documents permissions = %+v, want the store's own mode over the group default", got)
	}

	cases := []struct {
		name, body, want string
	}{
		{"unknown store", `"nope": {"user": "kyle"}`, "unknown store"},
		{"bad mode", `"photos": {"mode": "99"}`, "mode"},
		{"colon list", `"photos": {"user": "kyle:root"}`, "name or uid"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := writeConfig(t, `{
  "configVersion": 2,
  "root": "/home/kyle/Storeboard",
  "peer": "laptop",
  "dataDir": "/var/lib/storeboard",
  "socket": "/run/storeboard.sock",
  "groups": {"family": {"peers": ["laptop"]}},
  "stores": {"photos": {"group": "family"}},
  "permissions": {`+c.body+`}
}`)
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want it to mention %q", err, c.want)
			}
		})
	}
}

func TestParseMode(t *testing.T) {
	for in, want := range map[string]os.FileMode{"755": 0o755, "0755": 0o755, "0700": 0o700, "777": 0o777} {
		got, err := ParseMode(in)
		if err != nil || got != want {
			t.Errorf("ParseMode(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "0o755", "8", "1755", "7777", "-755"} {
		if _, err := ParseMode(in); err == nil {
			t.Errorf("ParseMode(%q): want an error", in)
		}
	}
}

func TestParseKeep(t *testing.T) {
	day := 24 * time.Hour
	cases := map[string]time.Duration{
		"1y":  365 * day,
		"2y":  730 * day,
		"90d": 90 * day,
		"4w":  28 * day,
		"36h": 36 * time.Hour,
		"":    DefaultKeep,
	}
	for in, want := range cases {
		got, err := ParseKeep(in)
		if err != nil || got != want {
			t.Errorf("ParseKeep(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"1y2d", "0d", "-1d", "d", "1m2", "soon"} {
		if _, err := ParseKeep(bad); err == nil {
			t.Errorf("ParseKeep(%q): want an error", bad)
		}
	}
}

func TestBackupConfig(t *testing.T) {
	base := `{
  "configVersion": 2,
  "root": "/home/kyle/Storeboard",
  "peer": "laptop",
  "dataDir": "/var/lib/storeboard",
  "socket": "/run/storeboard.sock",
  "groups": {"family": {"peers": ["laptop"]}},
  "stores": {"docs": {"group": "family"}, "media": {"group": "family"}},
  %s
}`

	// No block: no history anywhere.
	path := writeConfig(t, fmt.Sprintf(base, `"permissions": {}`))
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if on, _, err := cfg.BackupFor("docs"); err != nil || on {
		t.Fatalf("BackupFor without a block = %v, %v; want off", on, err)
	}

	// Enabled, one store opted out, a window of 90 days.
	path = writeConfig(t, fmt.Sprintf(base, `"backup": {"enable": true, "keep": "90d", "stores": {"media": false}}`))
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	on, keep, err := cfg.BackupFor("docs")
	if err != nil || !on || keep != 90*24*time.Hour {
		t.Fatalf("docs backup = %v/%v/%v, want on for 90d", on, keep, err)
	}
	if on, _, err := cfg.BackupFor("media"); err != nil || on {
		t.Fatalf("media backup = %v, %v; want it opted out", on, err)
	}

	// A keep value the daemon cannot read fails the config, not the daemon.
	path = writeConfig(t, fmt.Sprintf(base, `"backup": {"enable": true, "keep": "1y2d"}`))
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "keep") {
		t.Fatalf("err = %v, want a keep error", err)
	}
}
