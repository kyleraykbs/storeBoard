package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

// valid returns the baseline model: this peer, two more devices, one group
// with two folders, one local mount override.
func valid() *Config {
	return &Config{
		ConfigVersion: ConfigVersion,
		DataDir:       "/var/lib/storeboard",
		Socket:        "/run/storeboard.sock",
		Peer:          "laptop",
		Peers: map[string]Peer{
			"laptop":  {Endpoints: []string{"192.168.1.10:4242"}},
			"desktop": {Endpoints: []string{"192.168.1.11:4242", "[fd00::11]:4242"}},
			"phone":   {Endpoints: []string{}},
		},
		Groups: map[string]Group{
			"family": {
				Peers: []string{"laptop", "desktop", "phone"},
				Folders: map[string]GroupFolder{
					"photos":    {Mount: "/home/kyle/Pictures"},
					"documents": {},
				},
			},
		},
		Mounts:      map[string]string{"documents": "/mnt/docs"},
		KeepOffline: map[string]bool{},
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	path := writeConfig(t, `{"configVersion": 1, "folars": {}}`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("unknown key accepted")
	}
	if !strings.Contains(err.Error(), "folars") {
		t.Fatalf("error does not name the bad key: %v", err)
	}
}

func TestLoadFillsDefaultsAndClaimsHostname(t *testing.T) {
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
	// A nameless fresh install claims the hostname and registers itself.
	if cfg.Peer == "" {
		t.Error("peer is empty; the hostname default did not apply")
	}
	if _, ok := cfg.Peers[cfg.Peer]; !ok {
		t.Errorf("peer %q not registered in peers: %#v", cfg.Peer, cfg.Peers)
	}
	if cfg.Groups == nil || cfg.Mounts == nil {
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
	path := writeConfig(t, `{"configVersion": 1, "socket": ""}`)

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
		"configVersion": 1,
		"peer": "laptop",
		"peers": {
			"laptop":  { "endpoints": ["192.168.1.10:4242"] },
			"desktop": { "endpoints": ["192.168.1.11:4242"] }
		},
		"groups": {
			"family": {
				"peers": ["laptop", "desktop"],
				"folders": { "photos": { "mount": "/home/alice/Pictures" } }
			}
		},
		"mounts": {}
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Peer != "laptop" {
		t.Errorf("peer = %q, want laptop", cfg.Peer)
	}
	g, ok := cfg.Groups["family"]
	if !ok {
		t.Fatalf("groups = %#v, want family", cfg.Groups)
	}
	if g.Folders["photos"].Mount != "/home/alice/Pictures" {
		t.Errorf("photos mount = %q", g.Folders["photos"].Mount)
	}
}

func TestValidate(t *testing.T) {
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
		{"group without peers", func(c *Config) { c.Groups["family"] = Group{Peers: nil, Folders: map[string]GroupFolder{"photos": {}}} }, "no peers"},
		{"group with unknown peer", func(c *Config) {
			c.Groups["family"] = Group{Peers: []string{"laptop", "stranger"}, Folders: map[string]GroupFolder{"photos": {}}}
		}, "unknown peer"},
		{"group without this peer", func(c *Config) {
			c.Groups["family"] = Group{Peers: []string{"desktop", "phone"}, Folders: map[string]GroupFolder{"photos": {}}}
		}, "not a member"},
		{"duplicate folder across groups", func(c *Config) {
			c.Groups["work"] = Group{
				Peers:   []string{"laptop", "desktop"},
				Folders: map[string]GroupFolder{"photos": {}},
			}
		}, "global"},
		{"relative group mount", func(c *Config) {
			c.Groups["family"] = Group{
				Peers:   []string{"laptop", "desktop", "phone"},
				Folders: map[string]GroupFolder{"photos": {Mount: "Pictures"}},
			}
		}, "absolute"},
		{"folder name is a path", func(c *Config) {
			c.Groups["family"] = Group{
				Peers:   []string{"laptop", "desktop", "phone"},
				Folders: map[string]GroupFolder{"a/b": {}},
			}
		}, "identifier"},
		{"mount override for unknown folder", func(c *Config) {
			c.Mounts["ghost"] = "/mnt/ghost"
		}, "unknown folder"},
		{"relative mount override", func(c *Config) { c.Mounts["photos"] = "pics" }, "absolute"},
		{"keepOffline for unknown folder", func(c *Config) {
			c.KeepOffline["ghost"] = false
		}, "unknown folder"},
		{"stream instead of keep", func(c *Config) {
			c.KeepOffline["documents"] = false
		}, ""},
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

func TestMountPrecedence(t *testing.T) {
	cfg := valid()
	cfg.DataDir = "/var/lib/storeboard"

	byName := map[string]struct{ group, mount string }{}
	for _, f := range cfg.Folders() {
		byName[f.Name] = struct{ group, mount string }{f.Group, f.Mount}
	}

	// Local override beats the group's default mount.
	if got := byName["documents"]; got.mount != "/mnt/docs" {
		t.Errorf("documents mount = %q, want the local override /mnt/docs", got.mount)
	}
	// The group default applies when the peer declares no override.
	if got := byName["photos"]; got.mount != "/home/kyle/Pictures" {
		t.Errorf("photos mount = %q, want the group default", got.mount)
	}
	// Nothing declared: the folder lands inside the one storage directory.
	cfg.Mounts = map[string]string{}
	if got := byName["documents"]; got.mount != "/mnt/docs" {
		t.Errorf("precondition broken: %q", got.mount)
	}
	cfg.Groups["family"] = Group{
		Peers:   []string{"laptop", "desktop", "phone"},
		Folders: map[string]GroupFolder{"documents": {}},
	}
	for _, f := range cfg.Folders() {
		if f.Name == "documents" {
			if want := cfg.DefaultMountDir("documents"); f.Mount != want {
				t.Errorf("undeclared mount = %q, want the storage-dir default %q", f.Mount, want)
			}
			if f.Group != "family" {
				t.Errorf("group = %q, want family", f.Group)
			}
		}
	}

	// KeepOffline: undeclared means keep a replica.
	for _, f := range cfg.Folders() {
		if !f.KeepOffline {
			t.Errorf("folder %q: keepOffline = false, want the keep default", f.Name)
		}
	}
	// Declared false streams the folder instead of replicating it.
	cfg.KeepOffline["documents"] = false
	for _, f := range cfg.Folders() {
		if f.Name == "documents" && f.KeepOffline {
			t.Error("documents: keepOffline = true, want the declared false")
		}
	}
}

func TestViewsAreSortedAndNonNil(t *testing.T) {
	cfg := valid()
	cfg.Peers["alpha"] = Peer{Endpoints: []string{}}
	cfg.Groups["aaa"] = Group{Peers: []string{"laptop", "desktop", "phone"}, Folders: map[string]GroupFolder{}}

	peers := cfg.PeerList()
	if len(peers) != 4 || peers[0].Name != "alpha" || peers[3].Name != "phone" {
		t.Errorf("peers not sorted: %#v", peers)
	}
	groups := cfg.GroupList()
	if len(groups) != 2 || groups[0].Name != "aaa" || groups[1].Name != "family" {
		t.Errorf("groups not sorted: %#v", groups)
	}
	folders := cfg.Folders()
	if len(folders) != 2 || folders[0].Name != "documents" || folders[1].Name != "photos" {
		t.Errorf("folders not sorted: %#v", folders)
	}

	empty := &Config{ConfigVersion: ConfigVersion, DataDir: "/d", Socket: "/s", Peers: map[string]Peer{}, Groups: map[string]Group{}, Mounts: map[string]string{}}
	if peers := empty.PeerList(); peers == nil {
		t.Error("Peers() nil on empty config")
	}
	if groups := empty.GroupList(); groups == nil {
		t.Error("Groups() nil on empty config")
	}
	if folders := empty.Folders(); folders == nil {
		t.Error("Folders() nil on empty config")
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
  "configVersion": 1,
  "peer": "laptop",
  "dataDir": "/var/lib/storeboard",
  "socket": "/run/storeboard.sock",
  "groups": {
    "family": {
      "peers": ["laptop"],
      "folders": {"photos": {}, "documents": {}},
      "permissions": {"group": "users", "mode": "2770"}
    }
  },
  "permissions": {"photos": {"user": "kyle", "mode": "0755"}}
}`)
	if _, err := Load(path); err == nil {
		t.Fatal("mode 2770: setgid is not a directory mode this config accepts")
	}

	path = writeConfig(t, `{
  "configVersion": 1,
  "peer": "laptop",
  "dataDir": "/var/lib/storeboard",
  "socket": "/run/storeboard.sock",
  "groups": {
    "family": {
      "peers": ["laptop"],
      "folders": {"photos": {}, "documents": {}},
      "permissions": {"group": "users", "mode": "0770"}
    }
  },
  "permissions": {"photos": {"user": "kyle"}}
}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.PermissionsFor("photos", "family")
	if got == nil || got.User != "kyle" || got.Group != "users" || got.Mode != "0770" {
		t.Fatalf("photos permissions = %+v, want kyle/users/0770 (local field over group default)", got)
	}
	if got := cfg.PermissionsFor("documents", "family"); got == nil || got.User != "" || got.Mode != "0770" {
		t.Fatalf("documents permissions = %+v, want the group's default alone", got)
	}

	cases := []struct {
		name, body, want string
	}{
		{"unknown folder", `"nope": {"user": "kyle"}`, "unknown folder"},
		{"bad mode", `"photos": {"mode": "99"}`, "mode"},
		{"colon list", `"photos": {"user": "kyle:root"}`, "colon list"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := writeConfig(t, `{
  "configVersion": 1,
  "peer": "laptop",
  "dataDir": "/var/lib/storeboard",
  "socket": "/run/storeboard.sock",
  "groups": {"family": {"peers": ["laptop"], "folders": {"photos": {}}}},
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

func TestNestedMountsWarnAndDuplicatesFail(t *testing.T) {
	base := `{
  "configVersion": 1,
  "peer": "laptop",
  "dataDir": "/var/lib/storeboard",
  "socket": "/run/storeboard.sock",
  "groups": {"family": {"peers": ["laptop"], "folders": {"docs": {}, "pool": {}, "other": {}}}},
  "mounts": %s
}`
	path := writeConfig(t, fmt.Sprintf(base, `{"docs": "/mainpool/kyle/docs", "pool": "/mainpool", "other": "/srv/other"}`))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("nested mounts are a deliberate tradeoff, not an error: %v", err)
	}
	warns := cfg.Warnings()
	if len(warns) != 1 || !strings.Contains(warns[0], "nested") && !strings.Contains(warns[0], "contains") {
		t.Fatalf("warnings = %v, want one about the nesting", warns)
	}

	path = writeConfig(t, fmt.Sprintf(base, `{"docs": "/srv/docs", "pool": "/srv/docs", "other": "/srv/other"}`))
	_, err = Load(path)
	if err == nil || !strings.Contains(err.Error(), "both mount") {
		t.Fatalf("err = %v, want two folders on one path rejected", err)
	}

	// Siblings that merely share a prefix are not nesting.
	path = writeConfig(t, fmt.Sprintf(base, `{"docs": "/srv/docs", "pool": "/srv/docs-archive", "other": "/srv/other"}`))
	sib, err := Load(path)
	if err != nil {
		t.Fatalf("sibling mounts rejected: %v", err)
	}
	if w := sib.Warnings(); len(w) != 0 {
		t.Fatalf("siblings warned: %v", w)
	}
}
