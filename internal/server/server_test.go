package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/version"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	dir := t.TempDir()
	return &config.Config{
		ConfigVersion: config.ConfigVersion,
		Root:          filepath.Join(dir, "Storeboard"),
		DataDir:       filepath.Join(dir, "data"),
		Socket:        filepath.Join(dir, "storeboard.sock"),
		Peer:          "laptop",
		Peers: map[string]config.Peer{
			"laptop":  {Endpoints: []string{"192.168.1.10:4242"}},
			"desktop": {Endpoints: []string{"192.168.1.11:4242"}},
		},
		Groups: map[string]config.Group{
			"family": {Peers: []string{"laptop", "desktop"}},
		},
		StoreTree:   map[string]config.Store{"photos": {Group: "family"}},
		Permissions: map[string]config.Permissions{},
	}
}

// bindStaleSocket leaves a bound-then-closed AF_UNIX socket file behind,
// exactly what a crashed daemon leaves in the filesystem.
func bindStaleSocket(t *testing.T, path string) {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
		syscall.Close(fd)
		t.Fatal(err)
	}
	syscall.Close(fd)
}

func TestNewTakesOverAStaleSocket(t *testing.T) {
	cfg := testConfig(t)
	bindStaleSocket(t, cfg.Socket)

	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("stale socket not taken over: %v", err)
	}
	defer srv.Close()
}

func TestNewRefusesALiveSocket(t *testing.T) {
	cfg := testConfig(t)
	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	if _, err := New(cfg, nil); err == nil || !strings.Contains(err.Error(), "already listening") {
		t.Fatalf("second New on a live socket: %v", err)
	}
}

func TestNewRefusesANonSocketFile(t *testing.T) {
	cfg := testConfig(t)
	if err := os.WriteFile(cfg.Socket, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := New(cfg, nil); err == nil || !strings.Contains(err.Error(), "not a socket") {
		t.Fatalf("regular file at socket path: %v", err)
	}
}

func TestStatusDocument(t *testing.T) {
	cfg := testConfig(t)
	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/status = %s", resp.Status)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var st api.Status
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.APIVersion != api.Version {
		t.Errorf("apiVersion = %d, want %d", st.APIVersion, api.Version)
	}
	if st.Version != version.Version {
		t.Errorf("version = %q, want %q", st.Version, version.Version)
	}
	if st.Socket != cfg.Socket || st.DataDir != cfg.DataDir {
		t.Errorf("document does not echo the config: %+v", st)
	}
	if st.Peer != "laptop" {
		t.Errorf("peer = %q, want laptop", st.Peer)
	}
	if len(st.Peers) != 2 {
		t.Errorf("peers = %+v, want two", st.Peers)
	}
	if len(st.Groups) != 1 || st.Groups[0].Name != "family" ||
		len(st.Groups[0].Stores) != 1 || st.Groups[0].Stores[0] != "photos" {
		t.Errorf("groups = %+v, want family owning photos", st.Groups)
	}
	if len(st.Stores) != 1 || st.Stores[0].Name != "photos" ||
		st.Stores[0].Group != "family" || st.Stores[0].Sync != config.SyncLocal {
		t.Errorf("stores = %+v, want photos in family, local", st.Stores)
	}
	if want := filepath.Join(cfg.Root, "photos"); st.Stores[0].Root != want {
		t.Errorf("photos root = %q, want %q", st.Stores[0].Root, want)
	}
	// The transport manager reports the declared other peer even before the
	// wire is live (it is dialing or backing off).
	if len(st.Connections) != 1 || st.Connections[0].Peer != "desktop" {
		t.Errorf("connections = %+v, want one entry for desktop", st.Connections)
	}
}

func TestStatusDocumentStoresAlwaysAnArray(t *testing.T) {
	cfg := testConfig(t)
	cfg.Groups = map[string]config.Group{}
	cfg.StoreTree = map[string]config.Store{}
	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	rec := httptest.NewRecorder()
	srv.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	if !strings.Contains(rec.Body.String(), `"stores":[]`) {
		t.Fatalf("stores missing or null: %s", rec.Body.String())
	}
}

func TestUnknownEndpointAndMethod(t *testing.T) {
	cfg := testConfig(t)
	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	h := srv.handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown endpoint = %d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("404 Content-Type = %q, want JSON", ct)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/status", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /v1/status = %d, want 405", rec.Code)
	}
}

func TestRunServesUntilCancelledAndCleansUp(t *testing.T) {
	cfg := testConfig(t)
	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	// Wait for the socket to answer, then read the document over it — the
	// real transport, not the handler in isolation.
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err := net.Dial("unix", cfg.Socket)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket never came up: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", cfg.Socket)
		},
	}}
	resp, err := client.Get("http://storeboard/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	var st api.Status
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if st.APIVersion != api.Version {
		t.Fatalf("apiVersion over the socket = %d, want %d", st.APIVersion, api.Version)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(cfg.Socket); !os.IsNotExist(err) {
		t.Fatalf("socket file still present after close")
	}
}

// The takeover verb acts on the user's files, so it is authorized per
// caller: over the real unix socket the daemon knows who is asking, and the
// folder's owner (here: the test process) is allowed through.
func TestTakeoverOverUnixSocket(t *testing.T) {
	cfg := testConfig(t)
	busy := filepath.Join(t.TempDir(), "remote-docs")
	if err := os.MkdirAll(busy, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(busy, "mine.txt"), []byte("local"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg.StoreTree["docs"] = config.Store{Group: "family", Sync: config.SyncRemote, Symlinks: []string{busy}}

	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", cfg.Socket)
			},
		},
	}

	// Wait for the engine to prepare the folders (it reports busy once it has).
	waitBusy := func() bool {
		resp, err := client.Get("http://storeboard/v1/status")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var st api.Status
		if json.NewDecoder(resp.Body).Decode(&st) != nil {
			return false
		}
		for _, s := range st.Sync {
			if s.Store != "docs" {
				continue
			}
			for _, l := range s.Symlinks {
				if l.Path == busy {
					return l.State == api.SymlinkBusy && l.Busy == api.BusyContent
				}
			}
		}
		return false
	}
	deadline := time.Now().Add(5 * time.Second)
	for !waitBusy() {
		if time.Now().After(deadline) {
			t.Fatal("folder never reported busy")
		}
		time.Sleep(20 * time.Millisecond)
	}

	resp, err := client.Post("http://storeboard/v1/stores/docs/takeover", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST takeover = %s", resp.Status)
	}
	var res api.TakeoverResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatal(err)
	}
	if res.Moved != 1 || res.State != "detached" {
		t.Fatalf("takeover = %+v, want one entry moved and a remote store detached", res)
	}
	if fi, err := os.Lstat(busy); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("declared path is not a symlink after takeover: %v %v", fi, err)
	}
	if b, err := os.ReadFile(filepath.Join(busy, "mine.txt")); err != nil || string(b) != "local" {
		t.Fatalf("content not readable through the link: %q %v", b, err)
	}
	if b, err := os.ReadFile(filepath.Join(cfg.Root, "docs", "mine.txt")); err != nil || string(b) != "local" {
		t.Fatalf("content not in the canonical dir: %q %v", b, err)
	}
}

// Without peer credentials the daemon refuses the acting verb outright: it
// cannot tell root from a stranger, so it does not guess.
func TestTakeoverNeedsPeerCreds(t *testing.T) {
	cfg := testConfig(t)
	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	ts := httptest.NewServer(srv.handler())
	defer ts.Close()
	resp, err := http.Post(ts.URL+"/v1/stores/photos/takeover", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("takeover without credentials = %s, want 403", resp.Status)
	}
}

// The send verb over the real socket: a file the caller names lands in the
// folder as an ordinary local change, a taken name parks the old file
// instead of losing it, and the answer says where it went.
func TestSendOverUnixSocket(t *testing.T) {
	cfg := testConfig(t)
	root := filepath.Join(cfg.Root, "photos")

	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", cfg.Socket)
			},
		},
	}

	// No readiness wait: folders are prepared before the server serves, and
	// a send is an ordinary write into the mount.
	send := func(source, path string) api.SendResult {
		t.Helper()
		body, err := json.Marshal(map[string]string{"source": source, "path": path})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Post("http://storeboard/v1/stores/photos/send", "application/json", strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST send = %s", resp.Status)
		}
		var res api.SendResult
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			t.Fatal(err)
		}
		return res
	}

	first := filepath.Join(t.TempDir(), "holiday.txt")
	if err := os.WriteFile(first, []byte("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	res := send(first, "shared/holiday.txt")
	if res.Path != "shared/holiday.txt" || res.Parked != "" || res.Size != 5 {
		t.Fatalf("send = %+v", res)
	}
	if b, err := os.ReadFile(filepath.Join(root, "shared", "holiday.txt")); err != nil || string(b) != "first" {
		t.Fatalf("sent content = %q, %v", b, err)
	}

	// A second send onto the same name keeps both.
	second := filepath.Join(t.TempDir(), "holiday2.txt")
	if err := os.WriteFile(second, []byte("second"), 0o644); err != nil {
		t.Fatal(err)
	}
	res = send(second, "shared/holiday.txt")
	if res.Parked == "" {
		t.Fatalf("second send = %+v, want the first file parked", res)
	}
	if b, err := os.ReadFile(filepath.Join(root, "shared", "holiday.txt")); err != nil || string(b) != "second" {
		t.Fatalf("destination = %q, %v; want the new content", b, err)
	}
	if b, err := os.ReadFile(filepath.Join(root, res.Parked)); err != nil || string(b) != "first" {
		t.Fatalf("parked = %q, %v; want the old content kept", b, err)
	}
}
