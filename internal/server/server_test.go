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
		DataDir:       filepath.Join(dir, "data"),
		Socket:        filepath.Join(dir, "storeboard.sock"),
		Peer:          "laptop",
		Peers: map[string]config.Peer{
			"laptop":  {Endpoints: []string{"192.168.1.10:4242"}},
			"desktop": {Endpoints: []string{"192.168.1.11:4242"}},
		},
		Groups: map[string]config.Group{
			"family": {
				Peers: []string{"laptop", "desktop"},
				Folders: map[string]config.GroupFolder{
					"photos": {Mount: "/home/kyle/Pictures"},
				},
			},
		},
		Mounts: map[string]string{},
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
		len(st.Groups[0].Folders) != 1 || st.Groups[0].Folders[0] != "photos" {
		t.Errorf("groups = %+v, want family owning photos", st.Groups)
	}
	if len(st.Folders) != 1 || st.Folders[0].Name != "photos" ||
		st.Folders[0].Group != "family" || st.Folders[0].Mount != "/home/kyle/Pictures" {
		t.Errorf("folders = %+v, want photos in family at its group mount", st.Folders)
	}
}

func TestStatusDocumentFoldersAlwaysAnArray(t *testing.T) {
	cfg := testConfig(t)
	cfg.Groups = map[string]config.Group{}
	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	rec := httptest.NewRecorder()
	srv.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	if !strings.Contains(rec.Body.String(), `"folders":[]`) {
		t.Fatalf("folders missing or null: %s", rec.Body.String())
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
