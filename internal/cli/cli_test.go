package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/server"
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

func TestVersionJSON(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := Run(context.Background(), []string{"version", "--json"}, "", &out, &errOut); err != nil {
		t.Fatal(err)
	}

	var v struct {
		Name       string `json:"name"`
		APIVersion int    `json:"apiVersion"`
	}
	if err := json.Unmarshal(out.Bytes(), &v); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out.String())
	}
	if v.Name != "storeboard" || v.APIVersion != api.Version {
		t.Errorf("version document = %+v", v)
	}
}

func TestVersionHuman(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := Run(context.Background(), []string{"version"}, "", &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "storeboard ") || !strings.Contains(out.String(), "api v") {
		t.Errorf("human version output: %q", out.String())
	}
}

func TestConfigCheckValidFile(t *testing.T) {
	path := writeConfig(t, `{
		"configVersion": 1,
		"peer": "laptop",
		"peers": { "laptop": { "endpoints": ["127.0.0.1:4242"] } },
		"groups": {
			"family": {
				"peers": ["laptop"],
				"folders": { "photos": { "mount": "/home/kyle/Pictures" } }
			}
		},
		"mounts": {}
	}`)

	var out, errOut bytes.Buffer
	if err := Run(context.Background(), []string{"config", "check", "--json"}, path, &out, &errOut); err != nil {
		t.Fatal(err)
	}

	var report checkReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out.String())
	}
	if !report.Valid {
		t.Fatalf("valid config reported invalid: %+v", report)
	}
	if report.Path != path {
		t.Errorf("path = %q, want %q", report.Path, path)
	}
	if report.Peer != "laptop" {
		t.Errorf("peer = %q, want laptop", report.Peer)
	}
	if len(report.Peers) != 1 || report.Peers[0] != "laptop" {
		t.Errorf("peers = %v, want [laptop]", report.Peers)
	}
	if len(report.Groups) != 1 || report.Groups[0] != "family" {
		t.Errorf("groups = %v, want [family]", report.Groups)
	}
	if len(report.Folders) != 1 || report.Folders[0].Name != "photos" || report.Folders[0].Group != "family" {
		t.Errorf("folders = %+v, want photos in family", report.Folders)
	}
}

func TestConfigCheckValidFileHuman(t *testing.T) {
	path := writeConfig(t, `{}`)

	var out, errOut bytes.Buffer
	if err := Run(context.Background(), []string{"config", "check"}, path, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "config OK: "+path) {
		t.Errorf("human output: %q", out.String())
	}
}

func TestConfigCheckRejectsTypo(t *testing.T) {
	path := writeConfig(t, `{"configVersion": 1, "folars": []}`)

	var out, errOut bytes.Buffer
	err := Run(context.Background(), []string{"config", "check", "--json"}, path, &out, &errOut)
	if !errors.Is(err, ErrHandled) {
		t.Fatalf("err = %v, want ErrHandled", err)
	}

	var report checkReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out.String())
	}
	if report.Valid || !strings.Contains(report.Error, "folars") {
		t.Errorf("report = %+v, want invalid naming the bad key", report)
	}
}

func TestConfigCheckMissingFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.json")

	var out, errOut bytes.Buffer
	err := Run(context.Background(), []string{"config", "check", "--json"}, missing, &out, &errOut)
	if !errors.Is(err, ErrHandled) {
		t.Fatalf("err = %v, want ErrHandled", err)
	}

	var report checkReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out.String())
	}
	if report.Valid || !strings.Contains(report.Error, "no config file") {
		t.Errorf("report = %+v, want the missing-file failure", report)
	}
}

func TestUnknownCommand(t *testing.T) {
	var out, errOut bytes.Buffer
	err := Run(context.Background(), []string{"frobnicate"}, "", &out, &errOut)
	if !errors.Is(err, ErrHandled) {
		t.Fatalf("err = %v, want ErrHandled", err)
	}
	if !strings.Contains(errOut.String(), "unknown command") {
		t.Errorf("stderr = %q, want the unknown-command message", errOut.String())
	}
}

// TestStatusRoundTrip drives the real daemon over its real socket through
// both output modes — the whole client path the smoke test covers manually.
func TestStatusRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		ConfigVersion: config.ConfigVersion,
		DataDir:       filepath.Join(dir, "data"),
		Socket:        filepath.Join(dir, "storeboard.sock"),
		Peer:          "laptop",
		Peers:         map[string]config.Peer{"laptop": {Endpoints: []string{"127.0.0.1:4242"}}},
		Groups: map[string]config.Group{
			"family": {
				Peers:   []string{"laptop"},
				Folders: map[string]config.GroupFolder{"photos": {Mount: "/home/kyle/Pictures"}},
			},
		},
		Mounts: map[string]string{},
	}
	srv, err := server.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	// The CLI reads its socket from a config file, like any other caller.
	cfgPath := filepath.Join(dir, "config.json")
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	waitForSocket(t, cfg.Socket)

	var out, errOut bytes.Buffer
	if err := Run(ctx, []string{"status", "--json"}, cfgPath, &out, &errOut); err != nil {
		t.Fatalf("status --json: %v (%s)", err, errOut.String())
	}
	var st api.Status
	if err := json.Unmarshal(out.Bytes(), &st); err != nil {
		t.Fatalf("status is not the daemon's document: %v\n%s", err, out.String())
	}
	if st.APIVersion != api.Version || len(st.Folders) != 1 || st.Folders[0].Name != "photos" {
		t.Errorf("status = %+v", st)
	}

	out.Reset()
	if err := Run(ctx, []string{"status"}, cfgPath, &out, &errOut); err != nil {
		t.Fatalf("status: %v (%s)", err, errOut.String())
	}
	if !strings.Contains(out.String(), "photos") || !strings.Contains(out.String(), "api v1") {
		t.Errorf("human status output: %q", out.String())
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("daemon Run: %v", err)
	}
}

func waitForSocket(t *testing.T, socket string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Lstat(socket); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon socket never appeared: %s", socket)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFolderTakeoverUsage(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"folder"}, "usage: storeboard folder takeover <name>"},
		{[]string{"folder", "takeover"}, "usage: storeboard folder takeover <name>"},
		{[]string{"folder", "surrender", "docs"}, "unknown folder command"},
	}
	for _, c := range cases {
		var out, errOut bytes.Buffer
		err := Run(context.Background(), c.args, "", &out, &errOut)
		if err != ErrHandled {
			t.Errorf("%v: err = %v, want ErrHandled", c.args, err)
		}
		if !strings.Contains(errOut.String(), c.want) {
			t.Errorf("%v: stderr = %q, want it to mention %q", c.args, errOut.String(), c.want)
		}
	}
}

// The daemon's own refusal is what the user reads: a takeover that cannot
// happen must report why, not "daemon answered 409".
func TestFolderTakeoverReportsTheDaemonError(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "s.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"folder \"docs\" belongs to uid 1000: run this as that user, or as root"}`))
	})}
	go srv.Serve(ln)
	defer srv.Close()

	path := writeConfig(t, `{"configVersion": 1, "socket": "`+socket+`", "dataDir": "`+t.TempDir()+`"}`)
	var out, errOut bytes.Buffer
	err = Run(context.Background(), []string{"folder", "takeover", "docs"}, path, &out, &errOut)
	if err == nil || !strings.Contains(err.Error(), "belongs to uid 1000") {
		t.Fatalf("err = %v, want the daemon's own reason", err)
	}
}

// backup list renders a file's recorded versions; backup restore posts the
// version asked for and reports what the daemon did.
func TestBackupCommands(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "s.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var gotBody []byte
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/versions"):
			_, _ = w.Write([]byte(`{"folder":"docs","path":"note.txt","keep":"8760h0m0s","versions":[
				{"path":"note.txt","time":"2026-10-09T12:00:00Z","blob":"abc123def4567890","size":11},
				{"path":"note.txt","time":"2026-10-08T12:00:00Z","deleted":true,"size":0}]}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/restore"):
			gotBody, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte(`{"folder":"docs","path":"note.txt","blob":"abc123def4567890","at":"2026-10-08T12:00:00Z"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"no such endpoint"}`))
		}
	})}
	go srv.Serve(ln)
	defer srv.Close()

	path := writeConfig(t, `{"configVersion": 1, "socket": "`+socket+`", "dataDir": "`+t.TempDir()+`"}`)

	var out, errOut bytes.Buffer
	if err := Run(context.Background(), []string{"backup", "list", "docs", "note.txt"}, path, &out, &errOut); err != nil {
		t.Fatalf("backup list: %v (%s)", err, errOut.String())
	}
	for _, want := range []string{"2 recorded versions", "2026-10-09T12:00:00Z", "deleted", "abc123def456"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("backup list output missing %q:\n%s", want, out.String())
		}
	}

	out.Reset()
	if err := Run(context.Background(), []string{"backup", "restore", "docs", "note.txt", "--at", "2026-10-08T12:00:00Z"}, path, &out, &errOut); err != nil {
		t.Fatalf("backup restore: %v (%s)", err, errOut.String())
	}
	if !strings.Contains(string(gotBody), `"path":"note.txt"`) || !strings.Contains(string(gotBody), `"at":"2026-10-08T12:00:00Z"`) {
		t.Fatalf("restore body = %s", gotBody)
	}
	if !strings.Contains(out.String(), "restored note.txt") {
		t.Fatalf("restore output = %s", out.String())
	}

	// Usage: a missing argument and an unknown verb are errors, not panics.
	for _, args := range [][]string{{"backup"}, {"backup", "list"}, {"backup", "restore", "docs"}, {"backup", "revert"}} {
		var o, e bytes.Buffer
		if err := Run(context.Background(), args, path, &o, &e); err != ErrHandled {
			t.Errorf("%v: err = %v, want ErrHandled", args, err)
		}
	}
}
