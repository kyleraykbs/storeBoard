package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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
		"folders": [{"name": "photos", "path": "/home/kyle/Pictures"}]
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
	if len(report.Folders) != 1 || report.Folders[0] != "photos" {
		t.Errorf("folders = %v, want [photos]", report.Folders)
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
		Folders:       []api.Folder{{Name: "photos", Path: "/home/kyle/Pictures"}},
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
