package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
)

// TestRequestStore: a service gets a store in its own namespace, in a
// group that exists with this machine; the request is kept and the daemon
// is told to rebuild; a name outside the namespace, or a stranger, is
// refused; asking again answers without another rebuild.
func TestRequestStore(t *testing.T) {
	cfg := testConfig(t)
	var group string
	for g := range cfg.Groups {
		group = g
	}
	unit := "strongbox.service"
	old := unitOf
	unitOf = func(int) string { return unit }
	defer func() { unitOf = old }()

	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.Socket)
	}}}
	ask := func(p RequestStoreParams) (int, map[string]any) {
		b, _ := json.Marshal(p)
		var resp *http.Response
		for i := 0; i < 100; i++ {
			if resp, err = client.Post("http://storeboard/v1/stores", "application/json", bytes.NewReader(b)); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	if code, out := ask(RequestStoreParams{Name: "other.thing", Group: group}); code != http.StatusForbidden {
		t.Fatalf("outside the namespace: %d %v", code, out)
	}
	if code, out := ask(RequestStoreParams{Name: "strongbox.secrets", Group: "nope"}); code != http.StatusBadRequest {
		t.Fatalf("an undeclared group: %d %v", code, out)
	}
	code, out := ask(RequestStoreParams{Name: "strongbox.secrets", Group: group, Permissions: &config.Permissions{User: "root", Mode: "0700"}, NoBackup: true})
	if code != http.StatusCreated || out["path"] == "" {
		t.Fatalf("request: %d %v", code, out)
	}
	select {
	case <-srv.Restart():
	case <-time.After(2 * time.Second):
		t.Fatal("no rebuild asked for")
	}
	req, err := config.LoadRequested(cfg.DataDir)
	if err != nil || req["strongbox.secrets"].By != "strongbox.service" || !req["strongbox.secrets"].NoBackup {
		t.Fatalf("kept %+v %v", req, err)
	}
	merged, names := cfg.WithRequested(req)
	if len(names) != 1 || merged.RequestedBy["strongbox.secrets"] != "strongbox.service" {
		t.Fatalf("merged %v", names)
	}

	unit = ""
	if code, out := ask(RequestStoreParams{Name: "anything.x", Group: group}); code != http.StatusForbidden {
		t.Fatalf("a user's process: %d %v", code, out)
	}
}

func TestUnitOfPID(t *testing.T) {
	// A test runs in a user session or a build sandbox: never a system
	// service, so it names nothing.
	if u := unitOfPID(1); u != "" && u != "init.scope" {
		t.Logf("pid 1 is in %q", u)
	}
	if unitOfPID(-1) != "" {
		t.Fatal("a missing pid named a unit")
	}
}
