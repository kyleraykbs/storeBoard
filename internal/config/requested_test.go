package config

import (
	"testing"
	"time"
)

func TestWithRequested(t *testing.T) {
	c := &Config{
		Peer:      "a",
		Groups:    map[string]Group{"g": {Peers: []string{"a", "b"}}, "elsewhere": {Peers: []string{"b"}}},
		StoreTree: map[string]Store{"docs": {Group: "g"}, "strongbox.pinned": {Group: "g", Sync: SyncStream}},
		Backup:    &Backup{Enable: true},
	}
	req := map[string]Requested{
		"strongbox.secrets": {Group: "g", Permissions: &Permissions{User: "root", Mode: "0700"}, NoBackup: true, By: "strongbox.service", At: time.Now()},
		"strongbox.pinned":  {Group: "g", Sync: SyncLocal, By: "strongbox.service"},
		"other.notmine":     {Group: "elsewhere", By: "other.service"},
		"other.nogroup":     {Group: "missing", By: "other.service"},
	}
	m, merged := c.WithRequested(req)
	if len(merged) != 1 || merged[0] != "strongbox.secrets" {
		t.Fatalf("merged %v", merged)
	}
	if s := m.StoreTree["strongbox.secrets"]; s.Group != "g" || s.Permissions == nil || s.Permissions.Mode != "0700" {
		t.Fatalf("requested store %+v", s)
	}
	if m.StoreTree["strongbox.pinned"].Sync != SyncStream {
		t.Fatal("a request overrode a declaration")
	}
	if on, ok := m.Backup.Stores["strongbox.secrets"]; !ok || on {
		t.Fatal("noBackup not applied")
	}
	if m.RequestedBy["strongbox.secrets"] != "strongbox.service" {
		t.Fatalf("provenance %v", m.RequestedBy)
	}
	if _, ok := c.StoreTree["strongbox.secrets"]; ok || c.Backup.Stores != nil {
		t.Fatal("the original config was modified")
	}

	dir := t.TempDir()
	if err := SaveRequested(dir, req); err != nil {
		t.Fatal(err)
	}
	back, err := LoadRequested(dir)
	if err != nil || len(back) != 4 || back["strongbox.secrets"].By != "strongbox.service" {
		t.Fatalf("round trip: %v %v", back, err)
	}
}
