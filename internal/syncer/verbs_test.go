package syncer

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

// A peer in the store's group may ask a holder to restore into the store, but
// never to write a path of its choosing on the holder's disk: `to` is refused
// before anything is read or written.
func TestVerbRestoreRefusesAForwardedTo(t *testing.T) {
	n, _ := restoreFixture(t)
	defer n.eng.Close()
	rec := &recordingSender{}
	n.eng.mu.Lock()
	n.eng.sends["a"] = rec
	n.eng.mu.Unlock()

	target := filepath.Join(t.TempDir(), "planted.txt")
	payload, err := json.Marshal(verbsRequest{Action: "restore", Path: "note.txt", To: target})
	if err != nil {
		t.Fatal(err)
	}
	n.eng.handleVerbs("a", &frame{Type: "verbs", ID: 7, Store: "docs", Action: "restore", Data: payload})

	if exists(target) {
		t.Fatal("a forwarded restore wrote outside the store")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.control) != 1 {
		t.Fatalf("replies = %d, want exactly one", len(rec.control))
	}
	var reply frame
	if err := json.Unmarshal([]byte(strings.TrimSpace(rec.control[0])), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Type != "verbsResult" || reply.ID != 7 || !strings.Contains(reply.Error, "not forwarded") {
		t.Fatalf("reply = %+v, want a refusal", reply)
	}
}
