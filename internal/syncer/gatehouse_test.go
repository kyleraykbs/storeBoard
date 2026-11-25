package syncer

import (
	"strings"
	"testing"

	"codeberg.org/kyleraykbs/storeboard/internal/gatehouse"
)

// A person GateHouse grants a permission on a store may use the matching
// acting verbs on it, beyond root and the store's owner; nothing more.
func TestGatehouseGrantsBeyondTheOwner(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	const lain = 4242 // not the owner, not root
	if err := n.eng.CheckOwner("docs", lain, gatehouse.Read, "list the versions of"); err == nil || !strings.Contains(err.Error(), "storeboard.read@docs") {
		t.Fatalf("without a grant: %v", err)
	}
	n.eng.grants = func(uid int, perm, store string) bool {
		return uid == lain && perm == gatehouse.Read && store == "docs"
	}
	if err := n.eng.CheckOwner("docs", lain, gatehouse.Read, "list the versions of"); err != nil {
		t.Fatalf("granted read: %v", err)
	}
	if err := n.eng.CheckOwner("docs", lain, gatehouse.Manage, "cleanup"); err == nil {
		t.Fatal("read let lain manage")
	}
	if err := n.eng.CheckOwner("docs", 4343, gatehouse.Read, "list the versions of"); err == nil {
		t.Fatal("someone else got lain's grant")
	}
}
