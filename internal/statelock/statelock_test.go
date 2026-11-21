package statelock

import (
	"strings"
	"testing"
)

// flock is per open file description, so a second Acquire in the same process
// conflicts exactly like a second daemon would.
func TestSecondAcquireIsRefusedUntilReleased(t *testing.T) {
	dir := t.TempDir()
	first, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(dir); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("second Acquire = %v, want a refusal", err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	again, err := Acquire(dir)
	if err != nil {
		t.Fatalf("Acquire after Release: %v", err)
	}
	again.Release()
}
