package syncer

import (
	"os"
	"slices"
	"sync"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/gatehouse"
)

// Friends' devices (DESIGN invariant 12): people GateHouse knows, syncing
// from devices that are not declared peers. Such a device sees a store only
// while its person may read it (storeboard.read, or write), and its changes
// land only where they may write (storeboard.write). It never holds a store
// for this machine (streamed reads, liveness) and never runs the acting
// verbs. Every frame from one passes friendFrame first; the answers are
// GateHouse's, asked of the local agent and kept for friendTTL, so a grant
// taken back stops the next frames, not the next connection.

// friendTTL is how long a GateHouse answer is kept;
// STOREBOARD_FRIEND_TTL overrides it (tests).
var friendTTL = 30 * time.Second

func init() {
	if v := os.Getenv("STOREBOARD_FRIEND_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			friendTTL = d
		}
	}
}

type friendKey struct{ person, perm, store string }

type friendAnswer struct {
	allowed bool
	at      time.Time
}

type friendState struct {
	mu sync.Mutex
	// people names the person behind each connected friend's device.
	people  map[string]string
	answers map[friendKey]friendAnswer
}

// SetFriends tells the engine who is behind a device (the transport knows,
// from its sign-in) and how to ask GateHouse what a person may do. Call
// before Run.
func (e *Engine) SetFriends(person func(peer string) string, may func(person, perm, store string) bool) {
	e.friendPerson = person
	e.friendMay = may
}

// friendOf names the person behind peer, or "" for a declared peer.
func (e *Engine) friendOf(peer string) string {
	e.friends.mu.Lock()
	defer e.friends.mu.Unlock()
	return e.friends.people[peer]
}

// noteFriend records who is behind a newly connected peer.
func (e *Engine) noteFriend(peer string) {
	person := ""
	if e.friendPerson != nil && !slices.Contains(e.declaredPeers(), peer) {
		person = e.friendPerson(peer)
	}
	e.friends.mu.Lock()
	defer e.friends.mu.Unlock()
	if e.friends.people == nil {
		e.friends.people = map[string]string{}
	}
	if person == "" {
		delete(e.friends.people, peer)
	} else {
		e.friends.people[peer] = person
	}
}

func (e *Engine) declaredPeers() []string {
	out := make([]string, 0, len(e.cfg.Peers))
	for p := range e.cfg.Peers {
		out = append(out, p)
	}
	return out
}

// may asks (or remembers) whether person may do perm to store.
func (e *Engine) may(person, perm, store string) bool {
	if e.friendMay == nil {
		return false
	}
	k := friendKey{person, perm, store}
	e.friends.mu.Lock()
	a, ok := e.friends.answers[k]
	e.friends.mu.Unlock()
	if ok && time.Since(a.at) < friendTTL {
		return a.allowed
	}
	allowed := e.friendMay(person, perm, store)
	e.friends.mu.Lock()
	if e.friends.answers == nil {
		e.friends.answers = map[friendKey]friendAnswer{}
	}
	e.friends.answers[k] = friendAnswer{allowed, time.Now()}
	e.friends.mu.Unlock()
	return allowed
}

// friendReads reports whether peer is a friend's device that may receive
// store; writing implies reading.
func (e *Engine) friendReads(peer, store string) bool {
	person := e.friendOf(peer)
	return person != "" && (e.may(person, gatehouse.Read, store) || e.may(person, gatehouse.Write, store))
}

// friendFrame decides whether a frame from a friend's device is let in.
func (e *Engine) friendFrame(person string, fr *frame) bool {
	switch fr.Type {
	case "readData", "readErr", "verbsResult", "readCancel", "blobCredit":
		// Replies to this machine's own requests, and flow control.
		return true
	case "verbs":
		return false
	case "index", "rename", "blob", "blobEnd":
		return e.may(person, gatehouse.Write, fr.Store)
	default: // digest, pull, want, wantErr, read
		return e.may(person, gatehouse.Read, fr.Store) || e.may(person, gatehouse.Write, fr.Store)
	}
}
