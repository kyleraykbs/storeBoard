package transport

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// api_Connection is the status-document connection type the tests inspect.
type api_Connection = api.Connection

// freeEndpoint returns a loopback address that is free right now; the test
// binds it immediately after, so the window is tiny and standard.
func freeEndpoint(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func managerFor(t *testing.T, self, other, selfName, otherName string) *Manager {
	t.Helper()
	return New(&config.Config{
		ConfigVersion: config.ConfigVersion,
		DataDir:       "/var/lib/storeboard",
		Socket:        "/no-socket",
		Peer:          selfName,
		Peers: map[string]config.Peer{
			selfName:  {Endpoints: []string{self}},
			otherName: {Endpoints: []string{other}},
		},
	}, nil)
}

func stateOf(t *testing.T, m *Manager, peer string) api_Connection {
	t.Helper()
	for _, c := range m.Snapshot() {
		if c.Peer == peer {
			return c
		}
	}
	t.Fatalf("no connection entry for %q", peer)
	return api_Connection{}
}

// waitFor polls until fn holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition never held")
}

func TestHandshakeValidation(t *testing.T) {
	cases := []struct {
		name   string
		reply  string
		wantEr string
	}{
		{"bad protocol", `{"protocol": 99, "name": "b"}`, "wire protocol"},
		{"empty name", `{"protocol": 1, "name": ""}`, "empty peer name"},
		{"claims to be me", `{"protocol": 1, "name": "self"}`, "claims to be me"},
		{"undeclared peer", `{"protocol": 1, "name": "stranger"}`, "undeclared peer"},
		{"valid", `{"protocol": 1, "name": "b", "version": "9.9"}`, ""},
	}

	m := New(&config.Config{
		ConfigVersion: config.ConfigVersion,
		Peer:          "self",
		Peers:         map[string]config.Peer{"self": {}, "b": {}},
	}, nil)

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c1, c2 := net.Pipe()
			defer c1.Close()
			defer c2.Close()

			// The pipe is synchronous: consume our hello, then answer.
			go func() {
				bufio.NewReader(c2).ReadString('\n')
				c2.Write([]byte(tt.reply + "\n"))
			}()

			_, _, err := m.handshake(c1)
			if tt.wantEr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantEr) {
				t.Fatalf("error = %v, want it to mention %q", err, tt.wantEr)
			}
		})
	}
}

func TestPeersConnectAcrossSimultaneousDials(t *testing.T) {
	epA := freeEndpoint(t)
	epB := freeEndpoint(t)

	a := managerFor(t, epA, epB, "a", "b")
	b := managerFor(t, epB, epA, "b", "a")
	t.Cleanup(func() { a.Close() })
	t.Cleanup(func() { b.Close() })

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(cancelA)
	t.Cleanup(cancelB)
	go a.Run(ctxA)
	go b.Run(ctxB)

	// Both managers dial the other at once; the cross-dial race must end
	// with exactly one live link per side, both marked connected.
	waitFor(t, 3*time.Second, func() bool {
		return stateOf(t, a, "b").State == stateConnected &&
			stateOf(t, b, "a").State == stateConnected
	})

	for _, s := range a.Snapshot() {
		if s.Peer == "b" {
			if s.Endpoint == "" || s.Since.IsZero() {
				t.Errorf("b connection incomplete: %+v", s)
			}
		}
	}

	// Keepalives must not kill the link: with the interval pinned short,
	// several ping/pong rounds pass both directions.
	old := pingInterval
	pingInterval = 30 * time.Millisecond
	defer func() { pingInterval = old }()
	time.Sleep(150 * time.Millisecond)
	if stateOf(t, a, "b").State != stateConnected || stateOf(t, b, "a").State != stateConnected {
		t.Fatal("link did not survive keepalive rounds")
	}
}

func TestLinkRecoversAfterPeerRestart(t *testing.T) {
	epA := freeEndpoint(t)
	epB := freeEndpoint(t)

	a := managerFor(t, epA, epB, "a", "b")
	t.Cleanup(func() { a.Close() })
	ctxA, cancelA := context.WithCancel(context.Background())
	t.Cleanup(cancelA)
	go a.Run(ctxA)

	// First incarnation of b.
	b := managerFor(t, epB, epA, "b", "a")
	ctxB, cancelB := context.WithCancel(context.Background())
	go b.Run(ctxB)

	waitFor(t, 3*time.Second, func() bool {
		return stateOf(t, a, "b").State == stateConnected
	})

	// b goes away without a word (process kill): a must notice and back off.
	cancelB()
	b.Close()
	waitFor(t, 5*time.Second, func() bool {
		return stateOf(t, a, "b").State == stateBackoff
	})

	// b comes back on the same endpoint: a must remerge.
	b2 := managerFor(t, epB, epA, "b", "a")
	ctxB2, cancelB2 := context.WithCancel(context.Background())
	t.Cleanup(cancelB2)
	t.Cleanup(func() { b2.Close() })
	go b2.Run(ctxB2)

	waitFor(t, 8*time.Second, func() bool {
		return stateOf(t, a, "b").State == stateConnected &&
			stateOf(t, b2, "a").State == stateConnected
	})
}

func TestUnaddressedPeerNeverDials(t *testing.T) {
	m := New(&config.Config{
		ConfigVersion: config.ConfigVersion,
		Peer:          "self",
		Peers: map[string]config.Peer{
			"self":     {},
			"stranger": {}, // declared, no endpoints
			"b":        {Endpoints: []string{"127.0.0.1:1"}},
		},
	}, nil)

	got := m.Snapshot()
	byName := map[string]api_Connection{}
	for _, c := range got {
		byName[c.Peer] = c
	}
	if len(got) != 2 {
		t.Fatalf("snapshot = %+v, want an entry per other peer", got)
	}
	if byName["stranger"].State != stateUnaddressed || byName["stranger"].Error == "" {
		t.Errorf("stranger = %+v, want unaddressed with a reason", byName["stranger"])
	}
}

// blockingConn records each write and blocks the first one until released.
type blockingConn struct {
	net.Conn
	mu      sync.Mutex
	writes  []string
	release chan struct{}
	started chan string
}

func (c *blockingConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, string(b))
	c.mu.Unlock()
	select {
	case c.started <- string(b):
	default:
	}
	<-c.release
	return len(b), nil
}

func (c *blockingConn) SetWriteDeadline(time.Time) error { return nil }

// A blob transfer writes thousands of chunks. While one is in flight the
// socket is busy, so control frames wait for the next chunk boundary -- but
// they must never wait behind further bulk writes, or a 21 GB video starves
// every index frame to that peer and small stores stop converging.
func TestControlFramesOvertakeQueuedBulkWrites(t *testing.T) {
	c := &blockingConn{
		release: make(chan struct{}),
		started: make(chan string, 8),
	}
	w := newLinkWriter(c)
	go w.SendBulk([]byte("bulk-1"))
	<-c.started // bulk-1 owns the socket

	controlDone := make(chan struct{})
	go func() {
		_ = w.Send([]byte("control"))
		close(controlDone)
	}()
	// Give the control writer time to queue behind the in-flight chunk.
	time.Sleep(20 * time.Millisecond)

	bulkDone := make(chan struct{})
	go func() {
		_ = w.SendBulk([]byte("bulk-2"))
		close(bulkDone)
	}()
	time.Sleep(20 * time.Millisecond)

	// Let the first chunk complete, then keep draining.
	go func() {
		for i := 0; i < 2; i++ {
			<-c.started
			c.release <- struct{}{}
		}
	}()
	c.release <- struct{}{}

	select {
	case <-controlDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the control frame never got the socket")
	}
	<-bulkDone

	c.mu.Lock()
	got := append([]string(nil), c.writes...)
	c.mu.Unlock()
	if len(got) < 3 {
		t.Fatalf("writes = %v", got)
	}
	if got[0] != "bulk-1" || got[1] != "control" || got[2] != "bulk-2" {
		t.Fatalf("order = %v, want the queued control frame second", got)
	}
}
