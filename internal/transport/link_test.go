package transport

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
)

// recHandler keeps one sender per peer the way the sync engine does: a
// disconnect forgets the sender only if it is the one installed.
type recHandler struct {
	mu        sync.Mutex
	sends     map[string]Sender
	frames    chan string
	connected func(peer string, s Sender)
}

func newRecHandler() *recHandler {
	return &recHandler{sends: map[string]Sender{}, frames: make(chan string, 1024)}
}

func (h *recHandler) OnPeerConnected(peer string, s Sender) {
	h.mu.Lock()
	h.sends[peer] = s
	fn := h.connected
	h.mu.Unlock()
	if fn != nil {
		fn(peer, s)
	}
}

func (h *recHandler) OnPeerDisconnected(peer string, s Sender) {
	h.mu.Lock()
	if h.sends[peer] == s {
		delete(h.sends, peer)
	}
	h.mu.Unlock()
}

func (h *recHandler) OnPeerFrame(peer string, line []byte) {
	select {
	case h.frames <- string(line):
	default:
	}
}

func (h *recHandler) sender(peer string) Sender {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sends[peer]
}

// soloManager is "a", listening on ep, with "b" declared but never dialled:
// the tests play b by hand.
func soloManager(ep string) *Manager {
	return New(&config.Config{
		ConfigVersion: config.ConfigVersion,
		Peer:          "a",
		Peers: map[string]config.Peer{
			"a": {Endpoints: []string{ep}},
			"b": {},
		},
	}, nil)
}

// dialAsB connects to ep and completes the hello as peer b.
func dialAsB(t *testing.T, ep string) net.Conn {
	t.Helper()
	var c net.Conn
	var err error
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err = net.Dial("tcp", ep)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial %s: %v", ep, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := c.Write([]byte(`{"protocol":1,"name":"b","version":"test"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := bufio.NewReader(c).ReadString('\n'); err != nil {
		t.Fatalf("read hello: %v", err)
	}
	_ = c.SetReadDeadline(time.Time{})
	return c
}

// The read loop must never write. It used to answer every ping inline, and a
// bulk transfer holding the socket (a peer slow to read) parked the read loop
// behind it: nothing more was read, the link looked dead, and it was dropped.
func TestReadLoopKeepsReadingWhileTheSocketIsFull(t *testing.T) {
	ep := freeEndpoint(t)
	m := soloManager(ep)
	h := newRecHandler()
	bulkStarted := make(chan struct{})
	h.connected = func(_ string, s Sender) {
		// Fill the socket: the test's peer never reads.
		go func() {
			close(bulkStarted)
			_ = s.SendBulk([]byte(strings.Repeat("x", 64<<20) + "\n"))
		}()
	}
	m.SetHandler(h)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(m.Close)
	go m.Run(ctx)

	c := dialAsB(t, ep)
	defer c.Close()
	<-bulkStarted
	time.Sleep(200 * time.Millisecond) // let the bulk write fill the socket

	if _, err := c.Write([]byte("{\"ping\"}\n{\"ping\"}\n{\"type\":\"probe\"}\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-h.frames:
		if got != `{"type":"probe"}` {
			t.Fatalf("frame = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read loop stopped reading behind a full socket")
	}
}

// Link churn must leave the handler holding the live link's sender: a dying
// link's disconnect must not remove its replacement, and a superseded link's
// connect must not land after the replacement's.
func TestHandlerEndsWithTheLiveSenderAfterChurn(t *testing.T) {
	epA := freeEndpoint(t)
	epB := freeEndpoint(t)
	a := managerFor(t, epA, epB, "a", "b")
	b := managerFor(t, epB, epA, "b", "a")
	ha, hb := newRecHandler(), newRecHandler()
	a.SetHandler(ha)
	b.SetHandler(hb)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)
	go a.Run(ctx)
	go b.Run(ctx)

	liveWriter := func() bool {
		p := a.peers["b"]
		p.mu.Lock()
		c := p.conn
		p.mu.Unlock()
		lw, ok := ha.sender("b").(*linkWriter)
		return c != nil && ok && lw.c == c
	}
	for i := 0; i < 4; i++ {
		waitFor(t, 10*time.Second, liveWriter)
		p := a.peers["b"]
		p.mu.Lock()
		c := p.conn
		p.mu.Unlock()
		if c != nil {
			c.Close()
		}
	}
	waitFor(t, 10*time.Second, liveWriter)

	// And the sender works: a frame on it reaches b.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if s := ha.sender("b"); s != nil {
			_ = s.Send([]byte("{\"type\":\"probe\"}\n"))
		}
		select {
		case <-hb.frames:
			return
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("no frame crossed the link the handler holds")
		}
	}
}

// faultyListener fails Accept: a fixed number of times, or always until
// closed.
type faultyListener struct {
	net.Listener
	fails  atomic.Int32
	always bool
	closed atomic.Bool
}

var errFakeAccept = &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}

func (l *faultyListener) Accept() (net.Conn, error) {
	if l.closed.Load() {
		return nil, net.ErrClosed
	}
	if l.always {
		time.Sleep(time.Millisecond)
		return nil, errFakeAccept
	}
	if l.fails.Add(-1) >= 0 {
		return nil, errFakeAccept
	}
	return l.Listener.Accept()
}

func (l *faultyListener) Close() error {
	l.closed.Store(true)
	return l.Listener.Close()
}

// An Accept error (EMFILE, ECONNABORTED) used to end the accept loop for
// good: the daemon stayed unreachable until restarted. It is retried, and a
// listener that keeps failing is bound again.
func TestListenerSurvivesAcceptErrors(t *testing.T) {
	ep := freeEndpoint(t)
	m := soloManager(ep)
	var binds atomic.Int32
	m.listenFn = func(ep string) (net.Listener, error) {
		ln, err := net.Listen("tcp", ep)
		if err != nil {
			return nil, err
		}
		if binds.Add(1) == 1 {
			return &faultyListener{Listener: ln, always: true}, nil
		}
		fl := &faultyListener{Listener: ln}
		fl.fails.Store(3)
		return fl, nil
	}
	m.acceptRebind = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(m.Close)
	go m.Run(ctx)

	waitFor(t, 5*time.Second, func() bool { return binds.Load() >= 2 })
	c := dialAsB(t, ep)
	defer c.Close()
	waitFor(t, 5*time.Second, func() bool { return stateOf(t, m, "b").State == stateConnected })
	if !errors.Is(errFakeAccept, syscall.EMFILE) {
		t.Fatal("fake error is not EMFILE")
	}
}
