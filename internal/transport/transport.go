// Package transport is storeboard's peer wire: every daemon listens on the
// endpoints declared for itself and dials the endpoints declared for every
// other peer, exchanging newline-delimited JSON.
//
// The first line in both directions is a hello; after that the connection
// carries ping/pong keepalives and (later) the sync protocol. Nothing here
// is privileged: a connection learns a peer's name and version and nothing
// else, matching the status-document trust tier. Peers are never learned
// from the wire — an undeclared name is a rejected connection.
package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/version"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// WireVersion is the protocol version this build speaks (DESIGN invariant 9:
// peers advertise and agree on a version before anything flows).
const WireVersion = 1

const (
	handshakeTimeout = 10 * time.Second
	dialTimeout      = 5 * time.Second
	maxBackoff       = 60 * time.Second
	bindRetry        = 5 * time.Second
)

// pingInterval is a var only so tests can run keepalive rounds quickly.
var pingInterval = 30 * time.Second

// States, mirrored in api.Connection docs.
const (
	stateConnected   = "connected"
	stateDialing     = "dialing"
	stateBackoff     = "backoff"
	stateUnaddressed = "unaddressed"
)

type hello struct {
	Protocol int    `json:"protocol"`
	Name     string `json:"name"`
	Version  string `json:"version"`
}

// peerState is one declared peer's link bookkeeping. Every field is guarded
// by mu; the linkReset channel is closed (and replaced) whenever a live
// link dies, waking the dial loop without polling.
type peerState struct {
	name      string
	endps     []string // dial targets, from the peer's registry entry
	direction string   // the registered link: inbound or outbound
	state     string
	since     time.Time
	lastErr   string
	attempt   int

	conn      net.Conn
	linkReset chan struct{}

	mu sync.Mutex
}

func (p *peerState) connected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conn != nil
}

func (p *peerState) snapshot() api.Connection {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := api.Connection{
		Peer:  p.name,
		State: p.state,
		Since: p.since,
		Error: p.lastErr,
	}
	if p.conn != nil {
		out.Endpoint = p.conn.RemoteAddr().String()
	} else if len(p.endps) > 0 {
		out.Endpoint = p.endps[0]
	}
	return out
}

func (p *peerState) backoff() time.Duration {
	if p.attempt == 0 {
		return 0
	}
	d := time.Duration(1<<min(p.attempt, 6)) * time.Second
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

// Handler receives peer lifecycle and frames. The sync engine implements it;
// the transport itself only keeps links alive.
type Handler interface {
	// OnPeerConnected reports a live, handshaked link. The sender writes
	// newline-terminated frames and is safe from any goroutine.
	OnPeerConnected(peer string, send Sender)
	// OnPeerFrame delivers one frame line from a peer.
	OnPeerFrame(peer string, line []byte)
	// OnPeerDisconnected reports a link going away.
	OnPeerDisconnected(peer string)
}

// Manager owns the listener, the dial loops and every peer's link state.
type Manager struct {
	cfg    *config.Config
	logger *slog.Logger

	mu      sync.Mutex
	peers   map[string]*peerState
	listen  []net.Listener
	handler Handler

	// wg counts every goroutine that can still be inside a frame handler.
	// Close waits on it: a link being torn down may be mid-apply, writing a
	// blob into the store or a file into a store, and returning from Close
	// while that write is in flight races whatever removes those paths next
	// (tests do exactly that, and a restart does it for real).
	wg sync.WaitGroup

	// cancel ends the context Run derived from the caller's. Close uses it
	// so teardown never depends on the caller cancelling first: a caller
	// that just calls Close (every transport test does) would otherwise
	// leave the dial and listen loops waiting on a context that never ends.
	cancel context.CancelFunc
}

// spawn runs fn on a goroutine Close will wait for.
func (m *Manager) spawn(fn func()) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		fn()
	}()
}

// SetHandler installs the frame consumer. Call before Run.
func (m *Manager) SetHandler(h Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = h
}

func (m *Manager) getHandler() Handler {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.handler
}

// Sender writes frames to one peer. Blob chunks are bulk: a multi-gigabyte
// file is thousands of them, and while one is in flight every other frame to
// that peer waits. Separating the two lets index frames overtake a transfer
// instead of queueing behind 21 GB of video, which is what stopped small
// stores from converging at all on the real mesh.
type Sender interface {
	// Send writes one newline-terminated frame ahead of any bulk traffic.
	Send(b []byte) error
	// SendBulk writes one chunk of a blob, yielding to waiting Send calls.
	SendBulk(b []byte) error
}

// linkWriter serializes writes on one connection: keepalives and sync frames
// share the socket. Priority writers are admitted before bulk ones, so a
// transfer that owns the socket for minutes still lets control frames through
// between chunks.
type linkWriter struct {
	mu      sync.Mutex
	cond    *sync.Cond
	busy    bool
	waiting int
	c       net.Conn
}

func newLinkWriter(c net.Conn) *linkWriter {
	w := &linkWriter{c: c}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// acquire takes the socket. A bulk writer waits while any priority writer is
// queued; priority writers only wait for the chunk currently being written.
func (w *linkWriter) acquire(priority bool) {
	w.mu.Lock()
	if priority {
		w.waiting++
	}
	for w.busy || (!priority && w.waiting > 0) {
		w.cond.Wait()
	}
	if priority {
		w.waiting--
	}
	w.busy = true
	w.mu.Unlock()
}

func (w *linkWriter) release() {
	w.mu.Lock()
	w.busy = false
	w.cond.Broadcast()
	w.mu.Unlock()
}

func (w *linkWriter) writeWith(b []byte, priority bool) error {
	w.acquire(priority)
	defer w.release()
	if err := w.c.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	_, err := w.c.Write(b)
	_ = w.c.SetWriteDeadline(time.Time{})
	return err
}

func (w *linkWriter) Send(b []byte) error     { return w.writeWith(b, true) }
func (w *linkWriter) SendBulk(b []byte) error { return w.writeWith(b, false) }

// New prepares the manager. No sockets are opened until Run.
func New(cfg *config.Config, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	m := &Manager{cfg: cfg, logger: logger, peers: map[string]*peerState{}}
	now := time.Now().UTC()
	for name, p := range cfg.Peers {
		if name == cfg.Peer {
			continue
		}
		st := &peerState{
			name:      name,
			endps:     p.Endpoints,
			state:     stateDialing,
			since:     now,
			linkReset: make(chan struct{}),
		}
		if len(st.endps) == 0 {
			st.state = stateUnaddressed
			st.lastErr = "no endpoints declared"
		}
		m.peers[name] = st
	}
	return m
}

// selfEndpoints are the addresses this daemon serves on.
func (m *Manager) selfEndpoints() []string {
	self, ok := m.cfg.Peers[m.cfg.Peer]
	if !ok {
		return nil
	}
	return self.Endpoints
}

// Run opens the listeners and the dial loops and serves until ctx ends or
// Close is called.
func (m *Manager) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()

	m.runListeners(ctx)

	for _, p := range m.sortedPeers() {
		if p.state == stateUnaddressed {
			continue
		}
		m.spawn(func() { m.dialLoop(ctx, p) })
	}
}

func (m *Manager) sortedPeers() []*peerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*peerState, 0, len(m.peers))
	for _, p := range m.peers {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// runListeners binds every self endpoint. A bind can fail at boot because
// the nebula address is not assigned yet; it retries instead of dying.
func (m *Manager) runListeners(ctx context.Context) {
	for _, ep := range m.selfEndpoints() {
		m.spawn(func() { m.listenLoop(ctx, ep) })
	}
}

func (m *Manager) listenLoop(ctx context.Context, ep string) {
	for {
		ln, err := net.Listen("tcp", ep)
		if err == nil {
			m.mu.Lock()
			m.listen = append(m.listen, ln)
			m.mu.Unlock()
			m.logger.Info("transport listening", "endpoint", ep)
			m.acceptLoop(ctx, ln)
			return
		}
		if ctx.Err() != nil {
			return
		}
		m.logger.Warn("transport bind failed; retrying", "endpoint", ep, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(bindRetry):
		}
	}
}

func (m *Manager) acceptLoop(ctx context.Context, ln net.Listener) {
	acceptErr := make(chan error, 1)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				acceptErr <- err
				return
			}
			m.spawn(func() { m.accept(ctx, c) })
		}
	}()
	select {
	case <-ctx.Done():
		ln.Close()
	case err := <-acceptErr:
		m.logger.Warn("transport accept loop ended", "error", err)
	}
}

func (m *Manager) accept(ctx context.Context, c net.Conn) {
	h, reader, err := m.handshake(c)
	if err != nil {
		m.logger.Warn("rejected inbound connection", "remote", c.RemoteAddr().String(), "error", err)
		c.Close()
		return
	}
	m.register(ctx, h, c, reader, "inbound")
}

// dialLoop keeps one peer's link up: dial → handshake → serve until it
// dies → back off and go again.
func (m *Manager) dialLoop(ctx context.Context, p *peerState) {
	for ctx.Err() == nil {
		if p.connected() {
			reset := p.resetChan()
			select {
			case <-ctx.Done():
				return
			case <-reset:
			}
			continue
		}
		delay := p.backoff()
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		m.tryDial(ctx, p)
	}
}

func (p *peerState) resetChan() chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.linkReset
}

func (m *Manager) tryDial(ctx context.Context, p *peerState) {
	if p.connected() {
		return
	}

	p.mu.Lock()
	p.state = stateDialing
	p.since = time.Now().UTC()
	p.mu.Unlock()

	for _, ep := range p.endps {
		if p.connected() {
			return
		}
		var d net.Dialer
		c, err := d.DialContext(ctx, "tcp", ep)
		if err != nil {
			m.noteDialFailure(p, ep, err)
			continue
		}
		h, reader, herr := m.handshake(c)
		if herr != nil {
			m.noteDialFailure(p, ep, herr)
			c.Close()
			continue
		}
		if h.Name != p.name {
			c.Close()
			m.noteDialFailure(p, ep, fmt.Errorf("endpoint answered as %q", h.Name))
			continue
		}
		m.register(ctx, h, c, reader, "outbound")
		return
	}
}

func (m *Manager) noteDialFailure(p *peerState, ep string, err error) {
	p.mu.Lock()
	p.attempt++
	p.state = stateBackoff
	p.since = time.Now().UTC()
	p.lastErr = fmt.Sprintf("%s: %v", ep, err)
	p.mu.Unlock()
	m.logger.Debug("dial failed", "peer", p.name, "endpoint", ep, "error", err)
}

// handshake performs the hello exchange with hard deadlines. It returns the
// bufio reader it used, because bytes it buffered past the hello line would
// otherwise be lost when the serving loop builds its own reader.
func (m *Manager) handshake(c net.Conn) (hello, *bufio.Reader, error) {
	var got hello
	_ = c.SetDeadline(time.Now().Add(handshakeTimeout))
	defer c.SetDeadline(time.Time{})

	line, err := json.Marshal(hello{Protocol: WireVersion, Name: m.cfg.Peer, Version: version.Version})
	if err != nil {
		return got, nil, err
	}
	if _, err := c.Write(append(line, '\n')); err != nil {
		return got, nil, fmt.Errorf("write hello: %w", err)
	}

	reader := bufio.NewReader(c)
	if err := json.NewDecoder(reader).Decode(&got); err != nil {
		return got, reader, fmt.Errorf("read hello: %w", err)
	}
	switch {
	case got.Protocol != WireVersion:
		return got, reader, fmt.Errorf("wire protocol %d, want %d", got.Protocol, WireVersion)
	case got.Name == "":
		return got, reader, fmt.Errorf("empty peer name")
	case got.Name == m.cfg.Peer:
		return got, reader, fmt.Errorf("peer claims to be me")
	default:
		if _, ok := m.cfg.Peers[got.Name]; !ok {
			return got, reader, fmt.Errorf("undeclared peer %q", got.Name)
		}
	}
	return got, reader, nil
}

// connKey identifies a connection independently of which end looks at it:
// the sorted pair of local and remote addresses. Two daemons that dial each
// other simultaneously compute the same keys, so both keep the same
// connection and the tie-break is stable.
func connKey(c net.Conn) string {
	local, remote := c.LocalAddr().String(), c.RemoteAddr().String()
	pair := []string{local, remote}
	sort.Strings(pair)
	return pair[0] + "|" + pair[1]
}

// preferredDirection gives each peer pair one stable link: the lexically
// lower peer dials, so it keeps outbound; the higher peer keeps inbound.
func (m *Manager) preferredDirection(peer string) string {
	if m.cfg.Peer < peer {
		return "outbound"
	}
	return "inbound"
}

// register adopts a handshaked connection. The direction rule is symmetric
// on both peers and does not depend on ephemeral TCP ports. That matters for
// large initial indexes: choosing different links makes both sides close the
// other's connection and leaves the index exchange deadlocked.
func (m *Manager) register(ctx context.Context, h hello, c net.Conn, reader *bufio.Reader, direction string) {
	key := connKey(c)

	m.mu.Lock()
	p, ok := m.peers[h.Name]
	m.mu.Unlock()
	if !ok {
		c.Close()
		return
	}

	preferred := m.preferredDirection(h.Name)
	p.mu.Lock()
	if p.conn == nil && direction != preferred {
		// Two peers dialling each other can both reject the other's link as
		// noncanonical while neither holds one, and then both retry: a
		// livelock that only the retry timing breaks, which under load can
		// last minutes. Direction is a tie-break for choosing between two
		// links, not a reason to refuse the only one. Accept it; a canonical
		// link arriving later replaces it below.
		m.logger.Debug("noncanonical link accepted as the only one", "peer", h.Name, "direction", direction, "preferred", preferred)
	}
	if p.conn != nil && p.conn != c {
		old := p.conn
		switch {
		case p.direction == preferred:
			// The existing link has the canonical direction.
			p.mu.Unlock()
			m.logger.Debug("duplicate link dropped", "peer", h.Name, "direction", direction)
			c.Close()
			return
		case direction != preferred:
			// Neither link is canonical; retain the first one to avoid churn.
			p.mu.Unlock()
			m.logger.Debug("duplicate link dropped", "peer", h.Name, "direction", direction)
			c.Close()
			return
		default:
			// The new link has the canonical direction; replace the old one.
			old.Close()
		}
	}
	p.conn = c
	p.direction = direction
	p.attempt = 0
	p.state = stateConnected
	p.since = time.Now().UTC()
	p.lastErr = ""
	p.mu.Unlock()

	m.logger.Info("peer connected", "peer", h.Name, "direction", direction, "version", h.Version, "remote", c.RemoteAddr().String())
	m.spawn(func() { m.serveLink(ctx, p, c, reader, key, direction) })
}

// serveLink consumes one live connection: it answers pings and detects the
// peer going away.
func (m *Manager) serveLink(ctx context.Context, p *peerState, c net.Conn, reader *bufio.Reader, key, direction string) {
	w := newLinkWriter(c)
	handler := m.getHandler()
	p.mu.Lock()
	current := p.conn == c
	p.mu.Unlock()
	if !current {
		return
	}

	var frames chan []byte
	var framesDone chan struct{}
	var framesStop context.CancelFunc
	if handler != nil {
		framesCtx, cancelFrames := context.WithCancel(ctx)
		framesStop = cancelFrames
		frames = make(chan []byte, 1024)
		framesDone = make(chan struct{})
		m.spawn(func() {
			defer close(framesDone)
			for {
				select {
				case <-framesCtx.Done():
					return
				case line, ok := <-frames:
					if !ok {
						return
					}
					p.mu.Lock()
					stillCurrent := p.conn == c
					p.mu.Unlock()
					if stillCurrent {
						handler.OnPeerFrame(p.name, line)
					}
				}
			}
		})
		m.spawn(func() {
			p.mu.Lock()
			stillCurrent := p.conn == c
			p.mu.Unlock()
			if stillCurrent {
				handler.OnPeerConnected(p.name, w)
			}
		})
	}

	stop := make(chan struct{})
	defer func() {
		close(stop)
		if framesStop != nil {
			framesStop()
		}
		if frames != nil {
			close(frames)
			<-framesDone
		}
		m.dropLink(p, c, key)
	}()
	go func() {
		t := time.NewTicker(pingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				c.Close()
				return
			case <-stop:
				return
			case <-t.C:
				if err := w.Send([]byte("{\"ping\"}\n")); err != nil {
					c.Close()
					return
				}
			}
		}
	}()

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if ctx.Err() == nil && !isClosed(err) {
				m.logger.Info("link closed", "peer", p.name, "direction", direction, "error", err)
			}
			return
		}
		switch trimmed := strings.TrimSpace(line); trimmed {
		case "{\"ping\"}":
			if err := w.Send([]byte("{\"pong\"}\n")); err != nil {
				return
			}
		case "{\"pong\"}", "":
		default:
			if frames != nil {
				select {
				case frames <- []byte(trimmed):
				case <-ctx.Done():
					return
				}
			} else {
				m.logger.Debug("unhandled frame", "peer", p.name, "bytes", len(line))
			}
		}
	}
}

// dropLink tears down a connection that just died, if it is still the
// registered one, and wakes the dial loop.
func (m *Manager) dropLink(p *peerState, c net.Conn, key string) {
	c.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == c {
		p.conn = nil
		p.attempt++
		p.state = stateBackoff
		p.since = time.Now().UTC()
		p.lastErr = "connection closed"
		close(p.linkReset)
		p.linkReset = make(chan struct{})
		if h := m.getHandler(); h != nil {
			h.OnPeerDisconnected(p.name)
		}
	}
}

// Snapshot lists every declared peer's link state, sorted by name; the
// status document serves it verbatim.
func (m *Manager) Snapshot() []api.Connection {
	m.mu.Lock()
	peers := make([]*peerState, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, p)
	}
	m.mu.Unlock()

	sort.Slice(peers, func(i, j int) bool { return peers[i].name < peers[j].name })
	out := make([]api.Connection, 0, len(peers))
	for _, p := range peers {
		out = append(out, p.snapshot())
	}
	return out
}

// Close tears the transport down and waits for every link goroutine: a
// handler caught mid-apply must finish before the caller removes or reopens
// the paths it writes to. It cancels the context Run derived, so teardown
// works whether or not the caller got there first.
func (m *Manager) Close() {
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	ls := m.listen
	m.listen = nil
	m.mu.Unlock()
	for _, ln := range ls {
		ln.Close()
	}
	for _, p := range m.peers {
		p.mu.Lock()
		c := p.conn
		p.conn = nil
		p.mu.Unlock()
		if c != nil {
			c.Close()
		}
	}
	m.wg.Wait()
}

// isClosed reports errors that mean this side closed the connection: normal
// during shutdown and duplicate-link churn, noise as a "link closed" line.
func isClosed(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
