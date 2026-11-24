// Package transport is storeboard's peer wire: every daemon listens on the
// endpoints declared for itself and dials the endpoints declared for every
// other peer, exchanging newline-delimited JSON.
//
// The first line in both directions is a hello; after that the connection
// carries ping keepalives and (later) the sync protocol. Nothing here
// is privileged: a connection learns a peer's name and version and nothing
// else, matching the status-document trust tier. Peers are never learned
// from the wire — an undeclared name is a rejected connection.
package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
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
	// Time is the sender's clock (unix nanoseconds) as it wrote the hello.
	// Both sides write theirs before reading, so the difference is the skew
	// give or take half a round trip.
	Time int64 `json:"time,omitempty"`
	// Features names optional protocol the sender understands; frames only
	// a newer peer handles are sent to peers that list them.
	Features []string `json:"features,omitempty"`
}

// ClockSkewWarn is the skew worth telling the user about: conflicts are
// decided by modification time, and beyond this a machine with a fast clock
// wins edits it should lose.
const ClockSkewWarn = 2 * time.Second

// FeatureHandler is an optional Handler extension naming the features this
// side advertises in its hello.
type FeatureHandler interface {
	Features() []string
}

// InteractiveSender is implemented by the Sender handed to OnPeerConnected:
// SendInteractive writes a large frame someone is waiting on - a streamed
// read's reply - ahead of bulk sync traffic but behind Send.
type InteractiveSender interface {
	SendInteractive(b []byte) error
}

// FeatureSender is implemented by the Sender handed to OnPeerConnected: it
// reports what the peer's hello advertised.
type FeatureSender interface {
	HasFeature(name string) bool
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
	// version, skew and features describe the peer as its hello did, for
	// the life of the registered link.
	version  string
	skew     time.Duration
	features []string

	// events are lifecycle callbacks waiting for delivery, in the order the
	// links were installed and dropped; evRunning is set while a goroutine
	// drains them. Posting happens under mu, in the same critical section
	// that installs or drops the link, so a replacement's "connected" can
	// never be overtaken by its predecessor's, and a dying link's
	// "disconnected" can never land after the next link's "connected".
	events    []linkEvent
	evRunning bool

	mu sync.Mutex
}

// linkEvent is one lifecycle callback for a peer.
type linkEvent struct {
	connected bool
	c         net.Conn
	w         *linkWriter
	// ready is closed once a "connected" event has been delivered (or
	// skipped as stale): the link's frames wait for it, so the handler never
	// sees a frame from a link it has not been told about yet.
	ready chan struct{}
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
		out.Version = p.version
		out.ClockSkewMs = p.skew.Milliseconds()
	} else if len(p.endps) > 0 {
		out.Endpoint = p.endps[0]
	}
	return out
}

func (p *peerState) backoff() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
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
	// OnPeerDisconnected reports a link going away. send is the Sender
	// OnPeerConnected was given for that link: a handler forgets it only if
	// it is still the one installed. Callbacks for one peer are delivered
	// one at a time, in the order the links came and went.
	OnPeerDisconnected(peer string, send Sender)
}

// UrgentHandler is an optional Handler extension. The link's read loop offers
// it every frame first; a frame it claims is handled right there, ahead of the
// queue of ordinary frames. It is for replies a caller is blocked on - a
// ranged read's bytes - which otherwise wait behind every frame queued before
// them, so one store's slow index apply made another store's mount time out.
// It must be fast and must never block.
type UrgentHandler interface {
	OnPeerUrgent(peer string, line []byte) bool
}

// maxFrame bounds one wire line. Index frames are chunked well below this; a
// line past it is a broken or hostile peer, and buffering it would let either
// exhaust memory.
const maxFrame = 256 << 20

// readLine reads one newline-terminated line of at most limit bytes.
func readLine(r *bufio.Reader, limit int) ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.ReadSlice('\n')
		if len(line)+len(chunk) > limit {
			return nil, fmt.Errorf("frame longer than %d bytes", limit)
		}
		line = append(line, chunk...)
		if err == bufio.ErrBufferFull {
			continue
		}
		return line, err
	}
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

	// closing is set by Close before it waits on wg; spawn refuses new
	// goroutines after that, so an accept racing shutdown cannot Add to a
	// WaitGroup that is already being waited on.
	closing bool

	// ping is pingInterval as it was when the manager was made.
	ping time.Duration

	// listenFn opens a listener; acceptRebind is how long Accept may keep
	// failing before the listener is closed and bound again. Both are copied
	// from package variables so tests can turn them.
	listenFn     func(ep string) (net.Listener, error)
	acceptRebind time.Duration

	// cancel ends the context Run derived from the caller's. Close uses it
	// so teardown never depends on the caller cancelling first: a caller
	// that just calls Close (every transport test does) would otherwise
	// leave the dial and listen loops waiting on a context that never ends.
	cancel context.CancelFunc
}

// spawn runs fn on a goroutine Close will wait for. It reports false, and
// does not run fn, once Close has begun.
func (m *Manager) spawn(fn func()) bool {
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return false
	}
	m.wg.Add(1)
	m.mu.Unlock()
	go func() {
		defer m.wg.Done()
		fn()
	}()
	return true
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
	mu   sync.Mutex
	cond *sync.Cond
	busy bool
	// queues holds each lane's waiting writers by ticket, oldest first.
	queues [lanes][]uint64
	ticket uint64
	// lastShared is the lane that had the last turn when interactive and
	// bulk writers were both waiting.
	lastShared int
	c          net.Conn
	// features is what the peer's hello advertised.
	features []string
	// alive reports that the peer has been heard from recently. A write
	// that times out while it is true is a busy reader, not a dead link.
	alive func() bool
	// heard is when the link's read loop last read a line (unix nanos).
	heard atomic.Int64
}

// linkFrameQueue is how many frames a link buffers for the handler. The
// handler only makes the link wait for backpressure that is real (blob chunks
// arriving faster than the disk takes them), so this is a cushion rather
// than the only thing between a slow store and a stalled read loop - and at
// up to a blob chunk a frame, a large one is a lot of memory per link.
const linkFrameQueue = 128

// Lanes. Priority (control frames, keepalives) always goes first. The other
// two take turns frame for frame while both have writers waiting:
// interactive is a streamed read someone is looking at, bulk a background
// transfer, and strict priority let a playing video starve a copy - or a
// streamed read of a big file starve everything behind it. Within a lane
// writers go in the order they asked, and a transfer asks again for each
// chunk, so concurrent transfers share the link evenly instead of one
// winning every race for the lock.
const (
	lanePriority = iota
	laneInteractive
	laneBulk
	lanes
)

// notSentLowat bounds the bytes the kernel holds unsent (see tuneConn): about
// half a blob frame, so the next turn is decided within one frame's time.
const notSentLowat = 128 << 10

func newLinkWriter(c net.Conn) *linkWriter {
	w := &linkWriter{c: c, lastShared: laneBulk}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// acquire takes the socket for a writer at the given lane, in its turn.
func (w *linkWriter) acquire(lane int) {
	w.mu.Lock()
	w.ticket++
	me := w.ticket
	w.queues[lane] = append(w.queues[lane], me)
	for w.busy || w.nextLocked() != lane || w.queues[lane][0] != me {
		w.cond.Wait()
	}
	w.queues[lane] = w.queues[lane][1:]
	if lane != lanePriority {
		w.lastShared = lane
	}
	w.busy = true
	w.mu.Unlock()
}

// nextLocked is the lane whose oldest writer goes next.
func (w *linkWriter) nextLocked() int {
	if len(w.queues[lanePriority]) > 0 {
		return lanePriority
	}
	ia, bulk := len(w.queues[laneInteractive]) > 0, len(w.queues[laneBulk]) > 0
	switch {
	case ia && bulk:
		if w.lastShared == laneInteractive {
			return laneBulk
		}
		return laneInteractive
	case ia:
		return laneInteractive
	default:
		return laneBulk
	}
}

func (w *linkWriter) release() {
	w.mu.Lock()
	w.busy = false
	w.cond.Broadcast()
	w.mu.Unlock()
}

// writeTimeout bounds one attempt at a write. It is a var only for tests.
var writeTimeout = 30 * time.Second

func (w *linkWriter) writeWith(b []byte, lane int) error {
	w.acquire(lane)
	defer w.release()
	for {
		if err := w.c.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			return err
		}
		n, err := w.c.Write(b)
		b = b[n:]
		if err == nil {
			break
		}
		// A peer whose engine is busy (verifying a big file, applying a huge
		// index) stops reading for a while, and its socket fills. It is
		// still alive - its pings keep arriving - so keep waiting: the
		// deadline used to drop the link, which restarted the very transfer
		// that kept it busy, and the next one too.
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() && w.alive != nil && w.alive() {
			continue
		}
		_ = w.c.SetWriteDeadline(time.Time{})
		// A write that failed part-way left half a frame on the wire, and
		// the next frame would be glued onto it. The link is unusable:
		// close it so both sides redial and resync instead.
		w.c.Close()
		return err
	}
	_ = w.c.SetWriteDeadline(time.Time{})
	return nil
}

// HasFeature reports whether the peer advertised a feature.
func (w *linkWriter) HasFeature(name string) bool { return slices.Contains(w.features, name) }

func (w *linkWriter) Send(b []byte) error            { return w.writeWith(b, lanePriority) }
func (w *linkWriter) SendInteractive(b []byte) error { return w.writeWith(b, laneInteractive) }
func (w *linkWriter) SendBulk(b []byte) error        { return w.writeWith(b, laneBulk) }

// New prepares the manager. No sockets are opened until Run.
func New(cfg *config.Config, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	m := &Manager{cfg: cfg, logger: logger, peers: map[string]*peerState{}, ping: pingInterval,
		listenFn: listenTCP, acceptRebind: acceptRebindAfter}
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
		if len(p.endps) == 0 { // unaddressed; endps never changes, unlike state
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

// listenTCP and acceptRebindAfter are the defaults New copies; vars only for
// tests.
var (
	listenTCP         = func(ep string) (net.Listener, error) { return net.Listen("tcp", ep) }
	acceptRebindAfter = 30 * time.Second
)

const (
	acceptBackoffMin = 5 * time.Millisecond
	acceptBackoffMax = time.Second
)

func (m *Manager) listenLoop(ctx context.Context, ep string) {
	for {
		ln, err := m.listenFn(ep)
		if err == nil {
			m.mu.Lock()
			if m.closing {
				m.mu.Unlock()
				ln.Close()
				return
			}
			m.listen = append(m.listen, ln)
			m.mu.Unlock()
			m.logger.Info("transport listening", "endpoint", ep)
			rebind := m.acceptLoop(ctx, ln)
			m.mu.Lock()
			m.listen = slices.DeleteFunc(m.listen, func(l net.Listener) bool { return l == ln })
			m.mu.Unlock()
			ln.Close()
			if !rebind || ctx.Err() != nil {
				return
			}
			m.logger.Warn("transport listener kept failing; binding it again", "endpoint", ep)
			continue
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

// acceptLoop serves one listener until ctx ends or the listener is closed. It
// reports true when it gave up on a listener that kept failing, which the
// caller binds again. An Accept error is usually transient - out of
// descriptors (EMFILE), a connection reset before it was taken (ECONNABORTED)
// - and used to end the loop for good, leaving the daemon unreachable until it
// restarted.
func (m *Manager) acceptLoop(ctx context.Context, ln net.Listener) (rebind bool) {
	acceptErr := make(chan error, 1)
	go func() {
		var failing time.Time
		delay := acceptBackoffMin
		for {
			c, err := ln.Accept()
			if err == nil {
				tuneConn(c)
			}
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
					acceptErr <- nil
					return
				}
				now := time.Now()
				if failing.IsZero() {
					failing = now
					m.logger.Warn("transport accept failed; retrying", "error", err)
				} else if now.Sub(failing) >= m.acceptRebind {
					acceptErr <- err
					return
				}
				select {
				case <-ctx.Done():
					acceptErr <- nil
					return
				case <-time.After(delay):
				}
				delay = min(2*delay, acceptBackoffMax)
				continue
			}
			failing, delay = time.Time{}, acceptBackoffMin
			if !m.spawn(func() { m.accept(ctx, c) }) {
				c.Close()
			}
		}
	}()
	select {
	case <-ctx.Done():
		ln.Close()
		<-acceptErr
		return false
	case err := <-acceptErr:
		if err != nil {
			m.logger.Warn("transport accept keeps failing", "error", err)
			return true
		}
		return false
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
		// Bounded: the kernel's own connect timeout is minutes, and one
		// blackholed endpoint would hold up every endpoint after it.
		d := net.Dialer{Timeout: dialTimeout}
		c, err := d.DialContext(ctx, "tcp", ep)
		if err != nil {
			m.noteDialFailure(p, ep, err)
			continue
		}
		tuneConn(c)
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

	var features []string
	if fh, ok := m.getHandler().(FeatureHandler); ok {
		features = fh.Features()
	}
	line, err := json.Marshal(hello{Protocol: WireVersion, Name: m.cfg.Peer, Version: version.Version, Time: time.Now().UnixNano(), Features: features})
	if err != nil {
		return got, nil, err
	}
	if _, err := c.Write(append(line, '\n')); err != nil {
		return got, nil, fmt.Errorf("write hello: %w", err)
	}

	// The hello is read as exactly one line. A json.Decoder reads ahead
	// into its own buffer, and a frame the peer sent right behind its hello
	// was silently lost with it.
	reader := bufio.NewReaderSize(c, 64*1024)
	raw, err := readLine(reader, 64*1024)
	if err != nil {
		return got, reader, fmt.Errorf("read hello: %w", err)
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		return got, reader, fmt.Errorf("read hello: %w", err)
	}
	if got.Time != 0 {
		// Reuse the field for the measured skew; the peer's raw clock is of
		// no use past this point.
		got.Time = got.Time - time.Now().UnixNano()
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
	p.version = h.Version
	p.skew = time.Duration(h.Time)
	p.features = h.Features
	p.attempt = 0
	p.state = stateConnected
	p.since = time.Now().UTC()
	p.lastErr = ""
	w := newLinkWriter(c)
	w.features = h.Features
	// Anything read counts: the peer pings every m.ping even while its own
	// frame handling is stalled.
	w.heard.Store(time.Now().UnixNano())
	ping := m.ping
	w.alive = func() bool { return time.Since(time.Unix(0, w.heard.Load())) < 2*ping }
	// Told to the handler in the same critical section that installed the
	// link: see peerState.events.
	ready := make(chan struct{})
	m.postLocked(p, linkEvent{connected: true, c: c, w: w, ready: ready})
	p.mu.Unlock()

	m.logger.Info("peer connected", "peer", h.Name, "direction", direction, "version", h.Version, "remote", c.RemoteAddr().String())
	if skew := time.Duration(h.Time); skew > ClockSkewWarn || skew < -ClockSkewWarn {
		m.logger.Warn("peer clock differs from this machine's; conflicting edits are decided by modification time, so the faster clock wins them",
			"peer", h.Name, "skew", skew.Round(time.Millisecond).String())
	}
	if !m.spawn(func() { m.serveLink(ctx, p, c, w, ready, reader, direction) }) {
		m.dropLink(p, c, w)
	}
}

// postLocked queues a lifecycle callback for the peer and makes sure a
// goroutine is delivering them. Callers hold p.mu. With no handler there is
// nothing to tell; a "connected" event's frames are released at once.
func (m *Manager) postLocked(p *peerState, ev linkEvent) {
	if m.getHandler() == nil {
		if ev.ready != nil {
			close(ev.ready)
		}
		return
	}
	p.events = append(p.events, ev)
	if p.evRunning {
		return
	}
	p.evRunning = true
	if !m.spawn(func() { m.deliverEvents(p) }) {
		// Shutting down: nobody will be told, and nothing waits on ready
		// past the link's context.
		p.evRunning = false
		p.events = nil
	}
}

// deliverEvents drains one peer's lifecycle callbacks, one at a time.
func (m *Manager) deliverEvents(p *peerState) {
	h := m.getHandler()
	for {
		p.mu.Lock()
		if len(p.events) == 0 {
			p.evRunning = false
			p.mu.Unlock()
			return
		}
		ev := p.events[0]
		p.events[0] = linkEvent{}
		p.events = p.events[1:]
		// A link replaced or dropped before its turn came is not worth
		// announcing: its "disconnected" (if any) is queued behind, and the
		// handler would only install a dead sender to remove it again.
		stale := ev.connected && p.conn != ev.c
		p.mu.Unlock()
		switch {
		case ev.connected && !stale:
			h.OnPeerConnected(p.name, ev.w)
		case !ev.connected:
			h.OnPeerDisconnected(p.name, ev.w)
		}
		if ev.ready != nil {
			close(ev.ready)
		}
	}
}

// serveLink consumes one live connection: it hands frames to the handler and
// detects the peer going away. It never writes: a write here waits for the
// socket, which a bulk transfer can hold for as long as the peer is slow to
// read, and a read loop stuck behind it stopped hearing the peer at all -
// the link then looked dead (see linkWriter.alive) and was dropped while both
// ends were busy and well.
func (m *Manager) serveLink(ctx context.Context, p *peerState, c net.Conn, w *linkWriter, ready chan struct{}, reader *bufio.Reader, direction string) {
	handler := m.getHandler()
	urgent, _ := handler.(UrgentHandler)
	p.mu.Lock()
	current := p.conn == c
	p.mu.Unlock()
	if !current {
		c.Close()
		return
	}

	var frames chan []byte
	var framesDone chan struct{}
	var framesStop context.CancelFunc
	if handler != nil {
		framesCtx, cancelFrames := context.WithCancel(ctx)
		framesStop = cancelFrames
		frames = make(chan []byte, linkFrameQueue)
		framesDone = make(chan struct{})
		started := m.spawn(func() {
			defer close(framesDone)
			// Not a frame before the handler has this link's sender: a
			// want answered on the previous link's would be lost.
			select {
			case <-framesCtx.Done():
				return
			case <-ready:
			}
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
		if !started {
			// Shutting down: nothing will consume frames, so serve none.
			cancelFrames()
			m.dropLink(p, c, w)
			return
		}
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
		m.dropLink(p, c, w)
	}()
	go func() {
		t := time.NewTicker(m.ping)
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

	// A live peer sends a ping every pingInterval. Three missed in a row is
	// a link that died without a FIN (a laptop lid, a dropped route), which
	// TCP alone takes a quarter of an hour to notice - and while it holds
	// the peer's slot, the peer's fresh connections are refused as
	// duplicates.
	//
	// Pings are not answered. Both ends ping on their own schedule and any
	// line read is proof of life, so a pong carried nothing the peer's own
	// ping did not - and writing it is what blocked this loop. A peer from
	// before this change does not miss the pongs for the same reason: its
	// liveness is any read too, and this side's pings keep arriving.
	idle := 3 * m.ping
	for {
		_ = c.SetReadDeadline(time.Now().Add(idle))
		raw, err := readLine(reader, maxFrame)
		if err != nil {
			if ctx.Err() == nil && !isClosed(err) {
				m.logger.Info("link closed", "peer", p.name, "direction", direction, "error", err)
			}
			return
		}
		w.heard.Store(time.Now().UnixNano())
		trimmed := bytes.TrimSpace(raw)
		switch string(trimmed) {
		case "{\"ping\"}", "{\"pong\"}", "":
		default:
			if urgent != nil && urgent.OnPeerUrgent(p.name, trimmed) {
				continue
			}
			if frames != nil {
				select {
				case frames <- trimmed:
				case <-ctx.Done():
					return
				}
			} else {
				m.logger.Debug("unhandled frame", "peer", p.name, "bytes", len(raw))
			}
		}
	}
}

// dropLink tears down a connection that just died, if it is still the
// registered one, and wakes the dial loop.
//
// The handler hears of it on the peer's event goroutine, never here: it can
// wait on store locks held by a long scan, and the status snapshot and the
// next registration both need p.mu in the meantime.
func (m *Manager) dropLink(p *peerState, c net.Conn, w *linkWriter) {
	c.Close()
	p.mu.Lock()
	if p.conn == c {
		p.conn = nil
		p.attempt++
		p.state = stateBackoff
		p.since = time.Now().UTC()
		p.lastErr = "connection closed"
		close(p.linkReset)
		p.linkReset = make(chan struct{})
		m.postLocked(p, linkEvent{connected: false, c: c, w: w})
	}
	p.mu.Unlock()
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
	m.closing = true
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
