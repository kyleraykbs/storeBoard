// Package server runs the storeboard daemon's local API: a versioned status
// document on a unix socket, nothing else yet.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/gatehouse"
	"codeberg.org/kyleraykbs/storeboard/internal/statelock"
	"codeberg.org/kyleraykbs/storeboard/internal/syncer"
	"codeberg.org/kyleraykbs/storeboard/internal/transport"
	"codeberg.org/kyleraykbs/storeboard/internal/version"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// socketMode is deliberately world-writable: the socket only answers the
// status document today, and the NixOS CLI runs as an arbitrary user while
// the system service runs as its own user. DESIGN.md pins the invariant that
// no privileged verb ships while this holds.
const socketMode = 0o666

// maxBody bounds every JSON request body. The largest legitimate one is a
// send or restore naming two paths; the socket is open to every local user,
// and an unbounded decode would let any of them make the daemon buffer
// whatever they cared to stream at it.
const maxBody = 1 << 20

// Connection deadlines. There is deliberately no read or write timeout on the
// request as a whole: a send, a restore or a takeover legitimately runs for
// as long as the bytes take. What is bounded is a client that connects and
// never finishes its headers, or parks an idle keep-alive connection forever.
const (
	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 2 * time.Minute
)

// Server is the daemon: one unix socket, one document, the peer wire, and
// the sync engine behind it.
type Server struct {
	cfg       *config.Config
	logger    *slog.Logger
	startedAt time.Time
	http      *http.Server
	listener  net.Listener
	transport *transport.Manager
	engine    *syncer.Engine
	lock      *statelock.Lock

	// run is the engine's lifetime: Close cancels it and waits for engine.Run
	// to return before closing the engine, because the scanners and the link
	// reconciler it runs are still writing journals and paths until then.
	mu        sync.Mutex
	closed    bool
	cancelRun context.CancelFunc
	engineRun chan struct{}

	// restart is closed when a program requested a new store: the daemon
	// then builds itself again with the store merged in (cmd/storeboardd).
	restart     chan struct{}
	restartOnce sync.Once
}

// New prepares the state and socket directories, then binds the socket. A
// live daemon already on it is an error; a stale socket file left by a crash
// is taken over; anything else at that path refuses to start.
func New(cfg *config.Config, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	// The socket's directory must exist; for the XDG-runtime default it
	// does, and for the data-dir fallback this is among the daemon's first
	// writes. The data dir itself the daemon owns outright (mode 0700).
	if err := os.MkdirAll(filepath.Dir(cfg.Socket), 0o700); err != nil {
		return nil, fmt.Errorf("create socket directory: %w", err)
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	// The state is single-owner, and that is decided before the socket is
	// touched: a second daemon on the same data directory must not even
	// take over a socket the first one is about to answer on.
	lock, err := statelock.Acquire(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			lock.Release()
		}
	}()

	if fi, err := os.Lstat(cfg.Socket); err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("%s exists and is not a socket", cfg.Socket)
		}
		conn, err := net.Dial("unix", cfg.Socket)
		if err == nil {
			conn.Close()
			return nil, fmt.Errorf("storeboardd already listening on %s", cfg.Socket)
		}
		// Only a refused connection proves nobody is there: the socket file
		// outlived a crashed daemon. Anything else — EACCES, a full backlog
		// (EAGAIN), a timeout — can be a live daemon that simply did not
		// answer this dial, and unlinking its socket would strand it,
		// running and unreachable, until a restart.
		if !errors.Is(err, syscall.ECONNREFUSED) && !errors.Is(err, syscall.ENOENT) {
			return nil, fmt.Errorf("%s exists and cannot be probed (%v); not taking it over", cfg.Socket, err)
		}
		if err := os.Remove(cfg.Socket); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("remove stale socket: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("inspect %s: %w", cfg.Socket, err)
	}

	ln, err := net.Listen("unix", cfg.Socket)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", cfg.Socket, err)
	}
	if err := os.Chmod(cfg.Socket, socketMode); err != nil {
		ln.Close()
		return nil, fmt.Errorf("chmod %s: %w", cfg.Socket, err)
	}

	engine, err := syncer.New(cfg, logger)
	if err != nil {
		ln.Close()
		return nil, fmt.Errorf("sync engine: %w", err)
	}
	ok = true

	s := &Server{
		restart:   make(chan struct{}),
		cfg:       cfg,
		logger:    logger,
		startedAt: time.Now(),
		listener:  ln,
		transport: transport.New(cfg, logger),
		engine:    engine,
		lock:      lock,
	}
	s.transport.SetHandler(engine)
	// Offers that change what this machine carries rebuild it, as a request
	// does.
	engine.SetLearnedChanged(func() { s.restartOnce.Do(func() { close(s.restart) }) })
	// Friends' devices, checked by the local GateHouse agent (DESIGN 12).
	if cfg.Friends != nil {
		sock := gatehouse.Socket()
		s.transport.Friends = gatehouse.Verifier(sock)
		engine.SetFriends(s.transport.Person, gatehouse.People(sock))
	}
	// Peers of another grid: this device signs in there (storeboard login).
	tokens := &gatehouse.Tokens{}
	s.transport.SignIn = func(peer string) (string, error) {
		p, ok := cfg.Peers[peer]
		if !ok || p.SignIn == nil {
			return "", fmt.Errorf("%s needs no sign-in", peer)
		}
		return tokens.Token(p.SignIn.Credentials)
	}
	s.http = &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		// Every request carries the uid of the process on the other end of
		// the socket: the acting verb (takeover) authorizes per caller, so a
		// world-writable socket cannot be used to move somebody else's files
		// (DESIGN invariant 10).
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if uid, pid, ok := peerCred(c); ok {
				ctx = context.WithValue(ctx, peerUIDKey{}, uid)
				return context.WithValue(ctx, peerPIDKey{}, pid)
			}
			return ctx
		},
	}
	return s, nil
}

// peerUIDKey carries the socket peer's uid through the request context.
type peerUIDKey struct{}

// requestUID is the uid that made a request, and whether it could be read.
func requestUID(r *http.Request) (int, bool) {
	uid, ok := r.Context().Value(peerUIDKey{}).(int)
	return uid, ok
}

// handler is the whole API surface. Both errors answer as JSON, so a
// scripted client never has to parse an HTML error page.
func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", s.handleStatus)
	mux.HandleFunc("/v1/space", s.handleSpace)
	mux.HandleFunc("/v1/doctor", s.handleDoctor)
	mux.HandleFunc("/v1/cleanup", s.handleCleanup)
	mux.HandleFunc("/v1/stores", s.handleRequestStore)
	mux.HandleFunc("/v1/stores/", s.handleStore)
	mux.HandleFunc("/v1/offers", s.handleOffers)
	mux.HandleFunc("/v1/subscribe", s.handleSubscribe)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such endpoint"})
	})
	return mux
}

// decodeBody reads a JSON request body into v, bounded by maxBody. An empty
// body is no fields at all (every verb's fields are optional or checked by
// the verb); anything else that does not decode is the caller's mistake and
// is answered 400 here — it must never reach a verb as zero values, where a
// malformed takeover would have meant "the first busy path".
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Body == nil {
		return true
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return true
		}
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": fmt.Sprintf("body larger than %d bytes", maxBody)})
			return false
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("body: %v", err)})
		return false
	}
	if dec.More() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body: trailing data after the JSON value"})
		return false
	}
	return true
}

// handleSpace reports the size of each data category. Space reports are safe
// because a store's sustained retention (trash aging, history pruning) only
// touches one store at a time.
func (s *Server) handleSpace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, s.engine.Space())
}

// handleDoctor is the health report: the status document's numbers, judged.
func (s *Server) handleDoctor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, s.engine.Doctor(s.transport.Snapshot()))
}

// handleCleanup: GET reports what there is to clean up (anyone may look,
// as with space); POST runs one cleanup, which the engine authorizes per
// store.
func (s *Server) handleCleanup(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		st, err := s.engine.CleanupStatus()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, st)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, HEAD, POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	uid, ok := requestUID(r)
	if !ok {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "cannot determine the requesting user"})
		return
	}
	var req api.CleanupRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.RemoveOrphans {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": `removeOrphans is gone: ask for what "orphans"`})
		return
	}
	res, err := s.engine.RunCleanup(req, uid)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleStore serves the store verbs:
//
//	GET /v1/stores/<name>/versions?path=<p>
//	POST /v1/stores/<name>/takeover
//	POST /v1/stores/<name>/restore   {"path": …, "blob"?: …, "at"?: …, "to"?: …}
//	POST /v1/stores/<name>/send      {"source": …, "path"?: …}
//	POST /v1/stores/<name>/scan      scan now, not at the next poll
func (s *Server) handleStore(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/stores/")
	name, verb, ok := strings.Cut(rest, "/")
	if !ok || name == "" || strings.Contains(verb, "/") {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such endpoint; want /v1/stores/<name>/<verb>"})
		return
	}
	switch verb {
	case "takeover":
		s.handleTakeover(w, r, name)
	case "versions":
		s.handleVersions(w, r, name)
	case "restore":
		s.handleRestore(w, r, name)
	case "send":
		s.handleSend(w, r, name)
	case "scan":
		s.handleScan(w, r, name)
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("no such store verb %q", verb)})
	}
}

// handleScan scans a store now. Anyone who can reach the socket may ask:
// a scan only reads what is already there.
func (s *Server) handleScan(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if err := s.engine.ScanSoon(name); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"scan": "requested"})
}

// handleSend copies a local file into a store: the share sheet's path onto
// the mesh, and the same verb the app's engine offers directly. The file the
// caller names is read by the daemon, so it must be a path this machine can
// see — the CLI's own host, which is also the one the daemon runs on — given
// absolute (the daemon's working directory is its data dir), and the
// caller's own file: the engine checks the owner and copies, never moves.
func (s *Server) handleSend(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	uid, ok := requestUID(r)
	if !ok {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "cannot determine the requesting user"})
		return
	}
	var req struct {
		Source string `json:"source"`
		Path   string `json:"path"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Source == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body: source is required"})
		return
	}
	res, err := s.engine.Send(name, uid, req.Source, req.Path)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "result": res})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleTakeover(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	uid, ok := requestUID(r)
	if !ok {
		// No peer credentials (a transport that cannot say): refuse rather
		// than assume root.
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "cannot determine the requesting user"})
		return
	}
	var req struct {
		Path string `json:"path"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	res, err := s.engine.Takeover(name, req.Path, uid)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "result": res})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleVersions lists a file's recorded versions. It reads like status but
// is authorized like the acting verbs: a bare listing names every file a
// store ever held, deleted ones included, and the socket is open to every
// local user — so root or the store's owner, the same rule restore applies to
// the bytes behind the list.
func (s *Server) handleVersions(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	uid, ok := requestUID(r)
	if !ok {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "cannot determine the requesting user"})
		return
	}
	if err := s.engine.CheckOwner(name, uid, gatehouse.Read, "list the versions of"); err != nil {
		code := http.StatusForbidden
		if strings.HasPrefix(err.Error(), "unknown store") {
			code = http.StatusNotFound
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	res, err := s.engine.Versions(name, r.URL.Query().Get("path"))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleRestore(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	uid, ok := requestUID(r)
	if !ok {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "cannot determine the requesting user"})
		return
	}
	var req struct {
		Path string `json:"path"`
		Blob string `json:"blob"`
		At   string `json:"at"`
		To   string `json:"to"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	var at time.Time
	if req.At != "" {
		parsed, err := time.Parse(time.RFC3339, req.At)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("at: %v", err)})
			return
		}
		at = parsed
	}
	res, err := s.engine.Restore(name, uid, req.Path, req.Blob, at, req.To)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "result": res})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	// The method check lives here rather than in a method-aware ServeMux
	// pattern: the catch-all 404 route would win first and mask it.
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	// The config's view builders sort and return non-nil slices: the
	// contract says arrays, never null — a nil slice would marshal to null
	// and break `.stores[]` in jq.
	writeJSON(w, http.StatusOK, api.Status{
		APIVersion:    api.Version,
		Version:       version.Version,
		StartedAt:     s.startedAt.UTC(),
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
		Socket:        s.cfg.Socket,
		DataDir:       s.cfg.DataDir,
		Root:          s.cfg.Root,
		Peer:          s.cfg.Peer,
		Peers:         s.cfg.PeerList(),
		Groups:        s.cfg.GroupList(),
		Stores:        s.cfg.Stores(),
		Connections:   s.transport.Snapshot(),
		Sync:          s.engine.Snapshot(),
	})
}

// Run serves until ctx is cancelled, then shuts down gracefully. The socket
// file itself is removed by Close, which the caller must reach whatever Run
// returns: it is what unmounts streamed stores and stops the engine.
func (s *Server) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("server is closed")
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancelRun = cancel
	done := make(chan struct{})
	s.engineRun = done
	s.mu.Unlock()

	go func() {
		defer close(done)
		s.engine.Run(runCtx)
	}()
	s.transport.Run(runCtx)

	errCh := make(chan error, 1)
	go func() {
		if err := s.http.Serve(s.listener); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	s.logger.Info("storeboardd listening",
		"socket", s.cfg.Socket,
		"version", version.Version,
		"stores", len(s.cfg.Stores()),
	)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// A request still running after the grace period (a long send) is
		// not a reason to skip the rest of shutdown: Close force-closes the
		// connections and tears the engine down either way.
		if err := s.http.Shutdown(shutdownCtx); err != nil {
			s.logger.Warn("http shutdown did not finish; closing connections", "error", err)
		}
		return nil
	}
}

// Close stops frame callbacks before transport and durable state. This keeps
// a bounded sync queue from delaying network teardown. It then waits for
// engine.Run — scanners and link reconciliation write journals and paths until
// it returns — before closing the engine's journals and mounts. Safe to call
// whether or not Run ran, and more than once.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cancel, done := s.cancelRun, s.engineRun
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	s.http.Close()
	// Serve may never have run (a New that is closed straight away), and
	// then http.Close does not know the listener.
	s.listener.Close()
	s.engine.StopFrames()
	s.transport.Close()
	if done != nil {
		<-done
	}
	s.engine.Close()
	err := os.Remove(s.cfg.Socket)
	if err != nil && os.IsNotExist(err) {
		err = nil
	}
	// Last: the socket is gone and the state is closed, so the next daemon
	// may have both.
	s.lock.Release()
	return err
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// peerUID reads the uid of the process on the other end of a unix socket
// connection (Linux SO_PEERCRED). It reports false when the connection
// cannot say — the caller then refuses the request rather than guessing.
func peerUID(c net.Conn) (int, bool) {
	uid, _, ok := peerCred(c)
	return uid, ok
}

// peerCred is the uid and pid of the process on the other end.
func peerCred(c net.Conn) (int, int, bool) {
	sc, ok := c.(syscall.Conn)
	if !ok {
		return -1, -1, false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return -1, -1, false
	}
	uid, pid := -1, -1
	var readErr error
	if err := raw.Control(func(fd uintptr) {
		cred, cerr := syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
		if cerr != nil {
			readErr = cerr
			return
		}
		uid, pid = int(cred.Uid), int(cred.Pid)
	}); err != nil {
		return -1, -1, false
	}
	if readErr != nil || uid < 0 {
		return -1, -1, false
	}
	return uid, pid, true
}
