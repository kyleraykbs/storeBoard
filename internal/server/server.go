// Package server runs the storeboard daemon's local API: a versioned status
// document on a unix socket, nothing else yet.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
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

	if fi, err := os.Lstat(cfg.Socket); err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("%s exists and is not a socket", cfg.Socket)
		}
		if conn, err := net.Dial("unix", cfg.Socket); err == nil {
			conn.Close()
			return nil, fmt.Errorf("storeboardd already listening on %s", cfg.Socket)
		}
		// Nobody answers: a leftover socket file from a crashed daemon.
		if err := os.Remove(cfg.Socket); err != nil {
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

	s := &Server{
		cfg:       cfg,
		logger:    logger,
		startedAt: time.Now(),
		listener:  ln,
		transport: transport.New(cfg, logger),
		engine:    engine,
	}
	s.transport.SetHandler(engine)
	s.http = &http.Server{
		Handler: s.handler(),
		// Every request carries the uid of the process on the other end of
		// the socket: the acting verb (takeover) authorizes per caller, so a
		// world-writable socket cannot be used to move somebody else's files
		// (DESIGN invariant 10).
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if uid, ok := peerUID(c); ok {
				return context.WithValue(ctx, peerUIDKey{}, uid)
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
	mux.HandleFunc("/v1/folders/", s.handleFolder)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such endpoint"})
	})
	return mux
}

// handleFolder serves the folder verbs: POST /v1/folders/<name>/takeover.
func (s *Server) handleFolder(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/folders/")
	name, verb, ok := strings.Cut(rest, "/")
	if !ok || name == "" || strings.Contains(verb, "/") {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such endpoint; want /v1/folders/<name>/takeover"})
		return
	}
	if verb != "takeover" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("no such folder verb %q", verb)})
		return
	}
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
	res, err := s.engine.Takeover(name, uid)
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
	// and break `.folders[]` in jq.
	writeJSON(w, http.StatusOK, api.Status{
		APIVersion:    api.Version,
		Version:       version.Version,
		StartedAt:     s.startedAt.UTC(),
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
		Socket:        s.cfg.Socket,
		DataDir:       s.cfg.DataDir,
		Peer:          s.cfg.Peer,
		Peers:         s.cfg.PeerList(),
		Groups:        s.cfg.GroupList(),
		Folders:       s.cfg.Folders(),
		Connections:   s.transport.Snapshot(),
		Sync:          s.engine.Snapshot(),
	})
}

// Run serves until ctx is cancelled, then shuts down gracefully. The socket
// file itself is removed by Close.
func (s *Server) Run(ctx context.Context) error {
	go s.engine.Run(ctx)
	s.transport.Run(ctx)

	errCh := make(chan error, 1)
	go func() {
		if err := s.http.Serve(s.listener); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	s.logger.Info("storeboardd listening",
		"socket", s.cfg.Socket,
		"version", version.Version,
		"folders", len(s.cfg.Folders()),
	)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.http.Shutdown(shutdownCtx)
	}
}

// Close stops the server and removes the socket file.
func (s *Server) Close() error {
	s.engine.Close()
	s.transport.Close()
	s.http.Close()
	if err := os.Remove(s.cfg.Socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
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
	sc, ok := c.(syscall.Conn)
	if !ok {
		return -1, false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return -1, false
	}
	uid := -1
	var readErr error
	if err := raw.Control(func(fd uintptr) {
		cred, cerr := syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
		if cerr != nil {
			readErr = cerr
			return
		}
		uid = int(cred.Uid)
	}); err != nil {
		return -1, false
	}
	if readErr != nil || uid < 0 {
		return -1, false
	}
	return uid, true
}
