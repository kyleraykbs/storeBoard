package server

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
)

// peerPIDKey carries the socket peer's pid through the request context.
type peerPIDKey struct{}

// Restart is closed when a program has requested a new store; the daemon
// then builds itself again with it merged in.
func (s *Server) Restart() <-chan struct{} { return s.restart }

// RequestStoreParams ask for a store (POST /v1/stores).
type RequestStoreParams struct {
	Name        string              `json:"name"`
	Group       string              `json:"group"`
	Sync        string              `json:"sync,omitempty"`
	Permissions *config.Permissions `json:"permissions,omitempty"`
	NoBackup    bool                `json:"noBackup,omitempty"`
}

// handleRequestStore lets a program ask for its own store, synced within a
// group the config already declares (DESIGN: requested stores):
//
//   - A systemd service may only ask for names in its own namespace:
//     strongbox.service gets "strongbox.*". Root outside a service may ask
//     for any name; nobody else may ask.
//   - The group must exist and include this machine: a program never picks
//     peers.
//   - The config wins: a declared store of that name is the answer, as is
//     one requested before. A new one is recorded and the daemon rebuilds
//     itself to carry it.
func (s *Server) handleRequestStore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	uid, ok := requestUID(r)
	pid, _ := r.Context().Value(peerPIDKey{}).(int)
	if !ok {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "cannot determine the requesting process"})
		return
	}
	var p RequestStoreParams
	if !decodeBody(w, r, &p) {
		return
	}
	unit := unitOf(pid)
	by := unit
	switch {
	case unit != "":
		ns := strings.TrimSuffix(unit, ".service") + "."
		if !strings.HasSuffix(unit, ".service") || !strings.HasPrefix(p.Name, ns) || len(p.Name) == len(ns) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": fmt.Sprintf("%s may only request stores named %s*", unit, ns)})
			return
		}
	case uid == 0:
		by = "root"
	default:
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "only a system service or root may request a store"})
		return
	}
	path := filepath.Join(s.cfg.Root, p.Name)
	if _, ok := s.cfg.StoreTree[p.Name]; ok {
		how := "declared"
		if s.cfg.RequestedBy[p.Name] != "" {
			how = "requested"
		}
		writeJSON(w, http.StatusOK, map[string]any{"name": p.Name, "path": path, "store": how})
		return
	}
	g, ok := s.cfg.Groups[p.Group]
	if !ok || !containsPeer(g.Peers, s.cfg.Peer) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("group %q is not declared with this machine in it", p.Group)})
		return
	}
	req, err := config.LoadRequested(s.cfg.DataDir)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	req[p.Name] = config.Requested{Group: p.Group, Sync: p.Sync, Permissions: p.Permissions, NoBackup: p.NoBackup, By: by, At: time.Now().UTC()}
	// The merged config must be one storeboard would accept from a file.
	if merged, _ := s.cfg.WithRequested(map[string]config.Requested{p.Name: req[p.Name]}); merged.Validate() != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": merged.Validate().Error()})
		return
	}
	if err := config.SaveRequested(s.cfg.DataDir, req); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.logger.Info("store requested", "store", p.Name, "group", p.Group, "by", by)
	writeJSON(w, http.StatusCreated, map[string]any{"name": p.Name, "path": path, "store": "requested"})
	s.restartOnce.Do(func() { close(s.restart) })
}

func containsPeer(peers []string, me string) bool {
	for _, p := range peers {
		if p == me {
			return true
		}
	}
	return false
}

// unitOf is the system service a process runs in, from its cgroup, or ""
// for anything else (a login shell, the CLI, a user's own services, which
// any user can start and so name nothing).
var unitOf = unitOfPID

func unitOfPID(pid int) string {
	if pid <= 0 {
		return ""
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		path, ok := strings.CutPrefix(line, "0::/system.slice/")
		if !ok {
			continue
		}
		if unit, _, _ := strings.Cut(path, "/"); strings.HasSuffix(unit, ".service") {
			return unit
		}
	}
	return ""
}
