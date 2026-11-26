package syncer

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/transport"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// Doctor looks the daemon over and says what is wrong, in words: the status
// document has the numbers, but knowing which of them matter - a mount that
// died, a queue no holder will drain, a peer whose clock decides conflicts
// wrongly - took reading the code. conns is the transport's view of the peers.
func (e *Engine) Doctor(conns []api.Connection) api.Doctor {
	var checks []api.Check
	add := func(name, status, detail, hint string) {
		checks = append(checks, api.Check{Name: name, Status: status, Detail: detail, Hint: hint})
	}

	e.doctorDisk(add)
	e.doctorPeers(conns, add)
	for _, fs := range e.stores {
		e.doctorStore(fs, add)
	}
	e.doctorTransfers(add)

	out := api.Doctor{APIVersion: api.Version, Status: api.CheckOK, Checks: checks}
	for _, c := range checks {
		if checkRank(c.Status) > checkRank(out.Status) {
			out.Status = c.Status
		}
	}
	return out
}

func checkRank(s string) int {
	switch s {
	case api.CheckFail:
		return 2
	case api.CheckWarn:
		return 1
	}
	return 0
}

type addCheck func(name, status, detail, hint string)

// Free space under the data directory: journals, the content store, the
// offline queue and every incoming transfer all live there.
func (e *Engine) doctorDisk(add addCheck) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(e.dataDir, &st); err != nil {
		add("disk", api.CheckWarn, fmt.Sprintf("cannot read free space of %s: %v", e.dataDir, err), "")
		return
	}
	free := int64(st.Bavail) * int64(st.Bsize)
	total := int64(st.Blocks) * int64(st.Bsize)
	detail := fmt.Sprintf("%s free of %s on the data directory's filesystem (%s)", humanSize(free), humanSize(total), e.dataDir)
	switch {
	case free < 1<<30:
		add("disk", api.CheckFail, detail, "incoming files and journal writes will start failing; free some space")
	case total > 0 && free*20 < total || free < 5<<30:
		add("disk", api.CheckWarn, detail, "a large incoming file needs its full size free while it arrives")
	default:
		add("disk", api.CheckOK, detail, "")
	}
}

// Every peer this machine shares a store with: reachable, on the same build,
// and with a clock close enough to this one's.
func (e *Engine) doctorPeers(conns []api.Connection, add addCheck) {
	shared := map[string]bool{}
	for _, fs := range e.stores {
		for _, p := range e.cfg.Groups[fs.group].Peers {
			if p != e.me {
				shared[p] = true
			}
		}
	}
	byPeer := map[string]api.Connection{}
	for _, c := range conns {
		byPeer[c.Peer] = c
	}
	names := make([]string, 0, len(shared))
	for p := range shared {
		names = append(names, p)
	}
	sort.Strings(names)
	for _, p := range names {
		name := "peer " + p
		c, ok := byPeer[p]
		if !ok || c.State != "connected" {
			detail := "not connected"
			if ok {
				detail = fmt.Sprintf("%s since %s", c.State, c.Since.Local().Format(time.DateTime))
				if c.Error != "" {
					detail += ": " + c.Error
				}
			}
			add(name, api.CheckWarn, detail, "stores shared with it only sync once it is back; check it is running and reachable at its endpoints")
			continue
		}
		skew := time.Duration(c.ClockSkewMs) * time.Millisecond
		if skew > transport.ClockSkewWarn || skew < -transport.ClockSkewWarn {
			dir := "ahead of"
			if skew < 0 {
				dir, skew = "behind", -skew
			}
			add(name, api.CheckWarn, fmt.Sprintf("clock is %s %s this machine's", skew.Round(time.Millisecond), dir),
				"conflicting edits are decided by modification time, so the faster clock wins them; enable NTP on both machines")
			continue
		}
		detail := fmt.Sprintf("connected (%s)", c.Endpoint)
		if c.Version != "" {
			detail += ", version " + c.Version
		}
		add(name, api.CheckOK, detail, "")
	}
}

func (e *Engine) doctorStore(fs *StoreState, add addCheck) {
	name := "store " + fs.name
	snap := fs.publish()
	var problems []string
	status := api.CheckOK
	hint := ""
	raise := func(s, problem, h string) {
		if checkRank(s) > checkRank(status) {
			status = s
			hint = h
		} else if hint == "" {
			hint = h
		}
		problems = append(problems, problem)
	}

	if fs.rootEmpty.Load() {
		raise(api.CheckFail, fmt.Sprintf("the folder %s is empty but this machine held its files", fs.root),
			"if a disk is not mounted, mount it; deletions are held back until the folder is back")
	}
	if fs.modeNote != "" {
		raise(api.CheckFail, "configured local but still streamed: "+fs.modeNote,
			"free space (or keep the store streamed in the config), then restart the daemon")
	}
	if fs.migrationBlocked() {
		fs.migr.mu.Lock()
		why := fs.migr.blocked
		fs.migr.mu.Unlock()
		raise(api.CheckFail, "not mounted: "+why, "the local files are untouched; resolve it and restart the daemon")
	} else if n, bytes := fs.migr.pending(); n > 0 {
		h := "they stay readable meanwhile; each moves to the trash once a holder has it"
		if snap.State != stateAttached {
			h = "waiting for a holder to connect; the files stay readable meanwhile"
		}
		raise(api.CheckWarn, fmt.Sprintf("switching to stream: %d local files (%s) still to hand to a holder", n, humanSize(bytes)), h)
	} else if left := e.stagingLeftovers(fs); left != "" {
		raise(api.CheckWarn, "switched to stream; items the migration did not recognise were left in "+left, "look through them and move or delete what you want")
	}
	// Streamed: its files live in full on the holders (DESIGN: holders).
	if v, ok := e.cfg.StoreView(fs.name); ok && fs.stream && len(v.Holders) > 0 {
		e.mu.Lock()
		up := slices.ContainsFunc(v.Holders, func(h string) bool { _, ok := e.sends[h]; return ok })
		e.mu.Unlock()
		if !up {
			raise(api.CheckWarn, "no holder connected ("+strings.Join(v.Holders, ", ")+"): files not already here can't be read, and changes queue until one is",
				"check that "+strings.Join(v.Holders, " or ")+" is up and reachable")
		}
	}
	if fs.stream {
		if !fs.mounted() && !fs.migrationBlocked() {
			raise(api.CheckFail, "the streamed mount is not up", "the folder shows nothing; check the daemon log for the mount error (is /dev/fuse available?)")
		} else if fs.scanMu.TryLock() {
			// Never wait on the scan lock: a doctor that hangs behind the
			// problem it is meant to report is no use.
			m := fs.mount
			fs.scanMu.Unlock()
			if m != nil && !m.Alive() {
				raise(api.CheckFail, "the streamed mount has died", "it is remounted on the next link check, within a minute")
			}
		}
		fs.queueMu.Lock()
		ops, bytes := len(fs.queue), fs.queuedBytes
		fs.queueMu.Unlock()
		if ops > 0 {
			s := api.CheckOK
			h := ""
			if snap.State != stateAttached {
				s, h = api.CheckWarn, "the writes stay here until a holder connects"
			}
			raise(s, fmt.Sprintf("%d writes (%s) queued for a holder", ops, humanSize(bytes)), h)
		}
	}
	if fs.stream && snap.State != stateAttached {
		raise(api.CheckWarn, "no holder is connected", "reads fail until a peer holding the store's content connects")
	}
	if !fs.stream && !snap.LastScan.IsZero() && time.Since(snap.LastScan) > 3*e.scanEvery+time.Minute && !snap.Scanning {
		raise(api.CheckWarn, fmt.Sprintf("last scanned %s ago", time.Since(snap.LastScan).Round(time.Second)), "local changes are not being picked up; check the daemon log")
	}
	if snap.Conflicts > 0 {
		raise(api.CheckWarn, fmt.Sprintf("%d conflicts since the daemon started", snap.Conflicts),
			"both versions were kept; look for *.conflict-* files and keep the one you want")
	}
	if snap.Pending > 0 && !fs.stream {
		raise(api.CheckOK, fmt.Sprintf("%d files still arriving", snap.Pending), "")
	}

	detail := fmt.Sprintf("%s, %d files", snap.State, snap.Local)
	if fs.stream {
		detail = fmt.Sprintf("%s, %d entries (streamed)", snap.State, snap.Entries)
	}
	if len(problems) > 0 {
		detail += "; " + strings.Join(problems, "; ")
	}
	add(name, status, detail, hint)
}

// Transfers in both directions, and any incoming one that has gone quiet.
func (e *Engine) doctorTransfers(add addCheck) {
	active, queued := e.sendingCount()
	wanted := e.wantCount()
	var stalled []string
	e.mu.Lock()
	receiving := len(e.incoming)
	for _, inc := range e.incoming {
		if time.Since(inc.last) > wantRefresh {
			stalled = append(stalled, inc.store+"/"+inc.path)
		}
	}
	e.mu.Unlock()
	slices.Sort(stalled)
	detail := fmt.Sprintf("receiving %d, waiting for %d more; sending %d, %d queued", receiving, max(wanted-receiving, 0), active, queued)
	if len(stalled) > 0 {
		shown := stalled
		if len(shown) > 5 {
			shown = shown[:5]
		}
		add("transfers", api.CheckWarn, fmt.Sprintf("%s; %d stalled: %s", detail, len(stalled), strings.Join(shown, ", ")),
			"a stalled transfer is asked for again on the next index exchange; a peer that dropped mid-file causes this")
		return
	}
	add("transfers", api.CheckOK, detail, "")
}

func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
