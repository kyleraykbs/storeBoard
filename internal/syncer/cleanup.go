package syncer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// Explicit cleanup. The daemon's own retention (pruneOnce, Collect) only ever
// drops what its windows say may go. These are the things a person decides:
//
//   - trash: empty a store's trash now, or what in it is older than a cutoff;
//   - history: drop recorded versions - the old ones, or, with current, also
//     the backup copy of each file as it is now (the bytes stay on disk in
//     the file itself; only the safety copy goes);
//   - orphans: forget a store the config no longer names - its journal,
//     history, trash and bookkeeping. The folder under the root is the
//     user's and is left where it is.
//
// Each is planned first (what it would remove and the content blobs that
// would then be unreferenced) and the plan is what a dry run reports; the
// real run applies the plan and then collects.

// Cleanup kinds.
const (
	CleanupTrash   = "trash"
	CleanupHistory = "history"
	CleanupOrphans = "orphans"
)

// cleanupPlan is what an explicit cleanup removes, in the form keepSet needs
// to answer "and which blobs does that leave unreferenced".
type cleanupPlan struct {
	// history: stores whose versions are filtered by dropVersion.
	history map[string]bool
	cutoff  time.Time // zero: any age
	current bool
	// orphans whose journal and history no longer count as references.
	orphans map[string]bool
}

func (p *cleanupPlan) dropsHistory(store string) bool {
	return p != nil && p.history[store]
}

// dropMask marks which of a file's versions (chronological) a history
// cleanup drops: everything recorded at or before cutoff (zero: any time),
// except the current version of a file that still exists unless current is
// set.
func dropMask(versions []Version, cutoff time.Time, current bool) []bool {
	out := make([]bool, len(versions))
	last := len(versions) - 1
	for i, v := range versions {
		if !cutoff.IsZero() && time.Unix(0, v.Time).After(cutoff) {
			continue
		}
		if i == last && !v.Deleted && !current {
			continue
		}
		out[i] = true
	}
	return out
}

// Drop removes the versions dropMask selects and reports how many. A dry run
// counts without changing anything.
func (h *History) Drop(cutoff time.Time, current, dry bool) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, fmt.Errorf("history %s is closed", h.path)
	}
	dropped := 0
	kept := map[string][]Version{}
	for path, versions := range h.byPath {
		mask := dropMask(versions, cutoff, current)
		var out []Version
		for i, v := range versions {
			if mask[i] {
				dropped++
				continue
			}
			out = append(out, v)
		}
		if len(out) > 0 {
			kept[path] = out
		}
	}
	if dry || dropped == 0 {
		return dropped, nil
	}
	prev := h.byPath
	h.byPath = kept
	if err := h.rewriteLocked(); err != nil {
		// Nothing reached disk; the log and memory stay as they were, so
		// the collector cannot free blobs the file still names.
		h.byPath = prev
		return 0, err
	}
	h.pruned += dropped
	return dropped, nil
}

// readHistoryFile reads a history log grouped by path, chronological.
func readHistoryFile(path string) (map[string][]Version, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string][]Version{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		var rec struct {
			Path    string  `json:"path"`
			Version Version `json:"version"`
		}
		if json.Unmarshal(sc.Bytes(), &rec) != nil {
			continue
		}
		out[rec.Path] = append(out[rec.Path], rec.Version)
	}
	return out, sc.Err()
}

// refsFromHistoryPlanned adds the blobs a history file still names once
// the plan's versions are dropped.
func refsFromHistoryPlanned(path string, refs map[string]bool, plan *cleanupPlan) error {
	byPath, err := readHistoryFile(path)
	if err != nil {
		return err
	}
	for _, versions := range byPath {
		mask := dropMask(versions, plan.cutoff, plan.current)
		for i, v := range versions {
			if !mask[i] && v.Blob != "" {
				refs[v.Blob] = true
			}
		}
	}
	return nil
}

// emptyTrash removes the files in a store's trash that arrived at or before
// cutoff, and the directories that leaves empty. A dry run only counts.
func (e *Engine) emptyTrash(store string, cutoff time.Time, dry bool) (files int, bytes int64) {
	trash := filepath.Join(e.dataDir, "trash", store)
	_ = filepath.WalkDir(trash, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		fi, serr := d.Info()
		if serr != nil || trashTime(fi).After(cutoff) {
			return nil
		}
		if dry {
			files++
			bytes += fi.Size()
			return nil
		}
		if os.Remove(path) == nil {
			files++
			bytes += fi.Size()
		}
		return nil
	})
	if !dry && files > 0 {
		// A delete in flight makes its directory and then moves the file
		// in; holding the trash lock keeps this from removing the directory
		// in between.
		e.trashMu.Lock()
		entries, _ := os.ReadDir(trash)
		for _, ent := range entries {
			if ent.IsDir() {
				removeEmptyDirs(filepath.Join(trash, ent.Name()))
			}
		}
		e.trashMu.Unlock()
	}
	return files, bytes
}

// orphanFiles lists what storeboard itself keeps for a store: everything
// under the data directory named for it. The folder under the root is not
// in it.
func (e *Engine) orphanFiles(id string) []string {
	d := e.dataDir
	return []string{
		filepath.Join(d, "journal", id+".ndjson"),
		filepath.Join(d, "journal", id+".ndjson.corrupt"),
		filepath.Join(d, "journal", id),
		filepath.Join(d, "history", id+".ndjson"),
		filepath.Join(d, "history", id+".ndjson.corrupt"),
		filepath.Join(d, "trash", id),
		queueBytesDir(d, id),
		queueIndexPath(d, id),
		modePath(d, id),
		filepath.Join(d, "roots", id),
		stagingDir(d, id),
		manifestPath(d, id),
	}
}

// unsentWrites counts files waiting in an orphan's queue: writes made while
// it was streamed that no holder acknowledged. They may be the only copy.
func (e *Engine) unsentWrites(id string) int {
	n := 0
	_ = filepath.WalkDir(queueBytesDir(e.dataDir, id), func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return nil
	})
	return n
}

func validStoreName(id string) bool {
	return id != "" && id != "." && id != ".." && !strings.ContainsAny(id, `/\`) && !strings.HasPrefix(id, ".")
}

// RunCleanup is the cleanup verb. uid is the caller: routine cleanup (what
// the timers do anyway) is anyone's; emptying a trash or dropping history
// needs the store's owner or root; forgetting an orphan store, which has no
// owner left in the config, needs root.
func (e *Engine) RunCleanup(req api.CleanupRequest, uid int) (api.CleanupResult, error) {
	res := api.CleanupResult{APIVersion: api.Version, What: req.What, DryRun: req.DryRun, OrphanStores: []string{}, Items: []api.CleanupItem{}}
	var cutoff time.Time
	if req.OlderThan != "" {
		d, err := config.ParseKeep(req.OlderThan)
		if err != nil {
			return res, fmt.Errorf("older-than: %w", err)
		}
		cutoff = time.Now().Add(-d)
	}

	switch req.What {
	case "":
		return res, fmt.Errorf("name what to clean: trash, history or orphans (the daemon expires both by their windows on its own)")

	case CleanupTrash, CleanupHistory:
		if req.What == CleanupTrash && (req.Current || req.Force) {
			return res, fmt.Errorf("trash cleanup takes no --current or --force")
		}
		if req.What == CleanupHistory && req.Force {
			return res, fmt.Errorf("history cleanup takes no --force")
		}
		stores, err := e.cleanupStores(req.Stores, uid, req.What)
		if err != nil {
			return res, err
		}
		plan := &cleanupPlan{history: map[string]bool{}, cutoff: cutoff, current: req.Current}
		for _, fs := range stores {
			item := api.CleanupItem{Store: fs.name, What: req.What}
			if req.What == CleanupTrash {
				until := cutoff
				if until.IsZero() {
					until = time.Now().Add(time.Hour) // all of it, clock skew included
				}
				item.Files, item.Bytes = e.emptyTrash(fs.name, until, req.DryRun)
			} else {
				n, err := e.dropHistory(fs, cutoff, req.Current, req.DryRun)
				if err != nil {
					return res, fmt.Errorf("%s: %w", fs.name, err)
				}
				item.Files = n
				plan.history[fs.name] = true
			}
			res.TrashFiles += item.Files * boolInt(req.What == CleanupTrash)
			res.TrashBytes += item.Bytes
			res.Versions += item.Files * boolInt(req.What == CleanupHistory)
			res.Items = append(res.Items, item)
		}
		return e.finishCleanup(res, plan, req.DryRun)

	case CleanupOrphans:
		if uid != 0 {
			return res, fmt.Errorf("forgetting a store no longer in the config needs root: it has no owner left to ask")
		}
		if req.OlderThan != "" || req.Current {
			return res, fmt.Errorf("orphans cleanup takes no --older-than or --current")
		}
		orphans := e.orphanStoreIDs()
		ids := req.Stores
		if len(ids) == 0 {
			ids = orphans
		}
		known := map[string]bool{}
		for _, id := range orphans {
			known[id] = true
		}
		plan := &cleanupPlan{orphans: map[string]bool{}}
		for _, id := range ids {
			if _, configured := e.byName[id]; configured {
				return res, fmt.Errorf("store %q is in the config; only stores the config no longer names can be forgotten", id)
			}
			if !validStoreName(id) || !known[id] {
				return res, fmt.Errorf("no leftover store %q (storeboard space lists them)", id)
			}
			if n := e.unsentWrites(id); n > 0 && !req.Force {
				return res, fmt.Errorf("store %q has %d unsent writes in %s that may be the only copy; copy them out, or pass --force", id, n, queueBytesDir(e.dataDir, id))
			}
		}
		for _, id := range ids {
			item := api.CleanupItem{Store: id, What: CleanupOrphans}
			for _, p := range e.orphanFiles(id) {
				b, f := treeSize(p)
				if _, err := os.Lstat(p); err != nil {
					continue
				}
				if !req.DryRun {
					if err := os.RemoveAll(p); err != nil {
						return res, fmt.Errorf("%s: %w", id, err)
					}
				}
				item.Bytes += b
				item.Files += int(f)
			}
			root := filepath.Join(e.cfg.Root, id)
			if b, f := treeSize(root); f > 0 {
				item.Note = fmt.Sprintf("folder %s left in place (%d files, %s): it is yours to keep or delete", root, f, humanSize(b))
			} else if !req.DryRun {
				removeEmptyDirs(root)
			}
			plan.orphans[id] = true
			res.OrphanStores = append(res.OrphanStores, id)
			res.OrphanBytes += item.Bytes
			res.Items = append(res.Items, item)
			if !req.DryRun {
				e.logger.Info("forgot a store no longer in the config", "store", id, "bytes", item.Bytes)
			}
		}
		return e.finishCleanup(res, plan, req.DryRun)
	}
	return res, fmt.Errorf("unknown cleanup %q: want trash, history or orphans", req.What)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// cleanupStores resolves the named stores (none: every store) and checks the
// caller may clean each.
func (e *Engine) cleanupStores(names []string, uid int, what string) ([]*StoreState, error) {
	var out []*StoreState
	if len(names) == 0 {
		for _, fs := range e.stores {
			if err := e.checkOwner(fs, uid, "cleanup "+what); err == nil {
				out = append(out, fs)
			}
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("none of the stores is yours: name one, or run as root")
		}
		return out, nil
	}
	for _, name := range names {
		fs := e.byName[name]
		if fs == nil {
			return nil, fmt.Errorf("unknown store %q", name)
		}
		if err := e.checkOwner(fs, uid, "cleanup "+what); err != nil {
			return nil, err
		}
		out = append(out, fs)
	}
	return out, nil
}

// dropHistory drops a store's versions, in memory and on disk, or from the
// file alone when backup is off for it now and the log is a leftover.
func (e *Engine) dropHistory(fs *StoreState, cutoff time.Time, current, dry bool) (int, error) {
	if fs.hist != nil {
		n, err := fs.hist.Drop(cutoff, current, dry)
		if err == nil && n > 0 && !dry {
			e.logger.Info("dropped recorded versions", "store", fs.name, "versions", n, "current", current)
		}
		return n, err
	}
	path := filepath.Join(e.dataDir, "history", fs.name+".ndjson")
	if _, err := os.Stat(path); err != nil {
		return 0, nil
	}
	h, err := OpenHistory(path, DefaultKeep)
	if err != nil {
		return 0, err
	}
	defer h.Close()
	return h.Drop(cutoff, current, dry)
}

// finishCleanup reports the blobs the plan leaves unreferenced: counted for
// a dry run, collected for a real one.
func (e *Engine) finishCleanup(res api.CleanupResult, plan *cleanupPlan, dry bool) (api.CleanupResult, error) {
	if dry {
		keep, err := e.keepSetFor(plan)
		if err != nil {
			return res, err
		}
		res.Blobs, res.BlobBytes = e.unreferenced(keep)
		return res, nil
	}
	res.Blobs, res.BlobBytes = e.Collect()
	return res, nil
}

// unreferenced counts the blobs the collector would free given keep.
func (e *Engine) unreferenced(keep map[string]bool) (n int, bytes int64) {
	storeDir := filepath.Join(e.dataDir, "store")
	for _, ab := range listDir(storeDir) {
		for _, cd := range listDir(filepath.Join(storeDir, ab)) {
			dir := filepath.Join(storeDir, ab, cd)
			for _, sum := range listDir(dir) {
				if keep[sum] {
					continue
				}
				fi, err := os.Stat(filepath.Join(dir, sum))
				if err != nil || fi.IsDir() || time.Since(fi.ModTime()) < recentBlob {
					continue
				}
				n++
				bytes += fi.Size()
			}
		}
	}
	return n, bytes
}

// CleanupStatus reports what is there to clean up and changes nothing: each
// store's trash and recorded versions, the stores no longer in the config,
// and content nothing references. Content bytes are counted for the one
// thing that alone keeps them (a blob two of them name would survive either
// cleanup alone, and is not promised to either).
func (e *Engine) CleanupStatus() (api.CleanupStatus, error) {
	st := api.CleanupStatus{APIVersion: api.Version, Stores: []api.CleanupStoreStatus{}, Orphans: []api.CleanupOrphanStatus{}}

	// Blob sizes, once.
	sizes := map[string]int64{}
	storeDir := filepath.Join(e.dataDir, "store")
	for _, ab := range listDir(storeDir) {
		for _, cd := range listDir(filepath.Join(storeDir, ab)) {
			dir := filepath.Join(storeDir, ab, cd)
			for _, sum := range listDir(dir) {
				if fi, err := os.Stat(filepath.Join(dir, sum)); err == nil && !fi.IsDir() {
					sizes[sum] = fi.Size()
				}
			}
		}
	}

	// base: blobs that are the only copy of a live file here. No cleanup
	// frees them.
	base := map[string]bool{}
	for _, fs := range e.stores {
		for _, ent := range fs.db.All() {
			if ent.Blob != "" && !ent.Dir && !e.materialised(fs, ent) {
				base[ent.Blob] = true
			}
		}
	}

	// Groups: each store's old versions, its current copies, each orphan.
	var groups []map[string]bool
	owners := map[string][]int{}
	addGroup := func(set map[string]bool) int {
		id := len(groups)
		groups = append(groups, set)
		for b := range set {
			owners[b] = append(owners[b], id)
		}
		return id
	}

	type storeGroups struct{ old, cur int }
	byStore := map[string]storeGroups{}
	for _, fs := range e.stores {
		row := api.CleanupStoreStatus{Store: fs.name, Stream: fs.stream, TrashKeep: fs.trashWindow().String()}
		if fs.hist != nil {
			row.HistoryKeep = fs.hist.keep.String()
		}
		var oldest time.Time
		_ = filepath.WalkDir(filepath.Join(e.dataDir, "trash", fs.name), func(_ string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if fi, err := d.Info(); err == nil {
				row.TrashFiles++
				row.TrashBytes += fi.Size()
				if t := trashTime(fi); oldest.IsZero() || t.Before(oldest) {
					oldest = t
				}
			}
			return nil
		})
		if !oldest.IsZero() {
			row.TrashOldest = oldest.UTC().Format(time.RFC3339)
			row.TrashNextExpiry = oldest.Add(fs.trashWindow()).UTC().Format(time.RFC3339)
		}
		old, cur := map[string]bool{}, map[string]bool{}
		if byPath, err := readHistoryFile(filepath.Join(e.dataDir, "history", fs.name+".ndjson")); err == nil {
			for _, versions := range byPath {
				last := len(versions) - 1
				for i, v := range versions {
					if i == last && !v.Deleted {
						if v.Blob != "" {
							row.CurrentCopies++
							cur[v.Blob] = true
						}
						continue
					}
					row.OldVersions++
					if v.Blob != "" {
						old[v.Blob] = true
					}
				}
			}
		} else if !os.IsNotExist(err) {
			return st, fmt.Errorf("history %s: %w", fs.name, err)
		}
		byStore[fs.name] = storeGroups{addGroup(old), addGroup(cur)}
		st.Stores = append(st.Stores, row)
	}

	orphanGroup := map[string]int{}
	for _, id := range e.orphanStoreIDs() {
		refs := map[string]bool{}
		if err := refsFromJournal(filepath.Join(e.dataDir, "journal", id+".ndjson"), refs); err != nil && !os.IsNotExist(err) {
			return st, fmt.Errorf("journal %s: %w", id, err)
		}
		if err := refsFromHistory(filepath.Join(e.dataDir, "history", id+".ndjson"), refs); err != nil && !os.IsNotExist(err) {
			return st, fmt.Errorf("history %s: %w", id, err)
		}
		orphanGroup[id] = addGroup(refs)
		row := api.CleanupOrphanStatus{Store: id, UnsentWrites: e.unsentWrites(id)}
		for _, p := range e.orphanFiles(id) {
			b, f := treeSize(p)
			row.DataBytes += b
			row.DataFiles += int(f)
		}
		folder := filepath.Join(e.cfg.Root, id)
		if fi, err := os.Stat(folder); err == nil && fi.IsDir() {
			row.Folder = folder
			b, f := treeSize(folder)
			row.FolderBytes, row.FolderFiles = b, int(f)
		}
		st.Orphans = append(st.Orphans, row)
	}

	// Attribute each blob's bytes to the one group that alone keeps it.
	exclusive := make([]int64, len(groups))
	for blob, size := range sizes {
		if base[blob] {
			continue
		}
		own := owners[blob]
		switch len(own) {
		case 0:
			st.UnreferencedBlobs++
			st.UnreferencedBytes += size
		case 1:
			exclusive[own[0]] += size
		default:
			st.SharedBytes += size
		}
	}
	for i := range st.Stores {
		g := byStore[st.Stores[i].Store]
		st.Stores[i].OldVersionBytes = exclusive[g.old]
		st.Stores[i].CurrentCopyBytes = exclusive[g.cur]
	}
	for i := range st.Orphans {
		st.Orphans[i].ContentBytes = exclusive[orphanGroup[st.Orphans[i].Store]]
	}
	return st, nil
}
