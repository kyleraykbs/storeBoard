package syncer

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// Space reports the durable categories under dataDir. It deliberately reports
// orphan journals separately: they are data for stores removed from config,
// not garbage that cleanup may silently destroy.
func (e *Engine) Space() api.Space {
	entries := []api.SpaceEntry{}
	add := func(path, label string, configured bool) {
		bytes, files := treeSize(path)
		entries = append(entries, api.SpaceEntry{Path: label, Bytes: bytes, Files: files, Configured: configured})
	}
	add(filepath.Join(e.dataDir, "store"), "store", true)
	add(filepath.Join(e.dataDir, "tmp"), "tmp", true)
	for _, fs := range e.stores {
		add(filepath.Join(e.dataDir, "trash", fs.name), "trash/"+fs.name, true)
		add(filepath.Join(e.dataDir, "journal", fs.name+".ndjson"), "journal/"+fs.name, true)
		add(filepath.Join(e.dataDir, "history", fs.name+".ndjson"), "history/"+fs.name, true)
	}
	for _, id := range e.orphanStoreIDs() {
		add(filepath.Join(e.cfg.Root, id), "root/"+id, false)
		add(filepath.Join(e.dataDir, "journal", id+".ndjson"), "journal/"+id, false)
		add(filepath.Join(e.dataDir, "history", id+".ndjson"), "history/"+id, false)
		add(filepath.Join(e.dataDir, "trash", id), "trash/"+id, false)
		add(queueBytesDir(e.dataDir, id), "queue/"+id, false)
	}
	var total int64
	for _, ent := range entries {
		total += ent.Bytes
	}
	return api.Space{APIVersion: api.Version, DataDir: e.dataDir, TotalBytes: total, Entries: entries}
}

// Cleanup expires trash/history and reports orphaned store data separately
// from ordinary retention. Orphan removal is deliberately unimplemented: a
// store the config stopped declaring may still be valuable, and the engine
// does not know whether the user reorganized, unplugged a disk, or mistyped
// a name. The server is the authorization boundary for the cleanup call.
func (e *Engine) Cleanup(removeOrphans bool) api.CleanupResult {
	if removeOrphans {
		e.logger.Warn("orphan removal is reserved for a future confirmation protocol")
	}
	trashDir := filepath.Join(e.dataDir, "trash")
	trashBefore, trashFilesBefore := treeSize(trashDir)
	e.pruneOnce(time.Now())
	result := api.CleanupResult{APIVersion: api.Version, OrphanStores: []string{}}
	for _, id := range e.orphanStoreIDs() {
		var bytes int64
		for _, path := range []string{
			filepath.Join(e.cfg.Root, id),
			filepath.Join(e.dataDir, "journal", id+".ndjson"),
			filepath.Join(e.dataDir, "history", id+".ndjson"),
			filepath.Join(e.dataDir, "trash", id),
		} {
			if _, err := os.Stat(path); err != nil {
				continue
			}
			pathBytes, _ := treeSize(path)
			bytes += pathBytes
		}
		if bytes > 0 {
			result.OrphanStores = append(result.OrphanStores, id)
			result.OrphanBytes += bytes
		}
	}
	removed, freed := e.Collect()
	result.Blobs = removed
	result.BlobBytes = freed
	trashAfter, trashFilesAfter := treeSize(trashDir)
	result.TrashBytes = max64(0, trashBefore-trashAfter)
	result.TrashFiles = int(max64(0, trashFilesBefore-trashFilesAfter))
	return result
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (e *Engine) orphanStoreIDs() []string {
	seen := map[string]bool{}
	note := func(id string) {
		if !validStoreName(id) {
			return
		}
		if _, ok := e.byName[id]; !ok {
			seen[id] = true
		}
	}
	// Logs name a store by <id>.ndjson; trash and queue by a directory.
	for _, sub := range []string{"journal", "history", "queue"} {
		entries, _ := os.ReadDir(filepath.Join(e.dataDir, sub))
		for _, ent := range entries {
			if id, ok := strings.CutSuffix(ent.Name(), ".ndjson"); ok && !ent.IsDir() {
				note(id)
			}
		}
	}
	for _, sub := range []string{"trash", "queue"} {
		entries, _ := os.ReadDir(filepath.Join(e.dataDir, sub))
		for _, ent := range entries {
			if ent.IsDir() {
				note(ent.Name())
			}
		}
	}
	// A folder under the root the config does not name: a leftover store,
	// or something the user put there.
	entries, _ := os.ReadDir(e.cfg.Root)
	for _, ent := range entries {
		if ent.IsDir() && !strings.HasPrefix(ent.Name(), ".") {
			note(ent.Name())
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func treeSize(root string) (bytes, files int64) {
	_ = filepath.WalkDir(root, func(path string, ent os.DirEntry, err error) error {
		if err != nil || ent.IsDir() {
			return nil
		}
		info, err := ent.Info()
		if err != nil {
			return nil
		}
		bytes += info.Size()
		files++
		return nil
	})
	return
}
