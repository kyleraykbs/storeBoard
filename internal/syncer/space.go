package syncer

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

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
