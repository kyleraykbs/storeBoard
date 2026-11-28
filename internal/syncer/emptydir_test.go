package syncer

import (
	"strings"
	"testing"
)

func liveDir(fs *StoreState, p string) bool {
	ent := fs.db.Get(p)
	return ent != nil && !ent.Deleted && ent.Dir
}

func queuedMkdir(fs *StoreState, p string) bool {
	for _, op := range queueSnapshot(fs) {
		if op.Op == opMkdir && op.Path == p {
			return true
		}
	}
	return false
}

// A file manager moving a folder to another mount copies it, unlinks every
// file and then rmdirs the folder. The folder only existed because of its
// files, so it vanished with the last one and the rmdir failed with ENOENT -
// and every parent that held nothing else vanished with it.
func TestEmptyingAFolderThroughTheMountKeepsIt(t *testing.T) {
	_, fs, m := streamedNode(t)
	blob := strings.Repeat("ab", 32)
	for _, p := range []string{"Videos/Kubecord/RawClips/a.mov", "Videos/Kubecord/RawClips/b.mov"} {
		if err := fs.db.Put(&Entry{Path: p, Blob: blob, Size: 3, VV: map[string]uint64{"b": 1}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := m.Remove("Videos/Kubecord/RawClips/a.mov"); err != nil {
		t.Fatal(err)
	}
	if liveDir(fs, "Videos/Kubecord/RawClips") {
		t.Fatal("a folder that still holds a file was recorded as an empty directory")
	}
	if err := m.Remove("Videos/Kubecord/RawClips/b.mov"); err != nil {
		t.Fatal(err)
	}
	if _, ok := fs.tree.Get("Videos/Kubecord/RawClips"); !ok {
		t.Fatal("the emptied folder vanished from the mount")
	}
	if !liveDir(fs, "Videos/Kubecord/RawClips") || !queuedMkdir(fs, "Videos/Kubecord/RawClips") {
		t.Fatal("the emptied folder was not recorded and queued for the holder")
	}

	if err := m.RemoveDir("Videos/Kubecord/RawClips"); err != nil {
		t.Fatalf("rmdir of the emptied folder: %v", err)
	}
	if _, ok := fs.tree.Get("Videos/Kubecord/RawClips"); ok {
		t.Fatal("rmdir left the folder in place")
	}
	if _, ok := fs.tree.Get("Videos/Kubecord"); !ok {
		t.Fatal("removing the folder took its parent with it")
	}
	if !liveDir(fs, "Videos/Kubecord") {
		t.Fatal("the parent left empty was not recorded")
	}
}

// mv of the last file out of a folder leaves the folder; a rename inside the
// folder does not record anything.
func TestMovingTheLastFileOutKeepsTheFolder(t *testing.T) {
	_, fs, m := streamedNode(t)
	blob := strings.Repeat("ab", 32)
	if err := fs.db.Put(&Entry{Path: "src/only.txt", Blob: blob, Size: 3, VV: map[string]uint64{"b": 1}}); err != nil {
		t.Fatal(err)
	}

	if err := m.Rename("src/only.txt", "src/renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if liveDir(fs, "src") {
		t.Fatal("a rename inside the folder recorded it as empty")
	}

	if err := m.Rename("src/renamed.txt", "dst/moved.txt"); err != nil {
		t.Fatal(err)
	}
	if _, ok := fs.tree.Get("src"); !ok {
		t.Fatal("the folder the file left vanished")
	}
	if !liveDir(fs, "src") || !queuedMkdir(fs, "src") {
		t.Fatal("the folder the file left was not recorded and queued")
	}
	if _, ok := fs.tree.Get("dst/moved.txt"); !ok {
		t.Fatal("the file did not arrive")
	}
}
