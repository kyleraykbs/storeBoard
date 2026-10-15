package mobile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// The embedded engine end to end: start it from JSON, send a file (the share
// sheet's path), see it in the status and the history, and restore a version
// — all through the same calls the Android app makes.
func TestEmbeddedDaemon(t *testing.T) {
	dir := t.TempDir()
	mount := filepath.Join(dir, "documents")
	cfg := `{
  "configVersion": 1,
  "peer": "phone",
  "dataDir": "` + filepath.Join(dir, "data") + `",
  "socket": "` + filepath.Join(dir, "unused.sock") + `",
  "groups": {
    "kyle": {
      "peers": ["phone"],
      "folders": { "documents": { "mount": "` + mount + `" } }
    }
  },
  "backup": { "enable": true, "keep": "1y" }
}`

	d, err := Start(cfg, filepath.Join(dir, "storeboard.log"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	// A file to share, standing in for a camera-roll photo.
	src := filepath.Join(dir, "IMG_0001.jpg")
	if err := os.WriteFile(src, []byte("pretend this is a photo"), 0o644); err != nil {
		t.Fatal(err)
	}

	raw, err := d.Send("documents", src, "Camera/IMG_0001.jpg")
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	var sent api.SendResult
	if err := json.Unmarshal([]byte(raw), &sent); err != nil {
		t.Fatal(err)
	}
	if sent.Path != "Camera/IMG_0001.jpg" || sent.Parked != "" {
		t.Fatalf("send result = %+v", sent)
	}
	if got, err := os.ReadFile(filepath.Join(mount, "Camera", "IMG_0001.jpg")); err != nil || string(got) != "pretend this is a photo" {
		t.Fatalf("sent file = %q, %v", got, err)
	}

	// Status: the folder is there and the file is indexed.
	raw, err = d.Status()
	if err != nil {
		t.Fatal(err)
	}
	var st api.Status
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		t.Fatal(err)
	}
	if st.Peer != "phone" || len(st.Folders) != 1 || st.Folders[0].Name != "documents" {
		t.Fatalf("status = %+v", st)
	}
	if len(st.Sync) != 1 || st.Sync[0].Entries == 0 {
		t.Fatalf("sync section = %+v, want the sent file indexed", st.Sync)
	}

	// History: one version, restore a copy of it elsewhere.
	raw, err = d.Versions("documents", "Camera/IMG_0001.jpg")
	if err != nil {
		t.Fatalf("versions: %v", err)
	}
	var vs api.Versions
	if err := json.Unmarshal([]byte(raw), &vs); err != nil {
		t.Fatal(err)
	}
	if len(vs.Versions) != 1 || vs.Versions[0].Blob == "" {
		t.Fatalf("versions = %+v, want one recorded version", vs.Versions)
	}

	out := filepath.Join(dir, "restored.jpg")
	raw, err = d.Restore("documents", "Camera/IMG_0001.jpg", "", "", out)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	var res api.RestoreResult
	if err := json.Unmarshal([]byte(raw), &res); err != nil {
		t.Fatal(err)
	}
	if !res.Copied {
		t.Fatalf("restore result = %+v, want a copy", res)
	}
	if got, err := os.ReadFile(out); err != nil || string(got) != "pretend this is a photo" {
		t.Fatalf("restored file = %q, %v", got, err)
	}
}

// Sending onto a taken name keeps both: the existing file is parked, and the
// sender is told where.
func TestEmbeddedSendNeverOverwrites(t *testing.T) {
	dir := t.TempDir()
	mount := filepath.Join(dir, "documents")
	cfg := `{
  "configVersion": 1,
  "peer": "phone",
  "dataDir": "` + filepath.Join(dir, "data") + `",
  "socket": "` + filepath.Join(dir, "unused.sock") + `",
  "groups": {"kyle": {"peers": ["phone"], "folders": {"documents": {"mount": "` + mount + `"}}}}
}`
	d, err := Start(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Stop()

	first := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(first, []byte("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(dir, "b.txt")
	if err := os.WriteFile(second, []byte("second"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := d.Send("documents", first, "same.txt"); err != nil {
		t.Fatal(err)
	}
	raw, err := d.Send("documents", second, "same.txt")
	if err != nil {
		t.Fatal(err)
	}
	var sent api.SendResult
	if err := json.Unmarshal([]byte(raw), &sent); err != nil {
		t.Fatal(err)
	}
	if sent.Parked == "" {
		t.Fatalf("send result = %+v, want the existing file parked", sent)
	}
	if got, err := os.ReadFile(filepath.Join(mount, "same.txt")); err != nil || string(got) != "second" {
		t.Fatalf("destination = %q, %v; want the new content", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(mount, sent.Parked)); err != nil || string(got) != "first" {
		t.Fatalf("parked copy = %q, %v; want the old content kept", got, err)
	}
}
