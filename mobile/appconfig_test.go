package mobile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The document the Android app writes on first run (Engine.defaultConfig in
// android/app/src/main/java/dev/kyle/storeboard/Engine.kt) has to pass the
// daemon's own strict validator — the app starts the same engine, and an
// unknown key there is an error, not a warning. Keep the two in step: this
// test is the shape the app hands over, with only the paths substituted.
func TestAppDefaultConfigIsValid(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "storeboard")
	cfg := `{
  "configVersion": 2,
  "root": "` + filepath.Join(dir, "Storeboard") + `",
  "peer": "phone",
  "dataDir": "` + data + `",
  "socket": "` + filepath.Join(data, "storeboard.sock") + `",
  "backup": { "enable": true, "keep": "1y" },
  "peers": {
    "phone": {},
    "pool": { "endpoints": ["192.168.1.10:4242"] }
  },
  "groups": {
    "kyle": { "peers": ["phone", "pool"] }
  },
  "stores": {
    "camera": { "group": "kyle" },
    "photos": { "group": "kyle" }
  }
}`

	log := filepath.Join(dir, "storeboard.log")
	d, err := Start(cfg, log)
	if err != nil {
		t.Fatalf("the app's own default config does not start: %v", err)
	}
	defer d.Stop()

	// A phone that has never heard from the pool still reports a sane
	// document: this peer, both folders, nobody addressed but its own name.
	raw, err := d.Status()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(raw, `"peer":"phone"`) || !strings.Contains(raw, `"camera"`) || !strings.Contains(raw, `"photos"`) {
		t.Fatalf("status = %s", raw)
	}

	// And the log file it was told to write is a real file.
	if _, err := os.Stat(log); err != nil {
		t.Fatalf("log: %v", err)
	}
}
