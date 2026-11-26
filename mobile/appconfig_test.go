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
  "learn": { "subscribe": "off" },
  "peers": {
    "phone": {},
    "amanita": { "endpoints": ["amanita.grid:4242"] }
  }
}`

	log := filepath.Join(dir, "storeboard.log")
	d, err := Start(cfg, log)
	if err != nil {
		t.Fatalf("the app's own default config does not start: %v", err)
	}
	defer d.Stop()

	// A phone that has never heard from its peer still reports a sane
	// document: this peer, no stores until the peer offers them.
	raw, err := d.Status()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(raw, `"peer":"phone"`) {
		t.Fatalf("status = %s", raw)
	}

	// And the log file it was told to write is a real file.
	if _, err := os.Stat(log); err != nil {
		t.Fatalf("log: %v", err)
	}
}
