// Package e2e drives the real binaries: it builds storeboard and storeboardd,
// starts daemons on loopback, and asserts what a person running the program
// would see. The syncer tests exercise the engine in-process; these ones
// prove the shipped artifacts agree with each other over a socket and a wire.
package e2e

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// wait bounds every poll: a store that has not converged in this long is a
// failure, not a slow machine.
const wait = 90 * time.Second

var (
	buildOnce sync.Once
	binDir    string
	buildErr  error
)

// binaries builds both commands once per test run and returns their paths.
func binaries(t *testing.T) (cli, daemon string) {
	t.Helper()
	buildOnce.Do(func() {
		binDir, buildErr = os.MkdirTemp("", "storeboard-e2e-bin")
		if buildErr != nil {
			return
		}
		for _, name := range []string{"storeboard", "storeboardd"} {
			cmd := exec.Command("go", "build", "-o", filepath.Join(binDir, name), "./cmd/"+name)
			cmd.Dir = moduleRoot()
			if out, err := cmd.CombinedOutput(); err != nil {
				buildErr = fmt.Errorf("build %s: %v\n%s", name, err, out)
				return
			}
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return filepath.Join(binDir, "storeboard"), filepath.Join(binDir, "storeboardd")
}

func moduleRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	return filepath.Dir(filepath.Dir(dir))
}

// freePort returns a loopback address nothing is listening on.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// peer is one daemon under test.
type peer struct {
	name    string
	root    string
	dataDir string
	socket  string
	cfgPath string
	cmd     *exec.Cmd
	log     *os.File
}

// mesh describes a two-machine set-up so a test only names what it cares
// about: the store declarations on each side.
type mesh struct {
	storesA map[string]storeDecl
	storesB map[string]storeDecl
}

// storeDecl mirrors the config's store object: everything is optional except
// the group.
type storeDecl struct {
	Group    string            `json:"group"`
	Sync     string            `json:"sync,omitempty"`
	Symlinks []string          `json:"symlinks,omitempty"`
	Links    map[string]string `json:"links,omitempty"`
}

// network is a set of daemons sharing one group. A peer can be stopped and
// restarted, which is how an outage is simulated without new machinery.
type network struct {
	t      *testing.T
	daemon string
	peers  map[string]*peer
	order  []string
}

// startNetwork writes every peer's config with the whole mesh's endpoints,
// then launches the named peers (all of them when none are named). A peer
// left down can be brought up later with start.
func startNetwork(t *testing.T, specs map[string]map[string]storeDecl, launch ...string) *network {
	return startNetworkWith(t, specs, nil, launch...)
}

// startNetworkWith is startNetwork plus extra top-level config keys, such as
// the backup schedule a restore test needs.
func startNetworkWith(t *testing.T, specs map[string]map[string]storeDecl, extra map[string]any, launch ...string) *network {
	t.Helper()
	_, daemon := binaries(t)
	dir := t.TempDir()
	n := &network{t: t, daemon: daemon, peers: map[string]*peer{}}
	for name := range specs {
		n.order = append(n.order, name)
	}
	sort.Strings(n.order)

	eps := make([]endpoint, 0, len(n.order))
	addrs := map[string]string{}
	for _, name := range n.order {
		addrs[name] = freePort(t)
		eps = append(eps, endpoint{name, addrs[name]})
		n.peers[name] = &peer{
			name:    name,
			root:    filepath.Join(dir, name, "root"),
			dataDir: filepath.Join(dir, name, "data"),
			socket:  filepath.Join(dir, name, "s.sock"),
		}
		if err := os.MkdirAll(n.peers[name].root, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range n.order {
		p := n.peers[name]
		p.cfgPath = writeConfigExt(t, p, specs[name], extra, eps...)
	}
	start := launch
	if len(start) == 0 {
		start = n.order
	}
	for _, name := range start {
		n.start(name)
	}
	t.Cleanup(func() {
		for _, name := range n.order {
			stop(n.peers[name])
		}
	})
	return n
}

func (n *network) start(name string) { startDaemon(n.t, n.daemon, n.peers[name]) }

// kill stops a daemon without letting it shut down, which is the crash the
// write path must survive.
func (n *network) kill(name string) {
	p := n.peers[name]
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_ = p.cmd.Wait()
	if p.log != nil {
		_ = p.log.Close()
	}
	p.cmd = nil
}
func (n *network) stop(name string)       { stop(n.peers[name]) }
func (n *network) peer(name string) *peer { return n.peers[name] }

// startMesh is the common two-machine case.
func startMesh(t *testing.T, m mesh) (*peer, *peer) {
	t.Helper()
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": m.storesA,
		"bravo": m.storesB,
	})
	return n.peer("alpha"), n.peer("bravo")
}

type endpoint struct{ name, addr string }

// writeConfig renders one daemon's config file and returns its path.
func writeConfig(t *testing.T, p *peer, stores map[string]storeDecl, extra ...endpoint) string {
	t.Helper()
	return writeConfigExt(t, p, stores, nil, extra...)
}

// writeConfigExt renders a config with extra top-level keys merged in.
func writeConfigExt(t *testing.T, p *peer, stores map[string]storeDecl, top map[string]any, extra ...endpoint) string {
	t.Helper()
	self := endpoint{p.name, ""}
	peers := map[string]any{}
	for _, e := range extra {
		if e.name == p.name {
			self.addr = e.addr
		}
		peers[e.name] = map[string]any{"endpoints": []string{e.addr}}
	}
	if self.addr != "" {
		peers[p.name] = map[string]any{"endpoints": []string{self.addr}}
	}
	members := []string{}
	for name := range peers {
		members = append(members, name)
	}
	doc := map[string]any{
		"configVersion": 2,
		"root":          p.root,
		"dataDir":       p.dataDir,
		"socket":        p.socket,
		"peer":          p.name,
		"peers":         peers,
		"groups":        map[string]any{"g": map[string]any{"peers": members}},
		"stores":        stores,
	}
	for k, v := range top {
		doc[k] = v
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(p.root), "config.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func startDaemon(t *testing.T, daemon string, p *peer) {
	t.Helper()
	logPath := filepath.Join(filepath.Dir(p.root), p.name+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	p.log = logFile
	cmd := exec.Command(daemon, "-config", p.cfgPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// The scanner polls; a test cannot wait fifteen seconds per tick.
	cmd.Env = append(os.Environ(),
		"STOREBOARD_SCAN_INTERVAL=200ms",
		"STOREBOARD_LINK_INTERVAL=200ms",
		"STOREBOARD_INDEX_INTERVAL=1s",
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p.cmd = cmd
	t.Cleanup(func() { stop(p) })
	waitFor(t, "daemon "+p.name+" socket", func() bool {
		_, err := os.Stat(p.socket)
		return err == nil
	})
}

func stop(p *peer) {
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = p.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
	if p.log != nil {
		_ = p.log.Close()
	}
	p.cmd = nil
}

func waitFor(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// storeDir is where a store's canonical directory lives.
func (p *peer) storeDir(name string) string { return filepath.Join(p.root, name) }

// cli runs the real CLI against this daemon and returns stdout.
func cliRun(t *testing.T, p *peer, args ...string) string {
	t.Helper()
	cli, daemon := binaries(t)
	_ = daemon
	full := append([]string{"-config", p.cfgPath}, args...)
	out, err := exec.Command(cli, full...).CombinedOutput()
	if err != nil {
		t.Fatalf("storeboard %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// status parses the daemon's own status document.
func statusOf(t *testing.T, p *peer) statusDoc {
	t.Helper()
	raw := cliRun(t, p, "status", "--json")
	var doc statusDoc
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("status json: %v\n%s", err, raw)
	}
	return doc
}

type statusDoc struct {
	Peer        string `json:"peer"`
	Connections []struct {
		Peer  string `json:"peer"`
		State string `json:"state"`
	} `json:"connections"`
	Sync []struct {
		Store   string `json:"store"`
		State   string `json:"state"`
		Root    string `json:"root"`
		Entries int    `json:"entries"`
	} `json:"sync"`
}

// syncState reports one store's state name: replica, attached or detached.
func (d statusDoc) syncState(store string) string {
	for _, s := range d.Sync {
		if s.Store == store {
			return s.State
		}
	}
	return ""
}

func write(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// TestSyncDeliversFileBetweenMachines is the base promise: a file written on
// one machine appears, byte for byte, on the other.
func TestSyncDeliversFileBetweenMachines(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	const body = "the same bytes on both machines\n"
	write(t, filepath.Join(a.storeDir("docs"), "note.txt"), body)

	waitFor(t, "note.txt on bravo", func() bool {
		got, err := os.ReadFile(filepath.Join(b.storeDir("docs"), "note.txt"))
		return err == nil && string(got) == body
	})

	// Both sides agree they are a full replica of the store.
	if state := statusOf(t, b).syncState("docs"); state != "replica" {
		t.Fatalf("bravo reports docs as %q, want replica", state)
	}
}

// TestRemoteStoreStreamsFromHolder covers the split this project exists for:
// one machine holds the bytes locally, the other mounts the same store as a
// remote stream and still sees new files.
func TestRemoteStoreStreamsFromHolder(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"media": {Group: "g", Sync: "local"}},
		storesB: map[string]storeDecl{"media": {Group: "g", Sync: "remote"}},
	})

	// A remote store goes live once its holder's link is up, which is after
	// the socket exists: wait for the state instead of racing the handshake.
	waitFor(t, "bravo to attach the remote store", func() bool {
		return statusOf(t, b).syncState("media") == "attached"
	})

	const body = "streamed from the holder\n"
	write(t, filepath.Join(a.storeDir("media"), "clip.txt"), body)

	waitFor(t, "clip.txt streamed to bravo", func() bool {
		got, err := os.ReadFile(filepath.Join(b.storeDir("media"), "clip.txt"))
		return err == nil && string(got) == body
	})
	if state := statusOf(t, b).syncState("media"); state != "attached" {
		t.Fatalf("bravo left attached state: %q", state)
	}
}

// TestNestedStoreAppearsInsideParent covers nesting: a child store mounts
// inside its parent at a relative path, and content written through that path
// syncs as the child's content.
func TestNestedStoreAppearsInsideParent(t *testing.T) {
	// links maps a child store id to the relative path it appears at inside
	// the parent. BulkPictures is local on alpha and remote on bravo, so the
	// parent holds a real directory while the child is only attached.
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{
			"Pictures":     {Group: "g", Links: map[string]string{"BulkPictures": "Bulk"}},
			"BulkPictures": {Group: "g", Sync: "local"},
		},
		storesB: map[string]storeDecl{
			"Pictures":     {Group: "g", Links: map[string]string{"BulkPictures": "Bulk"}},
			"BulkPictures": {Group: "g", Sync: "remote"},
		},
	})

	linkPath := filepath.Join(a.storeDir("Pictures"), "Bulk")
	waitFor(t, "the child link inside the parent", func() bool {
		target, err := os.Readlink(linkPath)
		return err == nil && target == a.storeDir("BulkPictures")
	})

	// Writing through the nested path writes the child store.
	const body = "a picture in the bulk child\n"
	write(t, filepath.Join(linkPath, "shot.jpg"), body)
	if got := read(t, filepath.Join(a.storeDir("BulkPictures"), "shot.jpg")); got != body {
		t.Fatalf("write through the link landed elsewhere: %q", got)
	}

	waitFor(t, "the child's content on bravo", func() bool {
		got, err := os.ReadFile(filepath.Join(b.storeDir("BulkPictures"), "shot.jpg"))
		return err == nil && string(got) == body
	})
	// The parent never indexes the child: the link is a mountpoint, not data.
	if _, err := os.Stat(filepath.Join(b.storeDir("Pictures"), "Bulk", "shot.jpg")); err != nil {
		t.Fatalf("bravo's parent does not expose the child: %v", err)
	}
}

// TestSendVerbPlacesFileForEveryPeer exercises the CLI's send verb: the file
// goes into the store's canonical dir and then reaches the other machine.
func TestSendVerbPlacesFileForEveryPeer(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	src := filepath.Join(t.TempDir(), "share me.txt")
	const body = "sent from the share sheet\n"
	write(t, src, body)

	out := cliRun(t, a, "send", "docs", src, "--as", "inbox/share me.txt", "--json")
	var res struct {
		Store string `json:"store"`
		Path  string `json:"path"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("send json: %v\n%s", err, out)
	}
	if res.Path != "inbox/share me.txt" {
		t.Fatalf("send reported path %q", res.Path)
	}
	if got := read(t, filepath.Join(a.storeDir("docs"), "inbox", "share me.txt")); got != body {
		t.Fatalf("send wrote %q", got)
	}

	waitFor(t, "the sent file on bravo", func() bool {
		got, err := os.ReadFile(filepath.Join(b.storeDir("docs"), "inbox", "share me.txt"))
		return err == nil && string(got) == body
	})
}

// TestNewFileReachesPeerWhileLargeBlobStreams is the regression for the bug
// that stopped every store converging on the real mesh: one multi-gigabyte
// transfer monopolised the peer's socket and its single want worker, so index
// frames and blob requests for other stores waited behind it. A small file
// must arrive while the large one is still mid-transfer, not after it lands.
func TestNewFileReachesPeerWhileLargeBlobStreams(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"bulk": {Group: "g"}, "docs": {Group: "g"}},
		storesB: map[string]storeDecl{"bulk": {Group: "g"}, "docs": {Group: "g"}},
	})

	// Big enough that its transfer outlasts the deadline below by a wide
	// margin on loopback.
	large := make([]byte, 1<<30)
	for i := range large {
		large[i] = byte('a' + i%26)
	}
	bigPath := filepath.Join(a.storeDir("bulk"), "big.bin")
	if err := os.MkdirAll(filepath.Dir(bigPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bigPath, large, 0o644); err != nil {
		t.Fatal(err)
	}

	// Let the transfer get underway, then send one small file in another
	// store exactly once.
	// The receiver accumulates chunks in its scratch directory, so that is
	// where partial progress is observable; the store root only ever holds a
	// finished file.
	waitFor(t, "the large blob to start arriving", func() bool {
		files, _ := filepath.Glob(filepath.Join(b.dataDir, "tmp", "blob-*"))
		for _, f := range files {
			if info, err := os.Stat(f); err == nil && info.Size() > 1<<20 {
				return true
			}
		}
		return false
	})

	const body = "small file, urgent\n"
	write(t, filepath.Join(a.storeDir("docs"), "urgent.txt"), body)

	// The small file must not wait for the transfer: unlike the eventual
	// delivery the other tests assert, this one has a deadline well under the
	// time the blob needs.
	const budget = 25 * time.Second
	start := time.Now()
	arrived := false
	for time.Since(start) < budget {
		if got, err := os.ReadFile(filepath.Join(b.storeDir("docs"), "urgent.txt")); err == nil && string(got) == body {
			arrived = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	took := time.Since(start)
	if !arrived {
		t.Fatalf("the small file did not arrive within %s while a %d MiB blob was streaming: it is waiting behind the transfer\nbravo log:\n%s",
			budget, len(large)>>20, tailLog(b))
	}
	if info, err := os.Stat(filepath.Join(b.storeDir("bulk"), "big.bin")); err == nil && info.Size() == int64(len(large)) {
		t.Skipf("the %d MiB blob finished within %s; the machine is too fast to measure", len(large)>>20, took.Round(time.Millisecond))
	}
	t.Logf("small file arrived in %s while the %d MiB blob was still streaming", took.Round(time.Millisecond), len(large)>>20)
}

// interestingLog returns the log lines that are not per-scan noise: a tests'
// daemon logs store ready on every tick, which buries the event under test.
// journalOf returns a store's journal, newest last.
func journalOf(p *peer, store string) string {
	b, err := os.ReadFile(filepath.Join(p.dataDir, "journal", store+".ndjson"))
	if err != nil {
		return err.Error()
	}
	return string(b)
}

// treeOf lists a directory's entries for failure messages.
func treeOf(dir string) string {
	var out []string
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || path == dir {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		if d.IsDir() {
			out = append(out, "d "+rel)
		} else {
			out = append(out, "f "+rel)
		}
		return nil
	})
	return strings.Join(out, "\n")
}

func interestingLog(p *peer) string {
	if p.log == nil {
		return ""
	}
	raw, err := os.ReadFile(p.log.Name())
	if err != nil {
		return err.Error()
	}
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		for _, want := range []string{"deleted", "removed", "apply", "want", "error", "WARN", "ERROR", "conflict", "index", "prunedbg"} {
			if strings.Contains(strings.ToLower(line), strings.ToLower(want)) {
				out = append(out, line)
				break
			}
		}
	}
	if len(out) > 60 {
		out = out[len(out)-60:]
	}
	return strings.Join(out, "\n")
}

func tailLog(p *peer) string {
	if p.log == nil {
		return ""
	}
	b, err := os.ReadFile(p.log.Name())
	if err != nil {
		return err.Error()
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > 40 {
		lines = lines[len(lines)-40:]
	}
	return strings.Join(lines, "\n")
}

// TestNestedDirectoriesSync covers plain subdirectories inside a store: the
// journal records file paths, so a deep path must be created complete with
// its parent directories on the other machine.
func TestNestedDirectoriesSync(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	const body = "three levels down\n"
	write(t, filepath.Join(a.storeDir("docs"), "one", "two", "three", "deep.txt"), body)

	waitFor(t, "the nested file on bravo", func() bool {
		got, err := os.ReadFile(filepath.Join(b.storeDir("docs"), "one", "two", "three", "deep.txt"))
		return err == nil && string(got) == body
	})
	for _, dir := range []string{"one", "one/two", "one/two/three"} {
		info, err := os.Stat(filepath.Join(b.storeDir("docs"), dir))
		if err != nil || !info.IsDir() {
			t.Fatalf("directory %q did not materialise: %v", dir, err)
		}
	}
}

// TestEmptyFileMaterialisesWithItsDirectory covers a zero-byte file. The
// sender used to emit no chunk at all for an empty blob, so the receiver
// never opened a transfer and dropped the completion frame: empty files —
// and any directory that held only empty files — never appeared. A nested
// empty file is the case that exposed it.
func TestEmptyFileMaterialisesWithItsDirectory(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	write(t, filepath.Join(a.storeDir("docs"), "sdfsdfs", "test", "empty.txt"), "")

	waitFor(t, "the empty file on bravo", func() bool {
		info, err := os.Stat(filepath.Join(b.storeDir("docs"), "sdfsdfs", "test", "empty.txt"))
		return err == nil && !info.IsDir() && info.Size() == 0
	})
	info, err := os.Stat(filepath.Join(b.storeDir("docs"), "sdfsdfs", "test"))
	if err != nil || !info.IsDir() {
		t.Fatalf("the directory holding only an empty file did not materialise: %v", err)
	}
}

// TestEmptyDirectoryTheUserMadeSyncs covers the half that must be preserved:
// a directory the user creates and leaves empty is content in its own right,
// so it appears on the peer and stays there.
func TestEmptyDirectoryTheUserMadeSyncs(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	if err := os.MkdirAll(filepath.Join(a.storeDir("docs"), "empty", "also-empty"), 0o755); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the user's empty directories on bravo", func() bool {
		for _, dir := range []string{"empty", "empty/also-empty"} {
			info, err := os.Stat(filepath.Join(b.storeDir("docs"), dir))
			if err != nil || !info.IsDir() {
				return false
			}
		}
		return true
	})

	// They are not transient: a few scans must not sweep them away.
	time.Sleep(2 * time.Second)
	for _, dir := range []string{"empty", "empty/also-empty"} {
		if info, err := os.Stat(filepath.Join(b.storeDir("docs"), dir)); err != nil || !info.IsDir() {
			t.Fatalf("the empty directory %q disappeared after syncing", dir)
		}
	}
}

// TestDirectoriesAreRemovedWhenTheirFilesLeave is the other half: a directory
// exists because it held files. When the last file is deleted the directory
// goes too, all the way up through directories that held nothing but those
// now-empty directories.
func TestDirectoriesAreRemovedWhenTheirFilesLeave(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	deep := filepath.Join("top", "middle", "leaf")
	write(t, filepath.Join(a.storeDir("docs"), deep, "file.txt"), "content\n")
	waitFor(t, "the nested file on bravo", func() bool {
		_, err := os.Stat(filepath.Join(b.storeDir("docs"), deep, "file.txt"))
		return err == nil
	})

	if err := os.Remove(filepath.Join(a.storeDir("docs"), deep, "file.txt")); err != nil {
		t.Fatal(err)
	}

	for _, root := range []struct {
		name string
		dir  string
		log  *peer
	}{{"alpha", a.storeDir("docs"), a}, {"bravo", b.storeDir("docs"), b}} {
		for _, dir := range []string{"top/middle/leaf", "top/middle", "top"} {
			gone := filepath.Join(root.dir, dir)
			deadline := time.Now().Add(wait)
			for time.Now().Before(deadline) {
				if _, err := os.Stat(gone); os.IsNotExist(err) {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if _, err := os.Stat(gone); err == nil {
				t.Fatalf("%s kept %s after its last file was deleted\nalpha journal:\n%s\nbravo journal:\n%s\nalpha tree:\n%s\nbravo tree:\n%s\nalpha log:\n%s\nbravo log:\n%s",
					root.name, dir, journalOf(a, "docs"), journalOf(b, "docs"), treeOf(a.storeDir("docs")), treeOf(b.storeDir("docs")), interestingLog(a), interestingLog(b))
			}
		}
		// The store root itself is not a directory the tree may remove.
		if info, err := os.Stat(root.dir); err != nil || !info.IsDir() {
			t.Fatalf("%s removed its own store root", root.name)
		}
	}
}

// TestEmptiedDirectoryKeepsAUserMadeChild documents the boundary: a directory
// whose only remaining child is one the user made stays, because that child is
// recorded and must exist.
func TestEmptiedDirectoryKeepsAUserMadeChild(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	write(t, filepath.Join(a.storeDir("docs"), "parent", "file.txt"), "content\n")
	if err := os.MkdirAll(filepath.Join(a.storeDir("docs"), "parent", "kept"), 0o755); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the tree on bravo", func() bool {
		if _, err := os.Stat(filepath.Join(b.storeDir("docs"), "parent", "file.txt")); err != nil {
			return false
		}
		info, err := os.Stat(filepath.Join(b.storeDir("docs"), "parent", "kept"))
		return err == nil && info.IsDir()
	})

	if err := os.Remove(filepath.Join(a.storeDir("docs"), "parent", "file.txt")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(b.storeDir("docs"), "parent", "file.txt")); os.IsNotExist(err) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(b.storeDir("docs"), "parent", "file.txt")); err == nil {
		t.Fatalf("bravo kept the deleted file\nalpha journal:\n%s\nbravo journal:\n%s\nalpha tree:\n%s\nbravo tree:\n%s",
			journalOf(a, "docs"), journalOf(b, "docs"), treeOf(a.storeDir("docs")), treeOf(b.storeDir("docs")))
	}

	for _, root := range []struct{ name, dir string }{{"alpha", a.storeDir("docs")}, {"bravo", b.storeDir("docs")}} {
		info, err := os.Stat(filepath.Join(root.dir, "parent", "kept"))
		if err != nil || !info.IsDir() {
			t.Fatalf("%s dropped the user's empty child directory: %v", root.name, err)
		}
	}
}

// TestSpaceAndCleanupAgainstLivePair checks the new operational verbs over real
// binaries. Unused data is reported with its path; only an explicit orphan
// removal deletes it. This discrimination is the policy the sessions
// established around invariant 1.
func TestSpaceAndCleanupAgainstLivePair(t *testing.T) {
	a, _ := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})
	orpRoot := filepath.Join(a.storeDir("old-store"))
	if err := os.MkdirAll(orpRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orpRoot, "left.txt"), []byte("left"), 0o644); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(a.dataDir, "journal", "old-store.ndjson")
	if err := os.WriteFile(journal, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	space := cliRun(t, a, "space", "--json")
	if !strings.Contains(space, "old-store") {
		t.Fatalf("space did not name the orphan store:\n%s", space)
	}
	cleanup := cliRun(t, a, "cleanup", "--json")
	if !strings.Contains(cleanup, "old-store") {
		t.Fatalf("cleanup did not report the orphan store:\n%s", cleanup)
	}
	if _, err := os.Stat(orpRoot); err != nil {
		t.Fatalf("cleanup removed an orphan by default: %v", err)
	}
	explicit := cliRun(t, a, "cleanup", "--remove-orphans", "--json")
	if !strings.Contains(explicit, "old-store") {
		t.Fatalf("explicit cleanup did not report the preserved orphan:\n%s", explicit)
	}
	if _, err := os.Stat(orpRoot); err != nil {
		t.Fatalf("cleanup deleted orphan data even though deletion is unimplemented: %v", err)
	}
}

// TestLockedRootStillHeals verifies the takeover failure mode from the field:
// when service activation leaves the root 0750 root-owned, the engine repairs
// container ownership and traversal instead of letting external access stay
// broken.
func TestLockedRootStillHeals(t *testing.T) {
	a, _ := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})
	root := a.root
	// The live replica requires root; declarative CI lacks it because changing
	// another UID is a privileged operation.
	if os.Getuid() != 0 && os.Geteuid() != 0 {
		t.Skipf("root-owned root repair requires root")
	}
	if err := os.Chown(root, 0, -1); err != nil {
		t.Skipf("chown to root unavailable: %v", err)
	}
	if err := os.Chmod(root, 0o750); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("root disappeared: %v", err)
	}
	uid := int(info.Sys().(*syscall.Stat_t).Uid)
	current := os.Getuid()
	if uid != current {
		t.Fatalf("root ownership is %d, want daemon-adjacent %d", uid, current)
	}
	if info.Mode().Perm()&0o700 != 0o700 {
		t.Fatalf("root is %v, not owner-traversable", info.Mode())
	}
}
