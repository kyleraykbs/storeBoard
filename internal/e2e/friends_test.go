package e2e

import (
	"bufio"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeAgent answers a GateHouse agent's "verify" and "check" from tables
// the test changes as it goes.
type fakeAgent struct {
	mu     sync.Mutex
	tokens map[string]string          // token -> person
	grants map[string]map[string]bool // person -> "perm@store" -> allowed
}

func (a *fakeAgent) serve(t *testing.T, path string) {
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				var req struct {
					Method string            `json:"method"`
					Params map[string]string `json:"params"`
				}
				line, err := bufio.NewReader(c).ReadBytes('\n')
				if err != nil || json.Unmarshal(line, &req) != nil {
					return
				}
				a.mu.Lock()
				var resp any
				switch req.Method {
				case "verify":
					if who, ok := a.tokens[req.Params["token"]]; ok && req.Params["service"] == "storeboard" {
						resp = map[string]any{"result": map[string]string{"name": who}}
					} else {
						resp = map[string]any{"error": map[string]string{"code": "forbidden", "message": "bad token"}}
					}
				case "check":
					ok := a.grants[req.Params["user"]][req.Params["perm"]+"@"+req.Params["resource"]]
					resp = map[string]any{"result": map[string]bool{"allowed": ok}}
				}
				a.mu.Unlock()
				b, _ := json.Marshal(resp)
				c.Write(append(b, '\n'))
			}()
		}
	}()
}

func (a *fakeAgent) set(person, permAtStore string, allowed bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.grants[person] == nil {
		a.grants[person] = map[string]bool{}
	}
	a.grants[person][permAtStore] = allowed
}

// fakeGateHouse is the friend's grid's GateHouse /token: any refresh token
// gets the access token it maps to.
func fakeGateHouse(t *testing.T, access string) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.URL.Path != "/token" || r.PostForm.Get("grant_type") != "refresh_token" || r.PostForm.Get("client_id") != "storeboard" {
			http.Error(w, "no", http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"access_token": access, "expires_in": 300, "refresh_token": "r2"})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func startWith(t *testing.T, daemon string, p *peer, env ...string) {
	t.Helper()
	logFile, err := os.Create(filepath.Join(filepath.Dir(p.root), p.name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	p.log = logFile
	cmd := exec.Command(daemon, "-config", p.cfgPath)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.Env = append(os.Environ(), append([]string{
		"STOREBOARD_SCAN_INTERVAL=200ms", "STOREBOARD_LINK_INTERVAL=200ms",
		"STOREBOARD_INDEX_INTERVAL=1s", "STOREBOARD_LOG_LEVEL=debug", "STOREBOARD_FRIEND_TTL=0s",
	}, env...)...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p.cmd = cmd
	t.Cleanup(func() { stop(p) })
	waitFor(t, p.name+"'s socket", func() bool { _, err := os.Stat(p.socket); return err == nil })
}

func writeDoc(t *testing.T, p *peer, doc map[string]any) {
	t.Helper()
	doc["configVersion"] = 2
	doc["root"], doc["dataDir"], doc["socket"], doc["peer"] = p.root, p.dataDir, p.socket, p.name
	raw, _ := json.MarshalIndent(doc, "", "  ")
	p.cfgPath = filepath.Join(filepath.Dir(p.root), "config.json")
	if err := os.WriteFile(p.cfgPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func newPeer(t *testing.T, dir, name string) *peer {
	p := &peer{name: name, root: filepath.Join(dir, name, "root"), dataDir: filepath.Join(dir, name, "data"), socket: filepath.Join(dir, name, "s.sock")}
	if err := os.MkdirAll(p.root, 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestFriendSyncsOnlyWhatTheyAreGiven: a friend's device, not declared
// anywhere on the host, signs in with GateHouse and gets exactly the
// stores its person may read, changes exactly those it may write, and
// loses a store the moment the grant is taken back.
func TestFriendSyncsOnlyWhatTheyAreGiven(t *testing.T) {
	_, daemon := binaries(t)
	dir := t.TempDir()
	ports := freePorts(t, 3)
	hostEP, friendsEP, lainEP := ports[0], ports[1], ports[2]
	stores := map[string]any{"Photos": map[string]any{"group": "g"}, "Music": map[string]any{"group": "g"}, "Notes": map[string]any{"group": "g"}}

	sock, _ := os.MkdirTemp("", "gh")
	t.Cleanup(func() { os.RemoveAll(sock) })
	agent := &fakeAgent{tokens: map[string]string{"lain-token": "lain"}, grants: map[string]map[string]bool{}}
	agent.set("lain", "storeboard.read@Photos", true)
	agent.set("lain", "storeboard.write@Notes", true)
	agent.serve(t, filepath.Join(sock, "agent.sock"))

	host := newPeer(t, dir, "amanita")
	writeDoc(t, host, map[string]any{
		"peers":   map[string]any{"amanita": map[string]any{"endpoints": []string{hostEP}}},
		"groups":  map[string]any{"g": map[string]any{"peers": []string{"amanita"}}},
		"stores":  stores,
		"friends": map[string]any{"listen": []string{friendsEP}},
	})
	startWith(t, daemon, host, "STOREBOARD_GATEHOUSE_SOCKET="+filepath.Join(sock, "agent.sock"))

	gh := fakeGateHouse(t, "lain-token")
	device := func(name, access string) *peer {
		p := newPeer(t, dir, name)
		creds := filepath.Join(dir, name, "signin.json")
		g := gh
		if access != "lain-token" {
			g = fakeGateHouse(t, access)
		}
		b, _ := json.Marshal(map[string]string{"gatehouse": g.URL, "refresh_token": "r1"})
		os.WriteFile(creds, b, 0o600)
		writeDoc(t, p, map[string]any{
			"peers": map[string]any{
				name:      map[string]any{"endpoints": []string{lainEP}},
				"amanita": map[string]any{"endpoints": []string{friendsEP}, "signIn": map[string]string{"gatehouse": g.URL, "credentials": creds}},
			},
			"groups": map[string]any{"g": map[string]any{"peers": []string{name, "amanita"}}},
			"stores": stores,
		})
		startWith(t, daemon, p)
		return p
	}
	lain := device("lain.laptop", "lain-token")

	waitFor(t, "lain's laptop connected as lain", func() bool {
		for _, c := range statusOf(t, host).Connections {
			if c.Peer == "lain.laptop" && c.State == "connected" {
				return true
			}
		}
		return false
	})

	write(t, filepath.Join(host.storeDir("Photos"), "beach.jpg"), "sand")
	write(t, filepath.Join(host.storeDir("Music"), "song.flac"), "private")
	write(t, filepath.Join(host.storeDir("Notes"), "todo.txt"), "milk")
	waitFor(t, "Photos reaches lain", func() bool { b, _ := os.ReadFile(filepath.Join(lain.storeDir("Photos"), "beach.jpg")); return string(b) == "sand" })
	waitFor(t, "Notes reaches lain", func() bool { b, _ := os.ReadFile(filepath.Join(lain.storeDir("Notes"), "todo.txt")); return string(b) == "milk" })

	// She writes where she may, and nowhere else.
	write(t, filepath.Join(lain.storeDir("Notes"), "from-lain.txt"), "hi")
	write(t, filepath.Join(lain.storeDir("Photos"), "sneaky.jpg"), "x")
	waitFor(t, "lain's note reaches the host", func() bool {
		b, _ := os.ReadFile(filepath.Join(host.storeDir("Notes"), "from-lain.txt"))
		return string(b) == "hi"
	})
	time.Sleep(3 * time.Second)
	if _, err := os.Stat(filepath.Join(host.storeDir("Photos"), "sneaky.jpg")); err == nil {
		t.Fatal("a receive-only friend changed Photos")
	}
	if _, err := os.Stat(filepath.Join(lain.storeDir("Music"), "song.flac")); err == nil {
		t.Fatal("Music reached a friend who may not read it")
	}

	// Taken back: what is new on the host no longer goes to her.
	agent.set("lain", "storeboard.read@Photos", false)
	write(t, filepath.Join(host.storeDir("Photos"), "after.jpg"), "later")
	time.Sleep(4 * time.Second)
	if _, err := os.Stat(filepath.Join(lain.storeDir("Photos"), "after.jpg")); err == nil {
		t.Fatal("Photos kept flowing after the grant was taken back")
	}

	// A device whose sign-in GateHouse refuses, and one that claims to be
	// someone else's, are never peers.
	stop(lain)
	device("lain.phone", "forged-token")
	device("mira.phone", "lain-token")
	time.Sleep(3 * time.Second)
	for _, c := range statusOf(t, host).Connections {
		if (c.Peer == "lain.phone" || c.Peer == "mira.phone") && c.State == "connected" {
			t.Fatalf("%s connected", c.Peer)
		}
	}
	if !host.logContains("sign-in refused") || !host.logContains("is not a name of lain's") {
		t.Fatalf("host log does not say why:\n%s", grepLog(host, "rejected"))
	}
}
