package gatehouse

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"
)

// fakeAgent allows exactly one (user, perm, store).
func fakeAgent(t *testing.T, user, perm, store string) string {
	dir, _ := os.MkdirTemp("", "gh")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			line, _ := bufio.NewReader(c).ReadBytes('\n')
			var req struct {
				Params map[string]string `json:"params"`
			}
			json.Unmarshal(line, &req)
			ok := req.Params["user"] == user && req.Params["perm"] == perm && req.Params["resource"] == store
			b, _ := json.Marshal(map[string]any{"id": 1, "result": map[string]bool{"allowed": ok}})
			c.Write(append(b, '\n'))
			c.Close()
		}
	}()
	return sock
}

func TestAgent(t *testing.T) {
	me, err := user.Current()
	if err != nil {
		t.Skip("no current user")
	}
	uid, _ := strconv.Atoi(me.Uid)
	check := Agent(fakeAgent(t, me.Username, Write, "Photos"))
	if !check(uid, Write, "Photos") {
		t.Fatal("granted, but refused")
	}
	if check(uid, Write, "Documents") || check(uid, Manage, "Photos") {
		t.Fatal("allowed what was not granted")
	}
	if Agent(filepath.Join(t.TempDir(), "none"))(uid, Write, "Photos") {
		t.Fatal("no agent, but allowed")
	}
}
