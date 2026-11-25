// Package gatehouse asks the grid's GateHouse agent, on this machine, whether
// a person may do something to a store (GateHouse INTEGRATIONS.md). The
// agent answers from its own copy, so this works offline. Any failure (no
// agent, no answer, a uid that is no GateHouse account) is a no: the
// caller's own checks (root, the store's owner) are unchanged.
package gatehouse

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"os/user"
	"strconv"
	"time"
)

// Permissions storeboard checks, each scoped to a store's name.
const (
	Read   = "storeboard.read"
	Write  = "storeboard.write"
	Manage = "storeboard.manage"
)

// Socket is the agent's socket.
func Socket() string {
	if s := os.Getenv("STOREBOARD_GATEHOUSE_SOCKET"); s != "" {
		return s
	}
	return "/run/gatehouse/gatehouse.sock"
}

// Checker answers whether uid may do perm to store.
type Checker func(uid int, perm, store string) bool

// Agent is the Checker that asks the agent at socket.
func Agent(socket string) Checker {
	return func(uid int, perm, store string) bool {
		u, err := user.LookupId(strconv.Itoa(uid))
		if err != nil {
			return false
		}
		return ask(socket, u.Username, perm, store)
	}
}

func ask(socket, name, perm, store string) bool {
	c, err := net.DialTimeout("unix", socket, 2*time.Second)
	if err != nil {
		return false
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	req, _ := json.Marshal(map[string]any{"id": 1, "method": "check",
		"params": map[string]string{"user": name, "perm": perm, "resource": store}})
	if _, err := c.Write(append(req, '\n')); err != nil {
		return false
	}
	line, err := bufio.NewReader(c).ReadBytes('\n')
	if err != nil {
		return false
	}
	var resp struct {
		Result *struct {
			Allowed bool `json:"allowed"`
		} `json:"result"`
	}
	return json.Unmarshal(line, &resp) == nil && resp.Result != nil && resp.Result.Allowed
}
