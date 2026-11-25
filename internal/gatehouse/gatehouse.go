// Package gatehouse asks the grid's GateHouse agent, on this machine, whether
// a person may do something to a store (GateHouse INTEGRATIONS.md). The
// agent answers from its own copy, so this works offline. Any failure (no
// agent, no answer, a uid that is no GateHouse account) is a no: the
// caller's own checks (root, the store's owner) are unchanged.
package gatehouse

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
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

// People is Agent for a person by GateHouse name rather than a local uid:
// the person behind a friend's device.
func People(socket string) func(person, perm, store string) bool {
	return func(person, perm, store string) bool { return ask(socket, person, perm, store) }
}

// Verifier checks a friend's device's token with the agent (offline: the
// agent holds the server's key) and names its person. The token must be
// for storeboard, and its account still enabled.
func Verifier(socket string) func(token string) (string, error) {
	return func(token string) (string, error) {
		var out struct {
			Name string `json:"name"`
		}
		if err := call(socket, "verify", map[string]string{"token": token, "service": Service}, &out); err != nil {
			return "", err
		}
		if out.Name == "" {
			return "", errors.New("the token names nobody")
		}
		return out.Name, nil
	}
}

// Service is storeboard's name in GateHouse: its tokens' audience and its
// client id.
const Service = "storeboard"

func ask(socket, name, perm, store string) bool {
	var out struct {
		Allowed bool `json:"allowed"`
	}
	err := call(socket, "check", map[string]string{"user": name, "perm": perm, "resource": store}, &out)
	return err == nil && out.Allowed
}

func call(socket, method string, params, result any) error {
	c, err := net.DialTimeout("unix", socket, 2*time.Second)
	if err != nil {
		return fmt.Errorf("GateHouse agent: %w", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	req, _ := json.Marshal(map[string]any{"id": 1, "method": method, "params": params})
	if _, err := c.Write(append(req, '\n')); err != nil {
		return fmt.Errorf("GateHouse agent: %w", err)
	}
	line, err := bufio.NewReader(c).ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("GateHouse agent: %w", err)
	}
	var resp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(line, &resp); err != nil {
		return fmt.Errorf("GateHouse agent: %w", err)
	}
	if resp.Error != nil {
		return errors.New(resp.Error.Message)
	}
	return json.Unmarshal(resp.Result, result)
}
