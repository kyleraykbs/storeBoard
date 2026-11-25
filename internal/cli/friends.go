package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/gatehouse"
)

// openBrowser shows the sign-in address, and opens it where there is a
// desktop to open it on.
func openBrowser(stderr io.Writer) func(string) {
	return func(u string) {
		fmt.Fprintf(stderr, "Sign in at:\n  %s\n", u)
		if os.Getenv("WAYLAND_DISPLAY") != "" || os.Getenv("DISPLAY") != "" {
			if cmd := exec.Command("xdg-open", u); cmd.Start() == nil {
				go cmd.Wait()
			}
		}
	}
}

// loginCmd signs this device in to the grid a peer belongs to (a friend's
// grid), keeping the sign-in where the daemon reads it.
func loginCmd(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	_, positional, _, err := splitFlags(args, "login <peer>", nil, stderr)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		fmt.Fprintln(stderr, "storeboard: usage: storeboard login <peer>")
		return ErrHandled
	}
	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	p, ok := cfg.Peers[positional[0]]
	if !ok || p.SignIn == nil {
		fmt.Fprintf(stderr, "storeboard: %s is not a peer of another grid (no signIn in its declaration)\n", positional[0])
		return ErrHandled
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	who, err := gatehouse.Login(ctx, p.SignIn.GateHouse, p.SignIn.Credentials, openBrowser(stderr))
	if err != nil {
		if os.IsPermission(err) {
			return fmt.Errorf("login: %w (the daemon's sign-ins are root's: run it with sudo)", err)
		}
		return fmt.Errorf("login: %w", err)
	}
	fmt.Fprintf(stdout, "signed in to %s as %s; %s connects with it from now on\n", p.SignIn.GateHouse, who, positional[0])
	return nil
}

// shareCmd lets a person sync one store from their own devices, or stops
// it: GateHouse makes the grant on behalf of whoever runs this, who must
// manage the store (storeboard.manage on it).
func shareCmd(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	var write, revoke bool
	rest := args[:0:0]
	for _, a := range args {
		switch a {
		case "--write":
			write = true
		case "--revoke":
			revoke = true
		default:
			rest = append(rest, a)
		}
	}
	_, positional, values, err := splitFlags(rest, "share <store> <person>", []string{"--gatehouse"}, stderr)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		fmt.Fprintln(stderr, "storeboard: usage: storeboard share <store> <person> [--write] [--revoke] [--gatehouse URL]")
		return ErrHandled
	}
	store, person := positional[0], positional[1]
	gh := values["--gatehouse"]
	if gh == "" {
		cfg, err := config.Load(config.ResolvePath(configPath))
		if err != nil {
			return err
		}
		if cfg.Friends != nil {
			gh = cfg.Friends.GateHouse
		}
	}
	if gh == "" {
		fmt.Fprintln(stderr, "storeboard: which GateHouse? none in the config (friends.gatehouse); give --gatehouse URL")
		return ErrHandled
	}
	state := os.Getenv("XDG_STATE_HOME")
	if state == "" {
		home, _ := os.UserHomeDir()
		state = filepath.Join(home, ".local", "state")
	}
	creds := filepath.Join(state, "storeboard", "gatehouse-signin.json")
	var tokens gatehouse.Tokens
	tok, err := tokens.Token(creds)
	if err != nil {
		lctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
		if _, err := gatehouse.Login(lctx, gh, creds, openBrowser(stderr)); err != nil {
			return fmt.Errorf("sign in: %w", err)
		}
		if tok, err = tokens.Token(creds); err != nil {
			return fmt.Errorf("sign in: %w", err)
		}
	}
	perm := gatehouse.Read
	if write {
		perm = gatehouse.Write
	}
	body, _ := json.Marshal(map[string]any{"service": gatehouse.Service, "user": person, "permission": perm, "scope": store, "revoke": revoke})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(gh, "/")+"/v1/share", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("share: %w", err)
	}
	defer resp.Body.Close()
	var out struct {
		Error string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("share: %s", out.Error)
	}
	switch {
	case revoke:
		fmt.Fprintf(stdout, "%s no longer has %s on %s\n", person, perm, store)
	case write:
		fmt.Fprintf(stdout, "%s may now sync %s from their devices, and change it\n", person, store)
	default:
		fmt.Fprintf(stdout, "%s may now sync %s from their devices (receive only)\n", person, store)
	}
	return nil
}
