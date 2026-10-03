// Package cli is the storeboard command set. Every command takes --json:
// the JSON shapes are versioned through pkg/api (and documented by it),
// while human-readable output is free to change between releases.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/version"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// ErrHandled reports a failure the command has already written to its own
// output; main exits 1 without printing anything further.
var ErrHandled = errors.New("command failed")

const usage = `usage: storeboard [--config FILE] <command> [arguments]

Commands:
  config check    validate the configuration file
  status          show the daemon's status document
  version         print version information
  help            show this help

Every command accepts --json for machine-readable output.
`

// Run executes one CLI invocation. configPath is the already-parsed
// --config flag. stdout carries reports and data, stderr diagnostics — a
// script can pipe stdout into jq without filtering anything out.
func Run(ctx context.Context, args []string, configPath string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return ErrHandled
	}

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return nil
	case "version":
		return versionCmd(rest, stdout, stderr)
	case "config":
		if len(rest) == 0 || rest[0] != "check" {
			fmt.Fprintln(stderr, "storeboard: usage: storeboard config check [--json]")
			return ErrHandled
		}
		return configCheck(configPath, rest[1:], stdout, stderr)
	case "status":
		return status(ctx, configPath, rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "storeboard: unknown command %q\n\n%s", cmd, usage)
		return ErrHandled
	}
}

// jsonFlag parses the flag every command shares and rejects anything else.
// The usage string names the full command so an error reads as typed.
func jsonFlag(args []string, command string, stderr io.Writer) (bool, error) {
	jsonOut := false
	for _, arg := range args {
		switch arg {
		case "--json":
			jsonOut = true
		case "-h", "--help":
			fmt.Fprintf(stderr, "usage: storeboard %s [--json]\n", command)
			return false, ErrHandled
		default:
			fmt.Fprintf(stderr, "storeboard: %s: unexpected argument %q\n", command, arg)
			return false, ErrHandled
		}
	}
	return jsonOut, nil
}

func versionCmd(args []string, stdout, stderr io.Writer) error {
	jsonOut, err := jsonFlag(args, "version", stderr)
	if err != nil {
		return err
	}
	if jsonOut {
		return writeJSON(stdout, struct {
			Name       string `json:"name"`
			Version    string `json:"version"`
			APIVersion int    `json:"apiVersion"`
		}{"storeboard", version.Version, api.Version})
	}
	fmt.Fprintf(stdout, "storeboard %s (api v%d)\n", version.Version, api.Version)
	return nil
}

// checkReport is `config check --json` output. A failed check is still a
// well-formed document — "valid": false plus the error — so the command is
// scriptable in both directions.
type checkReport struct {
	Path    string   `json:"path"`
	Valid   bool     `json:"valid"`
	Error   string   `json:"error,omitempty"`
	Socket  string   `json:"socket,omitempty"`
	Folders []string `json:"folders,omitempty"`
}

// configCheck validates a real file: a missing file is a failure, not
// "defaults are fine" — the caller asked to check something and there is
// nothing there.
func configCheck(configPath string, args []string, stdout, stderr io.Writer) error {
	jsonOut, err := jsonFlag(args, "config check", stderr)
	if err != nil {
		return err
	}

	path := config.ResolvePath(configPath)
	if path == "" {
		path = config.DefaultPath()
	}
	report := checkReport{Path: path}

	fail := func(err error) error {
		report.Error = err.Error()
		if jsonOut {
			if werr := writeJSON(stdout, report); werr != nil {
				return werr
			}
		} else {
			fmt.Fprintf(stderr, "config check: %v\n", err)
		}
		return ErrHandled
	}

	if path == "" {
		return fail(errors.New("cannot determine config directory; pass --config"))
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return fail(fmt.Errorf("no config file at %s", path))
		}
		return fail(fmt.Errorf("stat %s: %w", path, err))
	}
	cfg, err := config.Load(path)
	if err != nil {
		return fail(err)
	}

	report.Valid = true
	report.Socket = cfg.Socket
	report.Folders = make([]string, 0, len(cfg.Folders))
	for _, f := range cfg.Folders {
		report.Folders = append(report.Folders, f.Name)
	}
	if jsonOut {
		return writeJSON(stdout, report)
	}
	fmt.Fprintf(stdout, "config OK: %s (%d folders)\n", path, len(cfg.Folders))
	return nil
}

func status(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	jsonOut, err := jsonFlag(args, "status", stderr)
	if err != nil {
		return err
	}

	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}

	body, err := fetch(ctx, cfg.Socket, "/v1/status")
	if err != nil {
		return fmt.Errorf("cannot reach storeboardd at %s: %w (is it running?)", cfg.Socket, err)
	}
	if jsonOut {
		// The document passes through untouched: the daemon owns its shape,
		// and `storeboard status --json` is that document verbatim.
		_, err := stdout.Write(body)
		return err
	}
	var st api.Status
	if err := json.Unmarshal(body, &st); err != nil {
		return fmt.Errorf("parse status document: %w", err)
	}
	renderStatus(stdout, &st)
	return nil
}

// fetch GETs one path from the daemon over its unix socket.
func fetch(ctx context.Context, socket, path string) ([]byte, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			},
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://storeboard"+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("daemon answered %s", resp.Status)
	}
	return body, nil
}

func renderStatus(w io.Writer, st *api.Status) {
	fmt.Fprintf(w, "storeboard %s (api v%d), up %s\n",
		st.Version, st.APIVersion, time.Duration(st.UptimeSeconds)*time.Second)
	fmt.Fprintf(w, "socket  %s\n", st.Socket)
	fmt.Fprintf(w, "data    %s\n", st.DataDir)
	fmt.Fprintf(w, "\nfolders (%d):\n", len(st.Folders))
	if len(st.Folders) == 0 {
		fmt.Fprintln(w, "  (none)")
		return
	}
	for _, f := range st.Folders {
		fmt.Fprintf(w, "  %s\n    %s\n", f.Name, f.Path)
	}
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
