// Package cli is the storeboard command set. Every command takes --json:
// the JSON shapes are versioned through pkg/api (and documented by it),
// while human-readable output is free to change between releases.
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
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
  backup <verb>   versions and restores: list a file's recorded history,
                  restore a version, a deleted file included
  store takeover <name>
                  turn a declared symlink path that already holds files
                  into a link to the store, keeping and syncing that content
  send <store> <file>
                  copy a file into a store: it syncs like any other
                  change, and a name already taken is parked, not lost
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
	case "send":
		return sendCmd(ctx, configPath, rest, stdout, stderr)
	case "store":
		return storeCmd(ctx, configPath, rest, stdout, stderr)
	case "backup":
		return backupCmd(ctx, configPath, rest, stdout, stderr)
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
	Path   string       `json:"path"`
	Valid  bool         `json:"valid"`
	Error  string       `json:"error,omitempty"`
	Root   string       `json:"root,omitempty"`
	Socket string       `json:"socket,omitempty"`
	Peer   string       `json:"peer,omitempty"`
	Peers  []string     `json:"peers,omitempty"`
	Groups []string     `json:"groups,omitempty"`
	Stores []checkStore `json:"stores,omitempty"`
}

type checkStore struct {
	Name  string `json:"name"`
	Group string `json:"group"`
	Sync  string `json:"sync"`
	Root  string `json:"root"`
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
	report.Root = cfg.Root
	report.Socket = cfg.Socket
	report.Peer = cfg.Peer
	for _, p := range cfg.PeerList() {
		report.Peers = append(report.Peers, p.Name)
	}
	for _, g := range cfg.GroupList() {
		report.Groups = append(report.Groups, g.Name)
	}
	stores := cfg.Stores()
	report.Stores = make([]checkStore, 0, len(stores))
	for _, s := range stores {
		report.Stores = append(report.Stores, checkStore{Name: s.Name, Group: s.Group, Sync: s.Sync, Root: s.Root})
	}
	if jsonOut {
		return writeJSON(stdout, report)
	}
	fmt.Fprintf(stdout, "config OK: %s (%d stores)\n", path, len(stores))
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

// call sends one request to the daemon over its unix socket and returns the
// body, with the daemon's own error message when it refuses. A non-nil body
// is encoded as JSON.
func call(ctx context.Context, socket, method, path string, body any) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			},
		},
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://storeboard"+path, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(respBody, &e) == nil && e.Error != "" {
			return nil, errors.New(e.Error)
		}
		return nil, fmt.Errorf("daemon answered %s", resp.Status)
	}
	return respBody, nil
}

func fetch(ctx context.Context, socket, path string) ([]byte, error) {
	return call(ctx, socket, http.MethodGet, path, nil)
}

// backupCmd is the backup verb group: list a file's recorded versions and
// restore one of them.
func backupCmd(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, "storeboard: usage: storeboard backup <list|restore> …\n")
		return ErrHandled
	}
	verb, rest := args[0], args[1:]
	switch verb {
	case "list":
		return backupList(ctx, configPath, rest, stdout, stderr)
	case "restore":
		return backupRestore(ctx, configPath, rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "storeboard: unknown backup command %q\n\nusage: storeboard backup <list|restore> …\n", verb)
		return ErrHandled
	}
}

// splitFlags separates the shared flags from positional arguments, so a
// command can take both without the flag parser tripping over the names.
func splitFlags(args []string, command string, stderr io.Writer) (bool, []string, error) {
	var flags, positional []string
	for _, arg := range args {
		if arg == "--json" || arg == "-h" || arg == "--help" {
			flags = append(flags, arg)
			continue
		}
		positional = append(positional, arg)
	}
	jsonOut, err := jsonFlag(flags, command, stderr)
	return jsonOut, positional, err
}

func backupList(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	jsonOut, positional, err := splitFlags(args, "backup list <store> [path]", stderr)
	if err != nil {
		return err
	}
	if len(positional) < 1 || len(positional) > 2 {
		fmt.Fprintln(stderr, "storeboard: usage: storeboard backup list <store> [path] [--json]")
		return ErrHandled
	}
	store := positional[0]
	path := ""
	if len(positional) == 2 {
		path = positional[1]
	}

	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	query := "/v1/stores/" + url.PathEscape(store) + "/versions"
	if path != "" {
		query += "?path=" + url.QueryEscape(path)
	}
	body, err := fetch(ctx, cfg.Socket, query)
	if err != nil {
		return fmt.Errorf("backup list %s: %w", store, err)
	}
	if jsonOut {
		_, err := stdout.Write(body)
		return err
	}
	var vs api.Versions
	if err := json.Unmarshal(body, &vs); err != nil {
		return fmt.Errorf("parse versions: %w", err)
	}
	kept := vs.Keep
	if kept == "" {
		kept = "1y"
	}
	what := store
	if path != "" {
		what += " " + path
	}
	if len(vs.Versions) == 0 {
		fmt.Fprintf(stdout, "%s: no recorded versions (keeping %s)\n", what, kept)
		return nil
	}
	fmt.Fprintf(stdout, "%s: %d recorded versions, keeping %s\n", what, len(vs.Versions), kept)
	for _, v := range vs.Versions {
		detail := fmt.Sprintf("%8d bytes", v.Size)
		if v.Deleted {
			detail = "deleted"
		}
		line := fmt.Sprintf("  %s  %s", v.Time.Format(time.RFC3339), detail)
		if v.Blob != "" {
			line += "  " + shortBlob(v.Blob)
		}
		if v.Path != "" && path == "" {
			line += "  " + v.Path
		}
		fmt.Fprintln(stdout, line)
	}
	return nil
}

func backupRestore(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	jsonOut, positional, err := splitFlags(args, "backup restore <store> <path>", stderr)
	if err != nil {
		return err
	}
	if len(positional) < 2 {
		fmt.Fprintln(stderr, "storeboard: usage: storeboard backup restore <store> <path> [--blob HASH] [--at RFC3339] [--to PATH] [--json]")
		return ErrHandled
	}
	req := struct {
		Path string `json:"path"`
		Blob string `json:"blob,omitempty"`
		At   string `json:"at,omitempty"`
		To   string `json:"to,omitempty"`
	}{Path: positional[1]}
	store := positional[0]

	// The value flags take an argument, which is why this walks by index.
	for i := range args {
		want := ""
		switch args[i] {
		case "--blob":
			want = "1"
		case "--at":
			want = "2"
		case "--to":
			want = "3"
		default:
			continue
		}
		if i+1 >= len(args) {
			return usageErr(stderr, "backup restore <store> <path>", args[i]+" needs a value")
		}
		switch want {
		case "1":
			req.Blob = args[i+1]
		case "2":
			req.At = args[i+1]
		case "3":
			req.To = args[i+1]
		}
	}

	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	body, err := call(ctx, cfg.Socket, http.MethodPost, "/v1/stores/"+url.PathEscape(store)+"/restore", req)
	if err != nil {
		return fmt.Errorf("backup restore %s: %w", store, err)
	}
	if jsonOut {
		_, err := stdout.Write(body)
		return err
	}
	var res api.RestoreResult
	if err := json.Unmarshal(body, &res); err != nil {
		return fmt.Errorf("parse restore result: %w", err)
	}
	if res.Copied {
		fmt.Fprintf(stdout, "%s: wrote the version of %s to %s (the store is untouched)\n", store, positional[1], res.Path)
		return nil
	}
	fmt.Fprintf(stdout, "%s: restored %s from %s; it is a new version and syncs like any other change\n",
		store, res.Path, res.At.Format(time.RFC3339))
	return nil
}

func usageErr(stderr io.Writer, command, problem string) error {
	fmt.Fprintf(stderr, "storeboard: %s: %s\n", command, problem)
	return ErrHandled
}

func shortBlob(blob string) string {
	if len(blob) > 12 {
		return blob[:12]
	}
	return blob
}

// sendCmd is the send verb: copy a local file into a store, which is how a
// share sheet and a desktop both put a file on the mesh. The daemon does the
// copying, so the file must be one it can see — the same machine, in
// practice. Nothing is overwritten: a taken name is parked and reported.
func sendCmd(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	jsonOut, positional, err := splitFlags(args, "send <store> <file>", stderr)
	if err != nil {
		return err
	}
	if len(positional) < 2 {
		fmt.Fprintln(stderr, "storeboard: usage: storeboard send <store> <file> [--as PATH] [--json]")
		return ErrHandled
	}
	store := positional[0]
	req := struct {
		Source string `json:"source"`
		Path   string `json:"path,omitempty"`
	}{Source: positional[1]}

	// --as takes a value, which is why this walks by index.
	for i := range args {
		if args[i] != "--as" {
			continue
		}
		if i+1 >= len(args) {
			return usageErr(stderr, "send <store> <file>", "--as needs a value")
		}
		req.Path = args[i+1]
	}

	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	body, err := call(ctx, cfg.Socket, http.MethodPost, "/v1/stores/"+url.PathEscape(store)+"/send", req)
	if err != nil {
		return fmt.Errorf("send %s: %w", store, err)
	}
	if jsonOut {
		_, err := stdout.Write(body)
		return err
	}
	var res api.SendResult
	if err := json.Unmarshal(body, &res); err != nil {
		return fmt.Errorf("parse send result: %w", err)
	}
	if res.Parked != "" {
		fmt.Fprintf(stdout, "%s: sent %s (%d bytes); the file already there is parked at %s\n",
			store, res.Path, res.Size, res.Parked)
		return nil
	}
	fmt.Fprintf(stdout, "%s: sent %s (%d bytes); it is a new change and syncs like any other\n",
		store, res.Path, res.Size)
	return nil
}

// storeCmd is the store verb group: `store takeover <name>` converts a
// declared symlink path that is busy because it holds the user's own content
// into a link to the store, moving that content into the canonical dir and
// letting the normal scan take it from there.
func storeCmd(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, "storeboard: usage: storeboard store takeover <name> [--json]\n")
		return ErrHandled
	}
	verb, rest := args[0], args[1:]
	switch verb {
	case "takeover":
		return storeTakeover(ctx, configPath, rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "storeboard: unknown store command %q\n\nusage: storeboard store takeover <name> [--json]\n", verb)
		return ErrHandled
	}
}

func storeTakeover(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	// Split the positional name from the flags; --path takes a value.
	var name, linkPath string
	var flags []string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--json", "-h", "--help":
			flags = append(flags, args[i])
		case "--path":
			if i+1 >= len(args) {
				return usageErr(stderr, "store takeover <name>", "--path needs a value")
			}
			linkPath = args[i+1]
			i++
		default:
			if name != "" {
				fmt.Fprintf(stderr, "storeboard: store takeover: unexpected argument %q\n", args[i])
				return ErrHandled
			}
			name = args[i]
		}
	}
	jsonOut, err := jsonFlag(flags, "store takeover <name>", stderr)
	if err != nil {
		return err
	}
	if name == "" {
		fmt.Fprintln(stderr, "storeboard: usage: storeboard store takeover <name> [--path PATH] [--json]")
		return ErrHandled
	}

	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	req := struct {
		Path string `json:"path,omitempty"`
	}{Path: linkPath}
	body, err := call(ctx, cfg.Socket, http.MethodPost, "/v1/stores/"+url.PathEscape(name)+"/takeover", req)
	if err != nil {
		return fmt.Errorf("takeover %s: %w", name, err)
	}
	if jsonOut {
		_, err := stdout.Write(body)
		return err
	}
	var res api.TakeoverResult
	if err := json.Unmarshal(body, &res); err != nil {
		return fmt.Errorf("parse takeover result: %w", err)
	}
	if res.Moved == 0 {
		fmt.Fprintf(stdout, "%s: %s is already a link (%s), nothing to take over\n", res.Store, res.Path, res.State)
		return nil
	}
	fmt.Fprintf(stdout, "%s: moved %d entries from %s into %s; the next scan syncs them\n",
		res.Store, res.Moved, res.Path, res.Root)
	return nil
}

func renderStatus(w io.Writer, st *api.Status) {
	fmt.Fprintf(w, "storeboard %s (api v%d), up %s\n",
		st.Version, st.APIVersion, time.Duration(st.UptimeSeconds)*time.Second)
	if st.Peer != "" {
		fmt.Fprintf(w, "peer    %s\n", st.Peer)
	}
	fmt.Fprintf(w, "socket  %s\n", st.Socket)
	fmt.Fprintf(w, "data    %s\n", st.DataDir)
	fmt.Fprintf(w, "root    %s\n", st.Root)

	fmt.Fprintf(w, "\npeers (%d):\n", len(st.Peers))
	if len(st.Peers) == 0 {
		fmt.Fprintln(w, "  (none)")
	}
	for _, p := range st.Peers {
		endpoints := "-"
		if len(p.Endpoints) > 0 {
			endpoints = strings.Join(p.Endpoints, " ")
		}
		fmt.Fprintf(w, "  %s\n    %s\n", p.Name, endpoints)
	}

	fmt.Fprintf(w, "\ngroups (%d):\n", len(st.Groups))
	if len(st.Groups) == 0 {
		fmt.Fprintln(w, "  (none)")
	}
	for _, g := range st.Groups {
		fmt.Fprintf(w, "  %s\n    peers  %s\n    stores %s\n",
			g.Name, strings.Join(g.Peers, ", "), strings.Join(g.Stores, ", "))
	}

	fmt.Fprintf(w, "\nstores (%d):\n", len(st.Stores))
	if len(st.Stores) == 0 {
		fmt.Fprintln(w, "  (none)")
	}
	for _, s := range st.Stores {
		where := s.Root
		if s.Parent != "" {
			where = s.Parent + "/" + s.Path + " -> " + s.Root
		}
		fmt.Fprintf(w, "  %s\n    group %s\n    sync  %s\n    root  %s\n", s.Name, s.Group, s.Sync, where)
		if len(s.Symlinks) > 0 {
			fmt.Fprintf(w, "    links %s\n", strings.Join(s.Symlinks, " "))
		}
	}

	fmt.Fprintf(w, "\nconnections (%d):\n", len(st.Connections))
	if len(st.Connections) == 0 {
		fmt.Fprintln(w, "  (none)")
	}
	for _, c := range st.Connections {
		detail := c.State
		if c.State != "connected" && c.Error != "" {
			detail = fmt.Sprintf("%s  (%s)", c.State, c.Error)
		}
		fmt.Fprintf(w, "  %s\n    %s\n", c.Peer, detail)
	}

	fmt.Fprintf(w, "\nsync (%d):\n", len(st.Sync))
	if len(st.Sync) == 0 {
		fmt.Fprintln(w, "  (none)")
	}
	for _, s := range st.Sync {
		detail := fmt.Sprintf("%s, %d entries", s.State, s.Entries)
		if s.Conflicts > 0 {
			detail += fmt.Sprintf(", %d conflicts", s.Conflicts)
		}
		if s.Permissions.Mode != "" {
			detail += fmt.Sprintf(", %s:%s %s", s.Permissions.User, s.Permissions.Group, s.Permissions.Mode)
		}
		fmt.Fprintf(w, "  %s\n    %s\n", s.Store, detail)
		if s.Scanning || s.Pending > 0 || s.Receiving > 0 {
			// The grind, visible: what a large store is doing right now.
			line := fmt.Sprintf("%d/%d local", s.Local, s.Entries)
			if s.Scanning {
				line += ", scanning"
			}
			if s.Receiving > 0 {
				line += fmt.Sprintf(", receiving %d", s.Receiving)
			}
			if s.Pending > 0 {
				line += fmt.Sprintf(" (%d to fetch)", s.Pending)
			}
			fmt.Fprintf(w, "    %s\n", line)
		}
		for _, link := range s.Symlinks {
			if link.State != api.SymlinkBusy {
				continue
			}
			// The path belongs to something else — the one thing the user
			// needs next is which something, and how to hand it over when it
			// is their own content.
			fmt.Fprintf(w, "    not linked: %s (%s)", link.Path, link.Busy)
			if link.Busy == api.BusyContent {
				fmt.Fprintf(w, " — storeboard store takeover %s --path %s", s.Store, link.Path)
			}
			fmt.Fprintln(w)
		}
	}
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
