// Package cli is the storeboard command set. Every command takes --json:
// the JSON shapes are versioned through pkg/api (and documented by it),
// while human-readable output is free to change between releases.
package cli

import (
	"bufio"
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
	"path/filepath"
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
  send <store> <file> [--as PATH]
                  copy a file of yours into a store (the original stays
                  where it is); a name already taken is parked, not lost
  space           show how much disk each data category and each
                  unconfigured store uses
  cleanup [status]
                  show what there is to clean up: each store's trash and
                  history, stores no longer in the config; changes nothing
  cleanup trash [STORE...] [--older-than DUR]
                  empty the trash now (all of it, or what is older)
  cleanup history [STORE...] [--older-than DUR] [--current]
                  drop recorded versions; --current also drops the backup
                  copy of each file as it is now
  cleanup orphans [STORE...] [--force]
                  forget stores no longer in the config (root only); their
                  folders under the root are left for you
                  These three show what would go and ask first; --dry-run
                  only shows, --yes skips the question.
  status          show the daemon's status document
  doctor          check the daemon over and explain anything wrong:
                  mounts, queues, peers, clocks, disk, stalled transfers
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
	case "space":
		return spaceCmd(ctx, configPath, rest, stdout, stderr)
	case "cleanup":
		return cleanupCmd(ctx, configPath, rest, stdout, stderr)
	case "status":
		return status(ctx, configPath, rest, stdout, stderr)
	case "doctor":
		return doctorCmd(ctx, configPath, rest, stdout, stderr)
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

	body, err := fetch(ctx, cfg.Socket, "/v1/status", statusTimeout)
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

// doctorCmd prints the daemon's health report. It exits non-zero when any
// check failed, so it can gate a deploy or a timer; warnings alone do not.
// A config that does not load or a daemon that does not answer are findings
// too, reported in the same shape.
func doctorCmd(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	jsonOut, err := jsonFlag(args, "doctor", stderr)
	if err != nil {
		return err
	}
	var doc api.Doctor
	path := config.ResolvePath(configPath)
	cfg, err := config.Load(path)
	switch {
	case err != nil:
		doc = api.Doctor{APIVersion: api.Version, Status: api.CheckFail, Checks: []api.Check{{
			Name: "config", Status: api.CheckFail, Detail: err.Error(), Hint: "fix it, then `storeboard config check`",
		}}}
	default:
		body, ferr := fetch(ctx, cfg.Socket, "/v1/doctor", statusTimeout)
		if ferr == nil {
			ferr = json.Unmarshal(body, &doc)
		}
		if ferr != nil {
			doc = api.Doctor{APIVersion: api.Version, Status: api.CheckFail, Checks: []api.Check{{
				Name: "daemon", Status: api.CheckFail, Detail: fmt.Sprintf("cannot reach storeboardd at %s: %v", cfg.Socket, ferr),
				Hint: "is it running? `systemctl status storeboard`",
			}}}
		}
	}
	if jsonOut {
		if err := writeJSON(stdout, doc); err != nil {
			return err
		}
	} else {
		renderDoctor(stdout, &doc)
	}
	if doc.Status == api.CheckFail {
		return ErrHandled
	}
	return nil
}

func renderDoctor(w io.Writer, doc *api.Doctor) {
	width := 0
	for _, c := range doc.Checks {
		width = max(width, len(c.Name))
	}
	warns, fails := 0, 0
	for _, c := range doc.Checks {
		mark := "ok  "
		switch c.Status {
		case api.CheckWarn:
			mark = "warn"
			warns++
		case api.CheckFail:
			mark = "FAIL"
			fails++
		}
		fmt.Fprintf(w, "%s  %-*s  %s\n", mark, width, c.Name, c.Detail)
		if c.Hint != "" && c.Status != api.CheckOK {
			fmt.Fprintf(w, "      %-*s  -> %s\n", width, "", c.Hint)
		}
	}
	switch {
	case fails > 0:
		fmt.Fprintf(w, "\n%d failed, %d warnings\n", fails, warns)
	case warns > 0:
		fmt.Fprintf(w, "\n%d warnings\n", warns)
	default:
		fmt.Fprintln(w, "\nall good")
	}
}

// spaceCmd shows how much disk each category of daemon state uses.
func spaceCmd(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	jsonOut, err := jsonFlag(args, "space", stderr)
	if err != nil {
		return err
	}
	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	body, err := fetch(ctx, cfg.Socket, "/v1/space", statusTimeout)
	if err != nil {
		return fmt.Errorf("cannot reach storeboardd at %s: %w (is it running?)", cfg.Socket, err)
	}
	if jsonOut {
		_, err := stdout.Write(body)
		return err
	}
	var sp api.Space
	if err := json.Unmarshal(body, &sp); err != nil {
		return fmt.Errorf("parse space document: %w", err)
	}
	renderSpace(stdout, &sp)
	return nil
}

// cleanupCmd runs routine cleanup, or one of the explicit ones. An explicit
// cleanup is planned first (a dry run at the daemon) and shown, then asked
// about, unless --yes or --dry-run says otherwise.
func cleanupCmd(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	const use = "usage: storeboard cleanup [status|trash|history|orphans] [STORE...] [--older-than DUR] [--current] [--force] [--dry-run] [--yes] [--json]"
	var req api.CleanupRequest
	jsonOut, yes := false, false
	if len(args) == 0 || strings.HasPrefix(args[0], "-") || args[0] == "status" {
		if len(args) > 0 && args[0] == "status" {
			args = args[1:]
		}
		for _, a := range args {
			if a == "--remove-orphans" {
				fmt.Fprintln(stderr, "storeboard: cleanup: --remove-orphans is gone; use `storeboard cleanup orphans`")
				return ErrHandled
			}
		}
		jsonOut, err := jsonFlag(args, "cleanup status", stderr)
		if err != nil {
			return err
		}
		return cleanupStatusCmd(ctx, configPath, jsonOut, stdout)
	}
	if len(args) > 0 {
		switch args[0] {
		case "trash", "history", "orphans":
			req.What = args[0]
			args = args[1:]
		default:
			fmt.Fprintf(stderr, "storeboard: cleanup: unknown kind %q: want trash, history or orphans\n", args[0])
			return ErrHandled
		}
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--remove-orphans":
			fmt.Fprintln(stderr, "storeboard: cleanup: --remove-orphans is gone; use `storeboard cleanup orphans`")
			return ErrHandled
		case arg == "--json":
			jsonOut = true
		case arg == "--yes" || arg == "-y":
			yes = true
		case arg == "--dry-run" || arg == "-n":
			req.DryRun = true
		case arg == "--current":
			req.Current = true
		case arg == "--force":
			req.Force = true
		case arg == "--older-than" && i+1 < len(args):
			i++
			req.OlderThan = args[i]
		case strings.HasPrefix(arg, "--older-than="):
			req.OlderThan = strings.TrimPrefix(arg, "--older-than=")
		case arg == "-h" || arg == "--help":
			fmt.Fprintln(stderr, use)
			return ErrHandled
		case strings.HasPrefix(arg, "-"):
			fmt.Fprintf(stderr, "storeboard: cleanup: unexpected argument %q\n%s\n", arg, use)
			return ErrHandled
		case req.What != "":
			req.Stores = append(req.Stores, arg)
		default:
			fmt.Fprintf(stderr, "storeboard: cleanup: unexpected argument %q\n%s\n", arg, use)
			return ErrHandled
		}
	}
	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	run := func(r api.CleanupRequest) (*api.CleanupResult, []byte, error) {
		body, err := call(ctx, cfg.Socket, http.MethodPost, "/v1/cleanup", r, noTimeout)
		if err != nil {
			return nil, nil, fmt.Errorf("cleanup: %w", err)
		}
		var res api.CleanupResult
		if err := json.Unmarshal(body, &res); err != nil {
			return nil, nil, fmt.Errorf("parse cleanup result: %w", err)
		}
		if res.What != r.What || res.DryRun != r.DryRun {
			// A daemon older than these kinds ignores the fields and runs a
			// routine cleanup instead; say so rather than report it as this.
			return nil, nil, fmt.Errorf("cleanup: the daemon does not know `cleanup %s`; update and restart storeboardd", r.What)
		}
		return &res, body, nil
	}

	if !req.DryRun && !yes {
		// Plan, show, ask.
		plan := req
		plan.DryRun = true
		res, _, err := run(plan)
		if err != nil {
			return err
		}
		renderCleanup(stderr, res)
		if cleanupIsEmpty(res) {
			fmt.Fprintln(stderr, "nothing to clean up")
			return nil
		}
		if !stdinIsTerminal() {
			fmt.Fprintln(stderr, "storeboard: cleanup: not a terminal to ask on; pass --yes to go ahead")
			return ErrHandled
		}
		fmt.Fprint(stderr, "Delete this for good? [y/N] ")
		answer, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if a := strings.ToLower(strings.TrimSpace(answer)); a != "y" && a != "yes" {
			fmt.Fprintln(stderr, "left as it was")
			return ErrHandled
		}
	}
	res, body, err := run(req)
	if err != nil {
		return err
	}
	if jsonOut {
		_, err := stdout.Write(body)
		return err
	}
	renderCleanup(stdout, res)
	return nil
}

// cleanupStatusCmd shows what there is to clean up.
func cleanupStatusCmd(ctx context.Context, configPath string, jsonOut bool, stdout io.Writer) error {
	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	body, err := call(ctx, cfg.Socket, http.MethodGet, "/v1/cleanup", nil, listTimeout)
	if err != nil {
		if strings.Contains(err.Error(), "method not allowed") {
			return fmt.Errorf("cleanup status: the daemon predates it; update and restart storeboardd")
		}
		return fmt.Errorf("cleanup status: %w", err)
	}
	if jsonOut {
		_, err := stdout.Write(body)
		return err
	}
	var st api.CleanupStatus
	if err := json.Unmarshal(body, &st); err != nil {
		return fmt.Errorf("parse cleanup status: %w", err)
	}
	renderCleanupStatus(stdout, &st)
	return nil
}

// renderCleanupStatus prints the stores with something to clean, then the
// leftover stores, then the commands that would act on them.
func renderCleanupStatus(w io.Writer, st *api.CleanupStatus) {
	day := func(rfc string) string {
		if t, err := time.Parse(time.RFC3339, rfc); err == nil {
			return t.Local().Format("2006-01-02")
		}
		return rfc
	}
	var total int64
	fmt.Fprintln(w, "trash (expires on its own after trash.keep):")
	any := false
	for _, s := range st.Stores {
		if s.TrashFiles == 0 {
			continue
		}
		any = true
		total += s.TrashBytes
		fmt.Fprintf(w, "  %-16s %10s  %6d files  oldest %s, next expiry %s (keep %s)\n",
			s.Store, humanBytes(s.TrashBytes), s.TrashFiles, day(s.TrashOldest), day(s.TrashNextExpiry), keepWords(s.TrashKeep))
	}
	if !any {
		fmt.Fprintln(w, "  empty")
	}
	fmt.Fprintln(w, "history (thinned hourly; nothing kept past backup.keep except current copies):")
	any = false
	for _, s := range st.Stores {
		if s.OldVersions == 0 && s.CurrentCopies == 0 {
			continue
		}
		any = true
		total += s.OldVersionBytes + s.CurrentCopyBytes
		fmt.Fprintf(w, "  %-16s old versions %7d  %10s   current copies %7d  %10s\n",
			s.Store, s.OldVersions, humanBytes(s.OldVersionBytes), s.CurrentCopies, humanBytes(s.CurrentCopyBytes))
	}
	if !any {
		fmt.Fprintln(w, "  none")
	}
	fmt.Fprintln(w, "stores no longer in the config (kept until you forget them):")
	if len(st.Orphans) == 0 {
		fmt.Fprintln(w, "  none")
	}
	for _, o := range st.Orphans {
		total += o.DataBytes + o.ContentBytes
		fmt.Fprintf(w, "  %-16s %10s", o.Store, humanBytes(o.DataBytes+o.ContentBytes))
		if o.Folder != "" && o.FolderFiles > 0 {
			fmt.Fprintf(w, "   folder %s: %d files, %s (left to you)", o.Folder, o.FolderFiles, humanBytes(o.FolderBytes))
		}
		if o.UnsentWrites > 0 {
			fmt.Fprintf(w, "   %d UNSENT WRITES", o.UnsentWrites)
		}
		fmt.Fprintln(w)
	}
	if st.UnreferencedBytes > 0 {
		fmt.Fprintf(w, "unreferenced content: %d blobs, %s (collected within the hour)\n", st.UnreferencedBlobs, humanBytes(st.UnreferencedBytes))
	}
	if st.SharedBytes > 0 {
		fmt.Fprintf(w, "shared content: %s (named by more than one of the above; freed once all of them go)\n", humanBytes(st.SharedBytes))
	}
	fmt.Fprintf(w, "reclaimable: %s\n", humanBytes(total))
	fmt.Fprintln(w, "free it with: storeboard cleanup trash|history [STORE...] [--older-than DUR] [--current], sudo storeboard cleanup orphans [STORE...]")
}

// keepWords turns a Go duration ("168h0m0s") back into days.
func keepWords(d string) string {
	dur, err := time.ParseDuration(d)
	if err != nil || dur < 24*time.Hour {
		return d
	}
	return fmt.Sprintf("%dd", int(dur.Hours()/24))
}

func cleanupIsEmpty(res *api.CleanupResult) bool {
	if res.Blobs > 0 || res.TrashFiles > 0 || res.Versions > 0 {
		return false
	}
	for _, it := range res.Items {
		if it.Files > 0 || it.Bytes > 0 {
			return false
		}
	}
	return len(res.OrphanStores) == 0
}

func stdinIsTerminal() bool {
	fi, err := os.Stdin.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// renderSpace prints a human-readable data directory breakdown.
func renderSpace(w io.Writer, sp *api.Space) {
	fmt.Fprintf(w, "dataDir: %s (%s)\n", sp.DataDir, humanBytes(sp.TotalBytes))
	for _, ent := range sp.Entries {
		orphan := ""
		if !ent.Configured {
			orphan = " (not in config)"
		}
		fmt.Fprintf(w, "  %-40s %10s%s\n", ent.Path, humanBytes(ent.Bytes), orphan)
	}
}

// renderCleanup prints what a cleanup removed, or would remove.
func renderCleanup(w io.Writer, res *api.CleanupResult) {
	verb := func(done, would string) string {
		if res.DryRun {
			return would
		}
		return done
	}
	if res.DryRun {
		fmt.Fprintln(w, "would remove:")
	}
	var direct int64
	for _, it := range res.Items {
		switch it.What {
		case "trash":
			fmt.Fprintf(w, "  trash/%-24s %6d files  %10s\n", it.Store, it.Files, humanBytes(it.Bytes))
		case "history":
			fmt.Fprintf(w, "  history/%-22s %6d versions\n", it.Store, it.Files)
		case "orphans":
			fmt.Fprintf(w, "  store %-25s %6d files  %10s\n", it.Store, it.Files, humanBytes(it.Bytes))
		}
		if it.Note != "" {
			fmt.Fprintf(w, "    %s\n", it.Note)
		}
		direct += it.Bytes
	}
	if len(res.Items) == 0 {
		fmt.Fprintln(w, "  nothing")
	}
	fmt.Fprintf(w, "content no longer referenced: %d blobs (%s)\n", res.Blobs, humanBytes(res.BlobBytes))
	fmt.Fprintf(w, "%s %s in all\n", verb("freed", "would free"), humanBytes(direct+res.BlobBytes))
}

func humanBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// Client timeouts. Status-grade reads answer from memory and a daemon that
// cannot answer them in seconds is wedged, so they fail fast. The acting verbs
// move real bytes — a send of a video, a takeover of a photo library, a restore
// through a holder — and a fixed deadline only ever cut them off half done; they
// run until they finish or the user interrupts (ctx).
const (
	statusTimeout = 10 * time.Second
	listTimeout   = 2 * time.Minute
	noTimeout     = time.Duration(0)
)

// call sends one request to the daemon over its unix socket and returns the
// body, with the daemon's own error message when it refuses. A non-nil body
// is encoded as JSON. A zero timeout means none beyond ctx.
func call(ctx context.Context, socket, method, path string, body any, timeout time.Duration) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	client := &http.Client{
		Timeout: timeout,
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

func fetch(ctx context.Context, socket, path string, timeout time.Duration) ([]byte, error) {
	return call(ctx, socket, http.MethodGet, path, nil, timeout)
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

// splitFlags separates flags from positional arguments, so a command can take
// both in any order. valueFlags are the flags that take a value (`--as x` or
// `--as=x`); the value is consumed with its flag. The old splitter knew only
// --json, so `send s f --as x` read `--as` and `x` as two more positionals and
// silently ignored them, and a typo'd flag became a path. Now any other
// argument starting with "-" is an error, and `--` ends the flags for a path
// that really does start with one.
func splitFlags(args []string, command string, valueFlags []string, stderr io.Writer) (bool, []string, map[string]string, error) {
	var flags, positional []string
	values := map[string]string{}
	takesValue := func(name string) bool {
		for _, f := range valueFlags {
			if f == name {
				return true
			}
		}
		return false
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--":
			positional = append(positional, args[i+1:]...)
			i = len(args)
		case arg == "--json" || arg == "-h" || arg == "--help":
			flags = append(flags, arg)
		case strings.HasPrefix(arg, "-") && arg != "-":
			name, value, inline := strings.Cut(arg, "=")
			if !takesValue(name) {
				fmt.Fprintf(stderr, "storeboard: %s: unknown flag %q\n", command, arg)
				return false, nil, nil, ErrHandled
			}
			if !inline {
				if i+1 >= len(args) {
					return false, nil, nil, usageErr(stderr, command, name+" needs a value")
				}
				i++
				value = args[i]
			}
			values[name] = value
		default:
			positional = append(positional, arg)
		}
	}
	jsonOut, err := jsonFlag(flags, command, stderr)
	return jsonOut, positional, values, err
}

func backupList(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	jsonOut, positional, _, err := splitFlags(args, "backup list <store> [path]", nil, stderr)
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
	body, err := fetch(ctx, cfg.Socket, query, listTimeout)
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
	jsonOut, positional, values, err := splitFlags(args, "backup restore <store> <path>", []string{"--blob", "--at", "--to"}, stderr)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		fmt.Fprintln(stderr, "storeboard: usage: storeboard backup restore <store> <path> [--blob HASH] [--at RFC3339] [--to PATH] [--json]")
		return ErrHandled
	}
	req := struct {
		Path string `json:"path"`
		Blob string `json:"blob,omitempty"`
		At   string `json:"at,omitempty"`
		To   string `json:"to,omitempty"`
	}{Path: positional[1], Blob: values["--blob"], At: values["--at"], To: values["--to"]}
	store := positional[0]
	if req.To != "" {
		// The daemon's working directory is its data dir, not this shell's:
		// a relative --to has to be resolved here, where it was meant.
		abs, err := filepath.Abs(req.To)
		if err != nil {
			return fmt.Errorf("--to %s: %w", req.To, err)
		}
		req.To = abs
	}

	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	body, err := call(ctx, cfg.Socket, http.MethodPost, "/v1/stores/"+url.PathEscape(store)+"/restore", req, noTimeout)
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
// practice — and one the caller owns. The original is left where it is.
// Nothing is overwritten: a taken name is parked and reported.
func sendCmd(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	jsonOut, positional, values, err := splitFlags(args, "send <store> <file>", []string{"--as"}, stderr)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		fmt.Fprintln(stderr, "storeboard: usage: storeboard send <store> <file> [--as PATH] [--json]")
		return ErrHandled
	}
	store := positional[0]
	// The daemon resolves paths from its own working directory (its data
	// dir), so the file is named the way this shell meant it.
	source, err := filepath.Abs(positional[1])
	if err != nil {
		return fmt.Errorf("send %s: %w", positional[1], err)
	}
	req := struct {
		Source string `json:"source"`
		Path   string `json:"path,omitempty"`
	}{Source: source, Path: values["--as"]}

	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	body, err := call(ctx, cfg.Socket, http.MethodPost, "/v1/stores/"+url.PathEscape(store)+"/send", req, noTimeout)
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
	jsonOut, positional, values, err := splitFlags(args, "store takeover <name>", []string{"--path"}, stderr)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		fmt.Fprintln(stderr, "storeboard: usage: storeboard store takeover <name> [--path PATH] [--json]")
		return ErrHandled
	}
	name, linkPath := positional[0], values["--path"]
	if linkPath != "" {
		// Declared symlinks are absolute; resolve a relative one here, where
		// the user typed it, rather than against the daemon's directory.
		abs, err := filepath.Abs(linkPath)
		if err != nil {
			return fmt.Errorf("--path %s: %w", linkPath, err)
		}
		linkPath = abs
	}

	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	req := struct {
		Path string `json:"path,omitempty"`
	}{Path: linkPath}
	body, err := call(ctx, cfg.Socket, http.MethodPost, "/v1/stores/"+url.PathEscape(name)+"/takeover", req, noTimeout)
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
