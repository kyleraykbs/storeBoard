// Package api defines storeboard's versioned JSON contract.
//
// These types are the contract: the daemon serves them under /v1, the CLI
// renders them, and the config file shares the same vocabulary (peers,
// groups, folders, mounts). Shapes change additively only — within one
// version, a field's meaning never changes and never disappears.
// Human-readable output is free to change; this is not.
//
// Every list field is emitted as an array, never null.
package api

import "time"

// Version is the API version this build speaks.
const Version = 1

// Peer is a named device: who it is and how other peers reach it. Endpoints
// are declared host:port addresses; an empty list means "known, not yet
// addressable".
type Peer struct {
	Name      string   `json:"name"`
	Endpoints []string `json:"endpoints"`
}

// Group is a set of peers that share folders. Folders lists the folder
// names; their mounts live on each Folder.
type Group struct {
	Name    string   `json:"name"`
	Peers   []string `json:"peers"`
	Folders []string `json:"folders"`
}

// Folder is one shared folder as this machine sees it: its effective local
// mount point (local override, group default, or a default inside the
// storage directory) and how it is kept. KeepOffline true means this
// machine holds a full local replica that works offline; false means the
// folder is streamed from a holder — mounted live while one is reachable,
// replaced by an empty dummy dir when none is, and remerged on reconnect.
// Storage, when set, is where the content actually lives: the mount is then
// a symlink to it, and content and mount point are independent.
type Folder struct {
	Name        string `json:"name"`
	Group       string `json:"group"`
	Mount       string `json:"mount"`
	Storage     string `json:"storage,omitempty"`
	KeepOffline bool   `json:"keepOffline"`
}

// Connection is this machine's link to one declared peer, as the daemon
// currently sees it. State is one of: connected, dialing, backoff,
// unaddressed.
type Connection struct {
	Peer     string    `json:"peer"`
	Endpoint string    `json:"endpoint,omitempty"`
	State    string    `json:"state"`
	Since    time.Time `json:"since"`
	Error    string    `json:"error,omitempty"`
}

// Permissions is a folder's resolved ownership and mode: what the daemon
// gives the directories and files it creates for the folder. The values are
// declared per folder and per group, field by field, and whatever is not
// declared is inherited from the directory the mount lives in — so they are
// only known to the daemon, never fully to the config. User and group are
// names where they resolve, numeric ids where they do not; Mode is octal.
type Permissions struct {
	User  string `json:"user"`
	Group string `json:"group"`
	Mode  string `json:"mode"`
}

// Busy reasons, FolderSync.Busy. The daemon reports which one applies so a
// user (and the CLI) can tell an actionable path from one that belongs to
// something else entirely.
const (
	// BusyContent is a real directory holding the user's files. It is the
	// one busy reason a takeover can resolve: `storeboard folder takeover`
	// moves the content into the folder's offline mount and mounts over it.
	BusyContent = "path has content"
	// BusySymlink is a symlink the daemon did not make.
	BusySymlink = "foreign symlink"
	// BusyMount is a live mount point (a samba mount, a tmpfs placeholder).
	BusyMount = "mount point"
	// BusyNoRemove is a directory that is empty but cannot be removed.
	BusyNoRemove = "empty dir cannot be removed"
	// BusyNotDir is a regular file or something else in the way.
	BusyNotDir = "not a directory"
)

// FolderSync is one folder's engine state on this machine. State is one of:
// replica (a full local copy that syncs), attached (a streamed folder
// mounted live), detached (a streamed folder replaced by an empty dir while
// no holder is reachable), busy (something else owns the mount path, e.g. a
// samba mount, and the engine leaves it alone). Busy carries the reason when
// state is busy; empty otherwise.
//
// The counts describe the work in progress, which is what a streamed folder
// grinding through a large tree looks like from outside: Entries is what the
// folder knows, Local how much of it exists here, Pending the difference
// (the queue being materialised), Scanning whether a walk is running right
// now, and Receiving how many blobs are arriving. All are read without the
// scan lock, so status stays answerable while the folder is busy.
type FolderSync struct {
	Folder      string      `json:"folder"`
	Root        string      `json:"root"`
	State       string      `json:"state"`
	Busy        string      `json:"busy,omitempty"`
	Entries     int         `json:"entries"`
	Local       int         `json:"local"`
	Pending     int         `json:"pending"`
	Scanning    bool        `json:"scanning"`
	Receiving   int         `json:"receiving"`
	Conflicts   int         `json:"conflicts"`
	LastScan    time.Time   `json:"lastScan"`
	Permissions Permissions `json:"permissions"`
}

// TakeoverResult reports what `storeboard folder takeover` did with a mount
// path that had content of its own: how many entries moved into the folder's
// offline mount and where the folder stands now (attached when a holder was
// reachable and the merge went through, detached otherwise — the merge then
// happens on reconnect without further help).
type TakeoverResult struct {
	Folder string `json:"folder"`
	State  string `json:"state"`
	Root   string `json:"root"`
	Moved  int    `json:"moved"`
}

// Status is the daemon's status document, served at GET /v1/status.
type Status struct {
	APIVersion    int          `json:"apiVersion"`
	Version       string       `json:"version"`
	StartedAt     time.Time    `json:"startedAt"`
	UptimeSeconds int64        `json:"uptimeSeconds"`
	Socket        string       `json:"socket"`
	DataDir       string       `json:"dataDir"`
	Peer          string       `json:"peer"`
	Peers         []Peer       `json:"peers"`
	Groups        []Group      `json:"groups"`
	Folders       []Folder     `json:"folders"`
	Connections   []Connection `json:"connections"`
	Sync          []FolderSync `json:"sync"`
}

// Version is one recorded version of one file: the blob that holds its bytes
// (content-addressed, so the same bytes are one blob everywhere), when this
// machine recorded it, and what it looked like. Deleted versions carry the
// content the deletion removed, which is what makes a delete restorable.
type FileVersion struct {
	Path    string    `json:"path,omitempty"`
	Time    time.Time `json:"time"`
	Blob    string    `json:"blob,omitempty"`
	Size    int64     `json:"size"`
	MTime   time.Time `json:"mtime,omitempty"`
	Mode    string    `json:"mode,omitempty"`
	Deleted bool      `json:"deleted,omitempty"`
}

// Versions is the answer to `backup list`: a file's recorded versions,
// newest first.
type Versions struct {
	Folder   string        `json:"folder"`
	Path     string        `json:"path"`
	Keep     string        `json:"keep"`
	Versions []FileVersion `json:"versions"`
}

// RestoreResult reports what a restore did.
type RestoreResult struct {
	Folder string    `json:"folder"`
	Path   string    `json:"path"`
	Blob   string    `json:"blob"`
	At     time.Time `json:"at"`
	Copied bool      `json:"copied,omitempty"` // written beside the folder rather than restored into it
}

// SendResult reports where a file sent into a folder landed. Parked names the
// conflict copy an existing file at that path was moved to, if there was one:
// a send never overwrites.
type SendResult struct {
	Folder string `json:"folder"`
	Path   string `json:"path"`
	Parked string `json:"parked,omitempty"`
	Size   int64  `json:"size"`
}
