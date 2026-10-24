// Package api defines storeboard's versioned JSON contract.
//
// These types are the contract: the daemon serves them under /v1, the CLI
// renders them, and the config file shares the same vocabulary (peers,
// groups, stores, symlinks). Shapes change additively only — within one
// version, a field's meaning never changes and never disappears. Human-
// readable output is free to change; this is not.
//
// Every list field is emitted as an array, never null.
package api

import "time"

// Version is the API version this build speaks.
const Version = 2

// Peer is a named device: who it is and how other peers reach it. Endpoints
// are declared host:port addresses; an empty list means "known, not yet
// addressable".
type Peer struct {
	Name      string   `json:"name"`
	Endpoints []string `json:"endpoints"`
}

// Group is a set of peers that share stores. Stores lists the ids this
// machine resolves to that group.
type Group struct {
	Name   string   `json:"name"`
	Peers  []string `json:"peers"`
	Stores []string `json:"stores"`
}

// Store is one store as this machine sees it. Root is the canonical
// directory, always under the machine's root; Parent and Path describe where
// it appears (a symlink at <parent>/<path>) and are empty at the top level.
// Sync is "local" (a replica that works offline) or "remote" (streamed from
// a holder). Symlinks are the absolute paths declared to point at the
// canonical directory.
type Store struct {
	Name     string   `json:"name"`
	Parent   string   `json:"parent,omitempty"`
	Path     string   `json:"path,omitempty"`
	Group    string   `json:"group"`
	Sync     string   `json:"sync"`
	Root     string   `json:"root"`
	Symlinks []string `json:"symlinks"`
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

// Permissions is a store's resolved ownership and mode: what the daemon gives
// the entries it creates. The values are declared per store and inherited
// from its parent, then overridden per machine, field by field; whatever is
// not declared is inherited from the directory the canonical dir lives in.
// User and group are names where they resolve, numeric ids where they do not;
// Mode is octal.
type Permissions struct {
	User  string `json:"user"`
	Group string `json:"group"`
	Mode  string `json:"mode"`
}

// Busy reasons, StoreSymlink.Busy. The daemon reports which one applies so a
// user (and the CLI) can tell an actionable path from one that belongs to
// something else entirely.
const (
	// BusyContent is a real directory holding the user's files. It is the
	// one busy reason `storeboard store takeover` can resolve.
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

// Symlink states, StoreSymlink.State.
const (
	SymlinkLinked = "linked" // the declared path points at the store
	SymlinkBusy   = "busy"   // something else owns it; never touched
	SymlinkAbsent = "absent" // declared but its parent could not be created
)

// StoreSymlink is one declared external path and what the daemon found there.
type StoreSymlink struct {
	Path  string `json:"path"`
	State string `json:"state"`
	Busy  string `json:"busy,omitempty"`
}

// StoreSync is one store's engine state on this machine. State is one of:
// replica (a full local copy that syncs), attached (a remote store with a
// holder reachable), detached (a remote store with no holder reachable — its
// canonical dir stays visible, it simply is not live). Busy carries why a
// declared symlink is not linked; empty otherwise.
//
// The counts describe the work in progress: Entries is what the store knows,
// Local how much of it exists here, Pending the difference, Scanning whether
// a walk is running right now, and Receiving how many blobs are arriving.
type StoreSync struct {
	Store       string         `json:"store"`
	Root        string         `json:"root"`
	State       string         `json:"state"`
	Entries     int            `json:"entries"`
	Local       int            `json:"local"`
	Pending     int            `json:"pending"`
	Scanning    bool           `json:"scanning"`
	Receiving   int            `json:"receiving"`
	Conflicts   int            `json:"conflicts"`
	LastScan    time.Time      `json:"lastScan"`
	Permissions Permissions    `json:"permissions"`
	Symlinks    []StoreSymlink `json:"symlinks"`
}

// TakeoverResult reports what `storeboard store takeover` did with a declared
// symlink path that already held the user's own content: how many entries
// moved into the store and where it stands now.
type TakeoverResult struct {
	Store string `json:"store"`
	Path  string `json:"path"`
	State string `json:"state"`
	Root  string `json:"root"`
	Moved int    `json:"moved"`
}

// Status is the daemon's status document, served at GET /v1/status.
type Status struct {
	APIVersion    int          `json:"apiVersion"`
	Version       string       `json:"version"`
	StartedAt     time.Time    `json:"startedAt"`
	UptimeSeconds int64        `json:"uptimeSeconds"`
	Socket        string       `json:"socket"`
	DataDir       string       `json:"dataDir"`
	Root          string       `json:"root"`
	Peer          string       `json:"peer"`
	Peers         []Peer       `json:"peers"`
	Groups        []Group      `json:"groups"`
	Stores        []Store      `json:"stores"`
	Connections   []Connection `json:"connections"`
	Sync          []StoreSync  `json:"sync"`
}

// FileVersion is one recorded version of one file: the blob that holds its
// bytes (content-addressed, so the same bytes are one blob everywhere), when
// this machine recorded it, and what it looked like. Deleted versions carry
// the content the deletion removed, which is what makes a delete restorable.
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
	Store    string        `json:"store"`
	Path     string        `json:"path"`
	Keep     string        `json:"keep"`
	Versions []FileVersion `json:"versions"`
}

// RestoreResult reports what a restore did.
type RestoreResult struct {
	Store  string    `json:"store"`
	Path   string    `json:"path"`
	Blob   string    `json:"blob"`
	At     time.Time `json:"at"`
	Copied bool      `json:"copied,omitempty"` // written beside the store rather than restored into it
}

// SendResult reports where a file sent into a store landed. Parked names the
// conflict copy an existing file at that path was moved to, if there was one:
// a send never overwrites.
type SendResult struct {
	Store  string `json:"store"`
	Path   string `json:"path"`
	Parked string `json:"parked,omitempty"`
	Size   int64  `json:"size"`
}
