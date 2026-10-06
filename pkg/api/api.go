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

// Folder is one shared folder with its effective local mount point on this
// machine: the local override, the group's declared default, or a default
// inside the storage directory — whichever applies.
type Folder struct {
	Name  string `json:"name"`
	Group string `json:"group"`
	Mount string `json:"mount"`
}

// Status is the daemon's status document, served at GET /v1/status.
type Status struct {
	APIVersion    int       `json:"apiVersion"`
	Version       string    `json:"version"`
	StartedAt     time.Time `json:"startedAt"`
	UptimeSeconds int64     `json:"uptimeSeconds"`
	Socket        string    `json:"socket"`
	DataDir       string    `json:"dataDir"`
	Peer          string    `json:"peer"`
	Peers         []Peer    `json:"peers"`
	Groups        []Group   `json:"groups"`
	Folders       []Folder  `json:"folders"`
}
