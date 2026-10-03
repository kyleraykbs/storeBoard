// Package api defines storeboard's versioned JSON contract.
//
// These types are the contract: the daemon serves them under /v1, the CLI
// renders them, and the config file reuses Folder. Shapes change additively
// only — within one version, a field's meaning never changes and never
// disappears. Human-readable output is free to change; this is not.
package api

import "time"

// Version is the API version this build speaks.
const Version = 1

// Folder is one synced folder: a stable name inside the mesh and the local
// path it is bound to on this machine.
type Folder struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// Status is the daemon's status document, served at GET /v1/status.
// Folders is always emitted as an array, never null.
type Status struct {
	APIVersion    int       `json:"apiVersion"`
	Version       string    `json:"version"`
	StartedAt     time.Time `json:"startedAt"`
	UptimeSeconds int64     `json:"uptimeSeconds"`
	Socket        string    `json:"socket"`
	DataDir       string    `json:"dataDir"`
	Folders       []Folder  `json:"folders"`
}
