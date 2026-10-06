# Structure

Go module `codeberg.org/kyleraykbs/storeboard`.

## Binaries

- `cmd/storeboardd` — the daemon: loads the config, serves the status
  document over a unix socket (`GET /v1/status`, HTTP).
- `cmd/storeboard` — the CLI: `config check`, `status`, `version`. Every
  command takes `--json` for machine-readable output.

## Internals

- `internal/config` — strict JSON config (unknown keys are errors), XDG
  defaults, `--config` > existing XDG > `$STOREBOARD_CONFIG` path, the
  peer/group/folder/mount model with validation (configVersion 1, host:port
  endpoints, group membership, global folder names, absolute mounts) and
  the sorted `PeerList`/`GroupList`/`Folders` views the status serves.
- `internal/server` — the unix-socket HTTP server, socket lifecycle (stale
  takeover, live-daemon refusal, cleanup) and the `/v1/status` document.
- `internal/cli` — the command set; stdout carries reports/data, stderr
  carries diagnostics; `ErrHandled` marks a failure already reported.
- `internal/version` — build version, stamped by the flake's ldflags
  (`dev` under a plain `go build`).

## Public

- `pkg/api` — the versioned JSON contract: `Status`, `Peer`, `Group`,
  `Folder`, `Version`. The daemon serves it and the CLI renders it; the
  config file speaks the same peer/group/folder/mount vocabulary.

## Nix

- `nix/storeboard-options.nix` — shared options + `configJSON`, generated
  through `checkedConfig` so the build runs the real `config check`.
- `nix/storeboard.nix` — the NixOS module: `services.storeboard` runs the
  daemon, installs the CLI for every user and points `STOREBOARD_CONFIG` at
  the daemon's config. The only NixOS options set.
- `nix/storeboard-hm.nix` — home-manager CLI module (`programs.storeboard`),
  that scope's only options set.

## agents/

Decisions, structure, terminology, notes, TODO — see the procedure.
