# Structure

Go module `codeberg.org/kyleraykbs/storeboard`.

## Binaries

- `cmd/storeboardd` — the daemon: loads the config, serves the status
  document over a unix socket (`GET /v1/status`, HTTP).
- `cmd/storeboard` — the CLI: `config check`, `status`, `version`. Every
  command takes `--json` for machine-readable output.

## Internals

- `internal/config` — strict JSON config (unknown keys are errors), XDG
  defaults, `--config` > `$STOREBOARD_CONFIG` > XDG path, validation
  (configVersion 1, unique folder names, absolute paths).
- `internal/server` — the unix-socket HTTP server, socket lifecycle (stale
  takeover, live-daemon refusal, cleanup) and the `/v1/status` document.
- `internal/cli` — the command set; stdout carries reports/data, stderr
  carries diagnostics; `ErrHandled` marks a failure already reported.
- `internal/version` — build version, stamped by the flake's ldflags
  (`dev` under a plain `go build`).

## Public

- `pkg/api` — the versioned JSON contract: `Status`, `Folder`, `Version`.
  The daemon serves it, the CLI renders it, the config file reuses `Folder`.

## Nix

- `nix/storeboard-options.nix` — shared option set + `configJSON` generator.
- `nix/storeboardd.nix` — NixOS service module (`services.storeboardd`).
- `nix/storeboard-nixos.nix` — system-wide CLI + `/etc/storeboard/config.json`.
- `nix/storeboard-hm.nix` — home-manager CLI + XDG config file.

## agents/

Decisions, structure, terminology, notes, TODO — see the procedure.
