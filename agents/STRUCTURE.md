# Structure

Go module `codeberg.org/kyleraykbs/storeboard`.

## Binaries

- `cmd/storeboardd` — the daemon: loads the config, serves the status
  document over a unix socket (`GET /v1/status`, HTTP).
- `cmd/storeboard` — the CLI: `config check`, `status`, `store takeover`,
  `send`, `backup`, `version`. Every command takes `--json` for
  machine-readable output.

## Internals

- `internal/config` — strict JSON config (unknown keys are errors), XDG
  defaults, `--config` > existing XDG > `$STOREBOARD_CONFIG` path, the
  root/store-tree model with validation (configVersion 2, ids unique across
  the tree, sibling relpaths unique, symlinks absolute and disjoint from the
  root, absolute endpoints, group membership), inheritance resolution
  (`StoreViews`), and the sorted `PeerList`/`GroupList`/`Stores` views the
  status serves. configVersion 1 is rejected; there is no migration.
- `internal/store` — the content-addressed blob store: sha256-keyed files
  under `<dataDir>/store`, scratch under `<dataDir>/tmp`, adopt = fsync +
  rename.
- `internal/syncer` — the engine: per-store journals with version vectors
  (`filedb.go`), the scanner (`scan.go`), the session protocol
  (`session.go`: index/want/blob/blobEnd, conflict resolution, trash), the
  tree (`symlinks.go`: canonical dirs, child links, external symlinks,
  takeover), and permissions (`perms.go`). There is no mount state machine:
  a store's canonical dir is stable, and attach/detach is a liveness flag.
- `internal/transport` — the peer wire: listener, dial loops, hello
  handshake, keepalives, cross-dial tie-break; hands frames to a Handler
  (the engine).
- `internal/server` — the unix-socket HTTP server, socket lifecycle (stale
  takeover, live-daemon refusal, cleanup) and the `/v1/status` document.
- `internal/cli` — the command set; stdout carries reports/data, stderr
  carries diagnostics; `ErrHandled` marks a failure already reported.
- `internal/version` — build version, stamped by the flake's ldflags
  (`dev` under a plain `go build`).

## Public

- `pkg/api` — the versioned JSON contract: `Status`, `Peer`, `Group`,
  `Store`, `StoreSync`, `StoreSymlink`, `Version`. The daemon serves it and
  the CLI renders it; the config file speaks the same root/store/symlink
  vocabulary.
- `mobile` — the engine as an app sees it, bound by gomobile: `Start`,
  `Stop`, `Status`, `Versions`, `Restore`, `Send`, all JSON strings in the
  shapes `pkg/api` names. No socket; the UI calls it in-process. The
  Android app that consumes it lives in `android/`.

## Nix

- `nix/storeboard-options.nix` — shared options + `configJSON`, generated
  through `checkedConfig` so the build runs the real `config check`. The
  store tree is a recursive submodule; `storesJSON` emits only declared
  fields.
- `nix/storeboard.nix` — the NixOS module: `services.storeboard` runs the
  daemon, installs the CLI for every user and points `STOREBOARD_CONFIG` at
  the daemon's config. The only NixOS options set. `ReadWritePaths` grants
  the data dir, the root and every declared symlink path.
- `nix/storeboard-hm.nix` — home-manager CLI module (`programs.storeboard`),
  that scope's only options set.

## agents/

Decisions, structure, terminology, notes, TODO — see the procedure.