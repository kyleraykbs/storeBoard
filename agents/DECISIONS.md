# Decisions

## [2026-10-03] Module path and binary names
- Decision: `codeberg.org/kyleraykbs/storeboard`, binaries `storeboard` (CLI)
  and `storeboardd` (daemon).
- Why: matches the repo directory and the musoak pattern (name + name+d).
- Reversible: no (touches every import)

## [2026-10-03] Foundation scope: config, JSON contract, status document
- Decision: the first pass ships the flake, the Nix modules, strict JSON
  config with `config check`, and a daemon serving a versioned `/v1/status`
  document over a unix socket. Sync engine, peer transport and the NDJSON
  event stream come in later passes.
- Why: the JSON/config contracts shape everything else; the engine needs
  protocol design first.
- Reversible: yes

## [2026-10-03] Status document carries no folder state yet
- Decision: `folders` in the status document is `[{name, path}]`; no `state`
  field until the sync engine defines its enum.
- Why: the JSON shapes are a versioned contract — adding fields is
  compatible, giving an existing field a new meaning later is not.
- Reversible: yes

## [2026-10-03] Config path precedence
- Decision: `--config` > an existing `$XDG_CONFIG_HOME/storeboard/config.json`
  > `$STOREBOARD_CONFIG` > the XDG default location.
- Why: a per-user file is the more specific declaration, so home-manager
  beats the NixOS module's /etc file when both are enabled (the layering
  musoak documents); the env var serves machines with no user file.
- Reversible: yes

## [2026-10-03] Socket is world-writable for now
- Decision: the daemon chmods its unix socket to 0666; the only verb it
  serves is the status document.
- Why: the NixOS CLI runs as an arbitrary user while the system service runs
  as its own user, and group plumbing can wait for the threat model.
  DESIGN.md pins the invariant: no privileged verb ships while the socket is
  world-accessible.
- Reversible: yes (mode is a constant in internal/server)
