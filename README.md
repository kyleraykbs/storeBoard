# Storeboard

Replicated folders across your own devices: every folder keeps healthy
copies on the machines that want it, and going offline never loses data.

Status: foundation. The flake, the daemon and the CLI build and run; the
sync engine, peer transport and event stream are not implemented yet.
`DESIGN.md` holds the invariants the JSON contracts already pin down.

## Build and run

    nix build              # packages both binaries
    nix run .#storeboard -- version --json
    nix develop            # Go toolchain, gopls, delve, jq

Binaries: `storeboard` (CLI) and `storeboardd` (daemon).

## Layout

- `cmd/`, `internal/`, `pkg/api` — Go. `pkg/api` is the versioned JSON
  contract; every command takes `--json` and prints those shapes verbatim.
- `nix/` — NixOS service module, NixOS + home-manager CLI modules.
- `agents/` — decisions, structure, terminology for agent sessions.
- `DESIGN.md` — invariants every change is checked against.

## Configuration

Strict JSON; unknown keys are an error. See `config.example.json`, and
validate any file with:

    storeboard --config config.example.json config check --json

Resolution: `--config` > an existing
`$XDG_CONFIG_HOME/storeboard/config.json` > `$STOREBOARD_CONFIG` > the XDG
default. The Nix modules generate and validate the file at build time, so a
bad option value fails nixos-rebuild instead of failing at startup.

## Query the daemon

    storeboardd --config config.example.json &
    storeboard --config config.example.json status --json | jq .folders

`status --json` is the daemon's `/v1/status` document verbatim (HTTP over
the unix socket); human output is free to change, JSON shapes are not.

## NixOS / home-manager

    # flake.nix
    inputs.storeboard.url = "codeberg.org/kyleraykbs/storeboard";

    # configuration.nix
    imports = [ inputs.storeboard.nixosModules.storeboardd ];
    services.storeboardd = {
      enable = true;
      folders = [ { name = "photos"; path = "/srv/photos"; } ];
    };

    # home-manager
    imports = [ inputs.storeboard.homeManagerModules.storeboard ];
    programs.storeboard.enable = true;
