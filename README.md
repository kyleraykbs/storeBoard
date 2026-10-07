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
- `nix/` — one NixOS module (daemon + CLI under `services.storeboard`)
  and the home-manager CLI module.
- `agents/` — decisions, structure, terminology for agent sessions.
- `DESIGN.md` — invariants every change is checked against.

## Configuration

Strict JSON; unknown keys are an error. The model, in full in
`config.example.json`:

- `peer` — this machine's name (hostname fallback while no groups exist).
- `peers` — named devices with declared `host:port` endpoints; discovery is
  config-only, the daemon never invents a peer.
- `groups` — peer memberships that own `folders`; each folder declares a
  default `mount`; folder names are unique across all groups.
- `mounts` — this machine's local override of a group folder's mount.
  Effective mount: local override → group default → `<dataDir>/mounts/<name>`.
- `permissions` — ownership and directory mode for what the daemon creates,
  declared per folder (this machine) and per group (the default). Resolution
  is field by field: folder entry → group default → **the directory the
  mount lives in** (its owner, its group, its mode), re-read on every mount
  transition and scan. So with nothing declared, a folder is permissioned
  exactly like the directory containing it; `{"user": "kyle"}` on a folder
  whose parent is root-owned is the common override. Modes are directories
  only — files keep the mode they were synced with.
- everything the daemon stores (store, journal, index) lives under the one
  `dataDir`.

Validate any file with:

    storeboard --config config.example.json config check --json

Resolution: `--config` > an existing
`$XDG_CONFIG_HOME/storeboard/config.json` > `$STOREBOARD_CONFIG` > the XDG
default. The Nix modules generate and validate the file at build time, so a
bad option value fails nixos-rebuild instead of failing at startup.

## Sync and mounts

- **Replicas** (`keepOffline = true`) hold the folder's content under the
  mount path and sync it with every other replica: creates, edits and
  deletes (deletes land in `<dataDir>/trash/<folder>/`).
- **Streamed** folders (`keepOffline = false`) are daemon-managed views: the
  mount path is a symlink into `<dataDir>/views/<folder>`, live while a
  holder answers. When no holder is reachable the path is swapped to an
  empty writable dir (`<dataDir>/detached/<folder>`); what is written there
  is merged back into the view on reconnect and synced onwards. A path
  owned by something else (a samba mount, a real directory) is left alone
  and reported as `busy`.
- Every path the daemon creates — the view, the detached dir, the
  directories under them — is chowned and chmod'ed to the folder's resolved
  permissions on every transition, and the storage chain to the mount target
  is made traversable, so the symlink is usable by the folder's owner.
  Resolved values show up in `status` (`sync[].permissions`).
- Concurrent edits to one file resolve deterministically on every replica
  (newer wins; the loser is kept as `<name>.conflict-<blob8>`). Deletes and
  conflicts are visible in `status --json` (`.sync[]`).
- Scans run every 15 s; `STOREBOARD_SCAN_INTERVAL` (a duration, e.g.
  `200ms`) overrides that for tests and tight loops.

## Query the daemon

    storeboardd --config config.example.json &
    storeboard --config config.example.json status --json | jq .folders

`status --json` is the daemon's `/v1/status` document verbatim (HTTP over
the unix socket); human output is free to change, JSON shapes are not.

## NixOS / home-manager

    # flake.nix
    inputs.storeboard.url = "codeberg.org/kyleraykbs/storeboard";

    # configuration.nix — one options set: runs the daemon and installs the
    # CLI for every user, pointed at the daemon's config via STOREBOARD_CONFIG
    imports = [ inputs.storeboard.nixosModules.storeboard ];
    services.storeboard = {
      enable = true;
      peer = "laptop";
      peers = {
        laptop.endpoints = [ "192.168.1.10:4242" ];
        desktop.endpoints = [ "192.168.1.11:4242" ];
      };
      groups.family = {
        peers = [ "laptop" "desktop" ];
        folders.photos.mount = "/srv/photos";
      };
      mounts = { };  # this machine's local mount overrides
    };

    # home-manager — optional, per-user: installs the CLI for one user and
    # writes their XDG config (it wins over the system-wide one)
    imports = [ inputs.storeboard.homeManagerModules.storeboard ];
    programs.storeboard.enable = true;
