# Storeboard

Replicated stores across your own devices: each store keeps healthy copies on
the machines that want it, stores nest without costing anything, and going
offline never loses data.

Status: working. The flake, the daemon, the CLI, the sync engine, the peer
transport and the embedded library build and run, with the invariants in
`DESIGN.md` covered by tests. The Android app builds (engine bound by
gomobile, APK via `scripts/build-android-app.sh`) and has not yet been run
on a device. The event stream is the live edge. `DESIGN.md` holds the
invariants the JSON contracts already pin down.

## Build and run

    nix build              # packages both binaries
    nix run .#storeboard -- version --json
    nix develop            # Go toolchain, gopls, delve, jq

Binaries: `storeboard` (CLI) and `storeboardd` (daemon).

## Layout

- `cmd/`, `internal/`, `pkg/api` — Go. `pkg/api` is the versioned JSON
  contract; every command takes `--json` and prints those shapes verbatim.
- `mobile/` — the engine as a library: gomobile binds this package, so the
  phone runs the same syncer, store and peer wire the daemon does. No local
  socket (an app has no CLI to serve); everything crossing the boundary is
  the JSON the contracts already name.
- `android/` — the app that embeds it (`scripts/build-android-app.sh` builds
  it; its README has the details).
- `nix/` — one NixOS module (daemon + CLI under `services.storeboard`)
  and the home-manager CLI module.
- `agents/` — decisions, structure, terminology for agent sessions.
- `DESIGN.md` — invariants every change is checked against.

## Configuration

Strict JSON; unknown keys are an error. The model, in full in
`config.example.json`:

- `root` — the one directory the bytes live in; a required setting with a
  sane default (`~/Storeboard`). A plain container: every store is a real
  directory at `<root>/<id>`, and the tree below it is made of symlinks.
- `peer` — this machine's name (hostname fallback while no groups exist).
- `peers` — named devices with declared `host:port` endpoints; discovery is
  config-only, the daemon never invents a peer.
- `groups` — peer memberships. A store names a group; a child inherits its
  parent's when it does not.
- `stores` — the tree, by id. Ids are globally unique and name the canonical
  directory under `root`. Each store:
  - `path` — its relpath inside the parent, and the name it appears under
    there. Absent means the id, at the parent's top level. Meaningless at the
    top level, where the canonical dir is the id.
  - `sync` — `local` (a replica that works offline) or `remote` (streamed
    from a holder). Absent inherits the parent's; the top-level default is
    `local`.
  - `symlinks` — absolute paths that become symlinks to the canonical dir.
    Never inherited: a path belongs to one machine. `/home/kyle/Documents`
    becomes a symlink to `~/Storeboard/documents`.
  - `permissions` — ownership and directory mode; overrides the parent field
    by field.
  - `stores` — child stores.
- `inheritance` — undeclared means follow the parent. Group, `sync`,
  `permissions` and the backup opt-out all resolve nearest-declared-wins; a
  child only states what makes it different.
- `permissions` — this machine's per-store override. Resolution is field by
  field: machine entry → store's chain → group default → **the directory the
  root lives in** (its owner, its group, its mode), re-read on every scan. So
  with nothing declared, a store is permissioned exactly like the directory
  containing root; `{"user": "kyle"}` on a store whose container is
  root-owned is the common override. Modes are directories only — files keep
  the mode they were synced with.
- `backup` — opt-in per machine, one `keep` window, per-store opt-out by id.
- everything the daemon stores (blobs, journals, history, trash) lives under
  the one `dataDir`.

Validate any file with:

    storeboard --config config.example.json config check --json

Resolution: `--config` > an existing
`$XDG_CONFIG_HOME/storeboard/config.json` > `$STOREBOARD_CONFIG` > the XDG
default. The Nix modules generate and validate the file at build time, so a
bad option value fails nixos-rebuild instead of failing at startup.

There is no migration from configVersion 1. A v1 file is rejected; the store
tree, the root and the symlinks are declared fresh.

## The tree, and sync

- **Every store's bytes live under root.** `<root>/<id>` is a real directory.
  A store's presentation is a symlink (or, at the top level, the canonical
  dir itself).
- **Children are symlinks.** `pictures` with a child at `Bulk` gets a symlink
  at `<root>/pictures/Bulk` → `<root>/bulk`. The scanner never descends into
  a symlink and never journals one, so the parent never indexes the child's
  bytes. Nesting therefore costs nothing: no double indexing, no content-store
  copy of the outer tree.
- **Declared paths are symlinks outward.** A declared foreign symlink may be
  replaced atomically; replacing the link inode never follows or deletes its
  old target. Real non-empty directories, live mounts and ordinary files are
  left alone and reported `busy` (`sync[].symlinks[]`).
- **`local`** stores hold a replica and sync it with every other participant:
  creates, edits and deletes (deletes land in `<dataDir>/trash/<id>/`).
- **`remote`** stores are streams from a holder. The canonical dir is always
  present and visible; `attached`/`detached` only reports whether a holder is
  reachable. Offline writes land in the store and sync when a holder returns
  — there is no dummy dir and no root flip.
- Concurrent edits to one file resolve deterministically on every replica
  (newer wins; the loser is kept as `<name>.conflict-<blob8>`). Deletes and
  conflicts are visible in `status --json` (`.sync[]`).
- The daemon reconciles canonical dirs and declared symlinks every 5 s
  (`STOREBOARD_LINK_INTERVAL` overrides it for tests). Missing canonical dirs
  and links are recreated; empty declared directories become links again;
  non-empty directories, ordinary files and mountpoints remain protected and
  report `busy`.
  resolved values show up in `status` (`sync[].permissions`).
- Scans run every 15 s; `STOREBOARD_SCAN_INTERVAL` (a duration, e.g.
  `200ms`) overrides that for tests and tight loops.

### Takeover

A declared path that already holds files is never touched automatically.
`storeboard store takeover <name> [--path PATH]` moves its entries into the
store's canonical dir (never overwriting: a name that collides lands under the
deterministic conflict name) and replaces the path with the symlink. The
ordinary scan hands the files to the mesh. Root or the store's owner may run
it; the daemon reads that from the socket's peer credentials, so the
world-writable socket is not a way in.

## Backup

Opt-in per machine (`backup.enable`), with one window (`backup.keep`, default
`1y`, units h/d/w/y) and per-store opt-outs (`backup.stores`). Every version
transition is recorded per store as a line referencing the blob that holds
those bytes — the content store is append-only, so the bytes are already
there; the history is what makes them findable. Retention thins the record the
standard way: the newest version of each hour for a day, each day for a week,
each week for a month, each month for a year, each year beyond — and nothing
older than the window. A file's current version is never dropped. A store this
machine marks `remote` keeps no history: its versions belong to the machine
that holds the content, which is what lets one declaration be right everywhere.

A startup collector frees content that nothing references — never a journal's,
a kept version's, an orphan journal's, or a recent write's — and refuses to run
at all when a reference source cannot be read. The trash ages out under the
same window.

    storeboard backup list photos "Screenshots/old.png"
    storeboard backup restore photos "Screenshots/old.png" --at 2026-09-01T10:00:00Z
    storeboard backup restore photos "gone.png"     # a deleted file comes back
    storeboard backup restore photos notes.md --to /tmp/notes.old

## Send a file

    storeboard send photos holiday.jpg --as "2026/holiday.jpg"

Copies a local file into a store: from then on it is an ordinary local change
— indexed at once, recorded in the history, synced onwards. Nothing is
overwritten; a name already taken parks the existing file under the same
deterministic conflict name the merge uses and reports where. The daemon does
the copying, so the file must be one it can see (the same machine, in
practice), and root or the store's owner is the only caller allowed.

The phone's share sheet is the same verb, one layer down: the app's UI calls
the engine directly (there is no socket inside an app), and a shared file is
copied in and sent into the camera store. `storeboard/mobile` and this command
are two doors onto one operation.

## Query the daemon

    storeboardd --config config.example.json &
    storeboard --config config.example.json status --json | jq .stores

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
      root = "/srv/storeboard";
      peers = {
        laptop.endpoints = [ "192.168.1.10:4242" ];
        desktop.endpoints = [ "192.168.1.11:4242" ];
      };
      groups.family.peers = [ "laptop" "desktop" ];
      stores.pictures = {
        group = "family";
        symlinks = [ "/home/kyle/Pictures" ];
        stores.bulk = { group = "media"; sync = "remote"; };
      };
    };

    # home-manager — optional, per-user: installs the CLI for one user and
    # writes their XDG config (it wins over the system-wide one)
    imports = [ inputs.storeboard.homeManagerModules.storeboard ];
    programs.storeboard.enable = true;