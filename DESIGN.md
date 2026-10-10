# Design

Invariants. Every change is checked against these; if a change must break
one, it changes here first, deliberately, with a decision log entry.

## Data safety

1. Removing a folder, a peer, or the whole config never deletes data on
   disk. Deletion is always an explicit act (`storeboard` will grow one),
   never a side effect of reconfiguration.
2. Unsynced data is never evicted from any cache.
3. Writes are temp-file-then-rename and fsync'd. The daemon must survive
   `kill -9` mid-write without corrupting state; crash consistency is tested
   by killing it in CI.
4. Policies fail closed. A receiver filter or any config expression that
   errors rejects and logs — never accepts.

## Ordering and conflicts

5. Concurrent writes are ordered by version vector. A conflict is recorded
   and surfaced, never silently overwritten.

## Contracts

6. JSON shapes are versioned (`apiVersion` in every document,
   `configVersion` in the config file): within a version, changes are
   additive only — a field's meaning never changes and never disappears.
   Human-readable output may change freely. `pkg/api` is the schema.
7. The config file is strict — unknown keys are an error. Validation runs in
   three places, all the same validator: `storeboard config check`, at daemon
   startup, and at build time under each Nix module's `checkedConfig`, so a
   bad value fails nixos-rebuild.

## Process

8. Hooks and policies never sit in the sync data path: a slow or failing
   hook cannot block a drain. When config expressions land (gojq, pure Go),
   they are compiled and validated at config-check time, then run sandboxed
   — no environment, no file access, hard timeout and output limit.
9. Peers advertise min/max protocol versions and pick the highest common
   one; unknown fields are tolerated on the wire, matching invariant 6.
10. Verbs that act on the user's data are authorized per caller, never by
    socket access alone. The daemon socket is world-writable so any user's
    CLI can read status; an acting verb (today: `folder takeover`) checks
    the requesting uid through the socket's peer credentials and allows only
    root or the folder's owner — a refusal, not a guess, when the uid cannot
    be read. This is the sequence the earlier version of this invariant
    demanded: the authorization exists before the verb ships.
11. Peers are declared, never invented: the config (Nix-generated) is the
    authoritative peer list, and the daemon learns a peer only from it.
    Address discovery (DNS, NAT traversal) may layer on top later;
    declaration stays authoritative, and removing a peer revokes it.
12. Every folder resolves to exactly one local mount point through a fixed
    precedence — local override, group default, then inside the one
    storage directory — and the config names its owner group, so no path
    is ever ambiguous about what syncs there. Two folders on one identical
    mount are an error; a mount that equals its own storage root is an
    error. Where content lives is independent of where it is mounted: with
    a `storageDir` (per machine, overridable per group) a folder's root is
    `<storageDir>/<folder>` and the mount is a symlink to it, so roots are
    disjoint by construction and content already at a mount path is
    migrated in on the next transition. Without one, a folder's root is its
    mount — and folders whose roots then nest are allowed but warned about
    (double indexing on the machine holding both, and a content-store copy
    of the outer tree): `config check` prints it, the daemon logs it at
    startup, so the cost is chosen rather than discovered. A folder only
    some devices should carry is restricted with the tag's
    `folders.<f>.devices`.
13. Remote (streamed) folders never dangle. A streamed folder is mounted
    live while a holder is reachable; losing the holder is reported
    (state detached) but never leaves the path pointing at nothing (the
    sambaMounts lesson) — the mount either stays on the view (storage-
    backed folders, whose content lives locally and where offline writes
    land in the folder directly) or is replaced by an empty dummy dir whose
    content is merged back on reconnect (the original layout). Which
    folders stream is each machine's own `keepOffline` declaration
    (default true: hold a replica that works offline).
14. The peer wire is newline-delimited JSON over TCP. Both sides open with
    a hello (protocol, name, version); a disagreement on the protocol
    version ends the connection, and a name that is not declared in config
    is rejected — a link can never introduce a peer (invariant 11 holds on
    the wire too). Unknown frames are tolerated and noted. The wire carries
    status-grade data only (names, versions, keepalives); TLS with device
    certificates, built on the threat model, precedes anything sensitive.
15. Mounts are usable by the human, not by the daemon. Every directory the
    daemon creates for a folder — the view, the detached dir, the tree
    under them — is owned and permissioned to the folder's resolved
    permissions: declared per folder on this machine, else the group's
    default, else the directory the mount lives in (its owner, group and
    mode, re-read on every transition and scan). Nothing declared therefore
    means inherited, and an inherited permission tracks its parent. The
    storage chain down to the mount target is traversable so the symlink
    resolves for the owner, and a path that belongs to something else is
    never touched — reported `busy` instead.

## Sync engine (2026-10-07)

- Content is content-addressed (`<dataDir>/store/<sha256>`), shared by all
  folders on a machine, written temp → fsync → rename.
- Every folder keeps an append-only NDJSON journal (`<dataDir>/journal/`)
  replayed into memory at open and checkpointed; each entry carries a
  version vector.
- A scan (default every 15 s, `STOREBOARD_SCAN_INTERVAL` overrides) hashes
  only files whose size or mtime moved; vanished files become tombstones.
  Size, mtime and mode are re-read after the hash, because the directory
  snapshot WalkDir saw can be stale (a file truncated and rewritten mid-walk
  reads as size 0) — and an entry whose size contradicts its blob makes
  every later apply park a false conflict. The blob copied into the store is
  hashed while it is copied, so a store entry can never be described by a
  sum its bytes do not have.
- Peers exchange index deltas; a side that lacks or is behind a version
  asks for the blob (`want` → `blob` chunks → `blobEnd`); a side that is
  ahead pushes its index entry. Concurrent edits resolve deterministically
  (newer mtime wins, blob hash breaks ties): the winner keeps the path on
  both machines, the loser is parked as `<name>.conflict-<blob8>` and
  indexed like any other file, so conflict copies themselves replicate.
- Deletes propagate as tombstones; the survivor's file is moved to
  `<dataDir>/trash/<folder>/`, never unlinked.
- Deletions are asserted, never inferred. A replica owns its root, so absence
  there is a deletion; a streamed folder may only tombstone while it is live
  (attached with a holder answering) and never an entry whose blob this
  machine does not hold. Absence in a mirror means "not here yet" — reading
  it as a deletion emptied 448 files into a holder's trash on 2026-10-09.
- A remote update that would land on top of an unscanned local change
  parks that change as a conflict copy first.

## Enforced today (2026-10-07)

- 1, 2: nothing is destroyed without a copy — remote deletes go to trash,
  the store is append-only, disputed content is parked as `.conflict-*`.
- 3: temp → fsync → rename for every write; journals are append-only with
  a checkpoint at open.
- 5: version vectors per file; the deterministic winner/conflict-copy rule
  converges replicas without ping-pong.
- 6, 7: strict config, three-point validation, versioned status document.
- 10: still only status-grade verbs over the local socket; the wire carries
  names, versions, journals and file content over the nebula overlay.
- 11: config-only peers; the transport rejects undeclared names.
- 12: enforced by `Validate` plus the mount-precedence tests.
- 13: attach/detach/remerge implemented (views + symlink swap + detached
  dir merge). Streams are currently full local mirrors; eviction and
  on-demand fetch are later work.
- 14: hello/version/NDJSON framing in the transport; `index`/`want`/`blob`/
  `blobEnd` extend it additively.
- 15: implemented (2026-10-07) — per-folder and per-group `permissions`,
  parent-directory inheritance, re-application on every transition and scan,
  traversable storage chain. The scanner's size/mtime re-read after hashing
  and hash-while-copying store writes came out of the same pass: a stale
  dirent snapshot used to poison an entry's size and turn every later apply
  into a false conflict.
- 4, 8, 9: forward-looking — policies/hooks and min/max protocol
  negotiation do not exist yet.
