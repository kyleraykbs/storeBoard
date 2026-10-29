# Design

Invariants. Every change is checked against these; if a change must break
one, it changes here first, deliberately, with a decision log entry.

## The model

- **One root.** A single `root` directory, declared in the config with a sane
  default (`~/Storeboard`), is the only place store content lives. It is a
  plain container, not a store.
- **Stores are flat under the root.** Every store is a real directory at
  `<root>/<id>`; ids are globally unique. Nothing is nested physically.
- **The tree is symlinks.** A child store appears inside its parent at its
  declared relpath (default: its id, at the parent's top level) as a symlink
  to the child's canonical dir. An external `symlinks` entry is the same
  object pointed outward: an absolute path that links to the canonical dir. A
  path is a name for a store, never where its bytes are kept.
- **Inheritance.** An undeclared field follows the parent: group, mount type,
  permissions, backup opt-out. Declared means its own thing. The tree shape
  (ids, relpaths) is shared by every peer; a machine chooses mount type,
  whether it carries a store, and its symlinks.

## Data safety

1. Removing a store, a peer, or the whole config never deletes data on
   disk. Deletion is always an explicit act, never a side effect of
   reconfiguration.
2. Unsynced data is never evicted from any cache.
3. Writes are temp-file-then-rename and fsync'd. The daemon must survive
   `kill -9` mid-write without corrupting state; crash consistency is tested
   by killing it in CI.
4. Policies fail closed. A receiver filter or any config expression that
   errors rejects and logs — never accepts.
5. The daemon never migrates, moves or adopts existing content on its own.
   A declared foreign symlink may be replaced atomically: replacing the link
   inode never follows or deletes its target, so old content remains intact.
   A real non-empty directory, a live mountpoint, or an ordinary file at the
   declared path is reported `busy` and left exactly where it is until the
   user runs `store takeover`. A store's canonical dir that already exists is
   adopted as it stands; nothing is renamed into it. Canonical dirs and links
   are reconciled every five seconds: missing paths are recreated, empty
   declared directories become links, and non-empty directories remain busy.

## Ordering and conflicts

6. Concurrent writes are ordered by version vector. A conflict is recorded
   and surfaced, never silently overwritten.

## Contracts

7. JSON shapes are versioned (`apiVersion` in every document,
   `configVersion` in the config file): within a version, changes are
   additive only — a field's meaning never changes and never disappears.
   Human-readable output may change freely. `pkg/api` is the schema.
8. The config file is strict — unknown keys are an error. Validation runs in
   three places, all the same validator: `storeboard config check`, at daemon
   startup, and at build time under each Nix module's `checkedConfig`, so a
   bad value fails nixos-rebuild. This build reads configVersion 2 and
   rejects version 1: there is no migration.

## Process

9. Hooks and policies never sit in the sync data path: a slow or failing
   hook cannot block a drain. When config expressions land (gojq, pure Go),
   they are compiled and validated at config-check time, then run sandboxed
   — no environment, no file access, hard timeout and output limit.
10. Peers advertise min/max protocol versions and pick the highest common
    one; unknown fields are tolerated on the wire, matching invariant 7.
11. Verbs that act on the user's data are authorized per caller, never by
    socket access alone. The daemon socket is world-writable so any user's
    CLI can read status; an acting verb (today: `store takeover`, `send`,
    `backup restore`) checks the requesting uid through the socket's peer
    credentials and allows only root or the store's owner — a refusal, not a
    guess, when the uid cannot be read.
12. Peers are declared, never invented: the config (Nix-generated) is the
    authoritative peer list, and the daemon learns a peer only from it.
    Address discovery (DNS, NAT traversal) may layer on top later;
    declaration stays authoritative, and removing a peer revokes it.

## The tree and the scanner

13. **A store's canonical dir is the only thing scanned.** `<root>/<id>` is
    the store's content; the scanner never descends into a symlink and never
    journals one as an entry. This is what makes nesting free: a child's
    bytes are never indexed by its parent, there is no second journal, and
    the content store never takes a copy of the outer tree. A declared link
    path is config, not content, and is recreated from config on every start.
14. **A remote store never dangles and never disappears.** Its canonical dir
    is the same directory whether or not a holder answers; `attached` /
    `detached` reports liveness only. Offline writes land in the store and
    sync when a holder returns. There is no dummy dir and no root flip.
15. **Merges never replace.** Content that would land on a name already taken
    is parked under the deterministic conflict name (`<name>.conflict-<blob8>`)
    and indexed like any other file. The same rule applies to `send` and to
    takeover.
16. **Deletions are asserted, never inferred.** A `local` store owns its
    canonical dir, so absence there is a deletion. A `remote` store may
    tombstone only while a holder is reachable and never an entry whose blob
    this machine does not hold. Absence in a mirror means "not here yet" —
    reading it as a deletion emptied 448 files into a holder's trash once.
17. **Mounts are usable by the human, not by the daemon.** Every directory
    the daemon creates — the root, the canonical dirs, the links' parents —
    is owned and permissioned to the store's resolved permissions: declared
    per store on this machine, else the store's chain, else the group default,
    else the directory the root lives in (re-read on every scan). Nothing
    declared means inherited. A path that belongs to something else is never
    touched — reported `busy` instead.

## Sync engine

- Every version transition is recorded per store in a history log
  (`<dataDir>/history/<id>.ndjson`, append-only, checkpointed like the
  journals) referencing the blob that holds those bytes; retention thins it
  on the schedule above and `storeboard backup list|restore` reads it. A
  store that is `remote` on this machine keeps no history.
- Content is content-addressed (`<dataDir>/store/<sha256>`), written temp →
  fsync → rename. It is a *cache*, not a second copy: once a file is
  materialised at its canonical path, the blob is released, and a peer is
  served by hashing that file when the blob is gone. Blobs are kept only where
  they are the only copy — a retained history version, a file that is not on
  disk here, or an entry this machine never materialised.
- The state directory lives inside the store root (`<root>/.storeboard`), so a
  store's bytes and its state are on the same filesystem and a root on a pool
  keeps everything there.
- Every store keeps an append-only NDJSON journal (`<dataDir>/journal/<id>.ndjson`)
  replayed into memory at open and checkpointed; each entry carries a version
  vector.
- A scan (default every 15 s, `STOREBOARD_SCAN_INTERVAL` overrides) hashes
  only files whose size or mtime moved; vanished files become tombstones.
  Size, mtime and mode are re-read after the hash, because the directory
  snapshot WalkDir saw can be stale (a file truncated and rewritten mid-walk
  reads as size 0) — and an entry whose size contradicts its blob makes every
  later apply park a false conflict. The blob copied into the store is hashed
  while it is copied, so a store entry can never be described by a sum its
  bytes do not have.
- Symlinks, sockets and fifos are not content: they are skipped, and never
  followed. The declared links are the daemon's to create and repair, from
  config.

### Streamed stores (decided 2026-10-29, implementation in progress)

A store declared `remote` is not a mirror: the canonical path is a FUSE mount
owned by storeboard, and file bytes are not retained on this machine.

- The journal stays local and is the whole truth about the tree. Directories
  are synthesised from it, so structure exists even with no holder.
- A read opens the file from the fastest reachable holder for that store.
  Holders are peers declaring the store `local`; throughput is measured from
  real transfers (seeded by a small probe on connect), with an optional pinned
  preference taking priority.
- Reads use ranged requests. A 20 GiB video does not require a 20 GiB temp
  file, and seeking does not restart a whole-file transfer.
- A write goes through to the chosen holder. When no holder is reachable, the
  mount still presents the directory tree and queues writes locally; the queue
  policy is configurable as `unbounded` (default), `capped` with a byte limit,
  or `readonly`. Queued writes merge on reconnect under the ordinary conflict
  rules.
- `versions` and `restore` are forwarded to a holder: the machine that keeps
  the history and the bytes answers them, and a peer in the store's group is
  already authorized by the config. `send` is an ordinary write through the
  mount.
- A streamed store's canonical path is storeboard's own mount, so engine code
  never touches it: journal, queue and mount only. A handler that walked or
  removed a path there would re-enter the filesystem while holding the scan
  lock.
- FUSE hands the filesystem `FLUSH` on every close(2) of a descriptor that
  refers to a file, not once per open: a shell that dups its redirect target
  closes the original *before* it writes. `FLUSH` therefore only syncs and
  `RELEASE` commits; a queue policy that refuses data refuses it at write(2),
  where the caller can still act on it.
- There is no read cache: a file is fetched per open/range. Disk cost is
  metadata plus queued offline writes only.
- A detached streamed store never tombstones: absence is not deletion, and the
  journal is authoritative.
- Rollout is staged: a scratch store proves the FUSE/read/write/failover path;
  only then does BulkPictures change from the current mirror to streamed mode.
  Its existing mirror is hash-verified against a holder before eviction.
- Linux uses FUSE. The Android engine gets an on-demand fetch API instead of
  mounting FUSE or silently keeping a second mirror.
- Peers exchange index deltas; a side that lacks or is behind a version asks
  for the blob (`want` → `blob` chunks → `blobEnd`); a side that is ahead
  pushes its index entry. Concurrent edits resolve deterministically (newer
  mtime wins, blob hash breaks ties): the winner keeps the path on both
  machines, the loser is parked as `<name>.conflict-<blob8>` and indexed like
  any other file, so conflict copies themselves replicate.
- Deletes propagate as tombstones; the survivor's file is moved to
  `<dataDir>/trash/<id>/`, never unlinked.
- A remote update that would land on top of an unscanned local change parks
  that change as a conflict copy first.

## Enforced today

- 1, 2: nothing is destroyed without a copy — remote deletes go to trash,
  the store is append-only, disputed content is parked as `.conflict-*`.
- 3: temp → fsync → rename for every write; journals are append-only with a
  checkpoint at open; partial scratches are wiped at startup.
- 4: reserved for policies, which do not exist yet.
- 5: takeover is explicit; nothing else moves content.
- 6: version vectors per file; the deterministic winner/conflict-copy rule
  converges replicas without ping-pong.
- 7, 8: strict config, three-point validation, versioned status document
  (apiVersion 2, configVersion 2, no v1 migration).
- 9, 10: forward-looking — policies/hooks and min/max protocol negotiation
  do not exist yet.
- 11: acting verbs exist and are authorized per caller: `store takeover`,
  `send`, `backup restore` (socket peer credentials, root or the store's
  owner, refusal when the uid cannot be read); everything else on the socket
  is status-grade.
- 12: config-only peers; the transport rejects undeclared names.
- 13: canonical dirs under root, child links and external symlinks created
  and reported; the scanner skips every symlink. Covered by
  `TestChildStoreIsALinkAndNeverIndexedByParent`,
  `TestExternalSymlinkLinkedAndBusy`, and the smoke.
- 14: remote stores keep their canonical dir; attach/detach is a state flag.
  Covered by `TestRemoteStoreStaysVisibleWithoutAHolder`.
- 15: content-based conflict resolution and the send/takeover park rules.
- 16: tombstone authority per store, both directions tested
  (`TestRemoteStoreTombstonesOnlyWhileLive`,
  `TestScanNeverTombstonesUnmaterialisedEntries`).
- 17: per-store/group/inherited permissions, re-applied on every scan.
- The peer wire is newline-delimited JSON over TCP. Both sides open with a
  hello (protocol, name, version); a disagreement on the protocol version
  ends the connection, and a name that is not declared in config is rejected.
  Unknown frames are tolerated and noted. TLS with device certificates,
  built on the threat model, precedes anything sensitive.
- Version history is opt-in per machine and thinned by a fixed schedule: the
  newest version of each hour for a day, of each day for a week, of each week
  for a month, of each month for a year, of each year beyond — and nothing
  older than the keep window (`backup.keep`, default one year). The current
  version of a file is never dropped. A store that is `remote` on a machine
  keeps no history there. The retention policy and the content collector are
  the only things the daemon ever deletes on its own: the collector frees a
  blob only when no journal, no kept version, no orphan journal and no recent
  write references it, and it refuses to act at all when a reference source
  cannot be read.
- One engine, no client-only mode. Anything that syncs runs the same syncer,
  content store and peer wire: the daemon, and the phone app via the bound
  library (`storeboard/mobile`). A device with no CLI to serve has no local
  socket — but the documents it answers with are the ones the daemon serves,
  `apiVersion` and all. Sending a file (the share sheet, `storeboard send`)
  is an ordinary local change: indexed at once, recorded in the history,
  synced onwards. Nothing is overwritten — a name already taken parks the
  existing file under the deterministic conflict name — and the sender is
  told where it went.