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
10. No privileged verb ships while the daemon socket is world-accessible
    (mode 0666, internal/server). Tightening the socket — and the threat
    model that decides who may talk to it — precedes `send`, `receive`, or
    anything else that acts.
11. Peers are declared, never invented: the config (Nix-generated) is the
    authoritative peer list, and the daemon learns a peer only from it.
    Address discovery (DNS, NAT traversal) may layer on top later;
    declaration stays authoritative, and removing a peer revokes it.
12. Every folder resolves to exactly one local mount point through a fixed
    precedence — local override, group default, then inside the one
    storage directory — and the config names its owner group, so no path
    is ever ambiguous about what syncs there.
13. Remote (streamed) folders never dangle. While a holder is reachable the
    folder mounts live; when none is, the mount point is replaced by an
    empty dummy dir — nothing in the mount table ever points at nothing
    (the sambaMounts lesson). On reconnect the view returns and the delta
    remerges. Which folders stream is each machine's own `keepOffline`
    declaration (default true: hold a replica that works offline).
14. The peer wire is newline-delimited JSON over TCP. Both sides open with
    a hello (protocol, name, version); a disagreement on the protocol
    version ends the connection, and a name that is not declared in config
    is rejected — a link can never introduce a peer (invariant 11 holds on
    the wire too). Unknown frames are tolerated and noted. The wire carries
    status-grade data only (names, versions, keepalives); TLS with device
    certificates, built on the threat model, precedes anything sensitive.

## Sync engine (2026-10-07)

- Content is content-addressed (`<dataDir>/store/<sha256>`), shared by all
  folders on a machine, written temp → fsync → rename.
- Every folder keeps an append-only NDJSON journal (`<dataDir>/journal/`)
  replayed into memory at open and checkpointed; each entry carries a
  version vector.
- A scan (default every 15 s, `STOREBOARD_SCAN_INTERVAL` overrides) hashes
  only files whose size or mtime moved; vanished files become tombstones.
- Peers exchange index deltas; a side that lacks or is behind a version
  asks for the blob (`want` → `blob` chunks → `blobEnd`); a side that is
  ahead pushes its index entry. Concurrent edits resolve deterministically
  (newer mtime wins, blob hash breaks ties): the winner keeps the path on
  both machines, the loser is parked as `<name>.conflict-<blob8>` and
  indexed like any other file, so conflict copies themselves replicate.
- Deletes propagate as tombstones; the survivor's file is moved to
  `<dataDir>/trash/<folder>/`, never unlinked.
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
- 4, 8, 9: forward-looking — policies/hooks and min/max protocol
  negotiation do not exist yet.
