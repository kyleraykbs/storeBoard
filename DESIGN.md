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

## Enforced today (2026-10-03)

- 6, 7: strict config, three-point validation, versioned status document.
- 10: holds trivially (only `/v1/status` exists) and is pinned in code and
  here.
- 1, 2, 3, 4, 5: forward-looking — nothing violates them because nothing
  writes yet; they bind the sync engine when it lands.
- 8, 9: forward-looking; both shape interfaces (hooks, wire format) that do
  not exist yet.
