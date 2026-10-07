
## [2026-10-06] Config model: peers → groups → folders, local mounts
- Decision: config declares `peers` (name → host:port endpoints), `groups`
  (peer membership + folders, each folder with an optional default mount),
  and `mounts` (this machine's per-folder override of a group folder's
  mount). Effective mount: local override → group default →
  `<dataDir>/mounts/<name>`. This machine names itself with `peer`
  (hostname fallback only while no groups are declared). Folder names are
  global across groups; every group must include this peer; endpoints must
  parse as host:port. All state lives under the one `dataDir`.
- Why: user direction — folders belong to groups of peers, each peer picks
  its own local mountpoint, one storage directory owns everything.
- Reversible: yes (pre-release; configVersion stays 1)

## 2026-10-07 — storeboard engine shipped (sync + mounts)

- **Shipped:** the sync engine (storeboard 69a17b0). Content-addressed
  blobs under `<dataDir>/store`; per-folder NDJSON journals replayed into
  memory with per-file version vectors; a scanner (15 s default,
  `STOREBOARD_SCAN_INTERVAL` overrides) that hashes only moved files and
  tombstones vanished ones; index/want/blob/blobEnd sync over the existing
  wire; deletes move to `<dataDir>/trash/<folder>/`; concurrent writes
  resolve deterministically (newer mtime, blob hash breaks ties) with the
  loser parked as `<name>.conflict-<blob8>` and replicated like any file.
- **Mounts:** streamed folders are symlinks into `<dataDir>/views/<name>`
  (attached), swapped to `<dataDir>/detached/<name>` when no holder answers
  (the "dummy dir"), and merged back on reconnect. A path owned by someone
  else (a samba mount, a real dir) is never touched — reported `busy`.
- **Bugs found and fixed while proving it:** local edits must inherit the
  entry's version vector (else every edit looked concurrent); the module's
  sandbox restricted socket families; per-folder operation locks are
  required so applies and mount transitions cannot interleave; libc-free
  cleanup of stale scratch at startup.
- **Reversible:** yes — the engine only touches paths its config declares.
