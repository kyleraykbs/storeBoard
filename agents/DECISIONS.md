
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

## 2026-10-07 — Folder permissions: declared per folder/group, else inherited

- **Decision:** the daemon owns the permissions of every path it creates for
  a folder. Resolution is field by field: per-folder entry on this machine
  (`permissions.<folder>`) → the group's default (`groups.<g>.permissions`)
  → the directory the mount lives in, re-read on every mount transition and
  scan. Fields are `user` (name or uid), `group` (name or gid) and `mode`
  (octal, directories only — files keep the mode they were synced with).
  Applies to the view and detached dirs and every directory materialised
  under them; the storage chain to the mount target is made traversable
  (0711) so the symlink resolves for the owner. Nix: `services.storeboard.
  permissions` / `groups.<g>.permissions`; the mycelion tag adds a
  folder-level declaration and a per-device override that map to the same
  two JSON levels.
- **Why:** user direction — "permission the folder every time using the same
  perms as its parent dir, with per-folder and per-group overrides in Nix,
  defaulting back to the parent". Streamed mounts sit in root-owned
  directories left over from the samba era, so without the inheritance the
  mount target would be root's and the user could not reach it (the actual
  reported symptom: `Permission denied` on `~/remote/Videos`).
- **Also fixed (found while proving it):** the scanner read size/mtime from
  WalkDir's dirent snapshot but hashed later, so a file caught mid-write was
  journalled as `Size: 0` with a real blob; the timestamp-only absorb path
  never repaired the size, and every later apply of that path parked a false
  conflict copy. Size/mtime/mode are now re-read after hashing, the absorb
  path refreshes size too, and `adoptFile` hashes the bytes it actually
  stores (a mid-write race could previously store a blob under a sum its
  bytes did not describe).
- **Reversible:** yes, within configVersion 1 (new optional keys; omitting
  them restores pure inheritance).
