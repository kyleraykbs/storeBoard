
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

## 2026-10-07 — Trash moves cross filesystems; a failed delete never tombstones

- **Decision:** `applyDelete` reports failure, and the caller records the
  tombstone only when the file actually left the folder. The move into
  `<dataDir>/trash/<folder>/` falls back to copy-then-remove when `rename`
  returns `EXDEV`, because the trash lives under the data directory while
  folder roots can sit on other mounts (amanita's pool hit exactly this).
- **Why:** a rename that fails silently while the tombstone propagates
  leaves the file on disk, the scanner indexes it again as a new version,
  and the delete/resurrect loop never converges — observed live as
  continuous re-sync traffic on projectArchive.
- **Also:** the transport now logs why a link closed (`link closed`,
  peer/direction/error): links to amanita were dying every couple of
  minutes with no daemon restart, and nothing recorded the reason.
- **Reversible:** yes (behavior-preserving for same-filesystem deletes).

## 2026-10-07 — A streamed folder is scanned only while attached

- **Decision:** `scanOnce` skips streamed folders whose state is not
  `attached`, and a detach no longer triggers a scan. The detached dir is a
  scratchpad for offline writes (merged into the view on attach); it is
  never treated as the folder's content.
- **Why:** the detach flips the scanned root from the view to the empty
  scratchpad, so a scan saw every known file as vanished and tombstoned the
  whole folder; the next attach resurrected it with fresh versions, and
  every holder reconnect (~2 min, live) churned the entire dataset — 101 GB
  read in 4 minutes, 37 GB RSS on the client, 40 GB written on the holder,
  and a status socket too starved to answer.
- **Reversible:** yes — the only scanning the change removes is one that
  could only produce false tombstones.

## 2026-10-07 — `storeboard folder takeover`, and what busy now says

- **Decision:** `status` reports *why* a folder is busy (`sync[].busy`, one of
  `path has content` / `foreign symlink` / `mount point` / `empty dir cannot
  be removed` / `not a directory`), and the CLI prints the reason with the
  way out where one exists — `storeboard folder takeover <name>`.
  Takeover handles the one case the user can resolve themselves: a real
  directory holding their files. It moves every entry into the folder's
  offline mount (the same landing zone offline writes use), replaces the
  path with the mount symlink, and, when a holder answers, runs the ordinary
  attach so the merge and the push to the mesh happen with no special path.
  Without a holder the mount simply stays offline and the reconnect merges.
  Collisions are parked under the deterministic conflict name, never
  overwritten; a crash mid-way leaves the path busy and the same command
  resumes. Refused for foreign symlinks and live mounts (unmount first).
- **Authorization:** the verb acts on the user's files, so the daemon
  authorizes per caller through unix peer credentials (SO_PEERCRED): root or
  the folder's owner, and a refusal when the uid cannot be read. This
  satisfies DESIGN invariant 10, which is amended to state the rule as
  per-caller authorization rather than "no acting verb at all".
- **Why:** user request — the busy reason was opaque, and telling someone to
  delete a directory is a data-loss instruction. Takeover turns the refusal
  into a supported migration that keeps every byte.
- **Reversible:** yes (a new verb and an additive status field; no config or
  wire change).

## 2026-10-07 — Identical bytes are never a conflict, in the merge too

- **Decision:** when the merge finds the same path on both sides, it compares
  content before calling it a conflict. Byte-identical files collapse into
  one (the newer timestamp survives, the same rule the mesh uses to pick a
  winner), and only genuinely different bytes produce a `.conflict-*` copy.
- **Why:** the merge compared size and mtime first and stopped there, so
  identical screenshots whose mtimes had moved (copied files, materialised
  files, clocks) each produced a conflict copy — observed live as
  `Screenshot from ….png.conflict-<blob8>` next to the original it matched
  byte for byte. Every other park site in the engine (incoming blob, index
  merge) was already content-based; this one was the outlier.
- **Reversible:** yes — strictly fewer copies, nothing else changes.

## 2026-10-07 — Overlapping mounts are a config error; folders can be device-scoped

- **Decision:** `config check` (and therefore every build) rejects two
  folders whose effective mounts are equal or nested. Folders stay a group
  property, and the tag gains `storeboard.groups.<g>.folders.<f>.devices`
  (null = every member) so a folder can be carried by specific devices only
  — the folder then simply does not exist elsewhere, which the wire already
  tolerates (a peer without a folder ignores its index).
- **Why:** the user asked whether a `mainpool` folder at `/mainpool`
  alongside folders at `/mainpool/kyle/...` works. It "works" — it
  converges — but on the holder every file is indexed by two folders (two
  journals, two scans, two broadcasts, two writers per path), the replica
  scan copies the outer tree into the content store, and on a streaming
  peer the inner trees are materialised twice. None of that is visible
  until the disk or the conflict copies show up, so it is a validation
  error with the two ways out in the message, not a runtime surprise.
- **Not solved by this:** a *browsable whole-pool view* on a client — every
  folder a device carries is still a full replica or a full streamed
  mirror. That needs on-demand fetch (FUSE work on the DESIGN list), or a
  network mount in the meantime.
- **Reversible:** the guard, yes; the `devices` option is additive.
