
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

## 2026-10-07 — Overlapping mounts warn; identical mounts fail; folders can be device-scoped

- **Decision:** `config check` rejects two folders whose effective mounts are
  *equal* (no defined outcome) and *warns*, in the CLI report and the daemon
  log, when they nest — the user asked for that shape deliberately: a pool
  root on the holder plus the subtrees other machines stream. Folders stay a group
  property, and the tag gains `storeboard.groups.<g>.folders.<f>.devices`
  (null = every member) so a folder can be carried by specific devices only
  — the folder then simply does not exist elsewhere, which the wire already
  tolerates (a peer without a folder ignores its index).
- **Why:** the user asked whether a `mainpool` folder at `/mainpool`
  alongside folders at `/mainpool/kyle/...` works. Mechanically it does:
  writes are temp-then-rename, content is content-addressed, and the
  holder's filesystem mediates, so the shape converges. It costs, though:
  on the holder every file is indexed by two folders (two journals, two
  scans, two broadcasts, two writers per path) and its content store takes
  a copy of the outer tree; a client that carries both materialises the
  inner trees twice. An error made the tradeoff undecidable for the user,
  who wants it; a warning makes it visible in every build and startup log.
- **Not solved by this:** a *browsable whole-pool view* on a client — every
  folder a device carries is still a full replica or a full streamed
  mirror. That needs on-demand fetch (FUSE work on the DESIGN list), or a
  network mount in the meantime.
- **Reversible:** the guard, yes; the `devices` option is additive.

## 2026-10-07 — storageDir: content location decoupled from the mount point

- **Decision:** `storageDir` (machine-level, overridable per group) puts every
  folder's content at `<storageDir>/<folder>`, with the mount path a symlink
  to it — for replicas and streamed folders alike. Absent (the default) keeps
  the original layout: a replica's content is the mount directory itself, a
  streamed folder's under the data directory. A folder with content still at
  its mount path is migrated into the storage directory on the next
  transition: entry by entry, recursive, never overwriting (a colliding name
  is parked under the deterministic conflict name), so a crash mid-way
  resumes and nothing is lost. Nix: `services.storeboard.storageDir` and
  `groups.<g>.storageDir`; the tag adds a group declaration and a per-device
  override (the device's value becomes that group's storageDir for its host).
- **Why:** the user's ask — "we never have this nesting issue": with roots of
  the form `<storageDir>/<folder>` no two folders can index the same bytes,
  whatever the mounts look like, so the pool-root-plus-subtrees shape stops
  costing double indexing. It also makes the mount a pure view: the mount
  point can sit anywhere, and content lives where the config says.
- **Rejected shapes:** mount == its own storage root (a self-symlink) is a
  config error; storage dirs must be absolute.
- **Reversible:** yes — the option is additive and null restores the old
  layout; migration is a move, so pointing the config back moves nothing
  (the content stays where it was put).

## 2026-10-07 — storageDir defaults to <dataDir>/storage; "" is the old layout

- **Decision:** an absent `storageDir` no longer means "content at the mount
  path" — it means the default, `<dataDir>/storage` (group override still
  wins). The original layout is reachable by setting `storageDir` to the
  empty string explicitly, which is how the pre-storage tests pin their
  behaviour and how anyone keeps a replica's content at the mount. The
  NixOS module computes the same default, so the sandbox grants exactly the
  path the daemon writes.
- **Why:** user direction — "the storageDir path has a sane default". With
  roots of the form `<storageDir>/<folder>`, nesting stops mattering by
  default, and content lands where the config says; the explicit "" keeps a
  supported escape hatch instead of a silent behaviour switch.
- **Also fixed:** `Load` decoded into `Default()`, so a config that declared
  its own `dataDir` still got the XDG-based storage default. Defaults are now
  computed after decoding, from what the file actually says.
- **Migration note:** with the default in place, every existing folder whose
  content sits at its mount path migrates into `<dataDir>/storage/<folder>`
  on the next transition (rename when the filesystems match, otherwise a
  copy), and the mount path becomes a symlink. Documented in README/DESIGN.
- **Reversible:** yes — the option is additive; moving content back is a
  move, and the mount can be pointed anywhere.

## 2026-10-07 — Storage-backed folders keep their mount on the view

- **Decision:** a streamed folder with a storage directory never swaps its
  mount path: the view (the storage root) is where the content already lives,
  so the path shows it whether or not a holder is reachable, offline writes
  land in the folder directly, and the folder is scanned at all times. State
  still reports `attached`/`detached` (is a holder reachable), but the mount
  no longer changes with it. Folders *without* a storage directory keep the
  original dummy-dir-and-merge behaviour.
- **Why:** found while testing the default-storage migration — after a
  migration the mount pointed at the empty scratchpad while the content sat
  in storage, which is exactly the "my files vanished" surprise, and it kept
  the whole root-flip class (scratchpad tombstones, merge-back) alive for no
  reason. With the mount fixed on the view there is no root flip to get wrong.
- **Invariant 13 amended** accordingly: never dangle still holds; "replaced by
  an empty dummy dir" is now the behaviour of the layout without a storage
  directory, not the universal rule.
- **Reversible:** yes (the old path is one branch); it changes only what a
  folder shows while no holder answers, never what is stored.

## 2026-10-09 — Deletions are asserted, and the tests that pin every incident

- **Decision:** a scan tombstones only where absence is evidence. Replicas own
  their root. A streamed folder may tombstone only while it is live (state
  attached with a holder answering) and never an entry whose blob this machine
  does not hold; every skip is logged with a count. The first rule's absence
  is what let an empty storage root delete 448 files across the mesh; the
  second covers the fresh-client variant. Both are tested in both directions:
  the detached/materialised case must survive, the live case must propagate.
- **Tests added for the day's failures** (each verified to fail against the
  old behaviour where a revert was practical):
  - tombstone storm: `TestScanNeverTombstonesUnmaterialisedEntries`,
    `TestStreamedFolderTombstonesOnlyWhileLive`;
  - upgrade paths: `TestLegacyViewMigratesIntoStorage`,
    `TestLegacyViewMovesWhileDetached`, `TestStorageMigrationFromMountPath`;
  - layout: `TestStorageBackedReplica`, `TestStorageBackedStreamedMergesOnReconnect`,
    `TestStorageBackedStreamedMigratesMountContent`, `TestStorageChainIsTraversable`,
    `TestStorageMountRefusesForeignSymlink`;
  - false conflicts: `TestMergeDetachedIdenticalContentIsNotAConflict`,
    `TestMergeDetachedDifferentContentStillConflicts`,
    `TestScanRepairsAStaleSizeWithoutNewVersion`;
  - availability: `TestSnapshotNeverWaitsOnTheScanLock`,
    `TestProgressIsVisibleWhileWorking`;
  - mount lifecycle: `TestDetachedFolderDoesNotTombstoneItsContent`,
    `TestTrashAcrossFilesystems`, `TestMountInheritsParentPermissions` and the
    permission set, `TestEmptyMountPathIsAdopted`, `TestForeignMountPathIsLeftAlone`;
  - end to end: the smoke's takeover, storage-migration and permission sections.
- **Why:** user request — "add tests so what happened before never happens
  again". Every incident of the day now has a test that describes it in its
  name and fails on the code that caused it.
- **Reversible:** tests only.

## 2026-10-09 — Backup: version history, thinning retention, and a collector

- **Decision:** opt-in per machine (`backup.enable`), one `keep` window
  (default one year, parsed as `1y`/`90d`/`4w`/`36h`), per-folder opt-out by
  name. Every version transition (local edit, incoming apply, delete — local
  or remote) appends a line to `<dataDir>/history/<folder>.ndjson`
  referencing the blob; the content store is already append-only, so the
  bytes were always there — this is the index that makes them recoverable.
  Retention keeps the newest version of each hour for a day, each day for a
  week, each week for a month, each month for a year, each year beyond — and
  nothing older than `keep`; the current version is never dropped. The
  collector runs at startup and frees blobs nothing references, refusing to
  act when a reference source cannot be read; the trash ages out by the same
  window. `storeboard backup list|restore` reads and restores it, `--at` or
  `--blob` picking a version, `--to` writing a copy instead of restoring.
- **Why:** user request: "each machine keeps a certain number of versions …
  sparser and sparser … no files sticking around past the file deadline
  1 year … a single configurable option for keep duration … restore through
  the CLI". Streaming machines ignoring backup came from the user in the same
  breath: a mirror should not accumulate versions of content it does not
  hold, which also means one declaration is right on every machine.
- **Reversible:** yes — turning `backup` off keeps the existing history files,
  and the collector reads them off disk as references exactly as it does
  orphan journals, so nothing a history points at is ever collected while the
  file exists (re-enabling makes those versions listable and restorable
  again).

## 2026-10-15 — The phone is a peer: the engine as a library

- **Decision:** the Android app embeds the engine (`mobile/`, bound by
  gomobile) instead of talking to a daemon: the same syncer, store and peer
  wire, with a boundary of JSON strings because that is what gomobile
  carries natively and it is the contract the daemon already serves
  (invariant 17). No local socket inside the app — there is no CLI to serve
  — and the acting verbs authorize the app's own uid, which is the owner of
  the folders the app created. `send` came out of the same work as a
  first-class verb: the share sheet needs it, the desktop wants it, and a
  send never overwrites — a taken name parks the existing file under the
  deterministic conflict name and reports where.
- **Why:** user request — "make an android app … the phone a full peer …
  engine inside the app". A thin client would be a second implementation of
  sync; embedding means the camera roll, the journals and the history are
  hosted by the phone, so a phone with no signal still has its files. The Go
  side was already pure and CGO-free for exactly this.
- **Reversible:** yes — `mobile/` is additive; deleting it leaves the daemon
  and the CLI untouched. `send` is additive on the wire and in the CLI, with
  `apiVersion` unchanged.
- **Open:** the app's own build (SDK/NDK, `gradle assembleDebug`) and the
  `DCIM/` adoption decision — both in NOTES.
