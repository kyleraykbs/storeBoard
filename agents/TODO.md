# TODO

## Stores cutover (2026-10-23)

- Manual fleet migration to configVersion 2: give each machine a `root`
  (amanita: `/mainpool/storeboard`, psilocybe: `~/Storeboard`), turn the old
  absolute mounts into `symlinks`, and run `store takeover` where a path
  already holds files. No migration code by design — the daemon leaves
  existing files alone.
- configVersion 1 is rejected. Deploy order: update the flake input, write the
  v2 config, then restart the daemon. A v2 daemon and a v1 daemon cannot share
  a wire (apiVersion 2 vs 1 shapes); upgrade the whole mesh together.
- Android: the app's default config is now v2 (`root` + two stores), but the
  installed APK still writes v1 until the AAR and APK are rebuilt.

## Android app (2026-10-15)

- Built and packaged; the remaining step is a device: `adb install
  android/app/build/outputs/apk/debug/app-debug.apk`, then edit the config
  on first run (pool name + endpoint here, `phone` in the pool's group).
  Nothing has exercised the app at runtime yet — the engine it embeds is
  covered on the host, and `TestAppDefaultConfigIsValid` pins the config it
  writes.
- Decide the camera-roll question: keep the app-private `camera` store, or
  adopt `DCIM/` with `MANAGE_EXTERNAL_STORAGE` (sideload-only) or a
  MediaStore importer that copies instead of linking.
- Restore UI: the engine call exists (`Engine.restore`); a version list and
  a "restore this version" action are one dialog away.
- Serve a local socket? Not needed for the app (the UI is in-process), but
  it would let `adb`-driven scripts and a desktop over USB talk to the
  phone's daemon with the same CLI.

## Streamed stores

- Add the holder-selection/read-through protocol: FUSE on Linux, ranged
  requests, throughput probes/EWMA, optional pinned holder, and fallback in
  holder-rate order.
- Add write-through plus offline queue policy: `unbounded` by default,
  `capped` with an explicit byte limit, or `readonly`. Offline mounts show the
  journal's directory tree but no file bytes.
- Add the mobile on-demand fetch API; Android does not mount FUSE.
- Stage the rollout on a scratch store. Only after the real path passes reads,
  writes, failover and reconnect tests should BulkPictures switch from its
  current mirror; hash-verify its 69 GiB mirror before eviction.

## Sync engine

- Event stream (`--follow`, NDJSON) — designed, not built.
- Min/max protocol negotiation across Nix-pinned versions (invariant 10).
- Remote stores: eviction and on-demand fetch. The canonical dir is a full
  local mirror for now; the tree makes per-store scoping free, so this is the
  last piece of "the whole pool without copying the whole pool".
- A view that hides the flat canonical dirs (`root/<id>`) and shows only the
  symlinked tree. Cosmetic; the bytes and the links already work.