# TODO

## Android app (2026-10-15)

- Built and packaged; the remaining step is a device: `adb install
  android/app/build/outputs/apk/debug/app-debug.apk`, then edit the config
  on first run (pool name + endpoint here, `phone` in the pool's group).
  Nothing has exercised the app at runtime yet — the engine it embeds is
  covered on the host, and `TestAppDefaultConfigIsValid` pins the config it
  writes.
- Decide the camera-roll question: keep the app-private `camera` folder, or
  adopt `DCIM/` with `MANAGE_EXTERNAL_STORAGE` (sideload-only) or a
  MediaStore importer that copies instead of mounting.
- Restore UI: the engine call exists (`Engine.restore`); a version list and
  a "restore this version" action are one dialog away.
- Serve a local socket? Not needed for the app (the UI is in-process), but
  it would let `adb`-driven scripts and a desktop over USB talk to the
  phone's daemon with the same CLI.

## Sync engine

- Event stream (`--follow`, NDJSON) — designed, not built.
- Min/max protocol negotiation across Nix-pinned versions (invariant 9).
- Storage-backed streamed folders: eviction and on-demand fetch
  (DESIGN invariant 13's later work).