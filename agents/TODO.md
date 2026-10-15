# TODO

## Android app (2026-10-15)

- Run the app's own build end to end: `scripts/build-android-engine.sh`
  (needs the SDK/NDK, ~19 GB on first evaluation — see NOTES) then
  `gradle assembleDebug` in `android/`. Until that passes, the app's
  *source* is verified only by inspection and by the Go-side tests that
  exercise the same calls.
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