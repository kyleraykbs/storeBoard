# TODO

## Android app (2026-10-15)

- Run the app's own build end to end: `scripts/build-android-app.sh`
  (composes a trimmed SDK — see NOTES — then binds the AAR and runs
  `gradle assembleDebug`). The bind and the Kotlin/Gradle side are the parts
  not yet verified; the Go-side calls they make are covered by `mobile/`'s
  tests on the host.
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