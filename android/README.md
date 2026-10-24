# storeboard for Android

The storeboard engine, running inside an app on the phone. Not a thin client:
the same Go code the pool runs — the syncer, the content store, the peer wire
— is compiled into this package by gomobile, so the phone is a real peer.
That is what makes the phone's camera roll mesh-sync rather than
mesh-adjacent, and it is why a phone with no signal still has its own files
and its own history.

## What is here

- `app/src/main/java/dev/kyle/storeboard/Engine.kt` — wraps the bound engine.
  Everything in and out is JSON in the shapes the daemon's `/v1` document,
  the config file and the CLI already use; the phone speaks the same contract
  as the desktop, minus the local socket (an app has no CLI to serve).
- `SyncService.kt` — a `dataSync` foreground service. On Android a socket in
  a background process is killed with the process, and a peer that vanishes
  is not a peer. The service is the engine's only owner.
- `MainActivity.kt` — one screen: engine state, the status document
  rendered, the config it reads, the log tail, a file picker, and the share
  sheet. A file shared into storeboard is copied in and sent into the camera
  store.

## Building

One command builds everything — the composed SDK, the bound engine, and the
APK:

```sh
scripts/build-android-app.sh
# -> android/app/build/outputs/apk/debug/app-debug.apk
```

For the fast loop after a Go change, bind only (this is the step that makes
`android/app/libs/storeboard.aar`, which is not committed):

```sh
scripts/build-android-engine.sh
```

Both compose the SDK from nixpkgs themselves — one platform, one set of
build tools, the NDK — because the nixpkgs aggregate also drags in every
other platform, every system image and the emulator. The AAR's `-androidapi
26` matches the app's `minSdk`. The SDK and NDK are unfree and the SDK build
needs Google's licence accepted (`NIXPKGS_ACCEPT_ANDROID_SDK_LICENSE=1`,
which the scripts set) — the same acceptance a NixOS Android setup already
makes. Gradle fetches the Android Gradle Plugin and AndroidX from Google's
Maven into `~/.gradle` on first run.

## First run

The app writes a config with two stores — `camera` (the roll, kept local,
syncing out) and `photos` (the library, syncing down, kept readable offline)
— and starts the engine. Two lines to edit before it is useful, and they are
the same line on both sides:

- here: the pool's name and a dialable endpoint (`peers.pool.endpoints`).
  The phone dials out, so the endpoint is what makes the pool reachable.
- on the pool: this phone, listed in the group (`peers = [ "…" "phone" ]`).
  No endpoint needed — a declared but unaddressed peer just waits to be
  dialled, and the daemon rejects a name it does not know (DESIGN
  invariant 11/14), which is what keeps pairing a config change on machines
  you own rather than a secret passed around.

Install with `adb install android/app/build/outputs/apk/debug/app-debug.apk`.
The APK carries all four ABIs (~30 MB, mostly the Go runtime), so any phone
or emulator will take it.

## Deliberate gaps

- The root and its stores are app-private (`/data/data/dev.kyle.storeboard/files`), so no
  storage permission is needed and nothing else on the phone can read or
  corrupt the store. Adopting the real `DCIM/` camera roll is the next step
  and needs a decision: a store rooted there (or a declared symlink) requires
  `MANAGE_EXTERNAL_STORAGE` (sideload-only, which this app is), or a
  MediaStore-based importer that copies rather than links.
- Restore is wired to the engine but not yet to a screen; the version list is
  one dialog away.
- The status view renders the document's fields tolerantly, so a daemon that
  grows a field does not break the app glance.
