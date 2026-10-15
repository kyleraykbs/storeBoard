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
  folder.

## Building

Two steps. First bind the engine (writes `android/app/libs/storeboard.aar`,
which is not committed):

```sh
scripts/build-android-engine.sh
```

Then build the app:

```sh
nix shell nixpkgs#jdk21 nixpkgs#gradle -c bash -c '
  cd android
  ANDROID_HOME=$ANDROID_SDK_ROOT gradle assembleDebug'
```

The AAR's `-androidapi 26` matches the app's `minSdk`. The SDK and NDK come
from nixpkgs and are unfree, and the SDK build needs Google's licence
accepted (`NIXPKGS_ACCEPT_ANDROID_SDK_LICENSE=1`, which the script sets) —
that is the same acceptance a NixOS Android setup already makes.

## First run

The app writes a config with two folders — `camera` (the roll, kept local,
syncing out) and `photos` (the library, syncing down, kept readable offline)
— and starts the engine. The one line to edit before it is useful is the
peer list: it must name this phone exactly as the pool's config declares it,
and the pool by fingerprint. Pairing stays a config change on machines you
own rather than a secret passed around.

## Deliberate gaps

- The folders are app-private (`/data/data/dev.kyle.storeboard/files`), so no
  storage permission is needed and nothing else on the phone can read or
  corrupt the store. Adopting the real `DCIM/` camera roll is the next step
  and needs a decision: a plain directory mount there requires
  `MANAGE_EXTERNAL_STORAGE` (sideload-only, which this app is), or a
  MediaStore-based importer that copies rather than mounts.
- Restore is wired to the engine but not yet to a screen; the version list is
  one dialog away.
- The status view renders the document's fields tolerantly, so a daemon that
  grows a field does not break the app glance.
