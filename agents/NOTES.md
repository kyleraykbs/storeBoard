# Notes

Facts learned the hard way, worth keeping.

## storeboard engine notes (2026-10-07)

- `STOREBOARD_SCAN_INTERVAL` (duration) drives the scanner fast for tests
  and smoke; default 15 s.
- Test harness traps: allocate endpoint pairs with both listeners held
  (sequential `freeEndpoint` calls can hand out the same ephemeral port,
  which makes a daemon dial itself and reject the connection as
  "peer claims to be me"); `STOREBOARD_TEST_LOG=1` turns on engine+transport
  logging in tests.
- Version vectors: a local change must INHERIT the previous entry's vector
  and bump its own counter. Replacing it makes history look concurrent and
  turns every ordinary edit into a conflict.

## Android toolchain (2026-10-15)

- The app's engine comes from `scripts/build-android-engine.sh`, which runs
  `gomobile bind -target=android -androidapi 26 -o android/app/libs/storeboard.aar ./mobile`.
  The AAR is not committed (`android/app/libs/*.aar` is ignored): it is a
  build product of the Go source and would go stale silently.
- The SDK and NDK come from nixpkgs (`nixpkgs#androidsdk`,
  `nixpkgs#androidenv.androidPkgs.ndk-bundle`), which needs **both**
  `NIXPKGS_ALLOW_UNFREE=1` (cmdline-tools is unfree) **and**
  `NIXPKGS_ACCEPT_ANDROID_SDK_LICENSE=1` — and the first evaluation pulls
  ~19 GB (every platform, system image and emulator the `androidsdk`
  package aggregates; a smaller custom SDK would cut that to a few GB).
  Under a system config the same acceptance is
  `nixpkgs.config.android_sdk.accept_license = true`.
- `mobile/` cross-compiles with `CGO_ENABLED=0 GOOS=android GOARCH=arm64 go build` —
  the pure-Go path works, no NDK needed for that check. `gomobile bind`
  does need the NDK (it builds the JNI bridge and all four ABIs).
- gomobile maps package `mobile` to Java class `mobile.Mobile` (package
  functions) and Go struct `Daemon` to `mobile.Daemon`; methods are
  lowercased (`daemon.status()`), every `error` becomes a Java exception,
  and `(string, error)` is the boundary's native shape — which is why the
  whole API is JSON strings.
- Android kills sockets in background processes, so the engine runs under a
  `dataSync` foreground service (declared `foregroundServiceType="dataSync"`,
  started with `ServiceCompat.startForeground(..., FOREGROUND_SERVICE_TYPE_DATA_SYNC)`).
  A peer that vanishes when the screen locks is not a peer.
- The app's folders are app-private (`/data/data/dev.kyle.storeboard/files`),
  so it needs no storage permission at all. Adopting the real `DCIM/` camera
  roll needs `MANAGE_EXTERNAL_STORAGE` (sideload-only) or a MediaStore-based
  importer — a decision, not a detail.