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
- The SDK and NDK come from nixpkgs, which needs **both**
  `NIXPKGS_ALLOW_UNFREE=1` (cmdline-tools is unfree) **and**
  `NIXPKGS_ACCEPT_ANDROID_SDK_LICENSE=1`. Do **not** use
  `nixpkgs#androidsdk`: the aggregate pulls every platform, every system
  image and the emulator — ~19 GB of first-evaluation downloads for the few
  binaries a bind or an app build uses. `scripts/build-android-app.sh` (and
  `-engine.sh`) compose instead: `androidenv.composeAndroidPackages` with
  `includeEmulator/includeSystemImages/includeSources/includeCmake = false`,
  one `platformVersions`, one `buildToolsVersions`, `platformToolsVersion`
  and the NDK — seconds of downloading, since the NDK is the only big piece.
  Gotcha: `includeExtras` takes a **list** (default `[]`), not a bool; the
  other `include*` flags take bools. Under a system config the licence
  acceptance is `nixpkgs.config.android_sdk.accept_license = true`.
- Never edit a long-running shell script: bash reads incrementally, so an
  edit mid-run resumes at the wrong byte and produces nonsense like
  `line 36: ersion: command not found` (hit exactly once, 2026-10-15).
- `mobile/` cross-compiles with `CGO_ENABLED=0 GOOS=android GOARCH=arm64 go build` —
  the pure-Go path works, no NDK needed for that check. `gomobile bind`
  does need the NDK (it builds the JNI bridge and all four ABIs).
- gomobile maps package `mobile` to Java class `mobile.Mobile` (package
  functions) and Go struct `Daemon` to `mobile.Daemon`; methods are
  lowercased (`daemon.status()`), every `error` becomes a Java exception,
  and `(string, error)` is the boundary's native shape — which is why the
  whole API is JSON strings. Verified against the built AAR with
  `javap -classpath classes.jar mobile.Mobile mobile.Daemon`: `Mobile.start`
  is a static returning `Daemon`, and `Daemon` carries `status`, `versions`,
  `restore`, `send`, `stop`, all `throws Exception`. All four ABIs ship
  (`jni/{armeabi-v7a,arm64-v8a,x86,x86_64}/libgojni.so`).
- `gomobile bind` refuses a module that does not require
  `golang.org/x/mobile` ("unable to import bind: no Go package in
  golang.org/x/mobile/bind"): the generated code imports `x/mobile/bind`, so
  the requirement has to be in the module graph. `mobile/tools.go`
  (`//go:build tools`) holds it so `go mod tidy` keeps it while nothing in a
  normal build imports it. Consequence: the repo is no longer stdlib-only —
  the flake's `vendorHash` went from `null` to a real hash (nix vendors, the
  repo commits nothing).
- Android kills sockets in background processes, so the engine runs under a
  `dataSync` foreground service (declared `foregroundServiceType="dataSync"`,
  started with `ServiceCompat.startForeground(..., FOREGROUND_SERVICE_TYPE_DATA_SYNC)`).
  A peer that vanishes when the screen locks is not a peer.
- The app's folders are app-private (`/data/data/dev.kyle.storeboard/files`),
  so it needs no storage permission at all. Adopting the real `DCIM/` camera
  roll needs `MANAGE_EXTERNAL_STORAGE` (sideload-only) or a MediaStore-based
  importer — a decision, not a detail.