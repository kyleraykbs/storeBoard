#!/usr/bin/env bash
# Binds the engine in mobile/ into android/app/libs/storeboard.aar — the one
# artifact the app build needs from this repository. Re-run it whenever the
# Go side changes; it takes a few minutes.
#
# gomobile needs the Android SDK, its build tools and the NDK. Both are
# unfree and the SDK also needs Google's licence accepted — the two variables
# below are that acceptance, the same one a NixOS Android setup already
# makes. The SDK is composed rather than taken whole: the nixpkgs aggregate
# also drags in every other platform, every system image and the emulator,
# ~19 GB of first-evaluation downloads for the few binaries a bind uses.
set -euo pipefail
cd "$(dirname "$0")/.."

out="${1:-android/app/libs/storeboard.aar}"
mkdir -p "$(dirname "$out")"

export NIXPKGS_ALLOW_UNFREE=1
export NIXPKGS_ACCEPT_ANDROID_SDK_LICENSE=1

sdk=$(nix build --impure --no-link --print-out-paths --expr '
  let
    flake = builtins.getFlake "nixpkgs";
    pkgs = flake.legacyPackages.${builtins.currentSystem};
  in
  (pkgs.androidenv.composeAndroidPackages {
    includeEmulator = false;
    includeSystemImages = false;
    includeSources = false;
    includeCmake = false;
    includeNDK = true;
    ndkVersion = "29.0.14206865";
    platformVersions = [ "35" ];
    buildToolsVersions = [ "35.0.0" ];
    platformToolsVersion = "35.0.2";
  }).androidsdk')

export ANDROID_HOME="$sdk/libexec/android-sdk"
export ANDROID_SDK_ROOT="$ANDROID_HOME"
if [[ -z "${ANDROID_NDK_HOME:-}" ]]; then
	ndk=$(ls -d "$ANDROID_HOME"/ndk/* 2>/dev/null | sort -V | tail -1 || true)
	if [[ -n "$ndk" ]]; then
		export ANDROID_NDK_HOME="$ndk" ANDROID_NDK_ROOT="$ndk"
	fi
fi
echo "SDK=$ANDROID_HOME NDK=${ANDROID_NDK_HOME:-none}"

nix shell --impure nixpkgs#gomobile nixpkgs#jdk21 -c \
	gomobile bind -target=android -androidapi 26 -o "$out" ./mobile

echo "wrote $out"