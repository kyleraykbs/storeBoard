#!/usr/bin/env bash
# Builds the whole Android app: the composed SDK, the bound engine, then the
# APK. `scripts/build-android-engine.sh` alone is the fast loop after a Go
# change; this is the one that produces something installable:
#
#   android/app/build/outputs/apk/debug/app-debug.apk
#
# Gradle fetches the Android Gradle Plugin and the AndroidX artifacts from
# Google's Maven and caches them under ~/.gradle, so the first run needs the
# network and a few minutes.
set -euo pipefail
cd "$(dirname "$0")/.."

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
echo "SDK=$ANDROID_HOME"

scripts/build-android-engine.sh

nix shell --impure nixpkgs#gradle nixpkgs#jdk21 -c \
	gradle --no-daemon -p android assembleDebug

echo "wrote android/app/build/outputs/apk/debug/app-debug.apk"