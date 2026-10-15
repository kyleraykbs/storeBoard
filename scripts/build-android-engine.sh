#!/usr/bin/env bash
# Binds the engine in mobile/ into android/app/libs/storeboard.aar — the one
# artifact the app build needs from this repository. Re-run it whenever the
# Go side changes; it takes a minute or so.
#
# gomobile needs the Android SDK and NDK. Both come from nixpkgs, both are
# unfree, and building the SDK also needs Google's licence accepted — the two
# variables below are that acceptance, the same one a NixOS Android setup
# already makes.
set -euo pipefail
cd "$(dirname "$0")/.."

out="${1:-android/app/libs/storeboard.aar}"
mkdir -p "$(dirname "$out")"

export NIXPKGS_ALLOW_UNFREE=1
export NIXPKGS_ACCEPT_ANDROID_SDK_LICENSE=1

nix shell --impure \
  nixpkgs#gomobile \
  nixpkgs#androidsdk \
  nixpkgs#androidenv.androidPkgs.ndk-bundle \
  -c bash -euo pipefail -c '
    export ANDROID_HOME="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}"
    if [[ -z "${ANDROID_NDK_HOME:-}" && -n "$ANDROID_HOME" ]]; then
      ndk=$(ls -d "$ANDROID_HOME"/ndk/* 2>/dev/null | sort -V | tail -1 || true)
      if [[ -n "$ndk" ]]; then
        export ANDROID_NDK_HOME="$ndk" ANDROID_NDK_ROOT="$ndk"
      fi
    fi
    if [[ -z "${ANDROID_NDK_HOME:-}" ]]; then
      echo "no NDK in the environment; letting gomobile fetch one" >&2
      gomobile init
    fi
    echo "SDK=${ANDROID_HOME:-none} NDK=${ANDROID_NDK_HOME:-none}"
    gomobile bind -target=android -androidapi 26 -o '"$out"' ./mobile
  '

echo "wrote $out"
