#!/usr/bin/env sh
set -eu

GRADLE_COMMAND="${GRADLE_COMMAND:-gradle}"
SYNCLITE_VERSION="${SYNCLITE_VERSION:-1.1.0}"
ANDROID_PROJECT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

command -v "$GRADLE_COMMAND" >/dev/null 2>&1 || {
  echo "Gradle 8.7+ is required to build the Android AAR." >&2
  exit 1
}
command -v cargo >/dev/null 2>&1 || {
  echo "Rust 1.86+ is required to build the Android JNI library." >&2
  exit 1
}
command -v cargo-ndk >/dev/null 2>&1 || {
  echo "cargo-ndk is required. Install it with: cargo install cargo-ndk" >&2
  exit 1
}

cd "$ANDROID_PROJECT"
exec "$GRADLE_COMMAND" "-PsyncliteVersion=$SYNCLITE_VERSION" assembleRelease
