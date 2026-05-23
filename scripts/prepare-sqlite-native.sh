#!/usr/bin/env sh
set -eu

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <sqlite-version-numeric> <sqlite-download-year> <destination-dir>" >&2
  exit 2
fi

SQLITE_VERSION_NUMERIC="$1"
SQLITE_DOWNLOAD_YEAR="$2"
DEST_DIR="$3"

mkdir -p "$DEST_DIR"

OS_NAME="$(uname -s)"
ARCH_NAME="$(uname -m)"

case "$OS_NAME" in
  Linux|Darwin) OUT_LIB="libsqlite3.a" ;;
  *)
    echo "Unsupported OS for prepare-sqlite-native.sh: $OS_NAME" >&2
    exit 1
    ;;
esac

TARGET_LIB="$DEST_DIR/$OUT_LIB"
TARGET_HEADER="$DEST_DIR/sqlite3.h"
TARGET_SOURCE="$DEST_DIR/sqlite3.c"

if [ -f "$TARGET_LIB" ] && [ -f "$TARGET_HEADER" ] && [ -f "$TARGET_SOURCE" ]; then
  echo "SQLite native library already prepared at $TARGET_LIB"
  exit 0
fi

TMP_DIR="$DEST_DIR/_tmp"
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR"

AMALG_ZIP="$TMP_DIR/sqlite-amalgamation-$SQLITE_VERSION_NUMERIC.zip"
AMALG_URL="https://www.sqlite.org/$SQLITE_DOWNLOAD_YEAR/sqlite-amalgamation-$SQLITE_VERSION_NUMERIC.zip"

EXTRACT_DIR="$TMP_DIR/extract"
mkdir -p "$EXTRACT_DIR"

if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$AMALG_URL" -o "$AMALG_ZIP"
elif command -v wget >/dev/null 2>&1; then
  wget -q "$AMALG_URL" -O "$AMALG_ZIP"
else
  echo "Neither curl nor wget is available to download $AMALG_URL" >&2
  exit 1
fi

(
  cd "$EXTRACT_DIR"
  if command -v unzip >/dev/null 2>&1; then
    unzip -qq "$AMALG_ZIP"
  else
    jar xf "$AMALG_ZIP"
  fi
)

EXTRACTED_C="$(find "$EXTRACT_DIR" -type f -name sqlite3.c | head -n 1)"
EXTRACTED_H="$(find "$EXTRACT_DIR" -type f -name sqlite3.h | head -n 1)"

if [ -z "$EXTRACTED_C" ] || [ ! -f "$EXTRACTED_C" ]; then
  echo "Could not find sqlite3.c in sqlite amalgamation archive: $AMALG_URL" >&2
  exit 1
fi

if [ -z "$EXTRACTED_H" ] || [ ! -f "$EXTRACTED_H" ]; then
  echo "Could not find sqlite3.h in sqlite amalgamation archive: $AMALG_URL" >&2
  exit 1
fi

cp "$EXTRACTED_C" "$TARGET_SOURCE"
cp "$EXTRACTED_H" "$TARGET_HEADER"

SQLITE_OBJ="$TMP_DIR/sqlite3.o"
cc -O2 -fPIC -DSQLITE_THREADSAFE=1 -c "$TARGET_SOURCE" -o "$SQLITE_OBJ"
ar rcs "$TARGET_LIB" "$SQLITE_OBJ"

rm -rf "$TMP_DIR"

echo "Prepared SQLite native library at $TARGET_LIB"
