#!/usr/bin/env bash
# Build PyPI-acceptable manylinux Python wheels for the `synclite` package.
#
# This is invoked by the root pom.xml (execution `build-python-wheels-linux`)
# via WSL on Windows hosts, or directly on a Linux build host. It produces
# self-contained manylinux_2_28 wheels for x86_64 and aarch64 by letting
# maturin run its built-in auditwheel repair, which bundles the DuckDB shared
# library and rewrites RPATH using patchelf (a POSIX-only tool that has no
# Windows binary - hence the need to run the wheel build inside Linux).
#
# The script is BEST-EFFORT by contract: the Maven execution that calls it is
# configured with failOnError=false, and this script additionally traps errors
# so a missing toolchain prints a clear skip message instead of aborting.
#
# Usage:
#   build_linux_wheels.sh <python-project-dir> <out-dir> <revision>
#
#   <python-project-dir>  path to synclite-logger-rust/python (Linux/WSL path)
#   <out-dir>             where the .whl files are written (Linux/WSL path)
#   <revision>            wheel/crate version, e.g. 1.0.0
#
# Environment:
#   DUCKDB_DOWNLOAD_LIB   forwarded to the duckdb build (default: true)
#   SYNCLITE_MANYLINUX    manylinux tag (default: manylinux_2_28)
#   SYNCLITE_WHEEL_TARGETS space-separated rust target triples
#                          (default: "x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu")

set -uo pipefail

PROJECT_DIR="${1:-}"
OUT_DIR="${2:-}"
REVISION="${3:-1.0.0}"

DUCKDB_DOWNLOAD_LIB="${DUCKDB_DOWNLOAD_LIB:-true}"
MANYLINUX="${SYNCLITE_MANYLINUX:-manylinux_2_28}"
TARGETS="${SYNCLITE_WHEEL_TARGETS:-x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu}"

log()  { printf '[linux-wheels] %s\n' "$*"; }
skip() { printf '[linux-wheels] SKIP: %s\n' "$*"; exit 0; }

if [[ -z "$PROJECT_DIR" || -z "$OUT_DIR" ]]; then
    skip "missing arguments (project-dir / out-dir); nothing to build"
fi
if [[ ! -d "$PROJECT_DIR" ]]; then
    skip "python project dir not found at '$PROJECT_DIR'"
fi

# --- toolchain discovery (best-effort; skip cleanly if anything is absent) ---
# Prefer a rustup/cargo env if present so non-login shells still find them.
[[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env" 2>/dev/null || true

need() { command -v "$1" >/dev/null 2>&1; }

for tool in cargo rustup python3 patchelf; do
    need "$tool" || skip "'$tool' not found in the Linux environment. Install the Linux build toolchain (see GETTING_STARTED.md 'Building Linux Python wheels') to enable manylinux wheel builds."
done

# maturin may be a standalone binary or a python module.
MATURIN=()
if need maturin; then
    MATURIN=(maturin)
elif python3 -c 'import maturin' >/dev/null 2>&1; then
    MATURIN=(python3 -m maturin)
else
    skip "maturin not found (pip install maturin). Skipping Linux wheel build."
fi

mkdir -p "$OUT_DIR"

# When the project lives on a Windows drive mount (/mnt/c under WSL), cargo's
# default target directory sits on the 9p/drvfs filesystem, which does not
# support some POSIX operations. Native build scripts such as libsqlite3-sys
# then fail with "Operation not permitted" while copying generated bindings.
# Redirect CARGO_TARGET_DIR to a native Linux path to avoid this. The produced
# wheels are still written to OUT_DIR (which may stay on /mnt).
if [[ -z "${CARGO_TARGET_DIR:-}" && "$PROJECT_DIR" == /mnt/* ]]; then
    CARGO_TARGET_DIR="${TMPDIR:-/tmp}/synclite-wheel-target"
    export CARGO_TARGET_DIR
    log "project is on a Windows mount; redirecting CARGO_TARGET_DIR to '$CARGO_TARGET_DIR' (9p filesystem cannot host cargo build artifacts)"
    mkdir -p "$CARGO_TARGET_DIR"
fi

overall_rc=0
for target in $TARGETS; do
    log "ensuring rust target '$target' is installed"
    if ! rustup target list --installed 2>/dev/null | grep -qx "$target"; then
        rustup target add "$target" || { log "could not add target '$target'; skipping it"; continue; }
    fi

    # Native target builds with the system linker; cross targets use zig.
    host_target="$(rustc -vV 2>/dev/null | sed -n 's/^host: //p')"
    zig_flag=()
    maturin_supports_zig=false
    if "${MATURIN[@]}" build --help 2>/dev/null | grep -q -- '--zig'; then
        maturin_supports_zig=true
    fi
    if $maturin_supports_zig && need zig; then
        # Always build through zig, even for the host architecture. A native
        # build on a modern host links against that host's (new) glibc and then
        # fails manylinux compliance with "too-recent versioned symbols". zig
        # cross-compiles against a pinned, older glibc so the resulting wheel is
        # manylinux_2_28 compliant on every target.
        zig_flag=(--zig)
    elif [[ "$target" != "$host_target" ]]; then
        # No zig: a cross target still needs a matching gcc, otherwise skip it.
        if ! need "${target%%-*}-linux-gnu-gcc"; then
            log "cross target '$target' needs zig or a matching gcc; neither found - skipping it"
            continue
        fi
    else
        # No zig and building the host arch natively: this may produce a wheel
        # that is not manylinux compliant on a modern host, but it is the best
        # we can do without zig or a container.
        log "zig not found; building '$target' with the native toolchain (wheel may not be manylinux compliant on a modern host)"
    fi

    log "building manylinux wheel: target=$target compat=$MANYLINUX"
    # Force a clean per-target link. On an incremental rebuild, cargo re-links
    # the extension against a fingerprint-renamed duckdb dylib
    # (libduckdb-<hash>.so) instead of the plain libduckdb.so that maturin's
    # auditwheel repair knows how to locate, which fails with
    # "libduckdb-<hash>.so could not be located". Removing this target's
    # compiled output, its downloaded duckdb copy, and maturin's staging dir
    # makes the next build download+link libduckdb.so cleanly every time.
    if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
        rm -rf \
            "$CARGO_TARGET_DIR/$target" \
            "$CARGO_TARGET_DIR/duckdb-download/$target" \
            "$CARGO_TARGET_DIR/maturin" 2>/dev/null || true
    fi
    # maturin reads the crate manifest from pyproject.toml's [tool.maturin]
    # manifest-path (resolved relative to the pyproject), so run from
    # PROJECT_DIR. Note: maturin's --manifest-path expects a Cargo.toml, NOT a
    # pyproject.toml, so we must not pass it here. A subshell keeps the cd local.
    (
        cd "$PROJECT_DIR" && \
        DUCKDB_DOWNLOAD_LIB="$DUCKDB_DOWNLOAD_LIB" \
        SYNCLITE_RUST_ARTIFACT_VERSION="$REVISION" \
        "${MATURIN[@]}" build \
            --release \
            --target "$target" \
            "${zig_flag[@]}" \
            --compatibility "$MANYLINUX" \
            --out "$OUT_DIR"
    )
    rc=$?
    if [[ $rc -ne 0 ]]; then
        log "wheel build FAILED for target '$target' (rc=$rc); continuing with remaining targets"
        overall_rc=$rc
    fi
done

if [[ $overall_rc -ne 0 ]]; then
    log "one or more Linux wheel builds failed; see log above. (non-fatal)"
fi
# Always exit 0: this step is best-effort and must never fail the parent build.
exit 0
