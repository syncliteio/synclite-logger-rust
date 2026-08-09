#!/usr/bin/env python3
"""Best-effort Linux Node package launcher, matching Python's WSL flow."""
from __future__ import annotations

import os
import subprocess
import sys


LINUX_BUILD_COMMAND = r'''
set -u
[[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env" 2>/dev/null || true
export PATH="$HOME/.local/bin:$PATH"

for tool in node npm cargo rustc zig; do
    tool_path="$(command -v "$tool" 2>/dev/null || true)"
    if [[ -z "$tool_path" || "$tool_path" == /mnt/* ]]; then
        printf '[linux-node-launcher] SKIP: native Linux %s not found (resolved path: %s)\n' "$tool" "${tool_path:-missing}"
        exit 0
    fi
done

node_target_base="${CARGO_TARGET_DIR:-${TMPDIR:-/tmp}/synclite-node-target}"
cd "$node_dir"
npm install && \
CARGO_TARGET_DIR="$node_target_base/x86_64" npm run build:linux:x64 && \
npm run pack:offline -- linux-x64-gnu && \
CC_aarch64_unknown_linux_gnu="$HOME/.cache/napi-rs-nodejs/zig-cc-aarch64-unknown-linux-gnu.sh" \
CXX_aarch64_unknown_linux_gnu="$HOME/.cache/napi-rs-nodejs/zig-cxx-aarch64-unknown-linux-gnu.sh" \
CARGO_TARGET_DIR="$node_target_base/aarch64" npm run build:linux:arm64 && \
npm run pack:offline -- linux-arm64-gnu
'''


def log(message: str) -> None:
    print(f"[linux-node-launcher] {message}", flush=True)


def distro_name(wsl: str) -> str | None:
    try:
        result = subprocess.run([wsl, "-l", "-q"], capture_output=True, timeout=30)
    except Exception as exc:  # noqa: BLE001 - best-effort probe
        log(f"could not list WSL distributions: {exc}")
        return None
    if result.returncode != 0:
        return None
    text = result.stdout.decode("utf-16-le", errors="ignore")
    if not text.strip() or "\ufffd" in text:
        text = result.stdout.decode("utf-8", errors="ignore")
    for line in text.splitlines():
        name = line.replace("\x00", "").replace("\ufeff", "").strip()
        if name:
            return name
    return None


def run_linux(node_dir: str) -> None:
    command = 'node_dir="$1"; ' + LINUX_BUILD_COMMAND
    try:
        result = subprocess.run(["bash", "-lc", command, "_", node_dir], check=False)
    except Exception as exc:  # noqa: BLE001 - best-effort build
        log(f"native Linux Node package invocation failed: {exc} (non-fatal)")
        return
    if result.returncode != 0:
        log("Linux Node package build failed; see the Linux output above (non-fatal)")


def run_wsl(node_dir: str) -> None:
    wsl = os.path.join(
        os.environ.get("SystemRoot", r"C:\Windows"), "System32", "wsl.exe"
    )
    if not os.path.exists(wsl):
        log("WSL not found; skipping Linux Node packages")
        return
    distro = distro_name(wsl)
    if not distro:
        log("no WSL distribution found; skipping Linux Node packages")
        return

    log(f"dispatching Linux Node package build into WSL distro '{distro}'")
    remote = 'node_dir="$(wslpath -u "$1")"; ' + LINUX_BUILD_COMMAND
    try:
        result = subprocess.run(
            [wsl, "-d", distro, "-e", "bash", "-lc", remote, "_", node_dir],
            check=False,
        )
    except Exception as exc:  # noqa: BLE001 - best-effort build
        log(f"WSL Linux Node package invocation failed: {exc} (non-fatal)")
        return
    if result.returncode != 0:
        log("WSL Linux Node package build failed; see the WSL output above (non-fatal)")


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        log("usage: build_linux_packages_launcher.py <node-package-directory>")
        return 0

    node_dir = os.path.abspath(argv[1])
    if sys.platform.startswith("win"):
        run_wsl(node_dir)
    elif sys.platform.startswith("linux"):
        run_linux(node_dir)
    else:
        log(f"host platform {sys.platform} cannot build Linux packages; use Linux/WSL")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
