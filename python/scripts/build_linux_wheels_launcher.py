#!/usr/bin/env python3
"""Cross-platform launcher for the Linux manylinux wheel build.

Invoked by the root pom.xml execution ``build-python-wheels-linux`` as:

    <pythonExecutable> scripts/build_linux_wheels_launcher.py <project-dir> <dist-dir> <revision>

Why a launcher: building a PyPI-acceptable ``manylinux`` wheel requires
``patchelf`` (auditwheel), which is POSIX-only and has NO Windows binary.
So the wheel build has to run inside Linux. On a Windows host this launcher
dispatches into WSL; on a Linux host it runs the build script directly.

Contract: this step is BEST-EFFORT. It never raises and always exits 0, so a
missing WSL / missing Linux toolchain only prints a skip notice and lets the
overall Maven build succeed. The Maven execution is additionally configured
with failOnError=false as a second safety net.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys


SCRIPT_NAME = "build_linux_wheels.sh"


def log(msg: str) -> None:
    print(f"[linux-wheels-launcher] {msg}", flush=True)


def _pick_wsl_distro(wsl_exe: str) -> str | None:
    """Return the default/first available WSL distro name, or None."""
    try:
        # -l -q lists installed distro names (UTF-16 on older Windows builds).
        out = subprocess.run(
            [wsl_exe, "-l", "-q"],
            capture_output=True,
            timeout=30,
        )
    except Exception as exc:  # noqa: BLE001 - best-effort probe
        log(f"could not list WSL distros: {exc}")
        return None
    if out.returncode != 0:
        return None
    # `wsl -l -q` output is UTF-16-LE on most Windows builds, UTF-8 on some.
    # Decode as UTF-16-LE first; if that yields replacement junk, fall back to
    # UTF-8. Either way strip ALL NUL/BOM chars (interior included) so the name
    # never carries an embedded null into subprocess args.
    raw = out.stdout.decode("utf-16-le", errors="ignore")
    if "\ufffd" in raw or not raw.strip():
        raw = out.stdout.decode("utf-8", errors="ignore")
    for line in raw.splitlines():
        name = line.replace("\x00", "").replace("\ufeff", "").strip()
        if name:
            return name
    return None


def _run_via_wsl(project_dir: str, dist_dir: str, revision: str) -> None:
    wsl_exe = os.path.join(
        os.environ.get("SystemRoot", r"C:\Windows"), "System32", "wsl.exe"
    )
    if not os.path.exists(wsl_exe):
        log("WSL (wsl.exe) not found; skipping Linux wheel build. "
            "Install WSL + a Linux build toolchain to enable it "
            "(see GETTING_STARTED.md 'Building Linux Python wheels').")
        return

    distro = _pick_wsl_distro(wsl_exe)
    if not distro:
        log("no WSL distribution installed; skipping Linux wheel build. "
            "Run `wsl --install` and set up the Linux build toolchain "
            "(see GETTING_STARTED.md).")
        return

    log(f"dispatching Linux wheel build into WSL distro '{distro}'")
    # Convert the Windows paths to WSL paths inside the shell, then run the
    # committed build script. Trailing args map to $1/$2/$3 after the '_' ($0).
    #
    # The script is piped through `sed 's/\r$//'` (strip trailing CR) before
    # bash so a Windows checkout that re-introduces CRLF line endings (VS Code
    # autosave, git core.autocrlf, etc.) can never break it with the classic
    # `$'\r': command not found` / `syntax error near unexpected token` failures.
    # `bash -s -- "$proj" "$out" "$rev"` feeds the cleaned script on stdin and
    # forwards the positional args.
    remote = (
        'set -u; '
        'proj="$(wslpath -u "$1")"; out="$(wslpath -u "$2")"; rev="$3"; '
        "sed 's/\\r$//' \"$proj/scripts/" + SCRIPT_NAME + '" '
        '| bash -s -- "$proj" "$out" "$rev"'
    )
    cmd = [
        wsl_exe, "-d", distro, "-e", "bash", "-lc", remote,
        "_", project_dir, dist_dir, revision,
    ]
    try:
        subprocess.run(cmd, check=False)
    except Exception as exc:  # noqa: BLE001 - best-effort
        log(f"WSL build invocation failed: {exc} (non-fatal)")


def _run_native(project_dir: str, dist_dir: str, revision: str) -> None:
    script = os.path.join(project_dir, "scripts", SCRIPT_NAME)
    if not os.path.exists(script):
        log(f"build script not found at {script}; skipping")
        return
    log("running Linux wheel build natively (host is Linux)")
    # Strip any trailing CR before bash so a CRLF checkout can't break the
    # script (see _run_via_wsl for the rationale).
    pipeline = (
        "sed 's/\\r$//' " + shlex.quote(script)
        + ' | bash -s -- '
        + ' '.join(shlex.quote(a) for a in (project_dir, dist_dir, revision))
    )
    try:
        subprocess.run(["bash", "-c", pipeline], check=False)
    except FileNotFoundError:
        log("bash not found; skipping Linux wheel build")
    except Exception as exc:  # noqa: BLE001 - best-effort
        log(f"native build invocation failed: {exc} (non-fatal)")


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        log("usage: build_linux_wheels_launcher.py <project-dir> <dist-dir> <revision>")
        return 0  # best-effort: never fail the build

    project_dir, dist_dir, revision = argv[1], argv[2], argv[3]

    try:
        if sys.platform.startswith("win"):
            _run_via_wsl(project_dir, dist_dir, revision)
        elif sys.platform.startswith("linux"):
            _run_native(project_dir, dist_dir, revision)
        else:
            log(f"host platform '{sys.platform}' cannot build Linux wheels; "
                "skipping (use CI or a Linux/WSL host).")
    except Exception as exc:  # noqa: BLE001 - absolute best-effort guard
        log(f"unexpected error: {exc} (non-fatal, continuing build)")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
