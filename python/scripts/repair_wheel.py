"""Post-build wheel-repair for the `synclite` PyO3 wheel.

Maturin produces a wheel that statically imports duckdb.dll (and the
platform-equivalent .so / .dylib). Stock pip + a plain `pip install
synclite-*.whl` on the end user's box has no way to find that DLL, so the
import fails with `ImportError: DLL load failed while importing _native`.

This script repairs the wheel in-place so it is self-sufficient:
  - Windows: delvewheel (bundles DLLs into a synclite.libs/ subfolder and
    patches synclite/__init__.py to add it to the search path).
  - Linux:   auditwheel  (patches RPATH and bundles .so files).
  - macOS:   delocate    (bundles .dylibs and rewrites install names).

Usage (invoked by the root pom.xml `repair-python-wheel` execution):
    python scripts/repair_wheel.py <dist-dir> <rust-target-release-deps-dir>

The repaired wheel replaces the unpatched one in <dist-dir>. If the
platform-specific repair tool is not installed the script prints a
warning and exits 0 so the rest of the build still succeeds.
"""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import zipfile
from pathlib import Path


def _run(cmd: list[str]) -> int:
    print("[repair_wheel] $ " + " ".join(cmd), flush=True)
    return subprocess.call(cmd)


def _tool_available(module: str) -> bool:
    try:
        __import__(module)
        return True
    except ImportError:
        return False


def _bundle_native_dlls_in_wheel(wheel: str, deps_dir: str) -> str:
    with tempfile.TemporaryDirectory(prefix="synclite-wheel-stage-") as stage_dir:
        stage_path = Path(stage_dir)
        with zipfile.ZipFile(wheel, "r") as wheel_zip:
            wheel_zip.extractall(stage_path)

        package_dir = None
        for init_file in stage_path.rglob("__init__.py"):
            if init_file.parent.name == "synclite":
                package_dir = init_file.parent
                break
        if package_dir is None:
            raise RuntimeError("unable to locate synclite package inside wheel")

        libs_dir = package_dir / "synclite.libs"
        libs_dir.mkdir(parents=True, exist_ok=True)

        dll_patterns = ["*.dll", "*.pyd", "*.so", "*.dylib"]
        copied = 0
        for pattern in dll_patterns:
            for source in sorted(Path(deps_dir).rglob(pattern)):
                if source.is_file():
                    target = libs_dir / source.name
                    shutil.copy2(source, target)
                    copied += 1

        if copied == 0:
            raise RuntimeError(f"no native libraries found in {deps_dir}")

        repaired_wheel = str(Path(wheel).with_name(f"{Path(wheel).stem}-repaired.whl"))
        with zipfile.ZipFile(repaired_wheel, "w", compression=zipfile.ZIP_DEFLATED) as output_zip:
            for item in sorted(stage_path.rglob("*")):
                if item.is_file():
                    output_zip.write(item, item.relative_to(stage_path))
        return repaired_wheel


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    dist_dir = os.path.abspath(argv[1])
    deps_dir = os.path.abspath(argv[2])

    wheels = sorted(glob.glob(os.path.join(dist_dir, "synclite-*.whl")))
    if not wheels:
        print(f"[repair_wheel] no wheel found in {dist_dir}; skipping")
        return 0
    wheel = wheels[-1]  # newest if multiple
    print(f"[repair_wheel] input wheel : {wheel}")
    print(f"[repair_wheel] native deps : {deps_dir}")

    platform = sysconfig.get_platform()
    print(f"[repair_wheel] platform    : {platform}")

    with tempfile.TemporaryDirectory(prefix="synclite-wheel-repair-") as out_dir:
        if sys.platform.startswith("win"):
            if not _tool_available("delvewheel"):
                print(
                    "[repair_wheel] WARNING: delvewheel not installed; bundling "
                    "native DLLs manually into the wheel instead."
                )
                try:
                    repaired_wheel = _bundle_native_dlls_in_wheel(wheel, deps_dir)
                except Exception as exc:  # pragma: no cover - defensive logging
                    print(f"[repair_wheel] manual bundling failed: {exc}", file=sys.stderr)
                    return 1
                os.remove(wheel)
                shutil.move(repaired_wheel, os.path.join(dist_dir, os.path.basename(repaired_wheel)))
                print(f"[repair_wheel] output wheel: {os.path.join(dist_dir, os.path.basename(repaired_wheel))}")
                return 0
            rc = _run([
                sys.executable, "-m", "delvewheel", "repair",
                "--add-path", deps_dir,
                "--wheel-dir", out_dir,
                wheel,
            ])
        elif sys.platform == "darwin":
            if not _tool_available("delocate"):
                print(
                    "[repair_wheel] WARNING: delocate not installed; wheel will "
                    "NOT bundle libduckdb.dylib. Run `pip install delocate` on "
                    "the build host to ship a self-sufficient wheel."
                )
                return 0
            env = os.environ.copy()
            env["DYLD_LIBRARY_PATH"] = (
                deps_dir + os.pathsep + env.get("DYLD_LIBRARY_PATH", "")
            )
            cmd = [
                sys.executable, "-m", "delocate.cmd.delocate_wheel",
                "--wheel-dir", out_dir,
                wheel,
            ]
            print("[repair_wheel] $ " + " ".join(cmd), flush=True)
            rc = subprocess.call(cmd, env=env)
        else:
            if not _tool_available("auditwheel"):
                print(
                    "[repair_wheel] WARNING: auditwheel not installed; wheel will "
                    "NOT bundle libduckdb.so. Run `pip install auditwheel` on the "
                    "build host to ship a self-sufficient wheel."
                )
                return 0
            env = os.environ.copy()
            env["LD_LIBRARY_PATH"] = (
                deps_dir + os.pathsep + env.get("LD_LIBRARY_PATH", "")
            )
            cmd = [
                sys.executable, "-m", "auditwheel", "repair",
                "--wheel-dir", out_dir,
                wheel,
            ]
            print("[repair_wheel] $ " + " ".join(cmd), flush=True)
            rc = subprocess.call(cmd, env=env)

        if rc != 0:
            print(f"[repair_wheel] repair tool exited {rc}", file=sys.stderr)
            return rc

        repaired = sorted(glob.glob(os.path.join(out_dir, "synclite-*.whl")))
        if not repaired:
            print(
                f"[repair_wheel] ERROR: repair tool produced no wheel in {out_dir}",
                file=sys.stderr,
            )
            return 1

        # Replace the unpatched wheel in dist/ with the repaired one.
        os.remove(wheel)
        dest = os.path.join(dist_dir, os.path.basename(repaired[0]))
        shutil.move(repaired[0], dest)
        print(f"[repair_wheel] output wheel: {dest}")
        return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
