"""Helpers for making the PyO3 extension resilient to missing native DLLs."""

from __future__ import annotations

import os
from pathlib import Path


def discover_native_dll_directories(package_dir: str | os.PathLike[str] | Path) -> list[Path]:
    """Return package-local directories that may contain native libraries.

    The layout is intentionally permissive so the same logic works both for
    installed wheels (which may carry DLLs under ``synclite.libs``) and for
    editable installs built from a source checkout.
    """

    package_path = Path(package_dir).resolve()
    candidates = [
        package_path / "synclite.libs",
        package_path / "libs",
        package_path.parent / "synclite.libs",
        package_path.parent / "libs",
    ]

    discovered: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate.exists():
            continue
        for current_root, _, files in os.walk(candidate):
            current_path = Path(current_root)
            if any(
                file_name.lower().endswith((".dll", ".pyd", ".so", ".dylib"))
                for file_name in files
            ):
                path_text = str(current_path)
                if path_text not in seen:
                    discovered.append(current_path)
                    seen.add(path_text)
    return discovered


def ensure_native_search_path(package_dir: str | os.PathLike[str] | Path) -> None:
    """Add package-local native library directories to the Windows DLL search path."""

    if os.name != "nt":
        return

    for dll_directory in discover_native_dll_directories(package_dir):
        try:
            os.add_dll_directory(str(dll_directory))
        except (OSError, RuntimeError):
            continue
