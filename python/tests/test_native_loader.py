import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from synclite._native_loader import discover_native_dll_directories
from repair_wheel import _bundle_native_dlls_in_wheel, _native_dependency_dirs


class NativeLoaderTests(unittest.TestCase):
    def test_discovers_package_local_dll_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            package_dir = Path(tmp) / "synclite"
            package_dir.mkdir(parents=True)
            libs_dir = package_dir / "synclite.libs"
            libs_dir.mkdir()
            (libs_dir / "duckdb.dll").write_text("fake", encoding="utf-8")

            found = discover_native_dll_directories(package_dir)

            self.assertEqual(found, [libs_dir])

    def test_discovers_nested_dll_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            package_dir = Path(tmp) / "synclite"
            package_dir.mkdir(parents=True)
            nested_dir = package_dir / "synclite.libs" / "bin"
            nested_dir.mkdir(parents=True)
            (nested_dir / "libssl.dll").write_text("fake", encoding="utf-8")

            found = discover_native_dll_directories(package_dir)

            self.assertEqual(found, [nested_dir])


class WheelRepairTests(unittest.TestCase):
    def test_keeps_legacy_deps_without_cargo_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            deps_dir = Path(tmp) / "release" / "deps"
            self.assertEqual(_native_dependency_dirs(str(deps_dir)), [deps_dir])

    def test_discovers_and_bundles_cargo_native_link_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            deps_dir = root / "target" / "release" / "deps"
            deps_dir.mkdir(parents=True)
            native_dir = root / "target" / "duckdb-download" / "native target" / "v2.0.0"
            native_dir.mkdir(parents=True)
            (native_dir / "duckdb.dll").write_bytes(b"test DuckDB DLL")
            output = deps_dir.parent / "build" / "libduckdb-sys-test" / "output"
            output.parent.mkdir(parents=True)
            output.write_text(
                f"cargo:rustc-link-search=native={native_dir}\n"
                f"cargo::rustc-link-search=native={native_dir}\n"
                f"cargo:rustc-link-search=native={root / 'missing'}\n"
                f"cargo:rustc-link-search=framework={root}\n",
                encoding="utf-8",
            )

            self.assertEqual(_native_dependency_dirs(str(deps_dir)), [deps_dir, native_dir])

            wheel = root / "synclite-1.2.0-py3-none-any.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("synclite/__init__.py", "")
            repaired = _bundle_native_dlls_in_wheel(str(wheel), str(deps_dir))
            with zipfile.ZipFile(repaired) as archive:
                self.assertEqual(
                    archive.read("synclite/synclite.libs/duckdb.dll"), b"test DuckDB DLL"
                )


if __name__ == "__main__":
    unittest.main()
