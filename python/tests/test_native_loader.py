import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from synclite._native_loader import discover_native_dll_directories


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


if __name__ == "__main__":
    unittest.main()
