from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from curation.zip_geopackages import zip_geopackages


class ZipGeopackagesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.workdir = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_creates_one_zip_per_geopackage(self) -> None:
        first = self.workdir / "one.gpkg"
        second = self.workdir / "two.gpkg"
        first.write_bytes(b"first geopackage payload")
        second.write_bytes(b"second geopackage payload")
        (self.workdir / "ignore.txt").write_text("not a geopackage", encoding="utf-8")

        summary = zip_geopackages(self.workdir)

        created_names = sorted(path.name for path in summary.created_archives)
        self.assertEqual(["one.zip", "two.zip"], created_names)
        self.assertEqual([], summary.skipped_archives)

        with ZipFile(self.workdir / "one.zip") as archive:
            self.assertEqual(["one.gpkg"], archive.namelist())
            self.assertEqual(b"first geopackage payload", archive.read("one.gpkg"))

    def test_skips_existing_zip_without_overwrite(self) -> None:
        gpkg_path = self.workdir / "dataset.gpkg"
        gpkg_path.write_bytes(b"new content")
        archive_path = self.workdir / "dataset.zip"

        with ZipFile(archive_path, mode="w") as archive:
            archive.writestr("dataset.gpkg", b"old content")

        summary = zip_geopackages(self.workdir, overwrite=False)

        self.assertEqual([], summary.created_archives)
        self.assertEqual([archive_path], summary.skipped_archives)

        with ZipFile(archive_path) as archive:
            self.assertEqual(b"old content", archive.read("dataset.gpkg"))

    def test_can_write_to_separate_output_dir_and_delete_originals(self) -> None:
        nested_dir = self.workdir / "nested"
        nested_dir.mkdir()
        gpkg_path = nested_dir / "dataset.gpkg"
        gpkg_path.write_bytes(b"payload")
        output_dir = self.workdir / "archives"

        summary = zip_geopackages(
            self.workdir,
            output_dir=output_dir,
            recursive=True,
            delete_original=True,
        )

        expected_archive = output_dir / "nested" / "dataset.zip"
        self.assertEqual([expected_archive], summary.created_archives)
        self.assertFalse(gpkg_path.exists())

        with ZipFile(expected_archive) as archive:
            self.assertEqual(["dataset.gpkg"], archive.namelist())
            self.assertEqual(b"payload", archive.read("dataset.gpkg"))


if __name__ == "__main__":
    unittest.main()
