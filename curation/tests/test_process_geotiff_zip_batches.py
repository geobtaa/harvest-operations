from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from curation.process_geotiff_zip_batches import (
    discover_existing_outputs,
    find_crop_geotiff_members,
    filename_from_content_disposition,
    item_id_from_remote_filename,
    matching_lightweight_members,
    read_manifest,
    safe_zip_members,
)


class ProcessGeotiffZipBatchesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.workdir = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_reads_manifest_rows(self) -> None:
        manifest = self.workdir / "manifest.csv"
        with manifest.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=["item_id", "zip_url"])
            writer.writeheader()
            writer.writerow({"item_id": "mdu-057047", "zip_url": "https://example.test/one.zip"})

        items = read_manifest(manifest)

        self.assertEqual(1, len(items))
        self.assertEqual("mdu-057047", items[0].item_id)
        self.assertEqual("https://example.test/one.zip", items[0].source_url)

    def test_reads_headerless_url_manifest(self) -> None:
        manifest = self.workdir / "urls.csv"
        manifest.write_text(
            "https://example.test/one\nhttps://example.test/two\n",
            encoding="utf-8",
        )

        items = read_manifest(manifest)

        self.assertEqual(["row-0001", "row-0002"], [item.item_id for item in items])
        self.assertEqual(
            ["https://example.test/one", "https://example.test/two"],
            [item.source_url for item in items],
        )

    def test_finds_crop_geotiff_and_ignores_other_large_assets(self) -> None:
        members = [
            "mdu-057047/mdu-057047-0001.tif",
            "mdu-057047/mdu-057047-0001-geoutm31.tif",
            "mdu-057047/mdu-057047-0001-croputm31.tif",
            "mdu-057047/mdu-057047-0001-croputm31.tif.enp",
            "mdu-057047/mdu-057047-0001.jpg",
            "mdu-057047/mdu-057047-0001.pdf",
        ]

        self.assertEqual(
            ["mdu-057047/mdu-057047-0001-croputm31.tif"],
            find_crop_geotiff_members(members),
        )

    def test_selects_crop_world_file_and_lightweight_metadata(self) -> None:
        members = [
            "mdu-057047/mdu-057047-0001-croputm31.tif",
            "mdu-057047/mdu-057047-0001-croputm31.tfw",
            "mdu-057047/mdu-057047-0001-geoutm31.tfw",
            "mdu-057047/mdu-057047-0001.txt",
            "mdu-057047/mdu-057047-geoblacklight.json",
            "mdu-057047/unrelated.json",
        ]

        selected = matching_lightweight_members(
            members,
            "mdu-057047/mdu-057047-0001-croputm31.tif",
        )

        self.assertEqual(
            [
                "mdu-057047/mdu-057047-0001-croputm31.tfw",
                "mdu-057047/mdu-057047-0001-croputm31.tif",
                "mdu-057047/mdu-057047-0001.txt",
                "mdu-057047/mdu-057047-geoblacklight.json",
            ],
            selected,
        )

    def test_safe_zip_members_excludes_absolute_and_parent_paths(self) -> None:
        archive_path = self.workdir / "item.zip"
        with ZipFile(archive_path, "w") as archive:
            archive.writestr("safe/file.tif", b"ok")
            archive.writestr("../bad.tif", b"bad")
            archive.writestr("/absolute/bad.tif", b"bad")

        with ZipFile(archive_path) as archive:
            self.assertEqual(["safe/file.tif"], safe_zip_members(archive))

    def test_discovers_existing_cog_and_thumbnail_for_resume(self) -> None:
        cog_dir = self.workdir / "cogs"
        thumbnail_dir = self.workdir / "thumbs"
        cog_dir.mkdir()
        thumbnail_dir.mkdir()
        cog = cog_dir / "mdu-057047-0001-croputm31_cog.tif"
        thumb = thumbnail_dir / "mdu-057047-0001-croputm31_cog.jpg"
        cog.write_bytes(b"cog")
        thumb.write_bytes(b"thumb")

        found_cog, found_thumbnail = discover_existing_outputs(
            "mdu-057047",
            cog_dir,
            thumbnail_dir,
        )

        self.assertEqual(cog, found_cog)
        self.assertEqual(thumb, found_thumbnail)

    def test_derives_item_id_from_content_disposition_filename(self) -> None:
        filename = filename_from_content_disposition(
            'attachment; filename="mdu-057205.zip"; size=373004888'
        )

        self.assertEqual("mdu-057205.zip", filename)
        self.assertEqual("mdu-057205", item_id_from_remote_filename(filename))


if __name__ == "__main__":
    unittest.main()
