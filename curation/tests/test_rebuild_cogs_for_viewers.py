from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from curation.rebuild_cogs_for_viewers import (  # noqa: E402
    alpha_band_selector,
    cog_creation_options,
    find_source_member,
    item_id_from_cog,
    output_band_arguments,
    reported_interleave,
    target_grid,
    validate_output,
)


class RebuildCogsForViewersTests(unittest.TestCase):
    def test_derives_item_id_from_cog(self) -> None:
        path = Path("mdu-057202-0001-croputm29_cog.tif")

        self.assertEqual("mdu-057202", item_id_from_cog(path))

    def test_finds_exact_source_member(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            zip_path = Path(tempdir) / "mdu-057202.zip"
            with ZipFile(zip_path, "w") as archive:
                archive.writestr("mdu-057202/mdu-057202-0001.tif", b"full")
                archive.writestr("mdu-057202/mdu-057202-0001-croputm29.tif", b"crop")

            member = find_source_member(zip_path, "mdu-057202-0001-croputm29")

        self.assertEqual("mdu-057202/mdu-057202-0001-croputm29.tif", member)

    def test_uses_explicit_pixel_interleaving(self) -> None:
        options = cog_creation_options("JPEG", 90)

        self.assertIn("INTERLEAVE=PIXEL", options)
        self.assertNotIn("INTERLEAVE=BAND", options)

    def test_legacy_options_use_pixel_interleaved_deflate(self) -> None:
        options = cog_creation_options("DEFLATE", 90)

        self.assertIn("INTERLEAVE=PIXEL", options)
        self.assertIn("COMPRESS=DEFLATE", options)
        self.assertNotIn("COMPRESS=JPEG", options)

    def test_internal_mask_selects_rgb_and_uses_alpha_as_mask(self) -> None:
        self.assertEqual(
            ["-b", "1", "-b", "2", "-b", "3", "-mask", "4"],
            output_band_arguments("mask"),
        )

    def test_explicit_alpha_keeps_all_rgba_bands(self) -> None:
        self.assertEqual([], output_band_arguments("band"))

    def test_treats_omitted_cog_interleave_as_pixel(self) -> None:
        metadata = {"metadata": {"IMAGE_STRUCTURE": {"LAYOUT": "COG"}}}

        self.assertEqual("PIXEL", reported_interleave(metadata))

    def test_selects_alpha_band_or_dataset_mask(self) -> None:
        rgba = {
            "bands": [
                {"band": 1, "colorInterpretation": "Red"},
                {"band": 4, "colorInterpretation": "Alpha"},
            ]
        }
        masked_rgb = {
            "bands": [
                {"band": 1, "mask": {"flags": ["PER_DATASET"]}},
            ]
        }

        self.assertEqual("4", alpha_band_selector(rgba))
        self.assertEqual("mask,1", alpha_band_selector(masked_rgb))

    def test_calculates_exact_grid_bounds(self) -> None:
        metadata = {
            "size": [10, 5],
            "geoTransform": [100.0, 2.0, 0.0, 200.0, 0.0, -3.0],
        }

        self.assertEqual((10, 5, (100.0, 185.0, 120.0, 200.0)), target_grid(metadata))

    def test_accepts_pixel_rgb_cog_with_internal_mask(self) -> None:
        input_metadata = {
            "size": [10, 5],
            "geoTransform": [100.0, 2.0, 0.0, 200.0, 0.0, -3.0],
            "stac": {"proj:epsg": 3857},
        }
        output_metadata = {
            **input_metadata,
            "metadata": {"IMAGE_STRUCTURE": {"LAYOUT": "COG", "INTERLEAVE": "PIXEL"}},
            "bands": [
                {
                    "band": 1,
                    "colorInterpretation": "Red",
                    "mask": {"flags": ["PER_DATASET"]},
                    "overviews": [{"size": [5, 2]}],
                },
                {"band": 2, "colorInterpretation": "Green"},
                {"band": 3, "colorInterpretation": "Blue"},
            ],
        }

        self.assertEqual([], validate_output(input_metadata, output_metadata, "mask"))

    def test_accepts_pixel_rgba_cog_for_legacy_viewers(self) -> None:
        input_metadata = {
            "size": [10, 5],
            "geoTransform": [100.0, 2.0, 0.0, 200.0, 0.0, -3.0],
            "stac": {"proj:epsg": 3857},
        }
        output_metadata = {
            **input_metadata,
            "metadata": {
                "IMAGE_STRUCTURE": {
                    "LAYOUT": "COG",
                    "INTERLEAVE": "PIXEL",
                    "COMPRESSION": "DEFLATE",
                }
            },
            "bands": [
                {
                    "band": 1,
                    "colorInterpretation": "Red",
                    "overviews": [{"size": [5, 2]}],
                },
                {"band": 2, "colorInterpretation": "Green"},
                {"band": 3, "colorInterpretation": "Blue"},
                {"band": 4, "colorInterpretation": "Alpha"},
            ],
        }

        self.assertEqual(
            [], validate_output(input_metadata, output_metadata, "band")
        )


if __name__ == "__main__":
    unittest.main()
