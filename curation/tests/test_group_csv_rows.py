from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from curation.group_csv_rows import reshape_csv


class GroupCsvRowsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.workdir = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_groups_first_non_empty_value_from_each_row(self) -> None:
        input_path = self.workdir / "input.csv"
        output_path = self.workdir / "output.csv"
        input_path.write_text(
            (
                "Standard,,,,,,,\n"
                "edge_acs-ed_children-enrolled_public__economic_characteristics_cdp03_2013-2017.gpkg.zip,,,,,,,\n"
                "zip,,,,,,,\n"
                "\"April 3, 2026, 17:43:42 (UTC-05:00)\",,,,,,,\n"
                "194.3 MB,,,,,,,\n"
                "Standard,,,,,,,\n"
                "edge_acs-ed_children-enrolled_public__economic_characteristics_cdp03_2014-2018.gpkg.zip,,,,,,,\n"
                "zip,,,,,,,\n"
                "\"April 3, 2026, 17:57:50 (UTC-05:00)\",,,,,,,\n"
                "191.8 MB,,,,,,,\n"
            ),
            encoding="utf-8",
        )

        row_count = reshape_csv(input_path, output_path, rows_per_record=5)

        self.assertEqual(2, row_count)
        with output_path.open("r", newline="", encoding="utf-8") as infile:
            rows = list(csv.reader(infile))

        self.assertEqual(
            [
                [
                    "Standard",
                    "edge_acs-ed_children-enrolled_public__economic_characteristics_cdp03_2013-2017.gpkg.zip",
                    "zip",
                    "April 3, 2026, 17:43:42 (UTC-05:00)",
                    "194.3 MB",
                ],
                [
                    "Standard",
                    "edge_acs-ed_children-enrolled_public__economic_characteristics_cdp03_2014-2018.gpkg.zip",
                    "zip",
                    "April 3, 2026, 17:57:50 (UTC-05:00)",
                    "191.8 MB",
                ],
            ],
            rows,
        )

    def test_raises_when_final_group_is_incomplete_without_padding(self) -> None:
        input_path = self.workdir / "input.csv"
        output_path = self.workdir / "output.csv"
        input_path.write_text("a\nb\nc\n", encoding="utf-8")

        with self.assertRaises(ValueError):
            reshape_csv(input_path, output_path, rows_per_record=2)

    def test_can_pad_final_group(self) -> None:
        input_path = self.workdir / "input.csv"
        output_path = self.workdir / "output.csv"
        input_path.write_text("a\nb\nc\n", encoding="utf-8")

        row_count = reshape_csv(input_path, output_path, rows_per_record=2, pad_missing=True)

        self.assertEqual(2, row_count)
        with output_path.open("r", newline="", encoding="utf-8") as infile:
            rows = list(csv.reader(infile))

        self.assertEqual([["a", "b"], ["c", ""]], rows)

    def test_can_drop_final_group(self) -> None:
        input_path = self.workdir / "input.csv"
        output_path = self.workdir / "output.csv"
        input_path.write_text("a\nb\nc\n", encoding="utf-8")

        row_count = reshape_csv(input_path, output_path, rows_per_record=2, drop_incomplete=True)

        self.assertEqual(1, row_count)
        with output_path.open("r", newline="", encoding="utf-8") as infile:
            rows = list(csv.reader(infile))

        self.assertEqual([["a", "b"]], rows)


if __name__ == "__main__":
    unittest.main()
