from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from curation.merge_csvs import merge_csvs


class MergeCsvsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.workdir = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_merges_rows_and_marks_unmatched_sources(self) -> None:
        left_path = self.workdir / "people.csv"
        right_path = self.workdir / "scores.csv"
        output_path = self.workdir / "merged.csv"

        left_path.write_text(
            "id,name,team\n1,Ada,blue\n2,Ben,green\n",
            encoding="utf-8",
        )
        right_path.write_text(
            "id,score\n1,95\n3,88\n",
            encoding="utf-8",
        )

        row_count = merge_csvs(
            left_path,
            right_path,
            output_path,
            left_key="id",
            left_label="people",
            right_label="scores",
        )

        self.assertEqual(3, row_count)
        with output_path.open("r", newline="", encoding="utf-8") as infile:
            rows = list(csv.DictReader(infile))

        self.assertEqual(
            [
                {
                    "id": "1",
                    "name": "Ada",
                    "team": "blue",
                    "score": "95",
                    "match_status": "matched",
                    "unmatched_source": "",
                },
                {
                    "id": "2",
                    "name": "Ben",
                    "team": "green",
                    "score": "",
                    "match_status": "unmatched",
                    "unmatched_source": "people",
                },
                {
                    "id": "3",
                    "name": "",
                    "team": "",
                    "score": "88",
                    "match_status": "unmatched",
                    "unmatched_source": "scores",
                },
            ],
            rows,
        )

    def test_suffixes_overlapping_non_key_columns(self) -> None:
        left_path = self.workdir / "left.csv"
        right_path = self.workdir / "right.csv"
        output_path = self.workdir / "merged.csv"

        left_path.write_text(
            "id,name,city\n1,Ada,Chicago\n",
            encoding="utf-8",
        )
        right_path.write_text(
            "id,name,state\n1,A.,Illinois\n",
            encoding="utf-8",
        )

        merge_csvs(
            left_path,
            right_path,
            output_path,
            left_key="id",
            left_label="left file",
            right_label="right-file",
        )

        with output_path.open("r", newline="", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            self.assertEqual(
                [
                    "id",
                    "name_left_file",
                    "city",
                    "name_right_file",
                    "state",
                    "match_status",
                ],
                reader.fieldnames,
            )
            rows = list(reader)

        self.assertEqual("Ada", rows[0]["name_left_file"])
        self.assertEqual("A.", rows[0]["name_right_file"])

    def test_supports_different_key_names(self) -> None:
        left_path = self.workdir / "left.csv"
        right_path = self.workdir / "right.csv"
        output_path = self.workdir / "merged.csv"

        left_path.write_text(
            "person_id,name\n1,Ada\n",
            encoding="utf-8",
        )
        right_path.write_text(
            "user_id,score\n1,95\n",
            encoding="utf-8",
        )

        merge_csvs(
            left_path,
            right_path,
            output_path,
            left_key="person_id",
            right_key="user_id",
        )

        with output_path.open("r", newline="", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            self.assertEqual(
                ["person_id", "name", "user_id", "score", "match_status"],
                reader.fieldnames,
            )
            rows = list(reader)

        self.assertEqual("1", rows[0]["person_id"])
        self.assertEqual("1", rows[0]["user_id"])

    def test_can_match_keys_ignoring_case(self) -> None:
        left_path = self.workdir / "left.csv"
        right_path = self.workdir / "right.csv"
        output_path = self.workdir / "merged.csv"

        left_path.write_text(
            "id,name\nABC,Ada\n",
            encoding="utf-8",
        )
        right_path.write_text(
            "id,score\nabc,95\n",
            encoding="utf-8",
        )

        merge_csvs(
            left_path,
            right_path,
            output_path,
            left_key="id",
            ignore_key_case=True,
        )

        with output_path.open("r", newline="", encoding="utf-8") as infile:
            rows = list(csv.DictReader(infile))

        self.assertEqual(1, len(rows))
        self.assertEqual("matched", rows[0]["match_status"])
        self.assertEqual("ABC", rows[0]["id"])
        self.assertEqual("95", rows[0]["score"])

    def test_omits_columns_that_are_blank_in_every_output_row(self) -> None:
        left_path = self.workdir / "left.csv"
        right_path = self.workdir / "right.csv"
        output_path = self.workdir / "merged.csv"

        left_path.write_text(
            "id,name,empty_left\n1,Ada,\n",
            encoding="utf-8",
        )
        right_path.write_text(
            "id,score,empty_right\n1,95,\n",
            encoding="utf-8",
        )

        merge_csvs(
            left_path,
            right_path,
            output_path,
            left_key="id",
        )

        with output_path.open("r", newline="", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            self.assertEqual(["id", "name", "score", "match_status"], reader.fieldnames)
            rows = list(reader)

        self.assertEqual(
            [{"id": "1", "name": "Ada", "score": "95", "match_status": "matched"}],
            rows,
        )


if __name__ == "__main__":
    unittest.main()
