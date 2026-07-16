#!/usr/bin/env python3
"""Merge two CSV files with a full outer join and unmatched-row tracking."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from curation.merge_csvs import merge_csvs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Merge two CSVs on a key, keeping matched rows together and labeling unmatched rows with their source."
        )
    )
    parser.add_argument("left_csv", help="Path to the first CSV file.")
    parser.add_argument("right_csv", help="Path to the second CSV file.")
    parser.add_argument("key", help="Column name to match on in the first CSV.")
    parser.add_argument(
        "-o",
        "--output",
        help="Path for the merged CSV. Defaults to <left stem>-<right stem>-merged.csv.",
    )
    parser.add_argument(
        "--right-key",
        help="Column name to match on in the second CSV. Defaults to the same value as key.",
    )
    parser.add_argument(
        "--left-label",
        help="Label used for unmatched rows from the first CSV. Defaults to the first file name.",
    )
    parser.add_argument(
        "--right-label",
        help="Label used for unmatched rows from the second CSV. Defaults to the second file name.",
    )
    parser.add_argument(
        "--status-column",
        default="match_status",
        help="Name of the output column describing whether each row matched. Default: match_status.",
    )
    parser.add_argument(
        "--source-column",
        default="unmatched_source",
        help="Name of the output column naming the CSV for unmatched rows. Default: unmatched_source.",
    )
    parser.add_argument(
        "--ignore-key-case",
        action="store_true",
        help="Match join keys case-insensitively.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    left_path = Path(args.left_csv)
    right_path = Path(args.right_csv)
    output_path = (
        Path(args.output)
        if args.output
        else left_path.with_name(f"{left_path.stem}-{right_path.stem}-merged.csv")
    )

    row_count = merge_csvs(
        left_path,
        right_path,
        output_path,
        left_key=args.key,
        right_key=args.right_key,
        left_label=args.left_label,
        right_label=args.right_label,
        status_column=args.status_column,
        source_column=args.source_column,
        ignore_key_case=args.ignore_key_case,
    )
    print(f"Wrote {row_count} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
