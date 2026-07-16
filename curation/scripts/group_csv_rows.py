#!/usr/bin/env python3
"""Combine every N source rows from a CSV into one output row."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from curation.group_csv_rows import reshape_csv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read one value from each CSV row and combine every N rows into a single output row."
        )
    )
    parser.add_argument("input_csv", help="Path to the input CSV file.")
    parser.add_argument(
        "-o",
        "--output",
        help="Path for the reshaped CSV. Defaults to <input stem>-grouped.csv.",
    )
    parser.add_argument(
        "-n",
        "--rows-per-record",
        type=int,
        default=5,
        help="Number of source rows to combine into one output row. Default: 5.",
    )
    parser.add_argument(
        "-c",
        "--column",
        type=int,
        help=(
            "Zero-based source column to read from each row. "
            "Default: use the first non-empty cell in each row."
        ),
    )
    parser.add_argument(
        "--pad-missing",
        action="store_true",
        help="Pad the final row with blanks if the input does not divide evenly.",
    )
    parser.add_argument(
        "--drop-incomplete",
        action="store_true",
        help="Discard a trailing partial row if the input does not divide evenly.",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite the input file instead of creating a new file.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    input_path = Path(args.input_csv)

    if args.in_place and args.output:
        parser.error("use either --in-place or --output, not both")
    if args.pad_missing and args.drop_incomplete:
        parser.error("use either --pad-missing or --drop-incomplete, not both")

    if args.in_place:
        output_path = input_path
    elif args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.with_name(f"{input_path.stem}-grouped{input_path.suffix}")

    record_count = reshape_csv(
        input_path,
        output_path,
        rows_per_record=args.rows_per_record,
        column=args.column,
        pad_missing=args.pad_missing,
        drop_incomplete=args.drop_incomplete,
    )
    print(f"Wrote {record_count} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
