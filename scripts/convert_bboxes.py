#!/usr/bin/env python3
"""
Convert MARC-style Bounding Box values to decimal degrees in W,S,E,N order.

The script handles the DMS-style formats observed in Chicago Luna exports, such as:
    W 96deg56'00"-W 89deg42'00"/N 16deg20'00"-N 12deg13'00"
    E 01deg00'--E 05deg30'/N 33deg00'--N 26deg00'
    W 88??00??--W 87??15??/N 42??00??--N 41??30??

Already-decimal bounding boxes are passed through unchanged. Non-coordinate values
like "2 maps" are also preserved unchanged.
"""

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.marc_coordinates import is_decimal_bbox, marc_bbox_to_decimal  # noqa: E402


DEFAULT_COLUMN = "Bounding Box"
DEFAULT_SUFFIX = "_decimal_bboxes"


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Convert a CSV Bounding Box column from DMS-like text to decimal degrees "
            "in west,south,east,north order."
        )
    )
    parser.add_argument("input_csv", type=Path, help="Path to the source CSV.")
    parser.add_argument(
        "-o",
        "--output-csv",
        type=Path,
        help=(
            "Path to the output CSV. Defaults to <input>_decimal_bboxes.csv in the "
            "same directory."
        ),
    )
    parser.add_argument(
        "--column",
        default=DEFAULT_COLUMN,
        help=f"Column to convert. Defaults to {DEFAULT_COLUMN!r}.",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite the input file instead of creating a separate output CSV.",
    )
    return parser.parse_args()


def normalize_column_name(value):
    return str(value or "").strip().casefold()


def resolve_column(fieldnames, requested_name):
    if requested_name in fieldnames:
        return requested_name

    normalized_requested = normalize_column_name(requested_name)
    for fieldname in fieldnames:
        if normalize_column_name(fieldname) == normalized_requested:
            return fieldname

    available = ", ".join(fieldnames)
    raise ValueError(f"Column {requested_name!r} was not found. Available columns: {available}")


def default_output_path(input_path):
    return input_path.with_name(f"{input_path.stem}{DEFAULT_SUFFIX}{input_path.suffix}")


def convert_bbox_value(value):
    raw_value = str(value or "").strip()
    if not raw_value:
        return "", "empty"

    if is_decimal_bbox(raw_value):
        return raw_value, "decimal"

    converted = marc_bbox_to_decimal(raw_value)
    if not converted:
        return raw_value, "preserved"
    return converted, "converted"


def process_csv(input_csv, output_csv, requested_column):
    with input_csv.open("r", encoding="utf-8-sig", errors="replace", newline="") as in_handle:
        reader = csv.DictReader(in_handle)
        if not reader.fieldnames:
            raise ValueError(f"{input_csv} is missing a header row.")

        fieldnames = list(reader.fieldnames)
        bbox_column = resolve_column(fieldnames, requested_column)
        counts = Counter()
        unparsed_samples = []

        with output_csv.open("w", encoding="utf-8", newline="") as out_handle:
            writer = csv.DictWriter(out_handle, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                counts["rows"] += 1
                original_value = row.get(bbox_column, "")

                try:
                    converted_value, status = convert_bbox_value(original_value)
                except ValueError:
                    converted_value = original_value
                    status = "preserved"

                row[bbox_column] = converted_value
                writer.writerow(row)
                counts[status] += 1

                if status == "preserved" and str(original_value or "").strip():
                    if len(unparsed_samples) < 10 and original_value not in unparsed_samples:
                        unparsed_samples.append(original_value)

    print(f"Input CSV: {input_csv}")
    print(f"Output CSV: {output_csv}")
    print(f"Bounding Box column: {bbox_column}")
    print(f"Rows processed: {counts['rows']}")
    print(f"Converted from DMS: {counts['converted']}")
    print(f"Already decimal: {counts['decimal']}")
    print(f"Empty values: {counts['empty']}")
    print(f"Preserved as-is: {counts['preserved']}")

    if unparsed_samples:
        print("Sample preserved values:")
        for sample in unparsed_samples:
            print(f"  - {sample}")


def main():
    args = parse_args()

    if args.output_csv and args.in_place:
        raise ValueError("Use either --output-csv or --in-place, not both.")

    output_csv = args.input_csv if args.in_place else (args.output_csv or default_output_path(args.input_csv))
    process_csv(args.input_csv, output_csv, args.column)


if __name__ == "__main__":
    main()
