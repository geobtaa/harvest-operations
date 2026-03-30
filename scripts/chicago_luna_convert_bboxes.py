#!/usr/bin/env python3
"""
Convert Chicago Luna Bounding Box values to decimal degrees in W,S,E,N order.

The script handles the DMS-style formats observed in Chicago Luna exports, such as:
    W 96deg56'00"-W 89deg42'00"/N 16deg20'00"-N 12deg13'00"
    E 01deg00'--E 05deg30'/N 33deg00'--N 26deg00'
    W 88??00??--W 87??15??/N 42??00??--N 41??30??

Already-decimal bounding boxes are passed through unchanged. Non-coordinate values
like "2 maps" are also preserved unchanged.
"""

import argparse
import csv
import re
from collections import Counter
from pathlib import Path


DEFAULT_COLUMN = "Bounding Box"
DEFAULT_SUFFIX = "_decimal_bboxes"
COORDINATE_PATTERN = re.compile(
    r"([NSEW])\s*([0-9]{1,3})"
    r"(?:[^0-9A-Za-z/+-]+([0-9]{1,2}))?"
    r"(?:[^0-9A-Za-z/+-]+([0-9]{1,2}))?",
    flags=re.IGNORECASE,
)


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


def is_decimal_bbox(value):
    parts = [part.strip() for part in value.split(",")]
    if len(parts) != 4:
        return False

    try:
        [float(part) for part in parts]
    except ValueError:
        return False

    return True


def dms_to_decimal(hemisphere, degrees_text, minutes_text=None, seconds_text=None):
    degrees = int(degrees_text)
    minutes = int(minutes_text or 0)
    seconds = int(seconds_text or 0)

    if minutes >= 60 or seconds >= 60:
        raise ValueError(f"Invalid DMS component: {degrees_text}, {minutes_text}, {seconds_text}")

    decimal_value = degrees + (minutes / 60) + (seconds / 3600)
    if hemisphere.upper() in {"W", "S"}:
        decimal_value *= -1

    return decimal_value


def normalize_zero(value):
    if abs(value) < 1e-12:
        return 0.0
    return value


def format_decimal(value):
    normalized = normalize_zero(value)
    return f"{normalized:.6f}".rstrip("0").rstrip(".")


def parse_axis(axis_text, allowed_hemispheres):
    matches = []
    for match in COORDINATE_PATTERN.finditer(axis_text):
        hemisphere = match.group(1).upper()
        if hemisphere not in allowed_hemispheres:
            continue

        matches.append(
            dms_to_decimal(
                hemisphere=hemisphere,
                degrees_text=match.group(2),
                minutes_text=match.group(3),
                seconds_text=match.group(4),
            )
        )

    if len(matches) != 2:
        raise ValueError(f"Expected 2 coordinates in {axis_text!r}, found {len(matches)}.")

    start, end = sorted(matches)
    return normalize_zero(start), normalize_zero(end)


def convert_bbox_value(value):
    raw_value = str(value or "").strip()
    if not raw_value:
        return "", "empty"

    if is_decimal_bbox(raw_value):
        return raw_value, "decimal"

    if "/" not in raw_value:
        return raw_value, "preserved"

    longitude_text, latitude_text = [part.strip() for part in raw_value.split("/", maxsplit=1)]

    west, east = parse_axis(longitude_text, {"E", "W"})
    south, north = parse_axis(latitude_text, {"N", "S"})
    converted = ",".join(
        [
            format_decimal(west),
            format_decimal(south),
            format_decimal(east),
            format_decimal(north),
        ]
    )
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
