#!/usr/bin/env python3
"""
Convert a directory of JSON files into a basic CSV.

Each JSON file becomes one CSV row. Top-level JSON keys become columns, and
list/dict values are preserved as JSON strings instead of being deeply flattened.
"""

import argparse
import csv
import json
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_INPUT_DIR = PROJECT_ROOT / "inputs" / "oregon-maps-json" / "oregondigital.org" / "concern" / "images"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "inputs" / "oregon-maps-json" / "oregon-images.csv"


def resolve_path(path_value: str) -> Path:
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (PROJECT_ROOT / candidate).resolve()


def stringify_value(value):
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def is_empty_value(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert a directory of JSON files into a basic CSV."
    )
    parser.add_argument(
        "--input-dir",
        default=str(DEFAULT_INPUT_DIR),
        help="Directory containing JSON files. Default: Oregon images folder.",
    )
    parser.add_argument(
        "--output-csv",
        default=str(DEFAULT_OUTPUT_CSV),
        help="Path to the output CSV file.",
    )
    parser.add_argument(
        "--drop-empty-columns",
        action="store_true",
        help="Omit columns that are empty across every JSON record.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    input_dir = resolve_path(args.input_dir)
    output_csv = resolve_path(args.output_csv)

    if not input_dir.exists():
        parser.error(f"Input directory not found: {input_dir}")
    if not input_dir.is_dir():
        parser.error(f"Input path is not a directory: {input_dir}")

    json_files = sorted(input_dir.glob("*.json"))
    if not json_files:
        parser.error(f"No JSON files found in {input_dir}")

    rows = []
    fieldnames = {"source_file"}
    non_empty_fields = set()

    for json_file in json_files:
        with json_file.open("r", encoding="utf-8") as handle:
            data = json.load(handle)

        if not isinstance(data, dict):
            raise ValueError(f"{json_file} does not contain a top-level JSON object.")

        row = {"source_file": json_file.name}
        for key, value in data.items():
            row[key] = stringify_value(value)
            fieldnames.add(key)
            if not is_empty_value(value):
                non_empty_fields.add(key)
        rows.append(row)

    if args.drop_empty_columns:
        ordered_fieldnames = ["source_file"] + sorted(non_empty_fields)
    else:
        ordered_fieldnames = ["source_file"] + sorted(
            name for name in fieldnames if name != "source_file"
        )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ordered_fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Input directory: {input_dir}")
    print(f"JSON files processed: {len(json_files)}")
    if args.drop_empty_columns:
        dropped_count = len(fieldnames) - 1 - len(non_empty_fields)
        print(f"Empty columns dropped: {dropped_count}")
    print(f"Output CSV: {output_csv}")


if __name__ == "__main__":
    main()
