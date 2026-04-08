#!/usr/bin/env python3
"""
Set `reference_type` values in a CSV based on each row's `distribution_url`.

Rules:
- contains `MapServer` -> `arcgis_dynamic_map_layer`
- contains `FeatureServer` -> `arcgis_feature_layer`
- contains `ImageServer` -> `arcgis_image_map_layer`
- otherwise -> `documentation_external`
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_INPUT_CSV = PROJECT_ROOT / "inputs" / "new.csv"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "inputs" / "new_reference_types.csv"

REFERENCE_TYPE_FIELD = "reference_type"
DISTRIBUTION_URL_FIELD = "distribution_url"


def resolve_path(path_value: str | Path) -> Path:
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (PROJECT_ROOT / candidate).resolve()


def derive_reference_type(distribution_url: str) -> str:
    url = str(distribution_url or "").casefold()
    if "featureserver" in url:
        return "arcgis_feature_layer"
    if "imageserver" in url:
        return "arcgis_image_map_layer"
    if "mapserver" in url:
        return "arcgis_dynamic_map_layer"
    return "documentation_external"


def read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [{str(key): str(value or "") for key, value in row.items() if key} for row in reader]
    return fieldnames, rows


def get_output_fieldnames(fieldnames: list[str]) -> list[str]:
    if REFERENCE_TYPE_FIELD in fieldnames:
        return fieldnames
    return [*fieldnames, REFERENCE_TYPE_FIELD]


def update_reference_types(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    updated_rows: list[dict[str, str]] = []
    for row in rows:
        updated_row = dict(row)
        updated_row[REFERENCE_TYPE_FIELD] = derive_reference_type(
            updated_row.get(DISTRIBUTION_URL_FIELD, "")
        )
        updated_rows.append(updated_row)
    return updated_rows


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-csv",
        default=str(DEFAULT_INPUT_CSV),
        help=f"CSV to read. Default: {DEFAULT_INPUT_CSV}",
    )
    parser.add_argument(
        "--output-csv",
        default=str(DEFAULT_OUTPUT_CSV),
        help=f"CSV to write. Default: {DEFAULT_OUTPUT_CSV}",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite the input CSV instead of writing a separate output file.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    input_csv = resolve_path(args.input_csv)
    output_csv = input_csv if args.in_place else resolve_path(args.output_csv)

    if not input_csv.exists():
        parser.error(f"Input CSV not found: {input_csv}")

    fieldnames, rows = read_rows(input_csv)
    if DISTRIBUTION_URL_FIELD not in fieldnames:
        parser.error(
            f"Input CSV must include a `{DISTRIBUTION_URL_FIELD}` column: {input_csv}"
        )

    updated_rows = update_reference_types(rows)
    output_fieldnames = get_output_fieldnames(fieldnames)
    write_rows(output_csv, output_fieldnames, updated_rows)

    print(f"Input CSV: {input_csv}")
    print(f"Rows processed: {len(updated_rows)}")
    print(f"Output CSV: {output_csv}")


if __name__ == "__main__":
    main()
