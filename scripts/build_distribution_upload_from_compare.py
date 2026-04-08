#!/usr/bin/env python3
"""
Build a distribution upload CSV containing rows present in a new CSV but absent
from a current CSV.

Rows are compared on the standard distribution columns:
- friendlier_id
- reference_type
- distribution_url
- label
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_NEW_CSV = PROJECT_ROOT / "inputs" / "new_reference_types.csv"
DEFAULT_CURRENT_CSV = PROJECT_ROOT / "inputs" / "current.csv"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "inputs" / "new_reference_types_upload.csv"

DISTRIBUTION_COLUMNS = ["friendlier_id", "reference_type", "distribution_url", "label"]


def resolve_path(path_value: str | Path) -> Path:
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (PROJECT_ROOT / candidate).resolve()


def load_distribution_csv_norm(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")

    for column in DISTRIBUTION_COLUMNS:
        if column not in df.columns:
            df[column] = ""
        df[column] = df[column].astype(str).str.strip()

    df["friendlier_id"] = df["friendlier_id"].astype(str).str.strip()
    return df[df["friendlier_id"].ne("")].copy()


def dedupe_distribution_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df[DISTRIBUTION_COLUMNS].drop_duplicates(subset=DISTRIBUTION_COLUMNS, keep="first")


def build_new_rows_for_upload(new_df: pd.DataFrame, current_df: pd.DataFrame) -> pd.DataFrame:
    new_rows = dedupe_distribution_rows(new_df)
    current_rows = dedupe_distribution_rows(current_df)

    upload_df = new_rows.merge(
        current_rows,
        on=DISTRIBUTION_COLUMNS,
        how="left",
        indicator=True,
    )
    upload_df = upload_df[upload_df["_merge"] == "left_only"].drop(columns=["_merge"])
    return upload_df.reset_index(drop=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--new-csv",
        default=str(DEFAULT_NEW_CSV),
        help=f"CSV containing the desired distribution rows. Default: {DEFAULT_NEW_CSV}",
    )
    parser.add_argument(
        "--current-csv",
        default=str(DEFAULT_CURRENT_CSV),
        help=f"CSV containing the current distribution rows. Default: {DEFAULT_CURRENT_CSV}",
    )
    parser.add_argument(
        "--output-csv",
        default=str(DEFAULT_OUTPUT_CSV),
        help=f"Path for the upload CSV. Default: {DEFAULT_OUTPUT_CSV}",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    new_csv = resolve_path(args.new_csv)
    current_csv = resolve_path(args.current_csv)
    output_csv = resolve_path(args.output_csv)

    if not new_csv.exists():
        parser.error(f"New CSV not found: {new_csv}")
    if not current_csv.exists():
        parser.error(f"Current CSV not found: {current_csv}")

    new_df = load_distribution_csv_norm(new_csv)
    current_df = load_distribution_csv_norm(current_csv)
    upload_df = build_new_rows_for_upload(new_df, current_df)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    upload_df.to_csv(output_csv, index=False)

    print(f"New CSV: {new_csv}")
    print(f"Current CSV: {current_csv}")
    print(f"Unique rows in new CSV: {len(dedupe_distribution_rows(new_df))}")
    print(f"Unique rows in current CSV: {len(dedupe_distribution_rows(current_df))}")
    print(f"Rows to upload: {len(upload_df)}")
    print(f"Output CSV: {output_csv}")


if __name__ == "__main__":
    main()
