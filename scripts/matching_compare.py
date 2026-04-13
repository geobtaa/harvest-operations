from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_NEW_CSV = PROJECT_ROOT / "outputs/2026-04-13_iowa-library_primary.csv"
DEFAULT_OLD_CSV = PROJECT_ROOT / "outputs/old-iowa-records.csv"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "outputs/iowa-records-comparison.csv"
DEFAULT_MATCH_COLUMN = "WxS Identifier"
MATCH_STATUS_COLUMN = "Match Status"
OLD_SUFFIX = " (old)"


def build_column_order(
    new_columns: list[str],
    old_column_map: dict[str, str],
    status_column: str,
) -> list[str]:
    ordered_columns: list[str] = []

    for index, column in enumerate(new_columns):
        ordered_columns.append(column)

        old_column = old_column_map.get(column)
        if old_column:
            ordered_columns.append(old_column)

        if index == 0:
            ordered_columns.append(status_column)

    for old_column in old_column_map.values():
        if old_column not in ordered_columns:
            ordered_columns.append(old_column)

    return ordered_columns


def normalize_match_value(value: str) -> str:
    # Iowa WxS identifiers differ only by embedded spaces between exports.
    if pd.isna(value):
        return ""
    return "".join(str(value).split())


def build_comparison_csv(
    new_csv_path: Path,
    old_csv_path: Path,
    output_csv_path: Path,
    match_column: str = DEFAULT_MATCH_COLUMN,
) -> pd.DataFrame:
    new_df = pd.read_csv(new_csv_path, dtype=str, keep_default_na=False)
    old_df = pd.read_csv(old_csv_path, dtype=str, keep_default_na=False)

    new_columns = list(new_df.columns)
    old_columns = list(old_df.columns)

    if match_column not in new_columns:
        raise KeyError(f"Match column '{match_column}' not found in {new_csv_path}")
    if match_column not in old_columns:
        raise KeyError(f"Match column '{match_column}' not found in {old_csv_path}")

    new_df["_match_key"] = new_df[match_column].map(normalize_match_value)
    old_df["_match_key"] = old_df[match_column].map(normalize_match_value)

    duplicate_new_keys = new_df["_match_key"].duplicated() & new_df["_match_key"].ne("")
    if duplicate_new_keys.any():
        duplicate_key = new_df.loc[duplicate_new_keys, "_match_key"].iloc[0]
        raise ValueError(
            f"Duplicate normalized '{match_column}' value '{duplicate_key}' found in {new_csv_path}"
        )

    duplicate_old_keys = old_df["_match_key"].duplicated() & old_df["_match_key"].ne("")
    if duplicate_old_keys.any():
        duplicate_key = old_df.loc[duplicate_old_keys, "_match_key"].iloc[0]
        raise ValueError(
            f"Duplicate normalized '{match_column}' value '{duplicate_key}' found in {old_csv_path}"
        )

    old_column_map = {column: f"{column}{OLD_SUFFIX}" for column in old_columns}
    renamed_old_df = old_df.rename(columns=old_column_map)

    comparison_df = new_df.merge(
        renamed_old_df,
        how="outer",
        left_on="_match_key",
        right_on="_match_key",
        indicator=True,
    )
    comparison_df[MATCH_STATUS_COLUMN] = comparison_df["_merge"].map(
        {"both": "matched", "left_only": "only_in_new", "right_only": "only_in_old"}
    )
    comparison_df = comparison_df.drop(columns=["_match_key", "_merge"])
    comparison_df = comparison_df.astype(object).fillna("")

    ordered_columns = build_column_order(
        new_columns=new_columns,
        old_column_map=old_column_map,
        status_column=MATCH_STATUS_COLUMN,
    )
    comparison_df = comparison_df[ordered_columns]

    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    comparison_df.to_csv(output_csv_path, index=False)

    return comparison_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare two CSV files by key, keeping all rows from both CSVs and "
            "placing old CSV columns beside their matching new CSV columns."
        )
    )
    parser.add_argument(
        "--new-csv",
        type=Path,
        default=DEFAULT_NEW_CSV,
        help=f"Path to the primary CSV. Default: {DEFAULT_NEW_CSV}",
    )
    parser.add_argument(
        "--old-csv",
        type=Path,
        default=DEFAULT_OLD_CSV,
        help=f"Path to the comparison CSV. Default: {DEFAULT_OLD_CSV}",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help=f"Where to write the comparison CSV. Default: {DEFAULT_OUTPUT_CSV}",
    )
    parser.add_argument(
        "--match-column",
        default=DEFAULT_MATCH_COLUMN,
        help=f"Column used to match rows. Default: {DEFAULT_MATCH_COLUMN}",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    comparison_df = build_comparison_csv(
        new_csv_path=args.new_csv,
        old_csv_path=args.old_csv,
        output_csv_path=args.output_csv,
        match_column=args.match_column,
    )

    matched_rows = int((comparison_df[MATCH_STATUS_COLUMN] == "matched").sum())
    new_only_rows = int((comparison_df[MATCH_STATUS_COLUMN] == "only_in_new").sum())
    old_only_rows = int((comparison_df[MATCH_STATUS_COLUMN] == "only_in_old").sum())

    print(f"Wrote {len(comparison_df)} rows to {args.output_csv}")
    print(f"Matched rows: {matched_rows}")
    print(f"Only in new CSV: {new_only_rows}")
    print(f"Only in old CSV: {old_only_rows}")


if __name__ == "__main__":
    main()
