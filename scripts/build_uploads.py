#!/usr/bin/env python3
import argparse
import re
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd


DEFAULT_SOURCE = "arcgis"
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUTS_DIR = SCRIPT_DIR.parent / "outputs"


def build_filename_regex(source: str, suffix: str) -> re.Pattern[str]:
    return re.compile(
        fr"^(\d{{4}}-\d{{2}}[-_]\d{{2}})_{re.escape(source)}_{suffix}\.csv$"
    )


def discover_dated_files(outputs_dir: Path, pattern: re.Pattern[str]) -> list[tuple[date, Path]]:
    candidates = []
    for path in outputs_dir.iterdir():
        if not path.is_file():
            continue
        match = pattern.match(path.name)
        if not match:
            continue
        iso_date = match.group(1).replace("_", "-")
        try:
            candidates.append((date.fromisoformat(iso_date), path))
        except ValueError:
            continue
    candidates.sort(key=lambda item: item[0])
    return candidates


def select_old_and_new_files(candidates: list[tuple[date, Path]]) -> tuple[tuple[date, Path], tuple[date, Path]]:
    if len(candidates) < 2:
        found = ", ".join(path.name for _, path in candidates) or "(none)"
        raise SystemExit(
            "Need at least two matching primary files to compare.\n"
            f"Found: {found}"
        )

    old_item = candidates[-2]
    new_item = candidates[-1]
    if old_item[1] == new_item[1]:
        raise SystemExit("Internal error: old and new paths resolved to the same file.")
    return old_item, new_item


def find_file_for_date(
    candidates: list[tuple[date, Path]],
    target_date: date,
) -> Optional[Path]:
    for candidate_date, path in candidates:
        if candidate_date == target_date:
            return path
    return None


def dated_file_info(source: str, suffix: str, path: Path) -> tuple[date, Path]:
    pattern = build_filename_regex(source, suffix)
    match = pattern.match(path.name)
    if not match:
        raise SystemExit(
            f"Expected {path.name} to match YYYY-MM-DD_{source}_{suffix}.csv"
        )

    iso_date = match.group(1).replace("_", "-")
    return date.fromisoformat(iso_date), path


def most_recent_file_before(
    candidates: list[tuple[date, Path]],
    current_path: Path,
) -> Optional[tuple[date, Path]]:
    current_path = current_path.resolve()
    prior_candidates = [
        (candidate_date, path)
        for candidate_date, path in candidates
        if path.resolve() != current_path
    ]
    if not prior_candidates:
        return None
    return prior_candidates[-1]


def load_table_norm(
    path: Path,
    required_column: str,
    *,
    dedupe_subset: Optional[list[str]] = None,
) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    if required_column not in df.columns:
        try:
            df = pd.read_csv(path, dtype=str, keep_default_na=False, sep="\t").fillna("")
        except Exception:
            pass

    if required_column not in df.columns:
        raise SystemExit(
            f"Missing '{required_column}' column in {path.name} (tried comma and tab)."
        )

    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].astype(str).str.strip()

    df[required_column] = df[required_column].astype(str).str.strip()
    df = df[df[required_column].ne("")].copy()

    if dedupe_subset:
        df = df.drop_duplicates(subset=dedupe_subset, keep="first")

    return df


def load_primary_csv_norm(path: Path) -> pd.DataFrame:
    return load_table_norm(path, "ID", dedupe_subset=["ID"])


def load_distribution_csv_norm(path: Path) -> pd.DataFrame:
    df = load_table_norm(path, "friendlier_id")
    for column in ["reference_type", "distribution_url", "label"]:
        if column not in df.columns:
            df[column] = ""
        df[column] = df[column].astype(str).str.strip()
    return df


def build_primary_upload(
    new_df: pd.DataFrame,
    old_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    parts = []

    websites = pd.DataFrame(columns=new_df.columns)
    if "Resource Class" in new_df.columns:
        websites = new_df[new_df["Resource Class"].str.lower() == "websites"].copy()
        if not websites.empty:
            parts.append(websites)

    new_only = new_df.merge(
        old_df[["ID"]].drop_duplicates(),
        on="ID",
        how="left",
        indicator=True,
    )
    new_only = new_only[new_only["_merge"] == "left_only"].drop(columns=["_merge"])
    if not new_only.empty:
        parts.append(new_only)

    old_only = old_df.merge(
        new_df[["ID"]].drop_duplicates(),
        on="ID",
        how="left",
        indicator=True,
    )
    old_only = old_only[old_only["_merge"] == "left_only"].drop(columns=["_merge"]).copy()
    if not old_only.empty:
        if "Publication State" not in old_only.columns:
            old_only["Publication State"] = ""
        if "Date Retired" not in old_only.columns:
            old_only["Date Retired"] = ""
        if "Display Note" not in old_only.columns:
            old_only["Display Note"] = ""

        today_str = date.today().isoformat()
        old_only["Publication State"] = "unpublished"
        old_only["Date Retired"] = today_str
        old_only["Display Note"] = (
            f"Danger: Record not found during verification on {today_str}; marked as retired."
        )
        parts.append(old_only)

    if parts:
        upload_df = pd.concat(parts, ignore_index=True)
        upload_df = upload_df.drop_duplicates(subset=["ID"], keep="first")
    else:
        upload_df = pd.DataFrame(columns=new_df.columns)

    return upload_df, new_only, old_only


def distribution_rows_by_id(df: pd.DataFrame) -> dict[str, set[tuple[str, str, str]]]:
    rows_by_id: dict[str, set[tuple[str, str, str]]] = {}
    if df.empty:
        return rows_by_id

    columns = ["friendlier_id", "reference_type", "distribution_url", "label"]
    work = df[columns].copy()
    for friendlier_id, group in work.groupby("friendlier_id", sort=True):
        rows_by_id[friendlier_id] = set(
            group[["reference_type", "distribution_url", "label"]].itertuples(
                index=False,
                name=None,
            )
        )
    return rows_by_id


def build_distribution_delta_files(
    new_dist_df: pd.DataFrame,
    old_dist_df: Optional[pd.DataFrame],
    *,
    new_ids: set[str],
    shared_ids: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame, set[str]]:
    columns = ["friendlier_id", "reference_type", "distribution_url", "label"]
    add_rows: list[dict[str, str]] = []
    delete_rows: list[dict[str, str]] = []
    changed_ids: set[str] = set()

    new_rows_by_id = distribution_rows_by_id(new_dist_df)
    old_rows_by_id = distribution_rows_by_id(old_dist_df) if old_dist_df is not None else {}

    for friendlier_id in sorted(new_ids):
        for reference_type, distribution_url, label in sorted(new_rows_by_id.get(friendlier_id, set())):
            add_rows.append(
                {
                    "friendlier_id": friendlier_id,
                    "reference_type": reference_type,
                    "distribution_url": distribution_url,
                    "label": label,
                }
            )

    if old_dist_df is not None:
        for friendlier_id in sorted(shared_ids):
            old_rows = old_rows_by_id.get(friendlier_id, set())
            new_rows = new_rows_by_id.get(friendlier_id, set())

            rows_to_add = sorted(new_rows - old_rows)
            rows_to_delete = sorted(old_rows - new_rows)

            if rows_to_add or rows_to_delete:
                changed_ids.add(friendlier_id)

            for reference_type, distribution_url, label in rows_to_add:
                add_rows.append(
                    {
                        "friendlier_id": friendlier_id,
                        "reference_type": reference_type,
                        "distribution_url": distribution_url,
                        "label": label,
                    }
                )

            for reference_type, distribution_url, label in rows_to_delete:
                delete_rows.append(
                    {
                        "friendlier_id": friendlier_id,
                        "reference_type": reference_type,
                        "distribution_url": distribution_url,
                        "label": label,
                    }
                )

    add_df = pd.DataFrame(add_rows, columns=columns)
    delete_df = pd.DataFrame(delete_rows, columns=columns)
    return add_df, delete_df, changed_ids


def run_build_uploads(
    source: str,
    outputs_dir: Path,
    upload_dir: Optional[Path] = None,
) -> dict[str, object]:
    primary_pattern = build_filename_regex(source, "primary")
    dist_pattern = build_filename_regex(source, "distributions")

    primary_candidates = discover_dated_files(outputs_dir, primary_pattern)
    if not primary_candidates:
        raise SystemExit(f"No {source} primary CSVs found in {outputs_dir}")

    (old_date, old_primary_path), (new_date, new_primary_path) = select_old_and_new_files(
        primary_candidates
    )

    print(f"Newest primary:         {new_primary_path.name}")
    print(f"Second-most-recent:     {old_primary_path.name}")

    new_df = load_primary_csv_norm(new_primary_path)
    old_df = load_primary_csv_norm(old_primary_path)
    primary_upload_df, new_only_df, old_only_df = build_primary_upload(new_df, old_df)

    print(f"New additions: {len(new_only_df)}")
    print(f"To retire:     {len(old_only_df)}")

    dist_candidates = discover_dated_files(outputs_dir, dist_pattern)
    if not dist_candidates:
        raise SystemExit(f"No {source} distributions CSVs found in {outputs_dir}")

    new_dist_path = find_file_for_date(dist_candidates, new_date)
    if new_dist_path is None:
        found_names = ", ".join(sorted(path.name for _, path in dist_candidates)) or "(none)"
        raise SystemExit(
            f"No distributions CSV found for date {new_date.isoformat()}.\n"
            f"Expected: {new_date.isoformat()}_{source}_distributions.csv (hyphen or underscore ok)\n"
            f"Found distributions files: {found_names}"
        )

    old_dist_path = find_file_for_date(dist_candidates, old_date)

    print(f"Matched new distributions: {new_dist_path.name}")
    if old_dist_path is None:
        print(
            "Matched old distributions: (none found for previous run; "
            "same-ID distribution changes will be skipped)"
        )
    else:
        print(f"Matched old distributions: {old_dist_path.name}")

    new_dist_df = load_distribution_csv_norm(new_dist_path)
    old_dist_df = load_distribution_csv_norm(old_dist_path) if old_dist_path else None

    new_ids = set(new_only_df["ID"].astype(str).str.strip())
    shared_ids = set(new_df["ID"].astype(str).str.strip()).intersection(
        old_df["ID"].astype(str).str.strip()
    )

    dist_new_df, dist_delete_df, changed_distribution_ids = build_distribution_delta_files(
        new_dist_df,
        old_dist_df,
        new_ids=new_ids,
        shared_ids=shared_ids,
    )

    print(f"Distribution rows to add:    {len(dist_new_df)}")
    print(f"Distribution rows to delete: {len(dist_delete_df)}")
    print(f"Records with distribution changes: {len(changed_distribution_ids)}")

    upload_dir = upload_dir or outputs_dir / "to_upload"
    upload_dir.mkdir(parents=True, exist_ok=True)

    today_str = date.today().isoformat()
    primary_out_path = upload_dir / f"{today_str}_{source}_primary_upload.csv"
    dist_new_out_path = upload_dir / f"{today_str}_{source}_distributions_new.csv"
    dist_delete_out_path = upload_dir / f"{today_str}_{source}_distributions_delete.csv"

    primary_upload_df.to_csv(primary_out_path, index=False, encoding="utf-8")
    dist_new_df.to_csv(dist_new_out_path, index=False, encoding="utf-8")
    dist_delete_df.to_csv(dist_delete_out_path, index=False, encoding="utf-8")

    print(f"Wrote {len(primary_upload_df)} rows to {primary_out_path}")
    print(f"Wrote {len(dist_new_df)} rows to {dist_new_out_path}")
    print(f"Wrote {len(dist_delete_df)} rows to {dist_delete_out_path}")

    return {
        "source": source,
        "new_primary_path": new_primary_path,
        "old_primary_path": old_primary_path,
        "new_dist_path": new_dist_path,
        "old_dist_path": old_dist_path,
        "primary_upload_path": primary_out_path,
        "dist_new_path": dist_new_out_path,
        "dist_delete_path": dist_delete_out_path,
        "new_count": len(new_only_df),
        "retired_count": len(old_only_df),
        "distribution_new_count": len(dist_new_df),
        "distribution_delete_count": len(dist_delete_df),
        "changed_distribution_ids": changed_distribution_ids,
    }


def run_build_uploads_for_current(
    source: str,
    outputs_dir: Path,
    new_primary_path: Path,
    new_dist_path: Path,
    upload_dir: Optional[Path] = None,
) -> dict[str, object]:
    primary_pattern = build_filename_regex(source, "primary")
    dist_pattern = build_filename_regex(source, "distributions")

    new_date, new_primary_path = dated_file_info(source, "primary", new_primary_path)
    _, new_dist_path = dated_file_info(source, "distributions", new_dist_path)

    primary_candidates = discover_dated_files(outputs_dir, primary_pattern)
    old_item = most_recent_file_before(primary_candidates, new_primary_path)
    if old_item is None:
        raise SystemExit(
            f"No prior {source} primary CSV found in {outputs_dir}; skipping upload deltas."
        )

    old_date, old_primary_path = old_item

    print(f"Newest primary:         {new_primary_path.name}")
    print(f"Previous primary:       {old_primary_path.name}")

    new_df = load_primary_csv_norm(new_primary_path)
    old_df = load_primary_csv_norm(old_primary_path)
    primary_upload_df, new_only_df, old_only_df = build_primary_upload(new_df, old_df)

    print(f"New additions: {len(new_only_df)}")
    print(f"To retire:     {len(old_only_df)}")

    dist_candidates = discover_dated_files(outputs_dir, dist_pattern)
    old_dist_path = find_file_for_date(dist_candidates, old_date)

    print(f"Matched new distributions: {new_dist_path.name}")
    if old_dist_path is None:
        print(
            "Matched old distributions: (none found for previous run; "
            "same-ID distribution changes will be skipped)"
        )
    else:
        print(f"Matched old distributions: {old_dist_path.name}")

    new_dist_df = load_distribution_csv_norm(new_dist_path)
    old_dist_df = load_distribution_csv_norm(old_dist_path) if old_dist_path else None

    new_ids = set(new_only_df["ID"].astype(str).str.strip())
    shared_ids = set(new_df["ID"].astype(str).str.strip()).intersection(
        old_df["ID"].astype(str).str.strip()
    )

    dist_new_df, dist_delete_df, changed_distribution_ids = build_distribution_delta_files(
        new_dist_df,
        old_dist_df,
        new_ids=new_ids,
        shared_ids=shared_ids,
    )

    print(f"Distribution rows to add:    {len(dist_new_df)}")
    print(f"Distribution rows to delete: {len(dist_delete_df)}")
    print(f"Records with distribution changes: {len(changed_distribution_ids)}")

    upload_dir = upload_dir or outputs_dir / "to_upload"
    upload_dir.mkdir(parents=True, exist_ok=True)

    today_str = date.today().isoformat()
    primary_out_path = upload_dir / f"{today_str}_{source}_primary_upload.csv"
    dist_new_out_path = upload_dir / f"{today_str}_{source}_distributions_new.csv"
    dist_delete_out_path = upload_dir / f"{today_str}_{source}_distributions_delete.csv"

    primary_upload_df.to_csv(primary_out_path, index=False, encoding="utf-8")
    dist_new_df.to_csv(dist_new_out_path, index=False, encoding="utf-8")
    dist_delete_df.to_csv(dist_delete_out_path, index=False, encoding="utf-8")

    print(f"Wrote {len(primary_upload_df)} rows to {primary_out_path}")
    print(f"Wrote {len(dist_new_df)} rows to {dist_new_out_path}")
    print(f"Wrote {len(dist_delete_df)} rows to {dist_delete_out_path}")

    return {
        "source": source,
        "new_primary_path": new_primary_path,
        "old_primary_path": old_primary_path,
        "new_dist_path": new_dist_path,
        "old_dist_path": old_dist_path,
        "primary_upload_path": primary_out_path,
        "dist_new_path": dist_new_out_path,
        "dist_delete_path": dist_delete_out_path,
        "new_count": len(new_only_df),
        "retired_count": len(old_only_df),
        "distribution_new_count": len(dist_new_df),
        "distribution_delete_count": len(dist_delete_df),
        "changed_distribution_ids": changed_distribution_ids,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build upload-ready primary and distribution delta CSVs by comparing the "
            "two most recent dated harvest outputs for a source."
        )
    )
    parser.add_argument(
        "source",
        nargs="?",
        default=DEFAULT_SOURCE,
        help=f"Source prefix used in output filenames. Default: {DEFAULT_SOURCE}",
    )
    parser.add_argument(
        "--outputs-dir",
        type=Path,
        default=DEFAULT_OUTPUTS_DIR,
        help=f"Directory containing dated harvest outputs. Default: {DEFAULT_OUTPUTS_DIR}",
    )
    parser.add_argument(
        "--upload-dir",
        type=Path,
        default=None,
        help="Directory for upload CSVs. Default: <outputs-dir>/to_upload",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    upload_dir = args.upload_dir.resolve() if args.upload_dir else None
    run_build_uploads(args.source, args.outputs_dir.resolve(), upload_dir)


if __name__ == "__main__":
    main()
