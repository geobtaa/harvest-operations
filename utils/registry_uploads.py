from datetime import date
from pathlib import Path

import pandas as pd

from scripts.build_uploads import (
    build_distribution_delta_files,
    load_distribution_csv_norm,
    load_primary_csv_norm,
)
from utils.harvester_helpers import first_non_empty
from utils.output_naming import infer_upload_source_prefix


PRIMARY_REGISTRY_FIELDS = [
    "Title",
    "Alternative Title",
    "Creator",
    "Publisher",
    "Resource Class",
    "Temporal Coverage",
    "Date Issued",
    "Date Accessioned",
    "ID",
    "Identifier",
    "Code",
]

DISTRIBUTION_REGISTRY_FIELDS = [
    "friendlier_id",
    "reference_type",
    "distribution_url",
    "label",
]


def build_uploads_from_registry(
    results: dict,
    config: dict,
    *,
    source_label: str,
    default_source: str,
    retired_resource_class: str,
) -> dict | None:
    """Compare current outputs with compact registry snapshots and update them."""
    if not config.get("build_uploads"):
        return None

    primary_csv = results.get("primary_csv")
    distributions_csv = results.get("distributions_csv")
    if not primary_csv or not distributions_csv:
        return {
            "status": "skipped",
            "reason": (
                f"{source_label} registry uploads require primary_csv "
                "and distributions_csv results."
            ),
        }

    primary_registry_path = Path(config["primary_registry_csv"])
    distributions_registry_path = Path(config["distributions_registry_csv"])
    if not primary_registry_path.exists():
        return {
            "status": "skipped",
            "reason": f"{source_label} primary registry not found: {primary_registry_path}",
        }

    today = date.today().isoformat()
    source = infer_upload_source_prefix(
        config.get("output_primary_csv", f"{default_source}_primary.csv")
    )
    primary_path = Path(primary_csv).resolve()
    distributions_path = Path(distributions_csv).resolve()
    upload_dir = primary_path.parent / "to_upload"
    upload_dir.mkdir(parents=True, exist_ok=True)

    current_primary_df = load_primary_csv_norm(primary_path)
    current_distribution_df = load_distribution_csv_norm(distributions_path)
    registry_primary_df = load_primary_registry(primary_registry_path)
    registry_distribution_df = load_distribution_registry(distributions_registry_path)

    current_item_df = item_rows_for_registry(current_primary_df)
    registry_item_df = item_rows_for_registry(registry_primary_df)
    active_registry_df = active_registry_rows(registry_item_df)

    current_ids = set(current_item_df["ID"].astype(str).str.strip())
    active_registry_ids = set(active_registry_df["ID"].astype(str).str.strip())
    new_ids = current_ids - active_registry_ids
    shared_ids = current_ids.intersection(active_registry_ids)

    refresh_rows = harvest_record_rows_for_upload(current_primary_df)
    new_primary_rows = current_item_df[current_item_df["ID"].isin(new_ids)].copy()
    retired_registry_rows = active_registry_df[
        ~active_registry_df["ID"].isin(current_ids)
    ].copy()
    retired_primary_rows = build_retired_upload_rows(
        retired_registry_rows,
        today,
        retired_resource_class,
    )

    primary_upload_df = build_primary_upload_dataframe(
        current_primary_df,
        refresh_rows,
        new_primary_rows,
        retired_primary_rows,
    )
    dist_new_df, dist_delete_df, changed_distribution_ids = (
        build_distribution_delta_files(
            current_distribution_df,
            registry_distribution_df,
            new_ids=new_ids,
            shared_ids=shared_ids,
        )
    )

    primary_upload_path = upload_dir / f"{today}_{source}_primary_upload.csv"
    dist_new_path = upload_dir / f"{today}_{source}_distributions_new.csv"
    dist_delete_path = upload_dir / f"{today}_{source}_distributions_delete.csv"

    primary_upload_df.to_csv(primary_upload_path, index=False, encoding="utf-8")
    dist_new_df.to_csv(dist_new_path, index=False, encoding="utf-8")
    dist_delete_df.to_csv(dist_delete_path, index=False, encoding="utf-8")

    updated_primary_registry = build_updated_primary_registry(
        current_item_df,
        registry_item_df,
        today,
    )
    updated_distribution_registry = build_updated_distribution_registry(
        current_distribution_df
    )
    write_registry(primary_registry_path, updated_primary_registry)
    write_registry(distributions_registry_path, updated_distribution_registry)

    return {
        "status": "created",
        "source": source,
        "primary_upload_csv": str(primary_upload_path),
        "distributions_new_csv": str(dist_new_path),
        "distributions_delete_csv": str(dist_delete_path),
        "primary_registry_csv": str(primary_registry_path),
        "distributions_registry_csv": str(distributions_registry_path),
        "new_count": len(new_primary_rows),
        "retired_count": len(retired_primary_rows),
        "distribution_new_count": len(dist_new_df),
        "distribution_delete_count": len(dist_delete_df),
        "changed_distribution_ids": sorted(changed_distribution_ids),
    }


def load_primary_registry(path: Path) -> pd.DataFrame:
    return load_registry(path, PRIMARY_REGISTRY_FIELDS, required_column="ID")


def load_distribution_registry(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=DISTRIBUTION_REGISTRY_FIELDS)
    return load_registry(
        path,
        DISTRIBUTION_REGISTRY_FIELDS,
        required_column="friendlier_id",
    )


def load_registry(path: Path, fields: list[str], required_column: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    for column in fields:
        if column not in df.columns:
            df[column] = ""
    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].astype(str).str.strip()
    df = df[df[required_column].astype(str).str.strip().ne("")].copy()
    if required_column == "ID":
        return df.drop_duplicates(subset=[required_column], keep="first")
    return df


def item_rows_for_registry(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "Resource Class" not in work.columns:
        work["Resource Class"] = ""
    if "ID" not in work.columns:
        work["ID"] = ""
    mask = ~is_harvest_record_row(work)
    return work[mask & work["ID"].astype(str).str.strip().ne("")].copy()


def harvest_record_rows_for_upload(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    return df[is_harvest_record_row(df)].copy()


def is_harvest_record_row(df: pd.DataFrame) -> pd.Series:
    resource_class = (
        df.get("Resource Class", pd.Series("", index=df.index))
        .astype(str)
        .str.strip()
        .str.casefold()
    )
    ids = df.get("ID", pd.Series("", index=df.index)).astype(str).str.strip()
    return resource_class.eq("websites") | ids.str.startswith("harvest_")


def active_registry_rows(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "registry_status" not in work.columns:
        work["registry_status"] = ""
    status = work["registry_status"].astype(str).str.strip().str.casefold()
    return work[status.ne("retired")].copy()


def build_retired_upload_rows(
    registry_rows: pd.DataFrame,
    retired_on: str,
    retired_resource_class: str,
) -> pd.DataFrame:
    if registry_rows.empty:
        return pd.DataFrame(columns=PRIMARY_REGISTRY_FIELDS)

    retired_rows = registry_rows.copy()
    for column in PRIMARY_REGISTRY_FIELDS:
        if column not in retired_rows.columns:
            retired_rows[column] = ""

    title_fallback = retired_rows["Title"].astype(str).str.strip().eq("")
    retired_rows.loc[title_fallback, "Title"] = retired_rows.loc[
        title_fallback,
        "Alternative Title",
    ]
    retired_rows["Display Note"] = (
        f"Danger: Record not found during verification on {retired_on}; marked as retired."
    )
    retired_rows["Date Retired"] = retired_on
    retired_rows["Resource Class"] = retired_rows["Resource Class"].replace(
        "",
        retired_resource_class,
    )
    retired_rows["Publication State"] = "unpublished"
    retired_rows["Access Rights"] = "Public"
    return retired_rows


def build_primary_upload_dataframe(
    current_primary_df: pd.DataFrame,
    refresh_rows: pd.DataFrame,
    new_primary_rows: pd.DataFrame,
    retired_primary_rows: pd.DataFrame,
) -> pd.DataFrame:
    parts = [
        part
        for part in [refresh_rows, new_primary_rows, retired_primary_rows]
        if part is not None and not part.empty
    ]
    upload_columns = list(
        dict.fromkeys(
            list(current_primary_df.columns)
            + ["Display Note", "Date Retired", "Publication State", "Access Rights"]
            + PRIMARY_REGISTRY_FIELDS
        )
    )
    if not parts:
        return pd.DataFrame(columns=upload_columns)

    upload_df = pd.concat(parts, ignore_index=True)
    upload_df = upload_df.drop_duplicates(subset=["ID"], keep="first")
    return upload_df.reindex(columns=upload_columns, fill_value="")


def build_updated_primary_registry(
    current_item_df: pd.DataFrame,
    existing_registry_df: pd.DataFrame,
    run_date: str,
) -> pd.DataFrame:
    existing_by_id = {
        str(row.get("ID", "")).strip(): row
        for row in existing_registry_df.to_dict("records")
        if str(row.get("ID", "")).strip()
    }
    rows = []

    for current_row in current_item_df.to_dict("records"):
        row_id = str(current_row.get("ID", "")).strip()
        if not row_id:
            continue
        existing_row = existing_by_id.get(row_id, {})
        registry_row = {
            field: str(current_row.get(field, "") or "").strip()
            for field in PRIMARY_REGISTRY_FIELDS
        }
        registry_row["Date Accessioned"] = first_non_empty(
            existing_row.get("Date Accessioned", ""),
            current_row.get("Date Accessioned", ""),
            run_date,
        )
        rows.append(registry_row)

    return pd.DataFrame(rows, columns=PRIMARY_REGISTRY_FIELDS)


def build_updated_distribution_registry(
    current_distribution_df: pd.DataFrame,
) -> pd.DataFrame:
    registry_df = current_distribution_df.copy()
    for column in DISTRIBUTION_REGISTRY_FIELDS:
        if column not in registry_df.columns:
            registry_df[column] = ""
    registry_df = registry_df[
        registry_df["friendlier_id"].astype(str).str.strip().ne("")
    ]
    registry_df = registry_df.drop_duplicates(
        subset=["friendlier_id", "reference_type", "distribution_url", "label"],
        keep="first",
    )
    return registry_df.reindex(columns=DISTRIBUTION_REGISTRY_FIELDS, fill_value="")


def write_registry(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
