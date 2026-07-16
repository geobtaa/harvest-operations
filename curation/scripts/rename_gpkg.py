#!/usr/bin/env python3
"""
Batch rename GeoPackages:
- rename the .gpkg file
- rename the single feature table inside
- update key GeoPackage system tables that reference the old table name

Assumptions:
- One feature layer (one feature table) per GeoPackage.
- The GeoPackage is not open in QGIS while this runs.
"""

from __future__ import annotations

import csv
import os
import shutil
import sqlite3
from pathlib import Path


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ? LIMIT 1",
        (name,),
    )
    return cur.fetchone() is not None


def rename_spatial_index_if_present(conn: sqlite3.Connection, old_table: str, new_table: str) -> None:
    """
    If a GeoPackage has an RTree spatial index, it typically creates tables like:
      rtree_<table>_<geomcol>
      rtree_<table>_<geomcol>_node
      rtree_<table>_<geomcol>_parent
      rtree_<table>_<geomcol>_rowid

    We rename those too when present.
    """
    cur = conn.execute(
        "SELECT column_name FROM gpkg_geometry_columns WHERE table_name = ?",
        (old_table,),
    )
    row = cur.fetchone()
    if not row:
        return

    geomcol = row[0]
    old_prefix = f"rtree_{old_table}_{geomcol}"
    new_prefix = f"rtree_{new_table}_{geomcol}"

    suffixes = ["", "_node", "_parent", "_rowid"]
    for sfx in suffixes:
        old_rtree = old_prefix + sfx
        new_rtree = new_prefix + sfx
        if table_exists(conn, old_rtree) and not table_exists(conn, new_rtree):
            conn.execute(f'ALTER TABLE "{old_rtree}" RENAME TO "{new_rtree}"')


def rename_gpkg_internal_tables(conn: sqlite3.Connection, old_table: str, new_table: str) -> None:
    """
    Update core GeoPackage tables (and a couple common optional ones) that store the table name.
    """
    # Core / common
    conn.execute(
        "UPDATE gpkg_contents SET table_name = ? WHERE table_name = ?",
        (new_table, old_table),
    )
    conn.execute(
        "UPDATE gpkg_geometry_columns SET table_name = ? WHERE table_name = ?",
        (new_table, old_table),
    )

    # If you’re using gpkg_metadata, references may point at the old table name
    # (this table is optional in the spec, so guard it)
    if table_exists(conn, "gpkg_metadata_reference"):
        conn.execute(
            "UPDATE gpkg_metadata_reference SET table_name = ? WHERE table_name = ?",
            (new_table, old_table),
        )

    # Optional tables some tools create/use
    if table_exists(conn, "gpkg_data_columns"):
        conn.execute(
            "UPDATE gpkg_data_columns SET table_name = ? WHERE table_name = ?",
            (new_table, old_table),
        )
    if table_exists(conn, "gpkg_extensions"):
        conn.execute(
            "UPDATE gpkg_extensions SET table_name = ? WHERE table_name = ?",
            (new_table, old_table),
        )


def rename_single_feature_table(conn: sqlite3.Connection, new_table: str) -> tuple[str, str]:
    """
    Find the single feature table in the GeoPackage and rename it to new_table.
    Returns (old_table, new_table).
    """
    cur = conn.execute(
        "SELECT table_name FROM gpkg_contents WHERE data_type = 'features' ORDER BY table_name"
    )
    tables = [r[0] for r in cur.fetchall()]

    if len(tables) != 1:
        raise RuntimeError(
            f"Expected exactly 1 feature table, found {len(tables)}: {tables}"
        )

    old_table = tables[0]
    if old_table == new_table:
        return old_table, new_table

    if table_exists(conn, new_table):
        raise RuntimeError(f"Target table name already exists inside gpkg: {new_table}")

    # Rename the feature table itself
    conn.execute(f'ALTER TABLE "{old_table}" RENAME TO "{new_table}"')

    # Rename spatial index tables if present (RTree)
    rename_spatial_index_if_present(conn, old_table, new_table)

    # Update gpkg_* tables that reference it
    rename_gpkg_internal_tables(conn, old_table, new_table)

    return old_table, new_table


def process_one(gpkg_path: Path, new_base: str, make_backup: bool = True) -> Path:
    """
    Rename file to new_base.gpkg and rename internal feature table to new_base.
    Returns the new gpkg path.
    """
    if not gpkg_path.exists():
        raise FileNotFoundError(gpkg_path)

    new_gpkg_path = gpkg_path.with_name(f"{new_base}{gpkg_path.suffix}")

    # Backup first (highly recommended)
    if make_backup:
        backup_path = gpkg_path.with_suffix(gpkg_path.suffix + ".bak")
        shutil.copy2(gpkg_path, backup_path)

    # Rename file on disk
    if gpkg_path.name != new_gpkg_path.name:
        if new_gpkg_path.exists():
            raise FileExistsError(new_gpkg_path)
        gpkg_path.rename(new_gpkg_path)

    # Rename table inside
    conn = sqlite3.connect(str(new_gpkg_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")  # gpkg does not rely on FK constraints for this
        conn.execute("BEGIN")
        old_table, _ = rename_single_feature_table(conn, new_base)
        conn.execute("COMMIT")
        print(f"OK: {new_gpkg_path.name} | {old_table} -> {new_base}")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    return new_gpkg_path


def build_gpkg_lookup(gpkg_paths: list[Path]) -> dict[str, Path]:
    """
    Build a lookup keyed by file stem (basename without suffix) for manually listed paths.
    """
    lookup: dict[str, Path] = {}
    for p in gpkg_paths:
        key = p.stem
        if key in lookup:
            raise ValueError(f"Duplicate gpkg stem in manual list: {key}")
        lookup[key] = p
    return lookup


def main() -> None:
    csv_path = Path("rename_map_3.csv")  # change this if needed

    # Optional: manually list full paths to gpkg files (useful if they live in different folders)
    gpkg_paths: list[Path] = [
        # Path("/full/path/to/one.gpkg"),
        # Path("/full/path/to/another.gpkg"),
    ]

    if gpkg_paths:
        gpkg_lookup = build_gpkg_lookup(gpkg_paths)
        gpkg_dir = None
    else:
        gpkg_lookup = None
        gpkg_dir = Path("30g-02/data")  # change this if you want to use a directory

    processed = 0
    skipped = 0
    failed = 0

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"CSV has no header row: {csv_path.resolve()}")

        normalized_headers = [(name or "").strip().lower() for name in reader.fieldnames]
        missing = [name for name in ("old_name", "new_name") if name not in normalized_headers]
        if missing:
            raise KeyError(
                "Missing required CSV columns "
                f"{missing}. Found headers: {reader.fieldnames} in {csv_path.resolve()}"
            )

        for row in reader:
            row_normalized = {
                (key or "").strip().lower(): (value or "")
                for key, value in row.items()
            }
            old_base = row_normalized["old_name"].strip()
            new_base = row_normalized["new_name"].strip()

            try:
                if gpkg_lookup is None:
                    gpkg_path = gpkg_dir / f"{old_base}.gpkg"
                    already_renamed_path = gpkg_dir / f"{new_base}.gpkg"
                    if not gpkg_path.exists() and already_renamed_path.exists():
                        print(f"SKIP already renamed: {old_base} -> {new_base}")
                        skipped += 1
                        continue
                else:
                    gpkg_path = gpkg_lookup.get(old_base)
                    if gpkg_path is None:
                        raise FileNotFoundError(
                            f"No gpkg path provided for old_name '{old_base}'"
                        )

                process_one(gpkg_path, new_base, make_backup=True)
                processed += 1
            except Exception as exc:
                print(f"ERROR: {old_base} -> {new_base}: {exc}")
                failed += 1

    print(f"Summary: processed={processed} skipped={skipped} failed={failed}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
