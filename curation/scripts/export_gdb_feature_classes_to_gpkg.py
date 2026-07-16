#!/usr/bin/env python3
"""Export each FileGDB feature class to its own GeoPackage.

Examples:
  python scripts/export_gdb_feature_classes_to_gpkg.py /path/to/data.gdb --out-dir out/gpkg
  python scripts/export_gdb_feature_classes_to_gpkg.py /path/to/data.gdb --overwrite
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import List


def _sanitize_filename(name: str) -> str:
    safe = name.replace(os.sep, "_")
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", safe)
    return safe.strip("._") or "layer"


def _list_feature_layers(gdb_path: Path) -> List[str]:
    try:
        from osgeo import gdal, ogr  # type: ignore
    except Exception as exc:
        raise SystemExit(
            "GDAL Python bindings are required to list layers. "
            "Install GDAL and ensure `osgeo` is importable."
        ) from exc

    gdal.UseExceptions()
    ogr.UseExceptions()

    ds = gdal.OpenEx(str(gdb_path), gdal.OF_VECTOR)
    if ds is None:
        raise SystemExit(f"Failed to open geodatabase: {gdb_path}")

    layers: List[str] = []
    for idx in range(ds.GetLayerCount()):
        layer = ds.GetLayerByIndex(idx)
        if layer is None:
            continue
        geom_type = layer.GetGeomType()
        if geom_type == ogr.wkbNone:
            continue
        name = layer.GetName()
        if name:
            layers.append(name)
    return layers


def _build_ogr2ogr_command(gdb_path: Path, layer_name: str, output_gpkg: Path, overwrite: bool) -> List[str]:
    escaped_layer_name = layer_name.replace('"', '""')
    sql = f'SELECT * FROM "{escaped_layer_name}"'
    cmd = [
        "ogr2ogr",
        "-f",
        "GPKG",
        str(output_gpkg),
        str(gdb_path),
        "-dialect",
        "OGRSQL",
        "-sql",
        sql,
        "-nln",
        layer_name,
    ]
    if overwrite:
        cmd.insert(1, "-overwrite")
    return cmd


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export each feature class in a FileGDB to a separate GeoPackage."
    )
    parser.add_argument("gdb", help="Path to .gdb directory")
    parser.add_argument(
        "--out-dir",
        help="Output directory for .gpkg files (default: <gdb_name>_gpkg in current directory)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output .gpkg files",
    )
    args = parser.parse_args()

    if shutil.which("ogr2ogr") is None:
        raise SystemExit("`ogr2ogr` is not installed or not on PATH.")

    gdb_path = Path(args.gdb).expanduser().resolve()
    if not gdb_path.exists():
        raise SystemExit(f"GDB path does not exist: {gdb_path}")
    if gdb_path.suffix.lower() != ".gdb":
        raise SystemExit(f"Expected a .gdb directory, got: {gdb_path}")

    out_dir = Path(args.out_dir) if args.out_dir else Path.cwd() / f"{gdb_path.stem}_gpkg"
    out_dir = out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    layer_names = _list_feature_layers(gdb_path)
    if not layer_names:
        print("No feature classes found.")
        return 0

    total = 0
    skipped = 0
    failed = 0

    for layer_name in layer_names:
        output_gpkg = out_dir / f"{_sanitize_filename(layer_name)}.gpkg"

        if output_gpkg.exists() and not args.overwrite:
            print(f"SKIP (exists): {output_gpkg}")
            skipped += 1
            continue

        cmd = _build_ogr2ogr_command(gdb_path, layer_name, output_gpkg, args.overwrite)
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"FAIL: {layer_name} -> {output_gpkg}")
            if result.stderr.strip():
                print(result.stderr.strip())
            failed += 1
            continue

        print(f"OK: {layer_name} -> {output_gpkg}")
        total += 1

    print(f"Done. exported={total} skipped={skipped} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
