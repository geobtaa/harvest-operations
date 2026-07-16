"""Extract WGS84 bounding boxes from COG GeoTIFFs."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from pathlib import Path
from typing import Any, Iterable


DEFAULT_INPUT_DIR = Path("04d-02-cogs-thumbs")
DEFAULT_OUTPUT_CSV = DEFAULT_INPUT_DIR / "04d-02_bbox_from_cogs.csv"

REPORT_FIELDS = [
    "filename",
    "cog_path",
    "west",
    "south",
    "east",
    "north",
    "bounding_box",
    "legacy_bounding_box",
]


def run_gdalinfo(path: Path) -> dict[str, Any]:
    """Return GDAL JSON metadata for a raster."""
    result = subprocess.run(
        ["gdalinfo", "-json", str(path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "gdalinfo failed")
    metadata = json.loads(result.stdout)
    return metadata[0] if isinstance(metadata, list) else metadata


def iter_positions(coordinates: Any) -> Iterable[tuple[float, float]]:
    """Yield lon/lat pairs from a GeoJSON coordinate array."""
    if (
        isinstance(coordinates, list)
        and len(coordinates) >= 2
        and isinstance(coordinates[0], (int, float))
        and isinstance(coordinates[1], (int, float))
    ):
        yield float(coordinates[0]), float(coordinates[1])
        return
    if isinstance(coordinates, list):
        for item in coordinates:
            yield from iter_positions(item)


def item_id_from_cog(path: Path) -> str:
    """Derive the item ID used by the existing 09d-02 bbox CSV."""
    stem = path.stem.removesuffix("_cog")
    return stem.split("-0001", 1)[0] if "-0001" in stem else stem


def bbox_from_metadata(metadata: dict[str, Any]) -> tuple[float, float, float, float]:
    """Return west, south, east, north decimal-degree bounds."""
    extent = metadata.get("wgs84Extent", {})
    positions = list(iter_positions(extent.get("coordinates")))
    if not positions:
        raise RuntimeError("gdalinfo did not report a WGS84 extent")
    longitudes = [position[0] for position in positions]
    latitudes = [position[1] for position in positions]
    return min(longitudes), min(latitudes), max(longitudes), max(latitudes)


def format_bbox(values: tuple[float, float, float, float], decimals: int) -> str:
    """Return a comma-separated bounding box string."""
    return ",".join(f"{value:.{decimals}f}" for value in values)


def extract_bboxes(input_dir: Path, output_csv: Path, decimals: int) -> None:
    """Write one WGS84 bbox row per COG."""
    cogs = sorted(input_dir.rglob("*_cog.tif"))
    if not cogs:
        raise RuntimeError(f"No COG GeoTIFFs found in {input_dir}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        for cog_path in cogs:
            west, south, east, north = bbox_from_metadata(run_gdalinfo(cog_path))
            writer.writerow(
                {
                    "filename": item_id_from_cog(cog_path),
                    "cog_path": str(cog_path),
                    "west": f"{west:.{decimals}f}",
                    "south": f"{south:.{decimals}f}",
                    "east": f"{east:.{decimals}f}",
                    "north": f"{north:.{decimals}f}",
                    "bounding_box": format_bbox((west, south, east, north), decimals),
                    "legacy_bounding_box": format_bbox(
                        (east, south, west, north),
                        decimals,
                    ),
                }
            )


def bbox_lookup(input_dir: Path, decimals: int) -> dict[str, str]:
    """Return item ID to formatted WGS84 bbox for all COGs under a directory."""
    cogs = sorted(input_dir.rglob("*_cog.tif"))
    if not cogs:
        raise RuntimeError(f"No COG GeoTIFFs found in {input_dir}")

    lookup = {}
    for cog_path in cogs:
        item_id = item_id_from_cog(cog_path)
        bbox = format_bbox(bbox_from_metadata(run_gdalinfo(cog_path)), decimals)
        if item_id in lookup:
            raise RuntimeError(f"Multiple COGs matched item ID {item_id}")
        lookup[item_id] = bbox
    return lookup


def update_metadata_csv(
    metadata_csv: Path,
    input_dir: Path,
    decimals: int,
    key_column: str,
    bbox_column: str,
    clear_missing: bool,
) -> tuple[int, list[str]]:
    """Fill a metadata CSV bbox column from COG bounds."""
    lookup = bbox_lookup(input_dir, decimals)
    with metadata_csv.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames:
            raise RuntimeError(f"{metadata_csv} has no header")
        if key_column not in reader.fieldnames:
            raise RuntimeError(f"{metadata_csv} has no {key_column!r} column")
        if bbox_column not in reader.fieldnames:
            raise RuntimeError(f"{metadata_csv} has no {bbox_column!r} column")
        rows = list(reader)

    updated = 0
    missing = []
    for row in rows:
        key = row.get(key_column, "")
        bbox = lookup.get(key)
        if bbox:
            row[bbox_column] = bbox
            updated += 1
        else:
            if clear_missing:
                row[bbox_column] = ""
            missing.append(key)

    with metadata_csv.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return updated, missing


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", nargs="?", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument(
        "--metadata-csv",
        type=Path,
        help="CSV to update in place with extracted bounding boxes",
    )
    parser.add_argument("--key-column", default="Title")
    parser.add_argument("--bbox-column", default="Bounding Box")
    parser.add_argument(
        "--clear-missing",
        action="store_true",
        help="Blank the bbox column when no matching COG is found",
    )
    parser.add_argument("--decimals", type=int, default=6)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the bbox extractor."""
    args = parse_args(argv)
    if args.decimals < 0:
        raise SystemExit("--decimals must be zero or greater")
    if args.metadata_csv:
        updated, missing = update_metadata_csv(
            args.metadata_csv,
            args.input_dir,
            args.decimals,
            args.key_column,
            args.bbox_column,
            args.clear_missing,
        )
        print(f"Updated {updated} row(s) in {args.metadata_csv}")
        if missing:
            print(f"Missing COG bbox for {len(missing)} row(s): {', '.join(missing)}")
    else:
        extract_bboxes(args.input_dir, args.output_csv, args.decimals)
        print(f"Wrote {args.output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
