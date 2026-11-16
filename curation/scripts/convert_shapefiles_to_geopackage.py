"""Recursively convert shapefiles to GeoPackages for the PMTiles workflow."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def convert_shapefile(
    shapefile_path: Path,
    output_path: Path,
    *,
    ogr2ogr: str,
) -> None:
    """Convert through a temporary file so failures preserve existing outputs."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".shapefile-", dir=output_path.parent
    ) as work:
        temporary_path = Path(work) / output_path.name
        result = subprocess.run(
            [
                ogr2ogr,
                "-f",
                "GPKG",
                str(temporary_path),
                str(shapefile_path),
                "-nln",
                shapefile_path.stem,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(
                result.stderr.strip() or f"ogr2ogr exited with code {result.returncode}"
            )
        if result.stderr.strip():
            print(result.stderr.strip(), file=sys.stderr)
        if not temporary_path.is_file() or temporary_path.stat().st_size == 0:
            raise RuntimeError("ogr2ogr did not produce a nonempty GeoPackage")
        temporary_path.replace(output_path)


def convert_directory(
    input_dir: Path,
    output_dir: Path,
    *,
    ogr2ogr: str,
    overwrite: bool = False,
    skip_existing: bool = False,
) -> int:
    """Convert every shapefile, preserving relative folders and reporting failures."""
    shapefiles = sorted(
        path
        for path in input_dir.rglob("*")
        if path.is_file() and path.suffix.lower() == ".shp"
    )
    print(f"Found {len(shapefiles)} shapefile(s).")
    if not shapefiles:
        print(f"No shapefiles found in {input_dir}", file=sys.stderr)
        return 1

    succeeded = skipped = failed = 0
    for source in shapefiles:
        destination = output_dir / source.relative_to(input_dir).with_suffix(".gpkg")
        if destination.exists() and not overwrite:
            if (
                skip_existing
                and destination.is_file()
                and destination.stat().st_size > 0
            ):
                print(f"Skipped existing {destination}")
                skipped += 1
                continue
            print(
                f"Failed {source}: output exists: {destination}; "
                "use --skip-existing for nonempty files or --overwrite to replace",
                file=sys.stderr,
            )
            failed += 1
            continue
        try:
            convert_shapefile(source, destination, ogr2ogr=ogr2ogr)
        except (OSError, RuntimeError) as exc:
            print(f"Failed {source}: {exc}", file=sys.stderr)
            failed += 1
        else:
            print(f"Wrote {destination}")
            succeeded += 1

    print(f"Summary: succeeded={succeeded} skipped={skipped} failed={failed}")
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    existing = parser.add_mutually_exclusive_group()
    existing.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip existing nonempty GeoPackages (without validating them).",
    )
    existing.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing GeoPackages only after successful conversion.",
    )
    args = parser.parse_args(argv)
    if not args.input_dir.is_dir():
        parser.error(f"--input-dir is not a directory: {args.input_dir}")
    if args.output_dir.exists() and not args.output_dir.is_dir():
        parser.error(f"--output-dir is not a directory: {args.output_dir}")
    ogr2ogr = shutil.which("ogr2ogr")
    if not ogr2ogr:
        parser.error("ogr2ogr is required; install GDAL (macOS: brew install gdal)")
    return convert_directory(
        args.input_dir,
        args.output_dir,
        ogr2ogr=ogr2ogr,
        overwrite=args.overwrite,
        skip_existing=args.skip_existing,
    )


if __name__ == "__main__":
    sys.exit(main())
