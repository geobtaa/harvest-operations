"""Create PMTiles from GeoPackages using GDAL."""

from __future__ import annotations

import os
import shutil
import subprocess

INPUT_DIR = "geopackages"
OUTPUT_DIR = "pmtiles"
MIN_ZOOM = 5
MAX_ZOOM = 12
TARGET_SRS = "EPSG:4326"


def convert_geopackage_to_pmtiles(
    geopackage_path: str,
    pmtiles_path: str,
    min_zoom: int,
    max_zoom: int,
    target_srs: str,
) -> None:
    """Convert a GeoPackage to PMTiles with ogr2ogr."""
    try:
        ogr2ogr = shutil.which("ogr2ogr")
        if not ogr2ogr:
            raise FileNotFoundError(
                "ogr2ogr not found in PATH. Install GDAL (e.g. `brew install gdal`) "
                "or ensure ogr2ogr is on your PATH."
            )
        subprocess.run(
            [
                ogr2ogr,
                "-t_srs",
                target_srs,
                "-dsco",
                f"MINZOOM={min_zoom}",
                "-dsco",
                f"MAXZOOM={max_zoom}",
                "-f",
                "PMTiles",
                pmtiles_path,
                geopackage_path,
            ],
            check=True,
        )
        print(f"PMTiles created at {pmtiles_path}")
    except FileNotFoundError as exc:
        print(f"Missing dependency while processing {geopackage_path}: {exc}")
    except subprocess.CalledProcessError as exc:
        print(f"Error processing {geopackage_path}: {exc}")


def create_pmtiles_for_directory(
    geopackage_dir: str,
    pmtiles_dir: str,
    min_zoom: int,
    max_zoom: int,
    target_srs: str,
) -> None:
    """Scan a directory of GeoPackages and generate PMTiles for each."""
    os.makedirs(pmtiles_dir, exist_ok=True)

    geopackages = []
    for root, _, files in os.walk(geopackage_dir):
        for filename in files:
            if filename.lower().endswith(".gpkg"):
                geopackages.append(os.path.join(root, filename))

    print(f"Found {len(geopackages)} GeoPackages to process.")

    for geopackage_path in geopackages:
        pmtiles_name = f"{os.path.splitext(os.path.basename(geopackage_path))[0]}.pmtiles"
        pmtiles_path = os.path.join(pmtiles_dir, pmtiles_name)
        convert_geopackage_to_pmtiles(
            geopackage_path, pmtiles_path, min_zoom, max_zoom, target_srs
        )


def main() -> None:
    create_pmtiles_for_directory(
        INPUT_DIR, OUTPUT_DIR, MIN_ZOOM, MAX_ZOOM, TARGET_SRS
    )


if __name__ == "__main__":
    main()
