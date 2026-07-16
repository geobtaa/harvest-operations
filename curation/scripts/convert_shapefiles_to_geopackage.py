"""Convert a folder of shapefiles into GeoPackages."""

from __future__ import annotations

import os

import geopandas as gpd

INPUT_DIR = "30g-01"
OUTPUT_DIR = "30g-01-geopackages"


def convert_shapefile(shapefile_path: str, output_path: str) -> None:
    """Convert a single shapefile to a GeoPackage."""
    try:
        gdf = gpd.read_file(shapefile_path)
        if os.path.exists(output_path):
            os.remove(output_path)
        gdf.to_file(output_path, driver="GPKG")
        print(f"Wrote {output_path}")
    except Exception as exc:
        print(f"Failed to convert {shapefile_path}: {exc}")


def convert_directory(input_dir: str, output_dir: str) -> None:
    """Convert all shapefiles in a directory tree to GeoPackages."""
    os.makedirs(output_dir, exist_ok=True)

    shapefiles = []
    for root, _, files in os.walk(input_dir):
        for filename in files:
            if filename.lower().endswith(".shp"):
                shapefiles.append(os.path.join(root, filename))

    print(f"Found {len(shapefiles)} shapefiles to process.")

    for shapefile_path in shapefiles:
        base_name = os.path.splitext(os.path.basename(shapefile_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.gpkg")
        convert_shapefile(shapefile_path, output_path)


def main() -> None:
    convert_directory(INPUT_DIR, OUTPUT_DIR)


if __name__ == "__main__":
    main()
