"""Create PMTiles from shapefiles using GDAL."""

from __future__ import annotations

import os
import subprocess


def reproject_shapefile(
    input_path: str, output_path: str, target_srs: str = "EPSG:4326"
) -> None:
    """
    Reproject a shapefile to a specified spatial reference system.
    """
    try:
        subprocess.run(
            [
                "ogr2ogr",
                "-t_srs",
                target_srs,
                "-f",
                "ESRI Shapefile",
                output_path,
                input_path,
            ],
            check=True,
        )
        print(f"Reprojected shapefile saved at {output_path}")
    except subprocess.CalledProcessError as exc:
        print(f"Error reprojecting shapefile {input_path}: {exc}")


def convert_shapefile_to_pmtiles(
    shapefile_path: str, pmtiles_path: str, min_zoom: int = 10, max_zoom: int = 15
) -> None:
    """
    Convert a shapefile to PMTiles format using GDAL's ogr2ogr.
    """
    try:
        subprocess.run(
            [
                "ogr2ogr",
                "-dsco",
                f"MINZOOM={min_zoom}",
                "-dsco",
                f"MAXZOOM={max_zoom}",
                "-f",
                "PMTiles",
                pmtiles_path,
                shapefile_path,
            ],
            check=True,
        )
        print(f"PMTiles created at {pmtiles_path}")
    except subprocess.CalledProcessError as exc:
        print(f"Error processing {shapefile_path}: {exc}")


def create_pmtiles_for_directory(
    shapefile_dir: str, pmtiles_dir: str, min_zoom: int = 5, max_zoom: int = 12
) -> None:
    """
    Scan a directory of shapefiles and generate PMTiles for each.
    """
    os.makedirs(pmtiles_dir, exist_ok=True)

    shapefiles = []
    for root, _, files in os.walk(shapefile_dir):
        for file in files:
            if file.endswith(".shp"):
                shapefiles.append(os.path.join(root, file))

    print(f"Found {len(shapefiles)} shapefiles to process.")

    for shapefile_path in shapefiles:
        reprojected_shapefile_path = os.path.join(
            pmtiles_dir, f"reprojected_{os.path.basename(shapefile_path)}"
        )
        pmtiles_name = f"{os.path.splitext(os.path.basename(shapefile_path))[0]}.pmtiles"
        pmtiles_path = os.path.join(pmtiles_dir, pmtiles_name)

        reproject_shapefile(shapefile_path, reprojected_shapefile_path)
        convert_shapefile_to_pmtiles(
            reprojected_shapefile_path, pmtiles_path, min_zoom, max_zoom
        )

        os.remove(reprojected_shapefile_path)
        print(f"Deleted temporary file {reprojected_shapefile_path}")


def main() -> None:
    shapefile_directory = "gpkg"
    pmtiles_directory = "pmtiles"
    create_pmtiles_for_directory(shapefile_directory, pmtiles_directory)


if __name__ == "__main__":
    main()
