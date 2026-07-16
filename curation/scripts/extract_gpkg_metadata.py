"""Extract selected metadata from GeoPackage feature layers."""

from __future__ import annotations

import logging
import os
import sqlite3

import geopandas as gpd
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

INPUT_DIR = "30g-02/data"
OUTPUT_CSV = "30g-02_gpkg_metadata.csv"
DECIMAL_PLACES = 4


def get_gpkg_layers(filepath: str) -> list[str]:
    """Return feature layer names from a GeoPackage."""
    try:
        with sqlite3.connect(filepath) as conn:
            rows = conn.execute(
                """
                SELECT table_name
                FROM gpkg_contents
                WHERE data_type = 'features'
                AND table_name IS NOT NULL
                """
            ).fetchall()
    except Exception as exc:
        logging.warning("Could not list layers for %s: %s", filepath, exc)
        return []

    return [table_name for (table_name,) in rows if table_name]


def format_crs(crs) -> str:
    """Return a readable CRS string."""
    if crs is None:
        return "Unknown"

    try:
        return crs.to_string()
    except Exception:
        return str(crs)


def calculate_bounding_box(gdf: gpd.GeoDataFrame, decimal_places: int = 4) -> str:
    """Return the layer bounds in WGS84 as minx,miny,maxx,maxy."""
    if gdf.empty or gdf.crs is None:
        return "Unknown"

    try:
        bounds = gdf.to_crs(epsg=4326).total_bounds
    except Exception:
        return "Unknown"

    rounded_bounds = [round(coord, decimal_places) for coord in bounds]
    return (
        f"{rounded_bounds[0]},{rounded_bounds[1]},"
        f"{rounded_bounds[2]},{rounded_bounds[3]}"
    )


def process_geometry_type(gdf: gpd.GeoDataFrame) -> str:
    """Return a simplified geometry type label for a layer."""
    if gdf.empty or gdf.geometry.is_empty.all():
        return "Unknown"

    try:
        geometry_types = gdf.geom_type.dropna().unique()
    except Exception:
        return "Unknown"

    if len(geometry_types) == 0:
        return "Unknown"
    if len(geometry_types) > 1:
        return "Mixed geometries"

    return (
        geometry_types[0]
        .replace("LineString", "Line")
        .replace("MultiPolygon", "Polygon")
    )


def extract_gpkg_metadata(input_dir: str, output_csv: str) -> None:
    rows: list[dict[str, str]] = []

    for root, _, files in os.walk(input_dir):
        for filename in files:
            if not filename.lower().endswith(".gpkg"):
                continue

            filepath = os.path.join(root, filename)
            folder_name = os.path.basename(os.path.dirname(filepath))
            layers = get_gpkg_layers(filepath)

            if not layers:
                logging.warning("No feature layers found in %s", filepath)
                continue

            for layer_name in layers:
                try:
                    gdf = gpd.read_file(filepath, layer=layer_name)
                except Exception as exc:
                    logging.warning(
                        "Could not read layer %s in %s: %s", layer_name, filepath, exc
                    )
                    continue

                rows.append(
                    {
                        "folder_name": folder_name,
                        "filename": f"{filename}:{layer_name}",
                        "crs": format_crs(gdf.crs),
                        "file_format": "GeoPackage",
                        "geometry_type": process_geometry_type(gdf),
                        "bounding_box": calculate_bounding_box(gdf, DECIMAL_PLACES),
                    }
                )

    metadata_df = pd.DataFrame(
        rows,
        columns=[
            "folder_name",
            "filename",
            "crs",
            "file_format",
            "geometry_type",
            "bounding_box",
        ],
    )
    metadata_df.to_csv(output_csv, index=False)
    logging.info("Wrote %s", output_csv)


def main() -> None:
    extract_gpkg_metadata(INPUT_DIR, OUTPUT_CSV)


if __name__ == "__main__":
    main()
