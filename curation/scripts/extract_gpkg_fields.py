"""Extract GeoPackage attribute table fields into per-file CSVs."""

from __future__ import annotations

import logging
import os
import sqlite3

import fiona
import geopandas as gpd
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


OUTPUT_COLUMNS = [
    "friendlier_id",
    "field_name",
    "field_type",
    "values",
    "definition",
    "definition_source",
    "parent_field_name",
    "position",
]

INPUT_DIR = "mke-ubl"
OUTPUT_DIR = "mke-ubl/data_dictionaries"


def get_gpkg_identifier(filepath: str) -> str:
    try:
        with sqlite3.connect(filepath) as conn:
            cursor = conn.execute(
                "SELECT identifier FROM gpkg_contents WHERE identifier IS NOT NULL LIMIT 1"
            )
            row = cursor.fetchone()
            return row[0] if row else ""
    except Exception as exc:
        logging.warning("Could not read gpkg_contents in %s: %s", filepath, exc)
        return ""


def extract_gpkg_fields(input_dir: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    for root, _, files in os.walk(input_dir):
        for filename in files:
            if not filename.lower().endswith(".gpkg"):
                continue

            filepath = os.path.join(root, filename)
            output_name = f"{os.path.splitext(filename)[0]}_fields.csv"
            output_path = os.path.join(output_dir, output_name)
            friendlier_id = get_gpkg_identifier(filepath)

            try:
                layers = fiona.listlayers(filepath)
            except Exception as exc:
                logging.warning("Could not list layers for %s: %s", filepath, exc)
                continue

            if not layers:
                logging.warning("No layers found in %s", filepath)
                continue

            field_rows: list[dict[str, str]] = []
            for layer in layers:
                try:
                    gdf = gpd.read_file(filepath, layer=layer)
                except Exception as exc:
                    logging.warning(
                        "Could not read layer %s in %s: %s", layer, filepath, exc
                    )
                    continue

                for column in gdf.columns:
                    field_rows.append(
                        {
                            "friendlier_id": friendlier_id,
                            "field_name": column,
                            "field_type": str(gdf[column].dtype),
                            "values": "",
                            "definition": "",
                            "definition_source": "",
                            "parent_field_name": "",
                            "position": "",
                        }
                    )

            if not field_rows:
                logging.warning("No fields extracted from %s", filepath)
                continue

            field_df = pd.DataFrame(field_rows, columns=OUTPUT_COLUMNS)
            field_df.to_csv(output_path, index=False)
            logging.info("Wrote %s", output_path)


def main() -> None:
    extract_gpkg_fields(INPUT_DIR, OUTPUT_DIR)


if __name__ == "__main__":
    main()
