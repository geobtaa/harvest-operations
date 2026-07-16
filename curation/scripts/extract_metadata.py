"""Extract metadata from geospatial datasets."""

from __future__ import annotations

import logging
import os
import sqlite3

import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.warp import transform_bounds
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import transform

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Define a mapping from variable names to desired column headers
column_mapping = {
    "folder_name": "Folder Name",
    "filename": "File Name",
    "crs": "Coordinate Reference System",
    "file_format": "Format",
    "spatial_resolution": "Spatial Resolution",
    "geometry_type": "Resource Type",
    "bounding_box": "Bounding Box",
    "wkt_outline": "Geometry",
    "folder_size": "File Size",
}

# Define global variables for the script
root_directory = "mpls2015"
output_csv = "mpls.csv"
decimal_places = 3

# Turn calculation of the Geometry (WKT Outline) to True or False.
# Complex shapes will have too many vertices to be useful.
simplify_tolerance = 50
include_wkt = True

# Define the output directory for the attribute table CSV files
output_directory = "data_dictionaries"


def get_folder_size(folder_path: str, unit: str = "MB", decimal_places: int = 3) -> float:
    """
    Calculate the total size of all files in a folder and return it in the specified unit.
    """
    total_size = 0
    for dirpath, _, filenames in os.walk(folder_path):
        for filename in filenames:
            fp = os.path.join(dirpath, filename)
            if os.path.isfile(fp):
                total_size += os.path.getsize(fp)

    if unit == "KB":
        total_size /= 1024
    elif unit == "MB":
        total_size /= 1024 * 1024

    return round(total_size, decimal_places)


def process_geometry_type(data, is_raster: bool = False) -> str:
    """
    Determine the geometry type of a GeoDataFrame or indicate if the dataset is a raster.
    """
    if is_raster:
        return "Raster data"

    if data.empty or data.geometry.is_empty.all():
        return "Unknown"

    try:
        geometry_types = data.geom_type.unique()
        if len(geometry_types) == 1:
            geometry_type = geometry_types[0].replace("LineString", "Line").replace(
                "MultiPolygon", "Polygon"
            )
        else:
            geometry_type = "Mixed geometries"

        return f"{geometry_type} data"
    except Exception as exc:
        print(f"Failed to determine geometry type: {exc}")
        return "Unknown"


def format_crs_uri(crs_string: str | None) -> str | None:
    if crs_string and crs_string.startswith("EPSG:"):
        epsg_code = crs_string.split(":")[1]
        return f"https://epsg.io/{epsg_code}"
    return crs_string


def round_coordinates(geometry, decimal_places: int = 2):
    """
    Round the coordinates of a geometry to the specified number of decimal places.
    """
    if geometry.is_empty:
        return geometry

    def rounder(x, y, z=None):
        if z is None:
            return (round(x, decimal_places), round(y, decimal_places))
        return (round(x, decimal_places), round(y, decimal_places), round(z, decimal_places))

    return transform(rounder, geometry)


def calculate_bounding_box(gdf, decimal_places: int = 4) -> str:
    """
    Calculate and format the bounding box for a GeoDataFrame in WGS84 (EPSG:4326).
    """
    if gdf.empty or gdf.crs is None:
        return "Unknown"

    try:
        gdf = gdf.to_crs(epsg=4326)
        bounds = gdf.total_bounds
        rounded_bounds = [round(coord, decimal_places) for coord in bounds]
        return (
            f"{rounded_bounds[0]},{rounded_bounds[1]},"
            f"{rounded_bounds[2]},{rounded_bounds[3]}"
        )
    except Exception:
        return "Unknown"


def calculate_bounding_box_raster(src, decimal_places: int = 4) -> tuple[str, str]:
    """
    Calculate the bounding box and WKT outline for a raster file in WGS84 (EPSG:4326).
    """
    if src.crs is None:
        return "Unknown", "None"

    try:
        left, bottom, right, top = src.bounds
        if src.crs.to_string() != "EPSG:4326":
            left, bottom, right, top = transform_bounds(
                src.crs, "EPSG:4326", left, bottom, right, top
            )

        rounded_bounds = [round(coord, decimal_places) for coord in [left, bottom, right, top]]
        bbox_str = (
            f"{rounded_bounds[0]},{rounded_bounds[1]},"
            f"{rounded_bounds[2]},{rounded_bounds[3]}"
        )

        wkt_outline = (
            f"POLYGON(({rounded_bounds[0]} {rounded_bounds[1]}, "
            f"{rounded_bounds[0]} {rounded_bounds[3]}, "
            f"{rounded_bounds[2]} {rounded_bounds[3]}, "
            f"{rounded_bounds[2]} {rounded_bounds[1]}, "
            f"{rounded_bounds[0]} {rounded_bounds[1]}))"
        )

        return bbox_str, wkt_outline
    except Exception as exc:
        print(f"Failed to calculate bounding box and WKT outline: {exc}")
        return "Unknown", "None"


def generate_wkt_outline(gdf, decimal_places: int = 2) -> str:
    """
    Generate a WKT representation of a generalized outline for the dataset.
    """
    if gdf.empty or gdf.crs is None:
        return "missing CRS"

    try:
        global simplify_tolerance

        gdf = gdf.to_crs(epsg=4326)
        logging.info("Converted GeoDataFrame to EPSG:4326.")

        unified_geom = gdf.geometry.union_all()
        logging.info("Unified geometry type: %s", type(unified_geom))

        num_vertices_before = count_vertices(unified_geom)
        logging.info("Number of vertices before simplification: %s", num_vertices_before)

        if simplify_tolerance is not None:
            generalized_outline = unified_geom.simplify(
                simplify_tolerance, preserve_topology=True
            )
            logging.info("Simplified geometry with tolerance %s.", simplify_tolerance)
        else:
            generalized_outline = unified_geom

        num_vertices_after = count_vertices(generalized_outline)
        logging.info("Number of vertices after simplification: %s", num_vertices_after)

        generalized_outline = round_coordinates(generalized_outline, decimal_places)
        logging.info("Rounded coordinates of the generalized outline.")

        if isinstance(generalized_outline, (Polygon, MultiPolygon)):
            wkt_outline = generalized_outline.wkt
            logging.info("Generated WKT outline.")
        else:
            logging.warning("Generalized outline is not a Polygon or MultiPolygon.")
            return ""

        return wkt_outline
    except Exception as exc:
        logging.error("Failed to generate WKT outline: %s", exc)
        return ""


def count_vertices(geometry) -> int:
    """
    Count the number of vertices in a geometry.
    """
    if geometry.is_empty:
        return 0
    if isinstance(geometry, Polygon):
        return len(geometry.exterior.coords)
    if isinstance(geometry, MultiPolygon):
        return sum(len(polygon.exterior.coords) for polygon in geometry.geoms)
    return 0


def sanitize_name(value: str) -> str:
    """Return a filesystem-safe name fragment."""
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in value)


def get_gpkg_layers(filepath: str) -> list[str]:
    """Return the list of vector layers in a GeoPackage."""
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
        logging.error("Could not list layers for GeoPackage %s: %s", filepath, exc)
        return []

    return [table_name for (table_name,) in rows if table_name]


def get_gpkg_layer_identifiers(filepath: str) -> dict[str, str]:
    """Return GeoPackage table_name -> identifier values from gpkg_contents."""
    try:
        with sqlite3.connect(filepath) as conn:
            rows = conn.execute(
                """
                SELECT table_name, identifier
                FROM gpkg_contents
                WHERE table_name IS NOT NULL
                """
            ).fetchall()
    except Exception as exc:
        logging.warning("Could not read gpkg_contents in %s: %s", filepath, exc)
        return {}

    return {
        table_name: identifier
        for table_name, identifier in rows
        if table_name and identifier
    }


def build_gpkg_display_name(filename: str, layer_name: str) -> str:
    """Return a readable label for a GeoPackage layer."""
    return f"{filename}:{layer_name}"


def process_vector(
    filepath: str,
    filename: str,
    file_format: str,
    folder_name: str,
    folder_size: float,
    metadata: dict,
    decimal_places: int,
    layer_name: str | None = None,
    display_name: str | None = None,
) -> None:
    global include_wkt
    try:
        source_name = display_name or filename
        logging.info("Processing vector file %s", source_name)
        read_kwargs = {"layer": layer_name} if layer_name is not None else {}
        gdf = gpd.read_file(filepath, **read_kwargs)

        if gdf.crs is None:
            logging.warning(
                "Dataset %s has no CRS. Spatial calculations may be inaccurate.",
                source_name,
            )
            gdf.crs = "EPSG:26916"
            crs_uri = format_crs_uri(gdf.crs)
            logging.info("Assigned CRS %s to dataset %s", gdf.crs, source_name)
        else:
            original_crs = gdf.crs.to_string()
            crs_uri = format_crs_uri(original_crs)

        bbox = calculate_bounding_box(gdf, decimal_places)
        if include_wkt:
            wkt_outline = generate_wkt_outline(gdf, decimal_places)
        else:
            wkt_outline = ""
        geometry_type = process_geometry_type(gdf)

        metadata["filename"].append(source_name)
        metadata["folder_name"].append(folder_name)
        metadata["crs"].append(crs_uri)
        metadata["file_format"].append(file_format)
        metadata["geometry_type"].append(geometry_type)
        metadata["bounding_box"].append(bbox)
        metadata["spatial_resolution"].append("")
        metadata["folder_size"].append(f"{folder_size} MB")
        metadata["wkt_outline"].append(wkt_outline)
    except Exception as exc:
        source_name = display_name or filename
        logging.error("Could not process vector file %s: %s", source_name, exc)
        append_empty_metadata(metadata, source_name, folder_name, file_format, folder_size)


def process_raster(
    filepath: str,
    filename: str,
    folder_name: str,
    folder_size: float,
    metadata: dict,
    decimal_places: int,
) -> None:
    global include_wkt
    try:
        with rasterio.open(filepath) as src:
            if src.crs is None:
                logging.warning(
                    "Raster dataset %s has no CRS. Spatial calculations may be inaccurate.",
                    filename,
                )
                crs_uri = "Unknown"
            else:
                original_crs = src.crs.to_string()
                crs_uri = format_crs_uri(original_crs)

            pixel_size_x, pixel_size_y = src.res
            spatial_resolution = round((abs(pixel_size_x) + abs(pixel_size_y)) / 2, 2)
            bbox, wkt_outline = calculate_bounding_box_raster(src, decimal_places)

            metadata["filename"].append(filename)
            metadata["folder_name"].append(folder_name)
            metadata["crs"].append(crs_uri)
            metadata["file_format"].append("GeoTIFF")
            metadata["geometry_type"].append("Raster data")
            metadata["bounding_box"].append(bbox)
            metadata["spatial_resolution"].append(spatial_resolution)
            metadata["folder_size"].append(f"{folder_size} MB")
            metadata["wkt_outline"].append(wkt_outline if include_wkt else None)

    except Exception as exc:
        logging.error("Could not read raster file %s: %s", filename, exc)


def process_geodatabase(
    root: str, folder_name: str, folder_size: float, metadata: dict
) -> None:
    """
    Process a geodatabase to extract metadata.
    """
    geodatabase_name = os.path.basename(root)

    metadata["filename"].append(geodatabase_name)
    metadata["folder_name"].append(folder_name)
    metadata["file_format"].append("Geodatabase")
    metadata["folder_size"].append(f"{folder_size} MB")

    metadata["crs"].append("")
    metadata["geometry_type"].append("")
    metadata["bounding_box"].append("")
    metadata["spatial_resolution"].append("")
    metadata["wkt_outline"].append("")


def append_empty_metadata(
    metadata: dict, filename: str, folder_name: str, file_format: str, folder_size: float
) -> None:
    metadata["filename"].append(filename)
    metadata["folder_name"].append(folder_name)
    metadata["crs"].append("")
    metadata["file_format"].append(file_format)
    metadata["geometry_type"].append("")
    metadata["bounding_box"].append("")
    metadata["spatial_resolution"].append("")
    metadata["folder_size"].append(f"{folder_size} MB")
    metadata["wkt_outline"].append("")


def extract_metadata() -> None:
    """
    Extract metadata from geospatial datasets in a directory.
    """
    metadata = {
        "filename": [],
        "folder_name": [],
        "crs": [],
        "file_format": [],
        "geometry_type": [],
        "bounding_box": [],
        "spatial_resolution": [],
        "folder_size": [],
        "wkt_outline": [],
    }

    vector_formats = {
        ".shp": "Shapefile",
        ".geojson": "GeoJSON",
    }

    for root, dirs, files in os.walk(root_directory):
        gdb_dirs = [dir_name for dir_name in dirs if dir_name.endswith(".gdb")]
        for dir_name in gdb_dirs:
            gdb_path = os.path.join(root, dir_name)
            folder_name = os.path.basename(os.path.dirname(gdb_path))
            folder_size = get_folder_size(gdb_path, unit="MB")
            process_geodatabase(gdb_path, folder_name, folder_size, metadata)
        # Prevent walking into geodatabases so we only record top-level metadata.
        dirs[:] = [dir_name for dir_name in dirs if not dir_name.endswith(".gdb")]
        for filename in files:
            file_ext = os.path.splitext(filename)[1].lower()
            filepath = os.path.join(root, filename)
            folder_name = os.path.basename(os.path.dirname(filepath))
            folder_size = get_folder_size(os.path.dirname(filepath), unit="MB")

            if file_ext in vector_formats:
                process_vector(
                    filepath,
                    filename,
                    vector_formats[file_ext],
                    folder_name,
                    folder_size,
                    metadata,
                    decimal_places,
                )
            elif file_ext == ".tif":
                process_raster(
                    filepath, filename, folder_name, folder_size, metadata, decimal_places
                )
            elif file_ext == ".gpkg":
                layers = get_gpkg_layers(filepath)
                if not layers:
                    append_empty_metadata(
                        metadata, filename, folder_name, "GeoPackage", folder_size
                    )
                    continue

                for layer_name in layers:
                    process_vector(
                        filepath,
                        filename,
                        "GeoPackage",
                        folder_name,
                        folder_size,
                        metadata,
                        decimal_places,
                        layer_name=layer_name,
                        display_name=build_gpkg_display_name(filename, layer_name),
                    )

    df = pd.DataFrame(metadata)
    df.rename(columns=column_mapping, inplace=True)

    output_csv_path = os.path.join(root_directory, output_csv)
    df.to_csv(output_csv_path, index=False)
    print(f"Metadata extraction complete. CSV saved to {output_csv_path}")


def extract_attribute_table_info(root_directory: str, output_dir: str) -> None:
    vector_formats = {
        ".shp": "Shapefile",
        ".geojson": "GeoJSON",
    }

    os.makedirs(output_dir, exist_ok=True)

    for root, _, files in os.walk(root_directory):
        for filename in files:
            file_ext = os.path.splitext(filename)[1].lower()
            filepath = os.path.join(root, filename)

            if file_ext in vector_formats:
                try:
                    gdf = gpd.read_file(filepath)
                    field_info = []
                    for column in gdf.columns:
                        field_info.append(
                            {
                                "friendlier_id": filename,
                                "field_name": column,
                                "field_type": str(gdf[column].dtype),
                                "values": "",
                                "definition": "",
                                "definition_source": "",
                            }
                        )

                    field_df = pd.DataFrame(field_info)
                    output_csv_name = f"{os.path.splitext(filename)[0]}_fields.csv"
                    output_csv_path = os.path.join(output_dir, output_csv_name)
                    field_df.to_csv(output_csv_path, index=False)
                    print(
                        f"Field information extracted for {filename}. CSV saved to {output_csv_path}"
                    )
                except Exception as exc:
                    print(f"Could not read {filename}: {exc}")
            elif file_ext == ".gpkg":
                layer_identifiers = get_gpkg_layer_identifiers(filepath)
                layers = get_gpkg_layers(filepath)

                for layer_name in layers:
                    try:
                        gdf = gpd.read_file(filepath, layer=layer_name)
                        friendlier_id = layer_identifiers.get(
                            layer_name, build_gpkg_display_name(filename, layer_name)
                        )
                        field_info = []
                        for column in gdf.columns:
                            field_info.append(
                                {
                                    "friendlier_id": friendlier_id,
                                    "field_name": column,
                                    "field_type": str(gdf[column].dtype),
                                    "values": "",
                                    "definition": "",
                                    "definition_source": "",
                                }
                            )

                        field_df = pd.DataFrame(field_info)
                        output_csv_name = (
                            f"{os.path.splitext(filename)[0]}_{sanitize_name(layer_name)}_fields.csv"
                        )
                        output_csv_path = os.path.join(output_dir, output_csv_name)
                        field_df.to_csv(output_csv_path, index=False)
                        print(
                            "Field information extracted for "
                            f"{filename} layer {layer_name}. CSV saved to {output_csv_path}"
                        )
                    except Exception as exc:
                        print(f"Could not read {filename} layer {layer_name}: {exc}")


def main() -> None:
    extract_metadata()
    extract_attribute_table_info(root_directory, output_directory)


if __name__ == "__main__":
    main()
