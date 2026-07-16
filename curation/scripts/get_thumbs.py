"""Generate thumbnails for vector and raster datasets."""

from __future__ import annotations

import os

import geopandas as gpd
import matplotlib.pyplot as plt
import rasterio
from rasterio.plot import show
from tqdm import tqdm


def create_vector_thumbnail(
    vector_path: str,
    thumbnail_path: str,
    width: int = 2,
    height: int = 2,
    dpi: int = 100,
) -> None:
    """Create a thumbnail for a vector dataset and save it as an image."""
    try:
        gdf = gpd.read_file(vector_path)
        fig, ax = plt.subplots(figsize=(width, height), dpi=dpi)
        gdf.plot(ax=ax)
        ax.axis("off")
        plt.savefig(thumbnail_path, bbox_inches="tight", pad_inches=0)
        plt.close(fig)
        print(f"Vector thumbnail saved at {thumbnail_path}")
    except Exception as exc:
        print(f"Error processing {vector_path}: {exc}")


def create_raster_thumbnail(
    raster_path: str,
    thumbnail_path: str,
    width: int = 2,
    height: int = 2,
    dpi: int = 100,
) -> None:
    """Create a thumbnail for a raster file and save it as an image."""
    try:
        with rasterio.open(raster_path) as src:
            fig, ax = plt.subplots(figsize=(width, height), dpi=dpi)
            show(src, ax=ax)
            ax.axis("off")
            plt.savefig(thumbnail_path, bbox_inches="tight", pad_inches=0)
            plt.close(fig)
            print(f"Raster thumbnail saved at {thumbnail_path}")
    except Exception as exc:
        print(f"Error processing {raster_path}: {exc}")


def create_thumbnails_for_directory(
    data_dir: str,
    thumbnail_dir: str,
    width: int = 2,
    height: int = 2,
    dpi: int = 100,
) -> None:
    """
    Scan a directory for shapefiles, geopackages, and rasters, generating thumbnails for each.
    """
    os.makedirs(thumbnail_dir, exist_ok=True)

    files_to_process = []
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".shp") or file.endswith(".gpkg") or file.endswith(".tif"):
                files_to_process.append(os.path.join(root, file))

    print(f"Found {len(files_to_process)} files to process.")

    for file_path in tqdm(files_to_process, desc="Generating Thumbnails"):
        thumbnail_name = f"{os.path.splitext(os.path.basename(file_path))[0]}.png"
        thumbnail_path = os.path.join(thumbnail_dir, thumbnail_name)

        if file_path.endswith(".shp") or file_path.endswith(".gpkg"):
            create_vector_thumbnail(file_path, thumbnail_path, width, height, dpi)
        elif file_path.endswith(".tif"):
            create_raster_thumbnail(file_path, thumbnail_path, width, height, dpi)


def main() -> None:
    data_directory = "mke-ubl"
    thumbnail_directory = "mke-ubl/thumbnails"
    create_thumbnails_for_directory(data_directory, thumbnail_directory)


if __name__ == "__main__":
    main()
