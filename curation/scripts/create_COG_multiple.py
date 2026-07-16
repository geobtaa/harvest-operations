"""Batch-generate COGs from a directory of GeoTIFFs.

This does not affect the bands or color profile. The output COG may be black.
"""

import os
import subprocess


def reproject_and_convert_to_cog(
    geotiff_path,
    temp_reprojected_path,
    cog_path,
    target_srs="EPSG:4326",
):
    """Reproject a GeoTIFF and convert it to a Cloud Optimized GeoTIFF."""
    try:
        subprocess.run(
            [
                "gdalwarp",
                "-t_srs",
                target_srs,
                "-of",
                "GTiff",
                geotiff_path,
                temp_reprojected_path,
            ],
            check=True,
        )
        print(f"Reprojected GeoTIFF saved at {temp_reprojected_path}")

        subprocess.run(
            [
                "gdal_translate",
                "-of",
                "COG",
                "-co",
                "COMPRESS=LZW",
                temp_reprojected_path,
                cog_path,
            ],
            check=True,
        )
        print(f"COG created at {cog_path}")

        os.remove(temp_reprojected_path)
    except subprocess.CalledProcessError as exc:
        print(f"Error processing {geotiff_path}: {exc}")


def create_cogs_with_reprojection(
    geotiff_dir,
    cog_dir,
    target_srs="EPSG:4326",
):
    """Scan a directory for GeoTIFFs, reproject them, and generate COGs."""
    os.makedirs(cog_dir, exist_ok=True)

    geotiff_files = []
    for root, _, files in os.walk(geotiff_dir):
        for filename in files:
            if filename.endswith(".tif"):
                geotiff_files.append(os.path.join(root, filename))

    print(f"Found {len(geotiff_files)} GeoTIFF files to process.")

    for geotiff_path in geotiff_files:
        base_name = os.path.splitext(os.path.basename(geotiff_path))[0]
        temp_reprojected_path = os.path.join(
            cog_dir, f"{base_name}_reprojected.tif"
        )
        cog_path = os.path.join(cog_dir, f"{base_name}_cog.tif")

        reproject_and_convert_to_cog(
            geotiff_path,
            temp_reprojected_path,
            cog_path,
            target_srs,
        )


def main():
    geotiff_directory = "purdue-campus"
    cog_directory = "purduecogs"
    target_crs = "EPSG:3857"

    create_cogs_with_reprojection(
        geotiff_directory,
        cog_directory,
        target_crs,
    )


if __name__ == "__main__":
    main()
