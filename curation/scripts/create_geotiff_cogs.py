"""Create, validate, and report on Cloud Optimized GeoTIFFs."""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from inventory_geotiffs import discover_geotiffs, run_gdalinfo

DEFAULT_INPUT_DIR = Path("wabash")
DEFAULT_OUTPUT_DIR = Path("wabash-cogs")
DEFAULT_REPORT = Path("wabash-processing-report.csv")
DEFAULT_MAX_PIXELS = 150_000_000
DEFAULT_TARGET_SRS = "EPSG:3857"

REPORT_FIELDS = [
    "source_path",
    "output_path",
    "profile",
    "status",
    "source_width",
    "source_height",
    "output_width",
    "output_height",
    "source_pixels",
    "output_pixels",
    "source_resolution_x",
    "output_resolution_x",
    "source_size_bytes",
    "output_size_bytes",
    "size_reduction_percent",
    "compression",
    "output_type",
    "source_srs",
    "target_srs",
    "output_srs",
    "reprojected",
    "is_cog",
    "warnings",
    "error",
]


def require_command(command: str) -> str:
    """Return an executable path or raise a useful error."""
    executable = shutil.which(command)
    if not executable:
        raise RuntimeError(
            f"{command} was not found in PATH. Install GDAL and try again."
        )
    return executable


def calculate_target_dimensions(
    width: int,
    height: int,
    maximum_pixels: int | None,
) -> tuple[int, int]:
    """Reduce dimensions proportionally when a raster exceeds a pixel limit."""
    pixel_count = width * height
    if not maximum_pixels or pixel_count <= maximum_pixels:
        return width, height

    scale = math.sqrt(maximum_pixels / pixel_count)
    return max(1, math.floor(width * scale)), max(1, math.floor(height * scale))


def profile_options(profile: str, jpeg_quality: int) -> list[str]:
    """Return GDAL COG creation options for a processing profile."""
    common = [
        "-co",
        "BLOCKSIZE=512",
        "-co",
        "NUM_THREADS=ALL_CPUS",
        "-co",
        "BIGTIFF=IF_SAFER",
        "-co",
        "OVERVIEW_RESAMPLING=AVERAGE",
        "-co",
        "STATISTICS=YES",
    ]
    if profile == "archival":
        return common + [
            "-co",
            "COMPRESS=ZSTD",
            "-co",
            "OVERVIEW_COMPRESS=ZSTD",
            "-co",
            "LEVEL=9",
            "-co",
            "PREDICTOR=YES",
            "-co",
            "OVERVIEW_PREDICTOR=YES",
        ]
    if profile == "compatible":
        return common + [
            "-co",
            "COMPRESS=LZW",
            "-co",
            "OVERVIEW_COMPRESS=LZW",
            "-co",
            "PREDICTOR=YES",
            "-co",
            "OVERVIEW_PREDICTOR=YES",
        ]
    return common + [
        "-co",
        "COMPRESS=JPEG",
        "-co",
        "OVERVIEW_COMPRESS=JPEG",
        "-co",
        "INTERLEAVE=BAND",
        "-co",
        f"QUALITY={jpeg_quality}",
        "-co",
        f"OVERVIEW_QUALITY={jpeg_quality}",
    ]


def detected_srs(metadata: dict[str, Any]) -> str | None:
    """Return an EPSG identifier when GDAL resolves one."""
    epsg = metadata.get("stac", {}).get("proj:epsg")
    return f"EPSG:{epsg}" if epsg else None


def jpeg_output_type(metadata: dict[str, Any]) -> str:
    """Use 8-bit output when source statistics show image values fit in a byte."""
    bands = metadata.get("bands", [])
    if not bands:
        raise RuntimeError("Source raster has no bands")
    for band in bands:
        if band.get("type") == "Byte" and (
            band.get("minimum") is None or band.get("maximum") is None
        ):
            continue
        minimum = band.get("minimum")
        maximum = band.get("maximum")
        if minimum is None or maximum is None or minimum < 0 or maximum > 255:
            raise RuntimeError(
                "JPEG profile requires source band values between 0 and 255"
            )
    return "Byte"


def has_out_of_range_nodata(metadata: dict[str, Any], output_type: str) -> bool:
    """Return whether source nodata cannot be represented by the output type."""
    if output_type != "Byte":
        return False
    return any(
        band.get("noDataValue") is not None
        and not 0 <= band["noDataValue"] <= 255
        for band in metadata.get("bands", [])
    )


def needs_rgb_normalization(metadata: dict[str, Any]) -> bool:
    """Return whether a three-band image lacks standard RGB interpretations."""
    interpretations = [
        band.get("colorInterpretation") for band in metadata.get("bands", [])
    ]
    return len(interpretations) == 3 and interpretations != ["Red", "Green", "Blue"]


def create_processing_vrt(
    source_path: Path,
    vrt_path: Path,
    output_type: str,
    ignore_source_nodata: bool,
) -> None:
    """Create a temporary VRT with viewer-compatible band metadata."""
    command = [
        require_command("gdal_translate"),
        "-q",
        "-of",
        "VRT",
        "-ot",
        output_type,
        "-colorinterp",
        "red,green,blue",
    ]
    if ignore_source_nodata:
        command.extend(["-a_nodata", "none"])
    command.extend([str(source_path.resolve()), str(vrt_path)])
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())


def projected_dimensions(source_path: Path, target_srs: str) -> tuple[int, int]:
    """Ask GDAL for the natural square-pixel dimensions after reprojection."""
    with tempfile.TemporaryDirectory(prefix="geotiff-warp-") as temporary_directory:
        vrt_path = Path(temporary_directory) / "projected.vrt"
        result = subprocess.run(
            [
                require_command("gdalwarp"),
                "-q",
                "-of",
                "VRT",
                "-t_srs",
                target_srs,
                str(source_path),
                str(vrt_path),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())
        width, height = run_gdalinfo(vrt_path).get("size", [0, 0])
    return width, height


def build_gdal_command(
    source_path: Path,
    output_path: Path,
    profile: str,
    target_width: int,
    target_height: int,
    natural_width: int,
    natural_height: int,
    target_srs: str | None,
    jpeg_quality: int,
    overwrite: bool,
    output_type: str | None,
    ignore_source_nodata: bool,
) -> list[str]:
    """Build a COG command, using gdalwarp when reprojection is requested."""
    if target_srs:
        command = [
            require_command("gdalwarp"),
            "-of",
            "COG",
            "-t_srs",
            target_srs,
            "-r",
            "average",
            "-multi",
            "-wo",
            "NUM_THREADS=ALL_CPUS",
        ]
        if overwrite:
            command.insert(1, "-overwrite")
        if ignore_source_nodata:
            command.extend(["-srcnodata", "none", "-dstnodata", "none"])
    else:
        command = [
            require_command("gdal_translate"),
            "-of",
            "COG",
            "-r",
            "average",
        ]
        if ignore_source_nodata:
            command.extend(["-a_nodata", "none"])
    if output_type:
        command.extend(["-ot", output_type])
    if (target_width, target_height) != (natural_width, natural_height):
        size_option = "-ts" if target_srs else "-outsize"
        command.extend([size_option, str(target_width), str(target_height)])
    command.extend(profile_options(profile, jpeg_quality))
    command.extend([str(source_path), str(output_path)])
    return command


def validate_cog(
    source_metadata: dict[str, Any],
    output_path: Path,
    target_srs: str | None,
    ignore_source_nodata: bool,
    normalized_rgb: bool,
) -> tuple[dict[str, Any], list[str]]:
    """Validate COG layout and important source/output properties."""
    output_metadata = run_gdalinfo(output_path)
    warnings = []
    image_structure = output_metadata.get("metadata", {}).get("IMAGE_STRUCTURE", {})
    source_bands = source_metadata.get("bands", [])
    output_bands = output_metadata.get("bands", [])

    if image_structure.get("LAYOUT") != "COG":
        warnings.append("output_not_reported_as_cog")
    if image_structure.get("SOURCE_COLOR_SPACE") == "YCbCr":
        warnings.append("ycbcr_jpeg_may_be_incompatible")
    if image_structure.get("COMPRESSION") == "JPEG" and any(
        band.get("type") != "Byte" for band in output_bands
    ):
        warnings.append("jpeg_output_not_byte")
    if len(source_bands) != len(output_bands):
        warnings.append("band_count_changed")
    if not normalized_rgb and [
        band.get("colorInterpretation") for band in source_bands
    ] != [
        band.get("colorInterpretation") for band in output_bands
    ]:
        warnings.append("color_interpretation_changed")
    if not ignore_source_nodata and [
        band.get("noDataValue") for band in source_bands
    ] != [
        band.get("noDataValue") for band in output_bands
    ]:
        warnings.append("nodata_changed")
    if output_bands and not output_bands[0].get("overviews"):
        warnings.append("missing_internal_overviews")
    output_srs = detected_srs(output_metadata)
    if target_srs and output_srs and output_srs.upper() != target_srs.upper():
        warnings.append("output_srs_does_not_match_target")
    if target_srs and not output_srs:
        warnings.append("output_srs_not_resolved_to_epsg")
    return output_metadata, warnings


def resolution_x(metadata: dict[str, Any]) -> float | None:
    """Return the absolute x pixel resolution."""
    transform = metadata.get("geoTransform", [])
    return abs(transform[1]) if len(transform) > 1 else None


def process_geotiff(
    source_path: Path,
    input_directory: Path,
    output_directory: Path,
    profile: str,
    maximum_pixels: int | None,
    target_srs: str | None,
    jpeg_quality: int,
    overwrite: bool,
    dry_run: bool,
) -> dict[str, Any]:
    """Create and validate one COG, returning a processing report record."""
    source_metadata = run_gdalinfo(source_path)
    source_width, source_height = source_metadata.get("size", [0, 0])
    source_srs = detected_srs(source_metadata)
    output_type = (
        jpeg_output_type(source_metadata)
        if profile in {"balanced", "reduced"}
        else None
    )
    source_types = {band.get("type") for band in source_metadata.get("bands", [])}
    nodata_output_type = output_type or (
        next(iter(source_types)) if len(source_types) == 1 else ""
    )
    ignore_source_nodata = has_out_of_range_nodata(
        source_metadata,
        nodata_output_type,
    )
    normalize_rgb = bool(output_type and needs_rgb_normalization(source_metadata))
    natural_width, natural_height = (
        projected_dimensions(source_path, target_srs)
        if target_srs
        else (source_width, source_height)
    )
    effective_maximum = maximum_pixels if profile == "reduced" else None
    target_width, target_height = calculate_target_dimensions(
        natural_width,
        natural_height,
        effective_maximum,
    )
    output_path = output_directory / f"{source_path.stem}_cog.tif"
    command = build_gdal_command(
        source_path,
        output_path,
        profile,
        target_width,
        target_height,
        natural_width,
        natural_height,
        target_srs,
        jpeg_quality,
        overwrite,
        output_type,
        ignore_source_nodata,
    )
    record: dict[str, Any] = {
        "source_path": str(source_path.relative_to(input_directory)),
        "output_path": str(output_path),
        "profile": profile,
        "status": "pending",
        "source_width": source_width,
        "source_height": source_height,
        "output_width": target_width,
        "output_height": target_height,
        "source_pixels": source_width * source_height,
        "output_pixels": target_width * target_height,
        "source_resolution_x": resolution_x(source_metadata),
        "output_resolution_x": "",
        "source_size_bytes": source_path.stat().st_size,
        "output_size_bytes": "",
        "size_reduction_percent": "",
        "compression": {
            "archival": "ZSTD",
            "compatible": "LZW",
        }.get(profile, "JPEG"),
        "output_type": output_type or "source",
        "source_srs": source_srs or "",
        "target_srs": target_srs or "",
        "output_srs": "",
        "reprojected": bool(
            target_srs
            and (not source_srs or source_srs.upper() != target_srs.upper())
        ),
        "is_cog": "",
        "warnings": [],
        "error": "",
        "command": command,
    }

    if output_path.exists() and not overwrite:
        record["status"] = "skipped"
        record["warnings"].append("output_exists")
        return record
    if dry_run:
        record["status"] = "dry_run"
        return record

    output_directory.mkdir(parents=True, exist_ok=True)
    try:
        temporary_directory = None
        if normalize_rgb:
            temporary_directory = tempfile.TemporaryDirectory(prefix="geotiff-rgb-")
            processing_source = Path(temporary_directory.name) / "source.vrt"
            create_processing_vrt(
                source_path,
                processing_source,
                output_type or "Byte",
                ignore_source_nodata,
            )
            command = build_gdal_command(
                processing_source,
                output_path,
                profile,
                target_width,
                target_height,
                natural_width,
                natural_height,
                target_srs,
                jpeg_quality,
                overwrite,
                output_type,
                ignore_source_nodata,
            )
            record["command"] = command
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())

        output_metadata, warnings = validate_cog(
            source_metadata,
            output_path,
            target_srs,
            ignore_source_nodata,
            normalize_rgb,
        )
        output_size = output_path.stat().st_size
        record.update(
            {
                "status": "success" if not warnings else "success_with_warnings",
                "output_width": output_metadata.get("size", [0, 0])[0],
                "output_height": output_metadata.get("size", [0, 0])[1],
                "output_pixels": math.prod(output_metadata.get("size", [0, 0])),
                "output_resolution_x": resolution_x(output_metadata),
                "output_srs": detected_srs(output_metadata) or "",
                "output_size_bytes": output_size,
                "size_reduction_percent": round(
                    100 * (1 - output_size / source_path.stat().st_size), 3
                ),
                "is_cog": "output_not_reported_as_cog" not in warnings,
                "warnings": warnings,
            }
        )
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = str(exc)
        if output_path.exists():
            output_path.unlink()
    finally:
        if "temporary_directory" in locals() and temporary_directory:
            temporary_directory.cleanup()
    return record


def write_reports(records: list[dict[str, Any]], report_path: Path) -> None:
    """Write CSV and JSON processing reports."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        for record in records:
            row = {field: record.get(field, "") for field in REPORT_FIELDS}
            row["warnings"] = "; ".join(row["warnings"])
            writer.writerow(row)

    json_path = report_path.with_suffix(".json")
    with json_path.open("w", encoding="utf-8") as file:
        json.dump(records, file, indent=2, ensure_ascii=False)


def process_directory(
    input_directory: Path,
    output_directory: Path,
    report_path: Path,
    profile: str,
    maximum_pixels: int | None,
    target_srs: str | None,
    jpeg_quality: int,
    overwrite: bool,
    dry_run: bool,
    limit: int | None,
    include: set[str],
) -> list[dict[str, Any]]:
    """Process a directory of source GeoTIFFs and write reports."""
    geotiffs = discover_geotiffs(input_directory)
    if include:
        geotiffs = [
            path
            for path in geotiffs
            if path.stem in include or path.name in include
        ]
    if limit is not None:
        geotiffs = geotiffs[:limit]

    duplicate_stems = {
        path.stem
        for path in geotiffs
        if sum(other.stem == path.stem for other in geotiffs) > 1
    }
    if duplicate_stems:
        raise RuntimeError(
            "Output filename collisions detected for: "
            + ", ".join(sorted(duplicate_stems))
        )

    records = []
    for index, source_path in enumerate(geotiffs, start=1):
        print(f"[{index}/{len(geotiffs)}] Processing {source_path}")
        record = process_geotiff(
            source_path,
            input_directory,
            output_directory,
            profile,
            maximum_pixels,
            target_srs,
            jpeg_quality,
            overwrite,
            dry_run,
        )
        records.append(record)
        print(f"  {record['status']}: {record['output_path']}")

    write_reports(records, report_path)
    return records


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create and validate Cloud Optimized GeoTIFFs."
    )
    parser.add_argument("input_directory", nargs="?", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument(
        "--output-directory", type=Path, default=DEFAULT_OUTPUT_DIR
    )
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument(
        "--profile",
        choices=["archival", "compatible", "balanced", "reduced"],
        default="reduced",
        help=(
            "archival=lossless ZSTD; compatible=lossless LZW; "
            "balanced=JPEG; reduced=JPEG plus pixel limit."
        ),
    )
    parser.add_argument(
        "--max-pixels",
        type=int,
        default=DEFAULT_MAX_PIXELS,
        help="Maximum pixels for the reduced profile.",
    )
    parser.add_argument(
        "--target-srs",
        default=DEFAULT_TARGET_SRS,
        help="CRS to reproject outputs into; use 'none' to preserve source CRS.",
    )
    parser.add_argument("--jpeg-quality", type=int, default=85)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, help="Only process the first N rasters.")
    parser.add_argument(
        "--include",
        nargs="*",
        default=[],
        help="Only process matching raster stems or filenames.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not 1 <= args.jpeg_quality <= 100:
        raise ValueError("--jpeg-quality must be between 1 and 100")
    target_srs = None if args.target_srs.lower() == "none" else args.target_srs
    records = process_directory(
        args.input_directory,
        args.output_directory,
        args.report,
        args.profile,
        args.max_pixels,
        target_srs,
        args.jpeg_quality,
        args.overwrite,
        args.dry_run,
        args.limit,
        set(args.include),
    )
    successes = sum(record["status"].startswith("success") for record in records)
    failures = sum(record["status"] == "failed" for record in records)
    print(
        f"Processed {len(records)} GeoTIFFs: {successes} succeeded, "
        f"{failures} failed. Report: {args.report}"
    )


if __name__ == "__main__":
    main()
