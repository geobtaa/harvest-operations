"""Rebuild COGs with pixel interleaving and preserved transparency.

The source RGB pixels are read from the original GeoTIFF in each ZIP.  The
transparency footprint is read from an existing COG, so rebuilding does not
need to classify black pixels again.  GDAL's COG driver converts the temporary
RGBA dataset to the requested output structure.  A three-band DEFLATE COG with
an internal dataset mask is the most conservative cross-viewer profile.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any
from zipfile import ZipFile


REPORT_FIELDS = [
    "input_cog",
    "source_zip",
    "source_member",
    "output_cog",
    "status",
    "input_interleave",
    "output_interleave",
    "input_band_count",
    "output_band_count",
    "output_mask",
    "compression",
    "alpha_storage",
    "alpha_threshold",
    "input_size_bytes",
    "output_size_bytes",
    "size_reduction_percent",
    "is_cog",
    "warnings",
    "error",
]


class RebuildError(RuntimeError):
    """Raised when one COG cannot be safely rebuilt."""


def require_command(command: str) -> str:
    """Return an executable path or raise a useful error."""
    executable = shutil.which(command)
    if not executable:
        raise RebuildError(
            f"{command} was not found in PATH. Install GDAL and try again."
        )
    return executable


def run_command(command: list[str]) -> None:
    """Run a subprocess command and raise a readable error on failure."""
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "command failed"
        raise RebuildError(message)


def run_gdalinfo(path: Path) -> dict[str, Any]:
    """Return GDAL's JSON metadata for a raster."""
    result = subprocess.run(
        [require_command("gdalinfo"), "-json", str(path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RebuildError(result.stderr.strip() or "gdalinfo failed")
    metadata = json.loads(result.stdout)
    return metadata[0] if isinstance(metadata, list) else metadata


def item_id_from_cog(path: Path) -> str:
    """Return the item ID used to name the corresponding source ZIP."""
    stem = path.stem.removesuffix("_cog")
    return stem.split("-0001", 1)[0] if "-0001" in stem else stem


def source_stem_from_cog(path: Path) -> str:
    """Return the original crop GeoTIFF stem represented by a COG."""
    return path.stem.removesuffix("_cog")


def safe_zip_members(zip_path: Path) -> list[str]:
    """Return file members that cannot escape the ZIP's virtual directory."""
    with ZipFile(zip_path) as archive:
        return [
            name
            for name in archive.namelist()
            if not name.endswith("/")
            and not Path(name).is_absolute()
            and ".." not in Path(name).parts
        ]


def find_source_member(zip_path: Path, source_stem: str) -> str:
    """Find the original GeoTIFF corresponding exactly to a COG stem."""
    matches = [
        member
        for member in safe_zip_members(zip_path)
        if Path(member).suffix.lower() in {".tif", ".tiff"}
        and Path(member).stem == source_stem
    ]
    if len(matches) != 1:
        raise RebuildError(
            f"Expected one source GeoTIFF named {source_stem!r} in {zip_path}, "
            f"found {len(matches)}: {matches}"
        )
    return matches[0]


def vsi_zip_path(zip_path: Path, member: str) -> str:
    """Return a GDAL /vsizip path without extracting the large source TIFF."""
    return f"/vsizip/{zip_path.resolve()}/{member}"


def image_structure(metadata: dict[str, Any]) -> dict[str, Any]:
    """Return the IMAGE_STRUCTURE metadata domain."""
    return metadata.get("metadata", {}).get("IMAGE_STRUCTURE", {})


def reported_interleave(metadata: dict[str, Any]) -> str:
    """Return normalized interleaving; omission in a COG means PIXEL."""
    return str(image_structure(metadata).get("INTERLEAVE") or "PIXEL").upper()


def alpha_band_selector(metadata: dict[str, Any]) -> str:
    """Return the GDAL band selector for existing transparency."""
    bands = metadata.get("bands", [])
    for band in bands:
        if band.get("colorInterpretation") == "Alpha":
            return str(band["band"])
    if bands and "PER_DATASET" in bands[0].get("mask", {}).get("flags", []):
        return "mask,1"
    raise RebuildError("Existing COG has neither an alpha band nor a dataset mask")


def target_grid(metadata: dict[str, Any]) -> tuple[int, int, tuple[float, ...]]:
    """Return the existing COG's north-up dimensions and exact outer bounds."""
    width, height = metadata.get("size", [0, 0])
    transform = metadata.get("geoTransform", [])
    if width <= 0 or height <= 0 or len(transform) != 6:
        raise RebuildError("Existing COG has an invalid raster grid")
    if not math.isclose(transform[2], 0.0, abs_tol=1e-12) or not math.isclose(
        transform[4], 0.0, abs_tol=1e-12
    ):
        raise RebuildError("Rotated output grids are not supported")
    if transform[1] <= 0 or transform[5] >= 0:
        raise RebuildError("Existing COG has an unsupported pixel orientation")
    west = transform[0]
    north = transform[3]
    east = west + width * transform[1]
    south = north + height * transform[5]
    return width, height, (west, south, east, north)


def target_srs(metadata: dict[str, Any]) -> str:
    """Return an EPSG identifier or WKT for the existing COG's CRS."""
    epsg = metadata.get("stac", {}).get("proj:epsg")
    if epsg:
        return f"EPSG:{epsg}"
    wkt = metadata.get("coordinateSystem", {}).get("wkt")
    if wkt:
        return str(wkt)
    raise RebuildError("Existing COG has no resolvable coordinate system")


def cog_creation_options(compression: str, jpeg_quality: int) -> list[str]:
    """Return explicitly pixel-interleaved, browser-oriented COG options."""
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
        "-co",
        "INTERLEAVE=PIXEL",
    ]
    if compression == "DEFLATE":
        return common + [
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "OVERVIEW_COMPRESS=DEFLATE",
            "-co",
            "LEVEL=9",
            "-co",
            "PREDICTOR=2",
            "-co",
            "OVERVIEW_PREDICTOR=2",
        ]
    return common + [
        "-co",
        "COMPRESS=JPEG",
        "-co",
        "OVERVIEW_COMPRESS=JPEG",
        "-co",
        f"QUALITY={jpeg_quality}",
        "-co",
        f"OVERVIEW_QUALITY={jpeg_quality}",
    ]


def output_band_arguments(alpha_storage: str) -> list[str]:
    """Return GDAL arguments for either an internal mask or alpha band."""
    if alpha_storage == "mask":
        return ["-b", "1", "-b", "2", "-b", "3", "-mask", "4"]
    return []


def build_rgba_vrt(
    source_path: str,
    existing_cog: Path,
    existing_metadata: dict[str, Any],
    temporary_directory: Path,
    alpha_threshold: int,
) -> tuple[Path, Path]:
    """Build an RGBA VRT using source RGB and the existing transparency."""
    width, height, bounds = target_grid(existing_metadata)
    warped_rgb = temporary_directory / "warped-rgb.vrt"
    run_command(
        [
            require_command("gdalwarp"),
            "-q",
            "-of",
            "VRT",
            "-t_srs",
            target_srs(existing_metadata),
            "-te",
            *(format(value, ".17g") for value in bounds),
            "-ts",
            str(width),
            str(height),
            "-r",
            "average",
            "-srcnodata",
            "none",
            "-multi",
            "-wo",
            "NUM_THREADS=ALL_CPUS",
            source_path,
            str(warped_rgb),
        ]
    )

    band_vrts = []
    for band_number, name in enumerate(("red", "green", "blue"), start=1):
        band_vrt = temporary_directory / f"{name}.vrt"
        run_command(
            [
                require_command("gdal_translate"),
                "-q",
                "-of",
                "VRT",
                "-b",
                str(band_number),
                str(warped_rgb),
                str(band_vrt),
            ]
        )
        band_vrts.append(band_vrt)

    raw_alpha_vrt = temporary_directory / "raw-alpha.vrt"
    run_command(
        [
            require_command("gdal_translate"),
            "-q",
            "-of",
            "VRT",
            "-b",
            alpha_band_selector(existing_metadata),
            str(existing_cog.resolve()),
            str(raw_alpha_vrt),
        ]
    )
    alpha_vrt = temporary_directory / "alpha.tif"
    run_command(
        [
            require_command("gdal_calc.py"),
            "--quiet",
            "-A",
            str(raw_alpha_vrt),
            "--A_band=1",
            f"--calc=255*(A>={alpha_threshold})",
            "--NoDataValue=none",
            "--type=Byte",
            "--format=GTiff",
            "--co=TILED=YES",
            "--co=COMPRESS=DEFLATE",
            "--co=PREDICTOR=2",
            f"--outfile={alpha_vrt}",
        ]
    )
    band_vrts.append(alpha_vrt)

    stacked_vrt = temporary_directory / "stacked.vrt"
    run_command(
        [
            require_command("gdalbuildvrt"),
            "-q",
            "-separate",
            str(stacked_vrt),
            *(str(path) for path in band_vrts),
        ]
    )
    rgba_vrt = temporary_directory / "rgba.vrt"
    run_command(
        [
            require_command("gdal_translate"),
            "-q",
            "-of",
            "VRT",
            "-colorinterp",
            "red,green,blue,alpha",
            str(stacked_vrt),
            str(rgba_vrt),
        ]
    )
    return rgba_vrt, alpha_vrt


def validate_mask_pixels(
    expected_mask: Path,
    output_cog: Path,
    temporary_directory: Path,
    output_selector: str,
) -> None:
    """Require the output internal mask to equal the thresholded alpha."""
    output_mask = temporary_directory / "output-mask.vrt"
    run_command(
        [
            require_command("gdal_translate"),
            "-q",
            "-of",
            "VRT",
            "-b",
            output_selector,
            "-colorinterp",
            "gray",
            str(output_cog),
            str(output_mask),
        ]
    )
    run_command(
        [
            require_command("gdalcompare.py"),
            "-skip_binary",
            "-skip_metadata",
            "-skip_srs",
            "-skip_geotransform",
            "-skip_overviews",
            str(expected_mask),
            str(output_mask),
        ]
    )


def validate_output(
    input_metadata: dict[str, Any],
    output_metadata: dict[str, Any],
    alpha_storage: str,
) -> list[str]:
    """Return compatibility or preservation problems in a rebuilt COG."""
    problems = []
    structure = image_structure(output_metadata)
    bands = output_metadata.get("bands", [])
    colors = [band.get("colorInterpretation") for band in bands]
    mask_flags = bands[0].get("mask", {}).get("flags", []) if bands else []

    if structure.get("LAYOUT") != "COG":
        problems.append("output_not_reported_as_cog")
    if reported_interleave(output_metadata) != "PIXEL":
        problems.append("output_not_pixel_interleaved")
    if alpha_storage == "mask":
        if colors != ["Red", "Green", "Blue"]:
            problems.append("output_not_three_band_rgb")
        if "PER_DATASET" not in mask_flags:
            problems.append("output_missing_internal_mask")
    elif colors != ["Red", "Green", "Blue", "Alpha"]:
        problems.append("output_not_four_band_rgba")
    if bands and not bands[0].get("overviews"):
        problems.append("output_missing_internal_overviews")
    if output_metadata.get("size") != input_metadata.get("size"):
        problems.append("output_grid_size_changed")

    input_transform = input_metadata.get("geoTransform", [])
    output_transform = output_metadata.get("geoTransform", [])
    if len(input_transform) != len(output_transform) or any(
        not math.isclose(before, after, rel_tol=1e-10, abs_tol=1e-8)
        for before, after in zip(input_transform, output_transform)
    ):
        problems.append("output_geotransform_changed")
    input_epsg = input_metadata.get("stac", {}).get("proj:epsg")
    output_epsg = output_metadata.get("stac", {}).get("proj:epsg")
    if input_epsg and output_epsg != input_epsg:
        problems.append("output_srs_changed")
    return problems


def write_reports(records: list[dict[str, Any]], report_path: Path) -> None:
    """Write CSV and JSON reports."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        for record in records:
            row = {field: record.get(field, "") for field in REPORT_FIELDS}
            if isinstance(row["warnings"], list):
                row["warnings"] = "; ".join(row["warnings"])
            writer.writerow(row)
    with report_path.with_suffix(".json").open("w", encoding="utf-8") as file:
        json.dump(records, file, indent=2, ensure_ascii=False)


def process_cog(path: Path, args: argparse.Namespace) -> dict[str, Any]:
    """Inspect or rebuild one COG and return its report record."""
    output_path = args.output_dir / path.relative_to(args.input_dir)
    zip_path = args.source_zip_dir / f"{item_id_from_cog(path)}.zip"
    record: dict[str, Any] = {
        "input_cog": str(path),
        "source_zip": str(zip_path),
        "source_member": "",
        "output_cog": str(output_path),
        "status": "pending",
        "input_size_bytes": path.stat().st_size,
        "output_size_bytes": "",
        "compression": args.compression,
        "alpha_storage": args.alpha_storage,
        "alpha_threshold": args.alpha_threshold,
        "warnings": [],
        "error": "",
    }
    temporary_output = output_path.with_name(f".{output_path.stem}.tmp.tif")
    try:
        metadata = run_gdalinfo(path)
        record["input_interleave"] = reported_interleave(metadata)
        record["input_band_count"] = len(metadata.get("bands", []))
        if record["input_interleave"] == "BAND":
            record["warnings"].append("input_is_band_interleaved")
        alpha_band_selector(metadata)
        if not zip_path.is_file():
            raise RebuildError(f"Source ZIP does not exist: {zip_path}")
        source_member = find_source_member(zip_path, source_stem_from_cog(path))
        record["source_member"] = source_member

        if args.dry_run:
            record["status"] = "needs_rebuild"
            return record
        if output_path.exists() and not args.overwrite:
            record["status"] = "skipped"
            record["warnings"].append("output_exists")
            return record

        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_output.unlink(missing_ok=True)
        with tempfile.TemporaryDirectory(prefix="viewer-cog-") as tempdir:
            rgba_vrt, expected_mask = build_rgba_vrt(
                vsi_zip_path(zip_path, source_member),
                path,
                metadata,
                Path(tempdir),
                args.alpha_threshold,
            )
            run_command(
                [
                    require_command("gdal_translate"),
                    "-q",
                    "-of",
                    "COG",
                    "-ot",
                    "Byte",
                    *output_band_arguments(args.alpha_storage),
                    *cog_creation_options(args.compression, args.jpeg_quality),
                    str(rgba_vrt),
                    str(temporary_output),
                ]
            )
            validate_mask_pixels(
                expected_mask,
                temporary_output,
                Path(tempdir),
                "mask,1" if args.alpha_storage == "mask" else "4",
            )

        output_metadata = run_gdalinfo(temporary_output)
        problems = validate_output(metadata, output_metadata, args.alpha_storage)
        if problems:
            raise RebuildError("; ".join(problems))
        os.replace(temporary_output, output_path)
        output_size = output_path.stat().st_size
        output_bands = output_metadata.get("bands", [])
        record.update(
            {
                "status": "success",
                "output_interleave": reported_interleave(output_metadata),
                "output_band_count": len(output_bands),
                "output_mask": "PER_DATASET"
                in output_bands[0].get("mask", {}).get("flags", []),
                "output_size_bytes": output_size,
                "size_reduction_percent": round(
                    100 * (1 - output_size / path.stat().st_size), 3
                ),
                "is_cog": True,
            }
        )
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = str(exc)
        temporary_output.unlink(missing_ok=True)
    return record


def selected_cogs(args: argparse.Namespace) -> list[Path]:
    """Return deterministically ordered COGs matching optional selectors."""
    cogs = sorted(args.input_dir.rglob("*_cog.tif"))
    if args.include:
        selectors = set(args.include)
        cogs = [
            path
            for path in cogs
            if path.name in selectors
            or path.stem in selectors
            or item_id_from_cog(path) in selectors
        ]
    if args.limit is not None:
        cogs = cogs[: args.limit]
    if not cogs:
        raise RebuildError(f"No matching COGs found in {args.input_dir}")
    return cogs


def process_directory(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Inspect or rebuild selected COGs, checkpointing the report each time."""
    cogs = selected_cogs(args)
    records = []
    for index, path in enumerate(cogs, start=1):
        print(f"[{index}/{len(cogs)}] {path.name}", flush=True)
        record = process_cog(path, args)
        records.append(record)
        write_reports(records, args.report)
        detail = record.get("error") or record.get("output_cog")
        print(f"  {record['status']}: {detail}", flush=True)
    return records


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=Path)
    parser.add_argument("source_zip_dir", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="Only process this item ID, filename, or filename stem; repeatable.",
    )
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--compression",
        choices=["JPEG", "DEFLATE"],
        default="JPEG",
        help=(
            "JPEG creates compact YCbCr RGB plus an internal mask; DEFLATE "
            "creates larger, legacy-compatible explicit RGBA."
        ),
    )
    parser.add_argument(
        "--alpha-storage",
        choices=["auto", "mask", "band"],
        default="auto",
        help=(
            "Store transparency as a dataset mask or explicit alpha band. "
            "Auto uses a mask for JPEG and a band for DEFLATE. For the widest "
            "viewer compatibility, use DEFLATE with mask."
        ),
    )
    parser.add_argument("--jpeg-quality", type=int, default=90)
    parser.add_argument(
        "--alpha-threshold",
        type=int,
        default=128,
        help=(
            "Existing alpha values at or above this become valid in the binary "
            "internal mask. The default removes JPEG ringing around a 0/255 alpha."
        ),
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the COG rebuild workflow."""
    args = parse_args(argv)
    if not 1 <= args.jpeg_quality <= 100:
        raise SystemExit("--jpeg-quality must be between 1 and 100")
    if not 1 <= args.alpha_threshold <= 255:
        raise SystemExit("--alpha-threshold must be between 1 and 255")
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be greater than zero")
    if args.alpha_storage == "auto":
        args.alpha_storage = "mask" if args.compression == "JPEG" else "band"
    if args.compression == "JPEG" and args.alpha_storage == "band":
        raise SystemExit(
            "JPEG COGs cannot retain a fourth alpha band; use "
            "--alpha-storage mask or --compression DEFLATE"
        )
    records = process_directory(args)
    failures = sum(record["status"] == "failed" for record in records)
    print(f"Processed {len(records)} COG(s): {failures} failed")
    print(f"Wrote {args.report} and {args.report.with_suffix('.json')}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
