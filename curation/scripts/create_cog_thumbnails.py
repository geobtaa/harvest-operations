"""Create JPEG thumbnails for Cloud Optimized GeoTIFFs."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

DEFAULT_INPUT_DIR = Path("purdue-campus-cogs")
DEFAULT_OUTPUT_DIR = DEFAULT_INPUT_DIR / "thumbs"
DEFAULT_WIDTH = 300
DEFAULT_DPI = 72
DEFAULT_QUALITY = 85


def require_command(command: str) -> str:
    """Return an executable path or raise a useful error."""
    executable = shutil.which(command)
    if not executable:
        raise RuntimeError(
            f"{command} was not found in PATH. Install GDAL and try again."
        )
    return executable


def discover_cogs(input_directory: Path, output_directory: Path) -> list[Path]:
    """Return all TIFF files below the input directory, excluding outputs."""
    output_directory = output_directory.resolve()
    return sorted(
        path
        for path in input_directory.rglob("*")
        if path.is_file()
        and path.suffix.lower() in {".tif", ".tiff"}
        and output_directory not in path.resolve().parents
    )


def run_gdalinfo(path: Path) -> dict[str, Any]:
    """Return GDAL's JSON metadata for a raster."""
    result = subprocess.run(
        [require_command("gdalinfo"), "-json", str(path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "gdalinfo failed")

    metadata = json.loads(result.stdout)
    # Some GDAL versions wrap metadata in a one-item array.
    return metadata[0] if isinstance(metadata, list) else metadata


def thumbnail_path(
    source_path: Path,
    input_directory: Path,
    output_directory: Path,
) -> Path:
    """Return a collision-safe thumbnail path based on the source location."""
    relative_path = source_path.relative_to(input_directory)
    name_parts = [*relative_path.parts[:-1], source_path.stem]
    return output_directory / f"{'_'.join(name_parts)}.jpg"


def create_thumbnail(
    source_path: Path,
    output_path: Path,
    width: int,
    dpi: int,
    quality: int,
    overwrite: bool,
) -> str:
    """Create one proportional JPEG thumbnail and return its status."""
    if output_path.exists() and not overwrite:
        return "skipped"

    metadata = run_gdalinfo(source_path)
    source_width, source_height = metadata.get("size", [0, 0])
    band_count = len(metadata.get("bands", []))
    if source_width <= 0 or source_height <= 0:
        raise RuntimeError("source raster has invalid dimensions")
    if band_count not in {1, 3}:
        raise RuntimeError(f"JPEG output requires 1 or 3 bands; found {band_count}")

    height = max(1, round(source_height * width / source_width))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        require_command("gdal_translate"),
        "-of",
        "JPEG",
        "-r",
        "average",
        "-outsize",
        str(width),
        str(height),
        "-co",
        f"QUALITY={quality}",
        "-co",
        "PROGRESSIVE=YES",
        "-co",
        "WRITE_EXIF_METADATA=YES",
        "-mo",
        f"EXIF_XResolution={dpi}/1",
        "-mo",
        f"EXIF_YResolution={dpi}/1",
        "-mo",
        "EXIF_ResolutionUnit=2",
        str(source_path),
        str(output_path),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        output_path.unlink(missing_ok=True)
        raise RuntimeError(result.stderr.strip() or "gdal_translate failed")
    return "created"


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--width", type=int, default=DEFAULT_WIDTH)
    parser.add_argument("--dpi", type=int, default=DEFAULT_DPI)
    parser.add_argument("--quality", type=int, default=DEFAULT_QUALITY)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> int:
    """Create thumbnails for every COG below the input directory."""
    args = parse_arguments()
    if args.width <= 0 or args.dpi <= 0:
        raise SystemExit("--width and --dpi must be positive integers")
    if not 1 <= args.quality <= 100:
        raise SystemExit("--quality must be between 1 and 100")
    if not args.input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {args.input_dir}")

    sources = discover_cogs(args.input_dir, args.output_dir)
    print(f"Found {len(sources)} COGs in {args.input_dir}")
    created = skipped = failed = 0

    for source_path in sources:
        output_path = thumbnail_path(source_path, args.input_dir, args.output_dir)
        try:
            status = create_thumbnail(
                source_path,
                output_path,
                args.width,
                args.dpi,
                args.quality,
                args.overwrite,
            )
            if status == "created":
                created += 1
            else:
                skipped += 1
            print(f"{status}: {source_path} -> {output_path}")
        except (OSError, RuntimeError, json.JSONDecodeError) as exc:
            failed += 1
            print(f"failed: {source_path}: {exc}")

    print(f"Complete: {created} created, {skipped} skipped, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
