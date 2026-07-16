"""Crop near-black outer collars from existing COG GeoTIFFs."""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from osgeo import gdal


DEFAULT_INPUT_DIR = Path("04d-02-cogs-thumbs")
DEFAULT_REPORT = DEFAULT_INPUT_DIR / "04d-02_black_collar_report.csv"

REPORT_FIELDS = [
    "path",
    "status",
    "thumbnail_status",
    "width",
    "height",
    "xoff",
    "yoff",
    "crop_width",
    "crop_height",
    "left_collar",
    "top_collar",
    "right_collar",
    "bottom_collar",
    "removed_pixels",
    "removed_percent",
    "error",
]


@dataclass(frozen=True)
class ContentWindow:
    xoff: int
    yoff: int
    width: int
    height: int
    source_width: int
    source_height: int

    @property
    def left_collar(self) -> int:
        return self.xoff

    @property
    def top_collar(self) -> int:
        return self.yoff

    @property
    def right_collar(self) -> int:
        return self.source_width - self.xoff - self.width

    @property
    def bottom_collar(self) -> int:
        return self.source_height - self.yoff - self.height

    @property
    def removed_pixels(self) -> int:
        source_pixels = self.source_width * self.source_height
        crop_pixels = self.width * self.height
        return source_pixels - crop_pixels

    @property
    def removed_percent(self) -> float:
        source_pixels = self.source_width * self.source_height
        return 100 * self.removed_pixels / source_pixels if source_pixels else 0

    def largest_collar(self) -> int:
        return max(
            self.left_collar,
            self.top_collar,
            self.right_collar,
            self.bottom_collar,
        )

    def is_full_image(self) -> bool:
        return (
            self.xoff == 0
            and self.yoff == 0
            and self.width == self.source_width
            and self.height == self.source_height
        )


def content_window(
    path: Path,
    threshold: int,
    chunk_rows: int,
) -> ContentWindow:
    """Return the bounding window of pixels that are not near-black."""
    dataset = gdal.Open(str(path), gdal.GA_ReadOnly)
    if dataset is None:
        raise RuntimeError("GDAL could not open raster")
    width = dataset.RasterXSize
    height = dataset.RasterYSize
    band_count = min(dataset.RasterCount, 3)
    if band_count == 0:
        raise RuntimeError("Raster has no bands")

    row_has_content = np.zeros(height, dtype=bool)
    col_has_content = np.zeros(width, dtype=bool)
    bands = [dataset.GetRasterBand(index) for index in range(1, band_count + 1)]

    for yoff in range(0, height, chunk_rows):
        rows = min(chunk_rows, height - yoff)
        arrays = [
            band.ReadAsArray(0, yoff, width, rows)
            for band in bands
        ]
        if any(array is None for array in arrays):
            raise RuntimeError(f"Could not read raster chunk at row {yoff}")
        stack = np.stack(arrays, axis=0)
        non_black = np.any(stack > threshold, axis=0)
        row_has_content[yoff : yoff + rows] = np.any(non_black, axis=1)
        col_has_content |= np.any(non_black, axis=0)

    content_rows = np.flatnonzero(row_has_content)
    content_cols = np.flatnonzero(col_has_content)
    if not len(content_rows) or not len(content_cols):
        raise RuntimeError("No non-black content found")

    yoff = int(content_rows[0])
    bottom = int(content_rows[-1]) + 1
    xoff = int(content_cols[0])
    right = int(content_cols[-1]) + 1
    return ContentWindow(
        xoff=xoff,
        yoff=yoff,
        width=right - xoff,
        height=bottom - yoff,
        source_width=width,
        source_height=height,
    )


def should_crop(
    window: ContentWindow,
    min_collar_pixels: int,
    min_removed_percent: float,
) -> bool:
    """Return whether the detected collar is large enough to rewrite."""
    return (
        not window.is_full_image()
        and window.largest_collar() >= min_collar_pixels
        and window.removed_percent >= min_removed_percent
    )


def cog_options(jpeg_quality: int) -> list[str]:
    """Return creation options matching the existing reduced COG profile."""
    return [
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


def crop_cog(
    path: Path,
    window: ContentWindow,
    input_dir: Path,
    backup_dir: Path | None,
    jpeg_quality: int,
) -> None:
    """Rewrite a COG to the detected content window."""
    temporary_path = path.with_name(f"{path.stem}.collar-crop.tmp.tif")
    command = [
        "gdal_translate",
        "-q",
        "-of",
        "COG",
        "-srcwin",
        str(window.xoff),
        str(window.yoff),
        str(window.width),
        str(window.height),
        *cog_options(jpeg_quality),
        str(path),
        str(temporary_path),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        temporary_path.unlink(missing_ok=True)
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())

    if backup_dir:
        backup_path = backup_dir / path.relative_to(input_dir)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, backup_path)
    os.replace(temporary_path, path)


def item_id_from_cog(path: Path) -> str:
    """Derive the item ID used for sibling thumbnails."""
    stem = path.stem.removesuffix("_cog")
    return stem.split("-0001", 1)[0] if "-0001" in stem else stem


def thumbnail_path(path: Path) -> Path:
    """Return the expected sibling thumbnail path for a COG."""
    return path.with_name(f"{item_id_from_cog(path)}.jpg")


def refresh_thumbnail(path: Path, width: int, dpi: int, quality: int) -> str:
    """Regenerate the sibling JPEG thumbnail for a cropped COG."""
    output_path = thumbnail_path(path)
    metadata = gdal.Info(str(path), format="json")
    source_width, source_height = metadata.get("size", [0, 0])
    if source_width <= 0 or source_height <= 0:
        raise RuntimeError("source raster has invalid dimensions")
    height = max(1, round(source_height * width / source_width))
    command = [
        "gdal_translate",
        "-q",
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
        str(path),
        str(output_path),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "gdal_translate failed")
    return str(output_path)


def report_row(
    path: Path,
    status: str,
    window: ContentWindow | None,
    error: str = "",
    thumbnail_status: str = "",
):
    """Return a report row."""
    if not window:
        return {
            "path": str(path),
            "status": status,
            "thumbnail_status": thumbnail_status,
            "error": error,
        }
    return {
        "path": str(path),
        "status": status,
        "thumbnail_status": thumbnail_status,
        "width": window.source_width,
        "height": window.source_height,
        "xoff": window.xoff,
        "yoff": window.yoff,
        "crop_width": window.width,
        "crop_height": window.height,
        "left_collar": window.left_collar,
        "top_collar": window.top_collar,
        "right_collar": window.right_collar,
        "bottom_collar": window.bottom_collar,
        "removed_pixels": window.removed_pixels,
        "removed_percent": f"{window.removed_percent:.3f}",
        "error": error,
    }


def process_directory(args: argparse.Namespace) -> list[dict[str, object]]:
    """Scan and optionally crop all COGs under the input directory."""
    cogs = sorted(args.input_dir.rglob("*_cog.tif"))
    if not cogs:
        raise RuntimeError(f"No COG GeoTIFFs found in {args.input_dir}")

    records = []
    for path in cogs:
        try:
            window = content_window(path, args.threshold, args.chunk_rows)
            crop = should_crop(
                window,
                args.min_collar_pixels,
                args.min_removed_percent,
            )
            status = "candidate" if crop else "unchanged"
            thumbnail_status = ""
            if crop and args.apply:
                crop_cog(path, window, args.input_dir, args.backup_dir, args.jpeg_quality)
                status = "cropped"
                if args.refresh_thumbnails:
                    thumbnail_status = refresh_thumbnail(
                        path,
                        args.thumbnail_width,
                        args.thumbnail_dpi,
                        args.thumbnail_quality,
                    )
            records.append(report_row(path, status, window, thumbnail_status=thumbnail_status))
        except Exception as exc:
            records.append(report_row(path, "failed", None, str(exc)))
    return records


def write_report(records: list[dict[str, object]], report_path: Path) -> None:
    """Write the processing report CSV."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        for record in records:
            writer.writerow({field: record.get(field, "") for field in REPORT_FIELDS})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", nargs="?", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--threshold", type=int, default=10)
    parser.add_argument("--chunk-rows", type=int, default=512)
    parser.add_argument("--min-collar-pixels", type=int, default=40)
    parser.add_argument("--min-removed-percent", type=float, default=0.5)
    parser.add_argument("--jpeg-quality", type=int, default=90)
    parser.add_argument("--refresh-thumbnails", action="store_true")
    parser.add_argument("--thumbnail-width", type=int, default=300)
    parser.add_argument("--thumbnail-dpi", type=int, default=72)
    parser.add_argument("--thumbnail-quality", type=int, default=85)
    parser.add_argument("--backup-dir", type=Path)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the black-collar crop pass."""
    args = parse_args(argv)
    if not 0 <= args.threshold <= 255:
        raise SystemExit("--threshold must be between 0 and 255")
    if args.chunk_rows <= 0:
        raise SystemExit("--chunk-rows must be greater than zero")
    records = process_directory(args)
    write_report(records, args.report)
    counts = {}
    for record in records:
        counts[record["status"]] = counts.get(record["status"], 0) + 1
    print(
        "Processed "
        f"{len(records)} COG(s): "
        + ", ".join(f"{status}={count}" for status, count in sorted(counts.items()))
    )
    print(f"Wrote {args.report}")
    return 1 if counts.get("failed") else 0


if __name__ == "__main__":
    raise SystemExit(main())
