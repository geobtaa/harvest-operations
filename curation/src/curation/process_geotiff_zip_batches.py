"""Download ZIP batches and create COGs and thumbnails from crop GeoTIFFs."""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import urllib.request
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zipfile import ZipFile

DEFAULT_MIN_FREE_GB = 12.0
DEFAULT_THUMB_WIDTH = 300
DEFAULT_THUMB_DPI = 72
DEFAULT_THUMB_QUALITY = 85

REPORT_FIELDS = [
    "item_id",
    "zip_url",
    "status",
    "crop_source",
    "cog_path",
    "thumbnail_path",
    "source_size_bytes",
    "cog_size_bytes",
    "thumbnail_size_bytes",
    "source_epsg",
    "output_epsg",
    "is_cog",
    "warnings",
    "error",
]


@dataclass(frozen=True)
class ManifestItem:
    """One downloadable item from a URL manifest."""

    item_id: str
    source_url: str


class BatchProcessingError(RuntimeError):
    """Raised for recoverable per-item processing failures."""


def read_manifest(path: Path) -> list[ManifestItem]:
    """Read item IDs and URLs from a CSV manifest or headerless URL list."""
    sample = path.read_text(encoding="utf-8-sig").splitlines()
    first_line = next((line for line in sample if line.strip()), "")
    if first_line.startswith("http") and "," not in first_line:
        return [
            ManifestItem(item_id=f"row-{index:04d}", source_url=line.strip())
            for index, line in enumerate(sample, start=1)
            if line.strip()
        ]

    with path.open(newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        required = {"item_id", "zip_url"}
        fields = set(reader.fieldnames or [])
        if not required <= fields:
            if "url" in fields:
                required = {"url"}
            else:
                missing = required - fields
                raise ValueError(
                    "Manifest is missing required column(s): "
                    + ", ".join(sorted(missing))
                )
        items = [
            ManifestItem(
                item_id=(row.get("item_id") or f"row-{index:04d}").strip(),
                source_url=(row.get("zip_url") or row.get("url") or "").strip(),
            )
            for index, row in enumerate(reader, start=2)
        ]

    bad_rows = [
        index
        for index, item in enumerate(items, start=2)
        if not item.item_id or not item.source_url
    ]
    if bad_rows:
        raise ValueError(f"Manifest has blank item_id or zip_url on row(s): {bad_rows}")
    return items


def free_gb(path: Path) -> float:
    """Return available disk space in GiB for the filesystem containing path."""
    path.mkdir(parents=True, exist_ok=True)
    return shutil.disk_usage(path).free / (1024**3)


def safe_zip_members(zip_file: ZipFile) -> list[str]:
    """Return ZIP member names that are safe to extract."""
    members = []
    for name in zip_file.namelist():
        parts = Path(name).parts
        if name.endswith("/") or Path(name).is_absolute() or ".." in parts:
            continue
        members.append(name)
    return members


def find_crop_geotiff_members(members: list[str]) -> list[str]:
    """Find candidate crop GeoTIFF members in a ZIP listing."""
    return sorted(
        member
        for member in members
        if Path(member).suffix.lower() in {".tif", ".tiff"}
        and "crop" in Path(member).name.lower()
        and "geoutm" not in Path(member).name.lower()
        and not Path(member).name.lower().endswith(".enp")
    )


def matching_lightweight_members(members: list[str], crop_member: str) -> list[str]:
    """Return the crop GeoTIFF and useful lightweight sidecar members."""
    selected = {crop_member}
    crop_path = Path(crop_member)
    crop_stem = crop_path.name.rsplit(".", 1)[0]
    item_prefix = crop_path.name.split("-0001", 1)[0]

    for member in members:
        member_path = Path(member)
        name = member_path.name
        lower = name.lower()
        if lower == f"{crop_stem}.tfw":
            selected.add(member)
        elif lower.endswith(".txt") and name.startswith(item_prefix):
            selected.add(member)
        elif lower.endswith(".json") and name.startswith(item_prefix):
            selected.add(member)
    return sorted(selected)


def extract_members(zip_path: Path, members: list[str], output_dir: Path) -> list[Path]:
    """Extract selected safe members and return their paths."""
    output_dir.mkdir(parents=True, exist_ok=True)
    extracted = []
    with ZipFile(zip_path) as archive:
        for member in members:
            destination = output_dir / member
            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, destination.open("wb") as target:
                shutil.copyfileobj(source, target)
            extracted.append(destination)
    return extracted


def discover_existing_outputs(
    item_id: str,
    cog_dir: Path,
    thumbnail_dir: Path,
) -> tuple[Path | None, Path | None]:
    """Find existing COG/thumbnail outputs for an item ID."""
    cogs = sorted(cog_dir.glob(f"{item_id}*_cog.tif"))
    if len(cogs) != 1:
        return None, None
    thumbnail = thumbnail_dir / f"{cogs[0].stem}.jpg"
    return cogs[0], thumbnail if thumbnail.exists() else None


def filename_from_content_disposition(value: str | None) -> str:
    """Extract a filename from a Content-Disposition header."""
    if not value:
        return ""
    match = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', value)
    return match.group(1).strip() if match else ""


def item_id_from_remote_filename(filename: str) -> str:
    """Derive an item ID from a remote ZIP filename."""
    path = Path(filename)
    stem = path.name
    if stem.lower().endswith(".zip"):
        stem = stem[:-4]
    return stem


def remote_item_id(url: str) -> str:
    """Use HTTP headers to identify an item before downloading the ZIP."""
    request = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(request, timeout=30) as response:
        filename = filename_from_content_disposition(
            response.headers.get("Content-Disposition")
        )
    return item_id_from_remote_filename(filename) if filename else ""


def discover_existing_outputs_for_source(
    source_stem: str,
    cog_dir: Path,
    thumbnail_dir: Path,
) -> tuple[Path | None, Path | None]:
    """Find existing outputs for a known crop source stem."""
    cog = cog_dir / f"{source_stem}_cog.tif"
    thumbnail = thumbnail_dir / f"{source_stem}_cog.jpg"
    if cog.exists() and thumbnail.exists():
        return cog, thumbnail
    return None, None


def run_command(command: list[str]) -> None:
    """Run a subprocess command and raise a readable error on failure."""
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "command failed"
        raise BatchProcessingError(message)


def run_gdalinfo(path: Path) -> dict[str, Any]:
    """Return GDAL JSON metadata for a raster."""
    result = subprocess.run(
        ["gdalinfo", "-json", str(path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise BatchProcessingError(result.stderr.strip() or "gdalinfo failed")
    metadata = json.loads(result.stdout)
    return metadata[0] if isinstance(metadata, list) else metadata


def detected_epsg(metadata: dict[str, Any]) -> str:
    """Return an EPSG string when GDAL resolved one."""
    epsg = metadata.get("stac", {}).get("proj:epsg")
    return f"EPSG:{epsg}" if epsg else ""


def validate_cog(path: Path) -> tuple[bool, str]:
    """Return whether a raster reports itself as a COG and its EPSG."""
    metadata = run_gdalinfo(path)
    layout = metadata.get("metadata", {}).get("IMAGE_STRUCTURE", {}).get("LAYOUT")
    return layout == "COG", detected_epsg(metadata)


def create_thumbnail(
    cog_path: Path,
    thumbnail_path: Path,
    width: int,
    dpi: int,
    quality: int,
) -> None:
    """Create a JPEG thumbnail using the same GDAL options as the repo script."""
    metadata = run_gdalinfo(cog_path)
    source_width, source_height = metadata.get("size", [0, 0])
    if source_width <= 0 or source_height <= 0:
        raise BatchProcessingError("COG has invalid dimensions")
    height = max(1, round(source_height * width / source_width))
    thumbnail_path.parent.mkdir(parents=True, exist_ok=True)
    run_command(
        [
            "gdal_translate",
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
            str(cog_path),
            str(thumbnail_path),
        ]
    )


def write_reports(records: list[dict[str, Any]], report_path: Path) -> None:
    """Write CSV and JSON batch reports."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        writer.writerows({field: record.get(field, "") for field in REPORT_FIELDS} for record in records)

    with report_path.with_suffix(".json").open("w", encoding="utf-8") as file:
        json.dump(records, file, indent=2, ensure_ascii=False)


def download_zip(url: str, destination: Path) -> None:
    """Download a ZIP URL to a destination path."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url) as response, destination.open("wb") as file:
        shutil.copyfileobj(response, file)


def member_item_id(crop_member: str) -> str:
    """Derive the durable item ID from a crop GeoTIFF member name."""
    name = Path(crop_member).name
    return name.split("-0001", 1)[0] if "-0001" in name else Path(name).stem


def process_item(
    item: ManifestItem,
    args: argparse.Namespace,
    repo_root: Path,
) -> dict[str, Any]:
    """Process one manifest item."""
    args.cog_dir.mkdir(parents=True, exist_ok=True)
    args.thumbnail_dir.mkdir(parents=True, exist_ok=True)
    if args.dry_run:
        return {
            "item_id": item.item_id,
            "zip_url": item.source_url,
            "status": "dry_run",
            "warnings": "download_and_processing_not_run",
        }

    lookup_item_id = item.item_id
    try:
        lookup_item_id = remote_item_id(item.source_url) or lookup_item_id
    except Exception:
        pass
    existing_cog, existing_thumbnail = discover_existing_outputs(
        lookup_item_id,
        args.cog_dir,
        args.thumbnail_dir,
    )
    if existing_cog and existing_thumbnail:
        is_cog, output_epsg = validate_cog(existing_cog)
        if is_cog:
            return {
                "item_id": lookup_item_id,
                "zip_url": item.source_url,
                "status": "skipped",
                "cog_path": str(existing_cog),
                "thumbnail_path": str(existing_thumbnail),
                "cog_size_bytes": existing_cog.stat().st_size,
                "thumbnail_size_bytes": existing_thumbnail.stat().st_size,
                "output_epsg": output_epsg,
                "is_cog": True,
                "warnings": "outputs_exist",
            }

    available = free_gb(args.work_dir)
    if available < args.min_free_gb:
        raise RuntimeError(
            f"Free disk space is {available:.1f} GiB, below threshold "
            f"{args.min_free_gb:.1f} GiB"
        )

    item_work_dir = args.work_dir / "extract" / lookup_item_id
    zip_path = args.work_dir / "downloads" / f"{lookup_item_id}.zip"
    shutil.rmtree(item_work_dir, ignore_errors=True)

    download_zip(item.source_url, zip_path)
    with ZipFile(zip_path) as archive:
        members = safe_zip_members(archive)
        crop_members = find_crop_geotiff_members(members)
        if len(crop_members) != 1:
            raise BatchProcessingError(
                f"Expected one crop GeoTIFF, found {len(crop_members)}: {crop_members}"
            )
        resolved_item_id = member_item_id(crop_members[0])
        source_stem = Path(crop_members[0]).stem
        existing_cog, existing_thumbnail = discover_existing_outputs_for_source(
            source_stem,
            args.cog_dir,
            args.thumbnail_dir,
        )
        if existing_cog and existing_thumbnail:
            is_cog, output_epsg = validate_cog(existing_cog)
            if is_cog:
                zip_path.unlink(missing_ok=True)
                return {
                    "item_id": resolved_item_id,
                    "zip_url": item.source_url,
                    "status": "skipped",
                    "crop_source": crop_members[0],
                    "cog_path": str(existing_cog),
                    "thumbnail_path": str(existing_thumbnail),
                    "cog_size_bytes": existing_cog.stat().st_size,
                    "thumbnail_size_bytes": existing_thumbnail.stat().st_size,
                    "output_epsg": output_epsg,
                    "is_cog": True,
                    "warnings": "outputs_exist_after_zip_inspection",
                }
        selected_members = matching_lightweight_members(members, crop_members[0])

    extracted = extract_members(zip_path, selected_members, item_work_dir)
    crop_source = next(path for path in extracted if path.suffix.lower() in {".tif", ".tiff"})
    source_metadata = run_gdalinfo(crop_source)
    source_epsg = detected_epsg(source_metadata)
    cog_report = args.work_dir / "reports" / f"{lookup_item_id}-cog-report.csv"

    run_command(
        [
            sys.executable,
            str(repo_root / "scripts" / "create_geotiff_cogs.py"),
            str(item_work_dir),
            "--output-directory",
            str(args.cog_dir),
            "--report",
            str(cog_report),
            "--profile",
            "reduced",
            "--target-srs",
            args.target_srs,
            "--jpeg-quality",
            str(args.jpeg_quality),
        ]
    )

    cog_path = args.cog_dir / f"{crop_source.stem}_cog.tif"
    if not cog_path.exists():
        raise BatchProcessingError(f"Expected COG was not created: {cog_path}")
    is_cog, output_epsg = validate_cog(cog_path)
    if not is_cog:
        raise BatchProcessingError(f"Output is not reported as a COG: {cog_path}")

    thumbnail_path = args.thumbnail_dir / f"{cog_path.stem}.jpg"
    create_thumbnail(
        cog_path,
        thumbnail_path,
        args.thumbnail_width,
        args.thumbnail_dpi,
        args.thumbnail_quality,
    )

    record = {
        "item_id": member_item_id(crop_source.name),
        "zip_url": item.source_url,
        "status": "success",
        "crop_source": str(crop_source),
        "cog_path": str(cog_path),
        "thumbnail_path": str(thumbnail_path),
        "source_size_bytes": crop_source.stat().st_size,
        "cog_size_bytes": cog_path.stat().st_size,
        "thumbnail_size_bytes": thumbnail_path.stat().st_size,
        "source_epsg": source_epsg,
        "output_epsg": output_epsg,
        "is_cog": True,
        "warnings": "",
        "error": "",
    }
    zip_path.unlink(missing_ok=True)
    shutil.rmtree(item_work_dir, ignore_errors=True)
    return record


def process_manifest(args: argparse.Namespace, repo_root: Path) -> list[dict[str, Any]]:
    """Process all manifest rows and write a combined report after each item."""
    items = read_manifest(args.manifest)
    records = []
    systemic_failures = 0
    for item in items:
        print(f"Processing {item.item_id}")
        try:
            record = process_item(item, args, repo_root)
            systemic_failures = 0
        except BatchProcessingError as exc:
            record = {
                "item_id": item.item_id,
                "zip_url": item.source_url,
                "status": "failed",
                "error": str(exc),
            }
        except Exception as exc:
            systemic_failures += 1
            record = {
                "item_id": item.item_id,
                "zip_url": item.source_url,
                "status": "failed",
                "error": str(exc),
            }
        records.append(record)
        write_reports(records, args.report)
        print(f"  {record['status']}: {record.get('error', '')}")
        if systemic_failures >= args.stop_after_systemic_failures:
            raise RuntimeError(
                f"Stopped after {systemic_failures} likely systemic failure(s)"
            )
    return records


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--cog-dir", type=Path, default=Path("09d-02-cogs"))
    parser.add_argument("--thumbnail-dir", type=Path, default=Path("09d-02-thumbnails"))
    parser.add_argument("--work-dir", type=Path, default=Path("09d-02-work"))
    parser.add_argument("--report", type=Path, default=Path("09d-02-full-processing-report.csv"))
    parser.add_argument("--target-srs", default="EPSG:3857")
    parser.add_argument("--jpeg-quality", type=int, default=90)
    parser.add_argument("--min-free-gb", type=float, default=DEFAULT_MIN_FREE_GB)
    parser.add_argument("--thumbnail-width", type=int, default=DEFAULT_THUMB_WIDTH)
    parser.add_argument("--thumbnail-dpi", type=int, default=DEFAULT_THUMB_DPI)
    parser.add_argument("--thumbnail-quality", type=int, default=DEFAULT_THUMB_QUALITY)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stop-after-systemic-failures", type=int, default=2)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the batch processor."""
    args = parse_args(argv)
    if not 1 <= args.jpeg_quality <= 100:
        raise SystemExit("--jpeg-quality must be between 1 and 100")
    if not 1 <= args.thumbnail_quality <= 100:
        raise SystemExit("--thumbnail-quality must be between 1 and 100")
    repo_root = Path(__file__).resolve().parents[2]
    records = process_manifest(args, repo_root)
    failures = sum(record.get("status") == "failed" for record in records)
    print(f"Processed {len(records)} item(s): {failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
