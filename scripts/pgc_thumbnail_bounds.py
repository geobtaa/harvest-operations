#!/usr/bin/env python3
"""Create high-quality thumbnails and spatial bounds from downloaded PGC ZIPs."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from PIL import Image
from rasterio.enums import Resampling

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from utils.pgc_raster import (
    derive_wgs84_bounds,
    format_union_bounds,
)


TIFF_SUFFIXES = {".tif", ".tiff"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--downloads-manifest",
        type=Path,
        default=Path("/Volumes/LaCie/PGC/manifests/download_results_latest.csv"),
    )
    parser.add_argument(
        "--primary", type=Path, default=Path("outputs/2026-10-09_pgc_primary.csv")
    )
    parser.add_argument(
        "--output-root", type=Path, default=Path("/Volumes/LaCie/PGC-thumbnails")
    )
    parser.add_argument(
        "--scratch-root",
        type=Path,
        default=Path.home() / "Desktop" / "PGC-thumbnail-work",
    )
    parser.add_argument(
        "--kind", choices=("standard", "cropped", "all"), default="standard"
    )
    parser.add_argument("--ids", nargs="*", default=[])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--thumbnail-min-side", type=int, default=1200)
    parser.add_argument(
        "--thumbnail-format", choices=("png", "jpeg"), default="png"
    )
    parser.add_argument("--jpeg-quality", type=int, default=95)
    parser.add_argument("--detach", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.thumbnail_min_side < 1:
        raise SystemExit("--thumbnail-min-side must be positive")
    if not 1 <= args.jpeg_quality <= 100:
        raise SystemExit("--jpeg-quality must be between 1 and 100")
    if args.detach and not args.worker:
        detach(args)
        return
    process_inventory(args)


def detach(args: argparse.Namespace) -> None:
    args.output_root.mkdir(parents=True, exist_ok=True)
    forwarded = [argument for argument in sys.argv[1:] if argument != "--detach"]
    command = [
        "/usr/bin/caffeinate",
        "-dimsu",
        sys.executable,
        str(Path(__file__).resolve()),
        *forwarded,
        "--worker",
        "--quiet",
    ]
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    manifests = args.output_root / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    (manifests / f"thumbnail_bounds_{args.kind}_launcher.pid").write_text(
        f"{process.pid}\n", encoding="utf-8"
    )
    print(f"PGC thumbnail/bounds processor started as process {process.pid}")
    print(f"Status: {manifests / f'thumbnail_bounds_{args.kind}_status.json'}")


def process_inventory(args: argparse.Namespace) -> None:
    output_root = args.output_root.resolve()
    scratch_root = args.scratch_root.resolve()
    manifests = output_root / "manifests"
    logs = output_root / "logs"
    for directory in (scratch_root, manifests, logs):
        directory.mkdir(parents=True, exist_ok=True)

    migrate_legacy_thumbnail_layout(output_root)

    manifest_path = manifests / f"thumbnail_bounds_{args.kind}_results.csv"
    latest_path = manifests / f"thumbnail_bounds_{args.kind}_results_latest.csv"
    status_path = manifests / f"thumbnail_bounds_{args.kind}_status.json"
    log_path = logs / f"thumbnail_bounds_{args.kind}.log"

    downloads = pd.read_csv(args.downloads_manifest, dtype=str).fillna("")
    primary = pd.read_csv(args.primary, dtype=str).fillna("")
    candidates = build_candidates(downloads, primary, args.kind, args.ids)
    if args.limit is not None:
        candidates = candidates.head(args.limit)
    completed = load_completed(manifest_path)
    pending = [
        row
        for row in candidates.itertuples(index=False)
        if not valid_completed(
            completed.get(row.distribution_url),
            args.thumbnail_min_side,
            args.jpeg_quality,
            args.thumbnail_format,
        )
    ]
    counters = {
        "selected": len(candidates),
        "complete": len(candidates) - len(pending),
        "failed": 0,
    }
    write_status(status_path, counters, running=True)
    log_message(
        log_path,
        f"Starting {args.kind}: {len(candidates)} selected, "
        f"{counters['complete']} already complete",
        args.quiet,
    )

    try:
        for position, row in enumerate(pending, counters["complete"] + 1):
            log_message(
                log_path,
                f"[{position}/{len(candidates)}] {row.friendlier_id}",
                args.quiet,
            )
            result = process_archive(row, args, scratch_root, output_root)
            append_manifest(manifest_path, result)
            counters["complete" if result["status"] == "complete" else "failed"] += 1
            write_status(status_path, counters, running=True)
            log_message(
                log_path,
                f"  {result['status']}: {result['thumbnail_dimensions'] or '-'}; "
                f"{result['elapsed_seconds']} seconds"
                + (f"; {result['error']}" if result["error"] else ""),
                args.quiet,
            )
    finally:
        write_latest_manifest(manifest_path, latest_path)
        write_status(status_path, counters, running=False)


def build_candidates(
    downloads: pd.DataFrame,
    primary: pd.DataFrame,
    kind: str,
    ids: list[str],
) -> pd.DataFrame:
    labels = {
        "standard": ["GeoTIFF"],
        "cropped": ["Cropped GeoTIFF"],
        "all": ["GeoTIFF", "Cropped GeoTIFF"],
    }[kind]
    candidates = downloads[
        downloads["status"].isin(["downloaded", "seeded", "existing"])
        & downloads["label"].isin(labels)
    ].copy()
    if ids:
        candidates = candidates[candidates["friendlier_id"].isin(ids)]
    candidates = candidates.merge(
        primary[["ID", "Bounding Box"]].rename(
            columns={"Bounding Box": "existing_bounding_box"}
        ),
        left_on="friendlier_id",
        right_on="ID",
        how="left",
        validate="many_to_one",
    )
    return candidates.sort_values(["bytes", "friendlier_id"]).reset_index(drop=True)


def process_archive(row, args, scratch_root: Path, output_root: Path) -> dict:
    started = time.monotonic()
    result = base_result(row, args)
    destination_dir = output_root / "thumbnails" / row.region
    destination_dir.mkdir(parents=True, exist_ok=True)

    try:
        with tempfile.TemporaryDirectory(
            prefix=f"pgc-{safe_name(row.friendlier_id)}-", dir=scratch_root
        ) as temporary:
            work_dir = Path(temporary)
            extract_dir = work_dir / "extracted"
            extract_dir.mkdir()
            safe_extract(Path(row.path), extract_dir)
            tiffs = sorted(
                path
                for path in extract_dir.rglob("*")
                if path.suffix.lower() in TIFF_SUFFIXES and ".ovr" not in path.suffixes
            )
            if not tiffs:
                raise RuntimeError("archive contains no TIFF files")

            wgs84 = [derive_wgs84_bounds(source) for source in tiffs]
            source = tiffs[0]
            extension = "png" if args.thumbnail_format == "png" else "jpg"
            staged = work_dir / f"{safe_name(row.friendlier_id)}.{extension}"
            width, height = create_thumbnail(
                source,
                staged,
                args.thumbnail_min_side,
                args.jpeg_quality,
                args.thumbnail_format,
            )
            destination = destination_dir / staged.name
            copy_atomic(staged, destination)

            result.update(
                status="complete",
                tiff_count=len(tiffs),
                thumbnail_path=str(destination),
                thumbnail_dimensions=f"{width}x{height}",
                thumbnail_bytes=destination.stat().st_size,
                thumbnail_sha256=sha256(destination),
                source_dimensions=source_dimensions(source),
                wgs84_bounding_box=format_union_bounds(wgs84),
                recommended_bounding_box=row.existing_bounding_box
                or format_union_bounds(wgs84),
                error="",
            )
    except Exception as error:
        result["status"] = "failed"
        result["error"] = f"{type(error).__name__}: {error}"
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    return result


def base_result(row, args) -> dict:
    return {
        "friendlier_id": row.friendlier_id,
        "label": row.label,
        "region": row.region,
        "distribution_url": row.distribution_url,
        "archive_path": row.path,
        "status": "started",
        "tiff_count": 0,
        "thumbnail_path": "",
        "thumbnail_dimensions": "",
        "thumbnail_min_side": args.thumbnail_min_side,
        "jpeg_quality": args.jpeg_quality,
        "thumbnail_bytes": 0,
        "thumbnail_sha256": "",
        "source_dimensions": "",
        "existing_bounding_box": row.existing_bounding_box,
        "wgs84_bounding_box": "",
        "recommended_bounding_box": "",
        "elapsed_seconds": 0.0,
        "error": "",
    }


def thumbnail_dimensions(width: int, height: int, minimum_side: int) -> tuple[int, int]:
    scale = minimum_side / min(width, height)
    return max(1, round(width * scale)), max(1, round(height * scale))


def create_thumbnail(
    source: Path,
    destination: Path,
    minimum_side: int,
    quality: int,
    thumbnail_format: str = "png",
) -> tuple[int, int]:
    with rasterio.open(source) as dataset:
        width, height = thumbnail_dimensions(dataset.width, dataset.height, minimum_side)
        if dataset.count >= 3:
            data = dataset.read(
                [1, 2, 3],
                out_shape=(3, height, width),
                masked=True,
                resampling=Resampling.lanczos,
            )
            rgb = np.stack([to_byte(data[index]) for index in range(3)], axis=-1)
            mask = np.any(np.ma.getmaskarray(data), axis=0)
        else:
            data = dataset.read(
                1,
                out_shape=(height, width),
                masked=True,
                resampling=Resampling.lanczos,
            )
            try:
                color_map = dataset.colormap(1)
            except ValueError:
                color_map = None
            if color_map and data.dtype == np.uint8:
                lookup = np.zeros((256, 3), dtype=np.uint8)
                for key, color in color_map.items():
                    if 0 <= key < 256:
                        lookup[key] = color[:3]
                rgb = lookup[np.ma.filled(data, 0).astype(np.uint8)]
            else:
                gray = to_byte(data)
                rgb = np.repeat(gray[..., np.newaxis], 3, axis=2)
            mask = np.ma.getmaskarray(data)
        rgb[mask] = 255
    image = Image.fromarray(rgb, mode="RGB")
    if thumbnail_format == "png":
        image.save(destination, format="PNG", compress_level=6)
    else:
        image.save(
            destination,
            format="JPEG",
            quality=quality,
            subsampling=0,
            optimize=True,
            progressive=True,
        )
    return width, height


def to_byte(data: np.ma.MaskedArray) -> np.ndarray:
    values = data.compressed()
    if not len(values):
        return np.zeros(data.shape, dtype=np.uint8)
    if data.dtype == np.uint8:
        return np.ma.filled(data, 255).astype(np.uint8)
    low, high = np.percentile(values, [2, 98])
    if high <= low:
        low, high = float(values.min()), float(values.max())
    if high <= low:
        return np.zeros(data.shape, dtype=np.uint8)
    scaled = (np.ma.filled(data, low).astype(np.float64) - low) * (255 / (high - low))
    return np.clip(scaled, 0, 255).astype(np.uint8)


def source_dimensions(source: Path) -> str:
    with rasterio.open(source) as dataset:
        return f"{dataset.width}x{dataset.height}"


def safe_extract(archive: Path, destination: Path) -> None:
    destination = destination.resolve()
    with zipfile.ZipFile(archive) as package:
        for member in package.infolist():
            target = (destination / member.filename).resolve()
            if destination not in target.parents and target != destination:
                raise RuntimeError(f"unsafe ZIP member: {member.filename}")
        package.extractall(destination)


def copy_atomic(source: Path, destination: Path) -> None:
    partial = destination.with_suffix(destination.suffix + ".part")
    shutil.copyfile(source, partial)
    os.replace(partial, destination)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_name(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in "-_." else "_"
        for character in value
    )


def migrate_legacy_thumbnail_layout(output_root: Path) -> None:
    """Move earlier region/kind thumbnails into the current flat region layout."""
    replacements: list[tuple[str, str]] = []
    for old_region, new_region in (("antarctica", "Antarctica"), ("arctic", "Arctic")):
        thumbnails = output_root / "thumbnails"
        old_region_directory = thumbnails / old_region
        old_directory = old_region_directory / "standard"
        new_directory = output_root / "thumbnails" / new_region
        if old_directory.is_dir():
            for source in old_directory.glob("*__standard.png"):
                destination = old_region_directory / source.name.replace(
                    "__standard.png", ".png"
                )
                if destination.exists():
                    raise RuntimeError(f"thumbnail migration target exists: {destination}")
                source.replace(destination)
            old_directory.rmdir()
        if old_region_directory.is_dir() and old_region_directory.name != new_region:
            temporary_region = thumbnails / f".pgc-{old_region}-rename"
            old_region_directory.replace(temporary_region)
            temporary_region.replace(new_directory)
        replacements.append((str(old_directory) + "/", str(new_directory) + "/"))

    manifests = output_root / "manifests"
    for manifest in (
        manifests / "thumbnail_bounds_standard_results.csv",
        manifests / "thumbnail_bounds_standard_results_latest.csv",
    ):
        if not manifest.exists():
            continue
        records = pd.read_csv(manifest, dtype=str).fillna("")
        paths = records["thumbnail_path"].astype(str)
        for old_prefix, new_prefix in replacements:
            paths = paths.str.replace(old_prefix, new_prefix, regex=False)
        records["thumbnail_path"] = paths.str.replace(
            "__standard.png", ".png", regex=False
        )
        temporary = manifest.with_suffix(".csv.tmp")
        records.to_csv(temporary, index=False)
        os.replace(temporary, manifest)


def append_manifest(path: Path, result: dict) -> None:
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(result))
        if write_header:
            writer.writeheader()
        writer.writerow(result)
        output.flush()
        os.fsync(output.fileno())


def load_completed(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    records = pd.read_csv(path, dtype=str).fillna("")
    latest = records.drop_duplicates("distribution_url", keep="last")
    return {
        row.distribution_url: row._asdict() for row in latest.itertuples(index=False)
    }


def valid_completed(
    record: dict | None, minimum_side: int, quality: int, thumbnail_format: str
) -> bool:
    if not record or record.get("status") != "complete":
        return False
    thumbnail = Path(record.get("thumbnail_path", ""))
    if not thumbnail.is_file() or int(record.get("thumbnail_min_side", 0)) < minimum_side:
        return False
    expected_suffix = ".png" if thumbnail_format == "png" else ".jpg"
    if thumbnail.suffix.lower() != expected_suffix:
        return False
    return thumbnail_format == "png" or int(record.get("jpeg_quality", 0)) >= quality


def write_latest_manifest(source: Path, destination: Path) -> None:
    if not source.exists():
        return
    records = pd.read_csv(source, dtype=str).fillna("")
    records.drop_duplicates("distribution_url", keep="last").to_csv(
        destination, index=False
    )


def write_status(path: Path, counters: dict, running: bool) -> None:
    selected = max(1, counters["selected"])
    data = {
        **counters,
        "running": running,
        "percent_complete": round(100 * counters["complete"] / selected, 3),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def log_message(path: Path, message: str, quiet: bool) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}"
    with path.open("a", encoding="utf-8") as output:
        output.write(line + "\n")
    if not quiet:
        print(line, flush=True)


if __name__ == "__main__":
    main()
