#!/usr/bin/env python3
"""Create inspection COGs, thumbnails, and derived bounds from PGC ZIPs."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
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
    TARGET_CRS,
    WEB_MERCATOR_HALF_WORLD,
    derive_wgs84_bounds,
    format_union_bounds,
    web_mercator_bounds,
)


TIFF_SUFFIXES = {".tif", ".tiff"}
COMPLETE_STATUSES = {"complete"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--downloads-manifest",
        type=Path,
        default=Path("/Volumes/LaCie/PGC/manifests/download_results_latest.csv"),
    )
    parser.add_argument(
        "--primary",
        type=Path,
        default=Path("outputs/2026-10-09_pgc_primary.csv"),
    )
    parser.add_argument("--output-root", type=Path, default=Path("/Volumes/LaCie/PGC"))
    parser.add_argument(
        "--scratch-root",
        type=Path,
        default=Path.home() / "Desktop" / "PGC-COG-work",
    )
    parser.add_argument(
        "--kind", choices=("standard", "cropped", "all"), default="standard"
    )
    parser.add_argument("--ids", nargs="*", default=[])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--large-pixel-count", type=int, default=100_000_000)
    parser.add_argument("--max-dimension", type=int, default=12_000)
    parser.add_argument("--max-cog-mib", type=int, default=200)
    parser.add_argument("--thumbnail-size", type=int, default=512)
    parser.add_argument("--jpeg-quality", type=int, default=85)
    parser.add_argument("--detach", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
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
    (manifests / f"cog_{args.kind}_launcher.pid").write_text(
        f"{process.pid}\n", encoding="utf-8"
    )
    print(f"PGC derivative processor started as process {process.pid}")
    print(f"Status: {manifests / f'cog_{args.kind}_status.json'}")
    print(f"Log: {args.output_root / 'logs' / f'cog_{args.kind}.log'}")


def process_inventory(args: argparse.Namespace) -> None:
    output_root = args.output_root.resolve()
    scratch_root = args.scratch_root.resolve()
    manifests = output_root / "manifests"
    logs = output_root / "logs"
    for directory in (scratch_root, manifests, logs):
        directory.mkdir(parents=True, exist_ok=True)

    log_path = logs / f"cog_{args.kind}.log"
    manifest_path = manifests / f"cog_{args.kind}_results.csv"
    latest_path = manifests / f"cog_{args.kind}_results_latest.csv"
    status_path = manifests / f"cog_{args.kind}_status.json"
    worker_pid = manifests / f"cog_{args.kind}_worker.pid"
    worker_pid.write_text(f"{os.getpid()}\n", encoding="utf-8")

    downloads = pd.read_csv(args.downloads_manifest, dtype=str).fillna("")
    primary = pd.read_csv(args.primary, dtype=str).fillna("")
    candidates = build_candidates(downloads, primary, args.kind, args.ids)
    if args.limit is not None:
        candidates = candidates.head(args.limit)
    completed = load_completed(manifest_path)
    pending = [
        row
        for row in candidates.itertuples(index=False)
        if not valid_completed(completed.get(row.distribution_url))
    ]
    counters = {
        "selected": len(candidates),
        "complete": len(candidates) - len(pending),
        "failed": 0,
        "source_bytes": int(candidates["bytes"].astype(int).sum()),
        "cog_bytes": sum(
            int(record.get("cog_bytes", 0))
            for url, record in completed.items()
            if url in set(candidates["distribution_url"]) and valid_completed(record)
        ),
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
                f"[{position}/{len(candidates)}] {row.friendlier_id} "
                f"({int(row.bytes) / 1024**2:.1f} MiB archive)",
                args.quiet,
            )
            result = process_archive(row, args, scratch_root, output_root)
            append_manifest(manifest_path, result)
            if result["status"] == "complete":
                counters["complete"] += 1
                counters["cog_bytes"] += int(result["cog_bytes"])
            else:
                counters["failed"] += 1
            write_status(status_path, counters, running=True)
            log_message(
                log_path,
                f"  {result['status']}: {int(result['cog_bytes']) / 1024**2:.1f} "
                f"MiB COG; {result['elapsed_seconds']} seconds"
                + (f"; {result['error']}" if result["error"] else ""),
                args.quiet,
            )
    finally:
        write_latest_manifest(manifest_path, latest_path)
        write_status(status_path, counters, running=False)
        worker_pid.unlink(missing_ok=True)


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
    candidates["bbox_status"] = candidates["existing_bounding_box"].map(
        lambda value: "existing" if value else "missing"
    )
    return candidates.sort_values(["bytes", "friendlier_id"]).reset_index(drop=True)


def process_archive(
    row, args: argparse.Namespace, scratch_root: Path, output_root: Path
) -> dict:
    started = time.monotonic()
    result = base_result(row)
    kind = "cropped" if row.label == "Cropped GeoTIFF" else "standard"
    region = row.region.lower()
    cogs_dir = output_root / "cogs" / region / kind
    thumbnails_dir = output_root / "thumbnails" / region / kind
    cogs_dir.mkdir(parents=True, exist_ok=True)
    thumbnails_dir.mkdir(parents=True, exist_ok=True)

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

            outputs = []
            bounds = []
            for index, source in enumerate(tiffs, 1):
                suffix = f"-{index}" if len(tiffs) > 1 else ""
                stem = f"{safe_name(row.friendlier_id)}__{kind}{suffix}"
                staged_cog = work_dir / f"{stem}.tif"
                derived_bounds = derive_wgs84_bounds(source)
                details = create_display_cog(
                    source, staged_cog, work_dir, args, derived_bounds
                )
                validate_cog(staged_cog)
                destination = cogs_dir / staged_cog.name
                copy_atomic(staged_cog, destination)
                output = {
                    **details,
                    "path": str(destination),
                    "bytes": destination.stat().st_size,
                    "sha256": sha256(destination),
                    "wgs84_bounds": derived_bounds,
                }
                outputs.append(output)
                bounds.append(derived_bounds)

                if index == 1:
                    staged_thumbnail = (
                        work_dir / f"{safe_name(row.friendlier_id)}__{kind}.jpg"
                    )
                    create_thumbnail(
                        staged_cog,
                        staged_thumbnail,
                        args.thumbnail_size,
                        args.jpeg_quality,
                    )
                    thumbnail_destination = thumbnails_dir / staged_thumbnail.name
                    copy_atomic(staged_thumbnail, thumbnail_destination)
                    result["thumbnail_path"] = str(thumbnail_destination)
                    result["thumbnail_bytes"] = thumbnail_destination.stat().st_size
                    result["thumbnail_sha256"] = sha256(thumbnail_destination)

            derived_bbox = format_union_bounds(bounds)
            result.update(
                status="complete",
                tiff_count=len(tiffs),
                cog_paths="|".join(item["path"] for item in outputs),
                cog_bytes=sum(int(item["bytes"]) for item in outputs),
                cog_sha256="|".join(item["sha256"] for item in outputs),
                source_dimensions="|".join(
                    f"{item['source_width']}x{item['source_height']}"
                    for item in outputs
                ),
                output_dimensions="|".join(
                    f"{item['output_width']}x{item['output_height']}"
                    for item in outputs
                ),
                derivative_mode="|".join(item["mode"] for item in outputs),
                compression="|".join(item["compression"] for item in outputs),
                crs="|".join(dict.fromkeys(item["crs"] for item in outputs)),
                derived_bounding_box=derived_bbox,
                recommended_bounding_box=row.existing_bounding_box or derived_bbox,
                error="",
            )
    except Exception as error:
        result["status"] = "failed"
        result["error"] = f"{type(error).__name__}: {error}"
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    return result


def base_result(row) -> dict:
    return {
        "friendlier_id": row.friendlier_id,
        "label": row.label,
        "region": row.region,
        "bbox_status": row.bbox_status,
        "distribution_url": row.distribution_url,
        "archive_path": row.path,
        "archive_bytes": int(row.bytes),
        "status": "started",
        "tiff_count": 0,
        "cog_paths": "",
        "cog_bytes": 0,
        "cog_sha256": "",
        "source_dimensions": "",
        "output_dimensions": "",
        "derivative_mode": "",
        "compression": "",
        "thumbnail_path": "",
        "thumbnail_bytes": 0,
        "thumbnail_sha256": "",
        "crs": "",
        "derived_bounding_box": "",
        "recommended_bounding_box": "",
        "elapsed_seconds": 0.0,
        "error": "",
    }


def create_display_cog(
    source: Path,
    destination: Path,
    work_dir: Path,
    args: argparse.Namespace,
    wgs84_bounds: tuple[float, float, float, float],
) -> dict:
    with rasterio.open(source) as dataset:
        source_width, source_height = dataset.width, dataset.height
        count = dataset.count
        gcps, gcp_crs = dataset.gcps
        color_interpretations = tuple(dataset.colorinterp)
    use_gcps = bool(gcps) and gcp_crs is not None
    source_for_warp, rgb_band_count = prepare_warp_source(
        source, work_dir, count, color_interpretations
    )
    target_bounds = web_mercator_bounds(wgs84_bounds)
    output_width, output_height = planned_projected_dimensions(
        target_bounds,
        source_width * source_height,
        args.large_pixel_count,
        args.max_dimension,
    )
    reduced = output_width * output_height < source_width * source_height
    compression = "DEFLATE"

    for attempt in range(5):
        destination.unlink(missing_ok=True)
        run_cog_conversion(
            source_for_warp,
            destination,
            output_width,
            output_height,
            target_bounds,
            rgb_band_count,
            use_gcps,
            work_dir,
        )
        maximum_bytes = args.max_cog_mib * 1024**2
        if destination.stat().st_size <= maximum_bytes:
            break
        scale = min(0.9, math.sqrt(maximum_bytes / destination.stat().st_size) * 0.92)
        new_width = max(512, int(output_width * scale))
        new_height = max(512, int(output_height * scale))
        if (new_width, new_height) == (output_width, output_height):
            break
        output_width, output_height = new_width, new_height
        reduced = True
    else:
        raise RuntimeError("unable to create COG under configured size ceiling")

    if destination.stat().st_size > args.max_cog_mib * 1024**2:
        raise RuntimeError(
            f"COG remains larger than {args.max_cog_mib} MiB after downsampling"
        )

    with rasterio.open(destination) as dataset:
        crs = dataset.crs
        if crs is None:
            raise RuntimeError("output COG has no CRS")
    return {
        "source_width": source_width,
        "source_height": source_height,
        "output_width": output_width,
        "output_height": output_height,
        "mode": "display_reduced" if reduced else "source_resolution",
        "compression": compression,
        "crs": crs.to_string(),
    }


def planned_dimensions(
    width: int,
    height: int,
    maximum_pixels: int,
    maximum_dimension: int,
) -> tuple[int, int]:
    scale = min(
        1.0,
        maximum_dimension / max(width, height),
        math.sqrt(maximum_pixels / (width * height)),
    )
    return max(1, round(width * scale)), max(1, round(height * scale))


def planned_projected_dimensions(
    bounds: tuple[float, float, float, float],
    source_pixels: int,
    maximum_pixels: int,
    maximum_dimension: int,
) -> tuple[int, int]:
    west, south, east, north = bounds
    aspect_ratio = (east - west) / (north - south)
    pixel_budget = min(source_pixels, maximum_pixels)
    width = math.sqrt(pixel_budget * aspect_ratio)
    height = width / aspect_ratio
    scale = min(1.0, maximum_dimension / width, maximum_dimension / height)
    return max(1, round(width * scale)), max(1, round(height * scale))


def prepare_warp_source(
    source: Path, work_dir: Path, count: int, color_interpretations: tuple
) -> tuple[Path, int]:
    if count == 1 and color_interpretations[0].name == "palette":
        expanded = work_dir / f"{safe_name(source.stem)}-rgb.vrt"
        run_command(
            [
                "gdal_translate",
                "-q",
                "-of",
                "VRT",
                "-expand",
                "rgb",
                str(source),
                str(expanded),
            ]
        )
        return expanded, 3
    return source, 3 if count >= 3 else 1


def run_cog_conversion(
    source: Path,
    destination: Path,
    width: int,
    height: int,
    target_bounds: tuple[float, float, float, float],
    rgb_band_count: int,
    use_gcps: bool,
    work_dir: Path,
) -> None:
    warped_vrt = work_dir / f"{destination.stem}-warped.vrt"
    warped_vrt.unlink(missing_ok=True)
    warp_command = [
        "gdalwarp",
        "-q",
        "-of",
        "VRT",
        "-t_srs",
        TARGET_CRS,
        "-te_srs",
        TARGET_CRS,
        "-te",
        *(format(value, ".17g") for value in target_bounds),
        "-ts",
        str(width),
        str(height),
        "-r",
        "average",
        "-srcnodata",
        "none",
        "-dstalpha",
        "-multi",
        "-wo",
        "NUM_THREADS=ALL_CPUS",
    ]
    if rgb_band_count == 3:
        warp_command.extend(
            ["-srcband", "1", "-srcband", "2", "-srcband", "3"]
        )
    if use_gcps:
        warp_command.extend(["-order", "1"])
    warp_command.extend([str(source), str(warped_vrt)])
    run_command(warp_command)

    if rgb_band_count == 3:
        band_arguments = ["-b", "1", "-b", "2", "-b", "3", "-mask", "4"]
    else:
        band_arguments = ["-b", "1", "-b", "1", "-b", "1", "-mask", "2"]
    command = [
        "gdal_translate",
        "-q",
        "-of",
        "COG",
        *band_arguments,
        "-colorinterp",
        "red,green,blue",
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
        str(warped_vrt),
        str(destination),
    ]
    run_command(command)


def create_thumbnail(
    source: Path, destination: Path, maximum_size: int, quality: int
) -> None:
    with rasterio.open(source) as dataset:
        scale = min(maximum_size / dataset.width, maximum_size / dataset.height, 1.0)
        width = max(1, round(dataset.width * scale))
        height = max(1, round(dataset.height * scale))
        if dataset.count >= 3:
            data = dataset.read(
                [1, 2, 3],
                out_shape=(3, height, width),
                masked=True,
                resampling=Resampling.average,
            )
            rgb = np.stack([to_byte(data[index]) for index in range(3)], axis=-1)
            mask = np.any(np.ma.getmaskarray(data), axis=0)
        else:
            data = dataset.read(
                1,
                out_shape=(height, width),
                masked=True,
                resampling=Resampling.average,
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
    Image.fromarray(rgb, mode="RGB").save(
        destination, format="JPEG", quality=quality, optimize=True
    )


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


def validate_cog(path: Path) -> None:
    command = [
        "gdalinfo",
        "--config",
        "GDAL_PAM_ENABLED",
        "NO",
        "-json",
        str(path),
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=True)
    metadata = json.loads(completed.stdout)
    structure = metadata.get("metadata", {}).get("IMAGE_STRUCTURE", {})
    if structure.get("LAYOUT") != "COG":
        raise RuntimeError(f"GDAL did not report COG layout for {path.name}")
    if str(structure.get("COMPRESSION", "")).upper() != "DEFLATE":
        raise RuntimeError(f"COG does not use DEFLATE compression: {path.name}")
    if str(structure.get("INTERLEAVE") or "PIXEL").upper() != "PIXEL":
        raise RuntimeError(f"COG is not pixel-interleaved: {path.name}")
    bands = metadata.get("bands", [])
    colors = [band.get("colorInterpretation") for band in bands]
    if colors != ["Red", "Green", "Blue"]:
        raise RuntimeError(f"COG is not three-band RGB: {path.name}")
    mask_flags = bands[0].get("mask", {}).get("flags", []) if bands else []
    if "PER_DATASET" not in mask_flags:
        raise RuntimeError(f"COG has no internal dataset mask: {path.name}")
    with rasterio.open(path) as dataset:
        if dataset.crs is None or dataset.crs.to_epsg() != 3857:
            raise RuntimeError(f"COG is not EPSG:3857: {path.name}")


def safe_extract(archive: Path, destination: Path) -> None:
    destination = destination.resolve()
    with zipfile.ZipFile(archive) as package:
        for member in package.infolist():
            target = (destination / member.filename).resolve()
            if destination not in target.parents and target != destination:
                raise RuntimeError(f"unsafe ZIP member: {member.filename}")
        package.extractall(destination)


def run_command(command: list[str]) -> None:
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())


def copy_atomic(source: Path, destination: Path) -> None:
    partial = destination.with_suffix(destination.suffix + ".part")
    # exFAT rejects some macOS metadata copied by shutil.copy2(); only the
    # derivative's bytes need to cross filesystems before the atomic rename.
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


def valid_completed(record: dict | None) -> bool:
    if not record or record.get("status") not in COMPLETE_STATUSES:
        return False
    if record.get("crs") != TARGET_CRS or set(
        filter(None, record.get("compression", "").split("|"))
    ) != {"DEFLATE"}:
        return False
    cogs = [Path(path) for path in record.get("cog_paths", "").split("|") if path]
    thumbnail = Path(record.get("thumbnail_path", ""))
    return bool(cogs and all(path.is_file() for path in cogs) and thumbnail.is_file())


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
        "source_gib": round(counters["source_bytes"] / 1024**3, 3),
        "cog_gib": round(counters["cog_bytes"] / 1024**3, 3),
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
