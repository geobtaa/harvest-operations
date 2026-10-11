#!/usr/bin/env python3
"""Download a stratified PGC sample and convert its GeoTIFFs to COGs."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time
import zipfile
from xml.etree import ElementTree
from pathlib import Path
from urllib.parse import unquote, urlparse

import pandas as pd
import rasterio
import requests
from rasterio.warp import transform_bounds
from rasterio.warp import transform as transform_coordinates
from shapely.geometry import MultiPoint, Point


USER_AGENT = "GeoBTAA-PGC-COG-pilot/1.0"
TIFF_SUFFIXES = {".tif", ".tiff"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--primary",
        type=Path,
        default=Path("outputs/2026-10-09_pgc_primary.csv"),
    )
    parser.add_argument(
        "--distributions",
        type=Path,
        default=Path("outputs/2026-10-09_pgc_distributions.csv"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("/Volumes/LaCie/PGC-COG-pilot"),
    )
    parser.add_argument("--count", type=int, default=15)
    parser.add_argument("--timeout", type=int, default=60)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root = args.output_root.resolve()
    archives_dir = output_root / "source-zips"
    cogs_dir = output_root / "cogs"
    manifests_dir = output_root / "manifests"
    for directory in (archives_dir, cogs_dir, manifests_dir):
        directory.mkdir(parents=True, exist_ok=True)

    primary = pd.read_csv(args.primary, dtype=str).fillna("")
    distributions = pd.read_csv(args.distributions, dtype=str).fillna("")
    candidates = build_candidates(primary, distributions)

    inventory_path = manifests_dir / "download_inventory.csv"
    inventory = inventory_downloads(candidates, inventory_path, args.timeout)
    selection = select_pilot(inventory, args.count)
    selection_path = manifests_dir / "pilot_selection.csv"
    selection.to_csv(selection_path, index=False)

    print(
        f"Selected {len(selection)} products totaling "
        f"{selection['archive_bytes'].sum() / 1024**3:.2f} GiB"
    )

    manifest_path = manifests_dir / "pilot_results.csv"
    existing = load_completed(manifest_path)
    for position, row in enumerate(selection.itertuples(index=False), 1):
        key = (row.friendlier_id, row.label, row.distribution_url)
        if key in existing:
            print(
                f"[{position}/{len(selection)}] already complete: {row.friendlier_id}"
            )
            continue
        print(
            f"[{position}/{len(selection)}] {row.friendlier_id} ({row.region}, "
            f"{row.label}, {row.archive_bytes / 1024**2:.1f} MiB)"
        )
        result = process_product(row, archives_dir, cogs_dir, args.timeout)
        append_manifest(manifest_path, result)
        print(
            f"  {result['status']}: {result['cog_bytes'] / 1024**2:.1f} MiB COG, "
            f"{result['elapsed_seconds']:.1f} seconds"
        )

    summarize(manifest_path, output_root)


def build_candidates(
    primary: pd.DataFrame, distributions: pd.DataFrame
) -> pd.DataFrame:
    downloads = distributions[
        distributions["label"].isin(["GeoTIFF", "Cropped GeoTIFF"])
    ].copy()
    downloads["region"] = downloads["distribution_url"].map(
        lambda url: "Antarctica" if "/antarctica/" in url else "Arctic"
    )
    downloads = downloads.merge(
        primary[["ID", "Bounding Box"]],
        left_on="friendlier_id",
        right_on="ID",
        how="left",
        validate="many_to_one",
    )
    downloads["bbox_status"] = downloads["Bounding Box"].map(
        lambda value: "existing" if value else "missing"
    )
    return downloads.drop(columns=["ID", "Bounding Box"]).drop_duplicates(
        ["friendlier_id", "label", "distribution_url"]
    )


def inventory_downloads(
    candidates: pd.DataFrame, inventory_path: Path, timeout: int
) -> pd.DataFrame:
    cached: dict[str, tuple[int, int]] = {}
    if inventory_path.exists():
        prior = pd.read_csv(
            inventory_path, dtype={"http_status": int, "archive_bytes": int}
        )
        cached = {
            row.distribution_url: (int(row.http_status), int(row.archive_bytes))
            for row in prior.itertuples(index=False)
        }

    records = []
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    for position, row in enumerate(candidates.itertuples(index=False), 1):
        status, size = cached.get(row.distribution_url, (0, 0))
        if not size:
            try:
                response = session.head(
                    row.distribution_url, allow_redirects=True, timeout=timeout
                )
                status = response.status_code
                size = int(response.headers.get("Content-Length", 0))
            except requests.RequestException:
                status, size = 0, 0
        records.append({**row._asdict(), "http_status": status, "archive_bytes": size})
        if position % 200 == 0:
            print(f"Inventoried {position}/{len(candidates)} downloads")

    inventory = pd.DataFrame(records)
    inventory.to_csv(inventory_path, index=False)
    return inventory


def select_pilot(inventory: pd.DataFrame, count: int) -> pd.DataFrame:
    available = inventory.query("http_status == 200 and archive_bytes > 0").copy()
    selected: list[pd.Series] = []
    used: set[tuple[str, str]] = set()

    groups = [
        ("GeoTIFF", region, bbox_status)
        for region in ("Antarctica", "Arctic")
        for bbox_status in ("missing", "existing")
    ]
    quantiles = (0.1, 0.5, 0.9)
    for label, region, bbox_status in groups:
        group = available[
            (available["label"] == label)
            & (available["region"] == region)
            & (available["bbox_status"] == bbox_status)
        ].sort_values("archive_bytes")
        for quantile in quantiles:
            add_nearest_quantile(group, quantile, selected, used)

    cropped = available[available["label"] == "Cropped GeoTIFF"].sort_values(
        "archive_bytes"
    )
    for quantile in quantiles:
        add_nearest_quantile(cropped, quantile, selected, used)

    if len(selected) < count:
        remainder = available.sort_values("archive_bytes")
        for _, row in remainder.iterrows():
            key = (row["friendlier_id"], row["label"])
            if key not in used:
                selected.append(row)
                used.add(key)
            if len(selected) >= count:
                break

    columns = list(inventory.columns)
    return pd.DataFrame(selected[:count], columns=columns).sort_values(
        ["region", "label", "archive_bytes"]
    )


def add_nearest_quantile(
    group: pd.DataFrame,
    quantile: float,
    selected: list[pd.Series],
    used: set[tuple[str, str]],
) -> None:
    if group.empty:
        return
    target = group["archive_bytes"].quantile(quantile)
    for _, row in (
        group.assign(distance=(group["archive_bytes"] - target).abs())
        .sort_values("distance")
        .iterrows()
    ):
        key = (row["friendlier_id"], row["label"])
        if key not in used:
            selected.append(row.drop(labels="distance"))
            used.add(key)
            return


def process_product(row, archives_dir: Path, cogs_dir: Path, timeout: int) -> dict:
    started = time.monotonic()
    safe_id = safe_name(row.friendlier_id)
    kind = "cropped" if row.label == "Cropped GeoTIFF" else "standard"
    remote_name = safe_name(Path(unquote(urlparse(row.distribution_url).path)).name)
    archive_path = archives_dir / f"{safe_id}__{kind}__{remote_name}"
    result = base_result(row, archive_path)
    try:
        download(row.distribution_url, archive_path, int(row.archive_bytes), timeout)
        result["archive_sha256"] = sha256(archive_path)
        with tempfile.TemporaryDirectory(prefix=f"pgc-{safe_id}-") as temporary:
            work_dir = Path(temporary)
            extract_dir = work_dir / "extracted"
            extract_dir.mkdir()
            safe_extract(archive_path, extract_dir)
            tiffs = sorted(
                path
                for path in extract_dir.rglob("*")
                if path.suffix.lower() in TIFF_SUFFIXES
            )
            if not tiffs:
                raise RuntimeError("archive contains no TIFF files")

            outputs = []
            all_bounds = []
            for index, source_tiff in enumerate(tiffs, 1):
                suffix = f"-{index}" if len(tiffs) > 1 else ""
                cog_name = f"{safe_id}__{kind}{suffix}.tif"
                staged_cog = work_dir / cog_name
                source_bounds = derive_wgs84_bounds(source_tiff)
                create_cog(source_tiff, staged_cog)
                details = inspect_raster(staged_cog)
                if not details["is_cog"]:
                    raise RuntimeError(f"GDAL did not report COG layout for {cog_name}")
                destination = cogs_dir / cog_name
                copy_atomic(staged_cog, destination)
                details["path"] = str(destination)
                details["sha256"] = sha256(destination)
                details["wgs84_bounds"] = source_bounds
                outputs.append(details)
                all_bounds.append(details["wgs84_bounds"])

            result.update(
                status="complete",
                tiff_count=len(tiffs),
                cog_paths="|".join(item["path"] for item in outputs),
                cog_bytes=sum(int(item["bytes"]) for item in outputs),
                cog_sha256="|".join(item["sha256"] for item in outputs),
                crs="|".join(dict.fromkeys(item["crs"] for item in outputs)),
                derived_bounding_box=format_union_bounds(all_bounds),
                error="",
            )
    except Exception as error:
        result["status"] = "failed"
        result["error"] = f"{type(error).__name__}: {error}"
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    return result


def base_result(row, archive_path: Path) -> dict:
    return {
        "friendlier_id": row.friendlier_id,
        "label": row.label,
        "region": row.region,
        "bbox_status": row.bbox_status,
        "distribution_url": row.distribution_url,
        "archive_path": str(archive_path),
        "archive_bytes": int(row.archive_bytes),
        "archive_sha256": "",
        "status": "started",
        "tiff_count": 0,
        "cog_paths": "",
        "cog_bytes": 0,
        "cog_sha256": "",
        "crs": "",
        "derived_bounding_box": "",
        "elapsed_seconds": 0.0,
        "error": "",
    }


def download(url: str, destination: Path, expected_size: int, timeout: int) -> None:
    if destination.exists() and destination.stat().st_size == expected_size:
        return
    partial = destination.with_suffix(destination.suffix + ".part")
    offset = partial.stat().st_size if partial.exists() else 0
    headers = {"User-Agent": USER_AGENT}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    with requests.get(url, headers=headers, stream=True, timeout=timeout) as response:
        if offset and response.status_code != 206:
            offset = 0
            partial.unlink(missing_ok=True)
        response.raise_for_status()
        mode = "ab" if offset else "wb"
        with partial.open(mode) as output:
            for chunk in response.iter_content(chunk_size=4 * 1024 * 1024):
                if chunk:
                    output.write(chunk)
    if partial.stat().st_size != expected_size:
        raise RuntimeError(
            f"download size {partial.stat().st_size} differs from expected {expected_size}"
        )
    os.replace(partial, destination)


def safe_extract(archive: Path, destination: Path) -> None:
    destination = destination.resolve()
    with zipfile.ZipFile(archive) as package:
        for member in package.infolist():
            target = (destination / member.filename).resolve()
            if destination not in target.parents and target != destination:
                raise RuntimeError(f"unsafe ZIP member: {member.filename}")
        package.extractall(destination)


def create_cog(source: Path, destination: Path) -> None:
    creation_options = [
        "-of",
        "COG",
        "-co",
        "COMPRESS=DEFLATE",
        "-co",
        "LEVEL=6",
        "-co",
        "NUM_THREADS=ALL_CPUS",
        "-co",
        "BIGTIFF=IF_SAFER",
        "-co",
        "OVERVIEWS=AUTO",
        "-co",
        "RESAMPLING=AVERAGE",
    ]
    with rasterio.open(source) as dataset:
        source_crs = dataset.crs
        gcps, gcp_crs = dataset.gcps

    if source_crs is None and gcps and gcp_crs:
        target_x = [gcp.x for gcp in gcps]
        target_y = [gcp.y for gcp in gcps]
        command = [
            "gdalwarp",
            *creation_options,
            "-t_srs",
            gcp_crs.to_wkt(),
            "-te_srs",
            gcp_crs.to_wkt(),
            "-te",
            str(min(target_x)),
            str(min(target_y)),
            str(max(target_x)),
            str(max(target_y)),
            "-order",
            "1",
            "-r",
            "bilinear",
            "-multi",
            "-wo",
            "NUM_THREADS=ALL_CPUS",
            str(source),
            str(destination),
        ]
    else:
        command = ["gdal_translate", *creation_options]
        if source_crs is None:
            sidecar_crs = crs_from_arcgis_sidecar(source)
            if sidecar_crs:
                command.extend(["-a_srs", sidecar_crs])
        command.extend([str(source), str(destination)])
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())


def derive_wgs84_bounds(source: Path) -> tuple[float, float, float, float]:
    with rasterio.open(source) as dataset:
        if dataset.crs:
            return transform_bounds(
                dataset.crs,
                "EPSG:4326",
                *dataset.bounds,
                densify_pts=41,
            )
        gcps, gcp_crs = dataset.gcps
    if not gcps or not gcp_crs:
        raise RuntimeError(f"raster has no usable georeferencing: {source.name}")

    target_points = [(gcp.x, gcp.y) for gcp in gcps]
    longitude, latitude = transform_coordinates(
        gcp_crs,
        "EPSG:4326",
        [point[0] for point in target_points],
        [point[1] for point in target_points],
    )
    pole_inside = MultiPoint(target_points).convex_hull.contains(Point(0, 0))
    if pole_inside:
        _, pole_latitude = transform_coordinates(gcp_crs, "EPSG:4326", [0], [0])
        latitude.append(pole_latitude[0])
        return -180.0, min(latitude), 180.0, max(latitude)
    return min(longitude), min(latitude), max(longitude), max(latitude)


def crs_from_arcgis_sidecar(source: Path) -> str | None:
    """Read a CRS that ArcGIS stored only in a TIFF PAM sidecar."""
    sidecar = source.with_suffix(source.suffix + ".aux.xml")
    if not sidecar.exists():
        return None
    try:
        root = ElementTree.parse(sidecar).getroot()
    except ElementTree.ParseError:
        return None
    wkt = root.findtext(".//GeodataXform/SpatialReference/WKT")
    return wkt.strip() if wkt else None


def inspect_raster(path: Path) -> dict:
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
    is_cog = (
        metadata.get("metadata", {}).get("IMAGE_STRUCTURE", {}).get("LAYOUT") == "COG"
    )
    with rasterio.open(path) as dataset:
        if dataset.crs is None:
            raise RuntimeError(f"raster has no CRS: {path.name}")
        bounds = transform_bounds(
            dataset.crs,
            "EPSG:4326",
            *dataset.bounds,
            densify_pts=41,
        )
        return {
            "bytes": path.stat().st_size,
            "crs": dataset.crs.to_string(),
            "wgs84_bounds": bounds,
            "is_cog": is_cog,
        }


def format_union_bounds(bounds: list[tuple[float, float, float, float]]) -> str:
    bounds = [
        (-180.0, item[1], 180.0, item[3]) if item[2] < item[0] else item
        for item in bounds
    ]
    west = min(item[0] for item in bounds)
    south = min(item[1] for item in bounds)
    east = max(item[2] for item in bounds)
    north = max(item[3] for item in bounds)
    return f"{west:.6f},{south:.6f},{east:.6f},{north:.6f}"


def copy_atomic(source: Path, destination: Path) -> None:
    partial = destination.with_suffix(destination.suffix + ".part")
    shutil.copy2(source, partial)
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


def load_completed(manifest_path: Path) -> set[tuple[str, str, str]]:
    if not manifest_path.exists():
        return set()
    prior = pd.read_csv(manifest_path, dtype=str).fillna("")
    return {
        (row.friendlier_id, row.label, row.distribution_url)
        for row in prior[prior["status"] == "complete"].itertuples(index=False)
    }


def append_manifest(manifest_path: Path, result: dict) -> None:
    write_header = not manifest_path.exists()
    with manifest_path.open("a", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(result))
        if write_header:
            writer.writeheader()
        writer.writerow(result)
        output.flush()
        os.fsync(output.fileno())


def summarize(manifest_path: Path, output_root: Path) -> None:
    results = pd.read_csv(manifest_path, dtype=str).fillna("")
    latest = results.drop_duplicates(
        ["friendlier_id", "label", "distribution_url"], keep="last"
    ).copy()
    latest["derived_bounding_box"] = latest["derived_bounding_box"].map(
        normalize_bbox_string
    )
    latest.to_csv(output_root / "manifests" / "pilot_results_latest.csv", index=False)
    complete = latest[latest["status"] == "complete"].copy()
    complete["archive_bytes"] = pd.to_numeric(complete["archive_bytes"])
    complete["cog_bytes"] = pd.to_numeric(complete["cog_bytes"])
    summary = {
        "output_root": str(output_root),
        "selected": int(len(latest)),
        "complete": int(len(complete)),
        "failed": int((latest["status"] == "failed").sum()),
        "archive_gib": float(complete["archive_bytes"].sum() / 1024**3),
        "cog_gib": float(complete["cog_bytes"].sum() / 1024**3),
        "cog_to_archive_ratio": float(
            complete["cog_bytes"].sum() / complete["archive_bytes"].sum()
        )
        if len(complete)
        else None,
    }
    summary_path = output_root / "manifests" / "pilot_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


def normalize_bbox_string(value: str) -> str:
    if not value:
        return value
    west, south, east, north = (float(part) for part in value.split(","))
    if east < west:
        west, east = -180.0, 180.0
    return f"{west:.6f},{south:.6f},{east:.6f},{north:.6f}"


if __name__ == "__main__":
    main()
