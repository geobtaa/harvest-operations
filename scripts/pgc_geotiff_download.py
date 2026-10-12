#!/usr/bin/env python3
"""Resumably download the complete PGC GeoTIFF ZIP inventory."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import threading
import time
import zipfile
from pathlib import Path
from urllib.parse import unquote, urlparse

import pandas as pd
import requests


USER_AGENT = "GeoBTAA-PGC-GeoTIFF-download/1.0"
LOGGER = logging.getLogger("pgc-download")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inventory",
        type=Path,
        default=Path("/Volumes/LaCie/PGC-COG-pilot/manifests/download_inventory.csv"),
    )
    parser.add_argument("--output-root", type=Path, default=Path("/Volumes/LaCie/PGC"))
    parser.add_argument(
        "--seed-root", type=Path, default=Path("/Volumes/LaCie/PGC-COG-pilot")
    )
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--detach", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_root = args.output_root.resolve()
    if args.detach and not args.worker:
        detach(args)
        return
    run_downloads(args)


def detach(args: argparse.Namespace) -> None:
    args.output_root.mkdir(parents=True, exist_ok=True)
    command = [
        "/usr/bin/caffeinate",
        "-dimsu",
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--quiet",
        "--inventory",
        str(args.inventory.resolve()),
        "--output-root",
        str(args.output_root),
        "--seed-root",
        str(args.seed_root.resolve()),
        "--workers",
        str(args.workers),
        "--retries",
        str(args.retries),
    ]
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    (args.output_root / "launcher.pid").write_text(f"{process.pid}\n", encoding="utf-8")
    print(f"PGC downloader started as process {process.pid}")
    print(f"Log: {args.output_root / 'logs' / 'download.log'}")
    print(f"Status: {args.output_root / 'manifests' / 'download_status.json'}")


def run_downloads(args: argparse.Namespace) -> None:
    archives_root = args.output_root / "source-zips"
    manifests_root = args.output_root / "manifests"
    logs_root = args.output_root / "logs"
    for directory in (archives_root, manifests_root, logs_root):
        directory.mkdir(parents=True, exist_ok=True)
    configure_logging(logs_root / "download.log", args.quiet)
    worker_pid = args.output_root / "worker.pid"
    worker_pid.write_text(f"{os.getpid()}\n", encoding="utf-8")

    manifest_path = manifests_root / "download_results.jsonl"
    status_path = manifests_root / "download_status.json"
    manifest_lock = threading.Lock()
    completed = load_completed(manifest_path)
    inventory = pd.read_csv(args.inventory, dtype=str).fillna("")
    inventory["archive_bytes"] = (
        pd.to_numeric(inventory["archive_bytes"], errors="coerce").fillna(0).astype(int)
    )
    inventory["http_status"] = (
        pd.to_numeric(inventory["http_status"], errors="coerce").fillna(0).astype(int)
    )

    available = inventory[
        (inventory["http_status"] == 200) & (inventory["archive_bytes"] > 0)
    ].copy()
    unavailable = inventory.drop(available.index)
    for row in unavailable.itertuples(index=False):
        if row.distribution_url not in completed:
            record = result_record(row, "unavailable", "", "", "inventory HTTP status")
            append_result(manifest_path, record, manifest_lock)

    rows = [
        row
        for row in available.itertuples(index=False)
        if not valid_completed_record(
            completed.get(row.distribution_url), row.archive_bytes
        )
    ]
    counters = {
        "total": len(inventory),
        "available": len(available),
        "unavailable": len(unavailable),
        "complete": len(available) - len(rows),
        "failed": 0,
        "bytes_complete": sum(
            int(record["bytes"])
            for record in completed.values()
            if record.get("status") in {"downloaded", "seeded", "existing"}
            and Path(record.get("path", "")).exists()
        ),
        "bytes_expected": int(available["archive_bytes"].sum()),
    }
    write_status(status_path, counters, running=True)
    LOGGER.info(
        "Starting: %s available, %s already complete, %.2f GiB expected",
        counters["available"],
        counters["complete"],
        counters["bytes_expected"] / 1024**3,
    )

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            future_rows = {
                pool.submit(
                    download_row,
                    row,
                    archives_root,
                    args.seed_root,
                    args.retries,
                ): row
                for row in rows
            }
            for future in concurrent.futures.as_completed(future_rows):
                row = future_rows[future]
                try:
                    record = future.result()
                except Exception as error:
                    record = result_record(
                        row,
                        "failed",
                        "",
                        "",
                        f"{type(error).__name__}: {error}",
                    )
                append_result(manifest_path, record, manifest_lock)
                if record["status"] in {"downloaded", "seeded", "existing"}:
                    counters["complete"] += 1
                    counters["bytes_complete"] += int(record["bytes"])
                else:
                    counters["failed"] += 1
                write_status(status_path, counters, running=True)
                LOGGER.info(
                    "%s/%s complete (%.2f/%.2f GiB): %s %s",
                    counters["complete"],
                    counters["available"],
                    counters["bytes_complete"] / 1024**3,
                    counters["bytes_expected"] / 1024**3,
                    record["status"],
                    record["friendlier_id"],
                )
    finally:
        write_status(status_path, counters, running=False)
        worker_pid.unlink(missing_ok=True)

    build_csv_manifest(manifest_path, manifests_root / "download_results_latest.csv")
    LOGGER.info("Finished with %s failures", counters["failed"])


def configure_logging(log_path: Path, quiet: bool) -> None:
    LOGGER.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)
    if not quiet:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        LOGGER.addHandler(console)


def download_row(row, archives_root: Path, seed_root: Path, retries: int) -> dict:
    kind = "cropped" if row.label == "Cropped GeoTIFF" else "standard"
    directory = archives_root / row.region.lower() / kind
    directory.mkdir(parents=True, exist_ok=True)
    remote_name = safe_name(Path(unquote(urlparse(row.distribution_url).path)).name)
    destination = directory / f"{safe_name(row.friendlier_id)}__{remote_name}"
    expected_size = int(row.archive_bytes)

    if destination.exists() and destination.stat().st_size == expected_size:
        validate_zip_directory(destination)
        return result_record(row, "existing", destination, sha256(destination), "")

    seed = find_seed(seed_root, row.friendlier_id, kind, expected_size)
    if seed:
        copy_atomic(seed, destination)
        validate_zip_directory(destination)
        return result_record(row, "seeded", destination, sha256(destination), "")

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            download(row.distribution_url, destination, expected_size)
            validate_zip_directory(destination)
            return result_record(
                row, "downloaded", destination, sha256(destination), ""
            )
        except Exception as error:
            last_error = error
            LOGGER.warning(
                "Attempt %s/%s failed for %s: %s",
                attempt,
                retries,
                row.friendlier_id,
                error,
            )
            time.sleep(attempt * 2)
    raise RuntimeError(f"all retries failed: {last_error}")


def download(url: str, destination: Path, expected_size: int) -> None:
    partial = destination.with_suffix(destination.suffix + ".part")
    offset = partial.stat().st_size if partial.exists() else 0
    headers = {"User-Agent": USER_AGENT}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    with requests.get(url, headers=headers, stream=True, timeout=(30, 180)) as response:
        if offset and response.status_code != 206:
            partial.unlink(missing_ok=True)
            offset = 0
        response.raise_for_status()
        with partial.open("ab" if offset else "wb") as output:
            for chunk in response.iter_content(chunk_size=4 * 1024 * 1024):
                if chunk:
                    output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
    if partial.stat().st_size != expected_size:
        raise RuntimeError(
            f"received {partial.stat().st_size} bytes; expected {expected_size}"
        )
    os.replace(partial, destination)


def find_seed(
    seed_root: Path, friendlier_id: str, kind: str, expected_size: int
) -> Path | None:
    seed_directory = seed_root / "source-zips"
    pattern = f"{safe_name(friendlier_id)}__{kind}__*"
    for path in seed_directory.glob(pattern):
        if path.is_file() and path.stat().st_size == expected_size:
            return path
    return None


def validate_zip_directory(path: Path) -> None:
    with zipfile.ZipFile(path) as package:
        if not package.infolist():
            raise RuntimeError("ZIP has no members")


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


def result_record(
    row, status: str, path: Path | str, checksum: str, error: str
) -> dict:
    return {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "friendlier_id": row.friendlier_id,
        "label": row.label,
        "region": row.region,
        "distribution_url": row.distribution_url,
        "status": status,
        "path": str(path),
        "bytes": int(row.archive_bytes),
        "sha256": checksum,
        "error": error,
    }


def append_result(path: Path, record: dict, lock: threading.Lock) -> None:
    with lock, path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(record, sort_keys=True) + "\n")
        output.flush()
        os.fsync(output.fileno())


def load_completed(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    records = {}
    with path.open(encoding="utf-8") as source:
        for line in source:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            records[record["distribution_url"]] = record
    return records


def valid_completed_record(record: dict | None, expected_size: int) -> bool:
    return bool(
        record
        and record.get("status") in {"downloaded", "seeded", "existing"}
        and int(record.get("bytes", 0)) == int(expected_size)
        and Path(record.get("path", "")).is_file()
        and Path(record["path"]).stat().st_size == int(expected_size)
    )


def write_status(path: Path, counters: dict, running: bool) -> None:
    data = {
        **counters,
        "running": running,
        "percent_files": round(100 * counters["complete"] / counters["available"], 3),
        "percent_bytes": round(
            100 * counters["bytes_complete"] / counters["bytes_expected"], 3
        ),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def build_csv_manifest(source: Path, destination: Path) -> None:
    records = load_completed(source)
    pd.DataFrame(records.values()).sort_values(
        ["region", "label", "friendlier_id"]
    ).to_csv(destination, index=False)


def safe_name(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in "-_." else "_"
        for character in value
    )


if __name__ == "__main__":
    main()
