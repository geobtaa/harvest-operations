#!/usr/bin/env python3
"""Run and track source-scoped stages for an OAI-PMH harvest job."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from harvesters.oai_qdc import OaiQdcHarvester  # noqa: E402
from scripts.oai_download import (  # noqa: E402
    configure_retry_session,
    download_set,
    write_text,
)
from scripts.oai_list_sets import fetch_all_sets  # noqa: E402
from utils.oai_pmh import (  # noqa: E402
    ALL_RECORDS_SET,
    load_configured_sets,
    normalize_space,
    oai_download_directory,
    oai_status_path,
    raise_for_oai_status,
    slugify,
    validate_oai_job_config,
)


OAI_NS = {"oai": "http://www.openarchives.org/OAI/2.0/"}
PIPELINE_STAGES = frozenset(
    {"validate", "identify", "discover", "download", "harvest", "all"}
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_config(config_path: Path) -> dict[str, Any]:
    with config_path.open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    if not isinstance(config, dict):
        raise ValueError("Job YAML must contain a mapping.")
    return config


def load_status(config: dict[str, Any]) -> dict[str, Any]:
    path = oai_status_path(config, REPO_ROOT)
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_status(config: dict[str, Any], status: dict[str, Any]) -> Path:
    path = oai_status_path(config, REPO_ROOT)
    write_text(path, json.dumps(status, indent=2) + "\n")
    return path


def begin_stage(
    config: dict[str, Any],
    config_path: Path,
    stage: str,
    set_spec: str,
) -> dict[str, Any]:
    config_hash = file_sha256(config_path)
    status = load_status(config)
    if status.get("config_sha256") != config_hash:
        status = {"stages": {}}
    status.update(
        {
            "job_id": normalize_space(config.get("name")) or config_path.stem,
            "source_name": normalize_space(config.get("source_name")) or config_path.stem,
            "config_path": str(config_path.resolve()),
            "config_sha256": config_hash,
            "download_directory": str(oai_download_directory(config, REPO_ROOT)),
            "updated_at": utc_now(),
        }
    )
    stages = status.setdefault("stages", {})
    stages[stage] = {
        "status": "running",
        "started_at": utc_now(),
        "set_spec": set_spec,
    }
    write_status(config, status)
    return status


def finish_stage(
    config: dict[str, Any],
    status: dict[str, Any],
    stage: str,
    result: dict[str, Any],
) -> dict[str, Any]:
    stage_status = status.setdefault("stages", {}).setdefault(stage, {})
    stage_status.update(
        {
            "status": "completed",
            "completed_at": utc_now(),
            "result": result,
        }
    )
    status["updated_at"] = utc_now()
    write_status(config, status)
    return result


def fail_stage(
    config: dict[str, Any],
    status: dict[str, Any],
    stage: str,
    exc: Exception,
) -> None:
    stage_status = status.setdefault("stages", {}).setdefault(stage, {})
    stage_status.update(
        {
            "status": "failed",
            "failed_at": utc_now(),
            "error": f"{type(exc).__name__}: {exc}",
        }
    )
    status["updated_at"] = utc_now()
    write_status(config, status)


def select_sets(
    config: dict[str, Any],
    config_path: Path,
    set_spec: str,
) -> list[dict[str, str]]:
    configured = load_configured_sets(config, REPO_ROOT, config_path)
    if not set_spec or set_spec == ALL_RECORDS_SET:
        return configured
    selected = [item for item in configured if item["set_spec"] == set_spec]
    if not selected:
        raise ValueError(f"Set {set_spec!r} is not configured for {config['name']}.")
    return selected


def scoped_output_filename(path_value: str, set_spec: str) -> str:
    """Keep a one-set test output from replacing the source's full output."""
    path = Path(path_value)
    scope = slugify(set_spec)
    for table_suffix in ("_primary", "_distributions"):
        if path.stem.endswith(table_suffix):
            source_stem = path.stem[: -len(table_suffix)]
            return str(
                path.with_name(f"{source_stem}-{scope}{table_suffix}{path.suffix}")
            )
    return str(path.with_name(f"{path.stem}-{scope}{path.suffix}"))


def request_settings(config: dict[str, Any]) -> dict[str, Any]:
    configured = config.get("oai_request", {}) or {}
    return {
        "delay": float(configured.get("delay_seconds", 1.0)),
        "timeout": int(configured.get("timeout_seconds", 60)),
        "retries": int(configured.get("retries", 3)),
        "backoff": float(configured.get("backoff_seconds", 1.0)),
        "from_date": normalize_space(configured.get("from", "")) or None,
        "until_date": normalize_space(configured.get("until", "")) or None,
    }


def run_identify(config: dict[str, Any]) -> dict[str, Any]:
    settings = request_settings(config)
    session = configure_retry_session(settings["retries"], settings["backoff"])
    response = session.get(
        config["oai_base_url"],
        params={"verb": "Identify"},
        timeout=settings["timeout"],
    )
    raise_for_oai_status(response)
    root = ET.fromstring(response.text)
    errors = [
        f"{item.attrib.get('code', 'oai_error')}: {normalize_space(item.text)}"
        for item in root.findall(".//oai:error", OAI_NS)
    ]
    if errors:
        raise RuntimeError("OAI Identify returned errors: " + "; ".join(errors))
    identify = root.find(".//oai:Identify", OAI_NS)
    if identify is None:
        raise RuntimeError("The endpoint response did not contain OAI-PMH Identify metadata.")

    def text(name: str) -> str:
        element = identify.find(f"oai:{name}", OAI_NS)
        return normalize_space(element.text if element is not None else "")

    result = {
        "repository_name": text("repositoryName"),
        "base_url": text("baseURL"),
        "protocol_version": text("protocolVersion"),
        "earliest_datestamp": text("earliestDatestamp"),
        "deleted_record_policy": text("deletedRecord"),
        "granularity": text("granularity"),
    }

    formats_response = session.get(
        config["oai_base_url"],
        params={"verb": "ListMetadataFormats"},
        timeout=settings["timeout"],
    )
    raise_for_oai_status(formats_response)
    formats_root = ET.fromstring(formats_response.text)
    format_errors = [
        f"{item.attrib.get('code', 'oai_error')}: {normalize_space(item.text)}"
        for item in formats_root.findall(".//oai:error", OAI_NS)
    ]
    if format_errors:
        raise RuntimeError(
            "OAI ListMetadataFormats returned errors: " + "; ".join(format_errors)
        )
    metadata_prefixes = [
        normalize_space(item.text)
        for item in formats_root.findall(".//oai:metadataPrefix", OAI_NS)
        if normalize_space(item.text)
    ]
    if config["metadata_prefix"] not in metadata_prefixes:
        raise RuntimeError(
            f"The endpoint does not advertise metadataPrefix={config['metadata_prefix']!r}. "
            f"Available prefixes: {', '.join(metadata_prefixes) or 'none'}"
        )
    result["metadata_formats"] = metadata_prefixes
    print(
        "[OAI-PMH] Identified "
        f"{result['repository_name'] or config['source_name']} "
        f"(protocol {result['protocol_version'] or 'unknown'})."
    )
    return result


def run_discover(config: dict[str, Any]) -> dict[str, Any]:
    settings = request_settings(config)
    session = configure_retry_session(settings["retries"], settings["backoff"])
    discovered = fetch_all_sets(
        session=session,
        base_url=config["oai_base_url"],
        delay=settings["delay"],
        timeout=settings["timeout"],
    )
    output_path = oai_download_directory(config, REPO_ROOT) / "discovered_sets.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f".{output_path.name}.tmp")
    with temporary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["set", "title"])
        writer.writeheader()
        writer.writerows(
            {"set": item["set_spec"], "title": item["set_title"]}
            for item in discovered
        )
    temporary_path.replace(output_path)
    print(f"[OAI-PMH] Discovered {len(discovered)} set(s):")
    for item in discovered:
        title = f" — {item['set_title']}" if item["set_title"] else ""
        print(f"[OAI-PMH] - {item['set_spec']}{title}")
    print(f"[OAI-PMH] Wrote the complete set list to {output_path}.")
    preview_limit = 200
    return {
        "set_count": len(discovered),
        "sets_csv": str(output_path),
        "sets_preview": [
            {"set_spec": item["set_spec"], "set_title": item["set_title"]}
            for item in discovered[:preview_limit]
        ],
        "preview_truncated": len(discovered) > preview_limit,
    }


def run_download(
    config: dict[str, Any],
    config_path: Path,
    set_spec: str,
) -> dict[str, Any]:
    selected_sets = select_sets(config, config_path, set_spec)
    settings = request_settings(config)
    output_dir = oai_download_directory(config, REPO_ROOT)
    output_dir.mkdir(parents=True, exist_ok=True)
    session = configure_retry_session(settings["retries"], settings["backoff"])
    manifests = []
    for item in selected_sets:
        print(f"[OAI-PMH] Downloading {item['set_spec']}...")
        manifest = download_set(
            session=session,
            base_url=config["oai_base_url"],
            metadata_prefix=config["metadata_prefix"],
            set_spec=item["set_spec"],
            set_title=item["set_title"],
            output_dir=output_dir,
            delay=settings["delay"],
            timeout=settings["timeout"],
            from_date=settings["from_date"],
            until_date=settings["until_date"],
        )
        manifests.append(manifest)
        print(
            f"[OAI-PMH] Saved {len(manifest['downloaded_files'])} page(s) "
            f"for {item['set_spec']}."
        )

    run_manifest = {
        "job_id": config["name"],
        "source_name": config["source_name"],
        "base_url": config["oai_base_url"],
        "metadata_prefix": config["metadata_prefix"],
        "downloaded_at": utc_now(),
        "from": settings["from_date"] or "",
        "until": settings["until_date"] or "",
        "sets": manifests,
        "error_count": sum(item["error_count"] for item in manifests),
    }
    manifest_path = output_dir / "manifest.json"
    write_text(manifest_path, json.dumps(run_manifest, indent=2) + "\n")
    if run_manifest["error_count"]:
        raise RuntimeError(
            f"OAI-PMH returned {run_manifest['error_count']} error(s); see {manifest_path}."
        )
    return {
        "set_count": len(manifests),
        "page_count": sum(len(item["downloaded_files"]) for item in manifests),
        "manifest_path": str(manifest_path),
    }


def run_harvest(
    config: dict[str, Any],
    config_path: Path,
    set_spec: str,
) -> dict[str, Any]:
    selected_sets = select_sets(config, config_path, set_spec)
    run_config = dict(config)
    run_config.pop("sets_csv", None)
    run_config["sets"] = [
        {"set": item["set_spec"], "title": item["set_title"]}
        for item in selected_sets
    ]
    if set_spec != ALL_RECORDS_SET:
        run_config["build_uploads"] = False
        for output_key in ("output_primary_csv", "output_distributions_csv"):
            run_config[output_key] = scoped_output_filename(
                run_config[output_key],
                set_spec,
            )
    print(
        f"[OAI-PMH] Harvesting {len(selected_sets)} configured set(s) "
        f"for {config['source_name']}."
    )
    results = OaiQdcHarvester(run_config).harvest_pipeline()
    return {key: value for key, value in results.items() if isinstance(value, (str, int, float, bool, dict, list))}


def execute_stage(config_path: Path, stage: str, set_spec: str = ALL_RECORDS_SET) -> dict[str, Any]:
    config_path = config_path.resolve()
    config = load_config(config_path)
    status = begin_stage(config, config_path, stage, set_spec)
    try:
        validate_oai_job_config(
            config,
            REPO_ROOT,
            config_path,
            job_id=config_path.stem,
            require_sets=stage in {"download", "harvest", "all"},
        )
        if stage == "validate":
            configured_sets = load_configured_sets(
                config,
                REPO_ROOT,
                config_path,
                required=False,
            )
            result = {
                "set_count": len(configured_sets),
                "ready_for_harvest": bool(configured_sets),
                "status_path": str(oai_status_path(config, REPO_ROOT)),
            }
            if configured_sets:
                print(
                    f"[OAI-PMH] Configuration is valid for {config['source_name']} "
                    f"with {len(configured_sets)} configured set(s)."
                )
            else:
                print(
                    f"[OAI-PMH] Base configuration is valid for {config['source_name']}. "
                    "No sets are configured yet; run Discover sets, copy the desired "
                    "setSpec into the YAML, save, and validate again before downloading."
                )
        elif stage == "identify":
            result = run_identify(config)
        elif stage == "discover":
            result = run_discover(config)
        elif stage == "download":
            result = run_download(config, config_path, set_spec)
        elif stage == "harvest":
            result = run_harvest(config, config_path, set_spec)
        elif stage == "all":
            download_status = begin_stage(config, config_path, "download", set_spec)
            try:
                download_result = run_download(config, config_path, set_spec)
                finish_stage(config, download_status, "download", download_result)
            except Exception as exc:
                fail_stage(config, download_status, "download", exc)
                raise
            harvest_status = begin_stage(config, config_path, "harvest", set_spec)
            try:
                harvest_result = run_harvest(config, config_path, set_spec)
                finish_stage(config, harvest_status, "harvest", harvest_result)
            except Exception as exc:
                fail_stage(config, harvest_status, "harvest", exc)
                raise
            status = load_status(config)
            result = {"download": download_result, "harvest": harvest_result}
        else:
            raise ValueError(f"Unsupported OAI-PMH stage: {stage}")
        return finish_stage(config, status, stage, result)
    except Exception as exc:
        fail_stage(config, load_status(config) or status, stage, exc)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a staged OAI-PMH source job.")
    parser.add_argument("config", type=Path, help="Path to an OAI-PMH YAML job.")
    parser.add_argument("stage", choices=sorted(PIPELINE_STAGES))
    parser.add_argument(
        "--set-spec",
        default=ALL_RECORDS_SET,
        help="Run one configured set; the default runs all configured sets.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    os.chdir(REPO_ROOT)
    try:
        result = execute_stage(args.config, args.stage, args.set_spec)
    except Exception as exc:
        print(f"PIPELINE_ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(f"PIPELINE_COMPLETE: {args.stage}: {json.dumps(result, sort_keys=True)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
