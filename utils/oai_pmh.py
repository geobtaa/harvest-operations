"""Shared configuration helpers for OAI-PMH harvest jobs."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse


ALL_RECORDS_SET = "__all__"
DEFAULT_OAI_USER_AGENT = "BTAA-Geoportal-OAI-PMH/1.0"
JOB_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
METADATA_PREFIX_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9._-]*$")


class OaiPmhConfigError(ValueError):
    """Raised when an OAI-PMH job configuration is incomplete or invalid."""


class OaiPmhAccessError(RuntimeError):
    """Raised when an intermediary blocks access to an OAI-PMH endpoint."""


def raise_for_oai_status(response: Any) -> None:
    """Raise an actionable error when a repository blocks machine access."""
    status_code = getattr(response, "status_code", None)
    headers = getattr(response, "headers", {}) or {}
    server = normalize_space(headers.get("server", "")).lower()
    mitigation = normalize_space(headers.get("cf-mitigated", "")).lower()
    response_text = str(getattr(response, "text", "") or "")
    cloudflare_challenge = mitigation == "challenge" or (
        server == "cloudflare" and "challenge-platform" in response_text
    )
    if status_code == 403 and cloudflare_challenge:
        raise OaiPmhAccessError(
            "The repository's Cloudflare policy presented an interactive browser "
            "challenge to this OAI-PMH request. This is not a YAML or OAI protocol "
            "error, and automated harvesting cannot complete the challenge. Ask the "
            "repository owner to exempt the OAI-PMH path from browser challenges or "
            "allowlist the harvester's public IP address."
        )
    response.raise_for_status()


def normalize_space(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def slugify(value: Any) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", normalize_space(value))
    cleaned = re.sub(r"-{2,}", "-", cleaned)
    return cleaned.strip("-") or "unnamed"


def resolve_repo_path(
    path_value: str | Path,
    repo_root: Path,
    config_path: Path | None = None,
) -> Path:
    """Resolve existing repository-style paths, with YAML-relative fallback."""
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()

    repo_candidate = (repo_root / candidate).resolve()
    if repo_candidate.exists() or config_path is None:
        return repo_candidate

    config_candidate = (config_path.parent / candidate).resolve()
    if config_candidate.exists():
        return config_candidate
    return repo_candidate


def _normalize_set_entry(
    entry: Any,
    index: int,
    set_column: str,
    title_column: str,
) -> dict[str, str]:
    if isinstance(entry, str):
        set_spec = normalize_space(entry)
        set_title = ""
    elif isinstance(entry, dict):
        set_spec = normalize_space(
            entry.get("set_spec", entry.get(set_column, entry.get("set", "")))
        )
        set_title = normalize_space(
            entry.get("set_title", entry.get(title_column, entry.get("title", "")))
        )
    else:
        raise OaiPmhConfigError(
            f"sets[{index}] must be a set spec string or a mapping."
        )

    if not set_spec:
        raise OaiPmhConfigError(f"sets[{index}] is missing a set spec.")
    return {"set_spec": set_spec, "set_title": set_title}


def load_configured_sets(
    config: dict[str, Any],
    repo_root: Path,
    config_path: Path | None = None,
    *,
    required: bool = True,
) -> list[dict[str, str]]:
    """Load set definitions from inline YAML or the legacy sets CSV."""
    set_column = normalize_space(config.get("sets_csv_set_column", "set")) or "set"
    title_column = (
        normalize_space(config.get("sets_csv_title_column", "title")) or "title"
    )
    sets: list[dict[str, str]] = []

    if "sets" in config:
        inline_sets = config.get("sets")
        if not isinstance(inline_sets, list):
            raise OaiPmhConfigError("sets must be a YAML list.")
        sets = [
            _normalize_set_entry(entry, index, set_column, title_column)
            for index, entry in enumerate(inline_sets)
        ]
    elif normalize_space(config.get("sets_csv", "")):
        csv_path = resolve_repo_path(
            normalize_space(config["sets_csv"]),
            repo_root,
            config_path,
        )
        if not csv_path.is_file():
            raise OaiPmhConfigError(f"sets_csv was not found: {csv_path}")
        with csv_path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames or set_column not in reader.fieldnames:
                raise OaiPmhConfigError(
                    f"sets_csv must contain a {set_column!r} column: {csv_path}"
                )
            for row in reader:
                set_spec = normalize_space(row.get(set_column, ""))
                if not set_spec:
                    continue
                sets.append(
                    {
                        "set_spec": set_spec,
                        "set_title": normalize_space(row.get(title_column, "")),
                    }
                )

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in sets:
        if item["set_spec"] in seen:
            continue
        seen.add(item["set_spec"])
        deduped.append(item)

    if required and not deduped:
        raise OaiPmhConfigError(
            "Configure at least one inline sets entry or a non-empty sets_csv. "
            f"Use {ALL_RECORDS_SET!r} as the set spec to harvest the whole repository."
        )
    return deduped


def oai_download_directory(config: dict[str, Any], repo_root: Path) -> Path:
    configured = normalize_space(config.get("oai_download_dir", ""))
    if configured:
        return resolve_repo_path(configured, repo_root)
    return (
        repo_root / "inputs" / "oai-downloads" / slugify(config.get("name", "oai"))
    ).resolve()


def oai_status_path(config: dict[str, Any], repo_root: Path) -> Path:
    configured = normalize_space(config.get("oai_status_path", ""))
    if configured:
        return resolve_repo_path(configured, repo_root)
    return oai_download_directory(config, repo_root) / "job-status.json"


def contains_placeholder(value: Any) -> bool:
    if isinstance(value, dict):
        return any(contains_placeholder(item) for item in value.values())
    if isinstance(value, list):
        return any(contains_placeholder(item) for item in value)
    return isinstance(value, str) and bool(re.search(r"<[^<>]+>", value))


def _validate_date(value: Any, label: str) -> None:
    text = normalize_space(value)
    if not text:
        return
    try:
        date.fromisoformat(text)
    except ValueError as exc:
        raise OaiPmhConfigError(f"{label} must use YYYY-MM-DD format.") from exc


def validate_oai_job_config(
    config: dict[str, Any],
    repo_root: Path,
    config_path: Path | None = None,
    *,
    job_id: str | None = None,
    require_sets: bool = True,
    allow_placeholders: bool = False,
) -> list[dict[str, str]]:
    """Validate the shared contract used by the UI, downloader, and harvester."""
    if not isinstance(config, dict):
        raise OaiPmhConfigError("Job YAML must contain a mapping.")
    if contains_placeholder(config) and not allow_placeholders:
        raise OaiPmhConfigError(
            "Replace every <placeholder> value before running the job."
        )

    name = normalize_space(config.get("name", ""))
    if not name:
        raise OaiPmhConfigError("name is required.")
    if not JOB_ID_PATTERN.fullmatch(name):
        raise OaiPmhConfigError(
            "name must start with a letter or number and contain only letters, "
            "numbers, periods, underscores, or hyphens."
        )
    if job_id is not None and name != job_id:
        raise OaiPmhConfigError(
            f"name must remain {job_id!r} to match the YAML filename."
        )
    if config.get("type") != "oai_qdc":
        raise OaiPmhConfigError("type must be 'oai_qdc'.")

    base_url = normalize_space(config.get("oai_base_url", ""))
    parsed_url = urlparse(base_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise OaiPmhConfigError("oai_base_url must be an HTTP(S) URL.")

    metadata_prefix = normalize_space(
        config.get("metadata_prefix", config.get("feed_type", ""))
    )
    if not METADATA_PREFIX_PATTERN.fullmatch(metadata_prefix):
        raise OaiPmhConfigError(
            "metadata_prefix is required and may contain letters, numbers, periods, "
            "underscores, and hyphens."
        )

    rdf_companion = config.get("rdf_companion", {}) or {}
    if not isinstance(rdf_companion, dict):
        raise OaiPmhConfigError("rdf_companion must be a mapping.")
    if rdf_companion:
        companion_prefix = normalize_space(rdf_companion.get("metadata_prefix", ""))
        if not METADATA_PREFIX_PATTERN.fullmatch(companion_prefix):
            raise OaiPmhConfigError(
                "rdf_companion.metadata_prefix is required and must be a valid "
                "OAI metadata prefix."
            )
        for field_name in ("download_dir", "aggregation_uri", "manifest_base_url"):
            if not normalize_space(rdf_companion.get(field_name, "")):
                raise OaiPmhConfigError(f"rdf_companion.{field_name} is required.")
        for field_name in ("aggregation_uri", "manifest_base_url"):
            parsed_companion_url = urlparse(normalize_space(rdf_companion[field_name]))
            if (
                parsed_companion_url.scheme not in {"http", "https"}
                or not parsed_companion_url.netloc
            ):
                raise OaiPmhConfigError(
                    f"rdf_companion.{field_name} must be an HTTP(S) URL."
                )

    dspace_companion = config.get("dspace_bitstream_companion", {}) or {}
    if not isinstance(dspace_companion, dict):
        raise OaiPmhConfigError("dspace_bitstream_companion must be a mapping.")
    if dspace_companion:
        for field_name in (
            "rest_base_url",
            "collection_handle",
            "inventory_json",
        ):
            if not normalize_space(dspace_companion.get(field_name, "")):
                raise OaiPmhConfigError(
                    f"dspace_bitstream_companion.{field_name} is required."
                )
        parsed_dspace_url = urlparse(
            normalize_space(dspace_companion["rest_base_url"])
        )
        if (
            parsed_dspace_url.scheme not in {"http", "https"}
            or not parsed_dspace_url.netloc
        ):
            raise OaiPmhConfigError(
                "dspace_bitstream_companion.rest_base_url must be an HTTP(S) URL."
            )

    reconciliation = config.get("metadata_reconciliation", {}) or {}
    if not isinstance(reconciliation, dict):
        raise OaiPmhConfigError("metadata_reconciliation must be a mapping.")
    if reconciliation:
        if normalize_space(reconciliation.get("identity", "")) != "handle":
            raise OaiPmhConfigError(
                "metadata_reconciliation.identity must be 'handle'."
            )
        for field_name in ("existing_primary_csv", "existing_distributions_csv"):
            configured_path = normalize_space(reconciliation.get(field_name, ""))
            if not configured_path:
                raise OaiPmhConfigError(
                    f"metadata_reconciliation.{field_name} is required."
                )
            if not (allow_placeholders and contains_placeholder(configured_path)):
                resolved_path = resolve_repo_path(
                    configured_path,
                    repo_root,
                    config_path,
                )
                if not resolved_path.is_file():
                    raise OaiPmhConfigError(
                        f"metadata_reconciliation.{field_name} was not found: "
                        f"{resolved_path}"
                    )
        decisions_csv = normalize_space(reconciliation.get("decisions_csv", ""))
        if decisions_csv and not (
            allow_placeholders and contains_placeholder(decisions_csv)
        ):
            resolved_decisions = resolve_repo_path(
                decisions_csv,
                repo_root,
                config_path,
            )
            if not resolved_decisions.is_file():
                raise OaiPmhConfigError(
                    "metadata_reconciliation.decisions_csv was not found: "
                    f"{resolved_decisions}"
                )
        preserve_ids = reconciliation.get("preserve_existing_ids", [])
        if not isinstance(preserve_ids, list) or not all(
            isinstance(record_id, str) and normalize_space(record_id)
            for record_id in preserve_ids
        ):
            raise OaiPmhConfigError(
                "metadata_reconciliation.preserve_existing_ids must be a list "
                "of nonblank IDs."
            )
        exclude_fields = reconciliation.get("exclude_fields", [])
        if not isinstance(exclude_fields, list) or not all(
            isinstance(field, str) and normalize_space(field)
            for field in exclude_fields
        ):
            raise OaiPmhConfigError(
                "metadata_reconciliation.exclude_fields must be a list of field names."
            )
        if not isinstance(
            reconciliation.get("ignore_unknown_fields", False),
            bool,
        ):
            raise OaiPmhConfigError(
                "metadata_reconciliation.ignore_unknown_fields must be true or false."
            )
        new_id_registry = normalize_space(reconciliation.get("new_id_registry", ""))
        new_id_prefix = normalize_space(reconciliation.get("new_id_prefix", ""))
        if bool(new_id_registry) != bool(new_id_prefix):
            raise OaiPmhConfigError(
                "metadata_reconciliation.new_id_registry and new_id_prefix must "
                "be configured together."
            )
        if new_id_prefix and not re.fullmatch(r"[A-Za-z0-9_]+", new_id_prefix):
            raise OaiPmhConfigError(
                "metadata_reconciliation.new_id_prefix may contain only letters, "
                "numbers, and underscores."
            )

    for field_name in ("provider", "source_name", "source_id_prefix"):
        if not normalize_space(config.get(field_name, "")):
            raise OaiPmhConfigError(f"{field_name} is required.")
    for field_name in ("output_primary_csv", "output_distributions_csv"):
        if not normalize_space(config.get(field_name, "")):
            raise OaiPmhConfigError(f"{field_name} is required.")

    request_config = config.get("oai_request", {}) or {}
    if not isinstance(request_config, dict):
        raise OaiPmhConfigError("oai_request must be a mapping.")
    _validate_date(request_config.get("from", ""), "oai_request.from")
    _validate_date(request_config.get("until", ""), "oai_request.until")
    if request_config.get("from") and request_config.get("until"):
        if date.fromisoformat(str(request_config["from"])) > date.fromisoformat(
            str(request_config["until"])
        ):
            raise OaiPmhConfigError(
                "oai_request.from cannot be after oai_request.until."
            )
    for key, minimum in (("timeout_seconds", 1), ("retries", 0)):
        if key not in request_config:
            continue
        value = request_config[key]
        if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
            raise OaiPmhConfigError(
                f"oai_request.{key} must be an integer >= {minimum}."
            )
    for key in ("delay_seconds", "backoff_seconds"):
        if key not in request_config:
            continue
        value = request_config[key]
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
            raise OaiPmhConfigError(f"oai_request.{key} must be a number >= 0.")

    return load_configured_sets(
        config,
        repo_root,
        config_path,
        required=require_sets,
    )
