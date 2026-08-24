"""Browser endpoints for source-scoped OAI-PMH harvest jobs."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import tempfile
from typing import Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import yaml

from utils.oai_pmh import (
    ALL_RECORDS_SET,
    JOB_ID_PATTERN,
    OaiPmhConfigError,
    load_configured_sets,
    oai_download_directory,
    oai_status_path,
    validate_oai_job_config,
)


router = APIRouter(prefix="/jobs/oai-pmh")
LOGGER = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
JOBS_ROOT = REPO_ROOT / "config"
JOB_TEMPLATE_PATH = JOBS_ROOT / "templates" / "oai_pmh.yaml"
PIPELINE_SCRIPT = REPO_ROOT / "scripts" / "oai_pmh_pipeline.py"
PIPELINE_STAGES = frozenset(
    {"validate", "identify", "discover", "download", "harvest", "all"}
)
_active_jobs: set[str] = set()


class CreateOaiJobRequest(BaseModel):
    job_id: str


class SaveOaiJobRequest(BaseModel):
    job_id: str
    content: str
    expected_sha256: str


def format_sse_message(message: str) -> str:
    lines = str(message).splitlines() or [""]
    return "".join(f"data: {line}\n" for line in lines) + "\n"


def validate_job_id(job_id: str) -> str:
    cleaned = job_id.strip()
    if not JOB_ID_PATTERN.fullmatch(cleaned):
        raise HTTPException(
            status_code=400,
            detail=(
                "Job ID must start with a letter or number and contain only "
                "letters, numbers, periods, underscores, or hyphens."
            ),
        )
    return cleaned


def job_config_path(job_id: str) -> Path:
    cleaned = validate_job_id(job_id)
    path = (JOBS_ROOT / f"{cleaned}.yaml").resolve()
    if path.parent != JOBS_ROOT.resolve():
        raise HTTPException(status_code=400, detail="Invalid OAI-PMH job path.")
    return path


def resolve_job_config(job_id: str) -> Path:
    path = job_config_path(job_id)
    if not path.is_file():
        raise HTTPException(
            status_code=404,
            detail=f"OAI-PMH job {job_id!r} was not found.",
        )
    config = load_job_yaml(path)
    if config.get("type") != "oai_qdc":
        raise HTTPException(status_code=404, detail=f"{path.name} is not an OAI-PMH job.")
    return path


def yaml_content_sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def load_job_yaml(config_path: Path) -> dict[str, Any]:
    try:
        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Could not read {config_path.name}: {exc}",
        ) from exc
    if not isinstance(config, dict):
        raise HTTPException(
            status_code=400,
            detail=f"{config_path.name} must contain a YAML mapping.",
        )
    return config


def validate_job_yaml_content(content: str, job_id: str) -> dict[str, Any]:
    try:
        config = yaml.safe_load(content) or {}
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=400, detail=f"YAML syntax error: {exc}") from exc
    try:
        validate_oai_job_config(
            config,
            REPO_ROOT,
            job_id=job_id,
            require_sets=False,
            allow_placeholders=True,
        )
    except OaiPmhConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return config


def write_new_job_config(config_path: Path, content: str) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            config_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o644,
        )
    except FileExistsError as exc:
        raise HTTPException(
            status_code=409,
            detail=f"An OAI-PMH job named {config_path.stem!r} already exists.",
        ) from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
    except OSError:
        config_path.unlink(missing_ok=True)
        raise


def replace_job_config(config_path: Path, content: str) -> None:
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=config_path.parent,
            prefix=f".{config_path.stem}-",
            suffix=".yaml.tmp",
            delete=False,
        ) as handle:
            handle.write(content)
            temporary_path = Path(handle.name)
        temporary_path.chmod(0o644)
        os.replace(temporary_path, config_path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def summarize_job(config_path: Path) -> dict[str, Any]:
    config = load_job_yaml(config_path)
    try:
        sets = load_configured_sets(
            config,
            REPO_ROOT,
            config_path,
            required=False,
        )
        configuration_error = ""
    except OaiPmhConfigError as exc:
        sets = []
        configuration_error = str(exc)
    return {
        "id": config_path.stem,
        "label": config.get("source_name") or config.get("name") or config_path.stem,
        "config_path": str(config_path.relative_to(REPO_ROOT)),
        "base_url": str(config.get("oai_base_url", "")),
        "metadata_prefix": str(config.get("metadata_prefix", "")),
        "download_directory": str(oai_download_directory(config, REPO_ROOT)),
        "set_count": len(sets),
        "sets": sets,
        "error": configuration_error,
    }


def build_pipeline_command(
    config_path: Path,
    stage: str,
    *,
    set_spec: str = ALL_RECORDS_SET,
    python_executable: str = sys.executable,
) -> list[str]:
    if stage not in PIPELINE_STAGES:
        raise ValueError(f"Unsupported OAI-PMH stage: {stage}")
    command = [python_executable, str(PIPELINE_SCRIPT), str(config_path), stage]
    if set_spec and set_spec != ALL_RECORDS_SET:
        command.extend(["--set-spec", set_spec])
    return command


@router.get("/jobs")
async def list_oai_jobs():
    jobs = []
    for config_path in sorted(JOBS_ROOT.glob("*.yaml")):
        if config_path.resolve().parent != JOBS_ROOT.resolve():
            continue
        try:
            config = load_job_yaml(config_path)
        except HTTPException:
            continue
        if config.get("type") != "oai_qdc":
            continue
        try:
            jobs.append(summarize_job(config_path))
        except HTTPException as exc:
            jobs.append(
                {
                    "id": config_path.stem,
                    "label": config_path.stem,
                    "config_path": str(config_path.relative_to(REPO_ROOT)),
                    "base_url": "",
                    "metadata_prefix": "",
                    "download_directory": "",
                    "set_count": 0,
                    "sets": [],
                    "error": exc.detail,
                }
            )
    return {"jobs": jobs}


@router.post("/jobs", status_code=201)
async def create_oai_job(request: CreateOaiJobRequest):
    job_id = validate_job_id(request.job_id)
    config_path = job_config_path(job_id)
    if not JOB_TEMPLATE_PATH.is_file():
        raise HTTPException(status_code=500, detail="The OAI-PMH job template is missing.")
    try:
        content = JOB_TEMPLATE_PATH.read_text(encoding="utf-8").replace("<job-id>", job_id)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Could not read the job template: {exc}") from exc
    validate_job_yaml_content(content, job_id)
    write_new_job_config(config_path, content)
    return {
        "status": "created",
        "job": summarize_job(config_path),
        "content": content,
        "sha256": yaml_content_sha256(content),
    }


@router.get("/yaml")
async def load_oai_yaml(job_id: str = Query(...)):
    config_path = resolve_job_config(job_id)
    content = config_path.read_text(encoding="utf-8")
    return {
        "job_id": config_path.stem,
        "config_path": str(config_path.relative_to(REPO_ROOT)),
        "content": content,
        "sha256": yaml_content_sha256(content),
    }


@router.put("/yaml")
async def save_oai_yaml(request: SaveOaiJobRequest):
    job_id = validate_job_id(request.job_id)
    config_path = resolve_job_config(job_id)
    if job_id in _active_jobs:
        raise HTTPException(
            status_code=409,
            detail="The YAML cannot be changed while this job is running.",
        )
    existing_content = config_path.read_text(encoding="utf-8")
    if yaml_content_sha256(existing_content) != request.expected_sha256:
        raise HTTPException(
            status_code=409,
            detail=(
                "This YAML changed after it was loaded. Reload it before saving so "
                "newer changes are not overwritten."
            ),
        )
    validate_job_yaml_content(request.content, job_id)
    replace_job_config(config_path, request.content)
    return {
        "status": "saved",
        "job": summarize_job(config_path),
        "sha256": yaml_content_sha256(request.content),
    }


@router.get("/status")
async def oai_job_status(job_id: str = Query(...)):
    config_path = resolve_job_config(job_id)
    config = load_job_yaml(config_path)
    status_path = oai_status_path(config, REPO_ROOT)
    status: dict[str, Any] | None = None
    if status_path.is_file():
        try:
            value = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Could not read {status_path}: {exc}",
            ) from exc
        if isinstance(value, dict):
            status = value
    current_hash = yaml_content_sha256(config_path.read_text(encoding="utf-8"))
    return {
        "job": summarize_job(config_path),
        "status_path": str(status_path),
        "status": status,
        "configuration_current": not status or status.get("config_sha256") == current_hash,
        "running": job_id in _active_jobs,
    }


async def stream_pipeline_process(
    job_id: str,
    command: list[str],
    stage: str,
) -> AsyncIterator[str]:
    if job_id in _active_jobs:
        yield format_sse_message(f"PIPELINE_FAILED: Job {job_id!r} is already running.")
        yield format_sse_message("DONE")
        return

    _active_jobs.add(job_id)
    process: asyncio.subprocess.Process | None = None
    try:
        yield format_sse_message(f"Starting {stage} for {job_id}...")
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=REPO_ROOT,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert process.stdout is not None
        last_output_line = ""
        while line := await process.stdout.readline():
            output_line = line.decode("utf-8", errors="replace").rstrip()
            if output_line:
                last_output_line = output_line
                LOGGER.info("[%s %s] %s", job_id, stage, output_line)
            yield format_sse_message(output_line)

        return_code = await process.wait()
        if return_code == 0:
            yield format_sse_message(f"PIPELINE_COMPLETE: {stage} completed.")
        else:
            detail = last_output_line or f"process exited with status {return_code}"
            yield format_sse_message(f"PIPELINE_FAILED: {stage} failed: {detail}")
        yield format_sse_message("DONE")
    except asyncio.CancelledError:
        if process is not None and process.returncode is None:
            process.terminate()
            await process.wait()
        raise
    except OSError as exc:
        yield format_sse_message(f"PIPELINE_FAILED: Could not start the pipeline: {exc}")
        yield format_sse_message("DONE")
    finally:
        _active_jobs.discard(job_id)


@router.get("/run-stream")
async def run_oai_stage(
    job_id: str = Query(...),
    stage: str = Query(...),
    set_spec: str = Query(default=ALL_RECORDS_SET),
):
    config_path = resolve_job_config(job_id)
    if stage not in PIPELINE_STAGES:
        raise HTTPException(status_code=400, detail=f"Unsupported OAI-PMH stage {stage!r}.")
    if stage in {"download", "harvest", "all"} and set_spec != ALL_RECORDS_SET:
        summary = summarize_job(config_path)
        if not any(item["set_spec"] == set_spec for item in summary["sets"]):
            raise HTTPException(
                status_code=404,
                detail=f"Set {set_spec!r} is not configured for {job_id!r}.",
            )
    command = build_pipeline_command(config_path, stage, set_spec=set_spec)
    return StreamingResponse(
        stream_pipeline_process(job_id, command, stage),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
