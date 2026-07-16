"""Browser endpoints for the staged ArcGIS curation pipeline."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import yaml


router = APIRouter(prefix="/jobs/arcgis-curation-pipeline")
LOGGER = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
CURATION_ROOT = REPO_ROOT / "curation"
JOBS_ROOT = CURATION_ROOT / "jobs"
JOB_TEMPLATE_PATH = JOBS_ROOT / "arcgis_curation_pipeline_template.yaml"
PIPELINE_SCRIPT = CURATION_ROOT / "scripts" / "arcgis_curation_pipeline.py"
JOB_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
PIPELINE_STAGES = frozenset(
    {
        "validate",
        "metadata",
        "review",
        "download",
        "enrich",
        "dictionaries",
        "embed",
        "thumbnails",
        "derivatives",
        "postprocess",
        "snapshot",
    }
)
OVERWRITE_STAGES = frozenset({"download", "derivatives", "postprocess"})
_active_jobs: set[str] = set()


class CreateCurationJobRequest(BaseModel):
    job_id: str


class SaveCurationJobRequest(BaseModel):
    job_id: str
    content: str
    expected_sha256: str


def format_sse_message(message: str) -> str:
    """Format text as a Server-Sent Events message."""
    lines = str(message).splitlines() or [""]
    return "".join(f"data: {line}\n" for line in lines) + "\n"


def validate_job_id(job_id: str) -> str:
    cleaned_job_id = job_id.strip()
    if not JOB_ID_PATTERN.fullmatch(cleaned_job_id):
        raise HTTPException(
            status_code=400,
            detail=(
                "Job ID must start with a letter or number and contain only "
                "letters, numbers, periods, underscores, or hyphens."
            ),
        )
    return cleaned_job_id


def job_config_path(job_id: str) -> Path:
    cleaned_job_id = validate_job_id(job_id)
    config_path = (JOBS_ROOT / f"{cleaned_job_id}.yaml").resolve()
    if config_path.parent != JOBS_ROOT.resolve():
        raise HTTPException(status_code=400, detail="Invalid curation job path.")
    if config_path.name.endswith("_template.yaml"):
        raise HTTPException(status_code=400, detail="The canonical template is read-only.")
    return config_path


def resolve_job_config(job_id: str) -> Path:
    """Resolve a browser-selected job ID without allowing path traversal."""
    config_path = job_config_path(job_id)
    if config_path.parent != JOBS_ROOT.resolve() or not config_path.is_file():
        raise HTTPException(
            status_code=404,
            detail=f"ArcGIS curation job '{job_id}' was not found.",
        )
    if config_path.name.endswith("_template.yaml"):
        raise HTTPException(status_code=404, detail="The job template cannot be run.")
    return config_path


def yaml_content_sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def validate_job_yaml_content(content: str, job_id: str) -> dict[str, Any]:
    try:
        config = yaml.safe_load(content) or {}
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=400, detail=f"YAML syntax error: {exc}") from exc
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="Job YAML must contain a mapping.")
    job = config.get("job")
    if not isinstance(job, dict):
        raise HTTPException(status_code=400, detail="Job YAML must contain a job mapping.")
    configured_job_id = str(job.get("id", "")).strip()
    if configured_job_id != job_id:
        raise HTTPException(
            status_code=400,
            detail=f"job.id must remain '{job_id}' to match the YAML filename.",
        )
    return config


def write_new_job_config(config_path: Path, content: str) -> None:
    """Create a job without overwriting a file created by another request."""
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
            detail=f"A curation job named '{config_path.stem}' already exists.",
        ) from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
    except OSError:
        config_path.unlink(missing_ok=True)
        raise


def replace_job_config(config_path: Path, content: str) -> None:
    """Atomically replace a job file after its conflict check succeeds."""
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


def record_existing_manifest_config_sha256(config_path: Path, config_sha256: str) -> None:
    """Version an older manifest before its YAML is changed in the browser."""
    try:
        summary = summarize_job(config_path)
    except HTTPException:
        return
    work_directory = summary.get("work_directory")
    if not work_directory:
        return
    manifest_path = Path(str(work_directory)) / "manifest.json"
    if not manifest_path.is_file():
        return
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(manifest, dict) or manifest.get("config_sha256"):
        return
    manifest["config_sha256"] = config_sha256
    temporary_path = manifest_path.with_suffix(".json.tmp")
    temporary_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary_path.replace(manifest_path)


def load_job_yaml(config_path: Path) -> dict[str, Any]:
    try:
        with config_path.open(encoding="utf-8") as handle:
            config = yaml.safe_load(handle) or {}
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


def resolve_config_relative_path(value: str, config_path: Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (config_path.parent / path).resolve()


def summarize_job(config_path: Path) -> dict[str, Any]:
    config = load_job_yaml(config_path)
    job = config.get("job") if isinstance(config.get("job"), dict) else {}
    records = config.get("records") if isinstance(config.get("records"), list) else []
    work_value = str(job.get("work_directory", "")).strip()
    work_dir = resolve_config_relative_path(work_value, config_path) if work_value else None
    return {
        "id": config_path.stem,
        "label": str(job.get("id") or config_path.stem),
        "config_path": str(config_path.relative_to(REPO_ROOT)),
        "record_count": len(records),
        "work_directory": str(work_dir) if work_dir else "",
    }


def build_pipeline_command(
    config_path: Path,
    stage: str,
    *,
    confirm: bool = False,
    overwrite: bool = False,
    uv_executable: str = "uv",
) -> list[str]:
    if stage not in PIPELINE_STAGES:
        raise ValueError(f"Unsupported ArcGIS curation stage: {stage}")
    command = [
        uv_executable,
        "run",
        "--project",
        str(CURATION_ROOT),
        "python",
        str(PIPELINE_SCRIPT),
        str(config_path),
        stage,
    ]
    if stage == "review" and confirm:
        command.append("--confirm")
    if stage in OVERWRITE_STAGES and overwrite:
        command.append("--overwrite")
    return command


@router.get("/jobs")
async def list_arcgis_curation_jobs():
    jobs = []
    for config_path in sorted(JOBS_ROOT.glob("*.yaml")):
        if config_path.name.endswith("_template.yaml"):
            continue
        if config_path.resolve().parent != JOBS_ROOT.resolve():
            continue
        try:
            jobs.append(summarize_job(config_path))
        except HTTPException as exc:
            jobs.append(
                {
                    "id": config_path.stem,
                    "label": config_path.stem,
                    "config_path": str(config_path.relative_to(REPO_ROOT)),
                    "record_count": 0,
                    "work_directory": "",
                    "error": exc.detail,
                }
            )
    return {"jobs": jobs}


@router.post("/jobs", status_code=201)
async def create_arcgis_curation_job(request: CreateCurationJobRequest):
    job_id = validate_job_id(request.job_id)
    config_path = job_config_path(job_id)
    if not JOB_TEMPLATE_PATH.is_file():
        raise HTTPException(status_code=500, detail="The canonical job template is missing.")
    try:
        template_content = JOB_TEMPLATE_PATH.read_text(encoding="utf-8")
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Could not read the canonical job template: {exc}",
        ) from exc
    content = template_content.replace("<job-id>", job_id)
    validate_job_yaml_content(content, job_id)
    write_new_job_config(config_path, content)
    return {
        "status": "created",
        "job": summarize_job(config_path),
        "content": content,
        "sha256": yaml_content_sha256(content),
    }


@router.get("/yaml")
async def load_arcgis_curation_yaml(job_id: str = Query(...)):
    config_path = resolve_job_config(job_id)
    try:
        content = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Could not read {config_path.name}: {exc}",
        ) from exc
    return {
        "job_id": config_path.stem,
        "config_path": str(config_path.relative_to(REPO_ROOT)),
        "content": content,
        "sha256": yaml_content_sha256(content),
    }


@router.put("/yaml")
async def save_arcgis_curation_yaml(request: SaveCurationJobRequest):
    job_id = validate_job_id(request.job_id)
    config_path = resolve_job_config(job_id)
    if job_id in _active_jobs:
        raise HTTPException(
            status_code=409,
            detail="The YAML cannot be changed while this job is running.",
        )
    try:
        existing_content = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Could not read {config_path.name}: {exc}",
        ) from exc
    if yaml_content_sha256(existing_content) != request.expected_sha256:
        raise HTTPException(
            status_code=409,
            detail=(
                "This YAML changed after it was loaded. Reload it before saving so "
                "the newer changes are not overwritten."
            ),
        )
    validate_job_yaml_content(request.content, job_id)
    if request.content != existing_content:
        record_existing_manifest_config_sha256(
            config_path,
            yaml_content_sha256(existing_content),
        )
    replace_job_config(config_path, request.content)
    return {
        "status": "saved",
        "job": summarize_job(config_path),
        "sha256": yaml_content_sha256(request.content),
    }


@router.get("/status")
async def arcgis_curation_status(job_id: str = Query(...)):
    config_path = resolve_job_config(job_id)
    summary = summarize_job(config_path)
    if not summary["work_directory"]:
        raise HTTPException(
            status_code=400,
            detail=f"{config_path.name} is missing job.work_directory.",
        )
    work_dir = Path(summary["work_directory"])
    manifest_path = work_dir / "manifest.json"
    metadata_path = work_dir / "metadata" / "metadata.csv"

    manifest: dict[str, Any] | None = None
    configuration_current = True
    if manifest_path.is_file():
        try:
            manifest_value = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Could not read {manifest_path}: {exc}",
            ) from exc
        if isinstance(manifest_value, dict):
            manifest = manifest_value
            recorded_config_sha256 = manifest.get("config_sha256")
            if recorded_config_sha256:
                configuration_current = (
                    recorded_config_sha256
                    == yaml_content_sha256(config_path.read_text(encoding="utf-8"))
                )

    return {
        "job": summary,
        "state": "in_progress" if manifest else "not_started",
        "metadata_path": str(metadata_path),
        "metadata_exists": metadata_path.is_file(),
        "manifest_path": str(manifest_path),
        "manifest": manifest,
        "configuration_current": configuration_current,
        "running": job_id in _active_jobs,
    }


async def stream_pipeline_process(
    job_id: str,
    command: list[str],
    stage: str,
) -> AsyncIterator[str]:
    if job_id in _active_jobs:
        yield format_sse_message(
            f"PIPELINE_FAILED: Job '{job_id}' already has a stage running."
        )
        yield format_sse_message("DONE")
        return

    _active_jobs.add(job_id)
    process: asyncio.subprocess.Process | None = None
    try:
        yield format_sse_message(f"Starting {stage} for {job_id}...")
        env = os.environ.copy()
        env.pop("VIRTUAL_ENV", None)
        env["PYTHONUNBUFFERED"] = "1"
        env["UV_NO_PROGRESS"] = "1"
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
            failure_detail = last_output_line or f"process exited with status {return_code}"
            yield format_sse_message(
                f"PIPELINE_FAILED: {stage} failed: {failure_detail}"
            )
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
async def run_arcgis_curation_stage(
    job_id: str = Query(...),
    stage: str = Query(...),
    confirm: bool = Query(default=False),
    overwrite: bool = Query(default=False),
):
    config_path = resolve_job_config(job_id)
    if stage not in PIPELINE_STAGES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported ArcGIS curation stage '{stage}'.",
        )
    if stage == "review" and not confirm:
        raise HTTPException(
            status_code=400,
            detail="Manual review must be explicitly confirmed.",
        )

    uv_executable = shutil.which("uv")
    if not uv_executable:
        raise HTTPException(
            status_code=500,
            detail="The uv executable is required to run the curation environment.",
        )
    command = build_pipeline_command(
        config_path,
        stage,
        confirm=confirm,
        overwrite=overwrite,
        uv_executable=uv_executable,
    )
    return StreamingResponse(
        stream_pipeline_process(job_id, command, stage),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
