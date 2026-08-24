"""Browser endpoints for the staged Socrata curation pipeline."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from routers import arcgis_curation as shared


router = APIRouter(prefix="/jobs/socrata-curation-pipeline")

REPO_ROOT = Path(__file__).resolve().parents[1]
CURATION_ROOT = REPO_ROOT / "curation"
JOBS_ROOT = CURATION_ROOT / "jobs" / "socrata"
JOB_TEMPLATE_PATH = JOBS_ROOT / "socrata_curation_pipeline_template.yaml"
PIPELINE_SCRIPT = CURATION_ROOT / "scripts" / "socrata_curation_pipeline.py"
PIPELINE_STAGES = shared.PIPELINE_STAGES
OVERWRITE_STAGES = shared.OVERWRITE_STAGES
_active_jobs = shared._active_jobs

CreateCurationJobRequest = shared.CreateCurationJobRequest
SaveCurationJobRequest = shared.SaveCurationJobRequest
format_sse_message = shared.format_sse_message
stream_pipeline_process = shared.stream_pipeline_process
validate_job_id = shared.validate_job_id
validate_job_yaml_content = shared.validate_job_yaml_content
write_new_job_config = shared.write_new_job_config
replace_job_config = shared.replace_job_config
load_job_yaml = shared.load_job_yaml
resolve_config_relative_path = shared.resolve_config_relative_path


def job_config_path(job_id: str) -> Path:
    cleaned_job_id = validate_job_id(job_id)
    config_path = (JOBS_ROOT / f"{cleaned_job_id}.yaml").resolve()
    if config_path.parent != JOBS_ROOT.resolve():
        raise HTTPException(status_code=400, detail="Invalid Socrata curation job path.")
    if config_path.name.endswith("_template.yaml"):
        raise HTTPException(status_code=400, detail="The canonical template is read-only.")
    return config_path


def resolve_job_config(job_id: str) -> Path:
    """Resolve a browser-selected Socrata job without allowing path traversal."""
    config_path = job_config_path(job_id)
    if config_path.parent != JOBS_ROOT.resolve() or not config_path.is_file():
        raise HTTPException(
            status_code=404,
            detail=f"Socrata curation job '{job_id}' was not found.",
        )
    if config_path.name.endswith("_template.yaml"):
        raise HTTPException(status_code=404, detail="The job template cannot be run.")
    return config_path


def yaml_content_sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


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


def record_existing_manifest_config_sha256(
    config_path: Path,
    config_sha256: str,
) -> None:
    """Version an older manifest before its YAML changes in the browser."""
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


def build_pipeline_command(
    config_path: Path,
    stage: str,
    *,
    confirm: bool = False,
    overwrite: bool = False,
    uv_executable: str = "uv",
) -> list[str]:
    if stage not in PIPELINE_STAGES:
        raise ValueError(f"Unsupported Socrata curation stage: {stage}")
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
async def list_socrata_curation_jobs():
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
async def create_socrata_curation_job(request: CreateCurationJobRequest):
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
async def load_socrata_curation_yaml(job_id: str = Query(...)):
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
async def save_socrata_curation_yaml(request: SaveCurationJobRequest):
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
async def socrata_curation_status(job_id: str = Query(...)):
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


@router.get("/run-stream")
async def run_socrata_curation_stage(
    job_id: str = Query(...),
    stage: str = Query(...),
    confirm: bool = Query(default=False),
    overwrite: bool = Query(default=False),
):
    config_path = resolve_job_config(job_id)
    if stage not in PIPELINE_STAGES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported Socrata curation stage '{stage}'.",
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
