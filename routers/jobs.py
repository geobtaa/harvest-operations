from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
import csv
import os
from pathlib import Path
import yaml

from harvesters.arcgis import ArcGISHarvester
from harvesters.ckan import CkanHarvester
from harvesters.socrata import SocrataHarvester
from harvesters.pasda import PasdaHarvester
from harvesters.pasda_portal import PasdaPortalHarvester
from harvesters.ogm_aardvark import OgmAardvarkHarvester
from harvesters.ogmWisc import OgmWiscHarvester
from harvesters.hdx import HdxHarvester
from harvesters.isgs import IsgsHarvester
from harvesters.chicago_luna import ChicagoLunaHarvester
from harvesters.hyrax import HyraxHarvester
from harvesters.oai_qdc import OaiQdcHarvester
from harvesters.standalone_websites import StandaloneWebsiteLinkChecker
from dashboard.harvest_task_dashboard import HarvestTaskDashboardJob


HARVESTER_REGISTRY = {
    "arcgis": ArcGISHarvester,
    "ckan": CkanHarvester,
    "socrata": SocrataHarvester,
    "pasda": PasdaHarvester,
    "pasda-portal": PasdaPortalHarvester,
    "ogm_aardvark": OgmAardvarkHarvester,
    "ogmWisc": OgmWiscHarvester,
    "hdx": HdxHarvester,
    "isgs": IsgsHarvester,
    "chicago-luna": ChicagoLunaHarvester,
    "hyrax": HyraxHarvester,
    "oai_qdc": OaiQdcHarvester,
    "standalone_websites": StandaloneWebsiteLinkChecker,
    "task_dashboard": HarvestTaskDashboardJob,

}


router = APIRouter()


OGM_GITHUB_JOB_IDS = {"ogmWisc", "ogm-aardvark"}
OGM_GITHUB_ALLOWED_OVERRIDES = {
    "source_mode",
    "github_owner",
    "github_repo",
    "github_ref",
    "github_branch",
    "github_recent_commits",
    "github_since",
    "github_until",
    "github_path",
    "github_token_env",
}


def load_job_config(job_id: str) -> dict:
    config_path = os.path.join("config", f"{job_id}.yaml")
    if not os.path.exists(config_path):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_ogm_repo_options(csv_path: str = "config/ogm-repos.csv") -> list[dict]:
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="OGM repository CSV not found")

    repositories = []
    with open(csv_path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            repository = str(row.get("Repository", "")).strip()
            if not repository:
                continue

            repositories.append(
                {
                    "repository": repository,
                    "code": str(row.get("Code", "")).strip(),
                    "member_of": str(row.get("Member Of", "")).strip(),
                }
            )

    return repositories


async def load_job_overrides(job_id: str, request: Request) -> dict:
    if job_id not in OGM_GITHUB_JOB_IDS:
        return {}

    if request.headers.get("content-length") in (None, "0"):
        return {}

    try:
        payload = await request.json()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON.") from exc

    if not payload:
        return {}
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object.")

    unknown = sorted(set(payload) - OGM_GITHUB_ALLOWED_OVERRIDES)
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported {job_id} option(s): {', '.join(unknown)}",
        )

    overrides = {key: value for key, value in payload.items() if value not in ("", None)}
    source_mode = overrides.get("source_mode")
    if source_mode and source_mode not in {"local_json", "github_tarball", "github_commits"}:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported {job_id} source_mode '{source_mode}'.",
        )

    if "github_recent_commits" in overrides:
        try:
            overrides["github_recent_commits"] = int(overrides["github_recent_commits"])
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail="github_recent_commits must be a whole number.",
            ) from exc
        if overrides["github_recent_commits"] < 1:
            raise HTTPException(
                status_code=400,
                detail="github_recent_commits must be at least 1.",
            )

    return overrides

@router.get("/jobs")
async def list_jobs():
    """
    List available harvesting jobs by scanning the config/ folder.
    """
    config_files = sorted(f for f in os.listdir("config") if f.endswith(".yaml"))
    jobs = []
    for filename in config_files:
        job_id = os.path.splitext(filename)[0]
        config_path = os.path.join("config", filename)
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        jobs.append({
            "id": job_id,
            "name": config.get("name", job_id)
        })
    return jobs


@router.get("/jobs/ogm-aardvark/repositories")
async def list_ogm_aardvark_repositories():
    return {"repositories": load_ogm_repo_options()}

@router.post("/jobs/{job_id}/run")
async def run_job(job_id: str, request: Request):
    """
    Run a harvesting job by ID, loading its configuration from the config/ folder.
    """
    # Load job configuration
    job_cfg = load_job_config(job_id)
    job_cfg.update(await load_job_overrides(job_id, request))

    # Load schema and instantiate the correct harvester
    # schema = load_local_schema()
    harvester_type = job_cfg.get("type")

    harvester_cls = HARVESTER_REGISTRY.get(harvester_type)
    if not harvester_cls:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported harvester type '{harvester_type}'",
        )

    harvester = harvester_cls(job_cfg)


    # Run the full harvester workflow (fetch, normalize, write outputs)
    results = harvester.harvest_pipeline()

    return {"status": "completed", **results}


@router.get("/jobs/harvest-task-dashboard/view", response_class=HTMLResponse)
async def view_harvest_task_dashboard(
    embedded: bool = Query(default=False),
    report: str = Query(default="full"),
    workflow: str = Query(default=""),
    report_date: str = Query(default=""),
):
    job_cfg = load_job_config("harvest-task-dashboard")
    dashboard_job = HarvestTaskDashboardJob(job_cfg)
    try:
        return HTMLResponse(
            content=dashboard_job.render_dashboard_view(
                embedded=embedded,
                report_type=report,
                workflow=workflow,
                report_date=report_date,
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/reports/{filename:path}")
async def view_report_file(filename: str):
    report_path = Path("reports", filename)
    reports_root = Path("reports").resolve()
    resolved_report_path = report_path.resolve()
    if reports_root not in resolved_report_path.parents or not resolved_report_path.is_file():
        raise HTTPException(status_code=404, detail="Report not found")
    return FileResponse(resolved_report_path)


@router.get("/jobs/harvest-task-dashboard/workflow-queue")
async def harvest_task_dashboard_workflow_queue():
    job_cfg = load_job_config("harvest-task-dashboard")
    dashboard_job = HarvestTaskDashboardJob(job_cfg)
    try:
        return dashboard_job.build_workflow_queue()
    except FileNotFoundError as exc:
        missing_file = exc.filename or str(exc)
        raise HTTPException(
            status_code=400,
            detail=f"Missing dashboard input file: {missing_file}",
        ) from exc


@router.get("/jobs/harvest-task-dashboard/reports/{workflow}", response_class=HTMLResponse)
async def view_harvest_workflow_report_archive(workflow: str):
    job_cfg = load_job_config("harvest-task-dashboard")
    dashboard_job = HarvestTaskDashboardJob(job_cfg)
    try:
        return HTMLResponse(content=dashboard_job.render_workflow_report_archive(workflow))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/jobs/harvest-task-dashboard/frequent-harvesters")
async def frequent_harvesters():
    job_cfg = load_job_config("harvest-task-dashboard")
    return HarvestTaskDashboardJob(job_cfg).build_frequent_harvesters()
