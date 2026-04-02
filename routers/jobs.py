from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse
import os
import yaml

from utils.file_io import load_local_schema

from harvesters.arcgis import ArcGISHarvester
from harvesters.ckan import CkanHarvester
from harvesters.socrata import SocrataHarvester
from harvesters.pasda import PasdaHarvester
from harvesters.ogmWisc import OgmWiscHarvester
from harvesters.hdx import HdxHarvester
from harvesters.isgs import IsgsHarvester
from harvesters.chicago_luna import ChicagoLunaHarvester
from harvesters.hyrax import HyraxHarvester
from harvesters.oai_qdc import OaiQdcHarvester
from scripts.harvest_task_dashboard import HarvestTaskDashboardJob


HARVESTER_REGISTRY = {
    "arcgis": ArcGISHarvester,
    "ckan": CkanHarvester,
    "socrata": SocrataHarvester,
    "pasda": PasdaHarvester,
    "ogmWisc": OgmWiscHarvester,
    "hdx": HdxHarvester,
    "isgs": IsgsHarvester,
    "chicago-luna": ChicagoLunaHarvester,
    "hyrax": HyraxHarvester,
    "oai_qdc": OaiQdcHarvester,
    "task_dashboard": HarvestTaskDashboardJob,

}


router = APIRouter()


def load_job_config(job_id: str) -> dict:
    config_path = os.path.join("config", f"{job_id}.yaml")
    if not os.path.exists(config_path):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)

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

@router.post("/jobs/{job_id}/run")
async def run_job(job_id: str):
    """
    Run a harvesting job by ID, loading its configuration from the config/ folder.
    """
    # Load job configuration
    job_cfg = load_job_config(job_id)

    # Load schema and instantiate the correct harvester
    # schema = load_local_schema()
    harvester_type = job_cfg.get("type")

    harvester_cls = HARVESTER_REGISTRY.get(harvester_type)
    if not harvester_cls:
        raise HTTPException(status_code=400, detail=f"Unsupported harvester type '{harvester_type}'")

    harvester = harvester_cls(job_cfg)


    # Run the full harvester workflow (fetch, normalize, write outputs)
    results = harvester.harvest_pipeline()

    return {"status": "completed", **results}


@router.get("/jobs/harvest-task-dashboard/view", response_class=HTMLResponse)
async def view_harvest_task_dashboard(
    embedded: bool = Query(default=False),
    report: str = Query(default="full"),
    workflow: str = Query(default=""),
):
    job_cfg = load_job_config("harvest-task-dashboard")
    dashboard_job = HarvestTaskDashboardJob(job_cfg)
    return HTMLResponse(
        content=dashboard_job.render_dashboard_view(
            embedded=embedded,
            report_type=report,
            workflow=workflow,
        )
    )
