from __future__ import annotations

import asyncio
from pathlib import Path

from main import app
from routers import ckan_curation


def test_ckan_curation_routes_are_registered() -> None:
    route_paths = {route.path for route in app.routes}

    assert "/jobs/ckan-curation-pipeline/jobs" in route_paths
    assert "/jobs/ckan-curation-pipeline/yaml" in route_paths
    assert "/jobs/ckan-curation-pipeline/status" in route_paths
    assert "/jobs/ckan-curation-pipeline/run-stream" in route_paths


def test_browser_job_list_includes_ann_arbor_but_not_template() -> None:
    result = asyncio.run(ckan_curation.list_ckan_curation_jobs())
    job_ids = {job["id"] for job in result["jobs"]}

    assert "ann-arbor-ckan-2026" in job_ids
    assert "ckan_curation_pipeline_template" not in job_ids


def test_ckan_curation_page_exposes_browser_workflow() -> None:
    html = Path("static/ckan-curation-pipeline.html").read_text(encoding="utf-8")
    dashboard_html = Path("static/task-dashboard.html").read_text(encoding="utf-8")

    assert "ckan_curation_pipeline" in html
    assert "/jobs/ckan-curation-pipeline" in html
    assert "Create from template" in html
    assert "Confirm manual review" in html
    assert 'id="yaml-editor"' in html
    assert "expected_sha256" in html
    for stage in (
        "validate",
        "metadata",
        "download",
        "enrich",
        "dictionaries",
        "embed",
        "thumbnails",
        "derivatives",
        "zip",
        "snapshot",
    ):
        assert f'"{stage}"' in html
    assert 'runStage(\'postprocess\')' not in html
    assert "CKAN Curation Pipeline" in html
    assert 'id="config-panel"' in html
    assert 'id="pipeline-readiness"' in html
    assert 'class="workflow"' in html
    assert html.count('class="step-number"') == 9
    assert html.count('class="run-button"') == 9
    assert 'id="activity-log"' in html
    assert "Refresh jobs" not in html
    assert "Refresh status" not in html
    assert "Overwrite existing" not in html
    assert "/static/ckan-curation-pipeline.html" in dashboard_html
    assert "CKAN Curation Pipeline" in dashboard_html
