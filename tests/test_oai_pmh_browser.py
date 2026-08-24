import asyncio
from pathlib import Path
import sys

import pytest
from fastapi import HTTPException

from main import app
from routers import oai_pmh


def test_oai_pmh_routes_are_registered() -> None:
    route_paths = {route.path for route in app.routes}

    assert "/jobs/oai-pmh/jobs" in route_paths
    assert "/jobs/oai-pmh/yaml" in route_paths
    assert "/jobs/oai-pmh/status" in route_paths
    assert "/jobs/oai-pmh/run-stream" in route_paths


def test_oai_job_list_discovers_existing_sources_dynamically() -> None:
    result = asyncio.run(oai_pmh.list_oai_jobs())
    job_ids = {job["id"] for job in result["jobs"]}

    assert {"iowa-library", "university-washington"} <= job_ids


def configure_temporary_jobs(monkeypatch, tmp_path: Path) -> Path:
    jobs_root = tmp_path / "config"
    templates_root = jobs_root / "templates"
    templates_root.mkdir(parents=True)
    template_path = templates_root / "oai_pmh.yaml"
    template_path.write_text(
        "\n".join(
            [
                "name: <job-id>",
                "type: oai_qdc",
                "feed_type: oai_dc",
                'oai_base_url: "https://<repository-host>/oai"',
                "metadata_prefix: oai_dc",
                'provider: "<university-name>"',
                'source_name: "<university-library-name>"',
                'source_id_prefix: "<prefix>"',
                "sets: []",
                "output_primary_csv: outputs/<job-id>_primary.csv",
                "output_distributions_csv: outputs/<job-id>_distributions.csv",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(oai_pmh, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(oai_pmh, "JOBS_ROOT", jobs_root)
    monkeypatch.setattr(oai_pmh, "JOB_TEMPLATE_PATH", template_path)
    monkeypatch.setattr(oai_pmh, "PIPELINE_SCRIPT", tmp_path / "scripts" / "oai_pmh_pipeline.py")
    return jobs_root


def test_browser_can_create_load_and_save_oai_source_job(monkeypatch, tmp_path) -> None:
    jobs_root = configure_temporary_jobs(monkeypatch, tmp_path)

    created = asyncio.run(
        oai_pmh.create_oai_job(
            oai_pmh.CreateOaiJobRequest(job_id="example-university")
        )
    )
    loaded = asyncio.run(oai_pmh.load_oai_yaml("example-university"))
    edited = loaded["content"].replace(
        "sets: []",
        'sets:\n  - set: maps\n    title: "Map Collection"',
    )
    saved = asyncio.run(
        oai_pmh.save_oai_yaml(
            oai_pmh.SaveOaiJobRequest(
                job_id="example-university",
                content=edited,
                expected_sha256=loaded["sha256"],
            )
        )
    )

    assert (jobs_root / "example-university.yaml").is_file()
    assert created["job"]["id"] == "example-university"
    assert saved["job"]["set_count"] == 1
    assert saved["job"]["sets"][0]["set_spec"] == "maps"


def test_browser_oai_save_rejects_changed_file(monkeypatch, tmp_path) -> None:
    jobs_root = configure_temporary_jobs(monkeypatch, tmp_path)
    asyncio.run(
        oai_pmh.create_oai_job(
            oai_pmh.CreateOaiJobRequest(job_id="example-university")
        )
    )
    loaded = asyncio.run(oai_pmh.load_oai_yaml("example-university"))
    path = jobs_root / "example-university.yaml"
    path.write_text(loaded["content"] + "# external edit\n", encoding="utf-8")

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            oai_pmh.save_oai_yaml(
                oai_pmh.SaveOaiJobRequest(
                    job_id="example-university",
                    content=loaded["content"],
                    expected_sha256=loaded["sha256"],
                )
            )
        )

    assert exc_info.value.status_code == 409
    assert "changed after it was loaded" in exc_info.value.detail


def test_oai_pipeline_command_can_scope_one_set() -> None:
    config_path = oai_pmh.JOBS_ROOT / "iowa-library.yaml"

    command = oai_pmh.build_pipeline_command(
        config_path,
        "download",
        set_spec="node:3186",
        python_executable=sys.executable,
    )

    assert command == [
        sys.executable,
        str(oai_pmh.PIPELINE_SCRIPT),
        str(config_path),
        "download",
        "--set-spec",
        "node:3186",
    ]


def test_oai_page_exposes_job_editor_and_staged_pipeline() -> None:
    html = Path("static/oai-qdc.html").read_text(encoding="utf-8")
    dashboard_html = Path("static/task-dashboard.html").read_text(encoding="utf-8")

    assert "/jobs/oai-pmh/jobs" in html
    assert "/jobs/oai-pmh/yaml" in html
    assert "/jobs/oai-pmh/status" in html
    assert "/jobs/oai-pmh/run-stream" in html
    assert "Create from template" in html
    assert "Save YAML" in html
    assert 'id="yaml-editor"' in html
    assert "expected_sha256" in html
    for stage in ("validate", "identify", "discover", "download", "harvest", "all"):
        assert f"runStage('{stage}')" in html
        assert f'data-stage-card="{stage}"' in html
    assert "/static/oai-qdc.html" in dashboard_html
    assert "OAI-PMH Harvester" in dashboard_html
