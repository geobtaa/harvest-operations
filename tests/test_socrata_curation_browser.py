import asyncio
import json
from pathlib import Path

import pytest
from fastapi import HTTPException

from main import app
from routers import socrata_curation


def test_socrata_curation_routes_are_registered() -> None:
    route_paths = {route.path for route in app.routes}

    assert "/jobs/socrata-curation-pipeline/jobs" in route_paths
    assert "/jobs/socrata-curation-pipeline/yaml" in route_paths
    assert "/jobs/socrata-curation-pipeline/status" in route_paths
    assert "/jobs/socrata-curation-pipeline/run-stream" in route_paths


def test_browser_job_list_includes_bloomington_but_not_template() -> None:
    result = asyncio.run(socrata_curation.list_socrata_curation_jobs())
    job_ids = {job["id"] for job in result["jobs"]}

    assert "bloomington-socrata-2026" in job_ids
    assert "socrata_curation_pipeline_template" not in job_ids


def test_resolve_job_config_rejects_path_traversal() -> None:
    with pytest.raises(HTTPException) as exc_info:
        socrata_curation.resolve_job_config("../bloomington-socrata-2026")

    assert exc_info.value.status_code == 400


def configure_temporary_jobs(monkeypatch, tmp_path: Path) -> Path:
    curation_root = tmp_path / "curation"
    jobs_root = curation_root / "jobs" / "socrata"
    jobs_root.mkdir(parents=True)
    template_path = jobs_root / "socrata_curation_pipeline_template.yaml"
    template_path.write_text(
        "\n".join(
            [
                "version: 1",
                "job:",
                '  id: "<job-id>"',
                "  work_directory: ../../work/<job-id>",
                "records: []",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(socrata_curation, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(socrata_curation, "CURATION_ROOT", curation_root)
    monkeypatch.setattr(socrata_curation, "JOBS_ROOT", jobs_root)
    monkeypatch.setattr(socrata_curation, "JOB_TEMPLATE_PATH", template_path)
    monkeypatch.setattr(
        socrata_curation,
        "PIPELINE_SCRIPT",
        curation_root / "scripts" / "socrata_curation_pipeline.py",
    )
    return jobs_root


def test_browser_can_create_load_and_save_socrata_job(
    monkeypatch,
    tmp_path: Path,
) -> None:
    jobs_root = configure_temporary_jobs(monkeypatch, tmp_path)
    created = asyncio.run(
        socrata_curation.create_socrata_curation_job(
            socrata_curation.CreateCurationJobRequest(job_id="madison-socrata-2027")
        )
    )
    config_path = jobs_root / "madison-socrata-2027.yaml"

    assert config_path.is_file()
    assert created["job"]["id"] == "madison-socrata-2027"
    loaded = asyncio.run(
        socrata_curation.load_socrata_curation_yaml("madison-socrata-2027")
    )
    edited_content = loaded["content"].replace(
        "records: []",
        "records:\n  - id: abcd-1234",
    )
    saved = asyncio.run(
        socrata_curation.save_socrata_curation_yaml(
            socrata_curation.SaveCurationJobRequest(
                job_id="madison-socrata-2027",
                content=edited_content,
                expected_sha256=loaded["sha256"],
            )
        )
    )

    assert saved["status"] == "saved"
    assert saved["job"]["record_count"] == 1
    assert config_path.read_text(encoding="utf-8") == edited_content


def test_status_reads_manifest_and_metadata_from_nested_job_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    configure_temporary_jobs(monkeypatch, tmp_path)
    created = asyncio.run(
        socrata_curation.create_socrata_curation_job(
            socrata_curation.CreateCurationJobRequest(job_id="test-socrata-2027")
        )
    )
    work_dir = tmp_path / "curation" / "work" / "test-socrata-2027"
    metadata_path = work_dir / "metadata" / "metadata.csv"
    metadata_path.parent.mkdir(parents=True)
    metadata_path.write_text("ID,Title\nb1g_test,Test\n", encoding="utf-8")
    (work_dir / "manifest.json").write_text(
        json.dumps(
            {
                "source_platform": "socrata",
                "config_sha256": created["sha256"],
                "manual_review": {"status": "pending"},
                "stages": {"metadata": {"status": "completed"}},
            }
        ),
        encoding="utf-8",
    )

    status = asyncio.run(
        socrata_curation.socrata_curation_status("test-socrata-2027")
    )

    assert status["metadata_exists"] is True
    assert status["metadata_path"] == str(metadata_path)
    assert status["configuration_current"] is True
    assert status["manifest"]["source_platform"] == "socrata"


def test_pipeline_command_uses_socrata_script_and_flags() -> None:
    config_path = (
        socrata_curation.JOBS_ROOT / "bloomington-socrata-2026.yaml"
    )
    review_command = socrata_curation.build_pipeline_command(
        config_path,
        "review",
        confirm=True,
        uv_executable="/usr/local/bin/uv",
    )
    postprocess_command = socrata_curation.build_pipeline_command(
        config_path,
        "postprocess",
        overwrite=True,
        uv_executable="/usr/local/bin/uv",
    )

    assert review_command[4:6] == [
        "python",
        str(socrata_curation.PIPELINE_SCRIPT),
    ]
    assert review_command[-2:] == ["review", "--confirm"]
    assert postprocess_command[-2:] == ["postprocess", "--overwrite"]


def test_socrata_curation_page_exposes_browser_workflow() -> None:
    html = Path("static/socrata-curation-pipeline.html").read_text(encoding="utf-8")
    dashboard_html = Path("static/task-dashboard.html").read_text(encoding="utf-8")

    assert "socrata_curation_pipeline" in html
    assert "/jobs/socrata-curation-pipeline/jobs" in html
    assert "/jobs/socrata-curation-pipeline/yaml" in html
    assert "/jobs/socrata-curation-pipeline/status" in html
    assert "/jobs/socrata-curation-pipeline/run-stream" in html
    assert "ordered SODA2 GeoJSON pages" in html
    assert "city abbreviation and download year once" in html
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
        "postprocess",
        "snapshot",
    ):
        assert f"runStage('{stage}')" in html
    assert "/static/socrata-curation-pipeline.html" in dashboard_html
    assert "Socrata Curation Pipeline" in dashboard_html
