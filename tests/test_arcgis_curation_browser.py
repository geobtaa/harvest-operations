import asyncio
from pathlib import Path
import sys

import pytest
from fastapi import HTTPException

from main import app
from routers import arcgis_curation


def test_arcgis_curation_routes_are_registered() -> None:
    route_paths = {route.path for route in app.routes}

    assert "/jobs/arcgis-curation-pipeline/jobs" in route_paths
    assert "/jobs/arcgis-curation-pipeline/yaml" in route_paths
    assert "/jobs/arcgis-curation-pipeline/status" in route_paths
    assert "/jobs/arcgis-curation-pipeline/run-stream" in route_paths


def test_browser_job_list_includes_example_but_not_template() -> None:
    result = asyncio.run(arcgis_curation.list_arcgis_curation_jobs())
    job_ids = {job["id"] for job in result["jobs"]}

    assert "stpaul-2026" in job_ids
    assert "arcgis_curation_pipeline_template" not in job_ids


def test_resolve_job_config_rejects_path_traversal() -> None:
    with pytest.raises(HTTPException) as exc_info:
        arcgis_curation.resolve_job_config("../stpaul-2026")

    assert exc_info.value.status_code == 400


def configure_temporary_jobs(monkeypatch, tmp_path: Path) -> Path:
    curation_root = tmp_path / "curation"
    jobs_root = curation_root / "jobs"
    jobs_root.mkdir(parents=True)
    template_path = jobs_root / "arcgis_curation_pipeline_template.yaml"
    template_path.write_text(
        "\n".join(
            [
                "version: 1",
                "job:",
                '  id: "<job-id>"',
                '  work_directory: "../work/<job-id>"',
                "records: []",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(arcgis_curation, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(arcgis_curation, "CURATION_ROOT", curation_root)
    monkeypatch.setattr(arcgis_curation, "JOBS_ROOT", jobs_root)
    monkeypatch.setattr(arcgis_curation, "JOB_TEMPLATE_PATH", template_path)
    return jobs_root


def test_browser_can_create_job_from_canonical_template(monkeypatch, tmp_path: Path) -> None:
    jobs_root = configure_temporary_jobs(monkeypatch, tmp_path)

    result = asyncio.run(
        arcgis_curation.create_arcgis_curation_job(
            arcgis_curation.CreateCurationJobRequest(job_id="duluth-2027")
        )
    )

    created_path = jobs_root / "duluth-2027.yaml"
    assert created_path.is_file()
    assert 'id: "duluth-2027"' in created_path.read_text(encoding="utf-8")
    assert result["job"]["id"] == "duluth-2027"
    assert result["sha256"] == arcgis_curation.yaml_content_sha256(result["content"])


def test_browser_can_load_and_save_job_yaml(monkeypatch, tmp_path: Path) -> None:
    configure_temporary_jobs(monkeypatch, tmp_path)
    asyncio.run(
        arcgis_curation.create_arcgis_curation_job(
            arcgis_curation.CreateCurationJobRequest(job_id="duluth-2027")
        )
    )
    loaded = asyncio.run(arcgis_curation.load_arcgis_curation_yaml("duluth-2027"))
    edited_content = loaded["content"].replace("records: []", "records:\n  - id: abc_0")

    result = asyncio.run(
        arcgis_curation.save_arcgis_curation_yaml(
            arcgis_curation.SaveCurationJobRequest(
                job_id="duluth-2027",
                content=edited_content,
                expected_sha256=loaded["sha256"],
            )
        )
    )

    assert result["status"] == "saved"
    assert result["job"]["record_count"] == 1
    assert asyncio.run(arcgis_curation.load_arcgis_curation_yaml("duluth-2027"))[
        "content"
    ] == edited_content


def test_browser_save_rejects_changed_file(monkeypatch, tmp_path: Path) -> None:
    jobs_root = configure_temporary_jobs(monkeypatch, tmp_path)
    asyncio.run(
        arcgis_curation.create_arcgis_curation_job(
            arcgis_curation.CreateCurationJobRequest(job_id="duluth-2027")
        )
    )
    loaded = asyncio.run(arcgis_curation.load_arcgis_curation_yaml("duluth-2027"))
    config_path = jobs_root / "duluth-2027.yaml"
    config_path.write_text(loaded["content"] + "# changed elsewhere\n", encoding="utf-8")

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            arcgis_curation.save_arcgis_curation_yaml(
                arcgis_curation.SaveCurationJobRequest(
                    job_id="duluth-2027",
                    content=loaded["content"],
                    expected_sha256=loaded["sha256"],
                )
            )
        )

    assert exc_info.value.status_code == 409
    assert "changed after it was loaded" in exc_info.value.detail


def test_browser_save_requires_matching_job_id(monkeypatch, tmp_path: Path) -> None:
    configure_temporary_jobs(monkeypatch, tmp_path)
    asyncio.run(
        arcgis_curation.create_arcgis_curation_job(
            arcgis_curation.CreateCurationJobRequest(job_id="duluth-2027")
        )
    )
    loaded = asyncio.run(arcgis_curation.load_arcgis_curation_yaml("duluth-2027"))
    mismatched_content = loaded["content"].replace("duluth-2027", "other-job")

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            arcgis_curation.save_arcgis_curation_yaml(
                arcgis_curation.SaveCurationJobRequest(
                    job_id="duluth-2027",
                    content=mismatched_content,
                    expected_sha256=loaded["sha256"],
                )
            )
        )

    assert exc_info.value.status_code == 400
    assert "job.id must remain 'duluth-2027'" in exc_info.value.detail


def test_pipeline_command_uses_curation_project_and_stage_flags() -> None:
    config_path = arcgis_curation.JOBS_ROOT / "stpaul-2026.yaml"

    review_command = arcgis_curation.build_pipeline_command(
        config_path,
        "review",
        confirm=True,
        uv_executable="/usr/local/bin/uv",
    )
    postprocess_command = arcgis_curation.build_pipeline_command(
        config_path,
        "postprocess",
        overwrite=True,
        uv_executable="/usr/local/bin/uv",
    )
    snapshot_command = arcgis_curation.build_pipeline_command(
        config_path,
        "snapshot",
        uv_executable="/usr/local/bin/uv",
    )

    assert review_command[:4] == [
        "/usr/local/bin/uv",
        "run",
        "--project",
        str(arcgis_curation.CURATION_ROOT),
    ]
    assert review_command[-2:] == ["review", "--confirm"]
    assert postprocess_command[-2:] == ["postprocess", "--overwrite"]
    assert snapshot_command[-1] == "snapshot"


def test_failed_pipeline_stream_includes_the_last_error_line() -> None:
    async def collect_stream() -> str:
        chunks = []
        async for chunk in arcgis_curation.stream_pipeline_process(
            "test-failure-job",
            [
                sys.executable,
                "-c",
                "import sys; print('ERROR: specific failure'); raise SystemExit(1)",
            ],
            "download",
        ):
            chunks.append(chunk)
        return "".join(chunks)

    body = asyncio.run(collect_stream())

    assert "PIPELINE_FAILED: download failed: ERROR: specific failure" in body
    assert "data: DONE" in body


def test_arcgis_curation_page_exposes_individual_and_postprocess_tasks() -> None:
    html = Path("static/arcgis-curation-pipeline.html").read_text(encoding="utf-8")
    dashboard_html = Path("static/task-dashboard.html").read_text(encoding="utf-8")

    assert "arcgis_curation_pipeline" in html
    assert "/jobs/arcgis-curation-pipeline/jobs" in html
    assert "/jobs/arcgis-curation-pipeline/yaml" in html
    assert "/jobs/arcgis-curation-pipeline/status" in html
    assert "/jobs/arcgis-curation-pipeline/run-stream" in html
    for stage in (
        "validate",
        "metadata",
        "download",
        "enrich",
        "dictionaries",
        "embed",
        "thumbnails",
        "derivatives",
        "postprocess",
        "snapshot",
    ):
        assert f"runStage('{stage}')" in html
    assert "confirmReview()" in html
    assert "Confirm manual review" in html
    assert "Run all postprocess tasks" in html
    assert "Save Run Record" in html
    assert "Save run record" in html
    assert 'data-stage-card="metadata"' in html
    assert 'data-stage-status="metadata"' in html
    assert 'id="metadata-output-path"' in html
    assert "✓ ${label}" in html
    assert "updateStageIndicators" in html
    assert "stageFailureMessage" in html
    assert "Create from template" in html
    assert "Save YAML" in html
    assert 'id="yaml-editor"' in html
    assert "expected_sha256" in html
    assert '/static/arcgis-curation-pipeline.html' in dashboard_html
    assert "ArcGIS Curation Pipeline" in dashboard_html
