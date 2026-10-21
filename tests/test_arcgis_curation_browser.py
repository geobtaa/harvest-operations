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
    zip_command = arcgis_curation.build_pipeline_command(
        config_path,
        "zip",
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
    assert zip_command[-2:] == ["zip", "--overwrite"]
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
        "zip",
        "snapshot",
    ):
        assert f"runStage('{stage}')" in html
    assert "confirmReview()" in html
    assert "Confirm manual review" in html
    assert "Run all postprocess tasks" not in html
    assert "runStage('postprocess')" not in html
    assert "<h3>Run Record</h3>" in html
    assert "Save run record" in html
    assert 'class="run-record"' in html
    assert 'data-stage-card="metadata"' in html
    assert 'data-stage-status="metadata"' not in html
    assert 'data-stage-status="snapshot"' not in html
    assert html.count('<span class="stage-status" data-stage-status="') == 1
    assert 'id="metadata-output-path"' not in html
    assert 'status.textContent = state === "completed"' in html
    assert 'stage === "snapshot"' in html
    assert "formattedStageDate(completedAt)" in html
    assert "parsed.toLocaleDateString()" in html
    assert "parsed.toLocaleString()" not in html
    assert "Run failed — see the live log" not in html
    assert "updateStageIndicators" in html
    assert "stageFailureMessage" in html
    assert "Create from template" in html
    assert "Save configuration" in html
    assert 'id="yaml-editor"' in html
    assert 'id="config-panel"' in html
    assert 'class="job-selection"' in html
    assert "font-size: 0.88rem" in html
    assert "background: var(--accent-soft)" in html
    assert ".file-badge { display: none; }" not in html
    job_select_position = html.index('id="job-select"')
    config_open_position = html.index('id="config-panel"')
    config_close_position = html.index("</details>", config_open_position)
    assert config_open_position < job_select_position < config_close_position
    assert "Choose a curation job" not in html
    assert "Open config +" in html
    assert "Close config −" in html
    validation_position = html.index('data-stage-card="validate"')
    pipeline_position = html.index('id="pipeline-tasks"')
    assert validation_position < config_close_position < pipeline_position
    assert "Validate before starting" in html
    assert 'data-validation-button' in html
    assert 'data-pipeline-button' in html
    assert "setPipelineAvailability" in html
    assert "Job config validated — pipeline ready" in html
    assert "Pipeline tasks" not in html
    config_summary_start = html.index("<summary>", config_open_position)
    config_summary_end = html.index("</summary>", config_summary_start)
    readiness_position = html.index('id="pipeline-readiness"')
    assert config_summary_start < readiness_position < config_summary_end
    assert '<span id="pipeline-readiness"' in html
    assert "openValidation" not in html
    assert 'class="stage-card" data-stage-card="validate"' not in html
    assert "shortFileName" not in html
    assert '.textContent = data.metadata_path' not in html
    assert "Configuration matches manifest:" not in html
    assert "Curation workspace" not in html
    assert ">Workflow<" not in html
    assert ">Active job<" not in html
    assert ">Configuration<" not in html
    assert ">Job Config<" in html
    assert ">Advanced<" not in html
    assert ">Refresh jobs<" not in html
    assert ">Refresh status<" not in html
    assert "Replace existing output files when supported" not in html
    assert 'id="overwrite-toggle"' not in html
    assert "max-width: 1440px" in html
    assert "h1, h2, h3 {" in html
    assert 'font-family: "Avenir Next", Avenir, "Helvetica Neue", sans-serif' in html
    assert "letter-spacing: 0.035em" in html
    assert "letter-spacing: 0.025em" in html
    assert 'class="workflow"' in html
    assert "grid-template-columns: 52px minmax(0, 1fr) auto" in html
    assert "--run:" in html
    assert "--run-ink: #332d00" in html
    assert '.workflow-step[data-status="completed"] {' in html
    assert "background: var(--success-soft)" in html
    assert ".play-icon svg {" in html
    assert "stroke-width: 1.7" in html
    assert "height: 30px" in html
    assert '.pipeline-readiness[data-ready="true"]::before {' in html
    assert ".step-detail p {\n      color: #000000;" in html
    assert "font-size: 1.45rem" in html
    assert ".workflow-step:hover .step-detail p" in html
    assert ".workflow-step:focus-within .step-detail p" in html
    assert "opacity: 0" in html
    assert "<h3>Harvest metadata</h3>" in html
    assert "<h3>Review metadata</h3>" in html
    assert "<h3>Zip GeoPackages</h3>" in html
    assert html.count('class="run-button"') == 9
    assert "min-width: 126px" in html
    assert "height: 42px" in html
    assert html.count('class="play-icon" aria-hidden="true">▶</span>') == 9
    assert "<span>Run</span>" not in html
    assert 'state === "completed" || state === "failed"' in html
    assert 'actionIcon === "rerun"' in html
    assert '<svg viewBox="0 0 24 24"' in html
    assert '"↻"' not in html
    assert '? "Rerun"' in html
    assert '? "Retry"' in html
    assert 'snapshotButton.textContent = `Save again${formattedStageDate(completedAt)}`' in html
    assert "font-size: 1.2rem" in html
    assert html.count('class="step-number"') == 9
    assert 'content: "↓"' in html
    assert "left: calc(1rem + 21px)" in html
    assert "left: calc(1rem + 19px)" in html
    assert "transform: translateX(-50%)" in html
    assert "border-radius: 12px" not in html
    assert "<h3>1. Metadata</h3>" not in html
    assert "<h3>2. Manual review</h3>" not in html
    assert 'class="panel job-picker-row"' not in html
    assert 'id="job-status"' not in html
    assert "tasks complete" not in html
    assert "Review ${reviewStatus" not in html
    assert 'id="activity-log"' in html
    assert "expected_sha256" in html
    assert '/static/arcgis-curation-pipeline.html' in dashboard_html
    assert "ArcGIS Curation Pipeline" in dashboard_html
    assert "<h1>ArcGIS Curation Pipeline</h1>" in html
    header_start = html.index('<header class="page-header">')
    header_end = html.index("</header>", header_start)
    assert header_start < html.index('class="back-link"') < header_end
    assert "&larr; Dashboard" in html
    assert "font-size: clamp(1.35rem, 2.5vw, 1.85rem)" in html
    assert "Prepare, review, and package selected ArcGIS datasets" not in html
