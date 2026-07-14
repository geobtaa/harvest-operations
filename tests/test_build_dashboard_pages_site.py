from pathlib import Path

from dashboard.build_pages_site import build_pages_site


def test_build_pages_site_publishes_current_reports_and_workflow_histories(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    output_dir = tmp_path / "site"
    reports_dir.mkdir()

    report_files = {
        "harvest-task-dashboard-institutions.html": "<html><body>institutions</body></html>",
        "harvest-task-dashboard-map-collections.html": "<html><body>maps</body></html>",
        "harvest-task-dashboard-standalone-websites.html": "<html><body>standalone</body></html>",
        "harvest-task-dashboard-review.html": "<html><body>triage</body></html>",
        "harvest-task-dashboard-todo.html": "<html><body>todo</body></html>",
        "harvest-task-dashboard-retrospective.html": "<html><body>retrospective</body></html>",
        "2026-03-30_harvest-task-dashboard-py-arcgis-hub.html": (
            "<html><head><title>ArcGIS Hubs Harvest Report - 2026-03-30</title></head>"
            "<body>arcgis-2026-03-30</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-py-arcgis-hub.html": (
            "<html><head><title>ArcGIS Hubs Harvest Report - 2026-04-01</title></head>"
            "<body>arcgis-2026-04-01</body></html>"
        ),
        "2026-04-04_harvest-task-dashboard-py-arcgis-hub.html": (
            "<html><head><title>ArcGIS Hubs Harvest Report - 2026-04-01</title></head>"
            "<body><span>Harvest report date</span><strong>2026-04-01</strong>"
            "arcgis-2026-04-01-refreshed</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-py-ckan.html": (
            "<html><head><title>CKAN Harvest Report - 2026-04-01</title></head>"
            "<body>ckan-2026-04-01</body></html>"
        ),
    }
    for filename, contents in report_files.items():
        reports_dir.joinpath(filename).write_text(contents, encoding="utf-8")

    build_pages_site(reports_dir, output_dir)

    index_html = output_dir.joinpath("index.html").read_text(encoding="utf-8")
    arcgis_reports_html = output_dir.joinpath("workflows/py-arcgis-hub/index.html").read_text(
        encoding="utf-8"
    )

    assert 'href="latest/institutions/"' in index_html
    assert 'href="latest/map-collections/"' in index_html
    assert 'href="latest/standalone-websites/"' in index_html
    assert 'href="latest/triage/"' in index_html
    assert 'href="latest/to-do/"' in index_html
    assert 'href="latest/retrospective/"' in index_html
    assert 'href="workflows/py-arcgis-hub/"' in index_html
    assert 'href="workflows/py-ckan/"' in index_html
    assert "Browse the archive" not in index_html
    assert "Published Report Dates" not in index_html

    assert output_dir.joinpath("latest/retrospective/index.html").read_text(encoding="utf-8") == (
        "<html><body>retrospective</body></html>"
    )
    assert output_dir.joinpath("latest/institutions/index.html").read_text(encoding="utf-8") == (
        "<html><body>institutions</body></html>"
    )
    assert "2026-04-01" in arcgis_reports_html
    assert "2026-03-30" in arcgis_reports_html
    assert 'href="../../2026-04-01/workflows/py-arcgis-hub/"' in arcgis_reports_html
    assert arcgis_reports_html.count("2026-04-01") == 2
    assert output_dir.joinpath("2026-04-01/workflows/py-arcgis-hub/index.html").read_text(
        encoding="utf-8"
    ).endswith("arcgis-2026-04-01-refreshed</body></html>")
    assert not output_dir.joinpath("archive/index.html").exists()
    assert not output_dir.joinpath("2026-04-01/retrospective/index.html").exists()


def test_build_pages_site_requires_dashboard_html(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    output_dir = tmp_path / "site"
    reports_dir.mkdir()
    reports_dir.joinpath("README.txt").write_text("not a report", encoding="utf-8")

    try:
        build_pages_site(reports_dir, output_dir)
    except ValueError as exc:
        assert "No dashboard HTML files" in str(exc)
    else:
        raise AssertionError("Expected ValueError when no dashboard reports are present")
