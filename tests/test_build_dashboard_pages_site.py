from pathlib import Path

from scripts.build_dashboard_pages_site import build_pages_site


def test_build_pages_site_creates_latest_and_archive_views(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    output_dir = tmp_path / "site"
    reports_dir.mkdir()

    report_files = {
        "2026-03-30_harvest-task-dashboard.html": "<html><body>full-2026-03-30</body></html>",
        "2026-03-30_harvest-task-dashboard-records.html": (
            "<html><body>records-2026-03-30</body></html>"
        ),
        "2026-03-30_harvest-task-dashboard-due.html": "<html><body>due-2026-03-30</body></html>",
        "2026-04-01_harvest-task-dashboard.html": "<html><body>full-2026-04-01</body></html>",
        "2026-04-01_harvest-task-dashboard-records.html": (
            "<html><body>records-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-records-public.html": (
            "<html><body>records-public-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-institutions.html": (
            "<html><body>institutions-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-institutions-public.html": (
            "<html><body>institutions-public-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-map-collections.html": (
            "<html><body>map-collections-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-map-collections-public.html": (
            "<html><body>map-collections-public-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-standalone-websites.html": (
            "<html><body>standalone-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-standalone-websites-public.html": (
            "<html><body>standalone-public-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-public.html": (
            "<html><body>full-public-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-due.html": "<html><body>due-2026-04-01</body></html>",
        "2026-04-01_harvest-task-dashboard-due-public.html": (
            "<html><body>due-public-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-retrospective.html": (
            "<html><body>retrospective-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-retrospective-public.html": (
            "<html><body>retrospective-public-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-py-arcgis-hub.html": (
            "<html><head><title>ArcGIS Hubs Harvest Report - 2026-04-01</title></head>"
            "<body>workflow-2026-04-01</body></html>"
        ),
        "2026-04-01_harvest-task-dashboard-py-arcgis-hub-public.html": (
            "<html><head><title>ArcGIS Hubs Harvest Report - 2026-04-01</title></head>"
            "<body>workflow-public-2026-04-01</body></html>"
        ),
    }

    for filename, contents in report_files.items():
        reports_dir.joinpath(filename).write_text(contents, encoding="utf-8")

    build_pages_site(reports_dir, output_dir)

    index_html = output_dir.joinpath("index.html").read_text(encoding="utf-8")
    archive_html = output_dir.joinpath("archive/index.html").read_text(encoding="utf-8")

    assert "2026-04-01" in index_html
    assert 'href="latest/all-harvest-records/"' in index_html
    assert 'href="latest/"' in index_html
    assert 'href="latest/institutions/"' in index_html
    assert 'href="latest/map-collections/"' in index_html
    assert 'href="latest/standalone-websites/"' in index_html
    assert 'href="latest/due/"' in index_html
    assert 'href="latest/retrospective/"' in index_html
    assert 'href="latest/workflows/py-arcgis-hub/"' in index_html
    assert "<h2>Triage</h2>" in index_html
    assert "<h2>Reports</h2>" in index_html
    assert "<h2>Lists</h2>" in index_html
    assert "All harvest records" in index_html
    assert "Due-only tasks" in index_html
    assert "Harvest records by Accrual Periodicity" in index_html
    assert "Map collections only" in index_html
    assert "ArcGIS Hub report" in index_html
    assert 'href="2026-04-01/all-harvest-records/"' in index_html
    assert 'href="2026-03-30/"' in index_html
    assert 'href="2026-04-01/institutions/"' in index_html
    assert 'href="2026-04-01/map-collections/"' in index_html
    assert 'href="2026-04-01/standalone-websites/"' in index_html
    assert 'href="2026-04-01/retrospective/"' in index_html
    assert 'href="2026-04-01/workflows/py-arcgis-hub/"' in index_html
    assert 'href="../2026-04-01/all-harvest-records/"' in archive_html
    assert 'href="../2026-04-01/institutions/"' in archive_html
    assert 'href="../2026-04-01/map-collections/"' in archive_html
    assert 'href="../2026-04-01/standalone-websites/"' in archive_html
    assert 'href="../2026-04-01/retrospective/"' in archive_html
    assert 'href="../2026-04-01/workflows/py-arcgis-hub/"' in archive_html

    assert (
        output_dir.joinpath("latest/all-harvest-records/index.html").read_text(
            encoding="utf-8"
        )
        == "<html><body>full-public-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("latest/index.html").read_text(encoding="utf-8")
        == "<html><body>records-public-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("latest/institutions/index.html").read_text(encoding="utf-8")
        == "<html><body>institutions-public-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("latest/map-collections/index.html").read_text(encoding="utf-8")
        == "<html><body>map-collections-public-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("latest/standalone-websites/index.html").read_text(encoding="utf-8")
        == "<html><body>standalone-public-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("latest/due/index.html").read_text(encoding="utf-8")
        == "<html><body>due-public-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("latest/retrospective/index.html").read_text(encoding="utf-8")
        == "<html><body>retrospective-public-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("latest/workflows/py-arcgis-hub/index.html").read_text(
            encoding="utf-8"
        )
        == "<html><head><title>ArcGIS Hubs Harvest Report - 2026-04-01</title></head>"
        "<body>workflow-public-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("2026-04-01/all-harvest-records/index.html").read_text(
            encoding="utf-8"
        )
        == "<html><body>full-public-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("2026-03-30/index.html").read_text(encoding="utf-8")
        == "<html><body>records-2026-03-30</body></html>"
    )


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
