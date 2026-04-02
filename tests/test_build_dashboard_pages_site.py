from pathlib import Path

from scripts.build_dashboard_pages_site import build_pages_site


def test_build_pages_site_creates_latest_and_archive_views(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    output_dir = tmp_path / "site"
    reports_dir.mkdir()

    report_files = {
        "2026-03-30_harvest-task-dashboard.html": "<html><body>full-2026-03-30</body></html>",
        "2026-03-30_harvest-task-dashboard-due.html": "<html><body>due-2026-03-30</body></html>",
        "2026-04-01_harvest-task-dashboard.html": "<html><body>full-2026-04-01</body></html>",
        "2026-04-01_harvest-task-dashboard-due.html": "<html><body>due-2026-04-01</body></html>",
        "2026-04-01_harvest-task-dashboard-retrospective.html": (
            "<html><body>retrospective-2026-04-01</body></html>"
        ),
    }

    for filename, contents in report_files.items():
        reports_dir.joinpath(filename).write_text(contents, encoding="utf-8")

    build_pages_site(reports_dir, output_dir)

    index_html = output_dir.joinpath("index.html").read_text(encoding="utf-8")
    archive_html = output_dir.joinpath("archive/index.html").read_text(encoding="utf-8")

    assert "2026-04-01" in index_html
    assert 'href="latest/"' in index_html
    assert 'href="latest/due/"' in index_html
    assert 'href="latest/retrospective/"' in index_html
    assert 'href="2026-03-30/"' in index_html
    assert 'href="2026-04-01/retrospective/"' in index_html
    assert 'href="../2026-04-01/retrospective/"' in archive_html

    assert (
        output_dir.joinpath("latest/index.html").read_text(encoding="utf-8")
        == "<html><body>full-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("latest/due/index.html").read_text(encoding="utf-8")
        == "<html><body>due-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("latest/retrospective/index.html").read_text(encoding="utf-8")
        == "<html><body>retrospective-2026-04-01</body></html>"
    )
    assert (
        output_dir.joinpath("2026-03-30/index.html").read_text(encoding="utf-8")
        == "<html><body>full-2026-03-30</body></html>"
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
