from pathlib import Path


def test_task_dashboard_static_page_includes_map_collections_link() -> None:
    html = Path("static/task-dashboard.html").read_text(encoding="utf-8")

    assert 'id="map-collections-report-link"' in html
    assert 'href="/jobs/harvest-task-dashboard/view?report=map-collections"' in html
    assert 'id="map-collections-report-url"' in html
    assert "const mapCollectionsUrl =" in html
    assert 'document.getElementById("map-collections-report-link").href = mapCollectionsUrl;' in html
