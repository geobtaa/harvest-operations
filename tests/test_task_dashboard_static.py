from pathlib import Path


def test_task_dashboard_static_page_includes_map_collections_link() -> None:
    html = Path("static/task-dashboard.html").read_text(encoding="utf-8")

    assert 'id="map-collections-report-link"' in html
    assert 'href="/jobs/harvest-task-dashboard/view?report=map-collections"' in html
    assert "<h2>Triage</h2>" in html
    assert "<h2>Reports</h2>" in html
    assert "<h2>Lists</h2>" in html
    assert "Due-only tasks" in html
    assert "All harvest records" in html
    assert "Harvest records by Accrual Periodicity" in html
    assert "By institution" in html
    assert "Map collections only" in html
    assert 'class="report-link' not in html
    assert "Refresh Report Links" not in html
    assert "Refresh Workflow Queue" not in html
    assert "const mapCollectionsUrl =" in html
    assert 'document.getElementById("map-collections-report-link").href = mapCollectionsUrl;' in html


def test_task_dashboard_static_page_includes_workflow_queue() -> None:
    html = Path("static/task-dashboard.html").read_text(encoding="utf-8")

    assert "Workflow Queue" in html
    assert "within the next month" in html
    assert "reference_data/websites.csv" in html
    assert "f%5Bb1g_publication_state_s%5D%5B%5D=draft" in html
    assert "/jobs/harvest-task-dashboard/workflow-queue" in html
    assert "loadWorkflowQueue()" in html
    assert "Open ${escapeHtml(workflow.static_page_label || workflow.label)}" in html
    assert "harvest_queue_count" in html
    assert "Queued Tasks" in html
