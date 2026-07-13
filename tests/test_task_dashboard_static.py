from pathlib import Path


def test_static_index_opens_the_harvest_task_dashboard() -> None:
    html = Path("static/index.html").read_text(encoding="utf-8")

    assert 'url=/static/task-dashboard.html' in html
    assert 'window.location.replace("/static/task-dashboard.html")' in html


def test_workflow_overview_describes_the_harvest_task_workflow() -> None:
    dashboard_html = Path("static/task-dashboard.html").read_text(encoding="utf-8")
    overview_html = Path("static/workflow-overview.html").read_text(encoding="utf-8")

    assert "Download the latest source CSVs" not in dashboard_html
    assert "Choose the task you want to refresh" not in dashboard_html
    assert 'href="/static/workflow-overview.html"' in dashboard_html
    assert "Workflow Overview" in overview_html
    assert "Open Harvest Dashboard and run the Triage task." in overview_html
    assert "queue:pending_*" in overview_html
    assert "due:yyyy-mm-dd" in overview_html
    assert "Create a GitHub issue for a task." in overview_html
    assert "update the Last Harvested field" in overview_html
    assert "Dashboard tags" in overview_html
    assert "queue:pending_harvest" in overview_html
    assert "queue:pending_updates" in overview_html
    assert "queue:pending_review" in overview_html
    assert "harvest_due:YYYY-MM-DD" in overview_html
    assert "review:Ny" in overview_html


def test_task_dashboard_static_page_includes_map_collections_link() -> None:
    html = Path("static/task-dashboard.html").read_text(encoding="utf-8")

    assert 'id="map-collections-report-link"' in html
    assert 'href="/jobs/harvest-task-dashboard/view?report=map-collections"' in html
    assert "<h2>Triage</h2>" in html
    assert "<h2>Reports</h2>" in html
    assert "<h2>Lists</h2>" in html
    assert "Due-only tasks" not in html
    assert 'id="review-report-link"' in html
    assert 'href="/jobs/harvest-task-dashboard/view?report=review"' in html
    assert 'id="todo-report-link"' in html
    assert 'href="/jobs/harvest-task-dashboard/view?report=todo"' in html
    assert 'id="arcgis-hub-report-link"' in html
    assert 'href="/jobs/harvest-task-dashboard/reports/py_arcgis_hub"' in html
    assert 'id="socrata-report-link"' in html
    assert 'href="/jobs/harvest-task-dashboard/reports/py_socrata"' in html
    assert 'id="ckan-report-link"' in html
    assert 'href="/jobs/harvest-task-dashboard/reports/py_ckan"' in html
    assert "CKAN reports" in html
    assert "Triage" in html
    assert "To do" in html
    assert "All harvest records" not in html
    assert "Harvest records by Accrual Periodicity" not in html
    assert "By institution" in html
    assert "Map collections only" in html
    assert 'class="report-link' not in html
    assert "Refresh Report Links" not in html
    assert "Refresh Workflow Queue" not in html
    assert "Run Triage" in html
    assert "Run Reports" in html
    assert "Run Lists" in html
    assert "`/jobs/harvest-task-${taskName}/run`" in html
    assert "runDashboardTask(taskName)" in html
    assert "background:" not in html
    assert "--link: #4f7f9f;" in html
    assert "a { color: var(--link); }" in html
    assert "background-color: #dff0df;" in html
    assert "color: #1d1d1d;" in html
    assert "--border-orange: #e0b08b;" in html
    assert "border: 1px solid var(--success);" in html
    assert "color: var(--success);" in html
    assert "const mapCollectionsUrl =" in html
    assert "const reviewUrl =" in html
    assert "const todoUrl =" in html
    assert "const arcgisHubUrl =" not in html
    assert "const socrataUrl =" not in html
    assert 'document.getElementById("map-collections-report-link").href = mapCollectionsUrl;' in html
    assert 'document.getElementById("review-report-link").href = reviewUrl;' in html
    assert 'document.getElementById("todo-report-link").href = todoUrl;' in html


def test_task_dashboard_static_page_includes_frequent_harvesters() -> None:
    html = Path("static/task-dashboard.html").read_text(encoding="utf-8")

    assert "Frequent Harvesters" in html
    assert "Last run dates and direct links" not in html
    assert "frequent harvester${entries.length" not in html
    assert "/jobs/harvest-task-dashboard/frequent-harvesters" in html
    assert "loadFrequentHarvesters()" in html
    assert "renderFrequentHarvesters(data)" in html
    assert "harvester.static_page_url" in html
    assert "harvester.harvest_record_url" in html
    assert "Last run:" in html
    assert "Batch tasks" in html
    assert "Single-site tasks" in html
    assert ">Harvest record</a>" in html
    assert html.index("Get Latest Source CSVs") < html.index("Frequent Harvesters")
    assert html.index("Run Triage") < html.index("Frequent Harvesters")
    assert 'class="dashboard-layout"' in html
    assert 'class="dashboard-primary"' in html
    assert 'class="dashboard-sidebar"' in html
    assert "Workflow Queue" not in html
    assert "harvest_queue_count" not in html
    assert "Other Harvesters" in html
    assert "OpenGeoMetadata Aardvark Harvester" in html
    assert html.index("Other Harvesters") > html.index('id="output"')
    other_harvesters = html.split('<h2>Other Harvesters</h2>', maxsplit=1)[1]
    assert "/static/arcgis.html" not in other_harvesters
    assert "/static/ckan.html" not in other_harvesters
    assert "/static/socrata.html" not in other_harvesters
    assert "/static/hdx.html" not in other_harvesters
    assert "/static/pasda-metadata.html" not in other_harvesters
    assert '<h2 id="task-output-label">Triage Output</h2>' in html
    assert 'aria-labelledby="task-output-label"' in html
