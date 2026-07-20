from pathlib import Path


def test_static_index_opens_the_harvest_task_dashboard() -> None:
    html = Path("static/index.html").read_text(encoding="utf-8")

    assert 'url=/static/task-dashboard.html' in html
    assert 'window.location.replace("/static/task-dashboard.html")' in html


def test_workflow_overview_describes_the_harvest_task_workflow() -> None:
    dashboard_html = Path("static/task-dashboard.html").read_text(encoding="utf-8")
    overview_html = Path("static/workflow-overview.html").read_text(encoding="utf-8")

    assert 'href="/static/workflow-overview.html"' in dashboard_html
    assert "Harvest Task Workflow" in overview_html
    assert "Open the Harvest Task Dashboard" in overview_html
    assert "Download both current CSVs" in overview_html
    assert "inputs/harvest-records.csv" in overview_html
    assert "reference_data/websites.csv" in overview_html
    assert "Generate Workflow Inputs" in overview_html
    assert "Run Triage" in overview_html
    assert "Open Triage and decide what should be worked on next" in overview_html
    assert "Admin Notes" in overview_html
    assert "Update the selected Harvest records" in overview_html
    assert "Refresh the dashboard data" in overview_html
    assert "Open To do and create the GitHub issues" in overview_html
    assert "assign the issue to the project lead or to yourself" in overview_html
    assert "Do not create a new issue directly from Triage" in overview_html
    assert "queue:pending_*" in overview_html
    assert "due:YYYY-MM-DD" in overview_html
    assert "Last Harvested field" in overview_html
    assert "Reports and Lists" in overview_html
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
    assert "<h3>Run Triage</h3>" in html
    assert "<h3>Reports</h3>" in html
    assert "<h3>Lists</h3>" in html
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
    assert "Generate Workflow Inputs" in html
    assert "runDashboardTask('source-csvs', 'Generate Workflow Inputs')" in html
    assert "Run Triage" in html
    assert "Open Triage" in html
    assert "Run Reports" in html
    assert "Run Lists" in html
    assert 'id="priority-workflow"' in html
    assert "grid-template-columns: minmax(0, 2fr) minmax(15rem, 1fr);" in html
    assert html.index('<main class="dashboard-layout">') < html.index(
        'id="priority-workflow"'
    )
    assert html.index('id="priority-workflow"') < html.index(
        '<aside class="dashboard-sidebar"'
    )
    assert 'id="workflow-step-downloads"' in html
    assert 'id="workflow-step-inputs"' in html
    assert 'id="workflow-step-triage"' in html
    assert 'id="workflow-step-open-triage"' in html
    assert 'id="secondary-tools"' in html
    assert html.index('id="workflow-step-downloads"') < html.index(
        'id="workflow-step-inputs"'
    )
    assert html.index('id="workflow-step-inputs"') < html.index(
        'id="workflow-step-triage"'
    )
    assert html.index('id="workflow-step-triage"') < html.index(
        'id="workflow-step-open-triage"'
    )
    assert html.index('id="workflow-step-open-triage"') < html.index(
        'id="secondary-tools"'
    )
    assert "Reports and Lists do not need to run before Triage" in html
    assert '<details id="technical-output" class="technical-output">' in html
    assert "Technical task output" in html
    assert "technicalOutput.open = true;" in html
    assert html.count('class="task-run-status"') == 4
    assert html.count("Not run yet") == 4
    assert 'data-task-status="source-csvs"' in html
    assert 'data-task-status="triage"' in html
    assert 'data-task-status="reports"' in html
    assert 'data-task-status="lists"' in html
    assert 'setTaskRunState(taskButton, taskStatus, "running", "Running...")' in html
    assert (
        'setTaskRunState(taskButton, taskStatus, "success", '
        "`Completed at ${formatRunTime()}`)"
    ) in html
    assert (
        'setTaskRunState(taskButton, taskStatus, "error", '
        "`Failed at ${formatRunTime()}`)"
    ) in html
    assert 'button.disabled = state === "running";' in html
    assert 'role="status" aria-live="polite"' in html
    assert "`/jobs/harvest-task-${taskName}/run`" in html
    assert 'runDashboardTask(taskName, taskLabel = "")' in html
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
    assert html.index("Download both current CSVs") < html.index("Frequent Harvesters")
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
    assert "/static/standalone-websites.html" not in other_harvesters
    assert '<h2 id="task-output-label">Triage Output</h2>' in html
    assert 'aria-labelledby="task-output-label"' in html
