from pathlib import Path

import pandas as pd
import pytest

from scripts.harvest_task_dashboard import HarvestTaskDashboardJob


def test_harvest_task_dashboard_generates_outputs_and_workflow_splits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harvest_records_path = tmp_path / "harvest-records.csv"
    websites_path = tmp_path / "websites.csv"
    code_schema_map_path = tmp_path / "code-schema-map.csv"
    outputs_dir = tmp_path / "outputs"
    outputs_dir.mkdir()

    pd.DataFrame(
        [
            {
                "ID": "task-1",
                "Title": "County Parcels",
                "Harvest Workflow": "py_arcgis_hub",
                "Identifier": "site-1",
                "Code": "27d-01",
                "Subject": "Maps",
                "Last Harvested": "2026-02-15",
                "Accrual Periodicity": "monthly",
                "Created At": "2026-01-01 10:00:00 -0500",
                "Updated At": "2026-03-01 10:00:00 -0500",
            },
            {
                "ID": "task-1b",
                "Title": "Road Centerlines",
                "Harvest Workflow": "py_arcgis_hub",
                "Identifier": "site-3",
                "Code": "27d-01",
                "Subject": "Transportation|Maps",
                "Last Harvested": "2026-02-20",
                "Accrual Periodicity": "monthly",
            },
            {
                "ID": "task-2",
                "Title": "Transit Stops",
                "Harvest Workflow": "py_socrata",
                "Identifier": "site-2",
                "Code": "07a-01",
                "Subject": "Transportation",
                "Last Harvested": "2026-03-23",
                "Accrual Periodicity": "weekly",
            },
            {
                "ID": "task-2b",
                "Title": "Building Permits",
                "Harvest Workflow": "py_socrata",
                "Identifier": "site-4",
                "Code": "99z-01",
                "Subject": "Structures",
                "Last Harvested": "2026-03-28",
                "Accrual Periodicity": "weekly",
            },
            {
                "ID": "task-3",
                "Title": "Geology Index",
                "Harvest Workflow": "py_pasda",
                "Identifier": "",
                "Subject": "Geology",
                "Last Harvested": "2026-01-01",
                "Accrual Periodicity": "irregular",
            },
            {
                "ID": "task-4",
                "Title": "Parcel Fabric",
                "Harvest Workflow": "py_pasda",
                "Identifier": "site-5",
                "Code": "05f-01",
                "Subject": "Imagery|Maps",
                "Last Harvested": "2026-03-29",
                "Accrual Periodicity": "weekly",
            },
        ]
    ).to_csv(harvest_records_path, index=False)

    pd.DataFrame(
        [
            {
                "ID": "site-1",
                "Name": "County GIS Portal",
                "Harvest Workflow": "py_arcgis_hub",
                "URL": "https://example.com/arcgis",
            },
            {
                "ID": "site-2",
                "Name": "City Open Data",
                "Harvest Workflow": "py_socrata",
                "URL": "https://example.com/socrata",
            },
            {
                "ID": "site-3",
                "Name": "Regional GIS Portal",
                "Harvest Workflow": "py_arcgis_hub",
                "URL": "https://example.com/arcgis-2",
            },
            {
                "ID": "site-4",
                "Name": "County Open Data",
                "Harvest Workflow": "py_socrata",
                "URL": "https://example.com/socrata-2",
            },
            {
                "ID": "site-5",
                "Name": "PASDA Site",
                "Harvest Workflow": "py_pasda",
                "URL": "https://example.com/pasda",
            },
        ]
    ).to_csv(websites_path, index=False)

    code_schema_map_path.write_text(
        "code_prefix,Related institution or source\n"
        "05,University of Minnesota\n",
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "Code": "27d-01",
                "Identifier": "site-1",
                "Harvest Run": "success",
                "Total Records Found": "12",
                "New Records": "3",
                "Unpublished Records": "1",
            },
            {
                "Code": "27d-02",
                "Identifier": "site-3",
                "Harvest Run": "success",
                "Total Records Found": "9",
                "New Records": "0",
                "Unpublished Records": "2",
            },
        ]
    ).to_csv(outputs_dir / "2026-03-29_arcgis_report.csv", index=False)
    pd.DataFrame(
        [
            {
                "Code": "27d-01",
                "Identifier": "site-1",
                "Harvest Run": "success",
                "Total Records Found": "18",
                "New Records": "4",
                "Unpublished Records": "0",
            },
            {
                "Code": "27d-01",
                "Identifier": "site-3",
                "Harvest Run": "error",
                "Harvest Message": "[ArcGIS] Failed site-3 - timeout",
                "Total Records Found": "21",
                "New Records": "5",
                "Unpublished Records": "2",
            },
            {
                "Code": "TOTAL",
                "Identifier": "",
                "Harvest Run": "success: 1; error: 1",
                "Total Records Found": "39",
                "New Records": "9",
                "Unpublished Records": "2",
            },
        ]
    ).to_csv(outputs_dir / "2026-03-30_arcgis_report.csv", index=False)
    pd.DataFrame(
        [
            {
                "Code": "07a-01",
                "Identifier": "site-2",
                "Harvest Run": "success",
                "Total Records Found": "7",
                "New Records": "2",
                "Unpublished Records": "0",
            },
            {
                "Code": "99z-01",
                "Identifier": "site-4",
                "Harvest Run": "success",
                "Total Records Found": "11",
                "New Records": "1",
                "Unpublished Records": "1",
            },
            {
                "Code": "TOTAL",
                "Identifier": "",
                "Harvest Run": "success: 2; error: 0",
                "Total Records Found": "18",
                "New Records": "3",
                "Unpublished Records": "1",
            },
        ]
    ).to_csv(outputs_dir / "2026-03-30_socrata_report.csv", index=False)

    job = HarvestTaskDashboardJob(
        {
            "harvest_records_csv": str(harvest_records_path),
            "websites_csv": str(websites_path),
            "code_schema_map_csv": str(code_schema_map_path),
            "geoportal_api_facet_url": "https://example.com/api/v1/search/facets/b1g_code_s",
            "output_tasks_csv": str(outputs_dir / "harvest-task-dashboard.csv"),
            "output_dashboard_html": str(outputs_dir / "harvest-task-dashboard.html"),
            "output_due_dashboard_html": str(outputs_dir / "harvest-task-dashboard-due.html"),
            "output_retrospective_dashboard_html": str(
                outputs_dir / "harvest-task-dashboard-retrospective.html"
            ),
            "output_workflow_dir": str(outputs_dir / "harvest-workflow-inputs"),
            "arcgis_reports_dir": str(outputs_dir),
            "socrata_reports_dir": str(outputs_dir),
            "issue_repositories": [
                {
                    "name": "harvest-operations",
                    "issues_new_url": "https://github.com/geobtaa/harvest-operations/issues/new",
                    "template": "harvest-task.md",
                    "labels": ["harvest-task"],
                }
            ],
            "today": "2026-03-30",
        }
    )

    fetch_calls = 0

    def fake_fetch_geoportal_code_counts() -> dict[str, int]:
        nonlocal fetch_calls
        fetch_calls += 1
        return {
            "27d-01": 12,
            "27d-02": 8,
            "05f-01": 3,
        }

    monkeypatch.setattr(job, "_fetch_geoportal_code_counts", fake_fetch_geoportal_code_counts)

    results = job.harvest_pipeline()

    task_df = pd.read_csv(results["task_csv"], dtype=str).fillna("")
    dashboard_html = Path(results["dashboard_html"]).read_text(encoding="utf-8")
    public_dashboard_html = Path(results["public_dashboard_html"]).read_text(encoding="utf-8")
    due_dashboard_html = Path(results["due_dashboard_html"]).read_text(encoding="utf-8")
    public_due_dashboard_html = Path(results["public_due_dashboard_html"]).read_text(
        encoding="utf-8"
    )
    records_dashboard_html = Path(results["records_dashboard_html"]).read_text(encoding="utf-8")
    public_records_dashboard_html = Path(results["public_records_dashboard_html"]).read_text(
        encoding="utf-8"
    )
    institution_dashboard_html = Path(results["institution_dashboard_html"]).read_text(
        encoding="utf-8"
    )
    public_institution_dashboard_html = Path(
        results["public_institution_dashboard_html"]
    ).read_text(encoding="utf-8")
    map_collections_dashboard_html = Path(results["map_collections_dashboard_html"]).read_text(
        encoding="utf-8"
    )
    public_map_collections_dashboard_html = Path(
        results["public_map_collections_dashboard_html"]
    ).read_text(encoding="utf-8")
    retrospective_dashboard_html = Path(results["retrospective_dashboard_html"]).read_text(
        encoding="utf-8"
    )
    public_retrospective_dashboard_html = Path(
        results["public_retrospective_dashboard_html"]
    ).read_text(encoding="utf-8")
    dedicated_dashboard_outputs = results["dedicated_dashboard_html"]
    public_dedicated_dashboard_outputs = results["public_dedicated_dashboard_html"]
    arcgis_dashboard_output_html = Path(
        dedicated_dashboard_outputs["py_arcgis_hub"]
    ).read_text(encoding="utf-8")
    socrata_dashboard_output_html = Path(
        dedicated_dashboard_outputs["py_socrata"]
    ).read_text(encoding="utf-8")
    public_arcgis_dashboard_output_html = Path(
        public_dedicated_dashboard_outputs["py_arcgis_hub"]
    ).read_text(encoding="utf-8")
    public_socrata_dashboard_output_html = Path(
        public_dedicated_dashboard_outputs["py_socrata"]
    ).read_text(encoding="utf-8")
    arcgis_dashboard_html = job.render_dashboard_view(workflow="py_arcgis_hub")
    public_arcgis_dashboard_html = job.render_dashboard_view(workflow="py_arcgis_hub", public=True)
    socrata_dashboard_html = job.render_dashboard_view(workflow="py_socrata")
    public_socrata_dashboard_html = job.render_dashboard_view(workflow="py_socrata", public=True)
    records_view_html = job.render_dashboard_view(report_type="records")
    public_records_view_html = job.render_dashboard_view(report_type="records", public=True)
    institution_view_html = job.render_dashboard_view(report_type="institutions")
    public_institution_view_html = job.render_dashboard_view(
        report_type="institutions",
        public=True,
    )
    map_collections_view_html = job.render_dashboard_view(report_type="map-collections")
    public_map_collections_view_html = job.render_dashboard_view(
        report_type="map_collections",
        public=True,
    )
    arcgis_due_dashboard_html = job.render_dashboard_view(
        report_type="due",
        workflow="py_arcgis_hub",
    )
    arcgis_retrospective_html = job.render_dashboard_view(
        report_type="retrospective",
        workflow="py_arcgis_hub",
    )
    socrata_retrospective_html = job.render_dashboard_view(
        report_type="retrospective",
        workflow="py_socrata",
    )
    workflow_queue = job.build_workflow_queue()

    arcgis_task = task_df.loc[task_df["ID"] == "py_arcgis_hub"].iloc[0]
    socrata_task = task_df.loc[task_df["ID"] == "py_socrata"].iloc[0]
    geology_index = task_df.loc[task_df["ID"] == "task-3"].iloc[0]

    assert len(task_df.loc[task_df["Effective Harvest Workflow"] == "py_arcgis_hub"]) == 1
    assert arcgis_task["Title"] == "Scan ArcGIS Hubs"
    assert arcgis_task["Due Date"] == "2026-03-15"
    assert arcgis_task["Due Status"] == "Due"
    assert arcgis_task["Website Name"] == "2 websites"
    assert arcgis_task["Website Match Count"] == "2"
    assert arcgis_task["Effective Harvest Workflow"] == "py_arcgis_hub"

    assert len(task_df.loc[task_df["Effective Harvest Workflow"] == "py_socrata"]) == 1
    assert socrata_task["Title"] == "Scan Socrata Sites"
    assert socrata_task["Due Date"] == "2026-03-30"
    assert socrata_task["Due Status"] == "Due"
    assert socrata_task["Website Name"] == "2 websites"
    assert socrata_task["Website Match Count"] == "2"

    assert geology_index["Due Date"] == ""
    assert geology_index["Due Status"] == "No Schedule"

    assert "2026-03-30" in dashboard_html
    assert "Scan Socrata Sites" not in dashboard_html
    assert "To be harvested (1)" in dashboard_html
    assert "To be reviewed (1)" in dashboard_html
    assert "Geology Index" in dashboard_html
    assert "py_socrata" not in dashboard_html
    assert "https://geo.btaa.org/admin/documents?f%5Bb1g_websitePlatform_s%5D%5B%5D=Socrata&amp;f%5Bgbl_resourceClass_sm%5D%5B%5D=Series&amp;rows=20&amp;sort=score+desc" not in dashboard_html
    assert "https://geo.btaa.org/admin/documents/site-5/edit" in dashboard_html
    assert "https://github.com/geobtaa/harvest-operations/issues/new" in dashboard_html
    assert "template=harvest-task.md" in dashboard_html
    assert "Create issue" in dashboard_html
    assert "Get Latest Source CSVs" in dashboard_html
    assert "f%5Bb1g_publication_state_s%5D%5B%5D=draft" in dashboard_html
    assert "Workflow Run Queue" in dashboard_html
    assert "Open ArcGIS Harvester" in dashboard_html
    assert "/static/arcgis.html" in dashboard_html
    assert "Open Socrata Harvester" in dashboard_html
    assert "/static/socrata.html" in dashboard_html
    assert "Open PASDA Harvester" in dashboard_html
    assert "within the next month" in dashboard_html
    assert "Create issue" not in public_dashboard_html
    assert "Create issue" not in public_due_dashboard_html
    assert "Get Latest Source CSVs" not in public_dashboard_html
    assert "Workflow Run Queue" not in public_dashboard_html
    assert "https://geo.btaa.org/admin/documents/site-5/edit" not in public_dashboard_html
    assert "https://geo.btaa.org/?search_field=all_fields&amp;q=%2205f-01%22" in public_dashboard_html
    assert "https://geo.btaa.org/?search_field=all_fields&amp;q=%2227d-01%22" in public_arcgis_dashboard_html
    assert "https://geo.btaa.org/admin/documents/task-1/edit" not in public_arcgis_dashboard_html
    assert "https://geo.btaa.org/admin/documents/site-1/edit" not in public_arcgis_dashboard_html
    assert "Reviews due" in dashboard_html
    assert "Harvests due" in dashboard_html
    assert "Harvest Records" in records_dashboard_html
    assert records_dashboard_html == records_view_html
    assert public_records_dashboard_html == public_records_view_html
    assert institution_dashboard_html == institution_view_html
    assert public_institution_dashboard_html == public_institution_view_html
    assert map_collections_dashboard_html == map_collections_view_html
    assert public_map_collections_dashboard_html == public_map_collections_view_html
    assert "Harvest Task Retrospective" in public_retrospective_dashboard_html
    assert "County Parcels" not in records_dashboard_html
    assert "Scan Socrata Sites" not in records_dashboard_html
    assert "py_socrata" not in records_dashboard_html
    assert "Actions" not in records_dashboard_html
    assert "Weekly" in records_dashboard_html
    assert "Irregular" in records_dashboard_html
    assert "Parcel Fabric" in records_dashboard_html
    assert "Geology Index" in records_dashboard_html
    assert "https://geo.btaa.org/?search_field=all_fields&amp;q=%2205f-01%22" in records_dashboard_html
    assert "https://geo.btaa.org/?search_field=all_fields&amp;q=%2205f-01%22" in public_records_dashboard_html
    assert "https://geo.btaa.org/admin/documents/site-5/edit" not in records_dashboard_html
    assert "Create issue" not in records_dashboard_html
    assert "Get Latest Source CSVs" not in records_dashboard_html
    assert records_dashboard_html.index("Parcel Fabric") < records_dashboard_html.index("Geology Index")
    assert "Transit Stops" not in records_dashboard_html
    assert "Building Permits" not in records_dashboard_html
    assert "Harvest Records by Institution" in institution_dashboard_html
    assert "Get Latest Source CSVs" not in institution_dashboard_html
    assert "Table of Contents" in institution_dashboard_html
    assert "Geoportal item counts are loaded from the development metadata API facet." in (
        institution_dashboard_html
    )
    assert "University of Minnesota" in institution_dashboard_html
    assert "Other" in institution_dashboard_html
    assert "Parcel Fabric" in institution_dashboard_html
    assert "County Parcels" in institution_dashboard_html
    assert "Road Centerlines" in institution_dashboard_html
    assert 'href="#university-of-minnesota"' in institution_dashboard_html
    assert 'href="#other"' in institution_dashboard_html
    assert 'id="university-of-minnesota"' in institution_dashboard_html
    assert 'id="other"' in institution_dashboard_html
    assert '<div class="date-line">' not in institution_dashboard_html
    assert "No schedule" not in institution_dashboard_html
    assert "Geoportal items: 12" in institution_dashboard_html
    assert "Geoportal items: 3" in institution_dashboard_html
    assert "Geoportal items: Not available" in institution_dashboard_html
    assert "https://geo.btaa.org/?search_field=all_fields&amp;q=%2227d-01%22" in institution_dashboard_html
    assert "https://geo.btaa.org/admin/documents/task-1/edit" in institution_dashboard_html
    assert "https://geo.btaa.org/?search_field=all_fields&amp;q=%2205f-01%22" in public_institution_dashboard_html
    assert "https://geo.btaa.org/admin/documents/task-1/edit" not in public_institution_dashboard_html
    assert institution_dashboard_html.index("Transit Stops") < institution_dashboard_html.index("County Parcels")
    assert institution_dashboard_html.index("County Parcels") < institution_dashboard_html.index("Building Permits")
    assert "Map Collections" in map_collections_dashboard_html
    assert "Get Latest Source CSVs" not in map_collections_dashboard_html
    assert "Subject includes Maps" in map_collections_dashboard_html
    assert "Table of Contents" in map_collections_dashboard_html
    assert "County Parcels" in map_collections_dashboard_html
    assert "Road Centerlines" in map_collections_dashboard_html
    assert "Parcel Fabric" in map_collections_dashboard_html
    assert "Transit Stops" not in map_collections_dashboard_html
    assert "Building Permits" not in map_collections_dashboard_html
    assert "Geology Index" not in map_collections_dashboard_html
    assert "Count by Code:" not in map_collections_dashboard_html
    assert "Periodicity:" not in map_collections_dashboard_html
    assert "Geoportal items: 12" in map_collections_dashboard_html
    assert "Geoportal items: 3" in map_collections_dashboard_html
    assert "https://geo.btaa.org/admin/documents/task-1/edit" in map_collections_dashboard_html
    assert "https://geo.btaa.org/?search_field=all_fields&amp;q=%2227d-01%22" in (
        public_map_collections_dashboard_html
    )
    assert "https://geo.btaa.org/admin/documents/task-1/edit" not in (
        public_map_collections_dashboard_html
    )
    assert fetch_calls == 1
    assert "Harvest Tasks Due Now" in due_dashboard_html
    assert "Get Latest Source CSVs" not in due_dashboard_html
    assert "Reviews due" in due_dashboard_html
    assert "Harvests due" in due_dashboard_html
    assert "Scheduled" not in due_dashboard_html
    assert "No Schedule" not in due_dashboard_html
    assert "Scan Socrata Sites" not in due_dashboard_html
    assert "Parcel Fabric" not in due_dashboard_html
    assert "Geology Index" not in due_dashboard_html
    assert "Harvest Task Retrospective" in retrospective_dashboard_html
    assert "Get Latest Source CSVs" not in retrospective_dashboard_html
    assert "ArcGIS Hubs Harvest Report - 2026-03-30" in retrospective_dashboard_html
    assert "Socrata Harvest Report - 2026-03-30" in retrospective_dashboard_html
    assert ">Harvest</span>" in retrospective_dashboard_html
    assert 'href="/reports/2026-03-30_harvest-task-dashboard-py-arcgis-hub.html"' in (
        retrospective_dashboard_html
    )
    assert 'href="/reports/2026-03-30_harvest-task-dashboard-py-socrata.html"' in (
        retrospective_dashboard_html
    )
    assert "Total Records Found: 39; New Records: 9; Unpublished Records: 2" in (
        retrospective_dashboard_html
    )
    assert "Total Records Found: 18; New Records: 3; Unpublished Records: 1" in (
        retrospective_dashboard_html
    )
    assert 'href="/harvest-operations/2026-03-30/workflows/py-arcgis-hub/"' in (
        public_retrospective_dashboard_html
    )
    assert 'href="/harvest-operations/2026-03-30/workflows/py-socrata/"' in (
        public_retrospective_dashboard_html
    )
    assert "/reports/2026-03-30_harvest-task-dashboard-py-arcgis-hub-public.html" not in (
        public_retrospective_dashboard_html
    )
    assert "Scan ArcGIS Hubs" not in dashboard_html
    assert "py_arcgis_hub" not in dashboard_html
    assert "Scan ArcGIS Hubs" not in due_dashboard_html
    assert "Scan Socrata Sites" not in due_dashboard_html
    assert "py_arcgis_hub" in retrospective_dashboard_html
    assert "py_socrata" in retrospective_dashboard_html

    assert set(dedicated_dashboard_outputs) == {"py_arcgis_hub", "py_socrata"}
    assert set(public_dedicated_dashboard_outputs) == {"py_arcgis_hub", "py_socrata"}
    assert Path(dedicated_dashboard_outputs["py_arcgis_hub"]).name == (
        "2026-03-30_harvest-task-dashboard-py-arcgis-hub.html"
    )
    assert Path(dedicated_dashboard_outputs["py_socrata"]).name == (
        "2026-03-30_harvest-task-dashboard-py-socrata.html"
    )
    assert Path(results["records_dashboard_html"]).name == (
        "2026-03-30_harvest-task-dashboard-records.html"
    )
    assert Path(results["public_records_dashboard_html"]).name == (
        "2026-03-30_harvest-task-dashboard-records-public.html"
    )
    assert Path(results["institution_dashboard_html"]).name == (
        "2026-03-30_harvest-task-dashboard-institutions.html"
    )
    assert Path(results["public_institution_dashboard_html"]).name == (
        "2026-03-30_harvest-task-dashboard-institutions-public.html"
    )
    assert Path(results["map_collections_dashboard_html"]).name == (
        "2026-03-30_harvest-task-dashboard-map-collections.html"
    )
    assert Path(results["public_map_collections_dashboard_html"]).name == (
        "2026-03-30_harvest-task-dashboard-map-collections-public.html"
    )
    assert Path(public_dedicated_dashboard_outputs["py_arcgis_hub"]).name == (
        "2026-03-30_harvest-task-dashboard-py-arcgis-hub-public.html"
    )
    assert Path(public_dedicated_dashboard_outputs["py_socrata"]).name == (
        "2026-03-30_harvest-task-dashboard-py-socrata-public.html"
    )
    assert arcgis_dashboard_output_html == arcgis_dashboard_html
    assert public_arcgis_dashboard_output_html == public_arcgis_dashboard_html
    assert socrata_dashboard_output_html == socrata_dashboard_html
    assert public_socrata_dashboard_output_html == public_socrata_dashboard_html
    assert "ArcGIS Hubs Harvest Report - 2026-03-30" in arcgis_dashboard_html
    assert "Get Latest Source CSVs" not in arcgis_dashboard_html
    assert "Harvest report date" in arcgis_dashboard_html
    assert '<strong class="status-value">2026-03-30</strong>' in arcgis_dashboard_html
    assert "ArcGIS Hub Harvest Results" in arcgis_dashboard_html
    assert "Last Harvested" not in arcgis_dashboard_html
    assert "Total Records Found" in arcgis_dashboard_html
    assert "New Records" in arcgis_dashboard_html
    assert "Unpublished Records" in arcgis_dashboard_html
    assert "Harvest Run" in arcgis_dashboard_html
    assert 'class="report-error"' in arcgis_dashboard_html
    assert '<span class="run-pill run-pill--error">error</span>' in arcgis_dashboard_html
    assert "[ArcGIS] Failed site-3 - timeout" in arcgis_dashboard_html
    assert "[ArcGIS] Fetched" not in arcgis_dashboard_html
    assert "Endpoint" not in arcgis_dashboard_html
    assert "18" in arcgis_dashboard_html
    assert "21" in arcgis_dashboard_html
    assert "<th scope=\"row\">Total</th>" in arcgis_dashboard_html
    assert '<td class="number-cell" data-label="Total Records Found">39</td>' in (
        arcgis_dashboard_html
    )
    assert '<td class="number-cell" data-label="New Records">9</td>' in arcgis_dashboard_html
    assert '<td class="number-cell" data-label="Unpublished Records">2</td>' in (
        arcgis_dashboard_html
    )
    assert "County Parcels" in arcgis_dashboard_html
    assert "Road Centerlines" in arcgis_dashboard_html
    assert "Scan Socrata Sites" not in arcgis_dashboard_html
    assert "https://example.com/arcgis" not in arcgis_dashboard_html
    assert "Socrata Harvest Report - 2026-03-30" in socrata_dashboard_html
    assert "Socrata Harvest Results" in socrata_dashboard_html
    assert "Transit Stops" in socrata_dashboard_html
    assert "Building Permits" in socrata_dashboard_html
    assert '<td class="number-cell" data-label="Total Records Found">18</td>' in (
        socrata_dashboard_html
    )
    assert '<td class="number-cell" data-label="New Records">3</td>' in socrata_dashboard_html
    assert '<td class="number-cell" data-label="Unpublished Records">1</td>' in (
        socrata_dashboard_html
    )
    assert "County Parcels" not in socrata_dashboard_html

    assert arcgis_due_dashboard_html == arcgis_dashboard_html
    assert arcgis_retrospective_html == arcgis_dashboard_html
    assert socrata_retrospective_html == socrata_dashboard_html

    assert workflow_queue["harvest_queue_count"] == 3
    assert workflow_queue["harvest_due_count"] == 2
    assert workflow_queue["review_due_count"] == 0
    assert workflow_queue["queue_end_date"] == "2026-04-30"
    assert [workflow["workflow"] for workflow in workflow_queue["workflows"]] == [
        "py_arcgis_hub",
        "py_socrata",
        "py_pasda",
    ]
    assert workflow_queue["workflows"][0]["static_page_url"] == "/static/arcgis.html"
    assert workflow_queue["workflows"][0]["queue_count"] == 1
    assert workflow_queue["workflows"][0]["due_now_count"] == 1
    assert workflow_queue["workflows"][0]["workflow_input_csv"].endswith(
        "harvest-workflow-inputs/py-arcgis-hub.csv"
    )
    assert workflow_queue["workflows"][1]["static_page_url"] == "/static/socrata.html"
    assert workflow_queue["workflows"][2]["static_page_url"] == "/static/pasda.html"
    assert workflow_queue["workflows"][2]["next_due_date"] == "2026-04-05"
    assert workflow_queue["workflows"][2]["due_now_count"] == 0
    assert workflow_queue["source_downloads"][0]["url"].endswith(
        "f%5Bgbl_resourceClass_sm%5D%5B%5D=Series"
    )
    assert workflow_queue["source_downloads"][1]["url"].endswith(
        "f%5Bgbl_resourceClass_sm%5D%5B%5D=Websites"
    )

    workflow_inputs = results["workflow_inputs"]
    assert set(workflow_inputs) == {"py_arcgis_hub", "py_pasda", "py_socrata"}
    assert Path(workflow_inputs["py_arcgis_hub"]).exists()
    assert Path(workflow_inputs["py_pasda"]).exists()
    assert Path(workflow_inputs["py_socrata"]).exists()
    assert Path(workflow_inputs["py_arcgis_hub"]).parent.name == "harvest-workflow-inputs"
    assert Path(workflow_inputs["py_arcgis_hub"]).name == "py-arcgis-hub.csv"

    arcgis_workflow_df = pd.read_csv(workflow_inputs["py_arcgis_hub"], dtype=str).fillna("")
    pasda_workflow_df = pd.read_csv(workflow_inputs["py_pasda"], dtype=str).fillna("")
    socrata_workflow_df = pd.read_csv(workflow_inputs["py_socrata"], dtype=str).fillna("")

    assert set(arcgis_workflow_df["ID"]) == {"task-1", "task-1b"}
    assert set(pasda_workflow_df["ID"]) == {"task-3", "task-4"}
    assert set(socrata_workflow_df["ID"]) == {"task-2", "task-2b"}
    assert "Identifier" in arcgis_workflow_df.columns
    assert "Name" not in arcgis_workflow_df.columns
    assert "Created At" not in arcgis_workflow_df.columns
    assert "Updated At" not in arcgis_workflow_df.columns
    assert "Created At" not in pasda_workflow_df.columns
    assert "Updated At" not in socrata_workflow_df.columns


def test_harvest_task_dashboard_generates_standalone_websites_report(tmp_path: Path) -> None:
    harvest_records_path = tmp_path / "harvest-records.csv"
    websites_path = tmp_path / "websites.csv"
    standalone_websites_path = tmp_path / "standalone-websites.csv"
    code_schema_map_path = tmp_path / "code-schema-map.csv"
    outputs_dir = tmp_path / "outputs"

    pd.DataFrame(columns=["ID", "Title", "Harvest Workflow"]).to_csv(
        harvest_records_path, index=False
    )
    pd.DataFrame(columns=["ID", "Name", "Harvest Workflow", "URL"]).to_csv(
        websites_path, index=False
    )
    pd.DataFrame(
        [
            {
                "ID": "05b-27003",
                "Title": "Anoka County GIS Data Downloads",
                "Code": "w00_01",
            },
            {
                "ID": "11b-39003",
                "Title": "Allen County GIS Data Download Page",
                "Code": "w00_01",
            },
            {
                "ID": "1000f-0004",
                "Title": "Diversity Data Kids",
                "Code": "w00_01",
            },
        ]
    ).to_csv(standalone_websites_path, index=False)

    code_schema_map_path.write_text(
        "code_prefix,Related institution or source\n"
        "05,University of Minnesota\n"
        "11,The Ohio State University\n",
        encoding="utf-8",
    )

    job = HarvestTaskDashboardJob(
        {
            "harvest_records_csv": str(harvest_records_path),
            "websites_csv": str(websites_path),
            "standalone_websites_csv": str(standalone_websites_path),
            "code_schema_map_csv": str(code_schema_map_path),
            "output_tasks_csv": str(outputs_dir / "harvest-task-dashboard.csv"),
            "output_dashboard_html": str(outputs_dir / "harvest-task-dashboard.html"),
            "output_due_dashboard_html": str(outputs_dir / "harvest-task-dashboard-due.html"),
            "output_retrospective_dashboard_html": str(
                outputs_dir / "harvest-task-dashboard-retrospective.html"
            ),
            "output_workflow_dir": str(outputs_dir / "harvest-workflow-inputs"),
            "issue_repositories": [
                {
                    "name": "harvest-operations",
                    "issues_new_url": "https://github.com/geobtaa/harvest-operations/issues/new",
                    "template": "harvest-task.md",
                    "labels": ["harvest-task"],
                }
            ],
            "today": "2026-04-01",
        }
    )

    results = job.harvest_pipeline()

    standalone_dashboard_html = Path(results["standalone_dashboard_html"]).read_text(
        encoding="utf-8"
    )
    public_standalone_dashboard_html = Path(
        results["public_standalone_dashboard_html"]
    ).read_text(encoding="utf-8")
    standalone_view_html = job.render_dashboard_view(report_type="standalone")
    standalone_alias_html = job.render_dashboard_view(report_type="standalone-websites")

    assert standalone_dashboard_html == standalone_view_html
    assert standalone_dashboard_html == standalone_alias_html
    assert "Standalone Websites by Institution" in standalone_dashboard_html
    assert "Get Latest Source CSVs" not in standalone_dashboard_html
    assert "Table of Contents" in standalone_dashboard_html
    assert "University of Minnesota" in standalone_dashboard_html
    assert "The Ohio State University" in standalone_dashboard_html
    assert "Other" in standalone_dashboard_html
    assert "Anoka County GIS Data Downloads" in standalone_dashboard_html
    assert "Allen County GIS Data Download Page" in standalone_dashboard_html
    assert "Diversity Data Kids" in standalone_dashboard_html
    assert 'href="#university-of-minnesota"' in standalone_dashboard_html
    assert 'href="#the-ohio-state-university"' in standalone_dashboard_html
    assert 'href="#other"' in standalone_dashboard_html
    assert 'id="university-of-minnesota"' in standalone_dashboard_html
    assert 'id="the-ohio-state-university"' in standalone_dashboard_html
    assert 'id="other"' in standalone_dashboard_html
    assert "https://geo.btaa.org/catalog/05b-27003" in standalone_dashboard_html
    assert "https://geo.btaa.org/catalog/11b-39003" in public_standalone_dashboard_html
    assert "https://geo.btaa.org/catalog/1000f-0004" in standalone_dashboard_html
    assert "https://github.com/geobtaa/harvest-operations/issues/new" in standalone_dashboard_html
    assert "template=standalone-website.md" in standalone_dashboard_html
    assert "Create issue on GitHub for new website" in standalone_dashboard_html
    assert standalone_dashboard_html.count("Create issue</a>") == 3
    assert (
        "https://geo.btaa.org/admin/documents?q=&amp;f%5Bb1g_code_s%5D%5B%5D=w00_01"
        not in standalone_dashboard_html
    )
    assert "Create issue on GitHub for new website" not in public_standalone_dashboard_html
    assert "Create issue</a>" not in public_standalone_dashboard_html
    assert "<code>w00_01</code>" not in standalone_dashboard_html
    assert "Last harvested:" not in standalone_dashboard_html
    assert "Periodicity:" not in standalone_dashboard_html
    assert Path(results["standalone_dashboard_html"]).name == (
        "2026-04-01_harvest-task-dashboard-standalone-websites.html"
    )
    assert Path(results["public_standalone_dashboard_html"]).name == (
        "2026-04-01_harvest-task-dashboard-standalone-websites-public.html"
    )


def test_harvest_task_dashboard_supports_alphanumeric_institution_prefixes(
    tmp_path: Path,
) -> None:
    harvest_records_path = tmp_path / "harvest-records.csv"
    websites_path = tmp_path / "websites.csv"
    code_schema_map_path = tmp_path / "code-schema-map.csv"
    outputs_dir = tmp_path / "outputs"

    pd.DataFrame(
        [
            {
                "ID": "task-b1g",
                "Title": "Curated Dataset Set",
                "Harvest Workflow": "template_csv",
                "Identifier": "site-b1g",
                "Code": "b1g-0001",
                "Last Harvested": "2026-04-01",
                "Accrual Periodicity": "weekly",
            },
            {
                "ID": "task-01",
                "Title": "Indiana Dataset Set",
                "Harvest Workflow": "template_csv",
                "Identifier": "site-01",
                "Code": "01a-01",
                "Last Harvested": "2026-04-01",
                "Accrual Periodicity": "weekly",
            },
        ]
    ).to_csv(harvest_records_path, index=False)

    pd.DataFrame(columns=["ID", "Harvest Workflow", "Name", "URL"]).to_csv(
        websites_path, index=False
    )

    code_schema_map_path.write_text(
        "code_prefix,Related institution or source\n"
        "01,Indiana University\n"
        "b1g,BTAA-GIN curated datasets\n",
        encoding="utf-8",
    )

    job = HarvestTaskDashboardJob(
        {
            "harvest_records_csv": str(harvest_records_path),
            "websites_csv": str(websites_path),
            "code_schema_map_csv": str(code_schema_map_path),
            "output_tasks_csv": str(outputs_dir / "harvest-task-dashboard.csv"),
            "output_dashboard_html": str(outputs_dir / "harvest-task-dashboard.html"),
            "output_due_dashboard_html": str(outputs_dir / "harvest-task-dashboard-due.html"),
            "output_retrospective_dashboard_html": str(
                outputs_dir / "harvest-task-dashboard-retrospective.html"
            ),
            "output_workflow_dir": str(outputs_dir / "harvest-workflow-inputs"),
            "today": "2026-04-03",
        }
    )

    institution_html = job.render_dashboard_view(report_type="institutions")

    assert "BTAA-GIN curated datasets" in institution_html
    assert "Indiana University" in institution_html
    assert 'href="#btaa-gin-curated-datasets"' in institution_html
    assert "Curated Dataset Set" in institution_html
    assert "Indiana Dataset Set" in institution_html


def test_harvest_task_dashboard_marks_pending_updates_tag_as_due(tmp_path: Path) -> None:
    harvest_records_path = tmp_path / "harvest-records.csv"
    websites_path = tmp_path / "websites.csv"
    outputs_dir = tmp_path / "outputs"

    pd.DataFrame(
        [
            {
                "ID": "task-pending",
                "Title": "Pending Updates Task",
                "Harvest Workflow": "template_csv",
                "Last Harvested": "2026-03-29",
                "Accrual Periodicity": "irregular",
                "Tags": "queue:pending_updates|ops",
            }
        ]
    ).to_csv(harvest_records_path, index=False)

    pd.DataFrame(columns=["ID", "Harvest Workflow", "Name", "URL"]).to_csv(websites_path, index=False)

    job = HarvestTaskDashboardJob(
        {
            "harvest_records_csv": str(harvest_records_path),
            "websites_csv": str(websites_path),
            "output_tasks_csv": str(outputs_dir / "harvest-task-dashboard.csv"),
            "output_dashboard_html": str(outputs_dir / "harvest-task-dashboard.html"),
            "output_due_dashboard_html": str(outputs_dir / "harvest-task-dashboard-due.html"),
            "output_retrospective_dashboard_html": str(
                outputs_dir / "harvest-task-dashboard-retrospective.html"
            ),
            "output_workflow_dir": str(outputs_dir / "harvest-workflow-inputs"),
            "today": "2026-03-30",
        }
    )

    results = job.harvest_pipeline()

    task_df = pd.read_csv(results["task_csv"], dtype=str).fillna("")
    dashboard_html = Path(results["dashboard_html"]).read_text(encoding="utf-8")
    due_dashboard_html = Path(results["due_dashboard_html"]).read_text(encoding="utf-8")

    pending_task = task_df.loc[task_df["ID"] == "task-pending"].iloc[0]

    assert pending_task["Due Date"] == "2026-03-30"
    assert pending_task["Due Status"] == "Due"
    assert "Pending Updates Task" in dashboard_html
    assert "To be reviewed (1)" in dashboard_html
    assert "Pending Updates Task" in due_dashboard_html
    assert "2026-03-30" in due_dashboard_html


def test_harvest_task_dashboard_creates_reviews_section_for_due_irregular_review_tags(
    tmp_path: Path,
) -> None:
    harvest_records_path = tmp_path / "harvest-records.csv"
    websites_path = tmp_path / "websites.csv"
    outputs_dir = tmp_path / "outputs"

    pd.DataFrame(
        [
            {
                "ID": "review-annual",
                "Title": "Annual Review Task",
                "Harvest Workflow": "template_csv",
                "Last Harvested": "2025-03-15",
                "Accrual Periodicity": "Irregular",
                "Tags": "ops|review:1y|collection",
            },
            {
                "ID": "review-biennial",
                "Title": "Biennial Review Task",
                "Harvest Workflow": "template_csv",
                "Last Harvested": "2025-05-01",
                "Accrual Periodicity": "Irregular",
                "Tags": "review:2y|ops",
            },
        ]
    ).to_csv(harvest_records_path, index=False)

    pd.DataFrame(columns=["ID", "Harvest Workflow", "Name", "URL"]).to_csv(websites_path, index=False)

    job = HarvestTaskDashboardJob(
        {
            "harvest_records_csv": str(harvest_records_path),
            "websites_csv": str(websites_path),
            "output_tasks_csv": str(outputs_dir / "harvest-task-dashboard.csv"),
            "output_dashboard_html": str(outputs_dir / "harvest-task-dashboard.html"),
            "output_due_dashboard_html": str(outputs_dir / "harvest-task-dashboard-due.html"),
            "output_retrospective_dashboard_html": str(
                outputs_dir / "harvest-task-dashboard-retrospective.html"
            ),
            "output_workflow_dir": str(outputs_dir / "harvest-workflow-inputs"),
            "today": "2026-04-01",
        }
    )

    results = job.harvest_pipeline()

    task_df = pd.read_csv(results["task_csv"], dtype=str).fillna("")
    dashboard_html = Path(results["dashboard_html"]).read_text(encoding="utf-8")
    due_dashboard_html = Path(results["due_dashboard_html"]).read_text(encoding="utf-8")

    annual_review = task_df.loc[task_df["ID"] == "review-annual"].iloc[0]
    biennial_review = task_df.loc[task_df["ID"] == "review-biennial"].iloc[0]

    assert annual_review["Review Date"] == "2026-03-15"
    assert annual_review["Review Status"] == "Due"
    assert annual_review["Due Status"] == "No Schedule"

    assert biennial_review["Review Date"] == "2027-05-01"
    assert biennial_review["Review Status"] == "Scheduled"
    assert biennial_review["Due Status"] == "No Schedule"

    assert "To be reviewed (2)" in dashboard_html
    assert "Annual Review Task" in dashboard_html
    assert "2026-03-15" in dashboard_html
    assert "Biennial Review Task" in dashboard_html
    assert "To be harvested" not in dashboard_html
    assert "To be reviewed (1)" in due_dashboard_html
    assert "Annual Review Task" in due_dashboard_html
    assert "Biennial Review Task" not in due_dashboard_html


def test_harvest_task_dashboard_routes_pending_harvest_rows_to_harvest_section(
    tmp_path: Path,
) -> None:
    harvest_records_path = tmp_path / "harvest-records.csv"
    websites_path = tmp_path / "websites.csv"
    outputs_dir = tmp_path / "outputs"

    pd.DataFrame(
        [
            {
                "ID": "task-pending-harvest",
                "Title": "Pending Harvest Task",
                "Harvest Workflow": "template_csv",
                "Last Harvested": "2026-03-29",
                "Accrual Periodicity": "Irregular",
                "Tags": "queue:pending_harvest|harvest_due:2026-04-01|ops",
            }
        ]
    ).to_csv(harvest_records_path, index=False)

    pd.DataFrame(columns=["ID", "Harvest Workflow", "Name", "URL"]).to_csv(websites_path, index=False)

    job = HarvestTaskDashboardJob(
        {
            "harvest_records_csv": str(harvest_records_path),
            "websites_csv": str(websites_path),
            "output_tasks_csv": str(outputs_dir / "harvest-task-dashboard.csv"),
            "output_dashboard_html": str(outputs_dir / "harvest-task-dashboard.html"),
            "output_due_dashboard_html": str(outputs_dir / "harvest-task-dashboard-due.html"),
            "output_retrospective_dashboard_html": str(
                outputs_dir / "harvest-task-dashboard-retrospective.html"
            ),
            "output_workflow_dir": str(outputs_dir / "harvest-workflow-inputs"),
            "today": "2026-04-01",
        }
    )

    results = job.harvest_pipeline()

    task_df = pd.read_csv(results["task_csv"], dtype=str).fillna("")
    dashboard_html = Path(results["dashboard_html"]).read_text(encoding="utf-8")
    due_dashboard_html = Path(results["due_dashboard_html"]).read_text(encoding="utf-8")
    pending_harvest_task = task_df.loc[task_df["ID"] == "task-pending-harvest"].iloc[0]

    assert pending_harvest_task["Due Date"] == "2026-04-01"
    assert pending_harvest_task["Due Status"] == "Due"
    assert "Pending Harvest Task" in dashboard_html
    assert "To be harvested (1)" in dashboard_html
    assert "To be reviewed" not in dashboard_html
    assert "Pending Harvest Task" in due_dashboard_html
    assert "2026-04-01" in due_dashboard_html


def test_harvest_task_dashboard_generates_retrospective_report_with_month_grouping(
    tmp_path: Path,
) -> None:
    harvest_records_path = tmp_path / "harvest-records.csv"
    websites_path = tmp_path / "websites.csv"
    outputs_dir = tmp_path / "outputs"

    pd.DataFrame(
        [
            {
                "ID": "task-retro-1",
                "Title": "Retro Record One",
                "Identifier": "retro-1",
                "Harvest Workflow": "template_csv",
                "Last Harvested": "2026-03-15",
                "Provenance": '2026-03-20 / review / completed|2026-04-01 / augment / Resource Type to "Index maps|Aerial Photographs"|2026-04-05 / harvest / added 365, retired 1',
            },
            {
                "ID": "task-retro-2",
                "Title": "Retro Record Two",
                "Identifier": "retro-2",
                "Harvest Workflow": "template_json",
                "Last Harvested": "2026-04-10",
                "Provenance": "2026-04-12 / harvest|2026-04-13 / ingest",
            },
        ]
    ).to_csv(harvest_records_path, index=False)

    pd.DataFrame(columns=["ID", "Harvest Workflow", "Name", "URL"]).to_csv(websites_path, index=False)

    job = HarvestTaskDashboardJob(
        {
            "harvest_records_csv": str(harvest_records_path),
            "websites_csv": str(websites_path),
            "output_tasks_csv": str(outputs_dir / "harvest-task-dashboard.csv"),
            "output_dashboard_html": str(outputs_dir / "harvest-task-dashboard.html"),
            "output_due_dashboard_html": str(outputs_dir / "harvest-task-dashboard-due.html"),
            "output_retrospective_dashboard_html": str(
                outputs_dir / "harvest-task-dashboard-retrospective.html"
            ),
            "output_workflow_dir": str(outputs_dir / "harvest-workflow-inputs"),
            "arcgis_reports_dir": str(outputs_dir),
            "today": "2026-04-15",
        }
    )

    results = job.harvest_pipeline()

    retrospective_dashboard_html = Path(results["retrospective_dashboard_html"]).read_text(
        encoding="utf-8"
    )
    retrospective_view_html = job.render_dashboard_view(report_type="retrospective")
    arcgis_retrospective_html = job.render_dashboard_view(
        report_type="retrospective",
        workflow="py_arcgis_hub",
    )

    assert "Harvest Task Retrospective" in retrospective_dashboard_html
    assert "April 2026" in retrospective_dashboard_html
    assert "March 2026" in retrospective_dashboard_html
    assert "Total Actions" not in retrospective_dashboard_html
    assert "Harvested</span>" not in retrospective_dashboard_html
    assert "Reviewed</span>" not in retrospective_dashboard_html
    assert "Months</span>" not in retrospective_dashboard_html
    assert "4 actions" in retrospective_dashboard_html
    assert "1 action" in retrospective_dashboard_html
    assert "2026-04-12" in retrospective_dashboard_html
    assert "2026-03-20" in retrospective_dashboard_html
    assert ">review</span>" in retrospective_dashboard_html
    assert ">harvest</span>" in retrospective_dashboard_html
    assert ">ingest</span>" in retrospective_dashboard_html
    assert ">augment</span>" in retrospective_dashboard_html
    assert ".status-pill--harvest { color: var(--action-harvest); background: var(--action-harvest-soft); }" in retrospective_dashboard_html
    assert ".status-pill--review { color: var(--action-review); background: var(--action-review-soft); }" in retrospective_dashboard_html
    assert ".status-pill--ingest { color: var(--action-ingest); background: var(--action-ingest-soft); }" in retrospective_dashboard_html
    assert ".status-pill--other { color: var(--action-other); background: var(--action-other-soft); }" in retrospective_dashboard_html
    assert 'class="status-pill status-pill--harvest">harvest</span>' in retrospective_dashboard_html
    assert 'class="status-pill status-pill--review">review</span>' in retrospective_dashboard_html
    assert 'class="status-pill status-pill--ingest">ingest</span>' in retrospective_dashboard_html
    assert 'class="status-pill status-pill--other">augment</span>' in retrospective_dashboard_html
    assert ">completed</div>" in retrospective_dashboard_html
    assert ">added 365, retired 1</div>" in retrospective_dashboard_html
    assert "harvest / added 365, retired 1" not in retrospective_dashboard_html
    assert "2026-04-12" in retrospective_dashboard_html
    assert "Not provided" not in retrospective_dashboard_html
    assert '>Resource Type to &quot;Index maps|Aerial Photographs&quot;</div>' in retrospective_dashboard_html
    assert retrospective_dashboard_html.count(">harvest</span>") >= 2
    assert "Last Harvested field" not in retrospective_dashboard_html
    assert "2026-04-10" not in retrospective_dashboard_html
    assert "2026-03-15" not in retrospective_dashboard_html
    assert "Harvest Task Retrospective" in retrospective_view_html
    assert "ArcGIS Hubs Harvest Report - Unknown date" in arcgis_retrospective_html
    assert "Harvest report date" in arcgis_retrospective_html
    assert "ArcGIS Hub Harvest Results" in arcgis_retrospective_html
    assert "No ArcGIS Hub harvest records were found in the input file." in arcgis_retrospective_html


def test_harvest_task_issue_body_includes_hidden_task_marker() -> None:
    job = HarvestTaskDashboardJob({"today": "2026-04-01"})

    body = job._build_issue_body(
        {
            "ID": "harvest_ornl",
            "Title": "Harvest record for ORNL LandScan Viewer",
            "Due Date": "2026-04-30",
            "Last Harvested": "2026-03-30",
            "Identifier": "04a-01",
        }
    )

    assert "<!-- harvest-task-key: harvest:harvest_ornl:2026-04-30 -->" in body


def test_standalone_website_issue_body_includes_hidden_task_marker() -> None:
    job = HarvestTaskDashboardJob({"today": "2026-04-01"})

    body = job._build_standalone_issue_body(
        {
            "ID": "05b-27003",
            "Title": "Anoka County GIS Data Downloads",
            "Identifier": "https://www.anokacounty.us/1990/Data-Downloads",
            "Website Platform": "Other",
            "Code": "w00_01",
            "__institution_group": "University of Minnesota",
        }
    )

    assert "<!-- harvest-task-key: standalone:05b-27003 -->" in body


def test_harvest_task_dashboard_links_existing_issue_when_marker_matches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harvest_records_path = tmp_path / "harvest-records.csv"
    websites_path = tmp_path / "websites.csv"
    outputs_dir = tmp_path / "outputs"

    pd.DataFrame(
        [
            {
                "ID": "harvest_ornl",
                "Title": "Harvest record for ORNL LandScan Viewer",
                "Harvest Workflow": "template_csv",
                "Identifier": "04a-01",
                "Code": "04a-01",
                "Last Harvested": "2026-03-30",
                "Accrual Periodicity": "monthly",
            }
        ]
    ).to_csv(harvest_records_path, index=False)

    pd.DataFrame(columns=["ID", "Harvest Workflow", "Name", "URL"]).to_csv(websites_path, index=False)

    job = HarvestTaskDashboardJob(
        {
            "harvest_records_csv": str(harvest_records_path),
            "websites_csv": str(websites_path),
            "output_tasks_csv": str(outputs_dir / "harvest-task-dashboard.csv"),
            "output_dashboard_html": str(outputs_dir / "harvest-task-dashboard.html"),
            "output_due_dashboard_html": str(outputs_dir / "harvest-task-dashboard-due.html"),
            "output_retrospective_dashboard_html": str(
                outputs_dir / "harvest-task-dashboard-retrospective.html"
            ),
            "output_workflow_dir": str(outputs_dir / "harvest-workflow-inputs"),
            "issue_repositories": [
                {
                    "name": "harvest-operations",
                    "repository": "geobtaa/harvest-operations",
                    "issues_new_url": "https://github.com/geobtaa/harvest-operations/issues/new",
                    "template": "harvest-task.md",
                    "lookup_existing_issues": True,
                    "labels": ["harvest-task"],
                }
            ],
            "today": "2026-04-01",
        }
    )

    def fake_fetch_existing_issue_index(
        issue_repository: dict[str, str],
        repository_slug: str,
    ) -> dict[str, dict[str, str]]:
        assert issue_repository["name"] == "harvest-operations"
        assert repository_slug == "geobtaa/harvest-operations"
        return {
            "harvest:harvest_ornl:2026-04-30": {
                "html_url": "https://github.com/geobtaa/harvest-operations/issues/123",
                "number": "123",
                "state": "open",
            }
        }

    monkeypatch.setattr(job, "_fetch_existing_issue_index", fake_fetch_existing_issue_index)

    results = job.harvest_pipeline()
    dashboard_html = Path(results["dashboard_html"]).read_text(encoding="utf-8")
    public_dashboard_html = Path(results["public_dashboard_html"]).read_text(encoding="utf-8")
    public_view_html = job.render_dashboard_view(public=True)
    public_retrospective_html = job.render_dashboard_view(report_type="retrospective", public=True)

    assert "Open issue #123" in dashboard_html
    assert "https://github.com/geobtaa/harvest-operations/issues/123" in dashboard_html
    assert "Create issue" not in dashboard_html
    assert "Open issue #123" in public_dashboard_html
    assert "https://github.com/geobtaa/harvest-operations/issues/123" in public_dashboard_html
    assert "Create issue" not in public_dashboard_html
    assert "https://geo.btaa.org/?search_field=all_fields&amp;q=%2204a-01%22" in public_dashboard_html
    assert "https://geo.btaa.org/admin/documents/harvest_ornl/edit" not in public_dashboard_html
    assert "https://geo.btaa.org/admin/documents/04a-01/edit" not in public_dashboard_html
    assert "https://geo.btaa.org/?search_field=all_fields&amp;q=%2204a-01%22" in public_retrospective_html
    assert "https://geo.btaa.org/admin/documents/harvest_ornl/edit" not in public_retrospective_html
    assert public_view_html == public_dashboard_html
