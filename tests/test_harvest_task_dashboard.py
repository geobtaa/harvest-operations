from pathlib import Path

import pandas as pd

from scripts.harvest_task_dashboard import HarvestTaskDashboardJob


def test_harvest_task_dashboard_generates_outputs_and_workflow_splits(tmp_path: Path) -> None:
    harvest_records_path = tmp_path / "harvest-records.csv"
    websites_path = tmp_path / "websites.csv"
    outputs_dir = tmp_path / "outputs"

    pd.DataFrame(
        [
            {
                "ID": "task-1",
                "Title": "County Parcels",
                "Harvest Workflow": "py_arcgis_hub",
                "Identifier": "site-1",
                "Last Harvested": "2026-02-15",
                "Accrual Periodicity": "monthly",
            },
            {
                "ID": "task-1b",
                "Title": "Road Centerlines",
                "Harvest Workflow": "py_arcgis_hub",
                "Identifier": "site-3",
                "Last Harvested": "2026-02-20",
                "Accrual Periodicity": "monthly",
            },
            {
                "ID": "task-2",
                "Title": "Transit Stops",
                "Harvest Workflow": "py_socrata",
                "Identifier": "site-2",
                "Last Harvested": "2026-03-23",
                "Accrual Periodicity": "weekly",
            },
            {
                "ID": "task-2b",
                "Title": "Building Permits",
                "Harvest Workflow": "py_socrata",
                "Identifier": "site-4",
                "Last Harvested": "2026-03-28",
                "Accrual Periodicity": "weekly",
            },
            {
                "ID": "task-3",
                "Title": "Geology Index",
                "Harvest Workflow": "py_pasda",
                "Identifier": "",
                "Last Harvested": "2026-01-01",
                "Accrual Periodicity": "irregular",
            },
            {
                "ID": "task-4",
                "Title": "Parcel Fabric",
                "Harvest Workflow": "py_pasda",
                "Identifier": "",
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
        ]
    ).to_csv(websites_path, index=False)

    job = HarvestTaskDashboardJob(
        {
            "harvest_records_csv": str(harvest_records_path),
            "websites_csv": str(websites_path),
            "output_tasks_csv": str(outputs_dir / "harvest-task-dashboard.csv"),
            "output_dashboard_html": str(outputs_dir / "harvest-task-dashboard.html"),
            "output_workflow_dir": str(outputs_dir / "harvest-workflow-inputs"),
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

    results = job.harvest_pipeline()

    task_df = pd.read_csv(results["task_csv"], dtype=str).fillna("")
    dashboard_html = Path(results["dashboard_html"]).read_text(encoding="utf-8")

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

    assert "2026-03-15" in dashboard_html
    assert "Scan ArcGIS Hubs" in dashboard_html
    assert "Scan Socrata Sites" in dashboard_html
    assert "2 websites" in dashboard_html
    assert "Due (" in dashboard_html
    assert "Scheduled (" in dashboard_html
    assert "No Schedule (" in dashboard_html
    assert "py_arcgis_hub" in dashboard_html
    assert "py_socrata" in dashboard_html
    assert "https://github.com/geobtaa/harvest-operations/issues/new" in dashboard_html
    assert "template=harvest-task.md" in dashboard_html
    assert "Create issue" in dashboard_html

    workflow_inputs = results["workflow_inputs"]
    assert set(workflow_inputs) == {"py_arcgis_hub", "py_socrata"}
    assert Path(workflow_inputs["py_arcgis_hub"]).exists()
    assert Path(workflow_inputs["py_socrata"]).exists()
