from pathlib import Path

import pandas as pd
import pytest

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
                "Identifier": "site-5",
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
    due_dashboard_html = Path(results["due_dashboard_html"]).read_text(encoding="utf-8")
    retrospective_dashboard_html = Path(results["retrospective_dashboard_html"]).read_text(
        encoding="utf-8"
    )
    dedicated_dashboard_outputs = results["dedicated_dashboard_html"]
    arcgis_dashboard_output_html = Path(
        dedicated_dashboard_outputs["py_arcgis_hub"]
    ).read_text(encoding="utf-8")
    arcgis_dashboard_html = job.render_dashboard_view(workflow="py_arcgis_hub")
    arcgis_due_dashboard_html = job.render_dashboard_view(
        report_type="due",
        workflow="py_arcgis_hub",
    )
    arcgis_retrospective_html = job.render_dashboard_view(
        report_type="retrospective",
        workflow="py_arcgis_hub",
    )

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
    assert "Scan Socrata Sites" in dashboard_html
    assert "To be harvested (2)" in dashboard_html
    assert "To be reviewed (1)" in dashboard_html
    assert "Geology Index" in dashboard_html
    assert "py_socrata" in dashboard_html
    assert "https://geo.btaa.org/admin/documents?f%5Bb1g_websitePlatform_s%5D%5B%5D=Socrata&amp;f%5Bgbl_resourceClass_sm%5D%5B%5D=Series&amp;rows=20&amp;sort=score+desc" in dashboard_html
    assert "https://geo.btaa.org/admin/documents/site-5/edit" in dashboard_html
    assert "https://github.com/geobtaa/harvest-operations/issues/new" in dashboard_html
    assert "template=harvest-task.md" in dashboard_html
    assert "Create issue" in dashboard_html
    assert "Reviews due" in dashboard_html
    assert "Harvests due" in dashboard_html
    assert "Harvest Tasks Due Now" in due_dashboard_html
    assert "Reviews due" in due_dashboard_html
    assert "Harvests due" in due_dashboard_html
    assert "Scheduled" not in due_dashboard_html
    assert "No Schedule" not in due_dashboard_html
    assert "Scan Socrata Sites" in due_dashboard_html
    assert "Parcel Fabric" not in due_dashboard_html
    assert "Geology Index" not in due_dashboard_html
    assert "Harvest Task Retrospective" in retrospective_dashboard_html
    assert "Scan ArcGIS Hubs" not in dashboard_html
    assert "py_arcgis_hub" not in dashboard_html
    assert "Scan ArcGIS Hubs" not in due_dashboard_html
    assert "py_arcgis_hub" not in retrospective_dashboard_html

    assert set(dedicated_dashboard_outputs) == {"py_arcgis_hub"}
    assert Path(dedicated_dashboard_outputs["py_arcgis_hub"]).name == (
        "2026-03-30_harvest-task-dashboard-py-arcgis-hub.html"
    )
    assert arcgis_dashboard_output_html == arcgis_dashboard_html
    assert "ArcGIS Hubs Harvest Overview" in arcgis_dashboard_html
    assert "Last time the process was run" in arcgis_dashboard_html
    assert "2026-02-20" in arcgis_dashboard_html
    assert "Currently Harvested ArcGIS Hubs" in arcgis_dashboard_html
    assert "County Parcels" in arcgis_dashboard_html
    assert "Road Centerlines" in arcgis_dashboard_html
    assert "Scan Socrata Sites" not in arcgis_dashboard_html
    assert "https://example.com/arcgis" not in arcgis_dashboard_html

    assert arcgis_due_dashboard_html == arcgis_dashboard_html
    assert arcgis_retrospective_html == arcgis_dashboard_html

    workflow_inputs = results["workflow_inputs"]
    assert set(workflow_inputs) == {"py_arcgis_hub", "py_pasda", "py_socrata"}
    assert Path(workflow_inputs["py_arcgis_hub"]).exists()
    assert Path(workflow_inputs["py_pasda"]).exists()
    assert Path(workflow_inputs["py_socrata"]).exists()


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
                "Tags": "queue:pending_harvest|ops",
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
                "Provenance": "2026-04-12 / harvest",
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
    assert "Total Actions" in retrospective_dashboard_html
    assert "<strong>4</strong>" in retrospective_dashboard_html
    assert "3 actions" in retrospective_dashboard_html
    assert "1 action" in retrospective_dashboard_html
    assert "2026-04-12" in retrospective_dashboard_html
    assert "2026-03-20" in retrospective_dashboard_html
    assert ">review</span>" in retrospective_dashboard_html
    assert ">harvest</span>" in retrospective_dashboard_html
    assert ">augment</span>" in retrospective_dashboard_html
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
    assert "ArcGIS Hubs Harvest Overview" in arcgis_retrospective_html
    assert "Last time the process was run" in arcgis_retrospective_html
    assert "Currently Harvested ArcGIS Hubs" in arcgis_retrospective_html
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

    assert "Open issue #123" in dashboard_html
    assert "https://github.com/geobtaa/harvest-operations/issues/123" in dashboard_html
    assert "Create issue" not in dashboard_html
