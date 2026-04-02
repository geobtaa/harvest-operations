from __future__ import annotations

from html import escape
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests


UNSCHEDULED_PERIODICITIES = {
    "",
    "ad hoc",
    "as needed",
    "irregular",
    "not planned",
    "none",
    "one time",
    "once",
    "unknown",
}

CONSOLIDATED_WORKFLOW_TITLES = {
    "py_arcgis_hub": "Scan ArcGIS Hubs",
    "py_socrata": "Scan Socrata Sites",
}

HARVEST_RECORD_LINKS = {
    "py_arcgis_hub": "https://geo.btaa.org/admin/documents?f%5Bb1g_harvestWorkflow_s%5D%5B%5D=py_arcgis_hub&f%5Bgbl_resourceClass_sm%5D%5B%5D=Series&rows=20&sort=score+desc",
    "py_socrata": "https://geo.btaa.org/admin/documents?f%5Bb1g_websitePlatform_s%5D%5B%5D=Socrata&f%5Bgbl_resourceClass_sm%5D%5B%5D=Series&rows=20&sort=score+desc",
}

DEFAULT_DEDICATED_WORKFLOW_VIEWS = ("py_arcgis_hub",)
ISSUE_TASK_MARKER_PREFIX = "harvest-task-key"


class HarvestTaskDashboardJob:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.harvest_records_path = Path(config.get("harvest_records_csv", "inputs/harvest-records.csv"))
        self.websites_path = Path(config.get("websites_csv", "inputs/websites.csv"))
        self.output_tasks_csv = Path(config.get("output_tasks_csv", "reports/harvest-task-dashboard.csv"))
        self.output_dashboard_html = Path(
            config.get("output_dashboard_html", "reports/harvest-task-dashboard.html")
        )
        self.output_due_dashboard_html = Path(
            config.get("output_due_dashboard_html", "reports/harvest-task-dashboard-due.html")
        )
        self.output_retrospective_dashboard_html = Path(
            config.get(
                "output_retrospective_dashboard_html",
                "reports/harvest-task-dashboard-retrospective.html",
            )
        )
        self.output_workflow_dir = Path(
            config.get("output_workflow_dir", "inputs/harvest-workflow-inputs")
        )
        self.issue_repositories = config.get("issue_repositories", [])
        configured_dedicated_workflows = config.get(
            "dedicated_workflow_views",
            DEFAULT_DEDICATED_WORKFLOW_VIEWS,
        )
        if isinstance(configured_dedicated_workflows, str):
            configured_dedicated_workflows = [configured_dedicated_workflows]
        self.dedicated_workflow_views = tuple(
            dict.fromkeys(
                workflow
                for workflow in (
                    self._clean_value(value) for value in configured_dedicated_workflows
                )
                if workflow
            )
        )

        configured_today = config.get("today")
        if configured_today:
            self.today = pd.Timestamp(configured_today).normalize()
        else:
            self.today = pd.Timestamp.now().normalize()
        self._issue_index_cache: dict[str, dict[str, dict[str, str]]] = {}

    def harvest_pipeline(self) -> dict[str, Any]:
        harvest_df = self._load_csv(self.harvest_records_path)
        websites_df = self._load_csv(self.websites_path)

        task_df = self._build_task_dataframe(harvest_df, websites_df)
        main_task_df = self._filter_task_view(task_df)
        main_harvest_df = self._filter_harvest_view(harvest_df)
        dashboard_html = self._render_dashboard_html(
            main_task_df,
            report_title=self._report_title(),
        )
        due_dashboard_html = self._render_dashboard_html(
            self._filter_due_only_tasks(main_task_df),
            report_title=self._report_title(report_type="due"),
        )
        retrospective_dashboard_html = self._render_retrospective_html(
            main_harvest_df,
            report_title=self._report_title(report_type="retrospective"),
        )

        task_output_path = self._write_dataframe(task_df, self.output_tasks_csv)
        dashboard_output_path = self._write_text(dashboard_html, self.output_dashboard_html)
        due_dashboard_output_path = self._write_text(due_dashboard_html, self.output_due_dashboard_html)
        retrospective_dashboard_output_path = self._write_text(
            retrospective_dashboard_html, self.output_retrospective_dashboard_html
        )
        dedicated_dashboard_outputs = self._write_dedicated_workflow_dashboards(harvest_df)
        workflow_outputs = self._write_workflow_inputs(websites_df)

        summary = self._build_summary(task_df)
        return {
            "status": "completed",
            "task_count": len(task_df),
            "workflow_count": len(workflow_outputs),
            "summary": summary,
            "task_csv": str(task_output_path),
            "dashboard_html": str(dashboard_output_path),
            "due_dashboard_html": str(due_dashboard_output_path),
            "retrospective_dashboard_html": str(retrospective_dashboard_output_path),
            "dedicated_dashboard_html": dedicated_dashboard_outputs,
            "workflow_inputs": workflow_outputs,
        }

    def render_dashboard_view(
        self,
        embedded: bool = False,
        report_type: str = "full",
        workflow: str = "",
    ) -> str:
        harvest_df = self._load_csv(self.harvest_records_path)
        websites_df = self._load_csv(self.websites_path)
        task_df = self._build_task_dataframe(harvest_df, websites_df)
        normalized_report_type = self._clean_value(report_type).lower()
        scoped_workflow = self._clean_value(workflow)
        report_workflow, workflow_from_report_type = self._extract_workflow_from_report_type(
            normalized_report_type
        )
        if workflow_from_report_type:
            scoped_workflow = report_workflow
            normalized_report_type = "full"
        if self._use_combined_workflow_view(scoped_workflow):
            return self._render_combined_workflow_html(
                self._filter_harvest_view(harvest_df, scoped_workflow),
                workflow=scoped_workflow,
                embedded=embedded,
            )
        if normalized_report_type == "due":
            task_df = self._filter_due_only_tasks(self._filter_task_view(task_df, scoped_workflow))
            return self._render_dashboard_html(
                task_df,
                embedded=embedded,
                report_title=self._report_title(report_type="due", workflow=scoped_workflow),
            )
        if normalized_report_type == "retrospective":
            return self._render_retrospective_html(
                self._filter_harvest_view(harvest_df, scoped_workflow),
                embedded=embedded,
                report_title=self._report_title(report_type="retrospective", workflow=scoped_workflow),
            )
        return self._render_dashboard_html(
            self._filter_task_view(task_df, scoped_workflow),
            embedded=embedded,
            report_title=self._report_title(workflow=scoped_workflow),
        )

    def _load_csv(self, path: Path) -> pd.DataFrame:
        df = pd.read_csv(path, dtype=str).fillna("")
        df.columns = [str(column).strip() for column in df.columns]
        return df

    def _build_task_dataframe(self, harvest_df: pd.DataFrame, websites_df: pd.DataFrame) -> pd.DataFrame:
        harvest_df = harvest_df.copy()
        websites_df = websites_df.copy()

        self._ensure_columns(
            harvest_df,
            ["ID", "Identifier", "Harvest Workflow", "Last Harvested", "Accrual Periodicity"],
        )
        self._ensure_columns(websites_df, ["ID", "Harvest Workflow"])

        website_columns = list(websites_df.columns)
        prefixed_website_columns = [f"Website {column}" for column in website_columns]

        websites_df["__normalized_id"] = websites_df["ID"].map(self._normalize_key)
        website_lookup = {}
        for _, website_row in websites_df.iterrows():
            website_key = website_row["__normalized_id"]
            if website_key:
                website_lookup.setdefault(website_key, []).append(website_row)

        task_rows: list[dict[str, Any]] = []

        for _, harvest_row in harvest_df.iterrows():
            row_dict = harvest_row.to_dict()
            base_task = row_dict.copy()

            due_date = self._calculate_due_date(
                row_dict.get("Last Harvested", ""),
                row_dict.get("Accrual Periodicity", ""),
            )
            base_task["Due Date"] = due_date.strftime("%Y-%m-%d") if due_date is not None else ""
            base_task["Due Status"] = self._determine_due_status(
                due_date,
                row_dict.get("Accrual Periodicity", ""),
                row_dict,
            )
            base_task["Days Until Due"] = (
                str((due_date - self.today).days) if due_date is not None else ""
            )
            review_date = self._calculate_review_date(
                row_dict.get("Last Harvested", ""),
                row_dict.get("Accrual Periodicity", ""),
                row_dict,
            )
            base_task["Review Date"] = review_date.strftime("%Y-%m-%d") if review_date is not None else ""
            base_task["Review Status"] = self._determine_review_status(review_date)

            identifier_values = self._extract_identifier_values(row_dict.get("Identifier", ""))
            matched_websites = []
            for identifier in identifier_values:
                matched_websites.extend(website_lookup.get(identifier, []))

            unique_matches = []
            seen_match_ids = set()
            for match in matched_websites:
                website_id = self._normalize_key(match.get("ID", ""))
                if website_id and website_id not in seen_match_ids:
                    seen_match_ids.add(website_id)
                    unique_matches.append(match)

            if not unique_matches:
                task_row = base_task.copy()
                for website_column in prefixed_website_columns:
                    task_row[website_column] = ""
                task_row["Effective Harvest Workflow"] = self._clean_value(
                    row_dict.get("Harvest Workflow", "")
                )
                task_row["Website Match Count"] = "0"
                task_rows.append(task_row)
                continue

            for website_match in unique_matches:
                task_row = base_task.copy()
                for website_column in website_columns:
                    task_row[f"Website {website_column}"] = self._clean_value(
                        website_match.get(website_column, "")
                    )
                task_row["Effective Harvest Workflow"] = self._first_non_empty(
                    row_dict.get("Harvest Workflow", ""),
                    website_match.get("Harvest Workflow", ""),
                )
                task_row["Website Match Count"] = str(len(unique_matches))
                task_rows.append(task_row)

        task_df = pd.DataFrame(task_rows)

        for column in prefixed_website_columns:
            if column not in task_df.columns:
                task_df[column] = ""

        if task_df.empty:
            return task_df

        task_df = self._consolidate_workflows(task_df, websites_df)

        task_df["Effective Harvest Workflow"] = task_df["Effective Harvest Workflow"].map(
            lambda value: self._clean_value(value) or "unspecified"
        )
        task_df["__due_sort"] = pd.to_datetime(task_df["Due Date"], errors="coerce")
        task_df["__display_name"] = task_df.apply(self._build_display_name, axis=1)

        task_df = task_df.sort_values(
            by=["__due_sort", "Effective Harvest Workflow", "__display_name"],
            ascending=[True, True, True],
            na_position="last",
        ).reset_index(drop=True)

        return task_df.drop(columns=["__due_sort", "__display_name"])

    def _consolidate_workflows(self, task_df: pd.DataFrame, websites_df: pd.DataFrame) -> pd.DataFrame:
        consolidated_rows: list[dict[str, Any]] = []
        remaining_frames: list[pd.DataFrame] = []

        for workflow, group in task_df.groupby("Effective Harvest Workflow", dropna=False):
            workflow_name = self._clean_value(workflow)
            if workflow_name not in CONSOLIDATED_WORKFLOW_TITLES:
                remaining_frames.append(group)
                continue

            website_count = self._count_grouped_websites(group, websites_df, workflow_name)

            consolidated_row = group.iloc[0].to_dict()
            consolidated_row["Title"] = CONSOLIDATED_WORKFLOW_TITLES[workflow_name]
            consolidated_row["ID"] = workflow_name
            consolidated_row["Identifier"] = ""
            consolidated_row["Website Match Count"] = str(website_count)
            consolidated_row["Website Name"] = self._format_website_count_label(website_count)
            consolidated_row["Website Title"] = self._format_website_count_label(website_count)
            consolidated_row["Website ID"] = ""
            consolidated_row["Effective Harvest Workflow"] = workflow_name
            consolidated_row["Due Date"] = self._select_due_date(group)
            consolidated_row["Due Status"] = self._select_due_status(group)
            consolidated_row["Last Harvested"] = self._format_date_range(group["Last Harvested"].tolist())
            consolidated_row["Accrual Periodicity"] = self._format_unique_values(
                group["Accrual Periodicity"].tolist()
            )
            consolidated_rows.append(consolidated_row)

        frames = remaining_frames.copy()
        if consolidated_rows:
            frames.append(pd.DataFrame(consolidated_rows))
        if not frames:
            return pd.DataFrame(columns=task_df.columns)
        return pd.concat(frames, ignore_index=True, sort=False)

    def _count_grouped_websites(
        self,
        group: pd.DataFrame,
        websites_df: pd.DataFrame,
        workflow_name: str,
    ) -> int:
        referenced_ids = {
            self._normalize_key(value)
            for value in group.get("Website ID", pd.Series(dtype=str)).tolist()
            if self._normalize_key(value)
        }
        if not referenced_ids:
            for identifier_value in group.get("Identifier", pd.Series(dtype=str)).tolist():
                referenced_ids.update(self._extract_identifier_values(identifier_value))

        if referenced_ids:
            website_ids = websites_df["ID"].map(self._normalize_key)
            return int(website_ids.isin(referenced_ids).sum())

        workflow_matches = websites_df["Harvest Workflow"].map(self._clean_value) == workflow_name
        return int(websites_df.loc[workflow_matches, "ID"].map(self._normalize_key).nunique())

    def _write_workflow_inputs(self, websites_df: pd.DataFrame) -> dict[str, str]:
        self._ensure_columns(websites_df, ["Harvest Workflow"])
        output_dir = self._dated_directory(self.output_workflow_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        workflow_outputs: dict[str, str] = {}
        workflow_series = websites_df["Harvest Workflow"].map(
            lambda value: self._clean_value(value) or "unspecified"
        )

        for workflow_name, workflow_group in websites_df.assign(
            **{"Harvest Workflow": workflow_series}
        ).groupby("Harvest Workflow", dropna=False):
            workflow_slug = self._slugify(workflow_name or "unspecified")
            output_path = output_dir / f"{workflow_slug}.csv"
            workflow_group.drop(columns=["__normalized_id"], errors="ignore").to_csv(
                output_path,
                index=False,
                encoding="utf-8",
            )
            workflow_outputs[workflow_name] = str(output_path)

        return dict(sorted(workflow_outputs.items()))

    def _write_dataframe(self, df: pd.DataFrame, configured_path: Path) -> Path:
        output_path = self._dated_output_path(configured_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding="utf-8")
        return output_path

    def _write_text(self, content: str, configured_path: Path) -> Path:
        output_path = self._dated_output_path(configured_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
        return output_path

    def _write_dedicated_workflow_dashboards(self, harvest_df: pd.DataFrame) -> dict[str, str]:
        dedicated_outputs: dict[str, str] = {}
        for workflow_name in self.dedicated_workflow_views:
            configured_path = self._dedicated_workflow_output_path(workflow_name)
            workflow_html = self._render_combined_workflow_html(
                self._filter_harvest_view(harvest_df, workflow_name),
                workflow=workflow_name,
            )
            output_path = self._write_text(workflow_html, configured_path)
            dedicated_outputs[workflow_name] = str(output_path)
        return dict(sorted(dedicated_outputs.items()))

    def _build_summary(self, task_df: pd.DataFrame) -> dict[str, int]:
        if "Due Status" in task_df.columns:
            due_status = task_df["Due Status"]
        else:
            due_status = pd.Series(dtype=str)

        return {
            "total": int(len(task_df)),
            "reviews": int((task_df.get("Review Status", pd.Series(dtype=str)) == "Due").sum()),
            "due": int((due_status == "Due").sum()),
            "scheduled": int((due_status == "Scheduled").sum()),
            "no_schedule": int((due_status == "No Schedule").sum()),
        }

    def _render_combined_workflow_html(
        self,
        harvest_df: pd.DataFrame,
        workflow: str,
        embedded: bool = False,
    ) -> str:
        workflow_name = self._clean_value(workflow) or "unspecified"
        workflow_label = self._workflow_view_label(workflow_name)
        last_run = self._latest_harvest_date(harvest_df)
        current_records = self._prepare_workflow_record_view(harvest_df)

        html_parts = [
            "<!DOCTYPE html>",
            "<html lang=\"en\">",
            "<head>",
            "  <meta charset=\"UTF-8\">",
            f"  <title>{escape(workflow_label)} Harvest Overview</title>",
            "  <style>",
            "    :root {",
            "      color-scheme: light;",
            "      --ink: #17324d;",
            "      --muted: #5b6b7d;",
            "      --line: #d7e1ec;",
            "      --line-strong: #bfd0e2;",
            "      --panel: #ffffff;",
            "      --panel-soft: #f6f9fc;",
            "      --bg: #eef3f8;",
            "      --accent: #1f6fb2;",
            "      --accent-soft: #dcecff;",
            "      --success: #0f766e;",
            "      --success-soft: #d8f3ee;",
            "    }",
            "    * { box-sizing: border-box; }",
            "    body { margin: 1.5rem auto; max-width: 1180px; padding: 0 1rem 2.5rem; font-family: \"Segoe UI\", sans-serif; line-height: 1.35; color: var(--ink); background: linear-gradient(180deg, #f8fbfd 0%, var(--bg) 100%); }",
            "    h1, h2, p { margin-top: 0; }",
            "    h1 { margin-bottom: 0.4rem; }",
            "    h2 { margin-bottom: 0.9rem; }",
            "    a { color: var(--accent); }",
            "    .muted { color: var(--muted); }",
            "    code { background: var(--panel-soft); padding: 0.08rem 0.32rem; border-radius: 6px; }",
            "    .source-box, .status-box, .section-box { background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 1rem; box-shadow: 0 12px 28px rgba(23, 50, 77, 0.05); }",
            "    .source-box { margin: 1rem 0 1.4rem; }",
            "    .source-box ul { margin: 0.5rem 0 0; padding-left: 1.15rem; }",
            "    .status-box { margin: 1.25rem 0 1.4rem; background: linear-gradient(135deg, var(--panel) 0%, var(--success-soft) 100%); border-color: #b7e4d7; }",
            "    .status-label { color: var(--muted); font-size: 0.82rem; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 700; }",
            "    .status-value { display: block; margin-top: 0.35rem; font-size: 1.65rem; font-weight: 700; color: var(--success); }",
            "    .section-box { padding: 0; overflow: hidden; }",
            "    .section-header { display: flex; justify-content: space-between; align-items: center; gap: 0.75rem; padding: 0.9rem 1rem; background: var(--panel-soft); border-bottom: 1px solid var(--line); }",
            "    .section-meta { color: var(--muted); font-size: 0.82rem; }",
            "    table { width: 100%; border-collapse: collapse; font-size: 0.92rem; }",
            "    th, td { padding: 0.6rem 0.75rem; text-align: left; vertical-align: top; border-bottom: 1px solid var(--line); }",
            "    th { background: #f9fbfd; color: var(--muted); font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.04em; }",
            "    tbody tr:last-child td { border-bottom: none; }",
            "    tbody tr:nth-child(even) { background: #fbfdff; }",
            "    .task-name { font-weight: 700; margin-bottom: 0.2rem; }",
            "    .task-meta, .detail-meta { color: var(--muted); font-size: 0.82rem; margin-top: 0.18rem; }",
            "    .task-meta code, .detail-meta code { background: #edf1f5; padding: 0.08rem 0.32rem; border-radius: 6px; }",
            "    .endpoint-link { display: inline-flex; align-items: center; gap: 0.35rem; }",
            "    @media (max-width: 840px) { body { padding: 0 0.7rem 2rem; } .section-header { align-items: flex-start; flex-direction: column; } table, thead, tbody, th, td, tr { display: block; } thead { display: none; } tbody tr { padding: 0.45rem 0.7rem; } td { border-bottom: none; padding: 0.25rem 0; } td::before { content: attr(data-label); display: block; color: var(--muted); font-size: 0.74rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 0.1rem; } }",
            "  </style>",
            "</head>",
            "<body>",
        ]

        if not embedded:
            html_parts.extend(
                [
                    f"  <h1>{escape(workflow_label)} Harvest Overview</h1>",
                    f"  <p class=\"muted\">Generated from <code>{escape(str(self.harvest_records_path))}</code>. This view combines the current workflow status with the active ArcGIS Hub harvest record list.</p>",
                    "  <div class=\"source-box\">",
                    "    <h2>Get Latest Source CSVs</h2>",
                    "    <p>Download the newest files before running the dashboard, then save them into <code>inputs/</code> with these names:</p>",
                    "    <ul>",
                    "      <li><a href=\"https://geo.btaa.org/admin/documents?f%5Bgbl_resourceClass_sm%5D%5B%5D=Series&rows=20&sort=score+desc\" target=\"_blank\" rel=\"noreferrer\"><code>harvest-records.csv</code></a> -> save as <code>inputs/harvest-records.csv</code></li>",
                    "      <li><a href=\"https://geo.btaa.org/admin/documents?f%5Bb1g_publication_state_s%5D%5B%5D=published&f%5Bgbl_resourceClass_sm%5D%5B%5D=Websites&rows=20&sort=score+desc\" target=\"_blank\" rel=\"noreferrer\"><code>websites.csv</code></a> -> save as <code>inputs/websites.csv</code></li>",
                    "    </ul>",
                    "  </div>",
                ]
            )

        html_parts.extend(
            [
                "  <div class=\"status-box\">",
                "    <span class=\"status-label\">Last time the process was run</span>",
                f"    <strong class=\"status-value\">{escape(last_run or 'Unknown')}</strong>",
                "  </div>",
                "  <section class=\"section-box\">",
                "    <div class=\"section-header\">",
                "      <h2>Currently Harvested ArcGIS Hubs</h2>",
                f"      <div class=\"section-meta\">{len(current_records)} record{'s' if len(current_records) != 1 else ''}</div>",
                "    </div>",
            ]
        )

        if current_records.empty:
            html_parts.extend(
                [
                    "    <div style=\"padding: 1rem;\">No ArcGIS Hub harvest records were found in the input file.</div>",
                    "  </section>",
                    "</body>",
                    "</html>",
                ]
            )
            return "\n".join(html_parts)

        html_parts.extend(
            [
                "    <table>",
                "      <thead>",
                "        <tr>",
                "          <th>Harvest Record</th>",
                "          <th>Last Harvested</th>",
                "          <th>Endpoint</th>",
                "        </tr>",
                "      </thead>",
                "      <tbody>",
            ]
        )
        for _, row in current_records.iterrows():
            html_parts.extend(
                [
                    "        <tr>",
                    f"          <td data-label=\"Harvest Record\">{self._render_task_cell(row)}</td>",
                    f"          <td data-label=\"Last Harvested\">{self._render_workflow_last_harvested_cell(row)}</td>",
                    f"          <td data-label=\"Endpoint\">{self._render_endpoint_cell(row)}</td>",
                    "        </tr>",
                ]
            )
        html_parts.extend(
            [
                "      </tbody>",
                "    </table>",
                "  </section>",
                "</body>",
                "</html>",
            ]
        )
        return "\n".join(html_parts)

    def _filter_task_view(self, task_df: pd.DataFrame, workflow: str = "") -> pd.DataFrame:
        if task_df.empty:
            return task_df.copy()

        workflow_name = self._clean_value(workflow)
        view_df = task_df.copy()
        self._ensure_columns(view_df, ["Effective Harvest Workflow"])
        normalized_workflow = view_df["Effective Harvest Workflow"].map(self._clean_value)
        if workflow_name:
            return view_df.loc[normalized_workflow == workflow_name].copy()

        if not self.dedicated_workflow_views:
            return view_df

        dedicated_workflows = set(self.dedicated_workflow_views)
        return view_df.loc[~normalized_workflow.isin(dedicated_workflows)].copy()

    def _filter_harvest_view(self, harvest_df: pd.DataFrame, workflow: str = "") -> pd.DataFrame:
        if harvest_df.empty:
            return harvest_df.copy()

        workflow_name = self._clean_value(workflow)
        view_df = harvest_df.copy()
        self._ensure_columns(view_df, ["Harvest Workflow"])
        normalized_workflow = view_df["Harvest Workflow"].map(self._clean_value)
        if workflow_name:
            return view_df.loc[normalized_workflow == workflow_name].copy()

        if not self.dedicated_workflow_views:
            return view_df

        dedicated_workflows = set(self.dedicated_workflow_views)
        return view_df.loc[~normalized_workflow.isin(dedicated_workflows)].copy()

    def _build_retrospective_dataframe(self, harvest_df: pd.DataFrame) -> pd.DataFrame:
        retrospective_columns = [
            "Action Month",
            "Action Date",
            "Type",
            "Title",
            "ID",
            "Identifier",
            "Harvest Workflow",
            "Details",
        ]

        if harvest_df.empty:
            return pd.DataFrame(columns=retrospective_columns)

        working_df = harvest_df.copy()
        self._ensure_columns(
            working_df,
            ["ID", "Identifier", "Title", "Harvest Workflow", "Last Harvested", "Provenance"],
        )

        action_rows: list[dict[str, str]] = []
        for _, harvest_row in working_df.iterrows():
            row_dict = harvest_row.to_dict()
            display_name = self._build_display_name(row_dict)
            workflow_name = self._clean_value(row_dict.get("Harvest Workflow", "")) or "unspecified"
            provenance_entries = self._extract_provenance_entries(row_dict.get("Provenance", ""))

            last_harvested = self._clean_value(row_dict.get("Last Harvested", ""))
            last_harvested_date = pd.to_datetime(last_harvested, errors="coerce")
            if not pd.isna(last_harvested_date) and not self._has_provenance_entry_for_month(
                provenance_entries, last_harvested_date
            ):
                action_rows.append(
                    {
                        "Action Month": last_harvested_date.strftime("%B %Y"),
                        "Action Date": last_harvested_date.strftime("%Y-%m-%d"),
                        "Type": self._select_month_provenance_action_type(
                            provenance_entries, last_harvested_date
                        )
                        or "Harvested",
                        "Title": display_name,
                        "ID": self._clean_value(row_dict.get("ID", "")),
                        "Identifier": self._clean_value(row_dict.get("Identifier", "")),
                        "Harvest Workflow": workflow_name,
                        "Details": self._select_month_provenance_note(
                            provenance_entries, last_harvested_date
                        ),
                    }
                )

            for provenance_entry in provenance_entries:
                provenance_date = self._extract_dated_entry_date(provenance_entry)
                if provenance_date is None:
                    continue
                action_rows.append(
                    {
                        "Action Month": provenance_date.strftime("%B %Y"),
                        "Action Date": provenance_date.strftime("%Y-%m-%d"),
                        "Type": self._extract_provenance_action_type(provenance_entry) or "Reviewed",
                        "Title": display_name,
                        "ID": self._clean_value(row_dict.get("ID", "")),
                        "Identifier": self._clean_value(row_dict.get("Identifier", "")),
                        "Harvest Workflow": workflow_name,
                        "Details": self._extract_provenance_details(provenance_entry),
                    }
                )

        if not action_rows:
            return pd.DataFrame(columns=retrospective_columns)

        retrospective_df = pd.DataFrame(action_rows)
        retrospective_df["__action_sort"] = pd.to_datetime(
            retrospective_df["Action Date"], errors="coerce"
        )
        retrospective_df["__month_sort"] = retrospective_df["__action_sort"].dt.to_period("M").dt.to_timestamp()
        retrospective_df = retrospective_df.sort_values(
            by=["__month_sort", "__action_sort", "Type", "Title"],
            ascending=[False, False, True, True],
            na_position="last",
        ).reset_index(drop=True)
        return retrospective_df

    def _build_retrospective_summary(self, retrospective_df: pd.DataFrame) -> dict[str, int]:
        if retrospective_df.empty:
            return {"total": 0, "harvested": 0, "reviewed": 0, "months": 0}

        action_types = retrospective_df.get("Type", pd.Series(dtype=str))
        month_values = retrospective_df.get("Action Month", pd.Series(dtype=str))
        return {
            "total": int(len(retrospective_df)),
            "harvested": int((action_types == "Harvested").sum()),
            "reviewed": int((action_types == "Reviewed").sum()),
            "months": int(month_values[month_values.astype(str).str.len() > 0].nunique()),
        }

    def _render_retrospective_html(
        self,
        harvest_df: pd.DataFrame,
        embedded: bool = False,
        report_title: str = "Harvest Task Retrospective",
    ) -> str:
        retrospective_df = self._build_retrospective_dataframe(harvest_df)
        summary = self._build_retrospective_summary(retrospective_df)

        html_parts = [
            "<!DOCTYPE html>",
            "<html lang=\"en\">",
            "<head>",
            "  <meta charset=\"UTF-8\">",
            f"  <title>{escape(report_title)}</title>",
            "  <style>",
            "    :root {",
            "      color-scheme: light;",
            "      --ink: #17324d;",
            "      --muted: #5b6b7d;",
            "      --line: #d7e1ec;",
            "      --panel: #ffffff;",
            "      --panel-soft: #f6f9fc;",
            "      --bg: #eef3f8;",
            "      --accent: #1f6fb2;",
            "      --accent-soft: #dcecff;",
            "      --harvested: #b42318;",
            "      --harvested-soft: #fee4e2;",
            "      --reviewed: #0f766e;",
            "      --reviewed-soft: #d8f3ee;",
            "    }",
            "    * { box-sizing: border-box; }",
            "    body { margin: 1.5rem auto; max-width: 1180px; padding: 0 1rem 2.5rem; font-family: \"Segoe UI\", sans-serif; line-height: 1.35; color: var(--ink); background: linear-gradient(180deg, #f8fbfd 0%, var(--bg) 100%); }",
            "    h1, h2, h3, p { margin-top: 0; }",
            "    h1 { margin-bottom: 0.4rem; }",
            "    h2 { margin-bottom: 0.9rem; }",
            "    a { color: var(--accent); }",
            "    .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 0.75rem; margin: 1.25rem 0 1.5rem; }",
            "    .card { background: var(--panel); border: 1px solid var(--line); border-top: 4px solid var(--accent); border-radius: 14px; padding: 0.8rem 0.9rem; box-shadow: 0 12px 30px rgba(23, 50, 77, 0.06); }",
            "    .card strong { display: block; margin-top: 0.2rem; font-size: 1.5rem; }",
            "    .card.card--harvested { border-top-color: var(--harvested); }",
            "    .card.card--reviewed { border-top-color: var(--reviewed); }",
            "    .month-section { margin-top: 1.6rem; }",
            "    .month-block { margin: 0.9rem 0 1.15rem; background: var(--panel); border: 1px solid var(--line); border-radius: 16px; overflow: hidden; box-shadow: 0 12px 28px rgba(23, 50, 77, 0.05); }",
            "    .month-header { display: flex; justify-content: space-between; align-items: center; gap: 0.75rem; padding: 0.75rem 0.95rem; background: var(--panel-soft); border-bottom: 1px solid var(--line); }",
            "    .month-meta { color: var(--muted); font-size: 0.82rem; }",
            "    table { width: 100%; border-collapse: collapse; font-size: 0.92rem; }",
            "    th, td { padding: 0.55rem 0.7rem; text-align: left; vertical-align: top; border-bottom: 1px solid var(--line); }",
            "    th { background: #f9fbfd; color: var(--muted); font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.04em; }",
            "    tbody tr:last-child td { border-bottom: none; }",
            "    tbody tr:nth-child(even) { background: #fbfdff; }",
            "    .task-name { font-weight: 700; margin-bottom: 0.2rem; }",
            "    .task-meta, .detail-meta { color: var(--muted); font-size: 0.82rem; margin-top: 0.18rem; }",
            "    .task-meta code, .muted code { background: #edf1f5; padding: 0.08rem 0.32rem; border-radius: 6px; }",
            "    .date-line { display: flex; align-items: center; flex-wrap: wrap; gap: 0.45rem; font-weight: 700; }",
            "    .status-pill { display: inline-flex; align-items: center; padding: 0.18rem 0.55rem; border-radius: 999px; font-size: 0.74rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; }",
            "    .status-pill--harvested { color: var(--harvested); background: var(--harvested-soft); }",
            "    .status-pill--reviewed { color: var(--reviewed); background: var(--reviewed-soft); }",
            "    .muted { color: var(--muted); }",
            "    code { background: var(--panel-soft); padding: 0.08rem 0.32rem; border-radius: 6px; }",
            "    .source-box { background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 1rem; margin: 1rem 0 1.4rem; box-shadow: 0 12px 28px rgba(23, 50, 77, 0.05); }",
            "    .source-box ul { margin: 0.5rem 0 0; padding-left: 1.15rem; }",
            "    @media (max-width: 840px) { body { padding: 0 0.7rem 2rem; } .month-header { align-items: flex-start; flex-direction: column; } table, thead, tbody, th, td, tr { display: block; } thead { display: none; } tbody tr { padding: 0.45rem 0.7rem; } td { border-bottom: none; padding: 0.25rem 0; } td::before { content: attr(data-label); display: block; color: var(--muted); font-size: 0.74rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 0.1rem; } }",
            "  </style>",
            "</head>",
            "<body>",
        ]

        if not embedded:
            html_parts.extend(
                [
                    f"  <h1>{escape(report_title)}</h1>",
                    f"  <p class=\"muted\">Generated from <code>{escape(str(self.harvest_records_path))}</code>. Each <code>Last Harvested</code> date is shown as <strong>Harvested</strong>; each dated <code>Provenance</code> entry is shown as <strong>Reviewed</strong>.</p>",
                    "  <div class=\"source-box\">",
                    "    <h2>Get Latest Source CSVs</h2>",
                    "    <p>Download the newest files before running the dashboard, then save them into <code>inputs/</code> with these names:</p>",
                    "    <ul>",
                    "      <li><a href=\"https://geo.btaa.org/admin/documents?f%5Bgbl_resourceClass_sm%5D%5B%5D=Series&rows=20&sort=score+desc\" target=\"_blank\" rel=\"noreferrer\"><code>harvest-records.csv</code></a> -> save as <code>inputs/harvest-records.csv</code></li>",
                    "      <li><a href=\"https://geo.btaa.org/admin/documents?f%5Bb1g_publication_state_s%5D%5B%5D=published&f%5Bgbl_resourceClass_sm%5D%5B%5D=Websites&rows=20&sort=score+desc\" target=\"_blank\" rel=\"noreferrer\"><code>websites.csv</code></a> -> save as <code>inputs/websites.csv</code></li>",
                    "    </ul>",
                    "  </div>",
                ]
            )

        html_parts.extend(
            [
                "  <div class=\"summary\">",
                "    <div class=\"card\">",
                "      <span>Total Actions</span>",
                f"      <strong>{summary['total']}</strong>",
                "    </div>",
                "    <div class=\"card card--harvested\">",
                "      <span>Harvested</span>",
                f"      <strong>{summary['harvested']}</strong>",
                "    </div>",
                "    <div class=\"card card--reviewed\">",
                "      <span>Reviewed</span>",
                f"      <strong>{summary['reviewed']}</strong>",
                "    </div>",
                "    <div class=\"card\">",
                "      <span>Months</span>",
                f"      <strong>{summary['months']}</strong>",
                "    </div>",
                "  </div>",
            ]
        )

        if retrospective_df.empty:
            html_parts.extend(
                [
                    "  <p>No retrospective actions were found in the input file.</p>",
                    "</body>",
                    "</html>",
                ]
            )
            return "\n".join(html_parts)

        for month_label, month_group in retrospective_df.groupby("Action Month", sort=False):
            html_parts.extend(
                [
                    "  <section class=\"month-section\">",
                    "    <div class=\"month-block\">",
                    "      <div class=\"month-header\">",
                    f"        <h2>{escape(month_label)}</h2>",
                    f"        <div class=\"month-meta\">{len(month_group)} action{'s' if len(month_group) != 1 else ''}</div>",
                    "      </div>",
                    "      <table>",
                    "        <thead>",
                    "          <tr>",
                    "            <th>Record</th>",
                    "            <th>Action</th>",
                    "            <th>Workflow</th>",
                    "            <th>Details</th>",
                    "          </tr>",
                    "        </thead>",
                    "        <tbody>",
                ]
            )
            for _, row in month_group.iterrows():
                html_parts.extend(
                    [
                        "          <tr>",
                        f"            <td data-label=\"Record\">{self._render_task_cell(row)}</td>",
                        f"            <td data-label=\"Action\">{self._render_retrospective_action_cell(row)}</td>",
                        f"            <td data-label=\"Workflow\"><code>{escape(self._clean_value(row.get('Harvest Workflow', '')) or 'unspecified')}</code></td>",
                        f"            <td data-label=\"Details\">{self._render_retrospective_details_cell(row)}</td>",
                        "          </tr>",
                    ]
                )
            html_parts.extend(
                [
                    "        </tbody>",
                    "      </table>",
                    "    </div>",
                    "  </section>",
                ]
            )

        html_parts.extend(["</body>", "</html>"])
        return "\n".join(html_parts)

    def _render_dashboard_html(
        self,
        task_df: pd.DataFrame,
        embedded: bool = False,
        report_title: str = "Harvest Task Dashboard",
    ) -> str:
        summary = self._build_summary(task_df)
        sections = self._build_dashboard_sections(task_df)

        html_parts = [
            "<!DOCTYPE html>",
            "<html lang=\"en\">",
            "<head>",
            "  <meta charset=\"UTF-8\">",
            f"  <title>{escape(report_title)}</title>",
            "  <style>",
            "    :root {",
            "      color-scheme: light;",
            "      --ink: #17324d;",
            "      --muted: #5b6b7d;",
            "      --line: #d7e1ec;",
            "      --line-strong: #bfd0e2;",
            "      --panel: #ffffff;",
            "      --panel-soft: #f6f9fc;",
            "      --bg: #eef3f8;",
            "      --accent: #1f6fb2;",
            "      --accent-soft: #dcecff;",
            "      --reviews: #0f766e;",
            "      --reviews-soft: #d8f3ee;",
            "      --due: #b42318;",
            "      --due-soft: #fee4e2;",
            "      --scheduled: #b76e00;",
            "      --scheduled-soft: #fff1d6;",
            "      --no-schedule: #667085;",
            "      --no-schedule-soft: #edf1f5;",
            "    }",
            "    * { box-sizing: border-box; }",
            "    body { margin: 1.5rem auto; max-width: 1180px; padding: 0 1rem 2.5rem; font-family: \"Segoe UI\", sans-serif; line-height: 1.35; color: var(--ink); background: linear-gradient(180deg, #f8fbfd 0%, var(--bg) 100%); }",
            "    h1, h2, h3, p { margin-top: 0; }",
            "    h1 { margin-bottom: 0.4rem; }",
            "    h2 { margin-bottom: 0.9rem; }",
            "    h3 { margin-bottom: 0; font-size: 1rem; }",
            "    a { color: var(--accent); }",
            "    .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 0.75rem; margin: 1.25rem 0 1.5rem; }",
            "    .card { background: var(--panel); border: 1px solid var(--line); border-top: 4px solid var(--accent); border-radius: 14px; padding: 0.8rem 0.9rem; box-shadow: 0 12px 30px rgba(23, 50, 77, 0.06); }",
            "    .card strong { display: block; margin-top: 0.2rem; font-size: 1.5rem; }",
            "    .card.card--reviews { border-top-color: var(--reviews); }",
            "    .card.card--due { border-top-color: var(--due); }",
            "    .card.card--scheduled { border-top-color: var(--scheduled); }",
            "    .card.card--no-schedule { border-top-color: var(--no-schedule); }",
            "    .due-section { margin-top: 1.6rem; }",
            "    .section-header { display: flex; align-items: center; gap: 0.7rem; margin-bottom: 0.8rem; }",
            "    .section-pill { display: inline-flex; align-items: center; padding: 0.25rem 0.7rem; border-radius: 999px; font-size: 0.8rem; font-weight: 700; letter-spacing: 0.02em; }",
            "    .section-pill--reviews { color: var(--reviews); background: var(--reviews-soft); }",
            "    .section-pill--due { color: var(--due); background: var(--due-soft); }",
            "    .section-pill--scheduled { color: var(--scheduled); background: var(--scheduled-soft); }",
            "    .section-pill--no-schedule { color: var(--no-schedule); background: var(--no-schedule-soft); }",
            "    .workflow-block { margin: 0.9rem 0 1.15rem; background: var(--panel); border: 1px solid var(--line); border-radius: 16px; overflow: hidden; box-shadow: 0 12px 28px rgba(23, 50, 77, 0.05); }",
            "    .workflow-header { display: flex; justify-content: space-between; align-items: center; gap: 0.75rem; padding: 0.75rem 0.95rem; background: var(--panel-soft); border-bottom: 1px solid var(--line); }",
            "    .workflow-meta { color: var(--muted); font-size: 0.82rem; }",
            "    table { width: 100%; border-collapse: collapse; font-size: 0.92rem; }",
            "    th, td { padding: 0.55rem 0.7rem; text-align: left; vertical-align: top; border-bottom: 1px solid var(--line); }",
            "    th { background: #f9fbfd; color: var(--muted); font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.04em; }",
            "    tbody tr:last-child td { border-bottom: none; }",
            "    tbody tr:nth-child(even) { background: #fbfdff; }",
            "    .task-name { font-weight: 700; margin-bottom: 0.2rem; }",
            "    .task-meta, .timing-meta { color: var(--muted); font-size: 0.82rem; margin-top: 0.18rem; }",
            "    .task-meta code, .timing-meta code, .muted code { background: var(--no-schedule-soft); padding: 0.08rem 0.32rem; border-radius: 6px; }",
            "    .date-line { display: flex; align-items: center; flex-wrap: wrap; gap: 0.45rem; font-weight: 700; }",
            "    .status-pill { display: inline-flex; align-items: center; padding: 0.18rem 0.55rem; border-radius: 999px; font-size: 0.74rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; }",
            "    .status-pill--reviews { color: var(--reviews); background: var(--reviews-soft); }",
            "    .status-pill--due { color: var(--due); background: var(--due-soft); }",
            "    .status-pill--scheduled { color: var(--scheduled); background: var(--scheduled-soft); }",
            "    .status-pill--no-schedule { color: var(--no-schedule); background: var(--no-schedule-soft); }",
            "    .website-name { font-weight: 600; }",
            "    .muted { color: var(--muted); }",
            "    code { background: var(--panel-soft); padding: 0.08rem 0.32rem; border-radius: 6px; }",
            "    .actions { min-width: 170px; }",
            "    .action-link { display: inline-flex; align-items: center; margin: 0.1rem 0.3rem 0.1rem 0; padding: 0.34rem 0.62rem; border: 1px solid var(--line-strong); border-radius: 999px; text-decoration: none; color: var(--accent); background: #fff; font-size: 0.84rem; font-weight: 600; }",
            "    .action-link:hover { background: var(--accent-soft); border-color: var(--accent-soft); }",
            "    .source-box { background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 1rem; margin: 1rem 0 1.4rem; box-shadow: 0 12px 28px rgba(23, 50, 77, 0.05); }",
            "    .source-box ul { margin: 0.5rem 0 0; padding-left: 1.15rem; }",
            "    @media (max-width: 840px) { body { padding: 0 0.7rem 2rem; } .workflow-header { align-items: flex-start; flex-direction: column; } table, thead, tbody, th, td, tr { display: block; } thead { display: none; } tbody tr { padding: 0.45rem 0.7rem; } td { border-bottom: none; padding: 0.25rem 0; } td::before { content: attr(data-label); display: block; color: var(--muted); font-size: 0.74rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 0.1rem; } .actions { min-width: 0; padding-top: 0.25rem; } }",
            "  </style>",
            "</head>",
            "<body>",
        ]

        if embedded:
            html_parts.append("  <div class=\"summary\">")
        else:
            html_parts.extend(
                [
                    f"  <h1>{escape(report_title)}</h1>",
                    f"  <p class=\"muted\">Generated from <code>{escape(str(self.harvest_records_path))}</code> and <code>{escape(str(self.websites_path))}</code>.</p>",
                    "  <div class=\"source-box\">",
                    "    <h2>Get Latest Source CSVs</h2>",
                    "    <p>Download the newest files before running the dashboard, then save them into <code>inputs/</code> with these names:</p>",
                    "    <ul>",
                    "      <li><a href=\"https://geo.btaa.org/admin/documents?f%5Bgbl_resourceClass_sm%5D%5B%5D=Series&rows=20&sort=score+desc\" target=\"_blank\" rel=\"noreferrer\"><code>harvest-records.csv</code></a> -> save as <code>inputs/harvest-records.csv</code></li>",
                    "      <li><a href=\"https://geo.btaa.org/admin/documents?f%5Bb1g_publication_state_s%5D%5B%5D=published&f%5Bgbl_resourceClass_sm%5D%5B%5D=Websites&rows=20&sort=score+desc\" target=\"_blank\" rel=\"noreferrer\"><code>websites.csv</code></a> -> save as <code>inputs/websites.csv</code></li>",
                    "    </ul>",
                    "  </div>",
                    "  <p class=\"muted\">Use the issue buttons to open a prefilled GitHub issue from the harvest-task template.</p>",
                    "  <div class=\"summary\">",
                ]
            )

        summary_cards = [
            ("Total Tasks", summary["total"], ""),
            ("Reviews due", summary["reviews"], "card--reviews"),
            ("Harvests due", summary["due"], "card--due"),
        ]
        if report_title != "Harvest Tasks Due Now":
            summary_cards.extend(
                [
                    ("Scheduled", summary["scheduled"], "card--scheduled"),
                    ("No Schedule", summary["no_schedule"], "card--no-schedule"),
                ]
            )
        for label, value, class_name in summary_cards:
            card_class = f"card {class_name}".strip()
            html_parts.extend(
                [
                    f"    <div class=\"{card_class}\">",
                    f"      <span>{escape(label)}</span>",
                    f"      <strong>{value}</strong>",
                    "    </div>",
                ]
            )

        html_parts.append("  </div>")

        if not sections:
            html_parts.extend(
                [
                    "  <p>No harvest tasks were found in the input file.</p>",
                    "</body>",
                    "</html>",
                ]
            )
            return "\n".join(html_parts)

        for due_label, workflow_groups in sections:
            total_in_section = sum(len(group) for _, group in workflow_groups)
            section_class = self._section_class_name(due_label)
            html_parts.extend(
                [
                    f"  <section class=\"due-section {section_class}\">",
                    "    <div class=\"section-header\">",
                    f"      <h2>{escape(due_label)} ({total_in_section})</h2>",
                    "    </div>",
                ]
            )
            for workflow_name, workflow_group in workflow_groups:
                html_parts.extend(
                    [
                        "    <div class=\"workflow-block\">",
                        "      <div class=\"workflow-header\">",
                        f"        <h3>{escape(workflow_name)}</h3>",
                        f"        <div class=\"workflow-meta\">{len(workflow_group)} task{'s' if len(workflow_group) != 1 else ''}</div>",
                        "      </div>",
                        "      <table>",
                        "        <thead>",
                        "          <tr>",
                        "            <th>Task</th>",
                        "            <th>Timing</th>",
                        "            <th class=\"actions\">Actions</th>",
                        "          </tr>",
                        "        </thead>",
                        "        <tbody>",
                    ]
                )
                for _, row in workflow_group.iterrows():
                    html_parts.extend(
                        [
                            "          <tr>",
                            f"            <td data-label=\"Task\">{self._render_task_cell(row)}</td>",
                            f"            <td data-label=\"Timing\">{self._render_timing_cell(row, due_label)}</td>",
                            f"            <td class=\"actions\" data-label=\"Actions\">{self._render_issue_links(row)}</td>",
                            "          </tr>",
                        ]
                    )
                html_parts.extend(
                    [
                        "        </tbody>",
                        "      </table>",
                        "    </div>",
                    ]
                )
            html_parts.append("  </section>")

        html_parts.extend(["</body>", "</html>"])
        return "\n".join(html_parts)

    def _build_dashboard_sections(self, task_df: pd.DataFrame) -> list[tuple[str, list[tuple[str, pd.DataFrame]]]]:
        if task_df.empty:
            return []

        sections: list[tuple[str, list[tuple[str, pd.DataFrame]]]] = []
        working_df = task_df.copy()
        working_df["__due_sort"] = pd.to_datetime(working_df["Due Date"], errors="coerce")
        working_df["__review_sort"] = pd.to_datetime(working_df.get("Review Date", ""), errors="coerce")

        periodicity_values = working_df.get("Accrual Periodicity", pd.Series("", index=working_df.index))
        irregular_mask = periodicity_values.map(self._normalize_periodicity) == "irregular"

        harvest_group = working_df[~irregular_mask].copy()
        if not harvest_group.empty:
            harvest_group["__section_date_display"] = harvest_group["Due Date"]
            harvest_group = harvest_group.sort_values(
                by=["__due_sort", "Effective Harvest Workflow", "Due Date", "Title"],
                ascending=[True, True, True, True],
                na_position="last",
            )
            sections.append(("To be harvested", self._group_section_by_workflow(harvest_group)))

        review_group = working_df[irregular_mask].copy()
        if not review_group.empty:
            review_group["__section_date_display"] = review_group["Review Date"]
            review_group = review_group.sort_values(
                by=["__review_sort", "Effective Harvest Workflow", "Review Date", "Title"],
                ascending=[True, True, True, True],
                na_position="last",
            )
            sections.append(("To be reviewed", self._group_section_by_workflow(review_group)))

        return sections

    def _filter_due_only_tasks(self, task_df: pd.DataFrame) -> pd.DataFrame:
        if task_df.empty:
            return task_df.copy()

        periodicity_values = task_df.get("Accrual Periodicity", pd.Series("", index=task_df.index))
        irregular_mask = periodicity_values.map(self._normalize_periodicity) == "irregular"
        review_due_mask = irregular_mask & (task_df.get("Review Status", pd.Series("", index=task_df.index)) == "Due")
        harvest_due_mask = (~irregular_mask) & (task_df.get("Due Status", pd.Series("", index=task_df.index)) == "Due")
        return task_df.loc[review_due_mask | harvest_due_mask].copy()

    def _group_section_by_workflow(self, section_df: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
        workflow_groups: list[tuple[str, pd.DataFrame]] = []
        for workflow_name, workflow_group in section_df.groupby("Effective Harvest Workflow", dropna=False):
            workflow_groups.append((workflow_name or "unspecified", workflow_group))

        workflow_groups.sort(key=lambda item: item[0])
        return workflow_groups

    def _section_class_name(self, label: str) -> str:
        section_classes = {
            "To be reviewed": "section-pill--reviews",
            "To be harvested": "section-pill--due",
        }
        return section_classes.get(label, "section-pill--no-schedule")

    def _render_retrospective_action_cell(self, row: pd.Series | dict[str, Any]) -> str:
        action_type = self._clean_value(row.get("Type", "")) or "Reviewed"
        action_date = self._clean_value(row.get("Action Date", "")) or "Unknown date"
        pill_class = self._retrospective_pill_class(action_type)
        return (
            f'<div class="date-line"><span class="status-pill {pill_class}">{escape(action_type)}</span>'
            f"<span>{escape(action_date)}</span></div>"
        )

    def _render_retrospective_details_cell(self, row: pd.Series | dict[str, Any]) -> str:
        details = self._clean_value(row.get("Details", "")) or "Not provided"
        if not details or details == "Not provided":
            return ""
        return f'<div class="detail-meta">{escape(details)}</div>'

    def _render_workflow_last_harvested_cell(self, row: pd.Series | dict[str, Any]) -> str:
        last_harvested = self._clean_value(row.get("Last Harvested", "")) or "Not yet harvested"
        updated_at = self._clean_value(row.get("Updated At", ""))
        detail_lines = [f"<div>{escape(last_harvested)}</div>"]
        if updated_at:
            detail_lines.append(f'<div class="detail-meta">Updated: {escape(updated_at)}</div>')
        return "".join(detail_lines)

    def _render_endpoint_cell(self, row: pd.Series | dict[str, Any]) -> str:
        endpoint_url = self._clean_value(row.get("Endpoint URL", ""))
        endpoint_description = self._clean_value(row.get("Endpoint Description", ""))
        website_platform = self._clean_value(row.get("Website Platform", ""))
        parts: list[str] = []
        if endpoint_url:
            parts.append(
                f'<a class="endpoint-link" href="{escape(endpoint_url, quote=True)}" target="_blank" rel="noreferrer">{escape(endpoint_url)}</a>'
            )
        else:
            parts.append("<span class=\"detail-meta\">Not provided</span>")

        metadata = self._format_unique_values([endpoint_description, website_platform])
        if metadata:
            parts.append(f'<div class="detail-meta">{escape(metadata)}</div>')
        return "".join(parts)

    def _retrospective_pill_class(self, action_type: str) -> str:
        pill_classes = {
            "Harvested": "status-pill--harvested",
            "Reviewed": "status-pill--reviewed",
            "harvest": "status-pill--harvested",
            "review": "status-pill--reviewed",
            "reviewed": "status-pill--reviewed",
        }
        return pill_classes.get(action_type, "status-pill--reviewed")

    def _render_task_cell(self, row: pd.Series | dict[str, Any]) -> str:
        display_name = escape(self._build_display_name(row))
        task_id = self._clean_value(row.get("ID", ""))
        identifier = self._clean_value(row.get("Identifier", ""))
        harvest_record_html = self._render_record_link(task_id, self._harvest_record_url(task_id))
        identifier_html = self._render_identifier_links(identifier)

        return (
            f'<div class="task-name">{display_name}</div>'
            f'<div class="task-meta">Harvest record: {harvest_record_html}</div>'
            f'<div class="task-meta">Identifier: {identifier_html}</div>'
        )

    def _render_timing_cell(self, row: pd.Series | dict[str, Any], due_label: str) -> str:
        section_date = self._clean_value(row.get("__section_date_display", row.get("Due Date", ""))) or "No schedule"
        last_harvested = self._clean_value(row.get("Last Harvested", "")) or "Not yet harvested"
        periodicity = self._clean_value(row.get("Accrual Periodicity", "")) or "Not provided"

        if due_label == "To be reviewed":
            review_status = self._clean_value(row.get("Review Status", ""))
            pill_label = f"Review {review_status}".strip()
            pill_class = self._status_pill_class(pill_label)
        else:
            pill_label = self._clean_value(row.get("Due Status", "")) or due_label
            pill_class = self._status_pill_class(pill_label)

        return (
            f'<div class="date-line"><span class="status-pill {pill_class}">{escape(pill_label)}</span>'
            f'<span>{escape(section_date)}</span></div>'
            f'<div class="timing-meta">Last harvested: {escape(last_harvested)}</div>'
            f'<div class="timing-meta">Periodicity: {escape(periodicity)}</div>'
        )

    def _status_pill_class(self, status_label: str) -> str:
        status_classes = {
            "Due": "status-pill--due",
            "Scheduled": "status-pill--scheduled",
            "No Schedule": "status-pill--no-schedule",
            "Review Due": "status-pill--reviews",
            "Review Scheduled": "status-pill--scheduled",
            "Review No Review": "status-pill--no-schedule",
        }
        return status_classes.get(status_label, "status-pill--no-schedule")

    def _harvest_record_url(self, record_id: str) -> str | None:
        cleaned_id = self._clean_value(record_id)
        if not cleaned_id:
            return None
        if cleaned_id in HARVEST_RECORD_LINKS:
            return HARVEST_RECORD_LINKS[cleaned_id]
        return f"https://geo.btaa.org/admin/documents/{cleaned_id}/edit"

    def _render_identifier_links(self, identifier_value: str) -> str:
        identifiers = self._extract_identifier_values(identifier_value)
        if not identifiers:
            return "<code>None</code>"

        links = [
            self._render_record_link(identifier, f"https://geo.btaa.org/admin/documents/{identifier}/edit")
            for identifier in identifiers
        ]
        return ", ".join(links)

    def _render_record_link(self, label: str, url: str | None) -> str:
        cleaned_label = self._clean_value(label)
        if not cleaned_label:
            return "<code>Not provided</code>"
        escaped_label = escape(cleaned_label)
        if not url:
            return f"<code>{escaped_label}</code>"
        return (
            f'<a href="{escape(url, quote=True)}" target="_blank" rel="noreferrer">'
            f"<code>{escaped_label}</code></a>"
        )

    def _calculate_due_date(self, last_harvested: str, periodicity: str) -> pd.Timestamp | None:
        periodicity_offset = self._periodicity_to_offset(periodicity)
        if periodicity_offset is None:
            return None

        last_harvested_date = pd.to_datetime(last_harvested, errors="coerce")
        if pd.isna(last_harvested_date):
            return self.today

        return (last_harvested_date + periodicity_offset).normalize()

    def _periodicity_to_offset(self, value: str) -> pd.DateOffset | pd.Timedelta | None:
        normalized = self._normalize_periodicity(value)
        if normalized in UNSCHEDULED_PERIODICITIES:
            return None

        every_match = re.fullmatch(
            r"every\s+(\d+)\s+(day|week|month|year)s?",
            normalized,
        )
        if every_match:
            interval = int(every_match.group(1))
            unit = every_match.group(2)
            return self._offset_from_unit(interval, unit)

        periodicity_map: list[tuple[tuple[str, ...], pd.DateOffset | pd.Timedelta | None]] = [
            (("continual", "continuous", "daily"), pd.Timedelta(days=1)),
            (("weekly",), pd.Timedelta(weeks=1)),
            (("biweekly", "fortnightly"), pd.Timedelta(days=14)),
            (("semimonthly", "semi monthly"), pd.Timedelta(days=15)),
            (("monthly",), pd.DateOffset(months=1)),
            (("bimonthly",), pd.DateOffset(months=2)),
            (("quarterly",), pd.DateOffset(months=3)),
            (("semiannual", "semi annual", "biannual", "twice a year"), pd.DateOffset(months=6)),
            (("annual", "annually", "yearly"), pd.DateOffset(years=1)),
        ]

        for aliases, offset in periodicity_map:
            if normalized in aliases:
                return offset

        return None

    def _offset_from_unit(self, interval: int, unit: str) -> pd.DateOffset | pd.Timedelta:
        if unit == "day":
            return pd.Timedelta(days=interval)
        if unit == "week":
            return pd.Timedelta(weeks=interval)
        if unit == "month":
            return pd.DateOffset(months=interval)
        return pd.DateOffset(years=interval)

    def _determine_due_status(
        self,
        due_date: pd.Timestamp | None,
        periodicity: str,
        row: pd.Series | dict[str, Any] | None = None,
    ) -> str:
        if self._has_pending_updates_tag(row):
            return "Due"
        if due_date is None:
            return "No Schedule"
        if due_date <= self.today:
            return "Due"
        return "Scheduled"

    def _calculate_review_date(
        self,
        last_harvested: str,
        periodicity: str,
        row: pd.Series | dict[str, Any] | None = None,
    ) -> pd.Timestamp | None:
        if self._normalize_periodicity(periodicity) != "irregular":
            return None

        review_interval_years = self._review_interval_years(row)
        if review_interval_years is None:
            return None

        last_harvested_date = pd.to_datetime(last_harvested, errors="coerce")
        if pd.isna(last_harvested_date):
            return None

        return last_harvested_date + pd.DateOffset(years=review_interval_years)

    def _determine_review_status(self, review_date: pd.Timestamp | None) -> str:
        if review_date is None:
            return "No Review"
        if review_date <= self.today:
            return "Due"
        return "Scheduled"

    def _has_pending_updates_tag(self, row: pd.Series | dict[str, Any] | None) -> bool:
        if row is None:
            return False

        return "queue:pending_updates" in self._extract_tag_values(row)

    def _review_interval_years(self, row: pd.Series | dict[str, Any] | None) -> int | None:
        if row is None:
            return None

        review_intervals = []
        for tag in self._extract_tag_values(row):
            match = re.fullmatch(r"review:(\d+)y", tag)
            if match:
                review_intervals.append(int(match.group(1)))

        if not review_intervals:
            return None
        return min(review_intervals)

    def _extract_tag_values(self, row: pd.Series | dict[str, Any] | None) -> set[str]:
        if row is None:
            return set()

        raw_values = [
            self._clean_value(row.get("Tag", "")),
            self._clean_value(row.get("Tags", "")),
        ]
        tags: set[str] = set()
        for raw_value in raw_values:
            if not raw_value:
                continue
            for tag in re.split(r"[|;,]", raw_value):
                cleaned_tag = self._clean_value(tag).lower()
                if cleaned_tag:
                    tags.add(cleaned_tag)
        return tags

    def _extract_identifier_values(self, value: str) -> list[str]:
        candidates = re.split(r"[|;,]", self._clean_value(value))
        return [normalized for candidate in candidates if (normalized := self._normalize_key(candidate))]

    def _build_display_name(self, row: pd.Series | dict[str, Any]) -> str:
        candidates = [
            "Title",
            "Name",
            "Collection Name",
            "Site Name",
            "Website Name",
            "ID",
            "Identifier",
        ]
        for candidate in candidates:
            value = self._clean_value(row.get(candidate, ""))
            if value:
                return value
        return "Unnamed task"

    def _build_website_name(self, row: pd.Series | dict[str, Any]) -> str:
        candidates = [
            "Website Title",
            "Website Name",
            "Website Site Name",
            "Website ID",
        ]
        for candidate in candidates:
            value = self._clean_value(row.get(candidate, ""))
            if value:
                return value
        return "None"

    def _select_due_date(self, group: pd.DataFrame) -> str:
        due_dates = sorted(
            {
                due_date
                for due_date in group["Due Date"].tolist()
                if self._clean_value(due_date)
            }
        )
        return due_dates[0] if due_dates else ""

    def _select_due_status(self, group: pd.DataFrame) -> str:
        priorities = {
            "Due": 0,
            "Scheduled": 1,
            "No Schedule": 2,
        }
        statuses = [
            self._clean_value(status)
            for status in group["Due Status"].tolist()
            if self._clean_value(status)
        ]
        if not statuses:
            return "No Schedule"
        return sorted(statuses, key=lambda status: priorities.get(status, 99))[0]

    def _format_date_range(self, values: list[Any]) -> str:
        cleaned = sorted({self._clean_value(value) for value in values if self._clean_value(value)})
        if not cleaned:
            return ""
        if len(cleaned) == 1:
            return cleaned[0]
        return f"{cleaned[0]} to {cleaned[-1]}"

    def _format_unique_values(self, values: list[Any]) -> str:
        cleaned = sorted({self._clean_value(value) for value in values if self._clean_value(value)})
        if not cleaned:
            return ""
        if len(cleaned) == 1:
            return cleaned[0]
        return ", ".join(cleaned)

    def _format_website_count_label(self, count: int) -> str:
        suffix = "website" if count == 1 else "websites"
        return f"{count} {suffix}"

    def _extract_provenance_entries(self, value: str) -> list[str]:
        cleaned_value = self._clean_value(value)
        if not cleaned_value:
            return []
        entries = re.split(r"\|(?=\d{4}-\d{2}-\d{2}\b)", cleaned_value)
        return [cleaned_entry for entry in entries if (cleaned_entry := self._clean_value(entry))]

    def _extract_dated_entry_date(self, value: str) -> pd.Timestamp | None:
        cleaned_value = self._clean_value(value)
        if not cleaned_value:
            return None
        match = re.match(r"^(\d{4}-\d{2}-\d{2})\b", cleaned_value)
        if not match:
            return None
        parsed_date = pd.to_datetime(match.group(1), errors="coerce")
        if pd.isna(parsed_date):
            return None
        return parsed_date.normalize()

    def _strip_dated_entry_prefix(self, value: str) -> str:
        cleaned_value = self._clean_value(value)
        stripped_value = re.sub(r"^\d{4}-\d{2}-\d{2}\s*", "", cleaned_value)
        stripped_value = re.sub(r"^[|/\-]+\s*", "", stripped_value)
        return stripped_value or cleaned_value

    def _extract_provenance_action_type(self, value: str) -> str:
        content = self._strip_dated_entry_prefix(value)
        if not content:
            return ""
        parts = [self._clean_value(part) for part in content.split("/")]
        for part in parts:
            if part:
                return part
        return ""

    def _extract_provenance_details(self, value: str) -> str:
        content = self._strip_dated_entry_prefix(value)
        if not content:
            return ""
        parts = [self._clean_value(part) for part in content.split("/")]
        non_empty_parts = [part for part in parts if part]
        if len(non_empty_parts) <= 1:
            return ""
        return " / ".join(non_empty_parts[1:])

    def _select_month_provenance_note(
        self, provenance_entries: list[str], target_date: pd.Timestamp | None
    ) -> str:
        if target_date is None:
            return "Not provided"

        target_period = target_date.to_period("M")
        matched_notes: list[str] = []
        for provenance_entry in provenance_entries:
            provenance_date = self._extract_dated_entry_date(provenance_entry)
            if provenance_date is None or provenance_date.to_period("M") != target_period:
                continue
            note = self._extract_provenance_details(provenance_entry)
            if note:
                matched_notes.append(note)

        if not matched_notes:
            return ""

        unique_notes = list(dict.fromkeys(matched_notes))
        return " | ".join(unique_notes)

    def _select_month_provenance_action_type(
        self, provenance_entries: list[str], target_date: pd.Timestamp | None
    ) -> str:
        if target_date is None:
            return ""

        target_period = target_date.to_period("M")
        for provenance_entry in provenance_entries:
            provenance_date = self._extract_dated_entry_date(provenance_entry)
            if provenance_date is None or provenance_date.to_period("M") != target_period:
                continue
            action_type = self._extract_provenance_action_type(provenance_entry)
            if action_type:
                return action_type
        return ""

    def _has_provenance_entry_for_month(
        self, provenance_entries: list[str], target_date: pd.Timestamp | None
    ) -> bool:
        if target_date is None:
            return False

        target_period = target_date.to_period("M")
        for provenance_entry in provenance_entries:
            provenance_date = self._extract_dated_entry_date(provenance_entry)
            if provenance_date is not None and provenance_date.to_period("M") == target_period:
                return True
        return False

    def _render_issue_links(self, row: pd.Series | dict[str, Any]) -> str:
        if not self.issue_repositories:
            return "<span class=\"muted\">No issue target configured</span>"

        links = []
        for issue_repository in self.issue_repositories:
            existing_issue = self._find_existing_issue(row, issue_repository)
            if existing_issue:
                issue_state = self._clean_value(existing_issue.get("state", "")).lower()
                issue_label = "Closed issue" if issue_state == "closed" else "Open issue"
                issue_number = self._clean_value(existing_issue.get("number", ""))
                label = f"{issue_label} #{issue_number}" if issue_number else issue_label
                links.append(
                    f'<a class="action-link" href="{escape(existing_issue["html_url"], quote=True)}" target="_blank" rel="noreferrer">{escape(label)}</a>'
                )
                continue

            issue_url = self._build_issue_url(row, issue_repository)
            links.append(
                f'<a class="action-link" href="{escape(issue_url, quote=True)}" target="_blank" rel="noreferrer">Create issue</a>'
            )
        return "".join(links)

    def _build_issue_url(self, row: pd.Series | dict[str, Any], issue_repository: dict[str, Any]) -> str:
        issues_new_url = self._clean_value(issue_repository.get("issues_new_url"))
        if not issues_new_url:
            return "#"

        query_params = {
            "template": self._clean_value(issue_repository.get("template")) or "harvest-task.md",
            "title": self._build_issue_title(row),
            "body": self._build_issue_body(row),
        }

        labels = [
            self._clean_value(label)
            for label in issue_repository.get("labels", [])
            if self._clean_value(label)
        ]
        issue_label = self._issue_label(row)
        if issue_label and issue_label not in labels:
            labels.append(issue_label)
        if labels:
            query_params["labels"] = ",".join(labels)

        projects = [
            self._clean_value(project)
            for project in issue_repository.get("projects", [])
            if self._clean_value(project)
        ]
        if projects:
            query_params["projects"] = ",".join(projects)

        return f"{issues_new_url}?{urlencode(query_params)}"

    def _build_issue_title(self, row: pd.Series | dict[str, Any]) -> str:
        return f"[{self._issue_prefix(row)}] {self._issue_display_name(row)}"

    def _build_issue_body(self, row: pd.Series | dict[str, Any]) -> str:
        issue_title = self._build_issue_title(row)
        due_date = self._issue_due_date(row)
        harvest_record_id = self._clean_value(row.get("ID", ""))
        harvest_record_line = self._markdown_link_line(
            "Harvest record",
            harvest_record_id or "Not provided",
            self._harvest_record_url(harvest_record_id),
        )
        identifier_line = self._build_identifier_markdown_line(row)
        lines = [
            f"# {issue_title}",
            "",
            harvest_record_line,
            identifier_line,
            f"- Due date: {due_date}",
            f"- Last harvested: {self._clean_value(row.get('Last Harvested', '')) or 'Not yet harvested'}",
            "",
            self._issue_task_marker(row),
            "",
            "## Notes",
            "",
        ]
        return "\n".join(lines)

    def _issue_prefix(self, row: pd.Series | dict[str, Any]) -> str:
        return "Review Due" if self._is_review_issue(row) else "Harvest Due"

    def _issue_display_name(self, row: pd.Series | dict[str, Any]) -> str:
        display_name = self._build_display_name(row)
        return re.sub(r"^\s*Harvest record for\s+", "", display_name, flags=re.IGNORECASE) or display_name

    def _issue_label(self, row: pd.Series | dict[str, Any]) -> str:
        return "review" if self._is_review_issue(row) else "harvest"

    def _issue_task_key(self, row: pd.Series | dict[str, Any]) -> str:
        task_id = self._clean_value(row.get("ID", "")) or self._issue_display_name(row)
        return f"{self._issue_label(row)}:{task_id}:{self._issue_due_date(row)}"

    def _issue_task_marker(self, row: pd.Series | dict[str, Any]) -> str:
        return f"<!-- {ISSUE_TASK_MARKER_PREFIX}: {self._issue_task_key(row)} -->"

    def _is_review_issue(self, row: pd.Series | dict[str, Any]) -> bool:
        return self._clean_value(row.get("Review Date", "")) != ""

    def _issue_due_date(self, row: pd.Series | dict[str, Any]) -> str:
        if self._is_review_issue(row):
            return self._clean_value(row.get("Review Date", "")) or "No review date"
        return self._clean_value(row.get("Due Date", "")) or "No schedule"

    def _markdown_link_line(self, label: str, text: str, url: str | None) -> str:
        cleaned_text = self._clean_value(text) or "Not provided"
        if url:
            return f"- {label}: [{cleaned_text}]({url})"
        return f"- {label}: {cleaned_text}"

    def _build_identifier_markdown_line(self, row: pd.Series | dict[str, Any]) -> str:
        identifiers = self._extract_identifier_values(self._clean_value(row.get("Identifier", "")))
        if not identifiers:
            return "- Identifier: None"

        links = [f"[{identifier}](https://geo.btaa.org/admin/documents/{identifier}/edit)" for identifier in identifiers]
        return f"- Identifier: {', '.join(links)}"

    def _find_existing_issue(
        self,
        row: pd.Series | dict[str, Any],
        issue_repository: dict[str, Any],
    ) -> dict[str, str] | None:
        task_key = self._issue_task_key(row)
        if not task_key:
            return None
        return self._existing_issue_index(issue_repository).get(task_key)

    def _existing_issue_index(self, issue_repository: dict[str, Any]) -> dict[str, dict[str, str]]:
        if not self._lookup_existing_issues_enabled(issue_repository):
            return {}
        repository_slug = self._issue_repository_slug(issue_repository)
        if not repository_slug:
            return {}
        if repository_slug not in self._issue_index_cache:
            self._issue_index_cache[repository_slug] = self._fetch_existing_issue_index(
                issue_repository,
                repository_slug,
            )
        return self._issue_index_cache[repository_slug]

    def _fetch_existing_issue_index(
        self,
        issue_repository: dict[str, Any],
        repository_slug: str,
    ) -> dict[str, dict[str, str]]:
        owner, repo = repository_slug.split("/", 1)
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "harvest-task-dashboard",
        }
        token = self._issue_repository_token(issue_repository)
        if token:
            headers["Authorization"] = f"Bearer {token}"

        issue_index: dict[str, dict[str, str]] = {}
        page = 1

        try:
            while True:
                params = {
                    "state": "all",
                    "per_page": "100",
                    "page": str(page),
                }

                response = requests.get(
                    f"https://api.github.com/repos/{owner}/{repo}/issues",
                    params=params,
                    headers=headers,
                    timeout=10,
                )
                response.raise_for_status()
                issues = response.json()
                if not isinstance(issues, list):
                    return issue_index

                for issue in issues:
                    if "pull_request" in issue:
                        continue
                    body = self._clean_value(issue.get("body", ""))
                    task_key = self._extract_issue_task_key(body)
                    if not task_key or task_key in issue_index:
                        continue

                    html_url = self._clean_value(issue.get("html_url", ""))
                    if not html_url:
                        continue

                    issue_index[task_key] = {
                        "html_url": html_url,
                        "number": self._clean_value(issue.get("number", "")),
                        "state": self._clean_value(issue.get("state", "")),
                    }

                if len(issues) < 100:
                    break
                page += 1
        except requests.RequestException:
            return {}

        return issue_index

    def _lookup_existing_issues_enabled(self, issue_repository: dict[str, Any]) -> bool:
        raw_value = self._clean_value(issue_repository.get("lookup_existing_issues", ""))
        return raw_value.lower() in {"1", "true", "yes", "on"}

    def _issue_repository_slug(self, issue_repository: dict[str, Any]) -> str:
        configured_repository = self._clean_value(issue_repository.get("repository", ""))
        if configured_repository:
            return configured_repository.strip("/")

        issues_new_url = self._clean_value(issue_repository.get("issues_new_url", ""))
        match = re.search(r"github\.com/([^/]+/[^/]+)/issues/new/?$", issues_new_url)
        if not match:
            return ""
        return match.group(1)

    def _issue_repository_token(self, issue_repository: dict[str, Any]) -> str:
        configured_env = self._clean_value(issue_repository.get("token_env", ""))
        candidate_envs = [configured_env] if configured_env else ["GITHUB_TOKEN", "GEOBTAA_PROJECTS_TOKEN"]
        for env_name in candidate_envs:
            token = os.environ.get(env_name, "").strip()
            if token:
                return token
        return ""

    def _extract_issue_task_key(self, issue_body: str) -> str:
        match = re.search(
            rf"<!--\s*{ISSUE_TASK_MARKER_PREFIX}:\s*(.*?)\s*-->",
            self._clean_value(issue_body),
            flags=re.IGNORECASE,
        )
        if not match:
            return ""
        return self._clean_value(match.group(1))

    def _normalize_periodicity(self, value: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", " ", self._clean_value(value).lower()).strip()
        return normalized

    def _normalize_key(self, value: str) -> str:
        return self._clean_value(value).strip().lower()

    def _dated_output_path(self, configured_path: Path) -> Path:
        filename = f"{self.today.strftime('%Y-%m-%d')}_{configured_path.name}"
        return configured_path.parent / filename

    def _dated_directory(self, configured_path: Path) -> Path:
        dated_name = f"{self.today.strftime('%Y-%m-%d')}_{configured_path.name}"
        return configured_path.parent / dated_name

    def _dedicated_workflow_output_path(self, workflow: str) -> Path:
        workflow_slug = self._slugify(self._clean_value(workflow) or "unspecified")
        filename = (
            f"{self.output_dashboard_html.stem}-{workflow_slug}{self.output_dashboard_html.suffix}"
        )
        return self.output_dashboard_html.with_name(filename)

    def _slugify(self, value: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
        return slug or "unspecified"

    def _clean_value(self, value: Any) -> str:
        if value is None:
            return ""
        if pd.isna(value):
            return ""
        return str(value).strip()

    def _first_non_empty(self, *values: Any) -> str:
        for value in values:
            cleaned = self._clean_value(value)
            if cleaned:
                return cleaned
        return ""

    def _ensure_columns(self, df: pd.DataFrame, required_columns: list[str]) -> None:
        for column in required_columns:
            if column not in df.columns:
                df[column] = ""

    def _extract_workflow_from_report_type(self, report_type: str) -> tuple[str, bool]:
        workflow_name = self._clean_value(report_type)
        if workflow_name in self.dedicated_workflow_views:
            return workflow_name, True
        return "", False

    def _use_combined_workflow_view(self, workflow: str) -> bool:
        return self._clean_value(workflow) in self.dedicated_workflow_views

    def _latest_harvest_date(self, harvest_df: pd.DataFrame) -> str:
        if harvest_df.empty:
            return ""

        self._ensure_columns(harvest_df, ["Last Harvested"])
        last_harvested = pd.to_datetime(harvest_df["Last Harvested"], errors="coerce")
        if last_harvested.isna().all():
            return ""
        return last_harvested.max().strftime("%Y-%m-%d")

    def _prepare_workflow_record_view(self, harvest_df: pd.DataFrame) -> pd.DataFrame:
        if harvest_df.empty:
            return harvest_df.copy()

        working_df = harvest_df.copy()
        self._ensure_columns(
            working_df,
            ["Last Harvested", "Title", "ID", "Identifier", "Endpoint URL", "Endpoint Description", "Website Platform", "Updated At"],
        )
        working_df["__last_harvested_sort"] = pd.to_datetime(
            working_df["Last Harvested"],
            errors="coerce",
        )
        working_df["__display_name"] = working_df.apply(self._build_display_name, axis=1)
        working_df = working_df.sort_values(
            by=["__last_harvested_sort", "__display_name", "ID"],
            ascending=[False, True, True],
            na_position="last",
        ).reset_index(drop=True)
        return working_df.drop(columns=["__last_harvested_sort", "__display_name"])

    def _report_title(self, report_type: str = "full", workflow: str = "") -> str:
        workflow_name = self._clean_value(workflow)
        if report_type == "retrospective":
            base_title = "Harvest Task Retrospective"
        elif report_type == "due":
            base_title = "Harvest Tasks Due Now"
        else:
            base_title = "Harvest Task Dashboard"

        if not workflow_name:
            return base_title

        workflow_label = self._workflow_view_label(workflow_name)
        return f"{workflow_label} {base_title}"

    def _workflow_view_label(self, workflow: str) -> str:
        workflow_name = self._clean_value(workflow)
        consolidated_title = CONSOLIDATED_WORKFLOW_TITLES.get(workflow_name, workflow_name)
        return re.sub(r"^\s*Scan\s+", "", consolidated_title).strip() or workflow_name


if __name__ == "__main__":
    default_config = {
        "harvest_records_csv": "inputs/harvest-records.csv",
        "websites_csv": "inputs/websites.csv",
        "output_tasks_csv": "reports/harvest-task-dashboard.csv",
        "output_dashboard_html": "reports/harvest-task-dashboard.html",
        "output_workflow_dir": "inputs/harvest-workflow-inputs",
        "dedicated_workflow_views": ["py_arcgis_hub"],
        "issue_repositories": [
            {
                "name": "harvest-operations",
                "repository": "geobtaa/harvest-operations",
                "issues_new_url": "https://github.com/geobtaa/harvest-operations/issues/new",
                "template": "harvest-task.md",
                "token_env": "GEOBTAA_PROJECTS_TOKEN",
                "lookup_existing_issues": True,
                "labels": ["harvest-task"],
                "projects": ["geobtaa/4"],
            }
        ],
    }
    results = HarvestTaskDashboardJob(default_config).harvest_pipeline()
    print(results)
