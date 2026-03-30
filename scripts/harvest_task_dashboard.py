from __future__ import annotations

from html import escape
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlencode

import pandas as pd


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


class HarvestTaskDashboardJob:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.harvest_records_path = Path(config.get("harvest_records_csv", "inputs/harvest-records.csv"))
        self.websites_path = Path(config.get("websites_csv", "inputs/websites.csv"))
        self.output_tasks_csv = Path(config.get("output_tasks_csv", "outputs/harvest-task-dashboard.csv"))
        self.output_dashboard_html = Path(
            config.get("output_dashboard_html", "outputs/harvest-task-dashboard.html")
        )
        self.output_workflow_dir = Path(
            config.get("output_workflow_dir", "outputs/harvest-workflow-inputs")
        )
        self.issue_repositories = config.get("issue_repositories", [])

        configured_today = config.get("today")
        if configured_today:
            self.today = pd.Timestamp(configured_today).normalize()
        else:
            self.today = pd.Timestamp.now().normalize()

    def harvest_pipeline(self) -> dict[str, Any]:
        harvest_df = self._load_csv(self.harvest_records_path)
        websites_df = self._load_csv(self.websites_path)

        task_df = self._build_task_dataframe(harvest_df, websites_df)
        dashboard_html = self._render_dashboard_html(task_df)

        task_output_path = self._write_dataframe(task_df, self.output_tasks_csv)
        dashboard_output_path = self._write_text(dashboard_html, self.output_dashboard_html)
        workflow_outputs = self._write_workflow_inputs(websites_df)

        summary = self._build_summary(task_df)
        return {
            "status": "completed",
            "task_count": len(task_df),
            "workflow_count": len(workflow_outputs),
            "summary": summary,
            "task_csv": str(task_output_path),
            "dashboard_html": str(dashboard_output_path),
            "workflow_inputs": workflow_outputs,
        }

    def render_dashboard_view(self) -> str:
        harvest_df = self._load_csv(self.harvest_records_path)
        websites_df = self._load_csv(self.websites_path)
        task_df = self._build_task_dataframe(harvest_df, websites_df)
        return self._render_dashboard_html(task_df)

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
            base_task["Due Status"] = self._determine_due_status(due_date, row_dict.get("Accrual Periodicity", ""))
            base_task["Days Until Due"] = (
                str((due_date - self.today).days) if due_date is not None else ""
            )

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

    def _build_summary(self, task_df: pd.DataFrame) -> dict[str, int]:
        if "Due Status" in task_df.columns:
            due_status = task_df["Due Status"]
        else:
            due_status = pd.Series(dtype=str)

        return {
            "total": int(len(task_df)),
            "overdue": int((due_status == "Overdue").sum()),
            "due_today": int((due_status == "Due Today").sum()),
            "upcoming": int((due_status == "Upcoming").sum()),
            "no_schedule": int((due_status == "No Schedule").sum()),
        }

    def _render_dashboard_html(self, task_df: pd.DataFrame) -> str:
        summary = self._build_summary(task_df)
        sections = self._build_dashboard_sections(task_df)

        html_parts = [
            "<!DOCTYPE html>",
            "<html lang=\"en\">",
            "<head>",
            "  <meta charset=\"UTF-8\">",
            "  <title>Harvest Task Dashboard</title>",
            "  <style>",
            "    body { font-family: sans-serif; margin: 2rem auto; max-width: 1200px; padding: 0 1rem 3rem; line-height: 1.4; }",
            "    h1, h2, h3 { margin-bottom: 0.5rem; }",
            "    .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 0.75rem; margin: 1.5rem 0; }",
            "    .card { background: #f5f5f5; border: 1px solid #ddd; border-radius: 8px; padding: 0.9rem; }",
            "    .card strong { display: block; font-size: 1.6rem; }",
            "    .due-section { margin-top: 2rem; }",
            "    .workflow-block { margin: 1rem 0 1.5rem; }",
            "    table { width: 100%; border-collapse: collapse; margin-top: 0.5rem; }",
            "    th, td { border: 1px solid #ddd; padding: 0.55rem; text-align: left; vertical-align: top; }",
            "    th { background: #f2f2f2; }",
            "    .muted { color: #666; }",
            "    code { background: #f2f2f2; padding: 0.1rem 0.3rem; }",
            "    .actions { min-width: 210px; }",
            "    .action-link { display: inline-block; margin: 0.2rem 0.35rem 0.2rem 0; padding: 0.35rem 0.55rem; border: 1px solid #ccc; border-radius: 6px; text-decoration: none; color: inherit; background: #fafafa; }",
            "  </style>",
            "</head>",
            "<body>",
            "  <h1>Harvest Task Dashboard</h1>",
            f"  <p class=\"muted\">Generated from <code>{escape(str(self.harvest_records_path))}</code> and <code>{escape(str(self.websites_path))}</code>.</p>",
            "  <p class=\"muted\">Use the issue buttons to open a prefilled GitHub issue from the harvest-task template.</p>",
            "  <div class=\"summary\">",
        ]

        summary_cards = [
            ("Total Tasks", summary["total"]),
            ("Overdue", summary["overdue"]),
            ("Due Today", summary["due_today"]),
            ("Upcoming", summary["upcoming"]),
            ("No Schedule", summary["no_schedule"]),
        ]
        for label, value in summary_cards:
            html_parts.extend(
                [
                    "    <div class=\"card\">",
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
            html_parts.extend(
                [
                    "  <section class=\"due-section\">",
                    f"    <h2>{escape(due_label)} ({total_in_section})</h2>",
                ]
            )
            for workflow_name, workflow_group in workflow_groups:
                html_parts.extend(
                    [
                        "    <div class=\"workflow-block\">",
                        f"      <h3>{escape(workflow_name)} ({len(workflow_group)})</h3>",
                        "      <table>",
                        "        <thead>",
                        "          <tr>",
                        "            <th>Task</th>",
                        "            <th>Last Harvested</th>",
                        "            <th>Accrual Periodicity</th>",
                        "            <th>Identifier</th>",
                        "            <th>Website Record</th>",
                        "            <th>Due Status</th>",
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
                            f"            <td>{escape(self._build_display_name(row))}</td>",
                            f"            <td>{escape(self._clean_value(row.get('Last Harvested', '')) or 'Not yet harvested')}</td>",
                            f"            <td>{escape(self._clean_value(row.get('Accrual Periodicity', '')) or 'Not provided')}</td>",
                            f"            <td>{escape(self._clean_value(row.get('Identifier', '')) or 'None')}</td>",
                            f"            <td>{escape(self._build_website_name(row))}</td>",
                            f"            <td>{escape(self._clean_value(row.get('Due Status', '')) or 'No Schedule')}</td>",
                            f"            <td class=\"actions\">{self._render_issue_links(row)}</td>",
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

        ordered_due_labels = []
        dated_rows = working_df.dropna(subset=["__due_sort"])
        for due_date in dated_rows["__due_sort"].drop_duplicates().sort_values():
            ordered_due_labels.append(due_date.strftime("%Y-%m-%d"))

        if working_df["__due_sort"].isna().any():
            ordered_due_labels.append("No Schedule")

        for due_label in ordered_due_labels:
            if due_label == "No Schedule":
                due_group = working_df[working_df["__due_sort"].isna()].copy()
            else:
                due_group = working_df[working_df["Due Date"] == due_label].copy()

            workflow_groups: list[tuple[str, pd.DataFrame]] = []
            for workflow_name, workflow_group in due_group.groupby(
                "Effective Harvest Workflow", dropna=False
            ):
                workflow_groups.append((workflow_name or "unspecified", workflow_group))

            workflow_groups.sort(key=lambda item: item[0])
            sections.append((due_label, workflow_groups))

        return sections

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

    def _determine_due_status(self, due_date: pd.Timestamp | None, periodicity: str) -> str:
        if due_date is None:
            return "No Schedule"
        if due_date < self.today:
            return "Overdue"
        if due_date == self.today:
            return "Due Today"
        return "Upcoming"

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

    def _render_issue_links(self, row: pd.Series | dict[str, Any]) -> str:
        if not self.issue_repositories:
            return "<span class=\"muted\">No issue target configured</span>"

        links = []
        for issue_repository in self.issue_repositories:
            issue_url = self._build_issue_url(row, issue_repository)
            link_label = self._clean_value(issue_repository.get("name")) or "Create Issue"
            links.append(
                f'<a class="action-link" href="{escape(issue_url, quote=True)}" target="_blank" rel="noreferrer">Issue: {escape(link_label)}</a>'
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
        if labels:
            query_params["labels"] = ",".join(labels)

        return f"{issues_new_url}?{urlencode(query_params)}"

    def _build_issue_title(self, row: pd.Series | dict[str, Any]) -> str:
        display_name = self._build_display_name(row)
        workflow = self._clean_value(row.get("Effective Harvest Workflow", ""))
        due_date = self._clean_value(row.get("Due Date", ""))

        title = f"[Harvest Task] {display_name}"
        if workflow:
            title += f" ({workflow})"
        if due_date:
            title += f" due {due_date}"
        return title

    def _build_issue_body(self, row: pd.Series | dict[str, Any]) -> str:
        website_name = self._build_website_name(row)
        lines = [
            "## Summary",
            f"Follow up on the scheduled harvest task for **{self._build_display_name(row)}**.",
            "",
            "## Dashboard Task Details",
            f"- Due Date: {self._clean_value(row.get('Due Date', '')) or 'No schedule'}",
            f"- Due Status: {self._clean_value(row.get('Due Status', '')) or 'No Schedule'}",
            f"- Harvest Workflow: {self._clean_value(row.get('Effective Harvest Workflow', '')) or 'unspecified'}",
            f"- Last Harvested: {self._clean_value(row.get('Last Harvested', '')) or 'Not yet harvested'}",
            f"- Accrual Periodicity: {self._clean_value(row.get('Accrual Periodicity', '')) or 'Not provided'}",
            f"- Task ID: {self._clean_value(row.get('ID', '')) or 'Not provided'}",
            f"- Identifier: {self._clean_value(row.get('Identifier', '')) or 'None'}",
            f"- Website Record: {website_name}",
            "",
            "## Inputs",
            f"- Harvest Records CSV: `{self.harvest_records_path}`",
            f"- Websites CSV: `{self.websites_path}`",
            "",
            "## Notes",
            "- Confirm whether the workflow-specific website input CSV needs to be regenerated before running the task.",
            "- Add implementation notes, blockers, and links here.",
        ]
        return "\n".join(lines)

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


if __name__ == "__main__":
    default_config = {
        "harvest_records_csv": "inputs/harvest-records.csv",
        "websites_csv": "inputs/websites.csv",
        "output_tasks_csv": "outputs/harvest-task-dashboard.csv",
        "output_dashboard_html": "outputs/harvest-task-dashboard.html",
        "output_workflow_dir": "outputs/harvest-workflow-inputs",
        "issue_repositories": [
            {
                "name": "harvest-operations",
                "issues_new_url": "https://github.com/geobtaa/harvest-operations/issues/new",
                "template": "harvest-task.md",
                "labels": ["harvest-task"],
            }
        ],
    }
    results = HarvestTaskDashboardJob(default_config).harvest_pipeline()
    print(results)
