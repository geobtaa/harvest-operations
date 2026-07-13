from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from pathlib import Path
import re
import shutil


SITE_TITLE = "Harvest Task Dashboard Reports"
DEDICATED_WORKFLOW_PREFIX = "harvest-task-dashboard-"
ARCGIS_WORKFLOW_SLUG = "py-arcgis-hub"
SOCRATA_WORKFLOW_SLUG = "py-socrata"
CKAN_WORKFLOW_SLUG = "py-ckan"
STANDARD_REPORT_TYPES = {
    "institutions": {
        "suffix": "harvest-task-dashboard-institutions.html",
        "label": "By institution",
        "description": "Harvest-record list grouped by institution code prefix.",
        "latest_href": "latest/institutions/",
        "archive_segment": "institutions",
    },
    "map-collections": {
        "suffix": "harvest-task-dashboard-map-collections.html",
        "label": "Map collections only",
        "description": "Harvest-record list filtered to Subject=Maps and grouped by institution.",
        "latest_href": "latest/map-collections/",
        "archive_segment": "map-collections",
    },
    "standalone": {
        "suffix": "harvest-task-dashboard-standalone-websites.html",
        "label": "Standalone websites",
        "description": "Standalone website records grouped by institution derived from record ID.",
        "latest_href": "latest/standalone-websites/",
        "archive_segment": "standalone-websites",
    },
    "due": {
        "suffix": "harvest-task-dashboard-due.html",
        "label": "Due-only tasks",
        "description": "Only tasks that are currently due.",
        "latest_href": "latest/due/",
        "archive_segment": "due",
    },
    "review": {
        "suffix": "harvest-task-dashboard-review.html",
        "label": "Triage",
        "description": "All harvest records, grouped for review triage.",
        "latest_href": "latest/triage/",
        "archive_segment": "triage",
    },
    "todo": {
        "suffix": "harvest-task-dashboard-todo.html",
        "label": "To do",
        "description": "Tagged harvest records that need work.",
        "latest_href": "latest/to-do/",
        "archive_segment": "to-do",
    },
    "retrospective": {
        "suffix": "harvest-task-dashboard-retrospective.html",
        "label": "Retrospective",
        "description": "Historical harvest review view.",
        "latest_href": "latest/retrospective/",
        "archive_segment": "retrospective",
    },
}
STANDARD_REPORT_ORDER = (
    "review",
    "todo",
    "retrospective",
    "institutions",
    "map-collections",
    "standalone",
)
REPORT_COLUMN_GROUPS = (
    ("Triage", ("review", "todo")),
    (
        "Reports",
        (
            "retrospective",
            f"workflow:{ARCGIS_WORKFLOW_SLUG}",
            f"workflow:{SOCRATA_WORKFLOW_SLUG}",
            f"workflow:{CKAN_WORKFLOW_SLUG}",
        ),
    ),
    ("Lists", ("institutions", "map-collections", "standalone")),
)


@dataclass(frozen=True)
class DashboardReport:
    date: str
    report_key: str
    source_path: Path
    label: str
    description: str
    latest_href: str
    archive_href: str

    def sort_order(self) -> tuple[int, int, str]:
        if self.report_key in STANDARD_REPORT_ORDER:
            return (0, STANDARD_REPORT_ORDER.index(self.report_key), self.label.lower())
        return (1, 0, self.label.lower())


def collect_reports(reports_dir: Path) -> dict[str, DashboardReport]:
    """Collect the current, shared dashboard reports.

    Dashboard report files are overwritten on each run. Older public-suffixed
    and date-prefixed files are deliberately ignored so Pages only publishes
    the current set of reports.
    """
    reports: dict[str, DashboardReport] = {}
    for report_type, report_config in STANDARD_REPORT_TYPES.items():
        report_path = reports_dir / str(report_config["suffix"])
        if not report_path.is_file():
            continue
        reports[report_type] = DashboardReport(
            date="",
            report_key=report_type,
            source_path=report_path,
            label=str(report_config["label"]),
            description=str(report_config["description"]),
            latest_href=str(report_config["latest_href"]),
            archive_href="",
        )

    for workflow_reports in collect_workflow_reports(reports_dir).values():
        if workflow_reports:
            latest_report = max(workflow_reports, key=lambda report: report.date)
            reports[latest_report.report_key] = latest_report

    return reports


def collect_workflow_reports(reports_dir: Path) -> dict[str, list[DashboardReport]]:
    workflow_reports: dict[str, list[DashboardReport]] = {}
    for report_path in sorted(reports_dir.glob(f"????-??-??_{DEDICATED_WORKFLOW_PREFIX}*.html")):
        report_date, report_name = report_path.name.split("_", 1)
        report = _collect_dedicated_workflow_report(report_date, report_name, report_path)
        if report is None:
            continue
        workflow_slug = report.report_key.removeprefix("workflow:")
        workflow_reports.setdefault(workflow_slug, []).append(report)
    return workflow_reports


def build_pages_site(reports_dir: Path, output_dir: Path) -> None:
    reports = collect_reports(reports_dir)
    workflow_reports = collect_workflow_reports(reports_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not reports:
        raise ValueError(f"No dashboard HTML files were found in {reports_dir}")

    for stale_archive_dir in (output_dir / "archive", output_dir / "workflows"):
        if stale_archive_dir.exists():
            shutil.rmtree(stale_archive_dir)
    for dated_directory in output_dir.iterdir():
        if dated_directory.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", dated_directory.name):
            shutil.rmtree(dated_directory)

    output_dir.joinpath(".nojekyll").write_text("", encoding="utf-8")

    latest_reports = _copy_reports(reports, output_dir)
    write_index_page(output_dir, latest_reports)
    write_workflow_report_pages(output_dir, workflow_reports)


def _copy_reports(
    reports: dict[str, DashboardReport],
    output_dir: Path,
) -> dict[str, DashboardReport]:
    for report in reports.values():
        if report.report_key.startswith("workflow:"):
            continue
        latest_target = output_dir / report.latest_href / "index.html"
        latest_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(report.source_path, latest_target)
    return reports


def write_index_page(
    output_dir: Path,
    latest_reports: dict[str, DashboardReport],
) -> None:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    latest_report_columns = _render_report_columns(
        latest_reports,
        href_attr="latest_href",
        link_text="label",
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{SITE_TITLE}</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #17324d;
      --muted: #5e6f83;
      --line: #d7e1ec;
      --link: #4f7f9f;
      --border-blue: #9fbfd0;
      --border-orange: #e0b08b;
      --border-purple: #c9b4d4;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", sans-serif;
      line-height: 1.5;
      color: var(--ink);
    }}
    main {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 2rem 1rem 3rem;
    }}
    h1, h2, p {{ margin-top: 0; }}
    a {{ color: var(--link); }}
    .hero {{
      border: 3px solid var(--border-blue);
      border-radius: 0;
    }}
    .hero {{
      padding: 1rem;
      margin-bottom: 1rem;
    }}
    .hero p:last-child {{
      margin-bottom: 0;
    }}
    .eyebrow {{
      color: var(--muted);
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-weight: 700;
    }}
    .report-columns {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 1.25rem;
      margin-bottom: 1rem;
    }}
    .report-columns > section {{
      border: 3px solid var(--border-blue);
      padding: 0.75rem;
    }}
    .report-columns > section:nth-child(2) {{ border-color: var(--border-orange); }}
    .report-columns > section:nth-child(3) {{ border-color: var(--border-purple); }}
    .report-columns h2 {{
      font-size: 1rem;
      margin-bottom: 0.35rem;
    }}
    .report-columns ul {{
      margin: 0;
      padding-left: 1.25rem;
    }}
    .report-columns li {{
      margin: 0.35rem 0;
    }}
    footer {{
      margin-top: 1rem;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    @media (max-width: 700px) {{
      main {{
        padding: 1.25rem 0.75rem 2rem;
      }}
      .report-columns {{
        grid-template-columns: minmax(0, 1fr);
      }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <p class="eyebrow">GitHub Pages</p>
      <h1>{SITE_TITLE}</h1>
      <p>This site publishes the current dashboard HTML files from <code>reports/</code>.</p>
      <p>The site index was generated at {escape(generated_at)}.</p>
    </section>

    <section class="report-columns" aria-label="Latest reports">
{latest_report_columns}
    </section>

    <footer>
      Publish flow: generate dashboard HTML locally, commit the new files under <code>reports/</code>, and push to <code>main</code>.
    </footer>
  </main>
</body>
</html>
"""
    output_dir.joinpath("index.html").write_text(html, encoding="utf-8")


def write_archive_index_page(
    output_dir: Path,
    reports_by_date: dict[str, dict[str, DashboardReport]],
) -> None:
    archive_rows = []
    for date, reports in reports_by_date.items():
        links = []
        for report in _ordered_reports(reports):
            links.append(f'<a href="../{escape(report.archive_href)}">{escape(report.label)}</a>')
        archive_rows.append(
            f"""
        <tr>
          <th scope="row">{escape(date)}</th>
          <td>{' | '.join(links)}</td>
        </tr>
"""
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{SITE_TITLE} Archive</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #17324d;
      --muted: #5e6f83;
      --line: #d7e1ec;
    }}
    body {{
      margin: 0;
      font-family: "Segoe UI", sans-serif;
      line-height: 1.5;
      color: var(--ink);
    }}
    main {{
      max-width: 960px;
      margin: 0 auto;
      padding: 2rem 1rem 3rem;
    }}
    .panel {{
      border: 1px solid var(--line);
      border-radius: 18px;
      overflow: hidden;
    }}
    .panel-header {{
      padding: 1rem 1.25rem;
      border-bottom: 1px solid var(--line);
    }}
    .eyebrow {{
      color: var(--muted);
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-weight: 700;
      margin: 0 0 0.35rem;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th,
    td {{
      padding: 0.9rem 1.25rem;
      text-align: left;
      border-top: 1px solid var(--line);
      vertical-align: top;
    }}
    tbody tr:first-child th,
    tbody tr:first-child td {{
      border-top: none;
    }}
  </style>
</head>
<body>
  <main>
    <p><a href="../">Back to latest reports</a></p>
    <section class="panel">
      <div class="panel-header">
        <p class="eyebrow">Archive</p>
        <h1>{SITE_TITLE}</h1>
      </div>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Reports</th>
          </tr>
        </thead>
        <tbody>
{''.join(archive_rows)}
        </tbody>
      </table>
    </section>
  </main>
</body>
</html>
"""
    archive_dir = output_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.joinpath("index.html").write_text(html, encoding="utf-8")


def write_workflow_report_pages(
    output_dir: Path,
    reports_by_workflow: dict[str, list[DashboardReport]],
) -> None:
    for workflow_slug, reports in reports_by_workflow.items():
        report_rows = []
        for report in sorted(reports, key=lambda item: item.date, reverse=True):
            report_target = output_dir / report.archive_href / "index.html"
            report_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(report.source_path, report_target)
            report_rows.append(
                f"""
        <tr>
          <th scope="row">{escape(report.date)}</th>
          <td><a href="../../{escape(report.archive_href)}">View report</a></td>
        </tr>
"""
            )

        workflow_label = _workflow_report_label("", workflow_slug)
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(workflow_label)} Reports</title>
  <style>
    :root {{ color-scheme: light; --ink: #17324d; --muted: #5e6f83; --line: #d7e1ec; --accent: #1f6fb2; }}
    body {{ margin: 0; font-family: "Segoe UI", sans-serif; line-height: 1.5; color: var(--ink); }}
    main {{ max-width: 900px; margin: 0 auto; padding: 2rem 1rem 3rem; }}
    a {{ color: var(--accent); }}
    .muted {{ color: var(--muted); }}
    .archive {{ border: 1px solid var(--line); border-radius: 16px; overflow: hidden; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 0.75rem; text-align: left; border-bottom: 1px solid var(--line); }}
    tbody tr:last-child th, tbody tr:last-child td {{ border-bottom: none; }}
  </style>
</head>
<body>
  <main>
    <p><a href="../../">Back to reports</a></p>
    <h1>{escape(workflow_label)} Reports</h1>
    <p class="muted">Select a report date to view the historical harvest results.</p>
    <section class="archive">
      <table>
        <thead><tr><th>Date</th><th>Report</th></tr></thead>
        <tbody>
{''.join(report_rows)}
        </tbody>
      </table>
    </section>
  </main>
</body>
</html>
"""
        archive_path = output_dir / "workflows" / workflow_slug / "index.html"
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        archive_path.write_text(html, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a GitHub Pages site from committed dashboard report HTML files."
    )
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("reports"),
        help="Directory containing dated dashboard HTML files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("site"),
        help="Directory to write the GitHub Pages site into.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_pages_site(args.reports_dir, args.output_dir)


def _collect_dedicated_workflow_report(
    date: str,
    report_name: str,
    report_path: Path,
) -> DashboardReport | None:
    if not report_name.startswith(DEDICATED_WORKFLOW_PREFIX) or not report_name.endswith(".html"):
        return None

    workflow_slug = report_name.removeprefix(DEDICATED_WORKFLOW_PREFIX).removesuffix(".html")
    if not workflow_slug:
        return None

    workflow_title = _extract_html_title(report_path)
    workflow_label = _workflow_report_label(workflow_title, workflow_slug)
    return DashboardReport(
        date=date,
        report_key=f"workflow:{workflow_slug}",
        source_path=report_path,
        label=workflow_label,
        description=_workflow_report_description(workflow_slug),
        latest_href=f"workflows/{workflow_slug}/",
        archive_href=f"{date}/workflows/{workflow_slug}/",
    )


def _extract_html_title(report_path: Path) -> str:
    html = report_path.read_text(encoding="utf-8", errors="ignore")
    match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if match is None:
        return ""
    return re.sub(r"\s+", " ", match.group(1)).strip()


def _workflow_report_label(workflow_title: str, workflow_slug: str) -> str:
    if workflow_slug == ARCGIS_WORKFLOW_SLUG:
        return "ArcGIS Hub report"
    if workflow_slug == SOCRATA_WORKFLOW_SLUG:
        return "Socrata report"
    if workflow_slug == CKAN_WORKFLOW_SLUG:
        return "CKAN report"
    if workflow_title:
        normalized_title = re.sub(
            r"\s+Harvest (?:Overview|Report)(?:\s+-\s+\d{4}-\d{2}-\d{2})?\s*$",
            "",
            workflow_title,
        ).strip()
        if normalized_title:
            return normalized_title
    return workflow_slug.replace("-", " ").title()


def _workflow_report_description(workflow_slug: str) -> str:
    if workflow_slug == ARCGIS_WORKFLOW_SLUG:
        return "Dedicated workflow harvest report with latest ArcGIS count columns."
    if workflow_slug == SOCRATA_WORKFLOW_SLUG:
        return "Dedicated workflow harvest report with latest Socrata count columns."
    return "Dedicated workflow report."


def _ordered_reports(reports: dict[str, DashboardReport]) -> list[DashboardReport]:
    return sorted(reports.values(), key=lambda report: report.sort_order())


def _render_report_columns(
    reports: dict[str, DashboardReport],
    *,
    href_attr: str,
    link_text: str,
) -> str:
    columns = []
    for heading, report_keys in REPORT_COLUMN_GROUPS:
        links = []
        for report_key in report_keys:
            report = reports.get(report_key)
            if report is None:
                continue
            href = _report_column_href(report, href_attr)
            text = getattr(report, link_text)
            links.append(f'<li><a href="{escape(href)}">{escape(text)}</a></li>')
        if not links:
            continue
        columns.append(
            f"""
      <section>
        <h2>{escape(heading)}</h2>
        <ul>
          {''.join(links)}
        </ul>
      </section>
"""
        )
    return "".join(columns)


def _report_column_href(report: DashboardReport, href_attr: str) -> str:
    if href_attr == "latest_href" and report.report_key.startswith("workflow:"):
        workflow_slug = report.report_key.removeprefix("workflow:")
        return f"workflows/{workflow_slug}/"
    return str(getattr(report, href_attr))


if __name__ == "__main__":
    main()
