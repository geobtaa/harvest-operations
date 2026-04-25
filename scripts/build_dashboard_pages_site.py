from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from pathlib import Path
import re
import shutil


SITE_TITLE = "Harvest Task Dashboard Reports"
STANDARD_REPORT_TYPES = {
    "records": {
        "suffix": "harvest-task-dashboard-records.html",
        "label": "Harvest records",
        "description": "Compact harvest-record list grouped by accrual periodicity.",
        "latest_href": "latest/",
        "archive_segment": "",
    },
    "institutions": {
        "suffix": "harvest-task-dashboard-institutions.html",
        "label": "By institution",
        "description": "Harvest-record list grouped by institution code prefix.",
        "latest_href": "latest/institutions/",
        "archive_segment": "institutions",
    },
    "map-collections": {
        "suffix": "harvest-task-dashboard-map-collections.html",
        "label": "Map collections",
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
        "label": "Due now",
        "description": "Only tasks that are currently due.",
        "latest_href": "latest/due/",
        "archive_segment": "due",
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
    "records",
    "institutions",
    "map-collections",
    "standalone",
    "due",
    "retrospective",
)
DEDICATED_WORKFLOW_PREFIX = "harvest-task-dashboard-"
PUBLIC_REPORT_SUFFIX = "-public"
ARCGIS_WORKFLOW_SLUG = "py-arcgis-hub"


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


def collect_reports(reports_dir: Path) -> dict[str, dict[str, DashboardReport]]:
    collected: dict[str, dict[str, tuple[bool, DashboardReport]]] = {}

    for report_path in sorted(reports_dir.glob("*.html")):
        if "_" not in report_path.name:
            continue

        report_date, report_name = report_path.name.split("_", 1)
        report_name, is_public = _normalize_report_name(report_name)
        for report_type, report_config in STANDARD_REPORT_TYPES.items():
            if report_name != report_config["suffix"]:
                continue
            _store_report(
                collected,
                report_date,
                report_type,
                DashboardReport(
                    date=report_date,
                    report_key=report_type,
                    source_path=report_path,
                    label=report_config["label"],
                    description=report_config["description"],
                    latest_href=report_config["latest_href"],
                    archive_href=_standard_archive_href(
                        report_date,
                        str(report_config.get("archive_segment", report_type)),
                    ),
                ),
                is_public,
            )
            break
        else:
            workflow_report = _collect_dedicated_workflow_report(
                date=report_date,
                report_name=report_name,
                report_path=report_path,
            )
            if workflow_report is not None:
                _store_report(
                    collected,
                    report_date,
                    workflow_report.report_key,
                    workflow_report,
                    is_public,
                )

    return {
        report_date: {
            report_key: report
            for report_key, (_, report) in sorted(report_map.items())
        }
        for report_date, report_map in sorted(collected.items(), reverse=True)
    }


def build_pages_site(reports_dir: Path, output_dir: Path) -> None:
    reports_by_date = collect_reports(reports_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not reports_by_date:
        raise ValueError(f"No dashboard HTML files were found in {reports_dir}")

    output_dir.joinpath(".nojekyll").write_text("", encoding="utf-8")

    latest_reports = _copy_reports(reports_by_date, output_dir)
    write_index_page(output_dir, reports_by_date, latest_reports)
    write_archive_index_page(output_dir, reports_by_date)


def _copy_reports(
    reports_by_date: dict[str, dict[str, DashboardReport]],
    output_dir: Path,
) -> dict[str, DashboardReport]:
    latest_reports: dict[str, DashboardReport] = {}

    for reports in reports_by_date.values():
        for report in reports.values():
            archive_target = output_dir / report.archive_href / "index.html"
            archive_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(report.source_path, archive_target)

            if report.report_key not in latest_reports:
                latest_target = output_dir / report.latest_href / "index.html"
                latest_target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(report.source_path, latest_target)
                latest_reports[report.report_key] = report

    return latest_reports


def write_index_page(
    output_dir: Path,
    reports_by_date: dict[str, dict[str, DashboardReport]],
    latest_reports: dict[str, DashboardReport],
) -> None:
    latest_date = next(iter(reports_by_date))
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    latest_cards = []
    for report in _ordered_reports(latest_reports):
        latest_cards.append(
            f"""
      <article class="card">
        <p class="eyebrow">Latest {escape(report.label)}</p>
        <h2><a href="{escape(report.latest_href)}">{escape(report.date)}</a></h2>
        <p>{escape(report.description)}</p>
      </article>
"""
        )

    archive_rows = []
    for date, reports in reports_by_date.items():
        links = []
        for report in _ordered_reports(reports):
            links.append(f'<a href="{escape(report.archive_href)}">{escape(report.label)}</a>')
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
  <title>{SITE_TITLE}</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #17324d;
      --muted: #5e6f83;
      --line: #d7e1ec;
      --panel: #ffffff;
      --panel-soft: #f5f8fb;
      --bg: #e9f0f6;
      --accent: #1f6fb2;
      --shadow: rgba(23, 50, 77, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", sans-serif;
      line-height: 1.5;
      color: var(--ink);
      background: linear-gradient(180deg, #f8fbfd 0%, var(--bg) 100%);
    }}
    main {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 2rem 1rem 3rem;
    }}
    h1, h2, p {{ margin-top: 0; }}
    a {{ color: var(--accent); }}
    .hero,
    .card,
    .archive {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 14px 30px var(--shadow);
    }}
    .hero {{
      padding: 1.5rem;
      margin-bottom: 1.5rem;
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
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 1rem;
      margin-bottom: 1.5rem;
    }}
    .card {{
      padding: 1.2rem;
    }}
    .card h2 {{
      margin-bottom: 0.4rem;
    }}
    .archive {{
      overflow: hidden;
    }}
    .archive-header {{
      padding: 1rem 1.25rem;
      border-bottom: 1px solid var(--line);
      background: var(--panel-soft);
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
    footer {{
      margin-top: 1rem;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    @media (max-width: 700px) {{
      main {{
        padding: 1.25rem 0.75rem 2rem;
      }}
      th,
      td {{
        padding: 0.75rem;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <p class="eyebrow">GitHub Pages</p>
      <h1>{SITE_TITLE}</h1>
      <p>This site publishes the committed dashboard HTML files from <code>reports/</code> in a stable, human-readable layout.</p>
      <p>The latest published report date is <strong>{escape(latest_date)}</strong>. The site index was generated at {escape(generated_at)}.</p>
      <p><a href="archive/">Browse the archive index</a></p>
    </section>

    <section class="cards">
{''.join(latest_cards)}
    </section>

    <section class="archive">
      <div class="archive-header">
        <p class="eyebrow">Archive</p>
        <h2>Published Report Dates</h2>
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
      --panel: #ffffff;
      --panel-soft: #f5f8fb;
      --bg: #e9f0f6;
    }}
    body {{
      margin: 0;
      font-family: "Segoe UI", sans-serif;
      line-height: 1.5;
      color: var(--ink);
      background: linear-gradient(180deg, #f8fbfd 0%, var(--bg) 100%);
    }}
    main {{
      max-width: 960px;
      margin: 0 auto;
      padding: 2rem 1rem 3rem;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      overflow: hidden;
    }}
    .panel-header {{
      padding: 1rem 1.25rem;
      border-bottom: 1px solid var(--line);
      background: var(--panel-soft);
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


def _standard_archive_href(report_date: str, archive_segment: str) -> str:
    if not archive_segment:
        return f"{report_date}/"
    return f"{report_date}/{archive_segment}/"


def _normalize_report_name(report_name: str) -> tuple[str, bool]:
    public_suffix = f"{PUBLIC_REPORT_SUFFIX}.html"
    if not report_name.endswith(public_suffix):
        return report_name, False
    return f"{report_name.removesuffix(public_suffix)}.html", True


def _store_report(
    collected: dict[str, dict[str, tuple[bool, DashboardReport]]],
    report_date: str,
    report_key: str,
    report: DashboardReport,
    is_public: bool,
) -> None:
    existing = collected.setdefault(report_date, {}).get(report_key)
    if existing is not None and existing[0] and not is_public:
        return
    collected[report_date][report_key] = (is_public, report)


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
        latest_href=f"latest/workflows/{workflow_slug}/",
        archive_href=f"{date}/workflows/{workflow_slug}/",
    )


def _extract_html_title(report_path: Path) -> str:
    html = report_path.read_text(encoding="utf-8", errors="ignore")
    match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if match is None:
        return ""
    return re.sub(r"\s+", " ", match.group(1)).strip()


def _workflow_report_label(workflow_title: str, workflow_slug: str) -> str:
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
    return "Dedicated workflow report."


def _ordered_reports(reports: dict[str, DashboardReport]) -> list[DashboardReport]:
    return sorted(reports.values(), key=lambda report: report.sort_order())


if __name__ == "__main__":
    main()
