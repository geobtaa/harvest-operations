#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
from io import StringIO
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable


SCRIPT_PATH = Path(__file__).resolve()
DEFAULT_CSV_PATH = SCRIPT_PATH.parents[1] / "codework" / "harvest-records.csv"
DEFAULT_REPORTS_DIR = SCRIPT_PATH.parents[1] / "reports"

PERIODICITY_ALIASES = {
    "weekly": "weekly",
    "monthy": "monthly",
    "monthly": "monthly",
    "quarterly": "quarterly",
    "annual": "annual",
    "annually": "annual",
    "yearly": "annual",
    "irregular": "irregular",
}

PERIODICITY_DAY_WINDOWS = {
    "weekly": 7,
}

PERIODICITY_MONTH_WINDOWS = {
    "monthly": 1,
    "quarterly": 3,
    "annual": 12,
}

CONSOLIDATED_WORKFLOWS = {
    "py_arcgis_hub",
}


@dataclass(frozen=True)
class Record:
    code: str
    title: str
    workflow: str
    periodicity: str
    normalized_periodicity: str
    last_harvested: date | None
    source_row: dict[str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze harvest records and generate a workflow schedule based on "
            "Last Harvested and Accrual Periodicity."
        )
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=str(DEFAULT_CSV_PATH),
        help="Path to the harvest records CSV file.",
    )
    parser.add_argument(
        "--today",
        default=None,
        help="Override today's date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional report output path. Defaults to reports/triage-harvest-records-YYYY-MM-DD.txt.",
    )
    return parser.parse_args()


def parse_date(raw_value: str) -> date | None:
    value = raw_value.strip()
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def normalize_periodicity(raw_value: str) -> str:
    value = raw_value.strip().lower()
    return PERIODICITY_ALIASES.get(value, value or "unknown")


def add_months(base_date: date, months: int) -> date:
    month_index = base_date.month - 1 + months
    year = base_date.year + month_index // 12
    month = month_index % 12 + 1
    last_day = (
        date(year + (month == 12), 1 if month == 12 else month + 1, 1) - timedelta(days=1)
    ).day
    day = min(base_date.day, last_day)
    return date(year, month, day)


def next_due_date(record: Record) -> date | None:
    if not record.last_harvested:
        return None
    if record.normalized_periodicity in PERIODICITY_DAY_WINDOWS:
        return record.last_harvested + timedelta(days=PERIODICITY_DAY_WINDOWS[record.normalized_periodicity])
    if record.normalized_periodicity in PERIODICITY_MONTH_WINDOWS:
        return add_months(record.last_harvested, PERIODICITY_MONTH_WINDOWS[record.normalized_periodicity])
    return None


def load_records(csv_path: Path) -> list[Record]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return [
            Record(
                code=(row.get("Code") or "").strip(),
                title=(row.get("Title") or "").strip(),
                workflow=(row.get("Harvest Workflow") or "").strip(),
                periodicity=(row.get("Accrual Periodicity") or "").strip(),
                normalized_periodicity=normalize_periodicity(row.get("Accrual Periodicity") or ""),
                last_harvested=parse_date(row.get("Last Harvested") or ""),
                source_row=row,
            )
            for row in reader
        ]


def format_date(value: date | None) -> str:
    return value.isoformat() if value else "N/A"


def format_record_line(record: Record, due_date: date | None = None, today_value: date | None = None) -> str:
    parts = [
        record.code or "<no code>",
        record.workflow or "<no workflow>",
        record.periodicity or "<no periodicity>",
        f"last={format_date(record.last_harvested)}",
    ]
    if due_date:
        parts.append(f"due={due_date.isoformat()}")
    if due_date and today_value:
        delta = (due_date - today_value).days
        if delta < 0:
            parts.append(f"{abs(delta)} day(s) overdue")
        elif delta == 0:
            parts.append("due today")
        else:
            parts.append(f"due in {delta} day(s)")
    return " | ".join(parts + [record.title])


def format_delta(due_date: date, today_value: date) -> str:
    delta = (due_date - today_value).days
    if delta < 0:
        return f"{abs(delta)} day(s) overdue"
    if delta == 0:
        return "due today"
    return f"due in {delta} day(s)"


def format_consolidated_task(
    workflow: str,
    items: list[tuple[date, Record]],
    today_value: date,
    label: str,
) -> str:
    due_dates = sorted({due_date for due_date, _ in items})
    last_dates = sorted({record.last_harvested for _, record in items if record.last_harvested is not None})
    record_count = len(items)
    code_preview = ", ".join(record.code for _, record in items[:5])
    if record_count > 5:
        code_preview = f"{code_preview}, ..."

    parts = [
        workflow,
        label,
        f"{record_count} record(s)",
    ]
    if last_dates:
        if len(last_dates) == 1:
            parts.append(f"last={last_dates[0].isoformat()}")
        else:
            parts.append(f"last={last_dates[0].isoformat()}..{last_dates[-1].isoformat()}")
    if due_dates:
        if len(due_dates) == 1:
            parts.append(f"due={due_dates[0].isoformat()}")
            parts.append(format_delta(due_dates[0], today_value))
        else:
            parts.append(f"due={due_dates[0].isoformat()}..{due_dates[-1].isoformat()}")
    parts.append(f"codes={code_preview}")
    return " | ".join(parts)


def format_task_lines(
    records_with_dates: list[tuple[date, Record]],
    today_value: date,
    label: str,
) -> list[str]:
    consolidated: dict[str, list[tuple[date, Record]]] = defaultdict(list)
    lines: list[str] = []

    for due_date, record in records_with_dates:
        workflow = record.workflow or "<no workflow>"
        if workflow in CONSOLIDATED_WORKFLOWS:
            consolidated[workflow].append((due_date, record))
            continue
        lines.append(format_record_line(record, due_date=due_date, today_value=today_value))

    consolidated_lines = [
        format_consolidated_task(workflow, items, today_value, label)
        for workflow, items in sorted(consolidated.items())
    ]
    return consolidated_lines + lines


def print_section(title: str, lines: Iterable[str], output: StringIO) -> None:
    rendered_lines = list(lines)
    print(title, file=output)
    if not rendered_lines:
        print("  None", file=output)
    else:
        for line in rendered_lines:
            print(f"  {line}", file=output)
    print(file=output)


def build_report(records: list[Record], csv_path: Path, today_value: date) -> str:
    output = StringIO()

    due_records: list[tuple[date, Record]] = []
    upcoming_records: list[tuple[date, Record]] = []
    irregular_records: list[Record] = []
    unscheduled_records: list[Record] = []
    unsupported_records: list[Record] = []

    for record in records:
        due_date = next_due_date(record)
        if record.normalized_periodicity == "irregular":
            irregular_records.append(record)
            continue
        if record.normalized_periodicity == "unknown":
            unsupported_records.append(record)
            continue
        if not record.last_harvested:
            unscheduled_records.append(record)
            continue
        if due_date is None:
            unsupported_records.append(record)
            continue
        if due_date <= today_value:
            due_records.append((due_date, record))
        else:
            upcoming_records.append((due_date, record))

    due_records.sort(key=lambda item: (item[0], item[1].workflow, item[1].code))
    upcoming_records.sort(key=lambda item: (item[0], item[1].workflow, item[1].code))
    irregular_records.sort(key=lambda record: (record.workflow, record.code))
    unscheduled_records.sort(key=lambda record: (record.workflow, record.code))
    unsupported_records.sort(key=lambda record: (record.workflow, record.code))

    due_by_workflow: dict[str, list[tuple[date, Record]]] = defaultdict(list)
    for due_date, record in due_records:
        due_by_workflow[record.workflow or "<no workflow>"].append((due_date, record))

    upcoming_schedule: dict[tuple[date, str], list[Record]] = defaultdict(list)
    for due_date, record in upcoming_records:
        upcoming_schedule[(due_date, record.workflow or "<no workflow>")].append(record)

    periodicity_counts = Counter(record.normalized_periodicity for record in records)
    workflow_counts = Counter(record.workflow or "<no workflow>" for record in records)

    print("Harvest Workflow Triage Report", file=output)
    print(f"CSV: {csv_path}", file=output)
    print(f"Today: {today_value.isoformat()}", file=output)
    print(f"Total records: {len(records)}", file=output)
    print(file=output)

    print_section(
        "Summary",
        [
            "Periodicity counts: "
            + ", ".join(f"{key}={value}" for key, value in sorted(periodicity_counts.items())),
            "Workflow counts: "
            + ", ".join(f"{key}={value}" for key, value in sorted(workflow_counts.items())),
            f"Due now or overdue: {len(due_records)}",
            f"Scheduled for future: {len(upcoming_records)}",
            f"Irregular review queue: {len(irregular_records)}",
            f"Missing Last Harvested: {len(unscheduled_records)}",
            f"Unsupported periodicity: {len(unsupported_records)}",
        ],
        output,
    )

    print_section(
        "Workflows To Run Now",
        [
            f"{workflow}: {len(items)} record(s)"
            for workflow, items in sorted(
                due_by_workflow.items(), key=lambda item: (-len(item[1]), item[0])
            )
        ],
        output,
    )

    print_section(
        "Due Records",
        format_task_lines(due_records, today_value, "single run"),
        output,
    )

    print_section(
        "Upcoming Schedule",
        [
            f"{due_date.isoformat()} | {workflow} | {len(records_for_slot)} record(s)"
            for (due_date, workflow), records_for_slot in sorted(upcoming_schedule.items())
        ],
        output,
    )

    print_section(
        "Upcoming Record Details",
        format_task_lines(upcoming_records, today_value, "single scheduled run"),
        output,
    )

    print_section(
        "Irregular Review Queue",
        [format_record_line(record) for record in irregular_records],
        output,
    )

    print_section(
        "Missing Last Harvested",
        [format_record_line(record) for record in unscheduled_records],
        output,
    )

    print_section(
        "Unsupported Periodicity",
        [format_record_line(record) for record in unsupported_records],
        output,
    )

    return output.getvalue()


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv_path).expanduser().resolve()
    today_value = parse_date(args.today) if args.today else date.today()
    records = load_records(csv_path)
    report_text = build_report(records, csv_path, today_value)
    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else DEFAULT_REPORTS_DIR / f"triage-harvest-records-{today_value.isoformat()}.txt"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report_text, encoding="utf-8")

    print(f"Report written to {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
