from datetime import date

from scripts.sync_issue_project_iteration import (
    IterationOption,
    due_month_title,
    extract_due_date,
    find_matching_iteration_id,
    find_project_item_id,
    parse_iteration_options,
)


def test_extract_due_date_reads_iso_due_date_line() -> None:
    issue_body = """
# [Harvest Due] ORNL LandScan Viewer

- Harvest record: [123](https://example.com/123)
- Identifier: [04a-01](https://example.com/04a-01)
- Due date: 2026-04-18
- Last harvested: 2026-03-20
"""

    assert extract_due_date(issue_body) == date(2026, 4, 18)


def test_extract_due_date_returns_none_when_no_iso_date_is_present() -> None:
    issue_body = "- Due date: No schedule"

    assert extract_due_date(issue_body) is None


def test_due_month_title_formats_month_and_year() -> None:
    assert due_month_title(date(2026, 4, 18)) == "April 2026"


def test_parse_iteration_options_skips_invalid_values() -> None:
    raw_iterations = [
        {"id": "iter-valid", "title": "April 2026", "startDate": "2026-04-01", "duration": 30},
        {"id": "iter-invalid", "title": "Broken", "startDate": "not-a-date", "duration": 30},
    ]

    assert parse_iteration_options(raw_iterations) == [
        IterationOption(
            id="iter-valid",
            title="April 2026",
            start_date=date(2026, 4, 1),
            duration=30,
        )
    ]


def test_find_matching_iteration_id_prefers_title_match() -> None:
    iterations = [
        IterationOption("iter-1", "April 2026", date(2026, 4, 7), 14),
        IterationOption("iter-2", "Late April 2026", date(2026, 4, 21), 14),
    ]

    assert find_matching_iteration_id(iterations, date(2026, 4, 18)) == "iter-1"


def test_find_matching_iteration_id_falls_back_to_date_range() -> None:
    iterations = [
        IterationOption("iter-1", "Sprint 1", date(2026, 4, 1), 30),
        IterationOption("iter-2", "Sprint 2", date(2026, 5, 1), 31),
    ]

    assert find_matching_iteration_id(iterations, date(2026, 4, 18)) == "iter-1"


def test_find_project_item_id_matches_org_project() -> None:
    issue_data = {
        "projectItems": {
            "nodes": [
                {
                    "id": "PVTI_other",
                    "project": {
                        "number": 9,
                        "owner": {"__typename": "Organization", "login": "geobtaa"},
                    },
                },
                {
                    "id": "PVTI_target",
                    "project": {
                        "number": 4,
                        "owner": {"__typename": "Organization", "login": "geobtaa"},
                    },
                },
            ]
        }
    }

    assert find_project_item_id(issue_data, "geobtaa", 4) == "PVTI_target"
