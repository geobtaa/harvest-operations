import re

def infer_temporal_coverage_from_title(row: dict) -> str:
    """
    Infer Temporal Coverage from the Alternative Title or Date Modified.

    Looks for a 4-digit year or year range in the title. If not found, falls back to Date Modified.
    """
    title = row.get('Alternative Title', '') or ''
    modified = row.get('Date Modified', '') or ''
    temporal_coverage = ''

    # Match year or year range, e.g. 1995 or 1995-2000 or 1995 – 2000
    title_match = re.search(r"\b(19\d{2}|20\d{2})(?:\s*[-–—]\s*(19\d{2}|20\d{2}))?\b", title)

    if title_match:
        if title_match.group(2):  # range
            temporal_coverage = f"{title_match.group(1)}-{title_match.group(2)}"
        else:  # single year
            temporal_coverage = title_match.group(1)
    elif modified:
        temporal_coverage = f"Last Modified: {modified.strip()}"

    return temporal_coverage


def create_date_range(row: dict, temporal_coverage: str) -> str:
    """
    Create Date Range from Temporal Coverage or fallback to Date Modified or Issued.

    Logic:
    - If Temporal Coverage is a year or range → Date Range = yyyy-yyyy
    - Else if Temporal Coverage is "Last Modified: yyyy-mm-dd" → Date Range = yyyy-yyyy
    - Else if Date Issued is valid → Date Range = yyyy-yyyy
    - Else → Date Range = ""
    """
    issued = row.get('Date Issued', '') or ''
    modified = row.get('Date Modified', '') or ''
    date_range = ''

    if re.match(r"^(19|20)\d{2}(-\d{4})?$", temporal_coverage):
        if "-" in temporal_coverage:
            date_range = temporal_coverage
        else:
            date_range = f"{temporal_coverage}-{temporal_coverage}"
    elif temporal_coverage.startswith("Last Modified:"):
        mod_year = modified[:4]
        if mod_year.isdigit():
            date_range = f"{mod_year}-{mod_year}"
    elif issued:
        issued_year = issued[:4]
        if issued_year.isdigit():
            date_range = f"{issued_year}-{issued_year}"

    return date_range