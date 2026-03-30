import re
import pandas as pd

def format_title_with_brackets(alternative_title: str, bracket_value: str = '', place_from_creator: str = None) -> str:
    """
    Clean and format an alternative title by:
    - Removing a known place name from the beginning
    - Removing any existing brackets from the title
    - Capitalizing the first letter
    - Appending the bracketed value (e.g., place or creator)

    Returns:
        str: Formatted title
    """
    alt = alternative_title.strip()

    # Step 1: Remove known place name from the beginning
    if place_from_creator:
        pattern = re.compile(rf"^{re.escape(place_from_creator)}\s*[-:|>]*\s*", re.IGNORECASE)
        alt = pattern.sub('', alt).strip()

    # Step 2: Remove stray brackets or punctuation
    alt = re.sub(r'\[.*?\]', '', alt).strip()
    alt = alt.strip(" ,-:|/")

    # Step 3: Capitalize first letter
    if alt:
        alt = alt[0].capitalize() + alt[1:]

    # Step 4: Append bracketed value
    title = f"{alt} [{bracket_value}]" if bracket_value else alt

    return title

def append_temporal_coverage_to_title(title: str, temporal_coverage: str) -> str:
    """
    Appends the Temporal Coverage to the title in curly brackets, e.g.:
    "Land use map [Illinois]" + "1995-2000" → "Land use map [Illinois] {1995-2000}"

    If Temporal Coverage is empty or None, returns the title unchanged.
    """
    if isinstance(temporal_coverage, str) and temporal_coverage.strip():
        return f"{title} {{{temporal_coverage.strip()}}}"
    return title

def title_wizard(df):
    """
    Formats the Title field using Alternative Title, Spatial Coverage, and Creator.
    Appends Temporal Coverage in curly braces if present.
    """
    def _build_title(row):
        alt = row.get('Alternative Title', '')
        spatial = row.get('Spatial Coverage', '').split('|')[0].strip()
        creator = row.get('Creator', '').strip()
        temporal_coverage = row.get('Temporal Coverage', '')

        # Infer place from Creator if it's Pennsylvania-based
        place = ''
        if creator.startswith("Pennsylvania--"):
            place = creator.replace("Pennsylvania--", "").strip()

        title = format_title_with_brackets(
            alternative_title=alt,
            bracket_value=spatial,
            place_from_creator=place
        )

        return append_temporal_coverage_to_title(title, temporal_coverage)

    # Apply row-wise and replace the Title column
    df["Title"] = df.apply(_build_title, axis=1)
    return df

