import pandas as pd
import re

import pandas as pd
import re

def creator_match(df, state: str, county_data_path: str = "reference_data/spatial_counties.csv"):
    """
    Clean and enrich the 'Creator' field based on county and city matches for a given U.S. state.
    Adds Geometry, GeoNames, and Bounding Box if missing and available in the reference sheet.

    Args:
        df (pd.DataFrame): The dataframe containing a 'Creator' column.
        state (str): U.S. state name, e.g., "Wisconsin".
        county_data_path (str): Path to spatial_counties.csv with County, Geometry, GeoNames, Bounding Box.

    Returns:
        pd.DataFrame: Modified dataframe with enriched 'Creator', 'Geometry', 'GeoNames', and 'Bounding Box'.
    """
    # Load reference data for the specified state
    counties_df = pd.read_csv(county_data_path, encoding="utf-8", dtype=str)
    prefix = f"{state}--"
    state_df = counties_df[counties_df["County"].str.startswith(prefix)].copy()

    # Build lookup dictionaries
    state_df["base_name"] = state_df["County"].str.replace(prefix, "").str.replace(" County", "")
    county_lookup = dict(zip(state_df["base_name"], state_df["County"]))
    geom_lookup = dict(zip(state_df["County"], state_df["Geometry"]))
    geonames_lookup = dict(zip(state_df["County"], state_df["GeoNames"]))
    bbox_lookup = dict(zip(state_df["County"], state_df["Bounding Box"]))

    # Clean and normalize Creator field
    def normalize_creator(value):
        if not isinstance(value, str) or not value.strip():
            return value  # return blank or None unchanged

        text = re.sub(r'<[^>]+>', '', value).strip().strip("|- ")

        # Match counties
        if text.endswith(" County"):
            base = text[:-len(" County")].strip()
            if base in county_lookup:
                return county_lookup[base]
            else:
                return value  # ← leave original unchanged if not matched

        # Match "city of"
        if text.startswith("City of "):
            city_name = text.replace("City of ", "", 1).strip()
            return f"{prefix}{city_name}"

        return value  # ← unchanged if no rule matched
    
    df["Creator"] = df["Creator"].apply(normalize_creator)

    # Helper to safely look up values
    def get_field_or_blank(row, lookup):
        return lookup.get(row.get("Creator", ""), "")

    # Enrich with Geometry and GeoNames
    df["Geometry"] = df.apply(lambda row: get_field_or_blank(row, geom_lookup), axis=1)
    df["GeoNames"] = df.apply(lambda row: get_field_or_blank(row, geonames_lookup), axis=1)

    # Fill Bounding Box only if missing or blank
    if "Bounding Box" not in df.columns:
        df["Bounding Box"] = ""

    df["Bounding Box"] = df.apply(
        lambda row: row["Bounding Box"] if row["Bounding Box"] else get_field_or_blank(row, bbox_lookup),
        axis=1
    )

    return df

