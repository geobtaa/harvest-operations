import os
import json
import re
import logging

import pandas as pd

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table


class OgmWiscHarvester(BaseHarvester):
    def __init__(self, config):
        super().__init__(config)
        self.json_path = self.config.get("json_path")
        self.county_lookup = {} 

    def load_reference_data(self):
        super().load_reference_data()  # Sets self.distribution_types
        counties_path = "reference_data/spatial_counties.csv"
        counties_df = pd.read_csv(counties_path, sep="\t" if "\t" in open(counties_path).readline() else ",")
        counties_df = counties_df.dropna(subset=["County"])

        self.county_lookup = {
            row["County"].split("--")[-1].replace(" County", "").strip(): {
                "full_name": row["County"],
                "geometry": row.get("Geometry", ""),
                "geonames": row.get("GeoNames", "")
            }
            for _, row in counties_df.iterrows()
        }

    def fetch(self):
        """
        Traverse the configured directory and load all .json files into a list of dictionaries.
        Ignores invalid JSON files and logs warnings.
        """
        dataset = []
        for root, _, files in os.walk(self.json_path):
            for filename in files:
                if filename.lower().endswith(".json"):
                    file_path = os.path.join(root, filename)
                    try:
                        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                            record = json.load(f)
                            dataset.append(record)
                    except json.JSONDecodeError as e:
                        logging.warning(f"[OGMWisc] Failed to parse JSON at {file_path}: {e}")
        return dataset

    def flatten(self, harvested_metadata):
        """
        Expands each record by parsing dct_references_s and adding one column per 'variable'
        defined in distribution_types.yaml. Columns are named according to those variables.
        """
        flattened = []

        # Build lookup: reference_uri → list of variable names
        uri_to_vars = {}
        for dist in self.distribution_types:
            uri = dist.get("reference_uri")
            variables = dist.get("variables", [])
            if uri:
                uri_to_vars[uri] = variables

        for rec in harvested_metadata:
            new_record = rec.copy()

            raw_refs = rec.get("dct_references_s")
            if isinstance(raw_refs, str):
                try:
                    references = json.loads(raw_refs.replace('""', '"'))  # handle malformed double quotes
                    for ref_uri, url in references.items():
                        for var in uri_to_vars.get(ref_uri, []):
                            new_record[var] = url
                except json.JSONDecodeError as e:
                    logging.warning(f"[OGMWisc] Invalid JSON in dct_references_s for record {rec.get('layer_slug_s')}: {e}")

            flattened.append(new_record)

        return flattened
    
    def build_dataframe(self, records):
        """
        Converts a list of UW-Madison GBL 1.0 records into a cleaned DataFrame,
        with renamed and normalized fields according to the GeoBTAA schema.
        """
        df = pd.DataFrame(records)

        # --- Normalize multivalued fields ---
        multivalue_fields = [
            'dc_creator_sm', 'dc_subject_sm', 'dct_spatial_sm',
            'dct_isPartOf_sm', 'dct_temporal_sm'
        ]
        for col in multivalue_fields:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: '|'.join(x) if isinstance(x, list) else x)


        # --- Rename to Aardvark/GeoBTAA field names ---
        rename_map = {
            'dc_title_s': 'Title',
            'dc_description_s': 'Description',
            'dc_creator_sm': 'Creator',
            'dct_issued_s': 'Date Issued',
            'dc_rights_s': 'Access Rights',
            'dc_format_s': 'Format',
            'layer_slug_s': 'ID',
            'layer_id_s': 'WxS Identifier',
            'dct_provenance_s': 'Provider',
            'dc_publisher_s': 'Publisher',
            'dc_publisher_sm': 'Publisher',  # sometimes multivalued
            'dct_temporal_sm': 'Temporal Coverage',
            'dct_isPartOf_sm': 'Local Collection',
            'dc_subject_sm': 'Subject',
            'uw_deprioritize_item_b': 'Child Record',
            'thumbnail_path_ss': 'B1G Image'
        }

        df = df.rename(columns=rename_map)

        return df
    
    
    def derive_fields(self, df):
        df = super().add_defaults(df)
        df = (
            df
            .pipe(self.ogmWisc_format_temporal_coverage)
            .pipe(self.ogmWisc_flag_georeferenced)
            .pipe(self.ogmWisc_generate_identifier)
            .pipe(self.ogmWisc_reorder_bbox)
            .pipe(self.ogmWisc_map_theme_from_subject)
            .pipe(self.ogmWisc_build_display_note)
            .pipe(self.ogmWisc_add_resource_class)
            .pipe(self.ogmWisc_add_resource_type)
            .pipe(self.ogmWisc_clean_creator_values)
        )
        return df
    
    def add_defaults(self, df):
        df['Code'] = "10"
        df['Is Part Of'] = "10d-03"
        df['Member Of'] = "dc8c18df-7d64-4ff4-a754-d18d0891187d"
        df['Language'] = "eng"
        df['Spatial Coverage'] = "Wisconsin"
        return df
    
    def add_provenance(self, df):
        df = super().add_provenance(df)
        df['Accrual Method'] = 'Mediated deposit'
        df["Website Platform"] = "GeoBlacklight"
        df["Harvest Workflow"] = "py_ogm_wisc"
        df["Endpoint Description"] = "GitHub"
        df["Endpoint URL"] = "https://github.com/opengeometadata/edu.wisc"
        return df
    
    def clean(self, df):
        df = super().clean(df)
        return df

    def validate(self, df):
        df = super().validate(df)
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        return super().write_outputs(primary_df, distributions_df)


# --- OGM Wisconsin-Specific Field Derive Functions ---

    def ogmWisc_format_temporal_coverage(self, df):
        def format_temporal(temporal):
            if pd.notna(temporal) and re.match(r'\d{4}-\d{4}', str(temporal)):
                return temporal
            if pd.notna(temporal):
                return f"{temporal}-{temporal}"
            return ''
        if 'Temporal Coverage' in df.columns:
            df['Date Range'] = df['Temporal Coverage'].apply(format_temporal)
        return df


    def ogmWisc_flag_georeferenced(self, df):
        if 'Format' in df.columns:
            df['Georeferenced'] = df['Format'].apply(lambda x: "true" if pd.notna(x) and "GeoTIFF" in x else "false")
        return df


    def ogmWisc_generate_identifier(self, df):
        if 'ID' in df.columns:
            df['Identifier'] = "https://geodata.wisc.edu/catalog/" + df['ID']
        return df


    def ogmWisc_reorder_bbox(self, df):
        """
        Extracts and reorders solr_geom ENVELOPE(w, e, n, s) to 'Bounding Box' as w,s,e,n.
        """
        def extract_bbox(val):
            if val.startswith("ENVELOPE(") and val.endswith(")"):
                try:
                    coords = val[len("ENVELOPE("):-1].split(",")
                    w, e, n, s = [c.strip() for c in coords]
                    return f"{w},{s},{e},{n}"
                except Exception:
                    return None
            return None

        df["Bounding Box"] = df["solr_geom"].apply(lambda x: extract_bbox(x) if isinstance(x, str) else None)
        return df


    def ogmWisc_map_theme_from_subject(self, df):
        theme_map = {
            "Farming": "Agriculture",
            "Biota": "Biology",
            "Atmospheric Sciences": "Climate",
            "Geoscientific Information": "Geology",
            "Imagery and Base Maps": "Imagery",
            "Planning and Cadastral": "Property",
            "Utilities and Communication": "Utilities"
        }

        def map_theme_multivalued(subject):
            if not isinstance(subject, str) or subject.strip() == "":
                return subject
            parts = subject.split("|")
            mapped = [theme_map.get(p.strip(), p.strip()) for p in parts]
            return "|".join(mapped)

        if 'Subject' in df.columns:
            df['Theme'] = df['Subject'].apply(map_theme_multivalued)
        return df


    def ogmWisc_build_display_note(self, df):
        def map_display_note(notice, supplemental):
            parts = []
            if isinstance(notice, str) and notice.strip():
                parts.append(notice.strip())
            if isinstance(supplemental, str) and supplemental.strip():
                parts.append(f"Info: {supplemental.strip()}")
            return "|".join(parts) if parts else ""

        if 'uw_notice_s' in df.columns or 'uw_supplemental_s' in df.columns:
            df['Display Note'] = [
                map_display_note(n, s)
                for n, s in zip(df.get('uw_notice_s', []), df.get('uw_supplemental_s', []))
            ]
        return df


    def ogmWisc_add_resource_class(self, df):
        if 'dc_type_s' in df.columns:
            df['Resource Class'] = df['dc_type_s'].apply(lambda x: 'Imagery' if x == 'Image' else 'Datasets')
        return df


    def ogmWisc_add_resource_type(self, df):
        if 'layer_geom_type_s' in df.columns:
            df['Resource Type'] = df['layer_geom_type_s'].astype(str) + " data"
        return df

    def ogmWisc_clean_creator_values(self, df):
        """
        Clean the 'Creator' column in the dataframe by:
        - Removing HTML tags and extra characters
        - Normalizing Wisconsin county and city names
        - Keeping original value if no match is found
        - Appending Geometry and GeoNames values only where matched
        """
        # Load only Wisconsin counties
        counties_df = pd.read_csv("reference_data/spatial_counties.csv", encoding="utf-8", dtype=str)
        wisconsin_df = counties_df[counties_df["County"].str.startswith("Wisconsin--")].copy()

        # Create lookup dicts
        wisconsin_df["base_name"] = wisconsin_df["County"].str.replace("Wisconsin--", "").str.replace(" County", "")
        county_lookup = dict(zip(wisconsin_df["base_name"], wisconsin_df["County"]))
        geom_lookup = dict(zip(wisconsin_df["County"], wisconsin_df["Geometry"]))
        geonames_lookup = dict(zip(wisconsin_df["County"], wisconsin_df["GeoNames"]))

        def normalize_creator(value):
            if not isinstance(value, str) or not value.strip():
                return value  # Leave blank or non-string as-is

            # Strip unwanted characters
            text = value.strip().strip("|- ")

            # County match (e.g., "Adams County")
            if text.endswith(" County"):
                base = text[:-len(" County")]
                if base in county_lookup:
                    return county_lookup[base]

            # City match (e.g., "City of Fitchburg")
            if text.startswith("City of "):
                city_name = text.replace("City of ", "", 1).strip()
                return f"Wisconsin--{city_name}"

            # Return original if no match
            return text

        # Normalize Creator column
        df["Creator"] = df["Creator"].apply(normalize_creator)

        # Only apply Geometry and GeoNames to matched counties
        def get_field_or_blank(row, lookup):
            val = row.get("Creator", "")
            return lookup.get(val, "")

        df["Geometry"] = df.apply(lambda row: get_field_or_blank(row, geom_lookup), axis=1)
        df["GeoNames"] = df.apply(lambda row: get_field_or_blank(row, geonames_lookup), axis=1)

        return df


