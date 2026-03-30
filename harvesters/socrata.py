import csv
import time
import os
import re

import requests
import pandas as pd

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table
from utils.temporal_fields import infer_temporal_coverage_from_title, create_date_range


class SocrataHarvester(BaseHarvester):
    def __init__(self, config):
        super().__init__(config)

    def load_reference_data(self):
        super().load_reference_data()

    def fetch(self):
        website_list = self.config.get("input_csv")
        with open(website_list, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                website_id = row['ID']
                endpoint_url = row['Endpoint URL']
                try:
                    resp = requests.get(endpoint_url, timeout=30)
                    resp.raise_for_status()
                    json_api = resp.json()
                except Exception as e:
                    yield f"[Socrata] Error fetching {website_id}: {e}"
                    continue

                row['fetched_catalog'] = json_api
                yield f"[Socrata] Fetched {website_id} — {row.get('Title', 'No Title')}"
                yield row


    def flatten(self, harvested_records):

        flattened_list = []

        for website_record in harvested_records:
            if not isinstance(website_record, dict):
                continue

            # Extract the list of datasets from within the fetched catalog
            resources = website_record.get("fetched_catalog", {}).get("dataset", [])
            
            # Creates a new, combined record for each individual dataset
            for resource in resources:
                flattened_list.append({
                    "website": website_record,  # The complete record for the parent hub
                    "resource": resource      # The record for each dataset
                })

        return flattened_list

    def build_dataframe(self, flattened_items):

        df = pd.DataFrame(flattened_items)

        df = (
            df.pipe(self.socrata_filter_rows)
            .pipe(self.socrata_map_to_schema)
        )

        return df


    def derive_fields(self, df):
        df = super().derive_fields(df)
        
        df = (
            df.pipe(self.socrata_parse_identifiers)
            .pipe(self.socrata_temporal_coverage)
            .pipe(self.socrata_format_date_ranges)
            .pipe(self.socrata_reformat_titles)
            .pipe(self.socrata_clean_creator_values)
            .pipe(self.socrata_set_resource_type)
            .pipe(self.socrata_derive_geojson)
        )

        return df

    def add_defaults(self, df):
        df = super().add_defaults(df)

        df['Display Note'] = "Tip: Check “Visit Source” link for download options."
        df['Language'] = 'eng'
        df['Resource Class'] = 'Web services'

        return df
    
    def add_provenance(self, df: pd.DataFrame) -> pd.DataFrame:
        # ---------- inherited defaults ----------
        df = super().add_provenance(df)

        today = time.strftime("%Y-%m-%d")

        # ---------- provenance fields for harvested dataset rows ----------
        df["Website Platform"] = "Socrata"
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = "py_socrata"
        df["Endpoint Description"] = "DCAT API"
        df["Provenance"] = df.apply(
            lambda row: (
                f"The metadata for this resource was last retrieved from "
                f"{row.get('Publisher', ' Open Data Portal')} on {today}."
            ),
            axis=1,
        )

        # ---------- load hub list and evaluate indexing status ----------
        hub_path = self.config.get("input_csv")

        if hub_path and os.path.exists(hub_path):
            hub_df = pd.read_csv(hub_path, dtype=str).fillna("")

            # Ensure required columns exist
            if "Is Harvested" not in hub_df.columns:
                hub_df["Is Harvested"] = ""

            # Convert to string for a reliable comparison
            indexed_ids = set(df["Is Part Of"].astype(str))

            hub_df["Is Harvested"] = hub_df["ID"].apply(
                lambda website_id: "true" if str(website_id) in indexed_ids else "false"
            )

            hub_df["Last Harvested"] = today


            # ---------- merge ----------
            df = pd.concat([df, hub_df], ignore_index=True)

            print(f"[Socrata] Updated Status for {len(hub_df)} hub records and "
                f"appended them to the harvested metadata dataframe.")
        else:
            print("[Socrata] input_csv not found or unspecified.")

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

# --- Socrata Specific Field Derive Functions --- #

    def socrata_filter_rows(self, df):
        def is_valid(row):
            resource = row['resource']
            title = (resource.get('title') or '').strip()
            if not title or title.startswith('{{'):
                return False

            keywords = [k.lower().strip() for k in resource.get('keyword', []) if isinstance(k, str)]
            themes = [t.lower().strip() for t in resource.get('theme', [])] if isinstance(resource.get('theme'), list) else []

            return 'gis' in keywords or 'gis/maps' in themes

        return df[df.apply(is_valid, axis=1)].reset_index(drop=True)
    
    def socrata_map_to_schema(self, df: pd.DataFrame) -> pd.DataFrame:

        # Helper function to extract creator name, kept from original logic
        def get_creator(resource):
            pub = resource.get('publisher')
            if isinstance(pub, dict):
                return pub.get('name') or next(iter(pub.values()), '')
            return pub or ''
        
        def get_first_spatial(website):
            spatial = website.get('Spatial Coverage', '')
            if isinstance(spatial, list):
                for val in spatial:
                    if isinstance(val, str) and val.strip():
                        return val.strip()
                return ''
            # Spatial Coverage is stored as a pipe-delimited string; grab the first entry.
            return str(spatial).split('|')[0].strip()

        # Create a dictionary of Series, where each key is a final column name
        metadata_map = {
            # --- Fields from the 'website' dictionary ---
            'Is Part Of':       df['website'].apply(lambda h: h.get('ID', '')),
            'Code':             df['website'].apply(lambda h: h.get('ID', '')),
            'Publisher':        df['website'].apply(lambda h: h.get('Title', '')),
            'Endpoint URL':     df['website'].apply(lambda h: h.get('Endpoint URL', '')),
            'Spatial Coverage': df['website'].apply(lambda h: h.get('Spatial Coverage', '')),
            'Bounding Box':     df['website'].apply(lambda h: h.get('Bounding Box', '')),
            'Member Of':        df['website'].apply(lambda h: h.get('Member Of', '')),
            'titlePlace':       df['website'].apply(get_first_spatial),

            # --- Fields from the 'resource' (dataset) dictionary ---
            'Alternative Title': df['resource'].apply(lambda d: (d.get('title') or '').strip()),
            'Description':       df['resource'].apply(lambda d: d.get('description', '')),
            'Creator':           df['resource'].apply(get_creator),
            'Keyword':           df['resource'].apply(lambda d: '|'.join(k.strip() for k in d.get('keyword', []) if isinstance(k, str)).replace(' ', '')),
            'Subject':           df['resource'].apply(lambda d: '|'.join(d.get('theme', [])) if isinstance(d.get('theme'), list) else d.get('theme')),
            'Date Issued':       df['resource'].apply(lambda d: (d.get('issued', '') or '').split('T')[0]),
            'Date Modified':     df['resource'].apply(lambda d: (d.get('modified', '') or '').split('T')[0]),
            'Rights':            df['resource'].apply(lambda d: d.get('license', '')),
            'Identifier':        df['resource'].apply(lambda d: d.get('identifier', '')),
            'information':       df['resource'].apply(lambda d: d.get('landingPage', ''))
        }

        return pd.DataFrame(metadata_map)
    
    def socrata_parse_identifiers(self, df):
        """
        Derive ID from Identifier; handles common Socrata URL forms.
        Example: https://data.city.gov/views/abcd-1234 → ID=abcd-1234
        """
        def _to_id(identifier):
            s = str(identifier or "")
            # common Socrata patterns
            for cut in ("/views/", "/d/"):
                if cut in s:
                    return s.split(cut, 1)[-1].split("/", 1)[0]
            return s.rsplit("/", 1)[-1] if "/" in s else s

        df["ID"] = df["Identifier"].apply(_to_id)
        return df
        
    def socrata_temporal_coverage(self, df):
        """
        Adds a 'Temporal Coverage' column based on Title or Date Modified.
        """
        df["Temporal Coverage"] = df.apply(infer_temporal_coverage_from_title, axis=1)
        return df
    
    def socrata_format_date_ranges(self, df):
        """
        Adds a 'Date Range' column based on 'Temporal Coverage', 'Date Modified', or 'Date Issued'.
        """
        df["Date Range"] = df.apply(
            lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
            axis=1
        )
        return df

    def socrata_reformat_titles(self, df):
        """
        Updates the Title field by concatenating 'Alternative Title' and 'titlePlace',
        with the titlePlace in square brackets. 
        """
        df['Title'] = df.apply(
            lambda row: f"{row['Alternative Title']} [{row['titlePlace']}]"
            if pd.notna(row['Alternative Title']) and pd.notna(row['titlePlace'])
            else row['Alternative Title'] if pd.notna(row['Alternative Title'])
            else f"[{row['titlePlace']}]" if pd.notna(row['titlePlace'])
            else "",
            axis=1
        )
        return df
    
    def socrata_clean_creator_values(self, df):
        def _clean(value):
            if isinstance(value, dict) and 'name' in value:
                return value['name']
            if isinstance(value, str):
                m = re.match(r"\{\s*'name'\s*:\s*'(.+?)'\s*\}", value)
                return m.group(1) if m else value
            return value
        df['Creator'] = df['Creator'].apply(_clean)
        return df
 
    def socrata_set_resource_type(self, df):
        """
        Assign values to 'Resource Type' based on keyword matches found in Title, Description, or Keyword.
        Existing values are preserved unless a new match is found.
        """
        keyword_map = {
            'lidar': 'LiDAR',
            'polygon': 'Polygon data'
        }

        def match_keywords(row):
            combined_text = f"{row.get('Alternative Title', '')} {row.get('Description', '')} {row.get('Keyword', '')}".lower()
            for keyword, resource_type in keyword_map.items():
                if keyword in combined_text:
                    return resource_type
            return row.get('Resource Type', '')  # Keep existing value if no match

        df['Resource Type'] = df.apply(match_keywords, axis=1)
        return df
    
    
    def socrata_derive_geojson(self, df):
        """
        Add a 'geojson' distribution link for certain hubs
        Uses the hub Identifier (portal base URL) and the dataset's ID.
        """
        allowed_hubs = {'01c-01', '12b-17031-2'}

        # For this to work, we need hub-level info carried over from socrata_map_to_schema.
        # You can add 'Hub ID' and 'Hub Identifier' there if not already present.
        if 'Hub ID' not in df.columns:
            df['Hub ID'] = df['Is Part Of']
        if 'Hub Identifier' not in df.columns:
            df['Hub Identifier'] = df['Endpoint URL'].str.rsplit('/data.json', n=1).str[0]

        def build_geojson(row):
            if row['Hub ID'] in allowed_hubs and pd.notna(row['Hub Identifier']) and pd.notna(row['ID']):
                base = str(row['Hub Identifier']).rstrip('/')
                return f"{base}/resource/{row['ID']}.geojson"
            return ''

        df['geo_json'] = df.apply(build_geojson, axis=1)
        return df


