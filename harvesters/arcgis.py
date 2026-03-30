import csv
import time
import os
import re
from urllib.parse import urlparse, parse_qs

import requests
import pandas as pd

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table
from utils.temporal_fields import infer_temporal_coverage_from_title, create_date_range

class ArcGISHarvester(BaseHarvester):
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
                    yield f"[ArcGIS] Error fetching {website_id}: {e}"
                    continue

                row['fetched_catalog'] = json_api
                yield f"[ArcGIS] Fetched {website_id} — {row.get('Title', 'No Title')}"
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
            df.pipe(self.arcgis_filter_rows)
            .pipe(self.arcgis_map_to_schema)
            .pipe(self.arcgis_extract_distributions) 
        )

        return df

    def derive_fields(self, df):
        df = super().derive_fields(df)
        df = (
            df.pipe(self.arcgis_parse_identifiers)
            .pipe(self.arcgis_temporal_coverage)
            .pipe(self.arcgis_format_date_ranges)
            .pipe(self.arcgis_compute_bbox_column)
            .pipe(self.arcgis_clean_creator_values)
            .pipe(self.arcgis_reformat_titles)
            .pipe(self.arcgis_set_resource_type)
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
        df["Website Platform"] = "ArcGIS Hub"
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = "py_arcgis_hub"
        df["Endpoint Description"] = "DCAT API"
        df["Provenance"] = df.apply(
            lambda row: (
                f"The metadata for this resource was last retrieved from "
                f"{row.get('Publisher', ' ArcGIS Hub')} on {today}."
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
                lambda hub_id: "True" if str(hub_id) in indexed_ids else "False"
            )

            hub_df["Last Harvested"] = today


            # ---------- merge ----------
            df = pd.concat([df, hub_df], ignore_index=True)

            print(f"[ArcGIS] Updated Status for {len(hub_df)} hub records and "
                f"appended them to the harvested metadata dataframe.")
        else:
            print("[ArcGIS] input_csv not found or unspecified.")

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

# --- ArcGIS-Specific Functions --- #

    def arcgis_filter_rows(self, df):

        ALLOWED_TITLES = {'Shapefile'}
        ACCESS_PATTERNS = ['ImageServer']  # extend if needed, e.g., 'FeatureServer', 'MapServer'

        def is_valid(row):
            resource = row['resource']
            # CORRECTED: Cast to string before stripping to avoid AttributeError
            title = str(resource.get('title', '')).strip()
            if not title or title.startswith('{{'):
                return False

            dists = resource.get('distribution', []) or []
            if not isinstance(dists, list):
                return False

            has_valid_title = any((dist.get('title') in ALLOWED_TITLES) for dist in dists)
            has_valid_url = any(
                any(pat in str(dist.get('accessURL', '')) for pat in ACCESS_PATTERNS)
                for dist in dists
            )
            return has_valid_title or has_valid_url

        return df[df.apply(is_valid, axis=1)].reset_index(drop=True)

    def arcgis_map_to_schema(self, df: pd.DataFrame) -> pd.DataFrame:

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

        output_data = {
            # --- Map Hub fields directly to Final Schema ---
            'Is Part Of':       df['website'].apply(lambda h: h.get('ID', '')),
            'Code':             df['website'].apply(lambda h: h.get('ID', '')),
            'Publisher':        df['website'].apply(lambda h: h.get('Title', '')),
            # 'Publisher':        df['website'].apply(lambda h: h.get('Creator', '')),
            'Endpoint URL':     df['website'].apply(lambda h: h.get('Endpoint URL', '')),
            'Spatial Coverage': df['website'].apply(lambda h: h.get('Spatial Coverage', '')),
            'default_bbox':     df['website'].apply(lambda h: h.get('Bounding Box', '')),
            'Member Of':        df['website'].apply(lambda h: h.get('Member Of', '')),
            'titlePlace':       df['website'].apply(get_first_spatial),

            # --- Map Dataset fields directly to Final Schema ---
            'Alternative Title': df['resource'].apply(lambda d: str(d.get('title', '')).strip()),
            'Description':       df['resource'].apply(lambda d: d.get('description', '')),
            'Creator':           df['resource'].apply(get_creator),
            'Keyword':           df['resource'].apply(lambda d: '|'.join(k.strip() for k in d.get('keyword', []) if isinstance(k, str)).replace(' ', '')),
            'Date Issued':       df['resource'].apply(lambda d: str(d.get('issued', '')).split('T')[0]),
            'Date Modified':     df['resource'].apply(lambda d: str(d.get('modified', '')).split('T')[0]),
            'Rights':            df['resource'].apply(lambda d: d.get('license', '')),
            'identifier_raw':    df['resource'].apply(lambda d: d.get('identifier', '')),
            'information':       df['resource'].apply(lambda d: d.get('landingPage', '')),

            # --- Create Pass-through columns for the next steps in the pipeline ---
            'spatial':           df['resource'].apply(lambda d: d.get('spatial', '')),
            'distributions':     df['resource'].apply(lambda d: d.get('distribution', []) or [])
        }
        
        return pd.DataFrame(output_data)

    def arcgis_extract_distributions(self, df):
        """
        Sorts webs service links
        """

        def derive_dist_fields(dists):
            out = {
                'featureService': '',
                'mapService': '',
                'imageService': '',
                'tileService': '',
                'Format': '',
            }
            # Ensure 'dists' is a list before iterating
            if not isinstance(dists, list):
                dists = []

            for dist in dists:
                title = str(dist.get('title', ''))
                access_url = str(dist.get('accessURL', ''))
                if title == 'ArcGIS GeoService' and access_url:
                    if 'FeatureServer' in access_url:
                        out['featureService'] = access_url
                        out['Format'] = 'ArcGIS FeatureLayer'
                    elif 'MapServer' in access_url:
                        out['mapService'] = access_url
                        out['Format'] = 'ArcGIS DynamicMapLayer'
                    elif 'ImageServer' in access_url:
                        out['imageService'] = access_url
                        out['Format'] = 'ArcGIS ImageMapLayer'
                    elif 'TileServer' in access_url:
                        out['tileService'] = access_url
                        out['Format'] = 'ArcGIS TiledMapLayer'
            return pd.Series(out)

        dist_df = df['distributions'].apply(derive_dist_fields)
        # merge columns into df (aligned by index)
        df = pd.concat([df, dist_df], axis=1)

        return df


    def arcgis_compute_bbox_column(self, df):
        """
        Populate 'Bounding Box' using 'spatial' if it has 4 comma-separated numbers
        and forms a non-degenerate box (xmin != xmax and ymin != ymax).
        Otherwise, use 'default_bbox'.
        """
        def _bbox(r):
            sp = r.get('spatial', None)
            fallback = r.get('default_bbox', '')

            def use_fallback():
                fb = '' if pd.isna(fallback) else str(fallback).strip()
                return fb

            if isinstance(sp, str):
                parts = [p.strip() for p in sp.split(',')]
                if len(parts) == 4:
                    try:
                        xmin, ymin, xmax, ymax = [float(p) for p in parts]

                        # Normalize if reversed
                        if xmin > xmax: xmin, xmax = xmax, xmin
                        if ymin > ymax: ymin, ymax = ymax, ymin

                        # Degenerate → line/point → use fallback
                        if xmin == xmax or ymin == ymax:
                            return use_fallback()

                        # Valid polygon bbox
                        return f"{xmin},{ymin},{xmax},{ymax}"
                    except ValueError:
                        pass

            # Not a valid 4-number bbox → use fallback
            return use_fallback()

        df['Bounding Box'] = df.apply(_bbox, axis=1)
        return df


    def arcgis_harvest_identifier_and_id(self, identifier: str) -> tuple:
        parsed = urlparse(identifier)
        qs = parse_qs(parsed.query)

        if 'id' in qs:
            resource_id = qs['id'][0]

            # Append sublayer number if present
            if 'sublayer' in qs:
                resource_id = f"{resource_id}_{qs['sublayer'][0]}"

            cleaned = f"https://hub.arcgis.com/datasets/{resource_id}"
            return cleaned, resource_id

        return identifier, identifier


    def arcgis_parse_identifiers(self, df):
        ids = df['identifier_raw'].apply(self.arcgis_harvest_identifier_and_id)
        df[['Identifier', 'ID']] = pd.DataFrame(ids.tolist(), index=df.index)
        return df

    def arcgis_temporal_coverage(self, df):
        """
        Adds a 'Temporal Coverage' column based on Title or Date Modified.
        """
        df["Temporal Coverage"] = df.apply(infer_temporal_coverage_from_title, axis=1)
        return df
    
    def arcgis_format_date_ranges(self, df):
        """
        Adds a 'Date Range' column based on 'Temporal Coverage', 'Date Modified', or 'Date Issued'.
        """
        df["Date Range"] = df.apply(
            lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
            axis=1
        )
        return df

    def arcgis_reformat_titles(self, df):
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

    def arcgis_clean_creator_values(self, df):
        def _clean(value):
            if isinstance(value, dict) and 'name' in value:
                return value['name']
            elif isinstance(value, str):
                match = re.match(r"\\{\\s*'name'\\s*:\\s*'(.+?)'\\s*\\}", value)
                if match:
                    return match.group(1)
                return value
            return value
    
        df['Creator'] = df['Creator'].apply(_clean)
        return df
    
    def arcgis_set_resource_type(self, df):
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
