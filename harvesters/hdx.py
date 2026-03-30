import time
import os
import re
import json
from urllib.parse import urlparse, parse_qs

import requests
import pandas as pd

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table
from utils.temporal_fields import infer_temporal_coverage_from_title, create_date_range



class HdxHarvester(BaseHarvester):
    def __init__(self, config):
        super().__init__(config)

    # Add this updated method to your HdxHarvester class
    def load_reference_data(self):
        super().load_reference_data()
        
        # --- Load and prepare spatial nations data ---
        self.bbox_map = {}
        self.geometry_map = {}
        self.geonames_map = {}
        
        nations_csv_path = self.config.get("nations_csv", "reference_data/spatial_nations.csv")
        try:
            nations_df = pd.read_csv(nations_csv_path, dtype=str).fillna('')
            
            # Prepare clean versions of labels for matching
            nations_df['clean_label'] = nations_df['Label'].str.strip().str.lower()
            nations_df['clean_altLabel'] = nations_df.get('altLabel', pd.Series(dtype=str)).str.strip().str.lower()

            for _, row in nations_df.iterrows():
                bbox = row['Bounding Box']
                geom = row['Geometry']
                geonames_id = row['GeoNames ID']
                
                # Map the primary label
                if row['clean_label']:
                    self.bbox_map[row['clean_label']] = bbox
                    self.geometry_map[row['clean_label']] = geom
                    self.geonames_map[row['clean_label']] = geonames_id
                
                # Map the alternate label if it exists
                if row['clean_altLabel']:
                    self.bbox_map[row['clean_altLabel']] = bbox
                    self.geometry_map[row['clean_altLabel']] = geom
                    self.geonames_map[row['clean_altLabel']] = geonames_id
            
            print(f"[HDX] Successfully loaded {len(self.bbox_map)} spatial mappings from {nations_csv_path}")

        except FileNotFoundError:
            print(f"[HDX] Warning: Nations CSV not found at {nations_csv_path}. Spatial fields will not be derived.")
        except Exception as e:
            print(f"[HDX] Error loading nations CSV: {e}")
            
        # --- Load and prepare theme data ---
        self.theme_map = {}
        themes_csv_path = self.config.get("themes_csv", "reference_data/themes.csv")
        try:
            themes_df = pd.read_csv(themes_csv_path, dtype=str).fillna('')
            
            for _, row in themes_df.iterrows():
                theme = row['Theme']
                # Split the pipe-separated keywords into a list
                keywords = row['Keyword'].split('|')
                
                for keyword in keywords:
                    clean_keyword = keyword.strip().lower()
                    if clean_keyword:
                        # Map the clean keyword to its theme
                        self.theme_map[clean_keyword] = theme
            
            print(f"[HDX] Successfully loaded {len(self.theme_map)} theme keyword mappings from {themes_csv_path}")

        except FileNotFoundError:
            print(f"[HDX] Warning: Themes CSV not found at {themes_csv_path}. Themes will not be derived.")
        except Exception as e:
            print(f"[HDX] Error loading themes CSV: {e}")


    def fetch(self):
        """
        Loads resources from a local HDX JSON file and yields them individually.
        """
        input_json = self.config.get("input_json")
        if not input_json or not os.path.exists(input_json):
            yield f"[HDX] Error: input_json not found at {input_json}"
            return

        try:
            with open(input_json, "r", encoding="utf-8") as f:
                resource_list = json.load(f)
        except Exception as e:
            yield f"[HDX] Error reading {input_json}: {e}"
            return

        yield f"[HDX] Loaded {len(resource_list)} records from {input_json}"

        # Yield each resource dictionary directly.
        for resource in resource_list:
            yield resource

    def flatten(self, resource_iterator):
        """
        Ensures the harvested items are a clean list of dictionaries.
        """
        return [resource for resource in resource_iterator if isinstance(resource, dict)]

    def build_dataframe(self, flattened_items):
        """
        Creates a DataFrame from the list of resource dictionaries.
        """
        # This now creates a DataFrame where each key in the JSON becomes a column.
        df = pd.DataFrame(flattened_items)

        df = df.pipe(self.hdx_map_to_schema)
        return df

    def derive_fields(self, df):
        df = super().derive_fields(df)
        
        df['information'] = "https://data.humdata.org/dataset/" + df['ID'].astype(str)
        df = (
            df.pipe(self.hdx_spatial_fields)
            .pipe(self.hdx_derive_date_range)
        )
        return df

    def add_defaults(self, df):
        df = super().add_defaults(df)

        df['Display Note'] = "Tip: Check “Visit Source” link for download options."
        # df['Language'] = 'eng'
        df['Resource Class'] = 'Datasets'
        df['Code'] = '99-1400'
        df['Member Of'] = 'b0153110-e455-4ced-9114-9b13250a7093'
        df['Is Part Of'] = '99-1400'
        df['Publisher'] = 'Humanitarian Data Exchange'

        return df
    
    def add_provenance(self, df: pd.DataFrame) -> pd.DataFrame:
        # ---------- inherited defaults ----------
        df = super().add_provenance(df)

        today = time.strftime("%Y-%m-%d")

        # ---------- provenance fields for harvested dataset rows ----------
        df["Website Platform"] = "CKAN"
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = "py_hdx"
        df["Endpoint URL"] = "https://data.humdata.org/api/3/"
        df["Endpoint Description"] = "CKAN API"
        df["Provenance"] = df.apply(
            lambda row: (
                f"The metadata for this resource was last retrieved from Humanitarian Data Exchange on {today}."
            ),
            axis=1,
        )

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
    
# --- HDX Specific Functions --- #

    import json


    def hdx_map_to_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the raw DataFrame from the HDX JSON into the final schema 
        by applying the specific mappings provided.
        """
        # Create a new, empty DataFrame to hold the final mapped data.
        df_out = pd.DataFrame()

        # --- 1. Direct Mappings (Simple key -> value) ---
        df_out['Display Note'] = df.get('caveats', '')
        df_out['Creator'] = df.get('dataset_source', '')
        df_out['ID'] = df.get('id', '')
        df_out['Identifier'] = df.get('name', '')
        df_out['License'] = df.get('license_url', '') # Note: JSON key is 'license_url'
        df_out['Description'] = df.get('notes', '')
        df_out['Title'] = df.get('title', '')
        
        # For the date, we extract just the date part before the 'T'
        df_out['Date Modified'] = df.get('last_modified', pd.Series(dtype=str)).str.split('T', n=1).str[0]
        df_out['Date Issued'] = df.get('metadata_created', pd.Series(dtype=str)).str.split('T', n=1).str[0]
        df_out['dataset_date_raw'] = df.get('dataset_date', '')
        # --- 2. Nested Mappings (Requiring .apply()) ---

        # Handle the 'tags' list of dictionaries
        if 'tags' in df.columns:
            df_out['Keyword'] = df['tags'].apply(
                lambda tags_list: '|'.join(
                    tag.get('display_name', '') for tag in tags_list
                ) if isinstance(tags_list, list) else ''
            )
        else:
            df_out['Keyword'] = ''

        # Handle the 'solr_additions' string-encoded JSON
        def extract_countries(solr_string):
            if not isinstance(solr_string, str):
                return ''
            try:
                # First, parse the string into a Python dictionary
                solr_data = json.loads(solr_string)
                # Then, get the list of countries from the dictionary
                countries = solr_data.get('countries', [])
                return '|'.join(countries) if isinstance(countries, list) else ''
            except (json.JSONDecodeError, TypeError):
                # Return empty string if the solr_string is not valid JSON
                return ''

        if 'solr_additions' in df.columns:
            # Note: The JSON key has an underscore, not a hyphen.
            df_out['Spatial Coverage'] = df['solr_additions'].apply(extract_countries)
        else:
            df_out['Spatial Coverage'] = ''

        return df_out
    

    def _lookup_spatial_values(self, place_names_str, lookup_map):
        """Helper to look up a pipe-separated list of names in a given map."""
        if not isinstance(place_names_str, str) or not lookup_map:
            return None
            
        found_values = []
        place_names = place_names_str.split('|')
        
        for name in place_names:
            clean_name = name.strip().lower()
            value = lookup_map.get(clean_name)
            if value:
                found_values.append(value)
        
        return '|'.join(found_values) if found_values else None

    def _combine_bounding_boxes(self, bboxes_str):
        """
        Calculates a single bounding box that encompasses multiple,
        pipe-separated bounding boxes.
        """
        # Return None if the input is None, NaN, or an empty string
        if not bboxes_str or pd.isna(bboxes_str):
            return None

        min_lon, min_lat = float('inf'), float('inf')
        max_lon, max_lat = float('-inf'), float('-inf')

        # Iterate through all bounding boxes in the pipe-separated string
        for bbox in bboxes_str.split('|'):
            if bbox:
                try:
                    # Parse the bounding box coordinates
                    coords = list(map(float, bbox.split(',')))
                    # Update the min and max values
                    min_lon = min(min_lon, coords[0])  # west
                    min_lat = min(min_lat, coords[1])  # south
                    max_lon = max(max_lon, coords[2])  # east
                    max_lat = max(max_lat, coords[3])  # north
                except (ValueError, IndexError):
                    # Skip malformed bounding box strings
                    continue

        # Check if any valid boxes were found
        if min_lon == float('inf'):
            return None # No valid coordinates were processed

        # Create and return the combined bounding box string
        return f"{min_lon},{min_lat},{max_lon},{max_lat}"

    def hdx_spatial_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Derives Bounding Box, Geometry, and GeoNames ID from Spatial Coverage.
        Ensures a single combined bounding box is created.
        """
        if 'Spatial Coverage' not in df.columns:
            print("[HDX] Warning: 'Spatial Coverage' column not found. Skipping spatial field derivation.")
            return df

        # Step A: Look up the values for each spatial attribute, which may be pipe-separated
        df['Bounding Box'] = df['Spatial Coverage'].apply(self._lookup_spatial_values, lookup_map=self.bbox_map)
        df['Geometry'] = df['Spatial Coverage'].apply(self._lookup_spatial_values, lookup_map=self.geometry_map)
        df['GeoNames ID'] = df['Spatial Coverage'].apply(self._lookup_spatial_values, lookup_map=self.geonames_map)

        # Step B: Apply the new combining function to the 'Bounding Box' column
        # This takes the pipe-separated result from Step A and combines it into one.
        df['Bounding Box'] = df['Bounding Box'].apply(self._combine_bounding_boxes)

        return df

    def _parse_hdx_date_range(self, date_str):
        """
        Parses a date string like "[YYYY-MM-DD... TO YYYY-MM-DD...]"
        and returns a formatted "YYYY-YYYY" string, ensuring the range
        format is always used.
        """
        if not isinstance(date_str, str) or not date_str.startswith('['):
            return None

        try:
            # Clean the string by removing brackets and splitting by " TO "
            parts = date_str.strip('[]').split(' TO ')
            if len(parts) != 2:
                return None

            start_date_str, end_date_str = parts
            
            # Extract the first 4 characters (the year) from each part
            start_year = start_date_str[:4]
            end_year = end_date_str[:4]

            # Ensure both are valid 4-digit years
            if not (start_year.isdigit() and len(start_year) == 4 and end_year.isdigit() and len(end_year) == 4):
                 return None
            return f"{start_year}-{end_year}"

        except Exception:
            # Return None if any parsing error occurs
            return None
        
    def _parse_hdx_temporal_coverage(self, date_str):
        """
        Parses a date string like "[YYYY-MM-DD... TO YYYY-MM-DD...]"
        and returns a formatted "YYYY-MM-DD to YYYY-MM-DD" string.
        Returns a single date if the start and end dates are the same.
        """
        if not isinstance(date_str, str) or not date_str.startswith('['):
            return None

        try:
            # Clean the string by removing brackets and splitting by " TO "
            parts = date_str.strip('[]').split(' TO ')
            if len(parts) != 2:
                return None

            start_datetime_str, end_datetime_str = parts
            
            # Extract the first 10 characters (the date) from each part
            start_date = start_datetime_str[:10]
            end_date = end_datetime_str[:10]

            # If start and end dates are the same, just return the single date
            if start_date == end_date:
                return start_date
            
            return f"{start_date} to {end_date}"

        except Exception:
            # Return None if any parsing error occurs
            return None

    def hdx_derive_date_range(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates 'Date Range' (YYYY-YYYY) and 'Temporal Coverage' (YYYY-MM-DD)
        columns from the raw 'dataset_date' field.
        """
        if 'dataset_date_raw' not in df.columns:
            print("[HDX] Warning: 'dataset_date_raw' column not found. Skipping date range derivation.")
            return df
        
        # 1. Create the 'Date Range' column with the YYYY-YYYY format
        df['Date Range'] = df['dataset_date_raw'].apply(self._parse_hdx_date_range)
        
        # 2. Create the 'Temporal Coverage' column with the YYYY-MM-DD to YYYY-MM-DD format
        df['Temporal Coverage'] = df['dataset_date_raw'].apply(self._parse_hdx_temporal_coverage)
        
        # Drop the temporary raw column as it's no longer needed
        df = df.drop(columns=['dataset_date_raw'])
        
        return df
    
