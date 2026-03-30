# Standard library
import re
import time
import requests
import urllib.parse
from datetime import datetime # Import datetime

# Third-party
import pandas as pd
from bs4 import BeautifulSoup

# Project-specific
from utils.distribution_writer import generate_secondary_table
from harvesters.base import BaseHarvester

from utils.temporal_fields import infer_temporal_coverage_from_title, create_date_range


class IsgsHarvester(BaseHarvester):
    """
    A harvester for the Illinois State Geological Survey Clearinghouse website.
    """
    def __init__(self, config):
        """
        Initialize the harvester with a configuration dictionary.
        """
        super().__init__(config)
        self.base_url = self.config['base_url']

    def fetch(self):
        """
        Fetches the list of all dataset landing pages from the main data catalog page.
        This method is a generator, yielding one record at a time.

        Yields:
            tuple: A tuple containing (theme, title, landing_page_url) for each dataset.
        """
        print(f"[FETCH] Fetching landing pages from {self.base_url}/data")
        try:
            response = requests.get(f"{self.base_url}/data")
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"[FETCH] ERROR: Could not retrieve the main data page. {e}")
            return

        soup = BeautifulSoup(response.text, 'html.parser')
        
        landing_pages_found = 0
        for theme_section in soup.select('.item-list'):
            theme_h3 = theme_section.find('h3')
            if not theme_h3:
                continue
            theme = theme_h3.text.strip()

            for dataset in theme_section.select('.views-row'):
                title_element = dataset.select_one('.views-field-title a')
                if title_element and title_element.has_attr('href'):
                    title = title_element.text.strip()
                    relative_url = title_element['href']
                    landing_page = f"{self.base_url}{relative_url}"
                    yield (theme, title, landing_page)
                    landing_pages_found += 1
        
        print(f"[FETCH] Found {landing_pages_found} dataset landing pages to parse.")

    def parse(self, raw_records):
            """
            Parses a list of raw records. This method focuses purely on EXTRACTION.
            The dictionary keys reflect the source data, not the target schema.

            Args:
                raw_records (list): A list of tuples from fetch(), e.g., [(theme, title, url), ...].

            Returns:
                list: A list of dictionaries with raw, unmapped data.
            """
            parsed_records = []
            print(f"[PARSE] Starting to extract data from {len(raw_records)} records.")
            
            for theme, title, landing_page in raw_records:
                try:
                    response = requests.get(landing_page, timeout=60)
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"[PARSE] ERROR: Could not fetch {landing_page}. Skipping. Reason: {e}")
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Dictionary keys now describe the source data
                dataset_info = {
                    'source_theme': theme,
                    'source_title': title,
                    'source_landing_page': landing_page,
                    'source_description': None,
                    'source_metadata_html': None,
                    'source_metadata_xml': None,
                    'source_documentation_pdf': None,
                    'source_arcgis_image_layer': None,
                    'source_arcgis_map_layer': None,
                    'source_arcgis_feature_layer': None,
                    'source_download_zip': None
                }

                summary_div = soup.select_one('fieldset.group-summary div.field-item')
                if summary_div:
                    dataset_info['source_description'] = summary_div.text.strip()

                download_links = soup.select('.group-downloads .field-item a')
                for link in download_links:
                    if link.has_attr('href') and link['href'].endswith('.zip'):
                        dataset_info['source_download_zip'] = link['href']
                        break

                metadata_links = soup.select('.group-metadata .field-item a')
                for link in metadata_links:
                    if link.has_attr('href'):
                        metadata_url = link['href']
                        if not metadata_url.startswith('http'):
                            metadata_url = f"{self.base_url}{metadata_url}"
                        
                        if metadata_url.endswith(('.htm', '.html')):
                            dataset_info['source_metadata_html'] = metadata_url
                        elif metadata_url.endswith('.xml'):
                            dataset_info['source_metadata_xml'] = metadata_url
                        elif metadata_url.endswith('.pdf'):
                            dataset_info['source_documentation_pdf'] = metadata_url

                service_links = soup.select('.group_services .field-item a')
                for link in service_links:
                    if link.has_attr('href'):
                        service_url = link['href']
                        if service_url.endswith('/ImageServer'):
                            dataset_info['source_arcgis_image_layer'] = service_url
                        elif service_url.endswith('/MapServer'):
                            dataset_info['source_arcgis_map_layer'] = service_url
                        elif service_url.endswith('/FeatureServer'):
                            dataset_info['source_arcgis_feature_layer'] = service_url
                
                parsed_records.append(dataset_info)
            
            print(f"[PARSE] Successfully extracted data for {len(parsed_records)} records.")
            return parsed_records

    def build_dataframe(self, parsed_data):
            """
            Converts the list of parsed dictionaries into a Pandas DataFrame
            and then maps the fields to the target schema.
            """
            if not parsed_data:
                print("[BUILD] No data was parsed. Returning an empty DataFrame.")
                return pd.DataFrame()

            # Create DataFrame with source field names
            df = pd.DataFrame(parsed_data)

            # Map to the final schema using the dedicated helper method
            schema_df = self.isgs_map_to_schema(df)
            
            print(f"[BUILD] Successfully built and mapped DataFrame with {len(schema_df)} records.")
            return schema_df
        
    def derive_fields(self, df):
        df = super().derive_fields(df)

        df['Title'] = df['Alternative Title'].astype(str) + " [Illinois]"

        
        df = (
            df.pipe(self.isgs_derive_ids)
            .pipe(self.isgs_temporal_coverage)
            .pipe(self.isgs_format_date_ranges)
        )

        return df
        
   
    def add_defaults(self, df):
        df = super().add_defaults(df)
        df['Code'] = '02a-01'
        # df['Creator'] = 'Illinois State Geological Survey'
        df['Publisher'] = 'Illinois Geospatial Data Clearinghouse'
        df['Language'] = 'eng'
        df['Is Part Of'] = '02a-01'
        df['Member Of'] = 'ba5cc745-21c5-4ae9-954b-72dd8db6815a'
        df['Format'] = 'File'
        df['Resource Class'] = 'Datasets'
        df['Bounding Box'] = '-91.51,36.97,-87.02,42.51'

        return df
    
    def add_provenance(self, df):
        # ---------- inherited defaults ----------
        df = super().add_provenance(df)

        # Use datetime for better date handling
        today = datetime.now().strftime("%Y-%m-%d")

        # ---------- provenance fields for harvested dataset rows ----------
        df["Website Platform"] = "Custom data portal"
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = "py_isgs"
        df["Endpoint Description"] = "HTML"
        df["Endpoint URL"] = "https://clearinghouse.isgs.illinois.edu/data"
        
        # --- FIX #1: Use an f-string to insert the date ---
        df["Provenance"] = f"The metadata for this resource was last retrieved from ISGS on {today}."

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

 # ────── ISGS Custom Methods ──────
    def isgs_map_to_schema(self, df):
            """
            Maps the raw DataFrame columns to the target schema.
            This is the single source of truth for field mapping.
            """
            print("[MAP] Mapping raw data to target schema.")
            schema_df = pd.DataFrame()

            # Use .get() to safely access columns that may not exist for all records
            schema_df['Alternative Title'] = df.get('source_title')
            schema_df['Keyword'] = df.get('source_theme')
            schema_df['Description'] = df.get('source_description')
            schema_df['information'] = df.get('source_landing_page')
            schema_df['download'] = df.get('source_download_zip')
            schema_df['featureService'] = df.get('source_arcgis_feature_layer')
            schema_df['mapService'] = df.get('source_arcgis_map_layer')
            schema_df['imageService'] = df.get('source_arcgis_image_layer')
            schema_df['html'] = df.get('source_metadata_html')
            schema_df['documentation'] = df.get('source_documentation_pdf')

            return schema_df



    def isgs_derive_ids(self, df):
            """
            Derives a unique ID for each record based on its landing page URL.
            """
            print("[DERIVE] Deriving unique IDs.")
            
            def generate_id(url):
                if not isinstance(url, str):
                    return None
                    
                path = urllib.parse.urlparse(url).path
                prefixes = ["/data/", "/datasets/"]
                
                start_pos = -1
                chosen_prefix = None
                
                for prefix in prefixes:
                    start_pos = path.find(prefix)
                    if start_pos != -1:
                        chosen_prefix = prefix
                        break
                
                if start_pos == -1:
                    return None
                
                start_pos += len(chosen_prefix)
                relevant_path = path[start_pos:]
                modified_path = relevant_path.replace('/', '-').strip('-')
                
                return f"02a-01_{modified_path}"

            if 'information' in df.columns:
                df['ID'] = df['information'].apply(generate_id)
            else:
                print("[DERIVE] WARNING: 'Landing Page' column not found. Cannot derive IDs.")
                df['ID'] = None
                
            return df
    
    def isgs_temporal_coverage(self, df):
        """
        Adds a 'Temporal Coverage' column based on Title or Date Modified.
        """
        df["Temporal Coverage"] = df.apply(infer_temporal_coverage_from_title, axis=1)
        return df
    
    def isgs_format_date_ranges(self, df):
        """
        Adds a 'Date Range' column based on 'Temporal Coverage', 'Date Modified', or 'Date Issued'.
        """
        df["Date Range"] = df.apply(
            lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
            axis=1
        )
        return df
