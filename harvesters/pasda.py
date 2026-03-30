# Standard library
import re
import time



# Third-party
import pandas as pd
from bs4 import BeautifulSoup

# Project-specific
from utils.distribution_writer import generate_secondary_table
from harvesters.base import BaseHarvester

from utils.creator_match import creator_match
from utils.temporal_fields import infer_temporal_coverage_from_title, create_date_range
from utils.title_formatter import title_wizard


class PasdaHarvester(BaseHarvester):

    def __init__(self, config):
        super().__init__(config)
        self.counties_in_pennsylvania = []
        self.cities_in_pennsylvania = []
        self.spatial_data = pd.DataFrame()

    def load_reference_data(self):
        super().load_reference_data()

        # Load spatial CSV with bbox and geonames
        spatial_csv_path = "reference_data/spatial_counties.csv"
        self.spatial_data = pd.read_csv(spatial_csv_path)

    def fetch(self):
        html_path = self.config["input_html"]
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()

    def parse(self, raw_data):
        soup = BeautifulSoup(raw_data, 'html.parser')
        rows = []

        for entry in soup.select('td > h3 > a[href^="DataSummary.aspx?dataset="]'):
            try:
                # Must have visible title text
                title = entry.get_text(strip=True)
                if not title:
                    continue

                # Look ahead for related tags
                row = entry.find_parent("tr")
                if not row:
                    continue

                publisher_tag = entry.find_next("td")
                date_tag = entry.find_previous("td").find_previous("td")
                desc_tag = entry.find_next("span", id=lambda x: x and x.startswith('DataGrid1_Label3_'))

                # Basic content extraction
                publisher = publisher_tag.get_text(strip=True) if publisher_tag else ""
                date_issued = date_tag.get_text(strip=True) if date_tag else ""
                description = desc_tag.get_text(strip=True) if desc_tag else ""

                # Skip records with no description or publisher (optional)
                if not description or not publisher:
                    continue

                # Validate Metadata inks
                meta_tag = row.find('a', string='Metadata')
                if not meta_tag or not meta_tag.get('href'):
                    continue

                metadata_link = f"https://www.pasda.psu.edu/uci/{meta_tag['href']}"
                
                

                landing_page = f"https://www.pasda.psu.edu/uci/{entry['href']}"
                identifier = 'pasda-' + landing_page.rsplit('=', 1)[-1]
                if not identifier:
                    continue

                # Build the row
                rows.append({
                    'Creator': publisher,
                    'Date Issued': date_issued,
                    'Alternative Title': title,
                    'Description': description,
                    'html': metadata_link,
                    # 'download': download_link,
                    'information': landing_page,
                    'ID': identifier
                })

            except Exception as e:
                print(f"[PASDA] Skipping entry due to error: {e}")
                continue

        print(f"[PASDA] Parsed {len(rows)} valid records from HTML")
        return pd.DataFrame(rows)


    def build_dataframe(self, parsed_or_flattened_data):
        return pd.DataFrame(parsed_or_flattened_data)

    def derive_fields(self, df):
        
        df = creator_match(df, state="Pennsylvania")

        return (
            df.pipe(self.pasda_drop_incomplete)
            .pipe(self.pasda_drop_federal)
            .pipe(self.pasda_spatial_coverage)
            .pipe(self.pasda_philadelphia_spatial)
            .pipe(self.pasda_temporal_coverage)
            .pipe(self.pasda_format_date_ranges)
            .pipe(self.pasda_reformat_titles)
        )

   
    def add_defaults(self, df):
        df = super().add_defaults(df)
        df['Code'] = '08a-01'
        df['Provider'] = 'Pennsylvania Spatial Data Access (PASDA)'
        df['Language'] = 'eng'
        df['Is Part Of'] = '08a-01'
        df['Member Of'] = 'ba5cc745-21c5-4ae9-954b-72dd8db6815a'
        df['Format'] = 'File'
        df['Resource Class'] = 'Datasets'

        return df
    
    def add_provenance(self, df):
        # ---------- inherited defaults ----------
        df = super().add_provenance(df)

        today = time.strftime("%Y-%m-%d")

        # ---------- provenance fields for harvested dataset rows ----------
        df["Website Platform"] = "HTML/JS"
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = "py_pasda"
        df["Endpoint Description"] = "HTML"
        df["Endpoint URL"] = "https://www.pasda.psu.edu/uci/SearchResults.aspx?Keyword=."
        df["Provenance"] = f"The metadata for this resource was last retrieved from PASDA on {today}."

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


    # ────── PASDA Custom Methods ──────

    def pasda_drop_incomplete(self, df):
        """
        Drops rows missing required fields like 'ID' or 'Alternative Title'.
        Logs how many were dropped.
        """
        before = len(df)
        df = df[df['ID'].notna() & df['Alternative Title'].notna()].copy()
        dropped = before - len(df)
        if dropped > 0:
            print(f"[PASDA] Dropped {dropped} records missing 'ID' or 'Alternative Title'.")
        return df

    def pasda_drop_federal(self, df):
        """
        Drops datasets with federal sources.
        """
        
        federal = [
            "United States Army Corps of Engineers USACE", "U S Geological Survey", "U S Fish and Wildlife Service",
            "U S Environmental Protection Agency", "U S Department of Justice", "U S Department of Commerce",
            "U S Department of Agriculture", "U S Census Bureau", "National Weather Service NOAA NWS",
            "National Renewable Energy Laboratory NREL", "National Park Service", "National Geodetic Survey",
            "National Aeronautics and Space Administration NASA", "Federal Emergency Management Agency"
        ]

        if df.empty or 'Creator' not in df.columns:
            print("[PASDA] Skipping federal filter: 'Creator' column not found or DataFrame is empty.")
            return df

        return df[~df['Creator'].isin(federal)].reset_index(drop=True)
    
    def pasda_spatial_coverage(self, df):
        """
        Set spatial metadata fields:
        - Derive 'Spatial Coverage' from 'Creator' using FAST format.
        - Fill in missing 'Bounding Box', 'Geometry', and 'GeoNames' with Pennsylvania defaults.
        """

        if df.empty or 'Creator' not in df.columns:
            print("[PASDA] Skipping spatial metadata: 'Creator' column not found or DataFrame is empty.")
            df['Spatial Coverage'] = ''
            return df

        # Step 1: Set Spatial Coverage
        def format_coverage(creator):
            if not isinstance(creator, str) or not creator.startswith("Pennsylvania--"):
                return "Pennsylvania"
            return f"{creator}|Pennsylvania"

        df['Spatial Coverage'] = df['Creator'].apply(format_coverage)

        # Step 2: Fill in missing spatial metadata
        defaults = {
            'Bounding Box': "-80.52,39.72,-74.69,42.27",
            'Geometry': (
                "MultiPolygon(((-75.6 39.8, -75.8 39.7, -80.5 39.7, -80.5 42.3, "
                "-79.8 42.5, -79.8 42, -75.3 42, -75.1 41.8, -75 41.5, -74.7 41.4, "
                "-75.1 41, -75.1 40.9, -75.2 40.7, -74.7 40.2, -75.1 39.9, -75.6 39.8)))"
            ),
            'GeoNames': "http://sws.geonames.org/6254927"
        }

        for column, default in defaults.items():
            if column not in df.columns:
                df[column] = default
            else:
                df[column] = df[column].replace("", default).fillna(default)

        return df
    
    def pasda_temporal_coverage(self, df):
        """
        Adds a 'Temporal Coverage' column based on Title or Date Modified.
        """
        df["Temporal Coverage"] = df.apply(infer_temporal_coverage_from_title, axis=1)
        return df
    
    def pasda_format_date_ranges(self, df):
        """
        Adds a 'Date Range' column based on 'Temporal Coverage', 'Date Modified', or 'Date Issued'.
        """
        df["Date Range"] = df.apply(
            lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
            axis=1
        )
        return df

    def pasda_reformat_titles(self, df):
        """
        Updates the Title field using a formatting pipeline.
        """
        return title_wizard(df)
       
    def pasda_philadelphia_spatial(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        For rows with Creator == 'Pennsylvania--Philadelphia':
        - set a precise Philadelphia Bounding Box
        - clear Geometry and GeoNames (leave blank)
        """
        if df.empty or "Creator" not in df.columns:
            return df

        philly_mask = df["Creator"] == "Pennsylvania--Philadelphia"
        if not philly_mask.any():
            return df

        # Ensure columns exist
        for col in ["Bounding Box", "Geometry", "GeoNames"]:
            if col not in df.columns:
                df[col] = ""

        df.loc[philly_mask, "Bounding Box"] = "-75.280298,39.867005,-74.955832,40.13796"
        df.loc[philly_mask, ["Geometry", "GeoNames"]] = ""  # clear statewide defaults
        return df









