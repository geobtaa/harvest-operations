import os
import time
import pandas as pd

from utils.field_order import PRIMARY_FIELD_ORDER  
from utils.dataframe_cleaner import dataframe_cleaning
from utils.spatial_cleaner import spatial_cleaning
from utils.validation import validation_pipeline
from utils.distribution_writer import load_distribution_types
from utils.derive_themes import derive_themes_from_keywords

class BaseHarvester:
    def __init__(self, config):
        """
        Initialize harvester with a config dictionary. Should include paths to input files and output locations.
        """
        self.config = config
        self.distribution_types = None
        self.theme_map = {} # Initialize theme_map

    def load_reference_data(self):
        """
        Load shared lookup tables and reference data needed for harvesting.
        This now includes distribution types and the theme keyword map.
        """
        self.distribution_types = load_distribution_types()
        
        # Load and prepare theme data for all harvesters
        themes_csv_path = self.config.get("themes_csv", "reference_data/themes.csv")
        try:
            themes_df = pd.read_csv(themes_csv_path, dtype=str).fillna('')
            
            for _, row in themes_df.iterrows():
                theme = row['Theme']
                keywords = row['Keyword'].split('|')
                
                for keyword in keywords:
                    clean_keyword = keyword.strip().lower()
                    if clean_keyword:
                        self.theme_map[clean_keyword] = theme
            
            # Add a success message to confirm the map was loaded.
            print(f"[Base] Successfully loaded {len(self.theme_map)} theme keyword mappings.")

        except FileNotFoundError:
            print(f"[Base] Warning: Themes CSV not found at {themes_csv_path}. Themes will not be derived.")
        except Exception as e:
            print(f"[Base] Error loading themes CSV: {e}")

    def fetch(self):
        """
        REQUIRED: Retrieve raw metadata from a source (file, API, etc.).
        Should return either a list (of records) or a generator.
        """
        raise NotImplementedError("Subclasses must implement fetch()")

    def parse(self, raw_data):
        """
        Default passthrough. Parsing is for unstructured formats, like HTML.
        """
        return raw_data

    def flatten(self, harvested_metadata):
        """
        Default passthrough. Use if records are nested and need flattening to 1 row per record.
        """
        return harvested_metadata

    def build_dataframe(self, parsed_or_flattened_data):
        """
        REQUIRED: Convert a list of dicts into a Pandas DataFrame. Also responsible for mapping source fields to target schema.
        """
        raise NotImplementedError("Subclasses must implement build_dataframe()")


    def derive_fields(self, df):
        """
        Override to add specific transformations.
        """
        df = derive_themes_from_keywords(df, self.theme_map)

        return df

    def add_defaults(self, df):
        """
        Add static default values required by schema
        Override in subclasses if needed
        """
        df['Publication State'] = 'published'
        df['Access Rights'] = 'Public'
        return df
    
    def add_provenance(self, df):
        """
        Add harvest metadata (e.g. timestamp, harvester name) for internal tracking.
        Override in subclasses if more detailed provenance is needed.
        """
        today = time.strftime('%Y-%m-%d')
        df['Date Accessioned'] = today
        return df


    def clean(self, df):
        """
        Shared cleaning logic—removes extra pipes, trims whitespace, deduplicates, etc.
        Override in subclasses for source-specific cleaning.
        """
        df = dataframe_cleaning(df)
        df = spatial_cleaning(df)
        return df


    def validate(self, df):
        """
        Shared validation pipeline—logs errors or enforces required fields.
        Override to skip or customize rules.
        """
        validation_pipeline(df)
        return df


    def write_outputs(self, primary_df: pd.DataFrame, distributions_df: pd.DataFrame = None) -> dict:
        """
        Write the primary and optional distributions DataFrames to CSVs in the outputs directory.
        Returns a dict of written file paths for logging or downstream use.
        """
        today = time.strftime("%Y-%m-%d")
        results = {}

        # Create outputs directory if it doesn't exist
        output_dir = "outputs"
        os.makedirs(output_dir, exist_ok=True)

        # Write primary CSV
        primary_out = self.config["output_primary_csv"]
        primary_filename = os.path.join(output_dir, f"{today}_{os.path.basename(primary_out)}")

        # Reorder columns to match schema field order
        primary_df = primary_df.reindex(
            columns=[col for col in PRIMARY_FIELD_ORDER if col in primary_df.columns]
        )
        primary_df.to_csv(primary_filename, index=False, encoding="utf-8")
        results["primary_csv"] = primary_filename

        # Write distributions CSV, if present
        if distributions_df is not None and self.config.get("output_distributions_csv"):
            distributions_out = self.config["output_distributions_csv"]
            distributions_filename = os.path.join(output_dir, f"{today}_{os.path.basename(distributions_out)}")

            distributions_df.to_csv(distributions_filename, index=False, encoding="utf-8")
            results["distributions_csv"] = distributions_filename

        return results


    def harvest_pipeline(self):
        """
        Main pipeline orchestrator. Runs all steps in order and writes output files.
        Subclasses can override specific steps but should not need to modify this method itself.
        """
        self.load_reference_data()
        raw = self.fetch()
        parsed = self.parse(raw)
        flat = self.flatten(parsed)
        df = self.build_dataframe(flat)

        df = (
            df.pipe(self.derive_fields)
              .pipe(self.add_defaults)
              .pipe(self.add_provenance)
              .pipe(self.clean)
              .pipe(self.validate)
        )

        results = self.write_outputs(df)
        print(f"[Pipeline] Harvest complete: {results}")
        return results