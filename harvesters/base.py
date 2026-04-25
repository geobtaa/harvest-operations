import os
import time
from pathlib import Path
import pandas as pd

from scripts.build_uploads import (
    build_filename_regex,
    discover_dated_files,
    most_recent_file_before,
    run_build_uploads_for_current,
)
from utils.field_order import PRIMARY_FIELD_ORDER  
from utils.output_naming import infer_upload_source_prefix
from utils.dataframe_cleaner import dataframe_cleaning
from utils.spatial_cleaner import spatial_cleaning
from utils.validation import validation_pipeline
from utils.distribution_writer import load_distribution_types
from utils.derive_themes import derive_themes_from_keywords

class BaseHarvester:
    def __init__(self, config):
        """
        Store the harvester configuration and initialize shared state used across
        the pipeline, including distribution metadata and keyword-to-theme mappings.
        """
        self.config = config
        self.distribution_types = None
        self.theme_map = {} # Initialize theme_map

    def load_reference_data(self):
        """
        Load lookup tables that are reused by many harvesters, including
        distribution type definitions and the keyword map used to derive themes.
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
        Retrieve raw records from the source system, such as a local file tree,
        web endpoint, or API. Subclasses must implement this source-specific step.
        """
        raise NotImplementedError("Subclasses must implement fetch()")

    def parse(self, raw_data):
        """
        Convert raw harvested content into a more structured form when needed.
        The base implementation is a passthrough for sources that are already structured.
        """
        return raw_data

    def flatten(self, harvested_metadata):
        """
        Normalize nested source records into a flat record-per-row shape before
        building a dataframe. The base implementation leaves already-flat data unchanged.
        """
        return harvested_metadata

    def build_dataframe(self, parsed_or_flattened_data):
        """
        Transform parsed records into a pandas DataFrame and map source fields
        into the target output schema. Subclasses must implement this mapping step.
        """
        raise NotImplementedError("Subclasses must implement build_dataframe()")


    def derive_fields(self, df):
        """
        Apply shared derived-field logic after the initial dataframe is built.
        By default this derives themes from configured keyword mappings.
        """
        df = derive_themes_from_keywords(df, self.theme_map)

        return df

    def add_defaults(self, df):
        """
        Populate schema-required fields with standard default values when the
        source does not provide them. Subclasses can extend or replace these defaults.
        """
        df['Publication State'] = 'published'
        df['Access Rights'] = 'Public'
        return df
    
    def add_provenance(self, df):
        """
        Add internal provenance metadata that records when the harvest ran.
        Subclasses can append source-specific provenance details to this baseline.
        """
        today = time.strftime('%Y-%m-%d')
        df['Date Accessioned'] = today
        return df


    def clean(self, df):
        """
        Run the shared dataframe cleanup routines that normalize values, fix
        spatial fields, and remove common formatting issues before validation.
        """
        df = dataframe_cleaning(df)
        df = spatial_cleaning(df)
        return df


    def validate(self, df):
        """
        Run the shared validation pipeline to check required fields and record-level
        quality issues before output files are written.
        """
        validation_pipeline(df)
        return df


    def write_outputs(self, primary_df: pd.DataFrame, distributions_df: pd.DataFrame = None) -> dict:
        """
        Write the finalized primary table and optional distributions table to
        dated CSV files in the outputs directory, then return their file paths.
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

    def build_uploads(self, results: dict) -> dict | None:
        """
        Optionally build upload delta files by comparing the current output
        against the most recent prior output for this harvester's source prefix.
        """
        if not self.config.get("build_uploads"):
            return None

        if not self.config.get("output_distributions_csv"):
            return {
                "status": "skipped",
                "reason": "build_uploads requires output_distributions_csv in config.",
            }

        primary_csv = results.get("primary_csv")
        if not primary_csv:
            return {
                "status": "skipped",
                "reason": "build_uploads requires a primary_csv result from write_outputs().",
            }

        distributions_csv = results.get("distributions_csv")
        if not distributions_csv:
            return {
                "status": "skipped",
                "reason": "build_uploads requires a distributions_csv result from write_outputs().",
            }

        primary_path = Path(primary_csv).resolve()
        distributions_path = Path(distributions_csv).resolve()
        outputs_dir = Path(primary_csv).resolve().parent
        source = infer_upload_source_prefix(self.config.get("output_primary_csv", ""))
        primary_candidates = discover_dated_files(
            outputs_dir,
            build_filename_regex(source, "primary"),
        )

        if most_recent_file_before(primary_candidates, primary_path) is None:
            return {
                "status": "skipped",
                "reason": (
                    f"No prior dated primary output found for '{source}'; "
                    "skipping upload deltas."
                ),
            }

        summary = run_build_uploads_for_current(
            source,
            outputs_dir,
            primary_path,
            distributions_path,
            outputs_dir / "to_upload",
        )
        return {
            "status": "created",
            "source": source,
            "primary_upload_csv": str(summary["primary_upload_path"]),
            "distributions_new_csv": str(summary["dist_new_path"]),
            "distributions_delete_csv": str(summary["dist_delete_path"]),
            "new_count": summary["new_count"],
            "retired_count": summary["retired_count"],
            "distribution_new_count": summary["distribution_new_count"],
            "distribution_delete_count": summary["distribution_delete_count"],
            "changed_distribution_ids": sorted(summary["changed_distribution_ids"]),
        }

    def harvest_pipeline(self):
        """
        Orchestrate the full harvest workflow from reference-data loading through
        output writing, calling each pipeline stage in the expected order.
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
        upload_summary = self.build_uploads(results)
        if upload_summary is not None:
            results["upload_summary"] = upload_summary
        print(f"[Pipeline] Harvest complete: {results}")
        return results
