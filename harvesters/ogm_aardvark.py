import csv
import json
import logging
import os
import re
import time

import pandas as pd

from harvesters.base import BaseHarvester
from utils.dataframe_cleaner import (
    clean_date_ranges,
    clean_descriptions,
    deduplicate_rows_and_columns,
    strip_text_fields,
)
from utils.distribution_writer import generate_secondary_table, load_distribution_types
from utils.field_order import FIELD_ORDER, PRIMARY_FIELD_ORDER
from utils.spatial_cleaner import spatial_cleaning


class OgmAardvarkHarvester(BaseHarvester):
    PRIMARY_OUTPUT_EXCLUDED_FIELDS = {"dct_references_s", "gbl_mdVersion_s"}

    def __init__(self, config):
        super().__init__(config)
        self.repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.json_path = self._resolve_path(self.config["json_path"])
        self.source_field_map = {}
        self.schema_field_names = set()
        self.field_separators = {}
        self.distribution_variables = set()
        self.reference_uri_to_variables = {}
        self.extra_output_columns = []
        self._load_schema_mapping()

    def _load_schema_mapping(self, schema_path="schemas/geobtaa_schema.csv"):
        with open(self._resolve_path(schema_path), newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                field_name = row["name"]
                field_uri = row["field_uri"]
                separator = row.get("separator", "") or ""

                self.schema_field_names.add(field_name)
                self.field_separators[field_name] = separator
                self.source_field_map[field_name] = field_name
                if field_uri:
                    self.source_field_map[field_uri] = field_name

        self.source_field_map["id"] = "ID"

    def load_reference_data(self):
        self.distribution_types = load_distribution_types(
            self._resolve_path("schemas/distribution_types.yaml")
        )

        self.theme_map = {}
        themes_csv_path = self._resolve_path(
            self.config.get("themes_csv", "reference_data/themes.csv")
        )
        try:
            themes_df = pd.read_csv(themes_csv_path, dtype=str).fillna("")
            for _, row in themes_df.iterrows():
                theme = row["Theme"]
                keywords = row["Keyword"].split("|")
                for keyword in keywords:
                    clean_keyword = keyword.strip().lower()
                    if clean_keyword:
                        self.theme_map[clean_keyword] = theme
            print(
                f"[Base] Successfully loaded {len(self.theme_map)} theme keyword mappings."
            )
        except FileNotFoundError:
            print(
                f"[Base] Warning: Themes CSV not found at {themes_csv_path}. "
                "Themes will not be derived."
            )
        except Exception as exc:
            print(f"[Base] Error loading themes CSV: {exc}")

        self.distribution_variables = {
            variable
            for dist in self.distribution_types
            for variable in dist.get("variables", [])
        }

        uri_lookup = {}
        for dist in self.distribution_types:
            ref_uri = self._normalize_reference_uri(dist.get("reference_uri", ""))
            if not ref_uri:
                continue
            uri_lookup.setdefault(ref_uri, [])
            for variable in dist.get("variables", []):
                if variable not in uri_lookup[ref_uri]:
                    uri_lookup[ref_uri].append(variable)

        self.reference_uri_to_variables = uri_lookup

    def fetch(self):
        dataset = []
        for root, _, files in os.walk(self.json_path):
            for filename in sorted(files):
                if not filename.lower().endswith(".json"):
                    continue

                file_path = os.path.join(root, filename)
                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as handle:
                        dataset.append(json.load(handle))
                except json.JSONDecodeError as exc:
                    logging.warning(
                        "[OGM Aardvark] Failed to parse JSON at %s: %s",
                        file_path,
                        exc,
                    )

        return dataset

    def flatten(self, harvested_metadata):
        if not self.reference_uri_to_variables:
            self.load_reference_data()

        flattened = []
        for record in harvested_metadata:
            new_record = record.copy()
            references = self._parse_references(record.get("dct_references_s"), record.get("id", ""))
            for ref_uri, url in references.items():
                normalized_uri = self._normalize_reference_uri(ref_uri)
                for variable in self.reference_uri_to_variables.get(normalized_uri, []):
                    existing = new_record.get(variable)
                    if not existing:
                        new_record[variable] = url
                    elif isinstance(existing, list):
                        if url not in existing:
                            existing.append(url)
                    elif existing != url:
                        new_record[variable] = [existing, url]

            flattened.append(new_record)

        return flattened

    def build_dataframe(self, records):
        if not records:
            return pd.DataFrame()

        normalized_records = []
        extra_columns = []

        for record in records:
            if self._is_restricted_record(record):
                continue

            normalized_record = {}
            for key, value in record.items():
                target = self.source_field_map.get(key, key)

                if self._has_value(normalized_record.get(target)) and target != key:
                    continue

                normalized_record[target] = self._normalize_value(target, value)

                if (
                    target == key
                    and key not in self.schema_field_names
                    and key not in self.distribution_variables
                    and key not in extra_columns
                ):
                    extra_columns.append(key)

            normalized_records.append(normalized_record)

        self.extra_output_columns = extra_columns
        return pd.DataFrame(normalized_records)

    def derive_fields(self, df):
        if df.empty:
            return df

        existing_theme = None
        if "Theme" in df.columns:
            existing_theme = df["Theme"].fillna("")

        df = super().derive_fields(df)

        if existing_theme is not None:
            df["Theme"] = existing_theme.where(
                existing_theme.astype(str).str.strip() != "",
                df["Theme"].fillna(""),
            )

        if "Bounding Box" in df.columns:
            df["Bounding Box"] = df["Bounding Box"].apply(self._envelope_to_bbox)
        elif "Geometry" in df.columns:
            df["Bounding Box"] = df["Geometry"].apply(self._envelope_to_bbox)

        if "Geometry" in df.columns:
            df["Geometry"] = df.apply(
                lambda row: self._normalize_geometry(
                    row.get("Geometry", ""),
                    row.get("Bounding Box", ""),
                ),
                axis=1,
            )

        if "Date Range" not in df.columns:
            df["Date Range"] = ""

        df["Date Range"] = df.apply(self._populate_date_range, axis=1)
        return df

    def add_defaults(self, df):
        existing_access_rights = (
            df["Access Rights"].copy() if "Access Rights" in df.columns else None
        )
        df = super().add_defaults(df)
        if df.empty:
            return df

        if existing_access_rights is not None:
            df["Access Rights"] = existing_access_rights.where(
                existing_access_rights.astype(str).str.strip() != "",
                df["Access Rights"],
            )

        return df

    def add_provenance(self, df):
        df = super().add_provenance(df)
        if df.empty:
            return df

        today = time.strftime("%Y-%m-%d")
        endpoint_url = self.config.get("endpoint_url", self.json_path)
        endpoint_description = self.config.get(
            "endpoint_description",
            "OGM Aardvark JSON directory",
        )

        df["Website Platform"] = self.config.get("website_platform", "GeoBlacklight")
        df["Endpoint URL"] = endpoint_url
        df["Endpoint Description"] = endpoint_description
        df["Accrual Method"] = self.config.get("accrual_method", "Automated retrieval")
        df["Harvest Workflow"] = self.config.get("harvest_workflow", "py_ogm_aardvark")
        df["Provenance"] = (
            f"The metadata for this resource was harvested from {endpoint_description} "
            f"at {endpoint_url} on {today}."
        )

        accrual_periodicity = self.config.get("accrual_periodicity", "")
        if accrual_periodicity:
            df["Accrual Periodicity"] = accrual_periodicity

        return df

    def clean(self, df):
        if df.empty:
            return df

        before_cols = set(df.columns)
        df = (
            df.pipe(deduplicate_rows_and_columns)
            .pipe(strip_text_fields)
            .pipe(clean_descriptions)
            .pipe(clean_date_ranges)
            .pipe(self._reorder_columns_with_extras)
        )

        if "Bounding Box" in df.columns:
            df = spatial_cleaning(df)

        after_cols = list(df.columns)
        dropped = before_cols - set(after_cols)
        print(
            f"[CLEAN] Dataframe cleaning complete: {len(df)} rows, {len(after_cols)} cols. "
            f"Dropped-by-order: {len(dropped)} ({', '.join(sorted(dropped))[:200]}...)"
        )
        return df

    def validate(self, df):
        return super().validate(df)

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)

        today = time.strftime("%Y-%m-%d")
        output_dir = "outputs"
        os.makedirs(output_dir, exist_ok=True)

        primary_columns = [
            col
            for col in PRIMARY_FIELD_ORDER
            if col in primary_df.columns and col not in self.PRIMARY_OUTPUT_EXCLUDED_FIELDS
        ]
        primary_columns.extend(
            col
            for col in primary_df.columns
            if col not in PRIMARY_FIELD_ORDER
            and col not in self.PRIMARY_OUTPUT_EXCLUDED_FIELDS
            and col not in self.distribution_variables
            and col not in primary_columns
        )
        primary_df = primary_df.reindex(columns=primary_columns)

        primary_filename = os.path.join(
            output_dir,
            f"{today}_{os.path.basename(self.config['output_primary_csv'])}",
        )
        primary_df.to_csv(primary_filename, index=False, encoding="utf-8")

        results = {"primary_csv": primary_filename}

        if self.config.get("output_distributions_csv"):
            distributions_filename = os.path.join(
                output_dir,
                f"{today}_{os.path.basename(self.config['output_distributions_csv'])}",
            )
            distributions_df.to_csv(distributions_filename, index=False, encoding="utf-8")
            results["distributions_csv"] = distributions_filename

        return results

    @staticmethod
    def _normalize_reference_uri(uri):
        if not isinstance(uri, str):
            return ""
        return uri.rstrip("/")

    @staticmethod
    def _is_restricted_record(record):
        access_rights = str(
            record.get("dct_accessRights_s", record.get("Access Rights", "")) or ""
        ).strip()
        return access_rights.lower() == "restricted"

    def _resolve_path(self, path):
        if os.path.isabs(path):
            return path
        return os.path.join(self.repo_root, path)

    @staticmethod
    def _has_value(value):
        if value is None:
            return False
        if isinstance(value, list):
            return any(str(item).strip() for item in value)
        return str(value).strip() != ""

    def _parse_references(self, raw_references, record_id):
        if isinstance(raw_references, dict):
            return raw_references
        if not isinstance(raw_references, str) or not raw_references.strip():
            return {}

        try:
            return json.loads(raw_references)
        except json.JSONDecodeError:
            try:
                return json.loads(raw_references.replace('""', '"'))
            except json.JSONDecodeError as exc:
                logging.warning(
                    "[OGM Aardvark] Invalid JSON in dct_references_s for record %s: %s",
                    record_id,
                    exc,
                )
                return {}

    def _normalize_value(self, target, value):
        if isinstance(value, list):
            cleaned_values = [self._normalize_scalar_value(item) for item in value]
            cleaned_values = [item for item in cleaned_values if self._has_value(item)]

            if target in self.distribution_variables:
                return cleaned_values

            separator = self.field_separators.get(target)
            if not cleaned_values:
                return ""
            if separator:
                return separator.join(str(item) for item in cleaned_values)
            if not separator and len(cleaned_values) == 1:
                return cleaned_values[0]
            return "|".join(str(item) for item in cleaned_values)

        return self._normalize_scalar_value(value)

    @staticmethod
    def _normalize_scalar_value(value):
        if value is None:
            return ""
        if isinstance(value, bool):
            return str(value).lower()
        return value

    @staticmethod
    def _envelope_to_bbox(value):
        if not isinstance(value, str):
            return value

        text = value.strip()
        if not text.upper().startswith("ENVELOPE(") or not text.endswith(")"):
            return text

        try:
            west, east, north, south = [
                coord.strip()
                for coord in text[text.index("(") + 1 : -1].split(",")
            ]
            return f"{west},{south},{east},{north}"
        except ValueError:
            return text

    def _normalize_geometry(self, geometry, bbox):
        if isinstance(geometry, str):
            text = geometry.strip()
            if text and not text.upper().startswith("ENVELOPE("):
                return text

        bbox_text = bbox if isinstance(bbox, str) else ""
        if not bbox_text:
            return geometry

        try:
            west, south, east, north = [coord.strip() for coord in bbox_text.split(",")]
            return (
                f"POLYGON(({west} {north}, {east} {north}, {east} {south}, "
                f"{west} {south}, {west} {north}))"
            )
        except ValueError:
            return geometry

    @staticmethod
    def _populate_date_range(row):
        raw_existing = row.get("Date Range", "")
        if pd.isna(raw_existing):
            existing = ""
        else:
            existing = str(raw_existing or "").strip()

        if existing and existing.lower() != "nan":
            return existing

        temporal = str(row.get("Temporal Coverage", "") or "")
        years = re.findall(r"\b(?:19|20)\d{2}\b", temporal)
        if years:
            return f"{years[0]}-{years[-1]}"

        index_year = str(row.get("Index Year", "") or "").split("|")[0].strip()
        if index_year.isdigit() and len(index_year) == 4:
            return f"{index_year}-{index_year}"

        return existing

    def _reorder_columns_with_extras(self, df):
        ordered_columns = [col for col in FIELD_ORDER if col in df.columns]
        ordered_columns.extend(
            col
            for col in self.extra_output_columns
            if col in df.columns and col not in ordered_columns
        )
        ordered_columns.extend(
            col for col in df.columns if col not in ordered_columns
        )
        return df.reindex(columns=ordered_columns)
