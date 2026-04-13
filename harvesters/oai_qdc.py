import argparse
import csv
import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import yaml

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table
from utils.field_order import FIELD_ORDER, PRIMARY_FIELD_ORDER
from utils.resource_type_match import split_resource_type_values
from utils.spatial_match import (
    load_city_spatial_lookup,
    load_county_spatial_lookup,
    load_plss_bbox_lookup,
    load_state_spatial_lookup,
    match_city_spatial,
    match_county_spatial,
    match_plss_bbox,
    match_state_spatial,
)


class OaiQdcHarvester(BaseHarvester):
    """
    Harvester for qualified Dublin Core OAI-PMH records that have already been
    downloaded to local XML files.
    """

    OAI_NS = {
        "oai": "http://www.openarchives.org/OAI/2.0/",
        "dc": "http://purl.org/dc/elements/1.1/",
        "dcterms": "http://purl.org/dc/terms/",
        "oai_qdc": "http://worldcat.org/xmlschemas/qdc-1.0/",
    }
    NS_PREFIXES = {uri: prefix for prefix, uri in OAI_NS.items() if prefix != "oai"}

    def __init__(self, config):
        super().__init__(config)
        self.project_root = Path(__file__).resolve().parents[1]
        self.field_separators = {}
        self.distribution_fields = set()
        self.crosswalk_target_aliases = {}
        self.oai_base_url = self.config["oai_base_url"]
        self.metadata_prefix = self.config.get(
            "metadata_prefix",
            self.config.get("feed_type", "oai_qdc"),
        )
        self.source_name = self.config.get(
            "source_name",
            self.config.get("name", "the source"),
        )
        self.source_id_prefix = self.config.get(
            "source_id_prefix",
            self.oai_slugify(self.config.get("name", "oai")).replace("-", "_"),
        )
        self.id_set_spec_prefixes_to_strip = [
            self.oai_normalize_space(prefix)
            for prefix in self.config.get("id_set_spec_prefixes_to_strip", [])
            if self.oai_normalize_space(prefix)
        ]
        self.provider = self.config.get("provider", "")
        self.publisher = self.config.get("publisher", "")
        self.harvest_workflow = self.config.get("harvest_workflow", "py_oai_qdc")
        self.spatial_match_state = self.config.get("spatial_match_state", "").strip()
        self.spatial_cities_csv = self.config.get(
            "spatial_cities_csv",
            "reference_data/spatial_cities.csv",
        )
        self.spatial_counties_csv = self.config.get(
            "spatial_counties_csv",
            "reference_data/spatial_counties.csv",
        )
        self.spatial_plss_csv = self.config.get(
            "spatial_plss_csv",
            "reference_data/spatial_plss.csv",
        )
        self.spatial_states_csv = self.config.get(
            "spatial_states_csv",
            "reference_data/spatial_us_states.csv",
        )
        self.spatial_plss_state_abbr = self.config.get(
            "spatial_plss_state_abbr",
            "",
        ).strip()
        self.spatial_default_when_blank = self.config.get(
            "spatial_default_when_blank",
            "",
        ).strip()
        self.spatial_append_state = self.config.get(
            "spatial_append_state",
            "",
        ).strip()
        self.spatial_normalization_replacements = {
            self.oai_normalize_space(source): self.oai_normalize_space(target)
            for source, target in self.config.get(
                "spatial_normalization_replacements",
                {},
            ).items()
            if self.oai_normalize_space(source)
        }
        self.county_spatial_lookup = {}
        self.county_spatial_alias_lookup = {}
        self.city_spatial_lookup = {}
        self.city_spatial_alias_lookup = {}
        self.state_spatial_lookup = {}
        self.state_spatial_alias_lookup = {}
        self.plss_lookup = {}

        self.sets_csv = self.oai_resolve_path(self.config["sets_csv"])
        self.set_column = self.config.get("sets_csv_set_column", "set")
        self.set_title_column = self.config.get("sets_csv_title_column", "title")
        self.download_dir = self.oai_resolve_path(
            self.config.get(
                "oai_download_dir",
                Path("inputs") / "oai-downloads" / self.config.get("name", "oai_qdc"),
            )
        )

        self.crosswalk_csv = self.config.get("metadata_crosswalk_csv", "")
        self.crosswalk_config = self.config.get("metadata_crosswalk", [])
        self.crosswalk_has_header = bool(
            self.config.get("metadata_crosswalk_has_header", True)
        )
        self.crosswalk_source_column = self.config.get(
            "metadata_crosswalk_source_column",
            "source",
        )
        self.crosswalk_target_column = self.config.get(
            "metadata_crosswalk_target_column",
            "target",
        )
        self.hardcoded_values = self.config.get("hardcoded_values", {})
        self.clear_output_fields = self.config.get("clear_output_fields", [])
        self.title_prefix_append_values = self.config.get(
            "title_prefix_append_values",
            [],
        )
        self.identifier_prefix_field_map = self.config.get(
            "identifier_prefix_field_map",
            {},
        )
        self.crosswalk_mappings = []
        self.allowed_crosswalk_targets = set(FIELD_ORDER)
        self.oai_load_schema_metadata()

    def load_reference_data(self):
        super().load_reference_data()
        self.distribution_fields = {
            variable
            for dist in self.distribution_types or []
            for variable in dist.get("variables", [])
        }
        cities_path = self.oai_resolve_path(self.spatial_cities_csv)
        self.city_spatial_lookup, self.city_spatial_alias_lookup = (
            load_city_spatial_lookup(cities_path, self.spatial_match_state)
        )
        if self.spatial_match_state:
            print(
                f"[OAI_QDC] Loaded {len(self.city_spatial_lookup)} spatial city reference row(s) "
                f"for {self.spatial_match_state} from {cities_path}."
            )

        counties_path = self.oai_resolve_path(self.spatial_counties_csv)
        self.county_spatial_lookup, self.county_spatial_alias_lookup = (
            load_county_spatial_lookup(counties_path, self.spatial_match_state)
        )
        if self.spatial_match_state:
            print(
                f"[OAI_QDC] Loaded {len(self.county_spatial_lookup)} spatial county reference row(s) "
                f"for {self.spatial_match_state} from {counties_path}."
            )

        states_path = self.oai_resolve_path(self.spatial_states_csv)
        self.state_spatial_lookup, self.state_spatial_alias_lookup = (
            load_state_spatial_lookup(states_path)
        )
        print(
            f"[OAI_QDC] Loaded {len(self.state_spatial_lookup)} spatial state reference row(s) "
            f"from {states_path}."
        )

        plss_path = self.oai_resolve_path(self.spatial_plss_csv)
        self.plss_lookup = load_plss_bbox_lookup(plss_path, self.spatial_plss_state_abbr)
        if self.spatial_plss_state_abbr:
            print(
                f"[OAI_QDC] Loaded {len(self.plss_lookup)} PLSS lookup row(s) "
                f"for {self.spatial_plss_state_abbr.upper()} from {plss_path}."
            )
        self.crosswalk_target_aliases = self.oai_build_target_aliases()
        self.crosswalk_mappings = self.oai_load_crosswalk()

    def fetch(self):
        sets = self.oai_load_sets()
        print(
            f"[OAI_QDC] Loaded {len(sets)} set definitions from {self.sets_csv} "
            f"for {self.oai_base_url}"
        )
        if not sets:
            return []

        raw_pages = []
        missing_sets = []

        for set_row in sets:
            local_files = self.oai_local_xml_files(set_row["set_spec"])
            if not local_files:
                missing_sets.append(set_row["set_spec"])
                continue

            for xml_path in local_files:
                raw_pages.append(
                    {
                        "set_spec": set_row["set_spec"],
                        "set_title": set_row.get("set_title", ""),
                        "xml_path": str(xml_path),
                        "xml_text": xml_path.read_text(encoding="utf-8"),
                    }
                )

        if missing_sets:
            missing_list = ", ".join(missing_sets)
            raise FileNotFoundError(
                "[OAI_QDC] Local XML files were not found for set(s): "
                f"{missing_list}. Expected files under {self.download_dir}/<set-spec>/ . "
                "Run scripts/oai_download.py first or set oai_download_dir in the job config."
            )

        print(
            f"[OAI_QDC] Loaded {len(raw_pages)} local XML page(s) from {self.download_dir}."
        )
        return raw_pages

    def parse(self, raw_data):
        parsed_pages = []
        set_page_counts = {}
        set_record_counts = {}

        for bundle in raw_data:
            page_records = self.oai_parse_xml(bundle["xml_text"], bundle)
            parsed_pages.append(
                {
                    "set_spec": bundle["set_spec"],
                    "set_title": bundle.get("set_title", ""),
                    "xml_path": bundle["xml_path"],
                    "records": page_records,
                }
            )

            set_spec = bundle["set_spec"]
            set_page_counts[set_spec] = set_page_counts.get(set_spec, 0) + 1
            set_record_counts[set_spec] = set_record_counts.get(set_spec, 0) + len(page_records)

        for set_spec in sorted(set_record_counts):
            print(
                f"[OAI_QDC] Parsed {set_record_counts[set_spec]} records for set "
                f"{set_spec} from {set_page_counts[set_spec]} local XML page(s)."
            )

        return parsed_pages

    def flatten(self, harvested_metadata):
        flattened = []
        for parsed_page in harvested_metadata:
            flattened.extend(parsed_page.get("records", []))

        print(
            f"[OAI_QDC] Prepared {len(flattened)} records across "
            f"{len(harvested_metadata)} XML page(s)."
        )
        return flattened

    def build_dataframe(self, record_rows):
        if not record_rows:
            print("[OAI_QDC] No local OAI records found. Returning an empty dataframe.")
            return pd.DataFrame(columns=self.oai_output_columns())

        context_df = pd.DataFrame([self.oai_prepare_record_context(record) for record in record_rows])
        df = self.oai_map_to_schema(context_df)
        df = self.oai_apply_crosswalk(df, context_df)
        df = self.oai_route_identifier_values(df)
        df = self.oai_ensure_output_columns(df)
        print(
            f"[OAI_QDC] Crosswalked {len(df)} qualified Dublin Core records using "
            f"metadataPrefix={self.metadata_prefix}."
        )
        return df

    def derive_fields(self, df):
        df = super().derive_fields(df)
        df = self.oai_enrich_spatial_fields(df)
        return df

    def add_defaults(self, df):
        df = super().add_defaults(df)
        if df.empty:
            return df

        optional_defaults = {
            "Provider": self.provider,
            "Publisher": self.publisher,
            "Language": self.config.get("language", ""),
            "Code": self.config.get("code", ""),
            "Member Of": self.config.get("member_of", ""),
            "Is Part Of": self.config.get("is_part_of", ""),
            "Spatial Coverage": self.config.get("spatial_coverage", ""),
            "Bounding Box": self.config.get("bounding_box", ""),
            "Resource Class": self.config.get("default_resource_class", ""),
            "Resource Type": self.config.get("default_resource_type", ""),
        }

        for field, value in optional_defaults.items():
            if not value:
                continue
            if field not in df.columns:
                df[field] = value
                continue
            df[field] = df[field].where(df[field].astype(str).str.strip() != "", value)

        return df

    def add_provenance(self, df):
        df = super().add_provenance(df)
        if df.empty:
            return df

        today = time.strftime("%Y-%m-%d")
        df["Website Platform"] = self.config.get("website_platform", "OAI-PMH")
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = self.harvest_workflow
        df["Endpoint Description"] = "OAI-PMH"
        df["Endpoint URL"] = self.oai_base_url

        harvest_statement = (
            f"The metadata for this resource was last retrieved from "
            f"{self.source_name} on {today}."
        )
        if "Provenance" in df.columns:
            df["Provenance"] = df["Provenance"].apply(
                lambda value: f"{value}|{harvest_statement}" if value else harvest_statement
            )
        else:
            df["Provenance"] = harvest_statement

        df = self.oai_apply_hardcoded_values(df)
        df = self.oai_clear_output_fields(df)
        df = self.oai_apply_title_prefix_append_values(df)
        return df

    def clean(self, df):
        return super().clean(df)

    def validate(self, df):
        return super().validate(df)

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = self.oai_build_distributions(primary_df.copy())
        return super().write_outputs(primary_df, distributions_df)

    # --- OAI-Specific Functions --- #

    def oai_resolve_path(self, path_value):
        candidate = Path(path_value).expanduser()
        if candidate.is_absolute():
            return candidate
        return (self.project_root / candidate).resolve()

    def oai_output_columns(self):
        columns = list(PRIMARY_FIELD_ORDER)
        for mapping in self.crosswalk_mappings:
            target = mapping["target"]
            if target not in columns:
                columns.append(target)
        return columns

    def oai_load_schema_metadata(self):
        schema_path = self.project_root / "schemas" / "geobtaa_schema.csv"
        with schema_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                field_name = row["name"]
                self.field_separators[field_name] = row.get("separator", "") or ""

    def oai_build_target_aliases(self):
        aliases = {}

        for dist in self.distribution_types or []:
            variables = dist.get("variables", [])
            if len(variables) != 1:
                continue

            variable = variables[0]
            aliases[variable] = variable

            key = str(dist.get("key", "")).strip()
            name = str(dist.get("name", "")).strip()
            if key:
                aliases[key] = variable
            if name:
                aliases[name] = variable

        aliases.update(self.config.get("metadata_crosswalk_target_aliases", {}))
        return aliases

    def oai_load_sets(self):
        sets = []
        with self.sets_csv.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                set_spec = str(row.get(self.set_column, "")).strip()
                set_title = str(row.get(self.set_title_column, "")).strip()
                if not set_spec:
                    continue
                sets.append({"set_spec": set_spec, "set_title": set_title})
        return sets

    def oai_load_crosswalk(self):
        if self.crosswalk_config:
            mappings = self.oai_load_crosswalk_from_config(self.crosswalk_config)
            print(f"[OAI_QDC] Loaded {len(mappings)} inline crosswalk mappings from config.")
            return mappings

        if not self.crosswalk_csv:
            return []

        crosswalk_path = self.oai_resolve_path(self.crosswalk_csv)
        if not crosswalk_path.exists():
            raise FileNotFoundError(f"[OAI_QDC] Crosswalk CSV not found: {crosswalk_path}")

        mappings = []
        with crosswalk_path.open(newline="", encoding="utf-8-sig") as handle:
            if self.crosswalk_has_header:
                reader = csv.DictReader(handle)
                for row in reader:
                    source = self.oai_extract_crosswalk_cell(row, self.crosswalk_source_column)
                    target = self.oai_extract_crosswalk_cell(row, self.crosswalk_target_column)
                    mapping = self.oai_normalize_crosswalk_mapping(source, target)
                    if mapping is not None:
                        mappings.append(mapping)
            else:
                reader = csv.reader(handle)
                for row in reader:
                    source = self.oai_extract_crosswalk_cell(row, self.crosswalk_source_column)
                    target = self.oai_extract_crosswalk_cell(row, self.crosswalk_target_column)
                    mapping = self.oai_normalize_crosswalk_mapping(source, target)
                    if mapping is not None:
                        mappings.append(mapping)

        print(
            f"[OAI_QDC] Loaded {len(mappings)} crosswalk mappings from {crosswalk_path}."
        )
        return mappings

    def oai_enrich_spatial_fields(self, df):
        if df.empty or "Spatial Coverage" not in df.columns:
            return df

        df["Spatial Coverage"] = df["Spatial Coverage"].apply(
            self.oai_normalize_spatial_field
        )

        city_matches = df["Spatial Coverage"].apply(
            lambda value: match_city_spatial(
                self.oai_deserialize_values("Spatial Coverage", value),
                self.city_spatial_lookup,
                self.city_spatial_alias_lookup,
            )
        )
        plss_matches = df["Spatial Coverage"].apply(
            lambda value: match_plss_bbox(
                self.oai_deserialize_values("Spatial Coverage", value),
                self.plss_lookup,
            )
        )
        spatial_matches = df["Spatial Coverage"].apply(
            lambda value: match_county_spatial(
                self.oai_deserialize_values("Spatial Coverage", value),
                self.county_spatial_lookup,
                self.county_spatial_alias_lookup,
            )
        )
        state_matches = df["Spatial Coverage"].apply(
            lambda value: match_state_spatial(
                self.oai_deserialize_values("Spatial Coverage", value),
                self.state_spatial_lookup,
                self.state_spatial_alias_lookup,
            )
        )

        existing_bboxes = df.get("Bounding Box", pd.Series([""] * len(df)))
        df["Bounding Box"] = [
            self.oai_normalize_space(existing_value)
            or plss_match.get("bounding_box", "")
            or city_match.get("bounding_box", "")
            or county_match.get("bounding_box", "")
            or state_match.get("bounding_box", "")
            for existing_value, city_match, county_match, plss_match, state_match in zip(
                existing_bboxes,
                city_matches,
                spatial_matches,
                plss_matches,
                state_matches,
            )
        ]

        existing_geometry = df.get("Geometry", pd.Series([""] * len(df)))
        df["Geometry"] = [
            self.oai_normalize_space(existing_value)
            or city_match.get("geometry", "")
            or (
                ""
                if plss_match.get("has_plss")
                else county_match.get("geometry", "") or state_match.get("geometry", "")
            )
            for existing_value, city_match, county_match, plss_match, state_match in zip(
                existing_geometry,
                city_matches,
                spatial_matches,
                plss_matches,
                state_matches,
            )
        ]

        existing_geonames = df.get("GeoNames", pd.Series([""] * len(df)))
        df["GeoNames"] = [
            self.oai_normalize_space(existing_value)
            or city_match.get("geonames", "")
            or county_match.get("geonames", "")
            or state_match.get("geonames", "")
            for existing_value, city_match, county_match, state_match in zip(
                existing_geonames,
                city_matches,
                spatial_matches,
                state_matches,
            )
        ]

        return df

    def oai_load_crosswalk_from_config(self, crosswalk_config):
        mappings = []

        if isinstance(crosswalk_config, dict):
            for source, target in crosswalk_config.items():
                mapping = self.oai_normalize_crosswalk_mapping(source, target)
                if mapping is not None:
                    mappings.append(mapping)
            return mappings

        if not isinstance(crosswalk_config, list):
            raise ValueError(
                "[OAI_QDC] metadata_crosswalk must be a list of mappings or a dict."
            )

        for entry in crosswalk_config:
            if isinstance(entry, dict):
                source = entry.get("source", "")
                target = entry.get("target", "")
                mode = entry.get("mode", "replace")
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                source, target = entry[0], entry[1]
                mode = entry[2] if len(entry) >= 3 else "replace"
            else:
                raise ValueError(
                    "[OAI_QDC] Each metadata_crosswalk entry must provide source and target."
                )

            mapping = self.oai_normalize_crosswalk_mapping(source, target, mode=mode)
            if mapping is not None:
                mappings.append(mapping)

        return mappings

    def oai_extract_crosswalk_cell(self, row, column):
        if isinstance(row, dict):
            if isinstance(column, int):
                values = list(row.values())
                if 0 <= column < len(values):
                    return values[column]
                return ""
            return row.get(str(column), "")

        if isinstance(column, str):
            try:
                column = int(column)
            except ValueError:
                return ""

        if 0 <= column < len(row):
            return row[column]
        return ""

    def oai_normalize_crosswalk_mapping(self, source, target, mode="replace"):
        source_key = self.oai_normalize_space(source)
        target_key = self.oai_normalize_space(target)
        if not source_key or not target_key:
            return None

        mode_key = self.oai_normalize_space(mode or "replace").lower()
        if mode_key not in {"replace", "append"}:
            raise ValueError(
                f"[OAI_QDC] Unsupported crosswalk mode '{mode}'. Use 'replace' or 'append'."
            )

        resolved_target = self.crosswalk_target_aliases.get(target_key, target_key)
        if resolved_target not in self.allowed_crosswalk_targets:
            print(
                f"[OAI_QDC] Skipping crosswalk target '{target_key}' "
                f"(resolved as '{resolved_target}') because it is not a known output field."
            )
            return None

        return {"source": source_key, "target": resolved_target, "mode": mode_key}

    def oai_normalize_spatial_field(self, value):
        raw_values = self.oai_deserialize_values("Spatial Coverage", value)
        normalized_values = self.oai_normalize_spatial_values(raw_values)
        if not normalized_values and self.spatial_default_when_blank:
            normalized_values = [self.spatial_default_when_blank]
        normalized_values = self.oai_append_spatial_state(normalized_values)
        return self.oai_serialize_values("Spatial Coverage", normalized_values)

    def oai_normalize_spatial_values(self, values):
        normalized_values = []
        for value in values:
            normalized_values.extend(self.oai_normalize_spatial_value(value))
        return self.oai_unique(normalized_values)

    def oai_normalize_spatial_value(self, value):
        clean_value = self.oai_normalize_space(value)
        if not clean_value:
            return []

        parts = [
            self.oai_normalize_spatial_part(part)
            for part in re.split(r"\s*--\s*", clean_value)
            if self.oai_normalize_spatial_part(part)
        ]
        if parts and parts[0].lower() == "united states":
            parts = parts[1:]

        if not parts:
            return []

        if len(parts) == 1:
            single_value = parts[0]
            if self.spatial_match_state and single_value.endswith(" County"):
                return [f"{self.spatial_match_state}--{single_value}"]
            return [single_value]

        parent_place = parts[0]
        child_parts = parts[1:]
        if child_parts[0].endswith("County") and len(child_parts) > 1:
            return [
                f"{parent_place}--{child_parts[0]}",
                f"{parent_place}--{'--'.join(child_parts[1:])}",
            ]

        return [f"{parent_place}--{'--'.join(child_parts)}"]

    def oai_normalize_spatial_part(self, value):
        clean_value = self.oai_normalize_space(value)
        if not clean_value:
            return ""
        return self.spatial_normalization_replacements.get(clean_value, clean_value)

    def oai_append_spatial_state(self, values):
        if not self.spatial_append_state:
            return values

        append_state = self.oai_normalize_space(self.spatial_append_state)
        if not append_state:
            return values

        normalized_values = self.oai_unique(values)
        append_state_lower = append_state.lower()
        if any(value.lower() == append_state_lower for value in normalized_values):
            return normalized_values

        state_pattern = re.compile(rf"\b{re.escape(append_state_lower)}\b")
        if any(
            value.lower().startswith(f"{append_state_lower}--")
            or state_pattern.search(value.lower())
            for value in normalized_values
        ):
            normalized_values.append(append_state)

        return self.oai_unique(normalized_values)

    def oai_build_distributions(self, df):
        if df.empty:
            return pd.DataFrame(
                columns=["friendlier_id", "reference_type", "distribution_url", "label"]
            )

        landing_base = self.config.get("distribution_landing_base", "").rstrip("/")
        iiif_base = self.config.get("distribution_iiif_base", "").rstrip("/")
        manifest_builder = self.config.get("distribution_manifest_builder", "").strip()

        if "information" not in df.columns:
            df["information"] = ""
        if "manifest" not in df.columns:
            df["manifest"] = ""

        if landing_base:
            df["information"] = df.apply(
                lambda row: row.get("information", "")
                or self.oai_build_distribution_url(row, landing_base, "landing"),
                axis=1,
            )

        if manifest_builder:
            df["manifest"] = df.apply(
                lambda row: row.get("manifest", "")
                or self.oai_build_custom_distribution_url(
                    row,
                    manifest_builder,
                    "manifest",
                ),
                axis=1,
            )
        elif iiif_base:
            df["manifest"] = df.apply(
                lambda row: row.get("manifest", "")
                or self.oai_build_distribution_url(row, iiif_base, "manifest"),
                axis=1,
            )

        return generate_secondary_table(df, self.distribution_types)

    def oai_build_custom_distribution_url(self, row, builder_name, distribution_type):
        builder = getattr(self, builder_name, None)
        if not callable(builder):
            raise ValueError(
                f"[OAI_QDC] Distribution builder '{builder_name}' is not defined."
            )
        return builder(row, distribution_type)

    def oai_build_distribution_url(self, row, base_url, distribution_type):
        set_slug, record_number = self.oai_distribution_parts(row)
        if not set_slug or not record_number:
            return ""

        if distribution_type == "landing":
            return f"{base_url}/{set_slug}/id/{record_number}"
        if distribution_type == "manifest":
            return f"{base_url}/{set_slug}:{record_number}/manifest.json"
        return ""

    def oai_build_iowa_library_manifest_url(self, row, distribution_type):
        if distribution_type != "manifest":
            return ""

        node_number = self.oai_extract_iowa_library_node_number(row)
        if not node_number:
            return ""

        return f"https://digital.lib.uiowa.edu/node/{node_number}/iiif-p/manifest"

    def oai_local_xml_files(self, set_spec):
        set_dir = self.download_dir / self.oai_slugify(set_spec)
        if not set_dir.exists():
            return []
        return sorted(path for path in set_dir.glob("*.xml") if path.is_file())

    def oai_ensure_output_columns(self, df):
        for column in self.oai_output_columns():
            if column not in df.columns:
                df[column] = ""
        return df

    def oai_prepare_record_context(self, record):
        title_values = self.oai_values(record, "dc:title", "dcterms:title")
        alternative_title_values = self.oai_values(
            record,
            "dcterms:alternative",
            "dc:alternative",
        )
        creator_values = self.oai_split_people(
            self.oai_values(record, "dc:creator", "dcterms:creator")
        )
        contributor_values = self.oai_split_people(
            self.oai_values(record, "dc:contributor", "dcterms:contributor")
        )
        subject_values = self.oai_values(record, "dc:subject", "dcterms:subject")
        type_values = self.oai_values(record, "dc:type", "dcterms:type")
        identifier_values = self.oai_values(record, "dc:identifier", "dcterms:identifier")
        all_identifiers = self.oai_unique([record.get("oai_identifier", "")] + identifier_values)
        wxs_identifier_values = self.oai_wxs_identifiers(identifier_values)
        landing_page = self.oai_select_landing_page(all_identifiers)
        date_values = self.oai_values(record, "dc:date", "dcterms:date", "dcterms:created")
        temporal_values = self.oai_values(record, "dcterms:temporal")
        spatial_values = self.oai_values(record, "dcterms:spatial", "dc:coverage")
        scale_values = [
            value for value in spatial_values if self.oai_looks_like_scale(value)
        ]
        format_values = self.oai_values(record, "dc:format", "dcterms:format")
        publisher_values = self.oai_values(record, "dc:publisher", "dcterms:publisher")
        rights_values = self.oai_values(record, "dc:rights", "dcterms:rights")
        provenance_values = self.oai_values(record, "dcterms:provenance")
        relation_values = self.oai_values(record, "dc:relation", "dcterms:relation")
        is_part_of_values = self.oai_values(record, "dcterms:isPartOf")
        source_values = self.oai_values(record, "dc:source", "dcterms:source")
        description_values = self.oai_values(record, "dc:description", "dcterms:description")
        language_values = self.oai_values(record, "dc:language", "dcterms:language")
        spatial_coverage = self.oai_spatial_coverage(spatial_values)

        return {
            "record": record,
            "title_values": title_values,
            "alternative_title_values": alternative_title_values,
            "creator_values": creator_values,
            "contributor_values": contributor_values,
            "subject_values": subject_values,
            "type_values": type_values,
            "identifier_values": identifier_values,
            "all_identifiers": all_identifiers,
            "wxs_identifier_values": wxs_identifier_values,
            "landing_page": landing_page,
            "date_values": date_values,
            "temporal_values": temporal_values,
            "spatial_values": spatial_values,
            "spatial_coverage": spatial_coverage,
            "bounding_box": self.oai_extract_bbox_from_values(spatial_values),
            "scale_values": scale_values,
            "format_values": format_values,
            "publisher_values": publisher_values,
            "rights_values": rights_values,
            "provenance_values": provenance_values,
            "relation_values": relation_values,
            "is_part_of_values": is_part_of_values,
            "source_values": source_values,
            "description_values": description_values,
            "language_values": language_values,
            "record_id": self.oai_build_id(record, landing_page),
            "resource_class": self.oai_resource_class(
                type_values,
                record.get("set_title", ""),
            ),
            "local_collection": self.oai_local_collection(record, is_part_of_values),
            "temporal_coverage": self.oai_build_temporal_coverage(temporal_values, date_values),
            "date_issued": self.oai_date_issued(date_values),
            "date_range": self.oai_date_range(temporal_values, date_values),
            "format": self.oai_format(format_values, all_identifiers),
            "file_size": self.oai_file_size(format_values),
        }

    def oai_map_to_schema(self, df):
        alternative_titles = df.apply(
            lambda row: self.oai_unique(row["title_values"][1:] + row["alternative_title_values"]),
            axis=1,
        )
        resource_type_split = df["type_values"].apply(split_resource_type_values)
        resource_type_values = resource_type_split.apply(lambda pair: pair[0])
        keyword_values = resource_type_split.apply(lambda pair: pair[1])

        output_data = {
            "ID": df["record_id"],
            "Title": df["title_values"].apply(lambda values: values[0] if values else ""),
            "Alternative Title": alternative_titles.apply(
                lambda values: self.oai_serialize_values("Alternative Title", values)
            ),
            "Description": df["description_values"].apply(
                lambda values: self.oai_serialize_values("Description", values)
            ),
            "Language": df["language_values"].apply(
                lambda values: self.oai_serialize_values("Language", values)
            ),
            "Creator": df["creator_values"].apply(
                lambda values: self.oai_serialize_values("Creator", values)
            ),
            "Publisher": df["publisher_values"].apply(
                lambda values: self.oai_serialize_values("Publisher", values)
            ),
            "Provider": self.provider,
            "Resource Class": df["resource_class"],
            "Resource Type": resource_type_values.apply(
                lambda values: self.oai_serialize_values(
                    "Resource Type",
                    values,
                )
            ),
            "Subject": df["subject_values"].apply(
                lambda values: self.oai_serialize_values("Subject", values)
            ),
            "Keyword": keyword_values.apply(
                lambda values: self.oai_serialize_values("Keyword", values)
            ),
            "Local Collection": df["local_collection"],
            "Temporal Coverage": df["temporal_coverage"],
            "Date Issued": df["date_issued"],
            "Date Range": df["date_range"],
            "Spatial Coverage": df["spatial_coverage"].apply(
                lambda values: self.oai_serialize_values("Spatial Coverage", values)
            ),
            "Bounding Box": df["bounding_box"],
            "Spatial Resolution as Text": df["scale_values"].apply(
                lambda values: self.oai_serialize_values("Spatial Resolution as Text", values)
            ),
            "Provenance": df["provenance_values"].apply(
                lambda values: self.oai_serialize_values("Provenance", values)
            ),
            "Identifier": df["all_identifiers"].apply(
                lambda values: self.oai_serialize_values("Identifier", values)
            ),
            "WxS Identifier": df["wxs_identifier_values"].apply(
                lambda values: self.oai_serialize_values("WxS Identifier", values)
            ),
            "Rights": df["rights_values"].apply(
                lambda values: self.oai_serialize_values("Rights", values)
            ),
            "Format": df["format"],
            "File Size": df["file_size"],
            "Source": df["source_values"].apply(
                lambda values: self.oai_serialize_values("Source", values)
            ),
            "Is Part Of": df["is_part_of_values"].apply(
                lambda values: self.oai_serialize_values("Is Part Of", values)
            ),
            "information": df["landing_page"],
        }

        return pd.DataFrame(output_data)

    def oai_apply_hardcoded_values(self, df):
        if df.empty or not self.hardcoded_values:
            return df

        for field_name, value in self.hardcoded_values.items():
            if field_name not in df.columns:
                df[field_name] = ""
            df[field_name] = value

        return df

    def oai_clear_output_fields(self, df):
        if df.empty or not self.clear_output_fields:
            return df

        for field_name in self.clear_output_fields:
            if field_name not in df.columns:
                df[field_name] = ""
            df[field_name] = ""

        return df

    def oai_apply_title_prefix_append_values(self, df):
        if df.empty or not self.title_prefix_append_values or "Title" not in df.columns:
            return df

        title_series = df["Title"].fillna("").astype(str)

        for rule in self.title_prefix_append_values:
            if not isinstance(rule, dict):
                continue

            title_prefix = self.oai_normalize_space(
                rule.get("title_startswith", rule.get("title_prefix", ""))
            ).lower()
            append_values = rule.get("append_values", rule.get("values", {}))

            if not title_prefix or not isinstance(append_values, dict):
                continue

            matching_rows = title_series.str.lower().str.startswith(title_prefix)
            if not matching_rows.any():
                continue

            for field_name, raw_value in append_values.items():
                values_to_append = self.oai_deserialize_values(field_name, raw_value)
                if not values_to_append:
                    continue

                if field_name not in df.columns:
                    df[field_name] = ""

                df.loc[matching_rows, field_name] = df.loc[matching_rows, field_name].apply(
                    lambda existing_value: self.oai_serialize_values(
                        field_name,
                        self.oai_deserialize_values(field_name, existing_value)
                        + values_to_append,
                    )
                )

        return df

    def oai_apply_crosswalk(self, schema_df, context_df):
        if not self.crosswalk_mappings:
            return schema_df

        for mapping in self.crosswalk_mappings:
            target = mapping["target"]
            if mapping.get("mode") == "append":
                schema_df[target] = [
                    self.oai_serialize_values(
                        target,
                        self.oai_deserialize_values(target, existing_value)
                        + self.oai_crosswalk_source_values(context_row, mapping["source"]),
                    )
                    for existing_value, (_, context_row) in zip(
                        schema_df.get(target, pd.Series([""] * len(schema_df))),
                        context_df.iterrows(),
                    )
                ]
                continue

            schema_df[target] = context_df.apply(
                lambda row: self.oai_serialize_values(
                    target,
                    self.oai_crosswalk_source_values(row, mapping["source"]),
                ),
                axis=1,
            )

        return schema_df

    def oai_route_identifier_values(self, df):
        if (
            df.empty
            or "Identifier" not in df.columns
            or not isinstance(self.identifier_prefix_field_map, dict)
            or not self.identifier_prefix_field_map
        ):
            return df

        normalized_rules = []
        for target_field, raw_prefixes in self.identifier_prefix_field_map.items():
            clean_field = self.oai_normalize_space(target_field)
            if not clean_field:
                continue

            if isinstance(raw_prefixes, str):
                raw_prefixes = [raw_prefixes]
            elif not isinstance(raw_prefixes, list):
                continue

            prefixes = [
                self.oai_normalize_space(prefix)
                for prefix in raw_prefixes
                if self.oai_normalize_space(prefix)
            ]
            if prefixes:
                normalized_rules.append((clean_field, prefixes))

        if not normalized_rules:
            return df

        for target_field, _ in normalized_rules:
            if target_field not in df.columns:
                df[target_field] = ""

        updated_identifiers = []
        routed_values_by_field = {target_field: [] for target_field, _ in normalized_rules}

        for _, row in df.iterrows():
            remaining_identifiers = []
            routed_for_row = {target_field: [] for target_field, _ in normalized_rules}

            for value in self.oai_deserialize_values("Identifier", row.get("Identifier", "")):
                matched_field = None
                lowered_value = value.lower()
                for target_field, prefixes in normalized_rules:
                    if any(lowered_value.startswith(prefix.lower()) for prefix in prefixes):
                        matched_field = target_field
                        break

                if matched_field is None:
                    remaining_identifiers.append(value)
                    continue

                routed_for_row[matched_field].append(value)

            updated_identifiers.append(
                self.oai_serialize_values("Identifier", remaining_identifiers)
            )

            for target_field, _ in normalized_rules:
                existing_values = self.oai_deserialize_values(
                    target_field,
                    row.get(target_field, ""),
                )
                routed_values_by_field[target_field].append(
                    self.oai_serialize_values(
                        target_field,
                        existing_values + routed_for_row[target_field],
                    )
                )

        df["Identifier"] = updated_identifiers
        for target_field, values in routed_values_by_field.items():
            df[target_field] = values

        return df

    def oai_crosswalk_source_values(self, context, source_key):
        source_key = self.oai_normalize_space(source_key)
        record = context["record"]

        special_sources = {
            "identifier": [context["landing_page"] or record.get("oai_identifier", "")],
            "oai_identifier": [record.get("oai_identifier", "")],
            "datestamp": [record.get("datestamp", "")],
            "set": [record.get("set_spec", "")],
            "set_spec": [record.get("set_spec", "")],
            "set_title": [record.get("set_title", "")],
            "landing_page": [context["landing_page"]],
        }
        if source_key in special_sources:
            return self.oai_unique(special_sources[source_key])

        candidates = [source_key]
        if ":" not in source_key:
            candidates.extend([f"dc:{source_key}", f"dcterms:{source_key}"])

        values = []
        for candidate in candidates:
            values.extend(self.oai_values(record, candidate))

        return self.oai_unique(values)

    def oai_serialize_values(self, target, values):
        cleaned_values = self.oai_unique(values if isinstance(values, list) else [values])
        if not cleaned_values:
            return ""

        if target in self.distribution_fields:
            return cleaned_values[0] if len(cleaned_values) == 1 else cleaned_values

        separator = self.field_separators.get(target, "")
        if separator:
            return separator.join(str(item) for item in cleaned_values)
        if len(cleaned_values) == 1:
            return cleaned_values[0]
        return "|".join(str(item) for item in cleaned_values)

    def oai_deserialize_values(self, target, value):
        if isinstance(value, list):
            return self.oai_unique(value)

        text = self.oai_normalize_space(value)
        if not text:
            return []

        separator = self.field_separators.get(target, "")
        if separator:
            return self.oai_unique(text.split(separator))

        return [text]

    def oai_parse_xml(self, xml_text, set_row):
        root = ET.fromstring(xml_text)

        errors = []
        for error in root.findall(".//oai:error", self.OAI_NS):
            errors.append(
                f"{error.attrib.get('code', 'oai_error')}: {(error.text or '').strip()}"
            )
        if errors:
            raise ValueError("[OAI_QDC] OAI-PMH error(s): " + "; ".join(errors))

        record_rows = []
        for record_el in root.findall(".//oai:record", self.OAI_NS):
            parsed_record = self.oai_parse_record(record_el, set_row)
            if parsed_record is not None:
                record_rows.append(parsed_record)

        return record_rows

    def oai_parse_record(self, record_el, set_row):
        header_el = record_el.find("oai:header", self.OAI_NS)
        if header_el is not None and header_el.attrib.get("status") == "deleted":
            return None

        metadata_el = record_el.find("oai:metadata", self.OAI_NS)
        if metadata_el is None:
            return None

        qualifieddc_el = metadata_el.find("oai_qdc:qualifieddc", self.OAI_NS)
        if qualifieddc_el is None:
            return None

        fields = {}
        for child in list(qualifieddc_el):
            key = self.oai_tag_name(child.tag)
            if not key:
                continue
            text = self.oai_normalize_space("".join(child.itertext()))
            if not text:
                continue
            fields.setdefault(key, []).append(text)

        return {
            "oai_identifier": self.oai_text(header_el, "oai:identifier"),
            "datestamp": self.oai_text(header_el, "oai:datestamp"),
            "set_spec": set_row["set_spec"],
            "set_title": set_row.get("set_title", ""),
            "source_xml_path": set_row.get("xml_path", ""),
            "fields": fields,
        }

    def oai_text(self, parent, xpath):
        if parent is None:
            return ""
        child = parent.find(xpath, self.OAI_NS)
        if child is None or child.text is None:
            return ""
        return child.text.strip()

    def oai_tag_name(self, tag):
        if not tag.startswith("{"):
            return ""
        namespace, local_name = tag[1:].split("}", 1)
        prefix = self.NS_PREFIXES.get(namespace)
        if not prefix:
            return ""
        return f"{prefix}:{local_name}"

    def oai_values(self, record, *keys):
        raw_fields = record.get("fields", {})
        values = []
        for key in keys:
            values.extend(raw_fields.get(key, []))
        return self.oai_unique(values)

    def oai_unique(self, values):
        seen = set()
        unique_values = []
        for value in values:
            clean_value = self.oai_normalize_space(value)
            if not clean_value or clean_value in seen:
                continue
            seen.add(clean_value)
            unique_values.append(clean_value)
        return unique_values

    def oai_normalize_space(self, value):
        return re.sub(r"\s+", " ", str(value or "")).strip()

    def oai_split_people(self, values):
        split_values = []
        for value in values:
            parts = [
                part.strip(" .")
                for part in re.split(r"\s*;\s*", value)
                if part.strip()
            ]
            split_values.extend(parts or [value])
        return self.oai_unique(split_values)

    def oai_select_landing_page(self, identifiers):
        http_identifiers = [
            value
            for value in identifiers
            if value.lower().startswith(("http://", "https://"))
        ]
        preferred_patterns = ("/cdm/ref/", "/collection/", "/node/")
        for pattern in preferred_patterns:
            for value in http_identifiers:
                if pattern in value:
                    return value
        for value in http_identifiers:
            if "_foxml" not in value.lower():
                return value
        return ""

    def oai_extract_iowa_library_node_number(self, row):
        candidates = [
            row.get("information", ""),
            row.get("Documentation (External)", ""),
            row.get("Identifier", ""),
            row.get("ID", ""),
        ]

        for candidate in candidates:
            for value in str(candidate or "").split("|"):
                match = re.search(
                    r"https?://digital\.lib\.uiowa\.edu/node/(\d+)\b",
                    value.strip(),
                )
                if match:
                    return match.group(1)

        return ""

    def oai_distribution_parts(self, row):
        identifier_values = str(row.get("Identifier", "")).split("|")
        for value in identifier_values:
            match = re.search(r"/collection/([^/]+)/id/([^/?#]+)", value)
            if match:
                return match.group(1), match.group(2)

        friendlier_id = str(row.get("ID", ""))
        prefix = f"{self.source_id_prefix}_"
        if friendlier_id.startswith(prefix):
            suffix = friendlier_id[len(prefix):]
            if "_" in suffix:
                set_slug, record_number = suffix.rsplit("_", 1)
                return set_slug, record_number

        return "", ""

    def oai_build_id(self, record, landing_page):
        set_part = self.oai_build_id_set_part(record.get("set_spec", ""))
        raw_identifier = (
            self.oai_extract_record_number(landing_page)
            or self.oai_extract_record_number(record.get("oai_identifier", ""))
            or self.oai_slugify(
                landing_page or record.get("oai_identifier", "")
            ).replace("-", "_")
        )
        return f"{self.source_id_prefix}_{set_part}_{raw_identifier}".strip("_")

    def oai_build_id_set_part(self, set_spec):
        raw_set_spec = self.oai_normalize_space(set_spec)
        if not raw_set_spec:
            return ""

        for prefix in self.id_set_spec_prefixes_to_strip:
            raw_set_spec = re.sub(
                rf"^{re.escape(prefix)}[:_\-\s]+",
                "",
                raw_set_spec,
                flags=re.IGNORECASE,
            )

        return self.oai_slugify(raw_set_spec).replace("-", "_")

    def oai_extract_record_number(self, value):
        text = str(value or "").strip()
        if not text:
            return ""

        patterns = [
            r"/id/([A-Za-z0-9]+)\b",
            r"/node/([A-Za-z0-9-]+)\b",
            r":([A-Za-z]+-)?([A-Za-z0-9]+)$",
            r"\b([A-Za-z]+-)?([A-Za-z0-9]+)$",
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                candidate = match.group(match.lastindex)
                return self.oai_slugify(candidate).replace("-", "_")
        return ""

    def oai_date_issued(self, dates):
        return dates[0] if dates else ""

    def oai_build_temporal_coverage(self, temporal_values, dates):
        if temporal_values:
            return "|".join(temporal_values)

        years = self.oai_years_from_values(dates)
        if not years:
            return ""
        if len(years) == 1:
            return years[0]
        return f"{years[0]}-{years[-1]}"

    def oai_date_range(self, temporal_values, dates):
        source_values = temporal_values if temporal_values else dates
        years = self.oai_years_from_values(source_values)
        if not years:
            return ""
        return f"{years[0]}-{years[-1]}"

    def oai_years_from_values(self, values):
        years = set()
        for value in values:
            for match in re.findall(r"\b(1[6-9]\d{2}|20\d{2}|2100)\b", value):
                years.add(match)
        return sorted(years)

    def oai_spatial_coverage(self, spatial_values):
        coverage = []
        for value in spatial_values:
            if self.oai_looks_like_scale(value):
                continue
            if self.oai_extract_bbox(value):
                continue
            if re.fullmatch(r"[tr]\d+[nsew]", value.lower()):
                continue
            coverage.append(value)
        return self.oai_unique(coverage)

    def oai_extract_bbox_from_values(self, spatial_values):
        for value in spatial_values:
            bbox = self.oai_extract_bbox(value)
            if bbox:
                return bbox
        return ""

    def oai_extract_bbox(self, value):
        clean_value = self.oai_normalize_space(value)
        envelope_match = re.match(r"ENVELOPE\(([^)]+)\)", clean_value, flags=re.IGNORECASE)
        if envelope_match:
            coords = [part.strip() for part in envelope_match.group(1).split(",")]
            if len(coords) == 4:
                west, east, north, south = coords
                return f"{west},{south},{east},{north}"

        bbox_match = re.match(
            r"^\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*$",
            clean_value,
        )
        if not bbox_match:
            return ""

        west, south, east, north = [float(part) for part in bbox_match.groups()]
        if (
            -180 <= west <= 180
            and -90 <= south <= 90
            and -180 <= east <= 180
            and -90 <= north <= 90
        ):
            return f"{west},{south},{east},{north}"
        return ""

    def oai_looks_like_scale(self, value):
        return bool(re.search(r"\b1\s*:\s*\d", value))

    def oai_resource_class(self, types, set_title):
        type_text = " ".join(types).lower()
        set_text = self.oai_normalize_space(set_title).lower()

        if "collection" in type_text:
            return "Collections"
        if any(term in type_text for term in ["aerial", "orthophoto", "satellite", "imagery"]):
            return "Imagery"
        if any(term in type_text for term in ["map", "maps", "cartograph", "stillimage", "image"]):
            return "Maps"
        if any(term in type_text for term in ["dataset", "data set", "tabular"]):
            return "Datasets"
        if any(term in type_text for term in ["website", "interactive resource", "web site"]):
            return "Websites"
        if "collection" in set_text:
            return "Collections"
        return "Other"

    def oai_local_collection(self, record, is_part_of_values):
        if is_part_of_values:
            return "|".join(is_part_of_values)
        if record.get("set_title"):
            return record["set_title"]
        return ""

    def oai_wxs_identifiers(self, identifier_values):
        return self.oai_unique(
            value
            for value in identifier_values
            if self.oai_normalize_space(value).lower().startswith("islandora:")
        )

    def oai_format(self, format_values, identifiers):
        extension_map = {
            ".tif": "TIFF",
            ".tiff": "TIFF",
            ".jpg": "JPEG",
            ".jpeg": "JPEG",
            ".jp2": "JPEG2000",
            ".png": "PNG",
            ".pdf": "PDF",
            ".geojson": "GeoJSON",
            ".json": "GeoJSON",
            ".zip": "Files",
        }

        format_text = " ".join(format_values).lower()
        for extension, mapped_format in extension_map.items():
            for identifier in identifiers:
                if identifier.lower().endswith(extension):
                    return mapped_format

        if "paper map" in format_text:
            return "TIFF"
        if "pdf" in format_text:
            return "PDF"
        if "jpeg" in format_text:
            return "JPEG"
        if "tiff" in format_text or "tif" in format_text:
            return "TIFF"
        return ""

    def oai_file_size(self, format_values):
        for value in format_values:
            clean_value = self.oai_normalize_space(value)
            if clean_value.isdigit():
                return clean_value
        return ""

    def oai_slugify(self, value):
        value = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip())
        value = re.sub(r"-{2,}", "-", value)
        return value.strip("-") or "unnamed"


def build_arg_parser():
    parser = argparse.ArgumentParser(description="Run the OAI QDC harvester.")
    parser.add_argument(
        "config",
        help="Path to the YAML config file for the OAI QDC job.",
    )
    return parser


def load_config(config_path):
    with open(config_path, encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def main():
    args = build_arg_parser().parse_args()
    config = load_config(args.config)
    harvester = OaiQdcHarvester(config)
    harvester.harvest_pipeline()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
