import csv
import hashlib
import json
import logging
import random
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse
from xml.etree import ElementTree as ET

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from harvesters.base import BaseHarvester
from utils.derive_themes import derive_themes_from_keywords
from utils.field_order import PRIMARY_FIELD_ORDER


LOGGER = logging.getLogger(__name__)
US_STATE_ABBREVIATIONS = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
}
BROAD_PLACE_KEYS = {
    "pa",
    "pennsylvania",
    "usa",
    "united states",
    "united states of america",
    "u s",
}


class PasdaHarvester(BaseHarvester):
    def __init__(self, config):
        config = dict(config)
        config.setdefault("build_uploads", True)
        config.setdefault("metadata_base_url", "https://www.pasda.psu.edu/metadata/")
        config.setdefault("download_base_url", "https://www.pasda.psu.edu/download/")
        config.setdefault("json_base_url", "https://www.pasda.psu.edu/json/")
        config.setdefault("source_manifest", "metadata_directory")
        config.setdefault("cache_dir", "inputs/pasda/metadata_xml")
        config.setdefault("output_dir", "outputs/pasda")
        config.setdefault("registry_dir", "registry")
        config.setdefault("metadata_registry_path", "registry/pasda_metadata_registry.csv")
        config.setdefault("normalized_registry_path", "registry/pasda_normalized_registry.jsonl")
        config.setdefault("incremental", True)
        config.setdefault("use_registry", True)
        config.setdefault("build_download_inventory", False)
        config.setdefault("build_json_inventory", False)
        config.setdefault("download_inventory_max_depth", 4)
        config.setdefault("download_inventory_max_pages", 500)
        config.setdefault("download_inventory_request_delay_seconds", 0)
        config.setdefault("json_inventory_max_depth", 0)
        config.setdefault("json_inventory_max_pages", 100)
        config.setdefault("json_inventory_request_delay_seconds", 0)
        config.setdefault("aardvark_ready_only", True)
        config.setdefault("sample_strategy", "first")
        config.setdefault("sample_seed", 42)
        config.setdefault("timeout", 30)
        config.setdefault("user_agent", "harvest-operations PASDA metadata harvester")
        super().__init__(config)
        self.inventory_rows = []
        self.manifest_rows = []
        self.normalized_records = []
        self.error_rows = []
        self.profile_summary = []
        self.spatial_data = pd.DataFrame()
        self.metadata_registry = {}
        self.normalized_registry = {}
        self.download_inventory_rows = []
        self.json_inventory_rows = []

    def load_reference_data(self):
        super().load_reference_data()
        spatial_counties_csv = self.config.get(
            "spatial_counties_csv",
            "reference_data/spatial_counties.csv",
        )
        try:
            self.spatial_data = pd.read_csv(spatial_counties_csv, dtype=str).fillna("")
        except FileNotFoundError:
            print(
                "[PASDA] Warning: spatial counties CSV not found at "
                f"{spatial_counties_csv}. County spatial coverage normalization will be skipped."
            )
            self.spatial_data = pd.DataFrame()

    def fetch(self):
        if self.config.get("use_registry", True):
            self.metadata_registry = load_pasda_metadata_registry(
                self.config.get("metadata_registry_path")
            )
            self.normalized_registry = load_pasda_normalized_registry(
                self.config.get("normalized_registry_path")
            )
        session = build_pasda_session(self.config.get("user_agent", "harvest-operations"))
        metadata_base_url = self.config["metadata_base_url"]
        timeout = int(self.config.get("timeout", 30))
        harvested_at = utc_now()
        cache_dir = Path(self.config["cache_dir"])
        output_dir = Path(self.config["output_dir"])
        cache_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"[PASDA] Cache directory ready: {cache_dir}")
        print(f"[PASDA] Output directory ready: {output_dir}")
        print(f"[PASDA] Fetching metadata directory listing: {metadata_base_url}")
        LOGGER.info("Fetching PASDA metadata directory listing: %s", metadata_base_url)
        response = session.get(metadata_base_url, timeout=timeout)
        response.raise_for_status()

        inventory_rows = parse_metadata_directory_listing(
            response.text,
            metadata_base_url,
            harvested_at=harvested_at,
        )
        sample_size = sample_size_from_config(self.config)
        sample_strategy = sample_strategy_from_config(self.config)
        manifest_rows = select_metadata_sample(
            inventory_rows,
            sample_size=sample_size,
            sample_strategy=sample_strategy,
            sample_seed=int(self.config.get("sample_seed", 42)),
        )
        self.inventory_rows = mark_inventory_sample(
            inventory_rows,
            manifest_rows,
            sample_strategy=sample_strategy,
        )

        if sample_size:
            print(
                f"[PASDA] Found {len(inventory_rows)} XML links; "
                f"selected {len(manifest_rows)} using '{sample_strategy}'."
            )
        else:
            print(f"[PASDA] Found {len(inventory_rows)} XML links; downloading all records.")

        if self.config.get("build_download_inventory", False):
            print(f"[PASDA] Building download inventory: {self.config['download_base_url']}")
            self.download_inventory_rows = inventory_pasda_directory_tree(
                session=session,
                base_url=self.config["download_base_url"],
                source_manifest="download_directory",
                timeout=timeout,
                harvested_at=harvested_at,
                max_depth=int(self.config.get("download_inventory_max_depth", 4)),
                max_pages=int(self.config.get("download_inventory_max_pages", 5000)),
                request_delay_seconds=float(
                    self.config.get("download_inventory_request_delay_seconds", 0) or 0
                ),
            )
            print(f"[PASDA] Download inventory files found: {len(self.download_inventory_rows)}")

        if self.config.get("build_json_inventory", False):
            print(f"[PASDA] Building GeoJSON inventory: {self.config['json_base_url']}")
            self.json_inventory_rows = inventory_pasda_directory_tree(
                session=session,
                base_url=self.config["json_base_url"],
                source_manifest="json_directory",
                timeout=timeout,
                harvested_at=harvested_at,
                max_depth=int(self.config.get("json_inventory_max_depth", 0)),
                max_pages=int(self.config.get("json_inventory_max_pages", 100)),
                file_extensions={".geojson", ".json"},
                request_delay_seconds=float(
                    self.config.get("json_inventory_request_delay_seconds", 0) or 0
                ),
            )
            print(f"[PASDA] GeoJSON inventory files found: {len(self.json_inventory_rows)}")

        fetched_rows = []
        total_records = len(manifest_rows)
        for index, row in enumerate(manifest_rows, start=1):
            fetched_row = prepare_registry_metadata_row(
                row,
                metadata_registry=self.metadata_registry,
                normalized_registry=self.normalized_registry,
            )
            if fetched_row is None:
                fetched_row = fetch_and_cache_metadata_xml(
                    row,
                    session=session,
                    cache_dir=cache_dir,
                    timeout=timeout,
                    incremental=bool(self.config.get("incremental", True)),
                    metadata_registry_entry=self.metadata_registry.get(
                        clean_text(row.get("metadata_filename", ""))
                    ),
                )
            fetched_rows.append(fetched_row)
            print(
                "[PASDA] "
                f"{index}/{total_records} {fetched_row['metadata_filename']}: "
                f"{fetched_row['xml_fetch_status']}"
            )

        print(f"[PASDA] Prepared manifest with {len(fetched_rows)} XML records.")
        LOGGER.info("Prepared PASDA manifest with %s XML records", len(fetched_rows))
        return fetched_rows

    def parse(self, raw_data):
        normalized_records = []
        manifest_rows = []
        error_rows = []

        for row in raw_data:
            if row.get("xml_fetch_status") == "registry":
                manifest_row, normalized_record = registry_pasda_manifest_row(
                    row,
                    normalized_registry=self.normalized_registry,
                )
            else:
                manifest_row, normalized_record = parse_pasda_manifest_row(row)
            manifest_rows.append(manifest_row)
            normalized_records.append(normalized_record)
            if manifest_row.get("xml_fetch_status") == "failed" or manifest_row.get(
                "xml_parse_status"
            ) in {"malformed", "failed"}:
                error_rows.append(build_error_row(manifest_row, normalized_record))

        self.manifest_rows = manifest_rows
        self.normalized_records = normalized_records
        self.error_rows = error_rows
        self.profile_summary = build_profile_summary(manifest_rows)
        return normalized_records

    def flatten(self, harvested_metadata):
        return harvested_metadata

    def build_dataframe(self, parsed_or_flattened_data):
        return pd.DataFrame(parsed_or_flattened_data)

    def derive_fields(self, df):
        return df

    def add_defaults(self, df):
        return df

    def add_provenance(self, df):
        return df

    def clean(self, df):
        return df

    def validate(self, df):
        required_columns = {
            "source_system",
            "source_record_id",
            "metadata_filename",
            "metadata_url",
            "metadata_profile",
            "xml_parse_status",
        }
        missing = required_columns - set(df.columns)
        if missing:
            raise ValueError(f"[PASDA] Missing normalized columns: {', '.join(sorted(missing))}")
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        del distributions_df

        today = time.strftime("%Y-%m-%d")
        output_dir = Path(self.config["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)

        inventory_dir = output_dir / "inventory"
        normalized_dir = output_dir / "normalized"
        upload_dir = output_dir / "upload"
        review_dir = output_dir / "review"
        reports_dir = output_dir / "reports"
        for subdir in [inventory_dir, normalized_dir, upload_dir, review_dir, reports_dir]:
            subdir.mkdir(parents=True, exist_ok=True)

        inventory_path = inventory_dir / f"{today}_pasda_directory_inventory.csv"
        manifest_path = inventory_dir / f"{today}_pasda_metadata_manifest.csv"
        download_inventory_path = inventory_dir / f"{today}_pasda_download_inventory.csv"
        json_inventory_path = inventory_dir / f"{today}_pasda_json_inventory.csv"
        normalized_jsonl_path = normalized_dir / f"{today}_pasda_normalized_records.jsonl"
        normalized_csv_path = normalized_dir / f"{today}_pasda_normalized_records.csv"
        aardvark_draft_path = upload_dir / f"{today}_pasda_aardvark_draft.csv"
        distributions_path = upload_dir / f"{today}_pasda_distributions.csv"
        asset_match_review_path = review_dir / f"{today}_pasda_asset_match_review.csv"
        series_review_path = review_dir / f"{today}_pasda_series_review.csv"
        unparsed_matched_review_path = review_dir / f"{today}_pasda_unparsed_matched_review.csv"
        new_records_review_path = review_dir / f"{today}_pasda_new_records_review.csv"
        changed_records_review_path = review_dir / f"{today}_pasda_changed_records_review.csv"
        deleted_records_review_path = review_dir / f"{today}_pasda_deleted_records_review.csv"
        errors_path = reports_dir / f"{today}_pasda_error_report.csv"
        profile_summary_path = reports_dir / f"{today}_pasda_profile_summary.csv"

        write_csv_rows(inventory_path, self.inventory_rows)
        write_csv_rows(manifest_path, self.manifest_rows)
        write_jsonl(normalized_jsonl_path, self.normalized_records)
        primary_df.to_csv(normalized_csv_path, index=False, encoding="utf-8")
        asset_match_review_rows = []
        series_review_rows = []
        unparsed_matched_review_rows = []
        if self.download_inventory_rows or self.json_inventory_rows:
            asset_match_review_rows = build_pasda_asset_match_review_records(
                self.normalized_records,
                download_inventory_rows=self.download_inventory_rows,
                json_inventory_rows=self.json_inventory_rows,
            )
            series_review_rows = build_pasda_series_review_records(
                self.normalized_records,
                asset_match_review_rows=asset_match_review_rows,
            )
            unparsed_matched_review_rows = build_pasda_unparsed_matched_review_records(
                self.normalized_records,
                asset_match_review_rows=asset_match_review_rows,
            )
        county_lookup = build_pasda_county_lookup(self.spatial_data)
        aardvark_draft_df = build_pasda_aardvark_draft_dataframe(
            self.normalized_records,
            county_lookup=county_lookup,
            theme_map=self.theme_map,
            asset_match_review_rows=asset_match_review_rows,
            series_review_rows=series_review_rows,
            ready_only=bool(asset_match_review_rows)
            and bool(self.config.get("aardvark_ready_only", True)),
        )
        aardvark_draft_df.to_csv(aardvark_draft_path, index=False, encoding="utf-8")
        new_records_review_rows, changed_records_review_rows = (
            build_pasda_new_changed_record_review_rows(
                aardvark_draft_df.to_dict("records"),
                normalized_records=self.normalized_records,
                existing_metadata_registry=self.metadata_registry,
            )
        )
        write_csv_rows(new_records_review_path, new_records_review_rows)
        write_csv_rows(changed_records_review_path, changed_records_review_rows)
        deleted_records_review_rows = build_pasda_deleted_record_review_rows(
            existing_metadata_registry=self.metadata_registry,
            inventory_rows=self.inventory_rows,
        )
        write_csv_rows(deleted_records_review_path, deleted_records_review_rows)
        if self.config.get("build_download_inventory", False):
            write_csv_rows(download_inventory_path, self.download_inventory_rows)
        if self.config.get("build_json_inventory", False):
            write_csv_rows(json_inventory_path, self.json_inventory_rows)
        if self.download_inventory_rows or self.json_inventory_rows:
            write_csv_rows(asset_match_review_path, asset_match_review_rows)
            write_csv_rows(series_review_path, series_review_rows)
            write_csv_rows(unparsed_matched_review_path, unparsed_matched_review_rows)
            distribution_rows = build_pasda_distribution_records(
                self.normalized_records,
                asset_match_review_rows=asset_match_review_rows,
            )
            write_csv_rows(distributions_path, distribution_rows)
        write_csv_rows(errors_path, self.error_rows)
        write_csv_rows(profile_summary_path, self.profile_summary)
        if self.config.get("use_registry", True):
            metadata_registry_path = Path(self.config["metadata_registry_path"])
            normalized_registry_path = Path(self.config["normalized_registry_path"])
            metadata_registry_rows = build_pasda_metadata_registry_rows(
                existing_registry=self.metadata_registry,
                inventory_rows=self.inventory_rows,
                manifest_rows=self.manifest_rows,
                normalized_records=self.normalized_records,
                seen_at=today,
            )
            normalized_registry_records = build_pasda_normalized_registry_records(
                existing_registry=self.normalized_registry,
                normalized_records=self.normalized_records,
            )
            write_csv_rows(metadata_registry_path, metadata_registry_rows)
            write_jsonl(normalized_registry_path, normalized_registry_records)

        results = {
            "directory_inventory_csv": str(inventory_path),
            "manifest_csv": str(manifest_path),
            "normalized_jsonl": str(normalized_jsonl_path),
            "normalized_csv": str(normalized_csv_path),
            "aardvark_draft_csv": str(aardvark_draft_path),
            "new_records_review_csv": str(new_records_review_path),
            "changed_records_review_csv": str(changed_records_review_path),
            "deleted_records_review_csv": str(deleted_records_review_path),
            "error_report_csv": str(errors_path),
            "profile_summary_csv": str(profile_summary_path),
        }
        if self.config.get("build_download_inventory", False):
            results["download_inventory_csv"] = str(download_inventory_path)
        if self.config.get("build_json_inventory", False):
            results["json_inventory_csv"] = str(json_inventory_path)
        if self.download_inventory_rows or self.json_inventory_rows:
            results["asset_match_review_csv"] = str(asset_match_review_path)
            results["series_review_csv"] = str(series_review_path)
            results["distributions_csv"] = str(distributions_path)
            results["unparsed_matched_review_csv"] = str(unparsed_matched_review_path)
        if self.config.get("use_registry", True):
            results["metadata_registry_csv"] = self.config["metadata_registry_path"]
            results["normalized_registry_jsonl"] = self.config["normalized_registry_path"]
        LOGGER.info("PASDA metadata-directory harvest outputs written: %s", results)
        return results

    def build_uploads(self, results: dict) -> dict | None:
        del results
        return None


# Custom functions for this harvester


NORMALIZED_FIELDS = [
    "source_system",
    "source_record_id",
    "metadata_filename",
    "metadata_url",
    "title",
    "alternate_title",
    "abstract",
    "purpose",
    "status",
    "creator",
    "publisher",
    "provider",
    "distributor",
    "contact_org",
    "contact_person",
    "contact_email",
    "metadata_contact_org",
    "metadata_contact_email",
    "publication_date",
    "issued",
    "modified",
    "temporal_start",
    "temporal_end",
    "metadata_date",
    "west_bbox",
    "east_bbox",
    "south_bbox",
    "north_bbox",
    "geometry_type",
    "spatial_reference",
    "place_keywords",
    "theme_keywords",
    "iso_topic_categories",
    "resource_type",
    "data_format",
    "native_data_set_environment",
    "online_links",
    "distribution_links",
    "download_links_found_in_metadata",
    "service_links_found_in_metadata",
    "license_or_use_constraints",
    "access_constraints",
    "use_constraints",
    "lineage",
    "source_scale",
    "metadata_standard_name",
    "metadata_standard_version",
    "metadata_profile",
    "metadata_profile_confidence",
    "xml_parse_status",
    "parse_warnings",
    "parse_error",
    "raw_xml_path",
    "xml_sha256",
]

PASDA_REGISTRY_VERSION = "1"
PASDA_METADATA_REGISTRY_FIELDS = [
    "metadata_filename",
    "metadata_url",
    "source_record_id",
    "pasda_record_id",
    "metadata_last_modified",
    "metadata_size_bytes",
    "xml_sha256",
    "metadata_profile",
    "metadata_profile_confidence",
    "xml_fetch_status",
    "xml_parse_status",
    "parse_error",
    "first_seen",
    "last_seen",
    "last_parsed",
    "registry_version",
]

PASDA_ASSET_MATCH_REVIEW_FIELDS = [
    "source_record_id",
    "metadata_filename",
    "title",
    "metadata_url",
    "pasda_asset_match_status",
    "pasda_asset_match_level",
    "pasda_distribution_candidate",
    "metadata_download_links",
    "exact_asset_urls",
    "geojson_exact_count",
    "geojson_exact_urls",
    "download_exact_count",
    "download_exact_urls",
    "geojson_normalized_count",
    "geojson_normalized_urls",
    "download_normalized_count",
    "download_normalized_urls",
    "metadata_archive_link_count",
    "metadata_archive_links",
    "archive_directory_candidate_count",
    "archive_directory_candidate_urls",
    "supplemental_asset_urls",
    "best_asset_url",
    "best_asset_filename",
    "best_asset_source_manifest",
    "best_asset_kind",
    "match_method",
    "confidence",
    "candidate_count",
    "review_flag",
]
PASDA_ASSET_MATCH_URL_LIMIT = 25
PASDA_ASSET_EXTENSION_RANK = {
    ".geojson": 0,
    ".json": 1,
    ".zip": 2,
    ".7z": 3,
    ".gz": 4,
    ".tar": 5,
    ".tgz": 6,
    ".gpkg": 7,
    ".gdb": 8,
    ".shp": 9,
    ".kml": 10,
    ".kmz": 11,
    ".csv": 12,
    ".tif": 13,
    ".tiff": 14,
    ".sid": 15,
}

PASDA_SERIES_REVIEW_FIELDS = [
    "series_key",
    "series_title",
    "series_status",
    "record_count",
    "record_ids",
    "metadata_filenames",
    "inferred_record_dates",
    "ready_count",
    "review_count",
    "no_asset_count",
    "exact_asset_count",
    "exact_assets_with_archive_count",
    "archive_only_count",
    "normalized_count",
    "most_recently_found_record_id",
    "most_recently_found_date",
    "most_recently_found_match_status",
    "most_recently_found_distribution_candidate",
    "latest_ready_record_id",
    "latest_ready_date",
    "title_needs_date_qualifier",
    "shared_title_count",
    "unique_titles",
    "series_currentness_note",
]
PASDA_SERIES_CURRENTNESS_NOTE = (
    "Treat all PASDA series members as historical snapshots. "
    "Most recently found is based on inferred metadata/file dates and is not a currentness claim; "
    "check PASDA or the creator for the most current data."
)
PASDA_DISTRIBUTION_FIELDS = [
    "friendlier_id",
    "reference_type",
    "distribution_url",
    "label",
]
PASDA_UNPARSED_MATCH_REVIEW_FIELDS = [
    "source_record_id",
    "public_id",
    "metadata_filename",
    "metadata_url",
    "raw_xml_path",
    "xml_fetch_status",
    "xml_parse_status",
    "parse_error",
    "title",
    "pasda_distribution_candidate",
    "pasda_asset_match_status",
    "pasda_asset_match_level",
    "match_method",
    "confidence",
    "best_asset_url",
    "supplemental_asset_urls",
]

FGDC_TAGS = {
    "idinfo",
    "citation",
    "citeinfo",
    "descript",
    "spdom",
    "bounding",
    "distinfo",
    "metainfo",
}

ARCGIS_TAGS = {
    "esri",
    "arcgisformat",
    "dataproperties",
    "idcitation",
    "restitle",
    "searchkeys",
    "idabs",
}

SERVICE_URL_RE = re.compile(r"https?://[^\s\"'<>]+/(?:rest/services|services)/[^\s\"'<>]+", re.I)
URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.I)
NONE_LIKE_VALUES = {"", "none", "none.", "n/a", "na", "not applicable", "no"}
SPATIALREFERENCE_EPSG_LOOKUP = {
    "gcs_north_american_1983": "4269",
    "north_american_datum_of_1983": "4269",
    "nad_83": "4269",
    "gcs_wgs_1984": "4326",
    "wgs_1984": "4326",
    "d_wgs_1984": "4326",
    "wgs84": "4326",
    "wgs_1984_web_mercator_auxiliary_sphere": "3857",
    "nad_1983_stateplane_pennsylvania_north_fips_3701_feet": "2271",
    "nad_1983_stateplane_pennsylvania_south_fips_3702_feet": "2272",
    "nad_1983_utm_zone_13n": "26913",
    "nad_1983_utm_zone_17n": "26917",
    "nad_1983_utm_zone_18n": "26918",
    "usa_contiguous_albers_equal_area_conic": "5070",
    "usa_contiguous_albers_equal_area_conic_usgs_version": "5070",
}
PASDA_AARDVARK_REVIEW_FIELDS = [
    "pasda_xml_parse_status",
    "pasda_raw_xml_path",
    "pasda_review_flags",
]
PASDA_AARDVARK_DRAFT_FIELDS = [
    field for field in PRIMARY_FIELD_ORDER if field != "Index Year"
] + PASDA_AARDVARK_REVIEW_FIELDS
PASDA_RECORD_CHANGE_REVIEW_FIELDS = [
    "pasda_change_type",
    "pasda_change_reason",
    "metadata_filename",
    "metadata_url",
    "previous_xml_sha256",
    "current_xml_sha256",
    "previous_metadata_last_modified",
    "current_metadata_last_modified",
    "previous_metadata_size_bytes",
    "current_metadata_size_bytes",
] + PASDA_AARDVARK_DRAFT_FIELDS
PASDA_RECORD_DELETE_REVIEW_FIELDS = [
    "pasda_change_type",
    "pasda_change_reason",
    "metadata_filename",
    "metadata_url",
    "pasda_record_id",
    "source_record_id",
    "previous_xml_sha256",
    "previous_metadata_last_modified",
    "previous_metadata_size_bytes",
    "metadata_profile",
    "metadata_profile_confidence",
    "xml_fetch_status",
    "xml_parse_status",
    "parse_error",
    "first_seen",
    "last_seen",
    "last_parsed",
    "registry_version",
]


def build_pasda_session(user_agent: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def parse_metadata_directory_listing(
    html: str,
    base_url: str,
    harvested_at: str | None = None,
) -> list[dict[str, Any]]:
    harvested_at = harvested_at or utc_now()
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    seen = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        filename = unquote(href.rstrip("/").rsplit("/", 1)[-1])
        if not filename.lower().endswith(".xml"):
            continue
        if filename in seen:
            continue
        seen.add(filename)

        parent_text = " ".join(anchor.find_parent().get_text(" ", strip=True).split())
        sibling_parts = []
        for sibling in anchor.next_siblings:
            if getattr(sibling, "name", None) == "a":
                break
            sibling_value = str(sibling).strip()
            if sibling_value:
                sibling_parts.append(sibling_value)
        sibling_text = " ".join(sibling_parts)
        context = sibling_text or parent_text
        last_modified = parse_listing_last_modified(context)
        size_bytes = parse_listing_size_bytes(context, filename)
        stem = Path(filename).stem

        rows.append(
            {
                "source_system": "PASDA",
                "source_manifest": "metadata_directory",
                "metadata_filename": filename,
                "metadata_url": urljoin(base_url, href),
                "metadata_file_stem": stem,
                "metadata_file_stem_normalized": normalize_file_stem(stem),
                "metadata_provider_token": infer_provider_token(stem),
                "metadata_date_tokens": "|".join(infer_date_tokens(stem)),
                "metadata_last_modified": last_modified,
                "metadata_size_bytes": size_bytes,
                "metadata_extension": Path(filename).suffix.lower(),
                "harvested_at": harvested_at,
                "xml_fetch_status": "pending",
                "xml_parse_status": "pending",
                "metadata_profile": "",
                "metadata_profile_confidence": "",
                "parse_error": "",
                "xml_sha256": "",
                "raw_xml_path": "",
            }
        )

    return rows


def inventory_pasda_directory_tree(
    session: requests.Session,
    base_url: str,
    source_manifest: str,
    timeout: int,
    harvested_at: str | None = None,
    max_depth: int = 4,
    max_pages: int = 5000,
    file_extensions: set[str] | None = None,
    request_delay_seconds: float = 0,
) -> list[dict[str, Any]]:
    harvested_at = harvested_at or utc_now()
    root_url = ensure_trailing_slash(base_url)
    pending = [(root_url, 0)]
    seen_directories = set()
    file_rows = []

    while pending and len(seen_directories) < max_pages:
        directory_url, depth = pending.pop(0)
        directory_url = ensure_trailing_slash(directory_url)
        if directory_url in seen_directories:
            continue
        seen_directories.add(directory_url)
        if len(seen_directories) == 1 or len(seen_directories) % 100 == 0:
            print(
                "[PASDA] Directory inventory progress: "
                f"{len(seen_directories)} pages, {len(file_rows)} files found."
            )
        if request_delay_seconds > 0:
            time.sleep(request_delay_seconds)

        try:
            response = session.get(directory_url, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            file_rows.append(
                {
                    "source_system": "PASDA",
                    "source_manifest": source_manifest,
                    "asset_url": directory_url,
                    "asset_filename": "",
                    "asset_file_stem": "",
                    "asset_file_stem_normalized": "",
                    "asset_extension": "",
                    "asset_kind": "directory_listing_error",
                    "asset_directory_url": directory_url,
                    "asset_relative_path": "",
                    "asset_last_modified": "",
                    "asset_size_bytes": "",
                    "asset_depth": depth,
                    "harvested_at": harvested_at,
                    "inventory_status": "failed",
                    "inventory_error": str(exc),
                }
            )
            continue

        parsed = parse_pasda_asset_directory_listing(
            response.text,
            directory_url=directory_url,
            root_url=root_url,
            source_manifest=source_manifest,
            harvested_at=harvested_at,
            depth=depth,
            file_extensions=file_extensions,
        )
        file_rows.extend(parsed["files"])
        if depth < max_depth:
            for child_url in parsed["directories"]:
                if child_url not in seen_directories:
                    pending.append((child_url, depth + 1))

    if pending and len(seen_directories) >= max_pages:
        print(
            "[PASDA] Directory inventory stopped at "
            f"{len(seen_directories)} pages because max_pages={max_pages}. "
            f"{len(pending)} directories remain queued."
        )

    return file_rows


def parse_pasda_asset_directory_listing(
    html: str,
    directory_url: str,
    root_url: str,
    source_manifest: str,
    harvested_at: str,
    depth: int = 0,
    file_extensions: set[str] | None = None,
) -> dict[str, list[Any]]:
    soup = BeautifulSoup(html, "html.parser")
    files = []
    directories = []
    seen_assets = set()
    root_url = ensure_trailing_slash(root_url)
    directory_url = ensure_trailing_slash(directory_url)

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if not href or href.startswith(("#", "?", "mailto:", "javascript:")):
            continue
        label = clean_text(anchor.get_text(" ", strip=True))
        if label.lower() in {"parent directory", "[to parent directory]"} or href.startswith("../"):
            continue

        asset_url = urljoin(directory_url, href)
        if not asset_url.startswith(root_url):
            continue

        filename = unquote(asset_url.rstrip("/").rsplit("/", 1)[-1])
        if not filename:
            continue
        if asset_url in seen_assets:
            continue
        seen_assets.add(asset_url)

        if href.endswith("/") or asset_url.endswith("/"):
            directories.append(ensure_trailing_slash(asset_url))
            continue

        extension = Path(filename).suffix.lower()
        if file_extensions is not None and extension not in file_extensions:
            continue

        context = directory_listing_context(anchor)
        last_modified = parse_listing_last_modified(context)
        size_bytes = parse_listing_size_bytes(context, filename)
        relative_path = asset_url.removeprefix(root_url)
        stem = Path(filename).stem
        files.append(
            {
                "source_system": "PASDA",
                "source_manifest": source_manifest,
                "asset_url": asset_url,
                "asset_filename": filename,
                "asset_file_stem": stem,
                "asset_file_stem_normalized": normalize_file_stem(stem),
                "asset_extension": extension,
                "asset_kind": pasda_asset_kind(extension, asset_url),
                "asset_directory_url": directory_url,
                "asset_relative_path": relative_path,
                "asset_last_modified": last_modified,
                "asset_size_bytes": size_bytes,
                "asset_depth": depth,
                "harvested_at": harvested_at,
                "inventory_status": "found",
                "inventory_error": "",
            }
        )

    return {"files": files, "directories": directories}


def directory_listing_context(anchor: Any) -> str:
    sibling_parts = []
    for sibling in anchor.next_siblings:
        if getattr(sibling, "name", None) == "a":
            break
        sibling_value = str(sibling).strip()
        if sibling_value:
            sibling_parts.append(sibling_value)
    sibling_text = " ".join(sibling_parts)
    if sibling_text:
        return sibling_text

    parent = anchor.find_parent(["tr", "li"])
    if parent is None:
        return ""
    return " ".join(parent.get_text(" ", strip=True).split())


def ensure_trailing_slash(url: str) -> str:
    if not url:
        return ""
    return url if url.endswith("/") else f"{url}/"


def pasda_asset_kind(extension: str, asset_url: str) -> str:
    normalized_url = asset_url.lower()
    if extension in {".json", ".geojson"}:
        return "geojson"
    if extension in {".zip", ".7z", ".gz", ".tar", ".tgz"}:
        return "download_archive"
    if extension in {".gdb", ".shp", ".kml", ".kmz", ".csv", ".txt", ".tif", ".tiff", ".sid"}:
        return "download_file"
    if "archive" in normalized_url or "historic" in normalized_url:
        return "archive_candidate"
    return "download_file"


def build_pasda_asset_match_review_records(
    records: list[dict[str, Any]],
    download_inventory_rows: list[dict[str, Any]] | None = None,
    json_inventory_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    download_rows = sorted_pasda_asset_rows(download_inventory_rows or [])
    json_rows = sorted_pasda_asset_rows(json_inventory_rows or [])
    download_exact_index = index_pasda_asset_rows(download_rows, "asset_file_stem")
    json_exact_index = index_pasda_asset_rows(json_rows, "asset_file_stem")
    download_normalized_index = index_pasda_asset_rows(download_rows, "asset_file_stem_normalized")
    json_normalized_index = index_pasda_asset_rows(json_rows, "asset_file_stem_normalized")
    archive_directories = pasda_download_archive_directories(download_rows)

    review_rows = []
    for record in records:
        source_record_id = clean_text(record.get("source_record_id", ""))
        metadata_filename = clean_text(record.get("metadata_filename", ""))
        metadata_stem = pasda_record_file_stem(record)
        normalized_stem = normalize_file_stem(metadata_stem)

        geojson_exact = json_exact_index.get(metadata_stem, [])
        download_exact = download_exact_index.get(metadata_stem, [])
        exact_urls = {row.get("asset_url", "") for row in [*geojson_exact, *download_exact]}
        geojson_normalized = [
            row
            for row in json_normalized_index.get(normalized_stem, [])
            if row.get("asset_url", "") not in exact_urls
        ]
        download_normalized = [
            row
            for row in download_normalized_index.get(normalized_stem, [])
            if row.get("asset_url", "") not in exact_urls
        ]
        exact_asset_urls = [row.get("asset_url", "") for row in [*geojson_exact, *download_exact]]
        metadata_archive_links = pasda_metadata_archive_links(record)
        archive_directory_candidates = pasda_archive_directory_candidates_for_download_rows(
            [*download_exact, *download_normalized],
            archive_directories,
        )
        normalized_asset_urls = [
            row.get("asset_url", "") for row in [*geojson_normalized, *download_normalized]
        ]
        supplemental_asset_urls = dedupe_list(
            [
                *exact_asset_urls,
                *metadata_archive_links,
                *archive_directory_candidates,
            ]
        )
        match_status, match_level, distribution_candidate = pasda_asset_match_status(
            exact_asset_urls=exact_asset_urls,
            normalized_asset_urls=normalized_asset_urls,
            metadata_archive_links=metadata_archive_links,
            archive_directory_candidates=archive_directory_candidates,
        )

        best_asset, match_method, confidence = select_pasda_best_asset_match(
            geojson_exact=geojson_exact,
            download_exact=download_exact,
            geojson_normalized=geojson_normalized,
            download_normalized=download_normalized,
        )
        review_flags = pasda_asset_match_review_flags(
            geojson_exact=geojson_exact,
            download_exact=download_exact,
            geojson_normalized=geojson_normalized,
            download_normalized=download_normalized,
        )
        candidate_count = (
            len(geojson_exact)
            + len(download_exact)
            + len(geojson_normalized)
            + len(download_normalized)
        )

        review_row = {field: "" for field in PASDA_ASSET_MATCH_REVIEW_FIELDS}
        review_row.update(
            {
                "source_record_id": source_record_id,
                "metadata_filename": metadata_filename,
                "title": clean_text(record.get("title", "")),
                "metadata_url": clean_text(record.get("metadata_url", "")),
                "pasda_asset_match_status": match_status,
                "pasda_asset_match_level": match_level,
                "pasda_distribution_candidate": distribution_candidate,
                "metadata_download_links": serialize_pasda_asset_urls(
                    ensure_list(record.get("download_links_found_in_metadata", ""))
                ),
                "exact_asset_urls": serialize_pasda_asset_urls(exact_asset_urls),
                "geojson_exact_count": len(geojson_exact),
                "geojson_exact_urls": serialize_pasda_asset_urls(
                    [row.get("asset_url", "") for row in geojson_exact]
                ),
                "download_exact_count": len(download_exact),
                "download_exact_urls": serialize_pasda_asset_urls(
                    [row.get("asset_url", "") for row in download_exact]
                ),
                "geojson_normalized_count": len(geojson_normalized),
                "geojson_normalized_urls": serialize_pasda_asset_urls(
                    [row.get("asset_url", "") for row in geojson_normalized]
                ),
                "download_normalized_count": len(download_normalized),
                "download_normalized_urls": serialize_pasda_asset_urls(
                    [row.get("asset_url", "") for row in download_normalized]
                ),
                "metadata_archive_link_count": len(metadata_archive_links),
                "metadata_archive_links": serialize_pasda_asset_urls(metadata_archive_links),
                "archive_directory_candidate_count": len(archive_directory_candidates),
                "archive_directory_candidate_urls": serialize_pasda_asset_urls(
                    archive_directory_candidates
                ),
                "supplemental_asset_urls": serialize_pasda_asset_urls(supplemental_asset_urls),
                "best_asset_url": clean_text(best_asset.get("asset_url", "")),
                "best_asset_filename": clean_text(best_asset.get("asset_filename", "")),
                "best_asset_source_manifest": clean_text(best_asset.get("source_manifest", "")),
                "best_asset_kind": clean_text(best_asset.get("asset_kind", "")),
                "match_method": match_method,
                "confidence": confidence,
                "candidate_count": candidate_count,
                "review_flag": "|".join(review_flags),
            }
        )
        review_rows.append(review_row)
    return review_rows


def sorted_pasda_asset_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [row for row in rows if pasda_asset_row_found(row)],
        key=pasda_asset_sort_key,
    )


def pasda_asset_row_found(row: dict[str, Any]) -> bool:
    if not clean_text(row.get("asset_url", "")):
        return False
    inventory_status = clean_text(row.get("inventory_status", "found"))
    return inventory_status in {"", "found"}


def index_pasda_asset_rows(rows: list[dict[str, Any]], field: str) -> dict[str, list[dict[str, Any]]]:
    indexed: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = clean_text(row.get(field, ""))
        if key:
            indexed.setdefault(key, []).append(row)
    return indexed


def pasda_record_file_stem(record: dict[str, Any]) -> str:
    source_record_id = clean_text(record.get("source_record_id", ""))
    if source_record_id:
        return source_record_id
    metadata_filename = clean_text(record.get("metadata_filename", ""))
    if metadata_filename:
        return Path(metadata_filename).stem
    metadata_url = clean_text(record.get("metadata_url", ""))
    if metadata_url:
        return Path(unquote(metadata_url.rstrip("/").rsplit("/", 1)[-1])).stem
    return ""


def select_pasda_best_asset_match(
    geojson_exact: list[dict[str, Any]],
    download_exact: list[dict[str, Any]],
    geojson_normalized: list[dict[str, Any]],
    download_normalized: list[dict[str, Any]],
) -> tuple[dict[str, Any], str, str]:
    if geojson_exact:
        return geojson_exact[0], "geojson_exact_filename", "95"
    if download_exact:
        confidence = "90" if download_exact[0].get("asset_kind") == "download_archive" else "85"
        return download_exact[0], "download_exact_filename", confidence
    if geojson_normalized:
        return geojson_normalized[0], "geojson_normalized_filename", "80"
    if download_normalized:
        confidence = "75" if download_normalized[0].get("asset_kind") == "download_archive" else "70"
        return download_normalized[0], "download_normalized_filename", confidence
    return {}, "", ""


def pasda_asset_match_review_flags(
    geojson_exact: list[dict[str, Any]],
    download_exact: list[dict[str, Any]],
    geojson_normalized: list[dict[str, Any]],
    download_normalized: list[dict[str, Any]],
) -> list[str]:
    exact_count = len(geojson_exact) + len(download_exact)
    normalized_count = len(geojson_normalized) + len(download_normalized)
    flags = []
    if not exact_count and not normalized_count:
        flags.append("no_filename_match")
    if not exact_count and normalized_count:
        flags.append("normalized_only_filename_match")
    return flags


def pasda_asset_match_status(
    exact_asset_urls: list[str],
    normalized_asset_urls: list[str],
    metadata_archive_links: list[str],
    archive_directory_candidates: list[str],
) -> tuple[str, str, str]:
    has_exact_assets = bool(dedupe_list(exact_asset_urls))
    has_normalized_assets = bool(dedupe_list(normalized_asset_urls))
    has_archives = bool(dedupe_list([*metadata_archive_links, *archive_directory_candidates]))
    if has_exact_assets and has_archives:
        return "exact_assets_with_archive", "exact", "ready"
    if has_exact_assets:
        return "exact_assets", "exact", "ready"
    if has_normalized_assets and has_archives:
        return "normalized_assets_with_archive", "normalized", "review"
    if has_normalized_assets:
        return "normalized_only", "normalized", "review"
    if has_archives:
        return "archive_only", "archive", "review"
    return "no_asset_match", "none", "no"


def build_pasda_distribution_records(
    records: list[dict[str, Any]],
    asset_match_review_rows: list[dict[str, Any]],
    ready_only: bool = True,
    parsed_only: bool = True,
) -> list[dict[str, Any]]:
    records_by_id = {
        clean_text(record.get("source_record_id", "")): record
        for record in records
        if clean_text(record.get("source_record_id", ""))
    }
    distribution_rows = []
    seen_rows = set()
    for match_row in asset_match_review_rows:
        source_record_id = clean_text(match_row.get("source_record_id", ""))
        if ready_only and clean_text(match_row.get("pasda_distribution_candidate", "")) != "ready":
            continue
        friendlier_id = pasda_record_id_from_source(source_record_id)
        if not friendlier_id:
            continue
        record = records_by_id.get(source_record_id, {})
        if parsed_only and not pasda_record_is_upload_parseable(record):
            continue
        for row in pasda_distribution_rows_for_match(friendlier_id, match_row, record):
            row_key = (
                row["friendlier_id"],
                row["reference_type"],
                row["distribution_url"],
                row["label"],
            )
            if row_key in seen_rows:
                continue
            seen_rows.add(row_key)
            distribution_rows.append(row)
    return distribution_rows


def pasda_distribution_rows_for_match(
    friendlier_id: str,
    match_row: dict[str, Any],
    record: dict[str, Any],
) -> list[dict[str, str]]:
    rows = []
    for url in pasda_distribution_asset_urls(match_row):
        reference_type, label = pasda_distribution_type_and_label(url)
        if reference_type:
            rows.append(pasda_distribution_row(friendlier_id, reference_type, url, label))

    metadata_url = clean_text(record.get("metadata_url", "")) or clean_text(
        match_row.get("metadata_url", "")
    )
    metadata_reference_type = pasda_metadata_distribution_type(record)
    if metadata_url and metadata_reference_type:
        rows.append(pasda_distribution_row(friendlier_id, metadata_reference_type, metadata_url, ""))
    return rows


def pasda_distribution_asset_urls(match_row: dict[str, Any]) -> list[str]:
    return deserialize_pasda_asset_urls(clean_text(match_row.get("supplemental_asset_urls", "")))


def pasda_distribution_type_and_label(url: str) -> tuple[str, str]:
    clean_url = clean_text(url)
    if not clean_url:
        return "", ""
    if pasda_distribution_url_is_historical_directory(clean_url):
        return "download", "Historical versions"
    return "download", pasda_distribution_label_from_url(clean_url)


def pasda_distribution_url_is_historical_directory(url: str) -> bool:
    return clean_text(url).endswith("/") and pasda_asset_url_is_archival(url)


def pasda_distribution_label_from_url(url: str) -> str:
    path = unquote(urlparse(url).path)
    for marker in ["/download/", "/json/"]:
        if marker in path:
            relative_path = path.split(marker, 1)[1].strip("/")
            if relative_path:
                return relative_path
    filename = clean_text(Path(path).name)
    if filename:
        return filename
    extension = Path(path).suffix.lower()
    if not extension:
        return "Download"
    return extension.lstrip(".").upper()


def pasda_metadata_distribution_type(record: dict[str, Any]) -> str:
    metadata_profile = clean_text(record.get("metadata_profile", ""))
    if metadata_profile == "iso_19139":
        return "metadata_iso"
    if metadata_profile == "fgdc_csdgm":
        return "metadata_fgdc"
    return ""


def pasda_distribution_row(
    friendlier_id: str,
    reference_type: str,
    distribution_url: str,
    label: str,
) -> dict[str, str]:
    return {
        "friendlier_id": clean_text(friendlier_id),
        "reference_type": clean_text(reference_type),
        "distribution_url": clean_text(distribution_url),
        "label": clean_text(label),
    }


def deserialize_pasda_asset_urls(value: str) -> list[str]:
    return [
        url
        for url in dedupe_list(clean_text(value).split("|"))
        if url and not url.startswith("... ")
    ]


def build_pasda_unparsed_matched_review_records(
    records: list[dict[str, Any]],
    asset_match_review_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    match_rows_by_id = {
        clean_text(row.get("source_record_id", "")): row
        for row in asset_match_review_rows
        if clean_text(row.get("source_record_id", ""))
    }
    rows = []
    for record in records:
        source_record_id = clean_text(record.get("source_record_id", ""))
        match_row = match_rows_by_id.get(source_record_id, {})
        if clean_text(match_row.get("pasda_distribution_candidate", "")) != "ready":
            continue
        if pasda_record_is_upload_parseable(record):
            continue
        rows.append(pasda_unparsed_matched_review_row(record, match_row))
    return rows


def pasda_unparsed_matched_review_row(
    record: dict[str, Any],
    match_row: dict[str, Any],
) -> dict[str, Any]:
    source_record_id = clean_text(record.get("source_record_id", ""))
    row = {
        "source_record_id": source_record_id,
        "public_id": pasda_record_id_from_source(source_record_id),
        "metadata_filename": clean_text(record.get("metadata_filename", "")),
        "metadata_url": clean_text(record.get("metadata_url", "")),
        "raw_xml_path": clean_text(record.get("raw_xml_path", "")),
        "xml_fetch_status": clean_text(record.get("xml_fetch_status", "")),
        "xml_parse_status": clean_text(record.get("xml_parse_status", "")),
        "parse_error": clean_text(record.get("parse_error", "")),
        "title": clean_text(record.get("title", "")),
        "pasda_distribution_candidate": clean_text(
            match_row.get("pasda_distribution_candidate", "")
        ),
        "pasda_asset_match_status": clean_text(match_row.get("pasda_asset_match_status", "")),
        "pasda_asset_match_level": clean_text(match_row.get("pasda_asset_match_level", "")),
        "match_method": clean_text(match_row.get("match_method", "")),
        "confidence": clean_text(match_row.get("confidence", "")),
        "best_asset_url": clean_text(match_row.get("best_asset_url", "")),
        "supplemental_asset_urls": clean_text(match_row.get("supplemental_asset_urls", "")),
    }
    return {field: row.get(field, "") for field in PASDA_UNPARSED_MATCH_REVIEW_FIELDS}


def build_pasda_new_changed_record_review_rows(
    aardvark_rows: list[dict[str, Any]],
    normalized_records: list[dict[str, Any]],
    existing_metadata_registry: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records_by_public_id = {
        pasda_record_id_from_source(clean_text(record.get("source_record_id", ""))): record
        for record in normalized_records
        if clean_text(record.get("source_record_id", ""))
    }
    new_rows = []
    changed_rows = []
    for aardvark_row in aardvark_rows:
        public_id = clean_text(aardvark_row.get("ID", ""))
        record = records_by_public_id.get(public_id, {})
        filename = clean_text(record.get("metadata_filename", ""))
        if not filename:
            continue
        existing_entry = existing_metadata_registry.get(filename)
        if not existing_entry:
            new_rows.append(
                pasda_record_change_review_row(
                    aardvark_row,
                    record,
                    existing_entry={},
                    change_type="new",
                    change_reason="new_metadata_file",
                )
            )
            continue

        previous_hash = clean_text(existing_entry.get("xml_sha256", ""))
        current_hash = clean_text(record.get("xml_sha256", ""))
        if previous_hash and current_hash and previous_hash != current_hash:
            changed_rows.append(
                pasda_record_change_review_row(
                    aardvark_row,
                    record,
                    existing_entry=existing_entry,
                    change_type="changed",
                    change_reason="source_xml_hash_changed",
                )
            )
    return new_rows, changed_rows


def pasda_record_change_review_row(
    aardvark_row: dict[str, Any],
    record: dict[str, Any],
    existing_entry: dict[str, Any],
    change_type: str,
    change_reason: str,
) -> dict[str, Any]:
    row = {
        "pasda_change_type": clean_text(change_type),
        "pasda_change_reason": clean_text(change_reason),
        "metadata_filename": clean_text(record.get("metadata_filename", "")),
        "metadata_url": clean_text(record.get("metadata_url", "")),
        "previous_xml_sha256": clean_text(existing_entry.get("xml_sha256", "")),
        "current_xml_sha256": clean_text(record.get("xml_sha256", "")),
        "previous_metadata_last_modified": clean_text(
            existing_entry.get("metadata_last_modified", "")
        ),
        "current_metadata_last_modified": clean_text(record.get("metadata_last_modified", "")),
        "previous_metadata_size_bytes": clean_text(existing_entry.get("metadata_size_bytes", "")),
        "current_metadata_size_bytes": clean_text(record.get("metadata_size_bytes", "")),
    }
    row.update({field: clean_text(aardvark_row.get(field, "")) for field in PASDA_AARDVARK_DRAFT_FIELDS})
    return {field: row.get(field, "") for field in PASDA_RECORD_CHANGE_REVIEW_FIELDS}


def build_pasda_deleted_record_review_rows(
    existing_metadata_registry: dict[str, dict[str, Any]],
    inventory_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    current_filenames = {
        clean_text(row.get("metadata_filename", ""))
        for row in inventory_rows
        if clean_text(row.get("metadata_filename", ""))
    }
    deleted_rows = []
    for filename in sorted(existing_metadata_registry):
        clean_filename = clean_text(filename)
        if not clean_filename or clean_filename in current_filenames:
            continue
        deleted_rows.append(
            pasda_record_delete_review_row(existing_metadata_registry[filename])
        )
    return deleted_rows


def pasda_record_delete_review_row(existing_entry: dict[str, Any]) -> dict[str, Any]:
    row = {
        "pasda_change_type": "deleted",
        "pasda_change_reason": "metadata_file_missing_from_current_directory_listing",
        "metadata_filename": clean_text(existing_entry.get("metadata_filename", "")),
        "metadata_url": clean_text(existing_entry.get("metadata_url", "")),
        "pasda_record_id": clean_text(existing_entry.get("pasda_record_id", "")),
        "source_record_id": clean_text(existing_entry.get("source_record_id", "")),
        "previous_xml_sha256": clean_text(existing_entry.get("xml_sha256", "")),
        "previous_metadata_last_modified": clean_text(
            existing_entry.get("metadata_last_modified", "")
        ),
        "previous_metadata_size_bytes": clean_text(existing_entry.get("metadata_size_bytes", "")),
        "metadata_profile": clean_text(existing_entry.get("metadata_profile", "")),
        "metadata_profile_confidence": clean_text(
            existing_entry.get("metadata_profile_confidence", "")
        ),
        "xml_fetch_status": clean_text(existing_entry.get("xml_fetch_status", "")),
        "xml_parse_status": clean_text(existing_entry.get("xml_parse_status", "")),
        "parse_error": clean_text(existing_entry.get("parse_error", "")),
        "first_seen": clean_text(existing_entry.get("first_seen", "")),
        "last_seen": clean_text(existing_entry.get("last_seen", "")),
        "last_parsed": clean_text(existing_entry.get("last_parsed", "")),
        "registry_version": clean_text(existing_entry.get("registry_version", "")),
    }
    return {field: row.get(field, "") for field in PASDA_RECORD_DELETE_REVIEW_FIELDS}


def build_pasda_series_review_records(
    records: list[dict[str, Any]],
    asset_match_review_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    match_rows_by_id = {
        clean_text(row.get("source_record_id", "")): row
        for row in asset_match_review_rows or []
        if clean_text(row.get("source_record_id", ""))
    }
    grouped_records: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        series_key = pasda_series_key(record)
        match_row = match_rows_by_id.get(clean_text(record.get("source_record_id", "")), {})
        grouped_records.setdefault(series_key, []).append(
            pasda_series_member_record(record, match_row, series_key)
        )

    return [
        build_pasda_series_review_record(series_key, members)
        for series_key, members in sorted(grouped_records.items())
    ]


def pasda_series_member_record(
    record: dict[str, Any],
    match_row: dict[str, Any],
    series_key: str,
) -> dict[str, Any]:
    inferred_date = pasda_series_record_date(record)
    match_status = clean_text(match_row.get("pasda_asset_match_status", "no_asset_match"))
    match_level = clean_text(match_row.get("pasda_asset_match_level", "none"))
    distribution_candidate = clean_text(match_row.get("pasda_distribution_candidate", "no"))
    return {
        "series_key": series_key,
        "source_record_id": clean_text(record.get("source_record_id", "")),
        "metadata_filename": clean_text(record.get("metadata_filename", "")),
        "title": clean_text(record.get("title", "")),
        "series_title": pasda_series_title(record),
        "inferred_date": inferred_date,
        "date_sort": pasda_series_date_sort_value(inferred_date),
        "match_status": match_status or "no_asset_match",
        "match_level": match_level or "none",
        "distribution_candidate": distribution_candidate or "no",
    }


def build_pasda_series_review_record(series_key: str, members: list[dict[str, Any]]) -> dict[str, Any]:
    sorted_members = sorted(
        members,
        key=lambda member: (member["date_sort"], member["source_record_id"]),
    )
    latest_member = sorted_members[-1]
    ready_members = [
        member for member in sorted_members if member.get("distribution_candidate") == "ready"
    ]
    latest_ready_member = ready_members[-1] if ready_members else {}
    title_counts = count_pasda_values([member.get("series_title", "") for member in sorted_members])
    shared_title_count = max(title_counts.values()) if title_counts else 0
    record_count = len(sorted_members)
    status_counts = count_pasda_values([member.get("match_status", "") for member in sorted_members])
    ready_count = sum(1 for member in sorted_members if member.get("distribution_candidate") == "ready")
    review_count = sum(1 for member in sorted_members if member.get("distribution_candidate") == "review")
    no_asset_count = status_counts.get("no_asset_match", 0)
    row = {field: "" for field in PASDA_SERIES_REVIEW_FIELDS}
    row.update(
        {
            "series_key": series_key,
            "series_title": pasda_primary_series_title(sorted_members),
            "series_status": pasda_series_status(record_count, ready_count, review_count, no_asset_count),
            "record_count": record_count,
            "record_ids": "|".join(member["source_record_id"] for member in sorted_members),
            "metadata_filenames": "|".join(member["metadata_filename"] for member in sorted_members),
            "inferred_record_dates": "|".join(
                member["inferred_date"] for member in sorted_members if member["inferred_date"]
            ),
            "ready_count": ready_count,
            "review_count": review_count,
            "no_asset_count": no_asset_count,
            "exact_asset_count": status_counts.get("exact_assets", 0),
            "exact_assets_with_archive_count": status_counts.get("exact_assets_with_archive", 0),
            "archive_only_count": status_counts.get("archive_only", 0),
            "normalized_count": status_counts.get("normalized_only", 0)
            + status_counts.get("normalized_assets_with_archive", 0),
            "most_recently_found_record_id": latest_member.get("source_record_id", ""),
            "most_recently_found_date": latest_member.get("inferred_date", ""),
            "most_recently_found_match_status": latest_member.get("match_status", ""),
            "most_recently_found_distribution_candidate": latest_member.get(
                "distribution_candidate", ""
            ),
            "latest_ready_record_id": latest_ready_member.get("source_record_id", ""),
            "latest_ready_date": latest_ready_member.get("inferred_date", ""),
            "title_needs_date_qualifier": "yes" if record_count > 1 and shared_title_count > 1 else "no",
            "shared_title_count": shared_title_count,
            "unique_titles": "|".join(title_counts.keys()),
            "series_currentness_note": PASDA_SERIES_CURRENTNESS_NOTE,
        }
    )
    return row


def count_pasda_values(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        clean_value = clean_text(value)
        if not clean_value:
            continue
        counts[clean_value] = counts.get(clean_value, 0) + 1
    return counts


def pasda_primary_series_title(members: list[dict[str, Any]]) -> str:
    title_counts = count_pasda_values([member.get("series_title", "") for member in members])
    if not title_counts:
        return ""
    return sorted(title_counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def pasda_series_status(
    record_count: int,
    ready_count: int,
    review_count: int,
    no_asset_count: int,
) -> str:
    if record_count == 1:
        if ready_count:
            return "single_record_ready"
        if review_count:
            return "single_record_review"
        return "single_record_no_asset_match"
    if ready_count == record_count:
        return "series_all_members_ready"
    if ready_count:
        return "series_partial_assets"
    if review_count:
        return "series_review_only"
    if no_asset_count == record_count:
        return "series_no_asset_match"
    return "series_mixed_unresolved"


def pasda_series_key(record: dict[str, Any]) -> str:
    stem = pasda_record_file_stem(record)
    without_dates = strip_pasda_series_date_tokens(stem)
    normalized = normalize_file_stem(without_dates)
    return normalized or normalize_file_stem(stem) or "unknown_series"


def pasda_series_title(record: dict[str, Any]) -> str:
    title = clean_text(record.get("title", ""))
    return format_pasda_title_dates(title) if title else ""


def strip_pasda_series_date_tokens(value: str) -> str:
    clean_value = clean_text(value)
    clean_value = re.sub(
        r"(?<!\d)(?:19|20)\d{2}[_\-. ]?(?:0[1-9]|1[0-2])[_\-. ]?(?:[0-3]\d)?(?!\d)",
        " ",
        clean_value,
    )
    clean_value = re.sub(r"(?:(?<=\D)|^)(?:19|20)\d{2}(?=\D|$)", " ", clean_value)
    return clean_text(clean_value)


def pasda_series_record_date(record: dict[str, Any]) -> str:
    for value in [
        record.get("source_record_id", ""),
        record.get("metadata_filename", ""),
        record.get("publication_date", ""),
        record.get("modified", ""),
        record.get("metadata_date", ""),
        record.get("temporal_end", ""),
        record.get("temporal_start", ""),
        record.get("title", ""),
    ]:
        date_value = pasda_series_date_from_text(value)
        if date_value:
            return date_value
    return ""


def pasda_series_date_from_text(value: Any) -> str:
    clean_value = clean_text(value)
    if not clean_value:
        return ""
    date_match = re.search(
        r"(?<!\d)((?:19|20)\d{2})[_\-. /]?(0[1-9]|1[0-2])[_\-. /]?([0-3]\d)(?!\d)",
        clean_value,
    )
    if date_match:
        year, month, day = date_match.groups()
        return f"{year}-{month}-{day}"
    date_match = re.search(
        r"(?<!\d)((?:19|20)\d{2})[_\-. /]?(0[1-9]|1[0-2])(?!\d)",
        clean_value,
    )
    if date_match:
        year, month = date_match.groups()
        return f"{year}-{month}"
    date_match = re.search(r"(?<!\d)((?:19|20)\d{2})(?!\d)", clean_value)
    return date_match.group(1) if date_match else ""


def pasda_series_date_sort_value(date_value: str) -> str:
    clean_value = clean_text(date_value)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", clean_value):
        return clean_value
    if re.fullmatch(r"\d{4}-\d{2}", clean_value):
        return f"{clean_value}-00"
    if re.fullmatch(r"\d{4}", clean_value):
        return f"{clean_value}-00-00"
    return "0000-00-00"


def pasda_metadata_archive_links(record: dict[str, Any]) -> list[str]:
    archive_links = []
    for field in [
        "online_links",
        "distribution_links",
        "download_links_found_in_metadata",
        "service_links_found_in_metadata",
    ]:
        for value in ensure_list(record.get(field, "")):
            if pasda_archive_link_is_explicit(value):
                archive_links.append(ensure_trailing_slash(value) if value.endswith("/") else value)
    return dedupe_list(archive_links)


def pasda_archive_link_is_explicit(url: str) -> bool:
    clean_url = clean_text(url)
    if not re.match(r"^(?:https?|ftp)://", clean_url, re.I):
        return False
    return pasda_asset_url_is_archival(clean_url)


def pasda_download_archive_directories(rows: list[dict[str, Any]]) -> set[str]:
    directories = set()
    for row in rows:
        directory_url = ensure_trailing_slash(clean_text(row.get("asset_directory_url", "")))
        if directory_url and pasda_asset_url_is_archival(directory_url):
            directories.add(directory_url)
    return directories


def pasda_archive_directory_candidates_for_download_rows(
    rows: list[dict[str, Any]],
    archive_directories: set[str],
) -> list[str]:
    candidates = []
    for row in rows:
        directory_url = ensure_trailing_slash(clean_text(row.get("asset_directory_url", "")))
        if not directory_url:
            continue
        if pasda_asset_url_is_archival(directory_url):
            candidates.append(directory_url)
            continue
        candidates.extend(
            archive_dir
            for archive_dir in archive_directories
            if pasda_archive_directory_is_near_download_directory(archive_dir, directory_url)
        )
    return dedupe_list(candidates)


def pasda_archive_directory_is_near_download_directory(archive_dir: str, download_dir: str) -> bool:
    archive_dir = ensure_trailing_slash(clean_text(archive_dir))
    download_dir = ensure_trailing_slash(clean_text(download_dir))
    if not archive_dir.startswith(download_dir) or archive_dir == download_dir:
        return False
    relative_parts = [part for part in archive_dir.removeprefix(download_dir).split("/") if part]
    if len(relative_parts) != 1:
        return False
    return pasda_archive_path_part_is_explicit(relative_parts[0])


def pasda_asset_sort_key(row: dict[str, Any]) -> tuple[int, int, int, str]:
    extension = clean_text(row.get("asset_extension", "")).lower()
    source_manifest = clean_text(row.get("source_manifest", ""))
    source_rank = 0 if source_manifest == "json_directory" else 1
    extension_rank = PASDA_ASSET_EXTENSION_RANK.get(extension, 99)
    archive_rank = 1 if pasda_asset_url_is_archival(row.get("asset_url", "")) else 0
    return (
        source_rank,
        extension_rank,
        archive_rank,
        clean_text(row.get("asset_url", "")).lower(),
    )


def pasda_asset_url_is_archival(url: str) -> bool:
    path_parts = [
        unquote(part).lower()
        for part in urlparse(clean_text(url)).path.split("/")
        if clean_text(part)
    ]
    return any(pasda_archive_path_part_is_explicit(part) for part in path_parts)


def pasda_archive_path_part_is_explicit(path_part: str) -> bool:
    normalized = clean_text(path_part).lower()
    return normalized in {"historic", "archive", "archived"} or normalized.endswith(
        ("_historic", "-historic", "_archive", "-archive")
    )


def serialize_pasda_asset_urls(values: list[str]) -> str:
    urls = dedupe_list(values)
    if len(urls) <= PASDA_ASSET_MATCH_URL_LIMIT:
        return "|".join(urls)
    visible_urls = urls[:PASDA_ASSET_MATCH_URL_LIMIT]
    visible_urls.append(f"... {len(urls) - PASDA_ASSET_MATCH_URL_LIMIT} more")
    return "|".join(visible_urls)


def sample_size_from_config(config: dict[str, Any]) -> int | None:
    sample_size = config.get("sample_size")
    if sample_size in ("", None):
        sample_size = config.get("max_records")
    if sample_size in ("", None):
        return None

    sample_size = int(sample_size)
    if sample_size < 1:
        raise ValueError("[PASDA] sample_size must be greater than zero.")
    return sample_size


def sample_strategy_from_config(config: dict[str, Any]) -> str:
    if config.get("sample_size") in ("", None) and config.get("max_records") not in ("", None):
        return "first"
    return str(config.get("sample_strategy", "first")).strip().lower()


def select_metadata_sample(
    rows: list[dict[str, Any]],
    sample_size: int | None = None,
    sample_strategy: str = "first",
    sample_seed: int = 42,
) -> list[dict[str, Any]]:
    if sample_size is None or sample_size >= len(rows):
        return list(rows)

    if sample_strategy == "first":
        return list(rows[:sample_size])

    if sample_strategy in {"mixed", "evenly_spaced"}:
        return [rows[index] for index in evenly_spaced_indices(len(rows), sample_size)]

    if sample_strategy == "random":
        indices = sorted(random.Random(sample_seed).sample(range(len(rows)), sample_size))
        return [rows[index] for index in indices]

    raise ValueError(
        "[PASDA] Unsupported sample_strategy. "
        "Use one of: first, mixed, evenly_spaced, random."
    )


def evenly_spaced_indices(total_count: int, sample_size: int) -> list[int]:
    if sample_size >= total_count:
        return list(range(total_count))
    if sample_size == 1:
        return [0]

    indices = {
        round(index * (total_count - 1) / (sample_size - 1))
        for index in range(sample_size)
    }
    if len(indices) < sample_size:
        for index in range(total_count):
            indices.add(index)
            if len(indices) == sample_size:
                break
    return sorted(indices)


def mark_inventory_sample(
    inventory_rows: list[dict[str, Any]],
    selected_rows: list[dict[str, Any]],
    sample_strategy: str,
) -> list[dict[str, Any]]:
    selected_lookup = {
        row.get("metadata_filename", ""): index
        for index, row in enumerate(selected_rows, start=1)
    }
    marked_rows = []
    for row in inventory_rows:
        marked_row = dict(row)
        sample_index = selected_lookup.get(row.get("metadata_filename", ""))
        marked_row["selected_for_download"] = "yes" if sample_index is not None else "no"
        marked_row["sample_index"] = sample_index or ""
        marked_row["sample_strategy"] = sample_strategy if sample_index is not None else ""
        marked_rows.append(marked_row)
    return marked_rows


def load_pasda_metadata_registry(path_value: str | Path | None) -> dict[str, dict[str, Any]]:
    if not path_value:
        return {}
    path = Path(path_value or "")
    if not path.exists():
        return {}

    rows = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
    return {
        clean_text(row.get("metadata_filename", "")): row
        for row in rows
        if clean_text(row.get("metadata_filename", ""))
    }


def load_pasda_normalized_registry(path_value: str | Path | None) -> dict[str, dict[str, Any]]:
    if not path_value:
        return {}
    path = Path(path_value or "")
    if not path.exists():
        return {}

    records = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            filename = clean_text(record.get("metadata_filename", ""))
            if filename:
                records[filename] = record
    return records


def prepare_registry_metadata_row(
    row: dict[str, Any],
    metadata_registry: dict[str, dict[str, Any]],
    normalized_registry: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    filename = clean_text(row.get("metadata_filename", ""))
    metadata_entry = metadata_registry.get(filename)
    normalized_record = normalized_registry.get(filename)
    if not registry_entry_reusable(row, metadata_entry, normalized_record):
        return None

    prepared = dict(row)
    prepared.update(
        {
            "xml_fetch_status": "registry",
            "xml_parse_status": metadata_entry.get("xml_parse_status", ""),
            "metadata_profile": metadata_entry.get("metadata_profile", ""),
            "metadata_profile_confidence": metadata_entry.get("metadata_profile_confidence", ""),
            "parse_error": metadata_entry.get("parse_error", ""),
            "xml_sha256": metadata_entry.get("xml_sha256", ""),
            "raw_xml_path": "",
            "registry_reuse_status": "reused",
            "registry_version": metadata_entry.get("registry_version", ""),
        }
    )
    return prepared


def registry_entry_reusable(
    inventory_row: dict[str, Any],
    metadata_entry: dict[str, Any] | None,
    normalized_record: dict[str, Any] | None,
) -> bool:
    if not metadata_entry or not normalized_record:
        return False
    if metadata_entry.get("registry_version") != PASDA_REGISTRY_VERSION:
        return False
    if not clean_text(metadata_entry.get("xml_sha256", "")):
        return False
    if metadata_entry.get("xml_parse_status") not in {"parsed", "partial", "malformed"}:
        return False
    if clean_text(normalized_record.get("metadata_filename", "")) != clean_text(
        inventory_row.get("metadata_filename", "")
    ):
        return False
    if clean_text(normalized_record.get("xml_sha256", "")) != clean_text(
        metadata_entry.get("xml_sha256", "")
    ):
        return False

    if not metadata_listing_has_comparable_fields(inventory_row, metadata_entry):
        return False
    return metadata_listing_matches_registry(inventory_row, metadata_entry)


def metadata_listing_comparable_fields(
    inventory_row: dict[str, Any],
    metadata_entry: dict[str, Any] | None,
) -> list[str]:
    if not metadata_entry:
        return []
    listing_fields = ["metadata_last_modified", "metadata_size_bytes"]
    return [
        field
        for field in listing_fields
        if clean_text(inventory_row.get(field, "")) and clean_text(metadata_entry.get(field, ""))
    ]


def metadata_listing_has_comparable_fields(
    inventory_row: dict[str, Any],
    metadata_entry: dict[str, Any] | None,
) -> bool:
    return bool(metadata_listing_comparable_fields(inventory_row, metadata_entry))


def metadata_listing_matches_registry(
    inventory_row: dict[str, Any],
    metadata_entry: dict[str, Any] | None,
) -> bool:
    comparable_fields = metadata_listing_comparable_fields(inventory_row, metadata_entry)
    if not comparable_fields:
        return False
    return all(
        clean_text(inventory_row.get(field, "")) == clean_text(metadata_entry.get(field, ""))
        for field in comparable_fields
    )


def registry_pasda_manifest_row(
    row: dict[str, Any],
    normalized_registry: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest_row = dict(row)
    filename = clean_text(row.get("metadata_filename", ""))
    normalized_record = dict(normalized_registry.get(filename, {}))
    if not normalized_record:
        manifest_row["xml_fetch_status"] = "failed"
        manifest_row["xml_parse_status"] = "failed"
        manifest_row["parse_error"] = "Registry normalized record was not found."
        return manifest_row, empty_normalized_record(manifest_row)

    for field in [
        "metadata_filename",
        "metadata_url",
        "metadata_profile",
        "metadata_profile_confidence",
        "xml_parse_status",
        "parse_error",
        "xml_sha256",
    ]:
        manifest_row[field] = normalized_record.get(field, manifest_row.get(field, ""))
    normalized_record["metadata_url"] = manifest_row.get("metadata_url", normalized_record.get("metadata_url", ""))
    normalized_record["raw_xml_path"] = manifest_row.get("raw_xml_path", "")
    normalized_record["registry_reuse_status"] = "reused"
    manifest_row["registry_reuse_status"] = "reused"
    return manifest_row, normalized_record


def build_pasda_metadata_registry_rows(
    existing_registry: dict[str, dict[str, Any]],
    inventory_rows: list[dict[str, Any]],
    manifest_rows: list[dict[str, Any]],
    normalized_records: list[dict[str, Any]],
    seen_at: str,
) -> list[dict[str, Any]]:
    rows_by_filename = {
        filename: {field: clean_text(row.get(field, "")) for field in PASDA_METADATA_REGISTRY_FIELDS}
        for filename, row in existing_registry.items()
    }

    for row in inventory_rows:
        filename = clean_text(row.get("metadata_filename", ""))
        if not filename:
            continue
        registry_row = rows_by_filename.setdefault(filename, empty_pasda_metadata_registry_row(filename))
        if not registry_row.get("first_seen"):
            registry_row["first_seen"] = seen_at
        registry_row.update(
            {
                "metadata_filename": filename,
                "metadata_url": clean_text(row.get("metadata_url", "")),
                "source_record_id": clean_text(row.get("metadata_file_stem", "")),
                "pasda_record_id": pasda_record_id_from_source(
                    clean_text(row.get("metadata_file_stem", ""))
                ),
                "metadata_last_modified": clean_text(row.get("metadata_last_modified", "")),
                "metadata_size_bytes": clean_text(row.get("metadata_size_bytes", "")),
                "last_seen": seen_at,
                "registry_version": registry_row.get("registry_version", PASDA_REGISTRY_VERSION),
            }
        )

    normalized_by_filename = {
        clean_text(record.get("metadata_filename", "")): record
        for record in normalized_records
        if clean_text(record.get("metadata_filename", ""))
    }
    for row in manifest_rows:
        filename = clean_text(row.get("metadata_filename", ""))
        if not filename:
            continue
        registry_row = rows_by_filename.setdefault(filename, empty_pasda_metadata_registry_row(filename))
        normalized_record = normalized_by_filename.get(filename, {})
        registry_row.update(
            {
                "metadata_filename": filename,
                "metadata_url": clean_text(row.get("metadata_url", "")),
                "source_record_id": clean_text(row.get("metadata_file_stem", ""))
                or clean_text(normalized_record.get("source_record_id", "")),
                "pasda_record_id": pasda_record_id_from_source(
                    clean_text(row.get("metadata_file_stem", ""))
                    or clean_text(normalized_record.get("source_record_id", ""))
                ),
                "metadata_last_modified": clean_text(row.get("metadata_last_modified", "")),
                "metadata_size_bytes": clean_text(row.get("metadata_size_bytes", "")),
                "xml_sha256": clean_text(row.get("xml_sha256", "")),
                "metadata_profile": clean_text(row.get("metadata_profile", "")),
                "metadata_profile_confidence": clean_text(row.get("metadata_profile_confidence", "")),
                "xml_fetch_status": clean_text(row.get("xml_fetch_status", "")),
                "xml_parse_status": clean_text(row.get("xml_parse_status", "")),
                "parse_error": clean_text(row.get("parse_error", "")),
                "last_seen": seen_at,
                "registry_version": PASDA_REGISTRY_VERSION,
            }
        )
        if row.get("xml_fetch_status") != "registry" and row.get("xml_parse_status") in {
            "parsed",
            "partial",
            "malformed",
        }:
            registry_row["last_parsed"] = seen_at
        if not registry_row.get("first_seen"):
            registry_row["first_seen"] = seen_at

    return [
        {field: rows_by_filename[filename].get(field, "") for field in PASDA_METADATA_REGISTRY_FIELDS}
        for filename in sorted(rows_by_filename)
    ]


def build_pasda_normalized_registry_records(
    existing_registry: dict[str, dict[str, Any]],
    normalized_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    records_by_filename = {
        filename: prepare_normalized_record_for_registry(record)
        for filename, record in existing_registry.items()
    }
    for record in normalized_records:
        filename = clean_text(record.get("metadata_filename", ""))
        if filename:
            records_by_filename[filename] = prepare_normalized_record_for_registry(record)
    return [records_by_filename[filename] for filename in sorted(records_by_filename)]


def empty_pasda_metadata_registry_row(filename: str) -> dict[str, str]:
    row = {field: "" for field in PASDA_METADATA_REGISTRY_FIELDS}
    row["metadata_filename"] = filename
    return row


def prepare_normalized_record_for_registry(record: dict[str, Any]) -> dict[str, Any]:
    registry_record = dict(record)
    registry_record["raw_xml_path"] = ""
    registry_record["registry_version"] = PASDA_REGISTRY_VERSION
    return registry_record


def pasda_record_id_from_source(source_record_id: str) -> str:
    clean_source_id = clean_text(source_record_id).replace("[", "").replace("]", "")
    return f"pasda-{clean_source_id}" if clean_source_id else ""


def pasda_record_is_upload_parseable(record: dict[str, Any]) -> bool:
    xml_parse_status = clean_text(record.get("xml_parse_status", ""))
    return xml_parse_status in {"", "parsed"}


def fetch_and_cache_metadata_xml(
    row: dict[str, Any],
    session: requests.Session,
    cache_dir: Path,
    timeout: int,
    incremental: bool,
    metadata_registry_entry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = dict(row)
    cache_path = cache_dir / safe_metadata_filename(row["metadata_filename"])
    cached_content = read_cached_xml_if_usable(
        cache_path,
        row,
        incremental,
        metadata_registry_entry=metadata_registry_entry,
    )

    if cached_content is not None:
        row["xml_fetch_status"] = "cached"
        row["raw_xml_path"] = str(cache_path)
        row["xml_sha256"] = sha256_bytes(cached_content)
        return row

    try:
        response = session.get(row["metadata_url"], timeout=timeout)
        response.raise_for_status()
        content = response.content
        cache_path.write_bytes(content)
        row["xml_fetch_status"] = "fetched"
        row["raw_xml_path"] = str(cache_path)
        row["xml_sha256"] = sha256_bytes(content)
    except requests.RequestException as exc:
        row["xml_fetch_status"] = "failed"
        row["parse_error"] = str(exc)
    return row


def read_cached_xml_if_usable(
    cache_path: Path,
    row: dict[str, Any],
    incremental: bool,
    metadata_registry_entry: dict[str, Any] | None = None,
) -> bytes | None:
    if not incremental or not cache_path.exists():
        return None

    if metadata_listing_has_comparable_fields(row, metadata_registry_entry) and not (
        metadata_listing_matches_registry(row, metadata_registry_entry)
    ):
        return None

    size_hint = row.get("metadata_size_bytes")
    if size_hint not in ("", None):
        try:
            if cache_path.stat().st_size != int(size_hint):
                return None
        except (OSError, ValueError):
            return None

    try:
        return cache_path.read_bytes()
    except OSError:
        return None


def parse_pasda_manifest_row(row: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest_row = dict(row)
    record = empty_normalized_record(manifest_row)

    if manifest_row.get("xml_fetch_status") == "failed":
        manifest_row["xml_parse_status"] = "not_parsed"
        record["xml_parse_status"] = "not_parsed"
        record["parse_error"] = manifest_row.get("parse_error", "")
        return manifest_row, record

    raw_xml_path = manifest_row.get("raw_xml_path", "")
    try:
        content = Path(raw_xml_path).read_bytes()
    except OSError as exc:
        manifest_row["xml_parse_status"] = "failed"
        manifest_row["metadata_profile"] = "empty_or_non_xml"
        manifest_row["metadata_profile_confidence"] = "high"
        manifest_row["parse_error"] = str(exc)
        record.update(
            {
                "metadata_profile": "empty_or_non_xml",
                "metadata_profile_confidence": "high",
                "xml_parse_status": "failed",
                "parse_error": str(exc),
            }
        )
        return manifest_row, record

    detection = detect_metadata_profile(content)
    manifest_row["metadata_profile"] = detection["metadata_profile"]
    manifest_row["metadata_profile_confidence"] = detection["metadata_profile_confidence"]

    if detection["metadata_profile"] in {"malformed_xml", "empty_or_non_xml"}:
        status = "malformed" if detection["metadata_profile"] == "malformed_xml" else "failed"
        manifest_row["xml_parse_status"] = status
        manifest_row["parse_error"] = detection.get("parse_error", "")
        record.update(
            {
                "metadata_profile": detection["metadata_profile"],
                "metadata_profile_confidence": detection["metadata_profile_confidence"],
                "xml_parse_status": status,
                "parse_error": detection.get("parse_error", ""),
            }
        )
        return manifest_row, record

    try:
        root = ET.fromstring(content)
        parsed_record = parse_metadata_root(root, manifest_row, detection)
        manifest_row["xml_parse_status"] = parsed_record["xml_parse_status"]
        manifest_row["parse_error"] = parsed_record.get("parse_error", "")
        record.update(parsed_record)
    except ET.ParseError as exc:
        manifest_row["xml_parse_status"] = "malformed"
        manifest_row["metadata_profile"] = "malformed_xml"
        manifest_row["metadata_profile_confidence"] = "high"
        manifest_row["parse_error"] = str(exc)
        record.update(
            {
                "metadata_profile": "malformed_xml",
                "metadata_profile_confidence": "high",
                "xml_parse_status": "malformed",
                "parse_error": str(exc),
            }
        )
    except Exception as exc:
        LOGGER.exception("PASDA parse failure for %s", manifest_row.get("metadata_filename"))
        manifest_row["xml_parse_status"] = "failed"
        manifest_row["parse_error"] = str(exc)
        record.update(
            {
                "metadata_profile": detection["metadata_profile"],
                "metadata_profile_confidence": detection["metadata_profile_confidence"],
                "xml_parse_status": "failed",
                "parse_error": str(exc),
            }
        )

    return manifest_row, record


def detect_metadata_profile(content: bytes | str) -> dict[str, str]:
    if isinstance(content, str):
        content = content.encode("utf-8", errors="replace")
    if not content or not content.strip():
        return profile_result("empty_or_non_xml", "high", "Empty XML content")

    stripped = content.lstrip()
    if not stripped.startswith(b"<"):
        return profile_result("empty_or_non_xml", "high", "Content does not begin with XML markup")

    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        return profile_result("malformed_xml", "high", str(exc))

    root_name = local_name(root.tag).lower()
    ns_uris = {namespace_uri(element.tag).lower() for element in root.iter() if namespace_uri(element.tag)}
    element_names = {local_name(element.tag).lower() for element in root.iter()}

    if root_name == "md_metadata" or any("isotc211.org/2005/gmd" in uri for uri in ns_uris):
        confidence = "high" if "identificationinfo" in element_names else "medium"
        return profile_result("iso_19139", confidence)

    fgdc_score = len(FGDC_TAGS & element_names)
    if root_name == "metadata" and fgdc_score >= 4:
        return profile_result("fgdc_csdgm", "high")
    if root_name == "metadata" and fgdc_score >= 2:
        return profile_result("fgdc_csdgm", "medium")

    arcgis_score = len(ARCGIS_TAGS & element_names)
    if arcgis_score >= 3:
        return profile_result("arcgis_metadata", "high")
    if arcgis_score >= 1:
        return profile_result("arcgis_metadata", "medium")

    return profile_result("unknown_xml", "low")


def parse_metadata_root(
    root: ET.Element,
    manifest_row: dict[str, Any],
    detection: dict[str, str],
) -> dict[str, Any]:
    profile = detection["metadata_profile"]
    if profile == "fgdc_csdgm":
        parsed = parse_fgdc_metadata(root, manifest_row)
    elif profile == "iso_19139":
        parsed = parse_iso_19139_metadata(root, manifest_row)
    elif profile == "arcgis_metadata":
        parsed = parse_arcgis_metadata(root, manifest_row)
    else:
        parsed = parse_unknown_xml_metadata(root, manifest_row)

    parsed["metadata_profile"] = profile
    parsed["metadata_profile_confidence"] = detection["metadata_profile_confidence"]
    if parsed.get("xml_parse_status") in {"", "pending", None}:
        parsed["xml_parse_status"] = "parsed"
    return parsed


def parse_fgdc_metadata(root: ET.Element, manifest_row: dict[str, Any]) -> dict[str, Any]:
    record = empty_normalized_record(manifest_row)
    record.update(
        {
            "title": first_text(root, ["idinfo/citation/citeinfo/title", "idinfo/citation/title"]),
            "alternate_title": first_text(root, ["idinfo/citation/citeinfo/edition"]),
            "abstract": first_text(root, ["idinfo/descript/abstract"]),
            "purpose": first_text(root, ["idinfo/descript/purpose"]),
            "status": first_text(root, ["idinfo/status/progress"]),
            "creator": first_text(root, ["idinfo/citation/citeinfo/origin"]),
            "publisher": first_text(root, ["idinfo/citation/citeinfo/pubinfo/publish"]),
            "publication_date": first_text(root, ["idinfo/citation/citeinfo/pubdate"]),
            "issued": first_text(root, ["idinfo/citation/citeinfo/pubdate"]),
            "modified": first_text(root, ["idinfo/citation/citeinfo/revdate"]),
            "temporal_start": first_text(root, ["idinfo/timeperd/timeinfo/rngdates/begdate"]),
            "temporal_end": first_text(root, ["idinfo/timeperd/timeinfo/rngdates/enddate"]),
            "west_bbox": first_text(root, ["idinfo/spdom/bounding/westbc"]),
            "east_bbox": first_text(root, ["idinfo/spdom/bounding/eastbc"]),
            "south_bbox": first_text(root, ["idinfo/spdom/bounding/southbc"]),
            "north_bbox": first_text(root, ["idinfo/spdom/bounding/northbc"]),
            "spatial_reference": fgdc_spatial_reference(root),
            "native_data_set_environment": first_text(root, ["eainfo/detailed/attr/attrdefs"]),
            "license_or_use_constraints": first_text(root, ["idinfo/useconst"]),
            "access_constraints": first_text(root, ["idinfo/accconst"]),
            "use_constraints": first_text(root, ["idinfo/useconst"]),
            "lineage": first_text(root, ["dataqual/lineage/procstep/procdesc"]),
            "source_scale": first_text(root, ["idinfo/citation/citeinfo/geoform"]),
            "metadata_standard_name": first_text(root, ["metainfo/metstdn"]),
            "metadata_standard_version": first_text(root, ["metainfo/metstdv"]),
            "metadata_date": first_text(root, ["metainfo/metd"]),
            "metadata_contact_org": first_text(root, ["metainfo/metc/cntinfo/cntorgp/cntorg"]),
            "metadata_contact_email": first_text(root, ["metainfo/metc/cntinfo/cntemail"]),
            "distributor": first_text(root, ["distinfo/distrib/cntinfo/cntorgp/cntorg"]),
            "contact_org": first_text(root, ["idinfo/ptcontac/cntinfo/cntorgp/cntorg"]),
            "contact_person": first_text(root, ["idinfo/ptcontac/cntinfo/cntperp/cntper"]),
            "contact_email": first_text(root, ["idinfo/ptcontac/cntinfo/cntemail"]),
            "data_format": fgdc_data_format(root),
        }
    )
    record["theme_keywords"] = all_text(root, "idinfo/keywords/theme/themekey")
    record["place_keywords"] = all_text(root, "idinfo/keywords/place/placekey")
    record["online_links"] = dedupe_list(all_text(root, ".//onlink") + extract_urls_from_text(root))
    record["distribution_links"] = dedupe_list(all_text(root, ".//networka/networkr"))
    record["download_links_found_in_metadata"] = filter_download_links(record["online_links"])
    record["service_links_found_in_metadata"] = filter_service_links(record["online_links"])
    record["parse_warnings"] = missing_required_warnings(record, ["title"])
    return record


def fgdc_data_format(root: ET.Element) -> str:
    values = []
    values.extend(values_for_local_names(root, {"formatName", "formname"}))
    values.extend(
        values_for_paths_by_local_name(
            root,
            [
                ("distinfo", "stdorder", "digform", "digtinfo", "formname"),
                ("distorFormat", "formatName"),
                ("distFormat", "formatName"),
            ],
        )
    )
    values = [value for value in values if value.strip().lower() not in {"true", "false"}]
    return "|".join(dedupe_list(values))


def fgdc_spatial_reference(root: ET.Element) -> str:
    evidence = {
        "mapprojn": first_by_local_names(root, ["mapprojn"]),
        "projcsn": first_by_local_names(root, ["projcsn"]),
        "geogcsn": first_by_local_names(root, ["geogcsn"]),
        "horizdn": first_by_local_names(root, ["horizdn"]),
        "ellips": first_by_local_names(root, ["ellips"]),
        "gridsysn": first_by_local_names(root, ["gridsysn"]),
        "utmzone": first_by_local_names(root, ["utmzone"]),
        "spcszone": first_by_local_names(root, ["spcszone"]),
        "plandu": first_by_local_names(root, ["plandu"]),
        "geogunit": first_by_local_names(root, ["geogunit"]),
        "identCode": fgdc_first_ident_code(root),
    }
    epsg = fgdc_spatial_reference_epsg(evidence)
    if epsg:
        return f"https://spatialreference.org/ref/epsg/{epsg}/"
    return fgdc_spatial_reference_description(evidence)


def fgdc_first_ident_code(root: ET.Element) -> str:
    for element in root.iter():
        if local_name(element.tag) != "identCode":
            continue
        text_value = clean_text(" ".join(element.itertext()))
        code_value = clean_text(element.attrib.get("code", ""))
        for value in [text_value, code_value]:
            if value and value != "0":
                return value
    return ""


def fgdc_spatial_reference_epsg(evidence: dict[str, str]) -> str:
    ident_code = clean_text(evidence.get("identCode", ""))
    if re.fullmatch(r"\d{4,5}", ident_code) and ident_code != "0":
        return ident_code

    for key in ["projcsn", "mapprojn"]:
        epsg = SPATIALREFERENCE_EPSG_LOOKUP.get(normalize_crs_key(evidence.get(key, "")))
        if epsg:
            return epsg

    projected_evidence = any(
        clean_text(evidence.get(key, ""))
        for key in ["projcsn", "mapprojn", "gridsysn", "utmzone", "spcszone", "plandu"]
    )
    gridsysn = normalize_crs_key(evidence.get("gridsysn", ""))
    datum = normalize_crs_key(evidence.get("horizdn", ""))
    unit = normalize_crs_key(evidence.get("plandu", ""))
    utmzone = clean_text(evidence.get("utmzone", ""))
    spcszone = clean_text(evidence.get("spcszone", ""))

    if gridsysn == "universal_transverse_mercator" and datum in {
        "north_american_datum_of_1983",
        "d_north_american_1983",
        "nad_83",
    }:
        if utmzone in {"13", "17", "18"}:
            return f"269{int(utmzone):02d}"

    if spcszone in {"3701", "3702"}:
        if unit in {"survey_feet", "foot_us", "foot_us", "feet", "foot"}:
            return "2271" if spcszone == "3701" else "2272"
        if unit in {"meters", "meter"}:
            return "32128" if spcszone == "3701" else "32129"

    if spcszone == "Pennsylvania, South":
        return "2272" if unit in {"survey_feet", "foot_us", "feet", "foot"} else ""

    if not projected_evidence:
        for key in ["geogcsn", "horizdn"]:
            epsg = SPATIALREFERENCE_EPSG_LOOKUP.get(normalize_crs_key(evidence.get(key, "")))
            if epsg:
                return epsg

    return ""


def fgdc_spatial_reference_description(evidence: dict[str, str]) -> str:
    parts = []
    labels = [
        ("Projected CRS", evidence.get("projcsn", "") or evidence.get("mapprojn", "")),
        ("Geographic CRS", evidence.get("geogcsn", "")),
        ("Projection", evidence.get("mapprojn", "")),
        ("Grid", evidence.get("gridsysn", "")),
        ("UTM Zone", evidence.get("utmzone", "")),
        ("State Plane Zone", evidence.get("spcszone", "")),
        ("Datum", evidence.get("horizdn", "")),
        ("Spheroid", evidence.get("ellips", "")),
        ("Planar Units", evidence.get("plandu", "")),
        ("Geographic Units", evidence.get("geogunit", "")),
        ("Identifier", evidence.get("identCode", "")),
    ]
    for label, value in labels:
        clean_value = clean_text(value)
        if clean_value:
            parts.append(f"{label}: {clean_value}")
    return "; ".join(dedupe_list(parts))


def normalize_crs_key(value: str) -> str:
    clean_value = clean_text(value).lower()
    clean_value = clean_value.replace("&", " and ")
    clean_value = re.sub(r"[^a-z0-9]+", "_", clean_value)
    return clean_value.strip("_")


def parse_iso_19139_metadata(root: ET.Element, manifest_row: dict[str, Any]) -> dict[str, Any]:
    record = empty_normalized_record(manifest_row)
    texts = texts_by_local_name(root)
    urls = dedupe_list(values_for_local_names(root, {"url", "linkage"}) + extract_urls_from_text(root))
    bbox = extract_iso_bbox(root)

    record.update(
        {
            "title": first_value(
                values_for_paths_by_local_name(
                    root,
                    [
                        ("identificationInfo", "MD_DataIdentification", "citation", "CI_Citation", "title"),
                        ("identificationInfo", "SV_ServiceIdentification", "citation", "CI_Citation", "title"),
                    ],
                )
            ),
            "abstract": first_value(texts.get("abstract", [])),
            "purpose": first_value(texts.get("purpose", [])),
            "status": first_value(texts.get("MD_ProgressCode".lower(), [])),
            "publication_date": first_iso_date_by_type(root, "publication"),
            "issued": first_iso_date_by_type(root, "creation") or first_iso_date_by_type(root, "publication"),
            "modified": first_iso_date_by_type(root, "revision"),
            "metadata_date": first_value(texts.get("dateStamp".lower(), [])),
            "west_bbox": bbox.get("west_bbox", ""),
            "east_bbox": bbox.get("east_bbox", ""),
            "south_bbox": bbox.get("south_bbox", ""),
            "north_bbox": bbox.get("north_bbox", ""),
            "metadata_standard_name": first_value(texts.get("metadataStandardName".lower(), [])),
            "metadata_standard_version": first_value(texts.get("metadataStandardVersion".lower(), [])),
            "lineage": first_value(texts.get("statement", [])),
            "source_scale": first_value(texts.get("denominator", [])),
            "license_or_use_constraints": first_value(texts.get("useLimitation".lower(), [])),
            "access_constraints": first_value(texts.get("accessConstraints".lower(), [])),
            "use_constraints": first_value(texts.get("useConstraints".lower(), [])),
            "data_format": first_value(texts.get("name", [])),
            "spatial_reference": first_value(texts.get("code", [])),
            "contact_org": first_value(texts.get("organisationName".lower(), [])),
            "contact_person": first_value(texts.get("individualName".lower(), [])),
            "contact_email": first_value(texts.get("electronicMailAddress".lower(), [])),
            "metadata_contact_org": first_value(texts.get("organisationName".lower(), [])),
            "metadata_contact_email": first_value(texts.get("electronicMailAddress".lower(), [])),
        }
    )
    record["creator"] = first_responsible_party_org(root, {"originator", "author", "principalInvestigator"})
    record["publisher"] = first_responsible_party_org(root, {"publisher"})
    record["provider"] = first_responsible_party_org(root, {"resourceProvider", "custodian"})
    record["distributor"] = first_responsible_party_org(root, {"distributor"})
    record["theme_keywords"] = values_for_local_names(root, {"keyword"})
    record["place_keywords"] = values_for_local_names(root, {"geographicIdentifier"})
    record["iso_topic_categories"] = values_for_local_names(root, {"MD_TopicCategoryCode"})
    record["online_links"] = urls
    record["distribution_links"] = urls
    record["download_links_found_in_metadata"] = filter_download_links(urls)
    record["service_links_found_in_metadata"] = filter_service_links(urls)
    record["parse_warnings"] = missing_required_warnings(record, ["title"])
    return record


def parse_arcgis_metadata(root: ET.Element, manifest_row: dict[str, Any]) -> dict[str, Any]:
    record = empty_normalized_record(manifest_row)
    urls = dedupe_list(extract_urls_from_text(root) + values_for_local_names(root, {"url", "linkage"}))
    record.update(
        {
            "title": first_by_local_names(root, ["resTitle", "title", "idCitation"]),
            "alternate_title": first_by_local_names(root, ["searchKeys"]),
            "abstract": first_by_local_names(root, ["idAbs", "abstract", "summary"]),
            "purpose": first_by_local_names(root, ["idPurp", "purpose"]),
            "status": first_by_local_names(root, ["resStatus"]),
            "creator": first_by_local_names(root, ["idCredit", "origin", "originator"]),
            "publisher": first_by_local_names(root, ["publisher"]),
            "provider": first_by_local_names(root, ["dataIdInfo", "envirDesc"]),
            "publication_date": first_by_local_names(root, ["pubDate", "date"]),
            "modified": first_by_local_names(root, ["revDate", "ModDate", "SyncDate"]),
            "west_bbox": first_by_local_names(root, ["westBL", "westbc"]),
            "east_bbox": first_by_local_names(root, ["eastBL", "eastbc"]),
            "south_bbox": first_by_local_names(root, ["southBL", "southbc"]),
            "north_bbox": first_by_local_names(root, ["northBL", "northbc"]),
            "spatial_reference": first_by_local_names(root, ["refSysName", "projection", "spref"]),
            "native_data_set_environment": first_by_local_names(root, ["envirDesc", "native"]),
            "license_or_use_constraints": first_by_local_names(root, ["useLimit", "useconst"]),
            "access_constraints": first_by_local_names(root, ["accessConsts", "accconst"]),
            "use_constraints": first_by_local_names(root, ["useConsts", "useconst"]),
            "metadata_standard_name": first_by_local_names(root, ["mdStanName", "metstdn"]),
            "metadata_standard_version": first_by_local_names(root, ["mdStanVer", "metstdv"]),
            "metadata_date": first_by_local_names(root, ["mdDateSt", "metd"]),
            "contact_org": first_by_local_names(root, ["rpOrgName", "cntorg"]),
            "contact_person": first_by_local_names(root, ["rpIndName", "cntper"]),
            "contact_email": first_by_local_names(root, ["eMailAdd", "cntemail"]),
            "data_format": first_by_local_names(root, ["formatName", "formname"]),
            "lineage": first_by_local_names(root, ["statement", "procdesc", "lineage"]),
        }
    )
    record["theme_keywords"] = values_for_local_names(root, {"keyword", "themeKeys", "themekey"})
    record["place_keywords"] = values_for_local_names(root, {"placekey", "placeKeys"})
    record["online_links"] = urls
    record["distribution_links"] = urls
    record["download_links_found_in_metadata"] = filter_download_links(urls)
    record["service_links_found_in_metadata"] = filter_service_links(urls)
    record["parse_warnings"] = missing_required_warnings(record, ["title"])
    if not record["parse_warnings"]:
        record["parse_warnings"] = ["arcgis_metadata_parser_is_heuristic"]
    else:
        record["parse_warnings"].append("arcgis_metadata_parser_is_heuristic")
    return record


def parse_unknown_xml_metadata(root: ET.Element, manifest_row: dict[str, Any]) -> dict[str, Any]:
    record = empty_normalized_record(manifest_row)
    record.update(
        {
            "title": first_by_local_names(root, ["title", "name"]),
            "abstract": first_by_local_names(root, ["abstract", "description", "summary"]),
            "metadata_standard_name": first_by_local_names(root, ["metadataStandardName", "metstdn"]),
            "metadata_standard_version": first_by_local_names(root, ["metadataStandardVersion", "metstdv"]),
            "xml_parse_status": "partial",
        }
    )
    urls = extract_urls_from_text(root)
    record["online_links"] = urls
    record["download_links_found_in_metadata"] = filter_download_links(urls)
    record["service_links_found_in_metadata"] = filter_service_links(urls)
    record["parse_warnings"] = ["unknown_xml_profile"]
    return record


def empty_normalized_record(manifest_row: dict[str, Any]) -> dict[str, Any]:
    record = {field: "" for field in NORMALIZED_FIELDS}
    for field in [
        "place_keywords",
        "theme_keywords",
        "iso_topic_categories",
        "online_links",
        "distribution_links",
        "download_links_found_in_metadata",
        "service_links_found_in_metadata",
        "parse_warnings",
    ]:
        record[field] = []

    record.update(
        {
            "source_system": "PASDA",
            "source_record_id": manifest_row.get("metadata_file_stem", ""),
            "metadata_filename": manifest_row.get("metadata_filename", ""),
            "metadata_url": manifest_row.get("metadata_url", ""),
            "metadata_profile": manifest_row.get("metadata_profile", ""),
            "metadata_profile_confidence": manifest_row.get("metadata_profile_confidence", ""),
            "xml_parse_status": manifest_row.get("xml_parse_status", ""),
            "parse_error": manifest_row.get("parse_error", ""),
            "raw_xml_path": manifest_row.get("raw_xml_path", ""),
            "xml_sha256": manifest_row.get("xml_sha256", ""),
        }
    )
    return record


def build_error_row(manifest_row: dict[str, Any], normalized_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "metadata_filename": manifest_row.get("metadata_filename", ""),
        "metadata_url": manifest_row.get("metadata_url", ""),
        "xml_fetch_status": manifest_row.get("xml_fetch_status", ""),
        "xml_parse_status": manifest_row.get("xml_parse_status", ""),
        "metadata_profile": manifest_row.get("metadata_profile", ""),
        "metadata_profile_confidence": manifest_row.get("metadata_profile_confidence", ""),
        "parse_error": manifest_row.get("parse_error") or normalized_record.get("parse_error", ""),
        "raw_xml_path": manifest_row.get("raw_xml_path", ""),
        "xml_sha256": manifest_row.get("xml_sha256", ""),
    }


def build_profile_summary(manifest_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[tuple[str, str], int] = {}
    for row in manifest_rows:
        key = (row.get("metadata_profile", "") or "unknown", row.get("xml_parse_status", "") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return [
        {"metadata_profile": profile, "xml_parse_status": status, "count": count}
        for (profile, status), count in sorted(counts.items())
    ]


def write_csv_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    for row in rows[1:]:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def profile_result(
    metadata_profile: str,
    confidence: str,
    parse_error: str = "",
) -> dict[str, str]:
    return {
        "metadata_profile": metadata_profile,
        "metadata_profile_confidence": confidence,
        "parse_error": parse_error,
    }


def parse_listing_last_modified(text: str) -> str:
    patterns = [
        r"\b(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})\b",
        r"\b(\d{1,2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2})\b",
        r"\b(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s*[AP]M?)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1).strip()
    return ""


def parse_listing_size_bytes(text: str, filename: str = "") -> int | str:
    cleaned = text.replace(filename, " ")
    matches = re.findall(r"(?<![-/:])\b(\d+(?:\.\d+)?)([KMGTP]B?|B)\b", cleaned, re.I)
    for number, unit in reversed(matches):
        unit = unit.upper()
        if unit in {"B", "K", "KB", "M", "MB", "G", "GB", "T", "TB"}:
            value = float(number)
            multiplier = {
                "B": 1,
                "K": 1024,
                "KB": 1024,
                "M": 1024**2,
                "MB": 1024**2,
                "G": 1024**3,
                "GB": 1024**3,
                "T": 1024**4,
                "TB": 1024**4,
            }[unit]
            return int(value * multiplier)

    plain_byte_matches = re.findall(r"(?:\s|>)(\d{3,})(?:\s|<|$)", cleaned)
    if plain_byte_matches:
        return int(plain_byte_matches[-1])
    return ""


def normalize_file_stem(stem: str) -> str:
    value = stem.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def infer_provider_token(stem: str) -> str:
    normalized = normalize_file_stem(stem)
    parts = normalized.split("_")
    return parts[0] if parts else ""


def infer_date_tokens(stem: str) -> list[str]:
    return dedupe_list(re.findall(r"\b(?:19|20)\d{2}\b", stem))


def safe_metadata_filename(filename: str) -> str:
    return Path(filename).name


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def namespace_uri(tag: str) -> str:
    if tag.startswith("{") and "}" in tag:
        return tag[1:].split("}", 1)[0]
    return ""


def clean_text(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def first_text(root: ET.Element, paths: list[str]) -> str:
    for path in paths:
        element = root.find(path)
        if element is not None:
            text = clean_text(" ".join(element.itertext()))
            if text:
                return text
    return ""


def all_text(root: ET.Element, path: str) -> list[str]:
    values = []
    for element in root.findall(path):
        text = clean_text(" ".join(element.itertext()))
        if text:
            values.append(text)
    return dedupe_list(values)


def first_value(values: list[str] | None) -> str:
    if not values:
        return ""
    return values[0]


def dedupe_list(values: list[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        clean = clean_text(value)
        if clean and clean not in seen:
            seen.add(clean)
            result.append(clean)
    return result


def texts_by_local_name(root: ET.Element) -> dict[str, list[str]]:
    values: dict[str, list[str]] = {}
    for element in root.iter():
        name = local_name(element.tag).lower()
        text = clean_text(element.text)
        if not text:
            text = clean_text(" ".join(element.itertext()))
        if text:
            values.setdefault(name, []).append(text)
    return values


def values_for_local_names(root: ET.Element, names: set[str]) -> list[str]:
    lookup = {name.lower() for name in names}
    values = []
    for element in root.iter():
        if local_name(element.tag).lower() in lookup:
            text = clean_text(" ".join(element.itertext()))
            if text:
                values.append(text)
        for attr_value in element.attrib.values():
            if clean_text(attr_value) and local_name(element.tag).lower() in lookup:
                values.append(attr_value)
    return dedupe_list(values)


def first_by_local_names(root: ET.Element, names: list[str]) -> str:
    values = values_for_local_names(root, set(names))
    return values[0] if values else ""


def extract_urls_from_text(root: ET.Element) -> list[str]:
    text = " ".join(clean_text(value) for value in root.itertext())
    urls = URL_RE.findall(text)
    for element in root.iter():
        urls.extend(URL_RE.findall(" ".join(element.attrib.values())))
    return dedupe_list([url.rstrip(".,);]") for url in urls])


def filter_download_links(urls: list[str]) -> list[str]:
    hints = ("download", ".zip", ".gdb", ".shp", ".csv", ".geojson", ".kml", ".kmz")
    return [url for url in urls if any(hint in url.lower() for hint in hints)]


def filter_service_links(urls: list[str]) -> list[str]:
    return [url for url in urls if SERVICE_URL_RE.search(url)]


def missing_required_warnings(record: dict[str, Any], fields: list[str]) -> list[str]:
    return [f"missing_{field}" for field in fields if not record.get(field)]


def values_for_paths_by_local_name(root: ET.Element, paths: list[tuple[str, ...]]) -> list[str]:
    values = []
    for element in root.iter():
        for path in paths:
            matched = find_descendant_path_by_local_name(element, path)
            if matched is not None:
                text = clean_text(" ".join(matched.itertext()))
                if text:
                    values.append(text)
    return dedupe_list(values)


def find_descendant_path_by_local_name(
    element: ET.Element,
    path: tuple[str, ...],
) -> ET.Element | None:
    current = element
    for part in path:
        next_element = None
        for child in list(current):
            if local_name(child.tag) == part:
                next_element = child
                break
        if next_element is None:
            return None
        current = next_element
    return current


def extract_iso_bbox(root: ET.Element) -> dict[str, str]:
    bbox = {"west_bbox": "", "east_bbox": "", "south_bbox": "", "north_bbox": ""}
    name_map = {
        "westBoundLongitude": "west_bbox",
        "eastBoundLongitude": "east_bbox",
        "southBoundLatitude": "south_bbox",
        "northBoundLatitude": "north_bbox",
    }
    for element in root.iter():
        key = name_map.get(local_name(element.tag))
        if not key:
            continue
        value = clean_text(" ".join(element.itertext()))
        if not value:
            value = clean_text(element.attrib.get("{http://www.isotc211.org/2005/gco}Decimal", ""))
        if not value:
            value = clean_text(element.attrib.get("value", ""))
        bbox[key] = value
    return bbox


def first_iso_date_by_type(root: ET.Element, date_type: str) -> str:
    for citation_date in root.iter():
        if local_name(citation_date.tag) != "CI_Date":
            continue
        found_type = ""
        found_date = ""
        for element in citation_date.iter():
            element_name = local_name(element.tag)
            if element_name in {"Date", "date", "DateTime"}:
                found_date = clean_text(element.text) or found_date
            if element_name == "CI_DateTypeCode":
                found_type = clean_text(element.attrib.get("codeListValue", "")) or clean_text(element.text)
        if found_type == date_type and found_date:
            return found_date
    return ""


def first_responsible_party_org(root: ET.Element, roles: set[str]) -> str:
    role_lookup = {role.lower() for role in roles}
    for party in root.iter():
        if local_name(party.tag) != "CI_ResponsibleParty":
            continue
        org = ""
        role = ""
        for element in party.iter():
            if local_name(element.tag) == "organisationName":
                org = clean_text(" ".join(element.itertext())) or org
            if local_name(element.tag) == "CI_RoleCode":
                role = clean_text(element.attrib.get("codeListValue", "")) or clean_text(element.text)
        if role.lower() in role_lookup and org:
            return org
    return ""


def pasda_asset_match_context_by_record_id(
    asset_match_review_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        clean_text(row.get("source_record_id", "")): dict(row)
        for row in asset_match_review_rows
        if clean_text(row.get("source_record_id", ""))
    }


def pasda_series_context_by_record_id(
    series_review_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    contexts = {}
    for row in series_review_rows:
        record_ids = clean_text(row.get("record_ids", "")).split("|")
        record_dates = clean_text(row.get("inferred_record_dates", "")).split("|")
        metadata_filenames = clean_text(row.get("metadata_filenames", "")).split("|")
        for index, record_id in enumerate(record_ids):
            clean_record_id = clean_text(record_id)
            if not clean_record_id:
                continue
            context = dict(row)
            context["record_date"] = pasda_series_date_from_text(clean_record_id) or (
                record_dates[index] if index < len(record_dates) else ""
            )
            context["record_metadata_filename"] = (
                metadata_filenames[index] if index < len(metadata_filenames) else ""
            )
            contexts[clean_record_id] = context
    return contexts


def pasda_asset_review_flags(asset_match_context: dict[str, Any]) -> list[str]:
    flags = []
    match_status = clean_text(asset_match_context.get("pasda_asset_match_status", ""))
    if match_status:
        flags.append(f"pasda_asset_match_status_{match_status}")
    return flags


def build_pasda_aardvark_draft_records(
    normalized_records: list[dict[str, Any]],
    accession_date: str | None = None,
    county_lookup: dict[str, Any] | None = None,
    asset_match_review_rows: list[dict[str, Any]] | None = None,
    series_review_rows: list[dict[str, Any]] | None = None,
    ready_only: bool = False,
) -> list[dict[str, Any]]:
    accession_date = accession_date or time.strftime("%Y-%m-%d")
    asset_match_by_id = pasda_asset_match_context_by_record_id(asset_match_review_rows or [])
    series_context_by_id = pasda_series_context_by_record_id(series_review_rows or [])
    draft_records = []
    for record in normalized_records:
        source_record_id = clean_text(record.get("source_record_id", ""))
        asset_match_context = asset_match_by_id.get(source_record_id, {})
        if ready_only and asset_match_context.get("pasda_distribution_candidate") != "ready":
            continue
        if ready_only and not pasda_record_is_upload_parseable(record):
            continue
        draft_records.append(
            build_pasda_aardvark_draft_record(
                record,
                accession_date=accession_date,
                county_lookup=county_lookup,
                asset_match_context=asset_match_context,
                series_context=series_context_by_id.get(source_record_id, {}),
            )
        )
    return draft_records


def build_pasda_aardvark_draft_dataframe(
    normalized_records: list[dict[str, Any]],
    accession_date: str | None = None,
    county_lookup: dict[str, Any] | None = None,
    theme_map: dict[str, str] | None = None,
    asset_match_review_rows: list[dict[str, Any]] | None = None,
    series_review_rows: list[dict[str, Any]] | None = None,
    ready_only: bool = False,
) -> pd.DataFrame:
    draft_df = pd.DataFrame(
        build_pasda_aardvark_draft_records(
            normalized_records,
            accession_date=accession_date,
            county_lookup=county_lookup,
            asset_match_review_rows=asset_match_review_rows,
            series_review_rows=series_review_rows,
            ready_only=ready_only,
        )
    )
    if theme_map:
        draft_df = derive_themes_from_keywords(draft_df, theme_map)
    return draft_df.reindex(columns=PASDA_AARDVARK_DRAFT_FIELDS)


def build_pasda_aardvark_draft_record(
    record: dict[str, Any],
    accession_date: str,
    county_lookup: dict[str, Any] | None = None,
    asset_match_context: dict[str, Any] | None = None,
    series_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = {field: "" for field in PASDA_AARDVARK_DRAFT_FIELDS}
    review_flags = []
    source_record_id = clean_text(record.get("source_record_id", ""))
    metadata_url = clean_text(record.get("metadata_url", ""))
    publication_date = normalize_pasda_date(record.get("publication_date", ""))
    modified_date = normalize_pasda_date(record.get("modified", ""))
    metadata_date = normalize_pasda_date(record.get("metadata_date", ""))
    temporal_start = normalize_pasda_date(record.get("temporal_start", ""))
    temporal_end = normalize_pasda_date(record.get("temporal_end", ""))
    bbox, bbox_flag = pasda_bounding_box(record)
    original_title = clean_text(record.get("title", ""))
    asset_match_context = asset_match_context or {}
    series_context = series_context or {}
    title = pasda_title(record, publication_date, modified_date, metadata_date)
    title = pasda_title_with_series_date(title, record, series_context)
    local_collection = pasda_local_collection_value(series_context)
    display_note = pasda_display_note_value(series_context)

    if bbox_flag:
        review_flags.append(bbox_flag)
    if not record.get("title"):
        review_flags.append("missing_title")
    if not publication_date and not modified_date:
        review_flags.append("missing_date")
    if record.get("xml_parse_status") != "parsed":
        review_flags.append(f"xml_parse_status_{record.get('xml_parse_status', 'unknown')}")
    if not record.get("download_links_found_in_metadata"):
        review_flags.append("no_download_link_in_metadata")

    row.update(
        {
            "ID": pasda_record_id_from_source(source_record_id),
            "Code": "08a-01",
            "Title": title,
            "Alternative Title": pasda_alternative_title(record, original_title),
            "Description": pasda_description(record),
            "Language": "eng",
            "Creator": clean_text(record.get("creator", "")),
            "Publisher": "Pennsylvania Spatial Data Access (PASDA)",
            "Resource Class": pasda_resource_class(record),
            "Resource Type": pasda_resource_type(record),
            "Keyword": pasda_keywords(record),
            "Temporal Coverage": pasda_temporal_coverage_value(
                temporal_start,
                temporal_end,
                publication_date,
                modified_date,
                metadata_date,
            ),
            "Date Issued": publication_date,
            "Date Range": pasda_date_range(temporal_start, temporal_end, publication_date, modified_date),
            "Spatial Coverage": pasda_spatial_coverage_value(record, county_lookup=county_lookup),
            "Bounding Box": bbox,
            "Coordinate Reference System": clean_text(record.get("spatial_reference", "")),
            "Access Rights": "Public",
            "Rights": pasda_rights(record),
            "Format": pasda_format(record),
            "Display Note": display_note,
            "Local Collection": local_collection,
            "Date Accessioned": accession_date,
            "Publication State": "draft",
            "Identifier": metadata_url,
            "Provenance": pasda_provenance(record, accession_date=accession_date),
            "Website Platform": "PASDA metadata directory",
            "Accrual Method": "Automated retrieval",
            "Harvest Workflow": "py_pasda_metadata_directory",
            "Admin Note": pasda_admin_note(record),
            "pasda_xml_parse_status": clean_text(record.get("xml_parse_status", "")),
            "pasda_raw_xml_path": clean_text(record.get("raw_xml_path", "")),
            "pasda_review_flags": "|".join(
                dedupe_list(
                    [
                        *review_flags,
                        *pasda_asset_review_flags(asset_match_context),
                    ]
                )
            ),
        }
    )
    return row


def pasda_description(record: dict[str, Any]) -> str:
    values = [clean_text(record.get("abstract", ""))]
    purpose = clean_text(record.get("purpose", ""))
    if purpose and purpose not in values:
        values.append(f"Purpose: {purpose}")
    return "|".join(value for value in values if value)


def pasda_title(
    record: dict[str, Any],
    publication_date: str = "",
    modified_date: str = "",
    metadata_date: str = "",
) -> str:
    title = format_pasda_title_dates(clean_text(record.get("title", "")))
    if not title:
        return ""

    spatial_label = pasda_primary_spatial_label(record)
    if spatial_label and not pasda_title_has_place_context(title, record, spatial_label):
        title = f"{title} [{spatial_label}]"
    return title


def pasda_title_with_series_date(
    title: str,
    record: dict[str, Any],
    series_context: dict[str, Any],
) -> str:
    clean_title = clean_text(title)
    if not series_context:
        return clean_title
    date_value = clean_text(series_context.get("record_date", "")) or pasda_series_record_date(record)
    if not clean_title or not date_value:
        return clean_title
    if pasda_title_already_has_date(clean_title, date_value):
        return clean_title
    return f"{clean_title} {{{date_value}}}"


def pasda_title_already_has_date(title: str, date_value: str) -> bool:
    clean_title = clean_text(title)
    clean_date = clean_text(date_value)
    if not clean_title or not clean_date:
        return False
    compact_date = clean_date.replace("-", "")
    loose_date = clean_date.replace("-", " ")
    if clean_date in clean_title or compact_date in clean_title or loose_date in clean_title:
        return True
    if re.fullmatch(r"\d{4}", clean_date):
        return bool(re.search(rf"(?<!\d){re.escape(clean_date)}(?!\d)", clean_title))
    return False


def pasda_local_collection_value(series_context: dict[str, Any]) -> str:
    if pasda_series_context_record_count(series_context) < 2:
        return ""
    series_title = clean_text(series_context.get("series_title", ""))
    series_key = clean_text(series_context.get("series_key", ""))
    label = series_title or series_key
    return f"PASDA series: {label}" if label else ""


def pasda_display_note_value(series_context: dict[str, Any]) -> str:
    if pasda_series_context_record_count(series_context) < 2:
        return ""
    return (
        "Info: This record is part of a PASDA dataset series and may represent "
        "a historical snapshot. Check PASDA or the data creator for the most current "
        "available version: https://www.pasda.psu.edu"
    )


def pasda_series_context_record_count(series_context: dict[str, Any]) -> int:
    try:
        return int(series_context.get("record_count", 0) or 0)
    except (TypeError, ValueError):
        return 0


def format_pasda_title_dates(title: str) -> str:
    return re.sub(
        r"(?<!\d)((?:19|20)\d{2})[_-]?(0[1-9]|1[0-2])(?!\d)",
        r"\1-\2",
        clean_text(title),
    )


def pasda_alternative_title(record: dict[str, Any], original_title: str) -> str:
    return "|".join(
        dedupe_list(
            [
                original_title,
                clean_text(record.get("alternate_title", "")),
            ]
        )
    )


def pasda_primary_spatial_label(record: dict[str, Any]) -> str:
    for value in pasda_place_keyword_candidates(record):
        clean_value = clean_text(value)
        if clean_value and normalize_title_place_key(clean_value) not in BROAD_PLACE_KEYS:
            return clean_value

    spatial_coverage = pasda_spatial_coverage_value(record)
    if not spatial_coverage:
        return ""
    return clean_text(spatial_coverage.split("|", 1)[0])


def pasda_title_has_place_context(
    title: str,
    record: dict[str, Any],
    spatial_label: str,
) -> bool:
    title_key = normalize_title_place_key(title)
    place_keys = [normalize_title_place_key(spatial_label)]
    place_keys.extend(normalize_title_place_key(value) for value in pasda_place_keyword_candidates(record))
    place_keys = [value for value in dedupe_list(place_keys) if len(value) >= 3]

    for place_key in place_keys:
        if place_key and place_key in title_key:
            return True

    return bool(
        re.search(
            r"\b(county|national forest|national monument|watershed|river|bay|creek|lake|"
            r"city of|district of columbia|pennsylvania|colorado|virginia|maryland|"
            r"tennessee|arizona|california|connecticut|netherlands|siberia)\b",
            title,
            re.I,
        )
    )


def normalize_title_place_key(value: str) -> str:
    clean_value = clean_text(value).lower()
    clean_value = clean_value.replace("&", " and ")
    clean_value = re.sub(r"\b(pa|usa|u s|u s a|us)\b", "", clean_value)
    clean_value = re.sub(r"[^a-z0-9]+", " ", clean_value)
    return re.sub(r"\s+", " ", clean_value).strip()


def pasda_keywords(record: dict[str, Any]) -> str:
    values = []
    values.extend(ensure_list(record.get("theme_keywords")))
    values.extend(pasda_place_keyword_candidates(record))
    status = clean_text(record.get("status", ""))
    if status:
        values.append(status)
    return "|".join(dedupe_list(values))


def pasda_place_keyword_candidates(record: dict[str, Any]) -> list[str]:
    values = []
    for value in ensure_list(record.get("place_keywords")):
        values.extend(split_pasda_place_keyword(value))
    return dedupe_list(values)


def split_pasda_place_keyword(value: str) -> list[str]:
    clean_value = clean_text(value)
    if not clean_value:
        return []

    parts = [clean_text(part) for part in clean_value.split(",") if clean_text(part)]
    if len(parts) >= 3:
        return parts
    if len(parts) == 2 and parts[1].upper().replace(".", "") in US_STATE_ABBREVIATIONS:
        return parts
    return [clean_value]


def pasda_spatial_coverage_value(
    record: dict[str, Any],
    county_lookup: dict[str, Any] | None = None,
) -> str:
    place_keywords = pasda_place_keyword_candidates(record)
    county_values = pasda_pa_county_spatial_values(place_keywords, county_lookup)
    values = []
    if county_values:
        values.extend(county_values)
        values.append("Pennsylvania")
    elif any(is_pasda_pennsylvania_keyword(value) for value in place_keywords):
        values.append("Pennsylvania")
    for value in place_keywords:
        clean_value = clean_text(value)
        if not clean_value:
            continue
        if is_pasda_pennsylvania_keyword(clean_value):
            continue
        matched_county = pasda_matches_pa_county(clean_value, county_lookup)
        if matched_county and matched_county in county_values:
            continue
        if clean_value not in values:
            values.append(clean_value)
        if len(values) >= 6:
            break
    return "|".join(dedupe_list(values))


def build_pasda_county_lookup(spatial_data: pd.DataFrame | None) -> dict[str, Any]:
    if spatial_data is None or spatial_data.empty or "County" not in spatial_data.columns:
        return {"pennsylvania_counties": {}, "state_names": set()}

    counties = spatial_data["County"].dropna().astype(str)
    state_names = set()
    pennsylvania_counties = {}
    for county_value in counties:
        state, separator, county_name = county_value.partition("--")
        if not separator:
            continue
        clean_state = clean_text(state)
        clean_county_name = clean_text(county_name)
        if clean_state:
            state_names.add(normalize_pasda_place_key(clean_state))
        if clean_state != "Pennsylvania" or not clean_county_name:
            continue
        for key in county_candidate_keys(clean_county_name):
            pennsylvania_counties[key] = county_value
    return {
        "pennsylvania_counties": pennsylvania_counties,
        "state_names": state_names,
    }


def pasda_pa_county_spatial_values(
    place_keywords: list[str],
    county_lookup: dict[str, Any] | None,
) -> list[str]:
    if not pasda_allow_pennsylvania_county_matches(place_keywords, county_lookup):
        return []
    values = []
    for value in place_keywords:
        county_value = pasda_matches_pa_county(value, county_lookup)
        if county_value:
            values.append(county_value)
    return dedupe_list(values)


def pasda_allow_pennsylvania_county_matches(
    place_keywords: list[str],
    county_lookup: dict[str, Any] | None,
) -> bool:
    if not county_lookup:
        return False
    if any(is_pasda_pennsylvania_keyword(value) for value in place_keywords):
        return True
    return not any(is_pasda_non_pennsylvania_state_context(value, county_lookup) for value in place_keywords)


def pasda_matches_pa_county(
    value: str,
    county_lookup: dict[str, Any] | None,
) -> str:
    if not county_lookup:
        return ""
    pennsylvania_counties = county_lookup.get("pennsylvania_counties", {})
    if not pennsylvania_counties:
        return ""

    for key in county_candidate_keys(value):
        county_value = pennsylvania_counties.get(key)
        if county_value:
            return county_value
    return ""


def county_candidate_keys(value: str) -> list[str]:
    clean_value = clean_text(value)
    county_match = re.search(r"\b([A-Za-z][A-Za-z .'-]+ County)\b", clean_value)
    values = [county_match.group(1)] if county_match else [clean_value]
    keys = []
    for candidate in values:
        key = normalize_pasda_place_key(candidate)
        if key:
            keys.append(key)
        if key.endswith(" county"):
            keys.append(key.removesuffix(" county").strip())
    return dedupe_list(keys)


def is_pasda_pennsylvania_keyword(value: str) -> bool:
    normalized = normalize_pasda_place_key(value)
    return normalized == "pennsylvania" or bool(re.search(r"\bpa\b", clean_text(value), re.I))


def is_pasda_non_pennsylvania_state_context(
    value: str,
    county_lookup: dict[str, Any],
) -> bool:
    normalized = normalize_pasda_place_key(value)
    state_names = county_lookup.get("state_names", set())
    for state_name in state_names:
        if state_name == "pennsylvania":
            continue
        if normalized == state_name:
            return True
        if normalized.startswith(f"{state_name} "):
            return True
        if f" {state_name} " in f" {normalized} " and "," in clean_text(value):
            return True
    return False


def normalize_pasda_place_key(value: str) -> str:
    clean_value = clean_text(value).lower()
    clean_value = clean_value.replace("&", " and ")
    clean_value = re.sub(r"[^a-z0-9]+", " ", clean_value)
    return re.sub(r"\s+", " ", clean_value).strip()


def pasda_temporal_coverage_value(
    start_date: str,
    end_date: str,
    publication_date: str = "",
    modified_date: str = "",
    metadata_date: str = "",
) -> str:
    if start_date and end_date:
        if start_date == end_date:
            return start_date
        return f"{start_date} to {end_date}"
    return start_date or end_date or publication_date or modified_date or metadata_date


def pasda_index_year(*date_values: str) -> str:
    for value in date_values:
        year = first_year(value)
        if year:
            return year
    return ""


def pasda_date_range(*date_values: str) -> str:
    years = [first_year(value) for value in date_values if first_year(value)]
    if not years:
        return ""
    return f"{min(years)}-{max(years)}"


def pasda_bounding_box(record: dict[str, Any]) -> tuple[str, str]:
    values = [
        record.get("west_bbox", ""),
        record.get("south_bbox", ""),
        record.get("east_bbox", ""),
        record.get("north_bbox", ""),
    ]
    if not all(clean_text(value) for value in values):
        return "", "missing_bbox"
    try:
        west, south, east, north = [float(value) for value in values]
    except (TypeError, ValueError):
        return "", "bad_bbox"

    if not (-180 <= west <= 180 and -180 <= east <= 180 and -90 <= south <= 90 and -90 <= north <= 90):
        return "", "bad_bbox"
    if east < west or north < south:
        return "", "bad_bbox"
    return f"{west},{south},{east},{north}", ""


def pasda_rights(record: dict[str, Any]) -> str:
    values = [
        record.get("license_or_use_constraints", ""),
        record.get("use_constraints", ""),
        record.get("access_constraints", ""),
    ]
    rights = []
    for value in values:
        clean_value = clean_text(value)
        if clean_value.lower() not in NONE_LIKE_VALUES:
            rights.append(clean_value)
    return "|".join(dedupe_list(rights))


def pasda_provenance(record: dict[str, Any], accession_date: str) -> str:
    metadata_url = clean_text(record.get("metadata_url", ""))
    lineage = clean_text(record.get("lineage", ""))
    provenance = f"Harvested from {metadata_url} on {accession_date}."
    if lineage:
        provenance = f"{provenance} Pasda lineage text: {lineage}"
    return provenance


def pasda_admin_note(record: dict[str, Any]) -> str:
    return f"PASDA metadata profile: {clean_text(record.get('metadata_profile', ''))}"


def pasda_format(record: dict[str, Any]) -> str:
    data_format = clean_text(record.get("data_format", ""))
    source_scale = clean_text(record.get("source_scale", ""))
    candidates = [data_format, source_scale]
    for candidate in candidates:
        normalized = normalize_pasda_format(candidate)
        if normalized:
            return normalized
    return ""


def normalize_pasda_format(value: str) -> str:
    clean_value = clean_text(value)
    normalized = clean_value.lower()
    if not normalized:
        return ""
    if "shapefile" in normalized or "shape file" in normalized or "shape-file" in normalized:
        return "Shapefile"
    if "geotiff" in normalized or "geo tiff" in normalized:
        return "GeoTIFF"
    if "geodatabase" in normalized or "gdb" in normalized:
        return "File Geodatabase"
    if "arc/info export" in normalized or "arcinfo export" in normalized:
        return "Arc/Info Export"
    if "raster" in normalized:
        return "Raster Dataset"
    if "vector" in normalized or "feature class" in normalized:
        return "Vector data"
    if "html" in normalized:
        return "HTML"
    return clean_value


def pasda_resource_class(record: dict[str, Any]) -> str:
    if pasda_is_imagery_record(record):
        return "Imagery"
    return "Datasets"


def pasda_is_imagery_record(record: dict[str, Any]) -> bool:
    keyword_values = [
        value
        for value in ensure_list(record.get("theme_keywords"))
        if normalize_pasda_match_text(value)
        not in {"imagery", "imagerybasemapsearthcover", "base maps", "base map"}
    ]
    text = " ".join(
        [
            clean_text(record.get("title", "")),
            clean_text(record.get("alternate_title", "")),
            " ".join(keyword_values),
        ]
    )
    normalized = normalize_pasda_match_text(text)

    imagery_patterns = [
        r"\bortho ?imagery\b",
        r"\bortho ?image(?:s)?\b",
        r"\bortho ?photo(?:s|graph|graphs|graphy)?\b",
        r"\bdigital ortho ?photo(?:s|graph|graphs|graphy)?\b",
        r"\baerial photo(?:s|graph|graphs|graphy)?\b",
        r"\baerial image(?:s|ry)?\b",
        r"\bnaip\b",
        r"\bdoq\b",
        r"\bdoqq\b",
        r"\bsatellite image(?:s|ry)?\b",
    ]
    return any(re.search(pattern, normalized) for pattern in imagery_patterns)


def pasda_resource_type(record: dict[str, Any]) -> str:
    values = [
        clean_text(record.get("source_scale", "")),
        clean_text(record.get("data_format", "")),
    ]
    joined = " ".join(values).lower()
    if "remote-sensing" in joined or "imagery" in joined:
        return "Remote-sensing maps"
    if "raster" in joined:
        return "Raster data"
    if "map" in joined:
        return "Digital maps"
    return ""


def normalize_pasda_match_text(value: str) -> str:
    clean_value = clean_text(value).lower()
    clean_value = clean_value.replace("&", " and ")
    clean_value = re.sub(r"[^a-z0-9]+", " ", clean_value)
    return re.sub(r"\s+", " ", clean_value).strip()


def normalize_pasda_date(value: Any) -> str:
    clean_value = clean_text(str(value or ""))
    if not clean_value:
        return ""
    if re.fullmatch(r"\d{8}", clean_value):
        return f"{clean_value[:4]}-{clean_value[4:6]}-{clean_value[6:8]}"
    if re.fullmatch(r"\d{6}", clean_value):
        return f"{clean_value[:4]}-{clean_value[4:6]}"
    if re.fullmatch(r"\d{4}", clean_value):
        return clean_value
    date_match = re.search(r"((?:19|20)\d{2})(?:[-/](\d{1,2}))?(?:[-/](\d{1,2}))?", clean_value)
    if not date_match:
        return ""
    year, month, day = date_match.groups()
    if month and day:
        return f"{year}-{int(month):02d}-{int(day):02d}"
    if month:
        return f"{year}-{int(month):02d}"
    return year


def first_year(value: str) -> str:
    match = re.search(r"(?:19|20)\d{2}", clean_text(value))
    return match.group(0) if match else ""


def ensure_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [clean_text(item) for item in value if clean_text(item)]
    if isinstance(value, tuple | set):
        return [clean_text(item) for item in value if clean_text(item)]
    clean_value = clean_text(value)
    if not clean_value:
        return []
    if clean_value.startswith("[") and clean_value.endswith("]"):
        try:
            parsed = json.loads(clean_value.replace("'", '"'))
            if isinstance(parsed, list):
                return [clean_text(item) for item in parsed if clean_text(item)]
        except json.JSONDecodeError:
            pass
    return [part for part in (clean_text(part) for part in clean_value.split("|")) if part]
