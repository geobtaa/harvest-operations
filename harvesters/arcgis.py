import csv
import time
import os
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import requests
import pandas as pd

from harvesters.base import BaseHarvester
from scripts.build_uploads import (
    build_filename_regex,
    discover_dated_files,
    most_recent_file_before,
)
from utils.distribution_writer import generate_secondary_table
from utils.harvester_helpers import (
    first_non_empty,
    read_csv_rows,
)
from utils.output_naming import infer_upload_source_prefix
from utils.resource_type_match import match_resource_type
from utils.temporal_fields import infer_temporal_coverage_from_title, create_date_range

class ArcGISHarvester(BaseHarvester):
    def __init__(self, config):
        # Initialize the ArcGIS harvester with the shared harvester configuration.
        config = dict(config)
        config.setdefault("build_uploads", True)
        super().__init__(config)
        self.workflow_input_path = required_config_path(self.config, "input_csv")
        self.hub_metadata_path = required_config_path(self.config, "hub_metadata_csv")
        self.report_output_path = required_config_path(self.config, "output_report_csv")
        self._hub_metadata_lookup = None
        self._harvest_report_rows = []

    def load_reference_data(self):
        # Load shared lookup data and reference tables required by downstream transforms.
        super().load_reference_data()
        self._hub_metadata_lookup = load_hub_metadata_lookup(self.hub_metadata_path)

    def fetch(self):
        # Request each configured ArcGIS Hub endpoint and yield the harvest record plus matched defaults.
        if not self.workflow_input_path or not os.path.exists(self.workflow_input_path):
            raise FileNotFoundError(
                f"[ArcGIS] Workflow input CSV not found: {self.workflow_input_path or '<unset>'}"
            )
        if self._hub_metadata_lookup is None:
            self._hub_metadata_lookup = load_hub_metadata_lookup(self.hub_metadata_path)

        with open(self.workflow_input_path, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            harvest_records = list(reader)

        validate_unique_arcgis_endpoint_codes(harvest_records)

        for harvest_record in harvest_records:
            hub_defaults = match_hub_defaults(harvest_record, self._hub_metadata_lookup)
            website_id = first_non_empty(
                hub_defaults.get("ID", ""),
                hub_defaults.get("Code", ""),
                harvest_record.get("Code", ""),
                harvest_record.get("Identifier", ""),
                harvest_record.get("ID", ""),
            )
            endpoint_url = harvest_record.get('Endpoint URL', '')
            try:
                resp = requests.get(endpoint_url, timeout=30)
                resp.raise_for_status()
                json_api = resp.json()
            except Exception as e:
                self._harvest_report_rows.append(
                    build_harvest_report_run_row(
                        harvest_record,
                        "error",
                        f"[ArcGIS] Error fetching {website_id}: {e}",
                        0,
                    )
                )
                yield f"[ArcGIS] Error fetching {website_id}: {e}"
                continue

            hub_title = first_non_empty(
                hub_defaults.get("Title", ""),
                harvest_record.get("Title", ""),
            )
            datasets = json_api.get("dataset", [])
            total_found = len(datasets) if isinstance(datasets, list) else 0
            message = f"[ArcGIS] Fetched {website_id} — {hub_title or 'No Title'}"
            self._harvest_report_rows.append(
                build_harvest_report_run_row(
                    harvest_record,
                    "success",
                    message,
                    total_found,
                )
            )
            yield message
            yield {
                "workflow": harvest_record,
                "hub_defaults": hub_defaults,
                "fetched_catalog": json_api,
            }

    
    def flatten(self, harvested_records):
        # Expand each harvested hub record into one row per dataset resource.

        flattened_list = []

        for source_record in harvested_records:
            if not isinstance(source_record, dict):
                continue

            harvest_record = source_record.get("workflow", source_record)
            hub_defaults = source_record.get("hub_defaults") or harvest_record

            # Extract the list of datasets from within the fetched catalog
            resources = source_record.get("fetched_catalog", {}).get("dataset", [])
            

            # Creates a new, combined record for each individual dataset
            for resource in resources:
                flattened_list.append({
                    "workflow": harvest_record,  # The source harvest-record row
                    "hub_defaults": hub_defaults,  # The matched website defaults row
                    "resource": resource      # The record for each dataset
                })

        return flattened_list
    
    def build_dataframe(self, flattened_items):
        # Convert flattened records into a dataframe and apply ArcGIS-specific schema mapping steps.

        df = pd.DataFrame(flattened_items)

        df = (
            df.pipe(arcgis_filter_rows)
            .pipe(arcgis_map_to_schema)
            .pipe(arcgis_extract_distributions)
        )

        return df

    def derive_fields(self, df):
        # Populate derived metadata fields after the base harvester has applied shared derivations.
        df = super().derive_fields(df)
        df = (
            df.pipe(arcgis_parse_identifiers)
            .pipe(arcgis_temporal_coverage)
            .pipe(arcgis_format_date_ranges)
            .pipe(arcgis_compute_bbox_column)
            .pipe(arcgis_clean_creator_values)
            .pipe(arcgis_reformat_titles)
            .pipe(arcgis_set_resource_type)
        )

        return df

    def add_defaults(self, df):
        # Set ArcGIS defaults and apply matched website reference values.
        df = super().add_defaults(df)
        df = df.pipe(arcgis_apply_website_defaults)
        df["Display Note"] = "Tip: Check “Visit Source” link for download options."
        df["Language"] = "eng"
        df["Resource Class"] = "Web services"
        return df
    
    def add_provenance(self, df: pd.DataFrame) -> pd.DataFrame:
        # Add run-level provenance details and append updated harvest records.
        df = super().add_provenance(df)
        today = time.strftime("%Y-%m-%d")
        df["Provenance"] = df["Publisher"].apply(
            lambda publisher: arcgis_resource_provenance(publisher, today)
        )

        harvest_record_df = build_harvest_record_rows(self.workflow_input_path, today)
        if not harvest_record_df.empty:
            df = pd.concat([df, harvest_record_df], ignore_index=True)

            print(f"[ArcGIS] Updated Last Harvested for {len(harvest_record_df)} harvest records "
                f"and appended them to the harvested metadata dataframe.")
        else:
            print("[ArcGIS] Workflow input CSV not found or produced no harvest records.")

        return df



    def clean(self, df):
        # Run shared cleanup logic without adding ArcGIS-specific post-processing yet.
        df = super().clean(df)
        return df

    def validate(self, df):
        # Run shared validation rules before writing ArcGIS output files.
        df = super().validate(df)
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        # Generate the secondary distributions table and delegate final file writing to the base harvester.
        primary_df = drop_arcgis_output_columns(primary_df)
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        results = super().write_outputs(primary_df, distributions_df)
        report_path = write_arcgis_harvest_report(
            self._harvest_report_rows,
            results["primary_csv"],
            self.config["output_primary_csv"],
            self.report_output_path,
        )
        results["report_csv"] = report_path
        return results

    def build_uploads(self, results: dict) -> dict | None:
        # Delegate upload delta generation to the shared base implementation.
        return super().build_uploads(results)

# Custom functions for this harvester


def required_config_path(config: dict, key: str) -> str:
    # Read a required path from the harvester config without embedding path defaults in code.
    value = str(config.get(key, "")).strip()
    if not value:
        raise ValueError(f"[ArcGIS] Missing required config value: {key}")
    return value


def drop_arcgis_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Remove fields that should never be written by the ArcGIS harvester outputs.
    return df.drop(columns=["Created At", "Updated At"], errors="ignore")


def normalize_lookup_key(value: str) -> str:
    # Normalize row identifiers so harvest records and hub defaults can be matched reliably.
    return str(value or "").strip().lower()


def lookup_keys_for_row(row: dict) -> list[str]:
    # Generate candidate lookup keys from the row's code, identifier, and id values.
    keys = []
    for raw_value in (
        row.get("Code", ""),
        row.get("Identifier", ""),
        row.get("ID", ""),
    ):
        normalized = normalize_lookup_key(raw_value)
        if not normalized:
            continue
        keys.append(normalized)
        if normalized.startswith("harvest_"):
            keys.append(normalized.removeprefix("harvest_"))

    seen = set()
    ordered_keys = []
    for key in keys:
        if key not in seen:
            seen.add(key)
            ordered_keys.append(key)
    return ordered_keys


def normalize_endpoint_url(value: str) -> str:
    # Normalize endpoint URLs enough to catch duplicate ArcGIS harvest targets.
    return str(value or "").strip().lower().rstrip("/")


def validate_unique_arcgis_endpoint_codes(harvest_records: list[dict]) -> None:
    # Stop before fetching when one ArcGIS endpoint is assigned to multiple harvest codes.
    endpoint_rows: dict[str, list[dict]] = {}
    for row in harvest_records:
        endpoint_url = normalize_endpoint_url(row.get("Endpoint URL", ""))
        if not endpoint_url or endpoint_url == "none":
            continue
        endpoint_rows.setdefault(endpoint_url, []).append(row)

    duplicate_messages = []
    for endpoint_url, rows in endpoint_rows.items():
        codes = {
            first_non_empty(row.get("Code", ""), row.get("Identifier", ""), row.get("ID", ""))
            for row in rows
        }
        codes.discard("")
        if len(codes) <= 1:
            continue
        labels = "; ".join(
            f"{first_non_empty(row.get('Code', ''), row.get('Identifier', ''), row.get('ID', ''))}"
            f" ({row.get('Title', '')})"
            for row in rows
        )
        duplicate_messages.append(f"{endpoint_url}: {labels}")

    if duplicate_messages:
        raise ValueError(
            "[ArcGIS] Duplicate Endpoint URL assigned to multiple harvest codes: "
            + " | ".join(duplicate_messages)
        )


def load_hub_metadata_lookup(hub_metadata_path: str) -> dict[str, dict]:
    # Build a lookup of ArcGIS hub metadata rows keyed by their known identifiers.
    lookup = {}
    for row in read_csv_rows(hub_metadata_path):
        for key in lookup_keys_for_row(row):
            lookup[key] = row
    return lookup


def match_hub_defaults(harvest_record: dict, hub_metadata_lookup: dict[str, dict]) -> dict:
    # Retrieve the metadata defaults row that corresponds to the current harvest record.
    for key in lookup_keys_for_row(harvest_record):
        matched_row = hub_metadata_lookup.get(key)
        if matched_row:
            return matched_row.copy()
    return {}


def build_harvest_report_run_row(
    harvest_record: dict,
    status: str,
    message: str,
    total_found: int,
) -> dict:
    # Capture one harvest-record run outcome for the ArcGIS report.
    return {
        "Code": first_non_empty(harvest_record.get("Code", ""), harvest_record.get("ID", "")),
        "Title": harvest_record.get("Title", ""),
        "Identifier": harvest_record.get("Identifier", ""),
        "Harvest Run": status,
        "Harvest Message": message,
        "Total Records Found": total_found,
    }


def build_harvest_record_rows(workflow_input_path: str, today: str) -> pd.DataFrame:
    # Append harvest-record input rows as-is, updating only Last Harvested for this run.
    harvest_record_df = pd.DataFrame(read_csv_rows(workflow_input_path))
    if harvest_record_df.empty:
        return harvest_record_df

    harvest_record_df["Last Harvested"] = today
    return harvest_record_df


def arcgis_resource_provenance(publisher: str, today: str) -> str:
    # Build the provenance sentence for harvested ArcGIS dataset resources.
    publisher = first_non_empty(publisher)
    source = f"{publisher} ArcGIS Hub" if publisher else "ArcGIS Hub"
    return f"The metadata for this resource was last retrieved from {source} on {today}."


def write_arcgis_harvest_report(
    report_rows: list[dict],
    current_primary_csv: str,
    configured_primary_csv: str,
    configured_report_csv: str,
) -> str:
    # Write a per-harvest-record report with current, new, and unpublished counts.
    today = time.strftime("%Y-%m-%d")
    report_path = Path("outputs") / f"{today}_{Path(configured_report_csv).name}"
    report_df = build_arcgis_harvest_report_dataframe(
        report_rows,
        Path(current_primary_csv),
        configured_primary_csv,
    )
    os.makedirs(report_path.parent, exist_ok=True)
    report_df.to_csv(report_path, index=False, encoding="utf-8")
    print(f"[ArcGIS] Wrote harvest report: {report_path}")
    return str(report_path)


def build_arcgis_harvest_report_dataframe(
    report_rows: list[dict],
    current_primary_path: Path,
    configured_primary_csv: str,
) -> pd.DataFrame:
    # Combine fetch run outcomes with per-harvest-record delta counts.
    current_df = load_report_primary(current_primary_path)
    previous_df = load_previous_primary_for_report(current_primary_path, configured_primary_csv)
    counts_by_code = build_report_counts_by_code(current_df, previous_df)

    rows = []
    for report_row in report_rows:
        code = str(report_row.get("Code", "")).strip()
        counts = counts_by_code.get(code, {})
        rows.append(
            {
                **report_row,
                "Total Records Found": int(report_row.get("Total Records Found", 0)),
                "New Records": int(counts.get("new", 0)),
                "Unpublished Records": int(counts.get("unpublished", 0)),
            }
        )

    report_df = pd.DataFrame(
        rows,
        columns=[
            "Code",
            "Title",
            "Identifier",
            "Harvest Run",
            "Harvest Message",
            "Total Records Found",
            "New Records",
            "Unpublished Records",
        ],
    )

    total_row = {
        "Code": "TOTAL",
        "Title": "",
        "Identifier": "",
        "Harvest Run": build_harvest_run_tally(report_df),
        "Harvest Message": "",
        "Total Records Found": int(report_df["Total Records Found"].sum()) if not report_df.empty else 0,
        "New Records": int(report_df["New Records"].sum()) if not report_df.empty else 0,
        "Unpublished Records": int(report_df["Unpublished Records"].sum()) if not report_df.empty else 0,
    }
    return pd.concat([report_df, pd.DataFrame([total_row])], ignore_index=True)


def load_report_primary(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")


def load_previous_primary_for_report(
    current_primary_path: Path,
    configured_primary_csv: str,
) -> pd.DataFrame:
    outputs_dir = current_primary_path.resolve().parent
    source = infer_upload_source_prefix(configured_primary_csv)
    candidates = discover_dated_files(outputs_dir, build_filename_regex(source, "primary"))
    previous = most_recent_file_before(candidates, current_primary_path)
    if previous is None:
        return pd.DataFrame()
    return load_report_primary(previous[1])


def build_report_counts_by_code(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> dict[str, dict[str, int]]:
    # Count current, new, and unpublished dataset rows grouped by their parent harvest code.
    current_records = report_dataset_rows(current_df)
    previous_records = report_dataset_rows(previous_df)
    counts: dict[str, dict[str, int]] = {}

    current_ids = set(current_records["ID"]) if "ID" in current_records.columns else set()
    previous_ids = set(previous_records["ID"]) if "ID" in previous_records.columns else set()

    new_records = current_records[current_records["ID"].isin(current_ids - previous_ids)]
    unpublished_records = previous_records[previous_records["ID"].isin(previous_ids - current_ids)]

    for code, count in new_records.groupby("Is Part Of").size().items():
        counts.setdefault(str(code), {})["new"] = int(count)
    for code, count in unpublished_records.groupby("Is Part Of").size().items():
        counts.setdefault(str(code), {})["unpublished"] = int(count)
    return counts


def report_dataset_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "ID" not in df.columns:
        return pd.DataFrame(columns=["ID", "Is Part Of"])
    work = df.copy()
    for column in ["ID", "Is Part Of", "Resource Class"]:
        if column not in work.columns:
            work[column] = ""
        work[column] = work[column].astype(str).str.strip()
    return work[
        work["ID"].ne("")
        & work["Is Part Of"].ne("")
        & work["Resource Class"].str.lower().ne("websites")
    ].copy()


def build_harvest_run_tally(report_df: pd.DataFrame) -> str:
    if report_df.empty or "Harvest Run" not in report_df.columns:
        return "success: 0; error: 0"
    counts = report_df["Harvest Run"].value_counts()
    return f"success: {int(counts.get('success', 0))}; error: {int(counts.get('error', 0))}"


def arcgis_filter_rows(df):
    # Keep dataset rows with Shapefile distributions or ImageServer GeoService URLs.
    allowed_titles = {"Shapefile"}
    access_patterns = ["ImageServer"]

    def is_valid(row):
        # Check whether a resource has a usable title and at least one supported distribution entry.
        resource = row["resource"]
        title = str(resource.get("title", "")).strip()
        if not title or title.startswith("{{"):
            return False

        dists = resource.get("distribution", []) or []
        if not isinstance(dists, list):
            return False

        has_valid_title = any((dist.get("title") in allowed_titles) for dist in dists)
        has_valid_url = any(
            str(dist.get("title", "")) == "ArcGIS GeoService"
            and any(pattern in str(dist.get("accessURL", "")) for pattern in access_patterns)
            for dist in dists
        )
        return has_valid_title or has_valid_url

    return df[df.apply(is_valid, axis=1)].reset_index(drop=True)


def arcgis_map_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    # Combine separately mapped harvest-record and resource fields, keeping website rows for add_defaults.
    harvest_records = df["workflow"] if "workflow" in df.columns else df["website"]
    hub_defaults = df["hub_defaults"] if "hub_defaults" in df.columns else df["website"]
    resources = df["resource"]

    mapped_parts = [
        arcgis_map_harvest_record_fields(harvest_records),
        arcgis_map_resource_fields(resources),
    ]
    mapped_df = pd.concat(mapped_parts, axis=1)
    mapped_df["_hub_defaults"] = hub_defaults
    return mapped_df


def arcgis_map_harvest_record_fields(harvest_records: pd.Series) -> pd.DataFrame:
    # Map values that come directly from harvest-record input rows.
    harvest_df = pd.DataFrame(harvest_records.tolist(), index=harvest_records.index).fillna("")
    fields = {
        "Endpoint URL": "Endpoint URL",
        "Website Platform": "Website Platform",
        "Endpoint Description": "Endpoint Description",
        "Accrual Method": "Accrual Method",
        "Accrual Periodicity": "Accrual Periodicity",
        "Harvest Workflow": "Harvest Workflow",
        "Provenance": "Provenance",
    }
    return pd.DataFrame(
        {
            output_column: harvest_df.get(source_column, "")
            for output_column, source_column in fields.items()
        },
        index=harvest_records.index,
    )


def arcgis_apply_website_defaults(df: pd.DataFrame) -> pd.DataFrame:
    # Apply matched website reference values from websites.csv to each dataset row.
    if "_hub_defaults" not in df.columns:
        return df

    hub_defaults = df["_hub_defaults"]
    website_df = pd.DataFrame(hub_defaults.tolist(), index=hub_defaults.index).fillna("")

    def first_non_empty_columns(*column_names: str) -> pd.Series:
        values = pd.Series("", index=website_df.index, dtype=object)
        for column_name in column_names:
            if column_name not in website_df.columns:
                continue
            column = website_df[column_name].astype(str).str.strip()
            values = values.mask(values.eq("") & column.ne(""), column)
        return values

    def get_first_spatial(website):
        spatial = website.get("Spatial Coverage", "")
        if isinstance(spatial, list):
            for val in spatial:
                if isinstance(val, str) and val.strip():
                    return val.strip()
            return ""
        return str(spatial).split("|")[0].strip()

    df["Is Part Of"] = first_non_empty_columns("ID", "Code")
    df["Code"] = first_non_empty_columns("Code", "ID")
    df["Publisher"] = website_df.get("Title", "")
    df["Provider"] = website_df.get("Provider", "")
    df["Spatial Coverage"] = website_df.get("Spatial Coverage", "")
    df["default_bbox"] = website_df.get("Bounding Box", "")
    df["Member Of"] = website_df.get("Member Of", "")
    df["titlePlace"] = hub_defaults.apply(get_first_spatial)
    return df


def arcgis_map_resource_fields(resources: pd.Series) -> pd.DataFrame:
    # Map fields from each ArcGIS DCAT dataset resource.
    def get_creator(resource):
        pub = resource.get("publisher")
        if isinstance(pub, dict):
            return pub.get("name") or next(iter(pub.values()), "")
        return pub or ""

    return pd.DataFrame(
        {
            "Alternative Title": resources.apply(lambda data: str(data.get("title", "")).strip()),
            "Description": resources.apply(lambda data: data.get("description", "")),
            "Creator": resources.apply(get_creator),
            "Keyword": resources.apply(
                lambda data: "|".join(
                    keyword.strip()
                    for keyword in data.get("keyword", [])
                    if isinstance(keyword, str)
                ).replace(" ", "")
            ),
            "Date Issued": resources.apply(lambda data: str(data.get("issued", "")).split("T")[0]),
            "Date Modified": resources.apply(
                lambda data: str(data.get("modified", "")).split("T")[0]
            ),
            "Rights": resources.apply(lambda data: data.get("license", "")),
            "identifier_raw": resources.apply(lambda data: data.get("identifier", "")),
            "information": resources.apply(lambda data: data.get("landingPage", "")),
            "spatial": resources.apply(lambda data: data.get("spatial", "")),
            "distributions": resources.apply(lambda data: data.get("distribution", []) or []),
        },
        index=resources.index,
    )


def arcgis_extract_distributions(df):
    # Split distribution links into dedicated service columns and assign a matching format label.
    def derive_dist_fields(dists):
        out = {
            "featureService": "",
            "mapService": "",
            "imageService": "",
            "tileService": "",
            "Format": "",
        }
        if not isinstance(dists, list):
            dists = []

        for dist in dists:
            title = str(dist.get("title", ""))
            access_url = str(dist.get("accessURL", ""))
            if title == "ArcGIS GeoService" and access_url:
                if "FeatureServer" in access_url:
                    out["featureService"] = access_url
                    out["Format"] = "ArcGIS FeatureLayer"
                elif "MapServer" in access_url:
                    out["mapService"] = access_url
                    out["Format"] = "ArcGIS DynamicMapLayer"
                elif "ImageServer" in access_url:
                    out["imageService"] = access_url
                    out["Format"] = "ArcGIS ImageMapLayer"
                elif "TileServer" in access_url:
                    out["tileService"] = access_url
                    out["Format"] = "ArcGIS TiledMapLayer"
        return pd.Series(out)

    dist_df = df["distributions"].apply(derive_dist_fields)
    return pd.concat([df, dist_df], axis=1)


def arcgis_compute_bbox_column(df):
    # Build a normalized bounding box from dataset spatial values, falling back to the hub default.
    def website_default_value(row, column_name: str) -> str:
        defaults = row.get("_hub_defaults", {})
        if isinstance(defaults, dict):
            return defaults.get(column_name, "")
        return ""

    def row_bbox(row):
        spatial = row.get("spatial", None)
        fallback = first_non_empty(
            row.get("default_bbox", ""),
            website_default_value(row, "Bounding Box"),
        )

        def use_fallback():
            return "" if pd.isna(fallback) else str(fallback).strip()

        if isinstance(spatial, str):
            parts = [part.strip() for part in spatial.split(",")]
            if len(parts) == 4:
                try:
                    xmin, ymin, xmax, ymax = [float(part) for part in parts]
                    if xmin > xmax:
                        xmin, xmax = xmax, xmin
                    if ymin > ymax:
                        ymin, ymax = ymax, ymin
                    if xmin == xmax or ymin == ymax:
                        return use_fallback()
                    return f"{xmin},{ymin},{xmax},{ymax}"
                except ValueError:
                    pass

        return use_fallback()

    df["Bounding Box"] = df.apply(row_bbox, axis=1)
    return df


def arcgis_harvest_identifier_and_id(identifier: str) -> tuple:
    # Derive a stable dataset URL and output ID from an ArcGIS identifier query string.
    parsed = urlparse(identifier)
    qs = parse_qs(parsed.query)

    if "id" in qs:
        resource_id = qs["id"][0]
        if "sublayer" in qs:
            resource_id = f"{resource_id}_{qs['sublayer'][0]}"

        cleaned = f"https://hub.arcgis.com/datasets/{resource_id}"
        return cleaned, resource_id

    return identifier, identifier


def arcgis_parse_identifiers(df):
    # Expand raw identifier values into normalized Identifier and ID columns.
    ids = df["identifier_raw"].apply(arcgis_harvest_identifier_and_id)
    df[["Identifier", "ID"]] = pd.DataFrame(ids.tolist(), index=df.index)
    return df


def arcgis_temporal_coverage(df):
    # Infer temporal coverage from resource titles using the shared temporal parser.
    df["Temporal Coverage"] = df.apply(infer_temporal_coverage_from_title, axis=1)
    return df


def arcgis_format_date_ranges(df):
    # Convert temporal coverage and date fields into a normalized date range string.
    df["Date Range"] = df.apply(
        lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
        axis=1,
    )
    return df


def arcgis_reformat_titles(df):
    # Compose the final display title by combining the dataset title with its place label.
    def title_place(row):
        place = row.get("titlePlace", "")
        if first_non_empty(place):
            return first_non_empty(place)

        defaults = row.get("_hub_defaults", {})
        if not isinstance(defaults, dict):
            return ""

        spatial = defaults.get("Spatial Coverage", "")
        if isinstance(spatial, list):
            return first_non_empty(*spatial)
        return str(spatial).split("|")[0].strip()

    df["Title"] = df.apply(
        lambda row: (
            f"{row.get('Alternative Title', '')} [{title_place(row)}]"
            if first_non_empty(row.get("Alternative Title", "")) and title_place(row)
            else first_non_empty(row.get("Alternative Title", ""))
            or (f"[{title_place(row)}]" if title_place(row) else "")
        ),
        axis=1,
    )
    return df


def arcgis_clean_creator_values(df):
    # Normalize creator values that may arrive as dicts or stringified dict payloads.
    def clean_creator(value):
        if isinstance(value, dict) and "name" in value:
            return value["name"]
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned.startswith("{{") and cleaned.endswith("}}"):
                return ""
            match = re.match(r"\\{\\s*'name'\\s*:\\s*'(.+?)'\\s*\\}", value)
            if match:
                return match.group(1)
            return cleaned
        return value

    df["Creator"] = df["Creator"].apply(clean_creator)
    return df


def arcgis_set_resource_type(df):
    # Override resource types when ArcGIS metadata contains specific keyword-driven matches.
    keyword_map = {
        "lidar": "LiDAR",
        "polygon": "Polygon data",
    }

    def match_keywords(row):
        combined_text = (
            f"{row.get('Alternative Title', '')} "
            f"{row.get('Description', '')} "
            f"{row.get('Keyword', '')}"
        ).lower()
        for keyword, resource_type in keyword_map.items():
            if keyword in combined_text:
                return match_resource_type(resource_type)
        return match_resource_type(row.get("Resource Type", ""))

    df["Resource Type"] = df.apply(match_keywords, axis=1)
    return df
