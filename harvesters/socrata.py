import csv
import time
import os
import re
import json
from pathlib import Path

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
    build_updated_harvest_record_rows,
    first_non_empty,
    load_metadata_lookup,
    match_metadata_defaults,
    required_config_path,
)
from utils.output_naming import infer_upload_source_prefix
from utils.resource_type_match import match_resource_type
from utils.temporal_fields import infer_temporal_coverage_from_title, create_date_range


class SocrataHarvester(BaseHarvester):
    def __init__(self, config):
        config = dict(config)
        config.setdefault("build_uploads", True)
        config.setdefault("hub_metadata_csv", "reference_data/websites.csv")
        config.setdefault("output_report_csv", "reports/socrata/socrata_report.csv")
        super().__init__(config)
        self.workflow_input_path = required_config_path(self.config, "input_csv", "Socrata")
        self.hub_metadata_path = required_config_path(self.config, "hub_metadata_csv", "Socrata")
        self.report_output_path = required_config_path(self.config, "output_report_csv", "Socrata")
        self._hub_metadata_lookup = None
        self._harvest_report_rows = []

    def load_reference_data(self):
        super().load_reference_data()
        self._hub_metadata_lookup = load_metadata_lookup(self.hub_metadata_path)

    def fetch(self):
        if not self.workflow_input_path or not os.path.exists(self.workflow_input_path):
            raise FileNotFoundError(
                f"[Socrata] Workflow input CSV not found: {self.workflow_input_path or '<unset>'}"
            )
        if self._hub_metadata_lookup is None:
            self._hub_metadata_lookup = load_metadata_lookup(self.hub_metadata_path)

        with open(self.workflow_input_path, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for harvest_record in reader:
                hub_defaults = match_metadata_defaults(harvest_record, self._hub_metadata_lookup)
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
                        build_socrata_harvest_report_run_row(
                            harvest_record,
                            "error",
                            f"[Socrata] Error fetching {website_id}: {e}",
                            0,
                        )
                    )
                    yield f"[Socrata] Error fetching {website_id}: {e}"
                    continue

                hub_title = first_non_empty(
                    hub_defaults.get("Title", ""),
                    harvest_record.get("Title", ""),
                )
                datasets = json_api.get("dataset", [])
                total_found = len(datasets) if isinstance(datasets, list) else 0
                message = f"[Socrata] Fetched {website_id} — {hub_title or 'No Title'}"
                self._harvest_report_rows.append(
                    build_socrata_harvest_report_run_row(
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

        df = pd.DataFrame(flattened_items)

        df = (
            df.pipe(socrata_filter_rows)
            .pipe(socrata_map_to_schema)
        )

        return df


    def derive_fields(self, df):
        df = super().derive_fields(df)
        
        df = (
            df.pipe(socrata_parse_identifiers)
            .pipe(socrata_temporal_coverage)
            .pipe(socrata_format_date_ranges)
            .pipe(socrata_reformat_titles)
            .pipe(socrata_clean_creator_values)
            .pipe(socrata_set_resource_type)
            .pipe(socrata_derive_geojson)
        )

        return df

    def add_defaults(self, df):
        df = super().add_defaults(df)

        df = df.pipe(socrata_apply_website_defaults)
        df['Display Note'] = "Tip: Check “Visit Source” link for download options."
        df['Language'] = 'eng'
        df['Resource Class'] = 'Web services'

        return df
    
    def add_provenance(self, df: pd.DataFrame) -> pd.DataFrame:
        # ---------- inherited defaults ----------
        df = super().add_provenance(df)

        today = time.strftime("%Y-%m-%d")

        # ---------- provenance fields for harvested dataset rows ----------
        df["Provenance"] = df.apply(
            lambda row: (
                f"The metadata for this resource was last retrieved from "
                f"{first_non_empty(row.get('Publisher', ''), 'Open Data Portal')} on {today}."
            ),
            axis=1,
        )

        harvest_record_df = build_updated_harvest_record_rows(self.workflow_input_path, today)
        if not harvest_record_df.empty:
            df = pd.concat([df, harvest_record_df], ignore_index=True)

            print(f"[Socrata] Updated Last Harvested for {len(harvest_record_df)} harvest records "
                f"appended them to the harvested metadata dataframe.")
        else:
            print("[Socrata] Workflow input CSV not found or produced no harvest records.")

        return df

    def clean(self, df):
        df = super().clean(df)
        return df

    def validate(self, df):
        df = super().validate(df)
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        if distributions_df is None:
            distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        distributions_df = filter_valid_socrata_geojson_distributions(distributions_df)
        results = super().write_outputs(primary_df, distributions_df)
        report_path = write_socrata_harvest_report(
            self._harvest_report_rows,
            results["primary_csv"],
            self.config["output_primary_csv"],
            self.report_output_path,
        )
        results["report_csv"] = report_path
        return results

    def build_uploads(self, results: dict) -> dict | None:
        upload_summary = super().build_uploads(results)
        if upload_summary is None or upload_summary.get("status") != "created":
            return upload_summary

        primary_upload_csv = upload_summary.get("primary_upload_csv")
        if primary_upload_csv:
            upload_summary["primary_upload_harvest_record_count"] = (
                keep_socrata_harvest_records_in_primary_upload(primary_upload_csv)
            )
        return upload_summary

# Custom functions for this harvester


def socrata_filter_rows(df):
    def is_valid(row):
        resource = row['resource']
        title = (resource.get('title') or '').strip()
        if not title or title.startswith('{{'):
            return False

        keywords = [k.lower().strip() for k in resource.get('keyword', []) if isinstance(k, str)]
        themes = [t.lower().strip() for t in resource.get('theme', [])] if isinstance(resource.get('theme'), list) else []

        return 'gis' in keywords or 'gis/maps' in themes

    return df[df.apply(is_valid, axis=1)].reset_index(drop=True)


def build_socrata_harvest_report_run_row(
    harvest_record: dict,
    status: str,
    message: str,
    total_found: int,
) -> dict:
    return {
        "Code": first_non_empty(harvest_record.get("Code", ""), harvest_record.get("ID", "")),
        "Title": harvest_record.get("Title", ""),
        "Identifier": harvest_record.get("Identifier", ""),
        "Harvest Run": status,
        "Harvest Message": message,
        "Total Records Found": total_found,
    }


def keep_socrata_harvest_records_in_primary_upload(primary_upload_csv: str) -> int:
    upload_path = Path(primary_upload_csv)
    if not upload_path.exists():
        return 0

    upload_df = pd.read_csv(upload_path, dtype=str, keep_default_na=False).fillna("")
    if "ID" not in upload_df.columns:
        upload_df.to_csv(upload_path, index=False, encoding="utf-8")
        return 0

    harvest_record_df = upload_df[
        upload_df["ID"].astype(str).str.strip().str.startswith("harvest_")
    ].copy()
    harvest_record_df.to_csv(upload_path, index=False, encoding="utf-8")
    print(
        f"[Socrata] Filtered primary upload to {len(harvest_record_df)} harvest record rows: "
        f"{upload_path}"
    )
    return len(harvest_record_df)


def write_socrata_harvest_report(
    report_rows: list[dict],
    current_primary_csv: str,
    configured_primary_csv: str,
    configured_report_csv: str,
) -> str:
    today = time.strftime("%Y-%m-%d")
    configured_report_path = Path(configured_report_csv)
    report_dir = (
        configured_report_path.parent
        if configured_report_path.parent != Path(".")
        else Path("reports/socrata")
    )
    report_path = report_dir / f"{today}_{configured_report_path.name}"
    report_df = build_socrata_harvest_report_dataframe(
        report_rows,
        Path(current_primary_csv),
        configured_primary_csv,
    )
    os.makedirs(report_path.parent, exist_ok=True)
    report_df.to_csv(report_path, index=False, encoding="utf-8")
    print(f"[Socrata] Wrote harvest report: {report_path}")
    return str(report_path)


def build_socrata_harvest_report_dataframe(
    report_rows: list[dict],
    current_primary_path: Path,
    configured_primary_csv: str,
) -> pd.DataFrame:
    current_df = load_socrata_report_primary(current_primary_path)
    previous_df = load_previous_socrata_primary_for_report(current_primary_path, configured_primary_csv)
    counts_by_code = build_socrata_report_counts_by_code(current_df, previous_df)

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
        "Harvest Run": build_socrata_harvest_run_tally(report_df),
        "Harvest Message": "",
        "Total Records Found": int(report_df["Total Records Found"].sum()) if not report_df.empty else 0,
        "New Records": int(report_df["New Records"].sum()) if not report_df.empty else 0,
        "Unpublished Records": int(report_df["Unpublished Records"].sum()) if not report_df.empty else 0,
    }
    return pd.concat([report_df, pd.DataFrame([total_row])], ignore_index=True)


def load_socrata_report_primary(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")


def load_previous_socrata_primary_for_report(
    current_primary_path: Path,
    configured_primary_csv: str,
) -> pd.DataFrame:
    outputs_dir = current_primary_path.resolve().parent
    source = infer_upload_source_prefix(configured_primary_csv)
    candidates = discover_dated_files(outputs_dir, build_filename_regex(source, "primary"))
    previous = most_recent_file_before(candidates, current_primary_path)
    if previous is None:
        return pd.DataFrame()
    return load_socrata_report_primary(previous[1])


def build_socrata_report_counts_by_code(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> dict[str, dict[str, int]]:
    current_records = socrata_report_dataset_rows(current_df)
    previous_records = socrata_report_dataset_rows(previous_df)
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


def socrata_report_dataset_rows(df: pd.DataFrame) -> pd.DataFrame:
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


def build_socrata_harvest_run_tally(report_df: pd.DataFrame) -> str:
    if report_df.empty or "Harvest Run" not in report_df.columns:
        return "success: 0; error: 0"
    counts = report_df["Harvest Run"].value_counts()
    return f"success: {int(counts.get('success', 0))}; error: {int(counts.get('error', 0))}"


def socrata_map_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    workflow_records = df["workflow"] if "workflow" in df.columns else df["website"]
    hub_defaults = df["hub_defaults"] if "hub_defaults" in df.columns else df["website"]
    resources = df["resource"]

    mapped_parts = [
        socrata_map_workflow_fields(workflow_records),
        socrata_map_resource_fields(resources),
    ]
    mapped_df = pd.concat(mapped_parts, axis=1)
    mapped_df["_hub_defaults"] = hub_defaults
    return mapped_df


def socrata_map_workflow_fields(workflow_records: pd.Series) -> pd.DataFrame:
    workflow_df = pd.DataFrame(workflow_records.tolist(), index=workflow_records.index).fillna("")
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
            output_column: workflow_df.get(source_column, "")
            for output_column, source_column in fields.items()
        },
        index=workflow_records.index,
    )


def socrata_apply_website_defaults(df: pd.DataFrame) -> pd.DataFrame:
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
        spatial = website.get('Spatial Coverage', '')
        if isinstance(spatial, list):
            for val in spatial:
                if isinstance(val, str) and val.strip():
                    return val.strip()
            return ''
        # Spatial Coverage is stored as a pipe-delimited string; grab the first entry.
        return str(spatial).split('|')[0].strip()

    df['Is Part Of'] = first_non_empty_columns('ID', 'Code')
    df['Code'] = first_non_empty_columns('Code', 'ID')
    df['Publisher'] = website_df.get('Title', '')
    df['Provider'] = website_df.get('Provider', '')
    df['Spatial Coverage'] = website_df.get('Spatial Coverage', '')
    df['Bounding Box'] = website_df.get('Bounding Box', '')
    df['Member Of'] = website_df.get('Member Of', '')
    df['titlePlace'] = hub_defaults.apply(get_first_spatial)
    return df


def socrata_map_resource_fields(resources: pd.Series) -> pd.DataFrame:
    # Helper function to extract creator name, kept from original logic
    def get_creator(resource):
        pub = resource.get('publisher')
        if isinstance(pub, dict):
            return pub.get('name') or next(iter(pub.values()), '')
        return pub or ''

    return pd.DataFrame(
        {
            'Alternative Title': resources.apply(lambda d: (d.get('title') or '').strip()),
            'Description':       resources.apply(lambda d: d.get('description', '')),
            'Creator':           resources.apply(get_creator),
            'Keyword':           resources.apply(lambda d: '|'.join(k.strip() for k in d.get('keyword', []) if isinstance(k, str)).replace(' ', '')),
            'Subject':           resources.apply(lambda d: '|'.join(d.get('theme', [])) if isinstance(d.get('theme'), list) else d.get('theme')),
            'Date Issued':       resources.apply(lambda d: (d.get('issued', '') or '').split('T')[0]),
            'Date Modified':     resources.apply(lambda d: (d.get('modified', '') or '').split('T')[0]),
            'Rights':            resources.apply(lambda d: d.get('license', '')),
            'Identifier':        resources.apply(lambda d: d.get('identifier', '')),
            'information':       resources.apply(lambda d: d.get('landingPage', '')),
        },
        index=resources.index,
    )


def socrata_parse_identifiers(df):
    """
    Derive ID from Identifier; handles common Socrata URL forms.
    Example: https://data.city.gov/views/abcd-1234 -> ID=abcd-1234
    """
    def _to_id(identifier):
        s = str(identifier or "")
        # common Socrata patterns
        for cut in ("/views/", "/d/"):
            if cut in s:
                return s.split(cut, 1)[-1].split("/", 1)[0]
        return s.rsplit("/", 1)[-1] if "/" in s else s

    df["ID"] = df["Identifier"].apply(_to_id)
    return df


def socrata_temporal_coverage(df):
    """
    Adds a 'Temporal Coverage' column based on Title or Date Modified.
    """
    df["Temporal Coverage"] = df.apply(infer_temporal_coverage_from_title, axis=1)
    return df


def socrata_format_date_ranges(df):
    """
    Adds a 'Date Range' column based on 'Temporal Coverage', 'Date Modified', or 'Date Issued'.
    """
    df["Date Range"] = df.apply(
        lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
        axis=1
    )
    return df


def socrata_reformat_titles(df):
    """
    Updates the Title field by concatenating 'Alternative Title' and 'titlePlace',
    with the titlePlace in square brackets.
    """
    def title_place(row):
        place = first_non_empty(row.get('titlePlace', ''))
        if place:
            return place

        defaults = row.get('_hub_defaults', {})
        if not isinstance(defaults, dict):
            return ''

        spatial = defaults.get('Spatial Coverage', '')
        if isinstance(spatial, list):
            return first_non_empty(*spatial)
        return str(spatial).split('|')[0].strip()

    df['Title'] = df.apply(
        lambda row: (
            f"{row.get('Alternative Title', '')} [{title_place(row)}]"
            if first_non_empty(row.get('Alternative Title', '')) and title_place(row)
            else first_non_empty(row.get('Alternative Title', ''))
            or (f"[{title_place(row)}]" if title_place(row) else "")
        ),
        axis=1
    )
    return df


def socrata_clean_creator_values(df):
    def _clean(value):
        if isinstance(value, dict) and 'name' in value:
            return value['name']
        if isinstance(value, str):
            m = re.match(r"\{\s*'name'\s*:\s*'(.+?)'\s*\}", value)
            return m.group(1) if m else value
        return value
    df['Creator'] = df['Creator'].apply(_clean)
    return df


def socrata_set_resource_type(df):
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
                return match_resource_type(resource_type)
        return match_resource_type(row.get('Resource Type', ''))  # Keep existing value if no match

    df['Resource Type'] = df.apply(match_keywords, axis=1)
    return df


def socrata_derive_geojson(df):
    """
    Add a 'geojson' distribution link for certain hubs
    Uses the hub Identifier (portal base URL) and the dataset's ID.
    """
    allowed_hubs = {'01c-01', '12b-17031-2'}

    def default_value(row, column_name):
        defaults = row.get('_hub_defaults', {})
        if isinstance(defaults, dict):
            return defaults.get(column_name, '')
        return ''

    def endpoint_base(row):
        return str(row.get('Endpoint URL', '')).rsplit('/data.json', maxsplit=1)[0].rstrip('/')

    def build_geojson(row):
        hub_id = first_non_empty(
            row.get('Hub ID', ''),
            row.get('Is Part Of', ''),
            default_value(row, 'ID'),
            default_value(row, 'Code'),
        )
        hub_identifier = first_non_empty(
            row.get('Hub Identifier', ''),
            default_value(row, 'Identifier'),
            endpoint_base(row),
        )
        if hub_id in allowed_hubs and hub_identifier and pd.notna(row.get('ID')):
            base = str(hub_identifier).rstrip('/')
            return f"{base}/resource/{row['ID']}.geojson"
        return ''

    df['geo_json'] = df.apply(build_geojson, axis=1)
    return df


def filter_valid_socrata_geojson_distributions(distributions_df: pd.DataFrame) -> pd.DataFrame:
    if distributions_df is None or distributions_df.empty:
        return distributions_df
    if "reference_type" not in distributions_df.columns or "distribution_url" not in distributions_df.columns:
        return distributions_df

    keep_rows = []
    dropped_count = 0
    for _, row in distributions_df.iterrows():
        if row.get("reference_type") != "geo_json":
            keep_rows.append(row)
            continue

        url = row.get("distribution_url")
        if isinstance(url, str) and url.strip() and socrata_check_geojson(url.strip()):
            keep_rows.append(row)
        else:
            dropped_count += 1

    if dropped_count:
        print(f"[Socrata] Dropped {dropped_count} invalid GeoJSON distribution rows.")

    return pd.DataFrame(keep_rows, columns=distributions_df.columns).reset_index(drop=True)


def socrata_check_geojson(url, max_bytes=10_000_000, timeout=15, retries=2):
    """
    Returns True if the URL points to a valid GeoJSON FeatureCollection with at
    least one non-null geometry. Otherwise returns False.
    """
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, stream=True, timeout=timeout)
            resp.raise_for_status()

            size = int(resp.headers.get("Content-Length", 0))
            if size and size > max_bytes:
                print(f"[Socrata] Skipping GeoJSON larger than {max_bytes} bytes: {url}")
                return False

            content = b""
            for chunk in resp.iter_content(1024 * 1024):
                content += chunk
                if len(content) > max_bytes:
                    print(f"[Socrata] Skipping GeoJSON larger than {max_bytes} bytes while downloading: {url}")
                    return False

            data = json.loads(content.decode("utf-8"))
            if data.get("type") != "FeatureCollection":
                print(f"[Socrata] GeoJSON is not a FeatureCollection: {url}")
                return False

            features = data.get("features", [])
            if not features:
                print(f"[Socrata] GeoJSON has no features: {url}")
                return False

            if not any(feature.get("geometry") for feature in features if isinstance(feature, dict)):
                print(f"[Socrata] GeoJSON has only null geometries: {url}")
                return False

            return True
        except Exception as exc:
            if attempt >= retries:
                print(f"[Socrata] Error checking GeoJSON {url}: {exc}")
                return False

    return False
