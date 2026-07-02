import base64
from io import BytesIO
import json
import logging
import os
import re
import tarfile
import time

import pandas as pd
import requests

from harvesters.base import BaseHarvester
from scripts.build_uploads import build_distribution_delta_files
from utils.distribution_writer import generate_secondary_table
from utils.field_order import PRIMARY_FIELD_ORDER
from utils.resource_type_match import match_resource_type
from utils.spatial_match import load_county_spatial_lookup, match_county_spatial


GITHUB_API_ROOT = "https://api.github.com"
GITHUB_JSON_HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
COMMIT_DELETION_COLUMNS = [
    "file_path",
    "previous_file_path",
    "inferred_id",
    "status",
    "commit_sha",
    "commit_date",
]
DISTRIBUTION_DELTA_COLUMNS = [
    "friendlier_id",
    "reference_type",
    "distribution_url",
    "label",
]
EMPTY_PRIMARY_COLUMNS = [
    "ID",
    "Title",
    "Access Rights",
    "Resource Class",
    "Bounding Box",
    "Date Range",
]


class OgmWiscHarvester(BaseHarvester):
    def __init__(self, config):
        """Initialize the harvester and set Wisconsin-specific config values."""
        super().__init__(config)
        self.source_mode = self.config.get("source_mode", "local_json")
        self.json_path = self.config.get("json_path")
        self.county_spatial_lookup = {}
        self.county_spatial_alias_lookup = {}
        self.deleted_github_files = []
        self.previous_github_records = []

    def load_reference_data(self):
        """Load shared distribution metadata and county lookup data used by this harvester."""
        super().load_reference_data()
        counties_path = "reference_data/spatial_counties.csv"
        self.county_spatial_lookup, self.county_spatial_alias_lookup = (
            load_county_spatial_lookup(counties_path, "Wisconsin")
        )

    def fetch(self):
        """Fetch UW-Madison OGM JSON records from the configured source mode."""
        if self.source_mode == "local_json":
            return fetch_local_json_tree(self.json_path)

        if self.source_mode == "github_tarball":
            session = build_github_session(self.config)
            return fetch_github_tarball_json(self.config, session)

        if self.source_mode == "github_commits":
            session = build_github_session(self.config)
            records, deleted_files, previous_records = fetch_github_commit_json(
                self.config,
                session,
                include_previous=True,
            )
            self.deleted_github_files = deleted_files
            self.previous_github_records = previous_records
            return records

        raise ValueError(f"Unsupported ogmWisc source_mode: {self.source_mode}")

    def flatten(self, harvested_metadata):
        """
        Expands each record by parsing dct_references_s and adding one column per
        variable defined in distribution_types.yaml.
        """
        uri_to_vars = build_reference_uri_variable_lookup(self.distribution_types)
        return flatten_ogm_wisc_records(harvested_metadata, uri_to_vars)

    def build_dataframe(self, records):
        """
        Converts UW-Madison GBL 1.0 records into a DataFrame with renamed and
        normalized fields according to the GeoBTAA schema.
        """
        if not records:
            return pd.DataFrame(columns=EMPTY_PRIMARY_COLUMNS)

        df = pd.DataFrame(records)

        multivalue_fields = [
            "dc_creator_sm",
            "dc_subject_sm",
            "dct_spatial_sm",
            "dct_isPartOf_sm",
            "dct_temporal_sm",
        ]
        for col in multivalue_fields:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: "|".join(x) if isinstance(x, list) else x)

        rename_map = {
            "dc_title_s": "Title",
            "dc_description_s": "Description",
            "dc_creator_sm": "Creator",
            "dct_issued_s": "Date Issued",
            "dc_rights_s": "Access Rights",
            "dc_format_s": "Format",
            "layer_slug_s": "ID",
            "layer_id_s": "WxS Identifier",
            "dct_provenance_s": "Publisher",
            "dc_publisher_sm": "Publisher",
            "dct_temporal_sm": "Temporal Coverage",
            "dct_isPartOf_sm": "Local Collection",
            "dc_subject_sm": "Subject",
            "uw_deprioritize_item_b": "Child Record",
            "thumbnail_path_ss": "B1G Image",
        }

        return df.rename(columns=rename_map)

    def derive_fields(self, df):
        """Apply the Wisconsin-specific field derivation pipeline to the dataframe."""
        if df.empty:
            return df

        df = super().add_defaults(df)
        df = (
            df.pipe(ogm_wisc_format_temporal_coverage)
            .pipe(ogm_wisc_flag_georeferenced)
            .pipe(ogm_wisc_generate_identifier)
            .pipe(ogm_wisc_reorder_bbox)
            .pipe(ogm_wisc_map_theme_from_subject)
            .pipe(ogm_wisc_build_display_note)
            .pipe(ogm_wisc_add_resource_class)
            .pipe(ogm_wisc_add_resource_type)
            .pipe(
                ogm_wisc_clean_creator_values,
                self.county_spatial_lookup,
                self.county_spatial_alias_lookup,
            )
        )
        return df

    def add_defaults(self, df):
        """Populate fixed default values required for Wisconsin output records."""
        df["Code"] = "10d_00"
        df["Is Part Of"] = "10d-03"
        df["Member Of"] = "dc8c18df-7d64-4ff4-a754-d18d0891187d"
        df["Language"] = "eng"
        df["Spatial Coverage"] = "Wisconsin"
        df["Provider"] = "University of Wisconsin-Madison"
        return df

    def add_provenance(self, df):
        """Add provenance metadata describing how these records were harvested."""
        df = super().add_provenance(df)
        df["Accrual Method"] = "Mediated deposit"
        df["Website Platform"] = "GeoBlacklight"
        df["Harvest Workflow"] = "py_ogm_wisc"
        df["Endpoint Description"] = "GitHub"
        df["Endpoint URL"] = self.config.get(
            "endpoint_url",
            "https://github.com/opengeometadata/edu.wisc",
        )
        if self.source_mode == "github_commits":
            df["Harvest Workflow"] = "py_ogm_wisc_commit_delta"
        return df

    def clean(self, df):
        """Run the standard cleanup step for the harvested dataframe."""
        df = super().clean(df)
        return df

    def validate(self, df):
        """Run the standard validation step for the harvested dataframe."""
        df = super().validate(df)
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        """Build the secondary distribution table and write the final outputs."""
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)

        if self.source_mode == "github_commits":
            return write_commit_delta_outputs(primary_df, distributions_df, self)

        return super().write_outputs(primary_df, distributions_df)

    def build_uploads(self, results: dict) -> dict | None:
        """Skip full-harvest upload comparison for commit-delta harvests."""
        if self.source_mode == "github_commits":
            return None
        return super().build_uploads(results)


# Custom functions for this harvester

def build_github_session(config):
    session = requests.Session()
    session.headers.update(GITHUB_JSON_HEADERS)

    token_env = config.get("github_token_env", "GITHUB_TOKEN")
    token = os.environ.get(token_env, "")
    if token:
        session.headers.update({"Authorization": f"Bearer {token}"})

    return session


def fetch_local_json_tree(json_path):
    dataset = []
    for root, _, files in os.walk(json_path):
        for filename in sorted(files):
            if not filename.lower().endswith(".json"):
                continue

            file_path = os.path.join(root, filename)
            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as handle:
                    dataset.append(json.load(handle))
            except json.JSONDecodeError as exc:
                logging.warning(
                    "[OGMWisc] Failed to parse JSON at %s: %s",
                    file_path,
                    exc,
                )

    return dataset


def fetch_github_tarball_json(config, session):
    owner, repo = get_github_repo_config(config)
    ref = config.get("github_ref", config.get("github_branch", "main"))
    timeout = int(config.get("github_timeout", 60))
    url = f"{GITHUB_API_ROOT}/repos/{owner}/{repo}/tarball/{ref}"

    response = session.get(url, timeout=timeout)
    response.raise_for_status()

    dataset = []
    with tarfile.open(fileobj=BytesIO(response.content), mode="r:gz") as archive:
        for member in archive.getmembers():
            if not member.isfile() or not member.name.lower().endswith(".json"):
                continue

            extracted = archive.extractfile(member)
            if extracted is None:
                continue

            try:
                dataset.append(json.loads(extracted.read().decode("utf-8", errors="ignore")))
            except json.JSONDecodeError as exc:
                logging.warning(
                    "[OGMWisc] Failed to parse JSON in tarball at %s: %s",
                    member.name,
                    exc,
                )

    return dataset


def fetch_github_commit_json(config, session, include_previous=False):
    owner, repo = get_github_repo_config(config)
    records = []
    deleted_files = []
    previous_records = []
    seen_paths = set()

    for commit in iter_selected_github_commits(config, session, owner, repo):
        detail = get_github_commit_detail(config, session, owner, repo, commit["sha"])
        parent_ref = get_first_parent_sha(detail)
        commit_date = (
            detail.get("commit", {})
            .get("committer", {})
            .get("date", "")
        )

        for changed_file in detail.get("files", []):
            event = build_changed_file_event(changed_file, detail["sha"], commit_date)
            new_path = event["file_path"]
            previous_path = event["previous_file_path"]

            if not is_json_path(new_path) and not is_json_path(previous_path):
                continue

            if new_path in seen_paths or (previous_path and previous_path in seen_paths):
                continue

            status = event["status"]
            if status == "removed" or (status == "renamed" and not is_json_path(new_path)):
                deleted_files.append(event)
                seen_paths.add(new_path)
                if previous_path:
                    seen_paths.add(previous_path)
                continue

            if not is_json_path(new_path):
                continue

            record = fetch_github_json_file(
                config,
                session,
                owner,
                repo,
                new_path,
                detail["sha"],
            )
            if record is not None:
                records.append(record)

            previous_record = fetch_previous_github_json_file(
                config,
                session,
                owner,
                repo,
                previous_path or new_path,
                parent_ref,
                status,
            )
            if previous_record is not None:
                previous_records.append(previous_record)

            seen_paths.add(new_path)
            if previous_path:
                seen_paths.add(previous_path)

    if include_previous:
        return records, deleted_files, previous_records
    return records, deleted_files


def iter_selected_github_commits(config, session, owner, repo):
    recent_commit_count = config.get("github_recent_commits")
    since = config.get("github_since")
    until = config.get("github_until")

    if not recent_commit_count and not since:
        raise ValueError(
            "github_commits mode requires either github_recent_commits or github_since."
        )

    limit = int(recent_commit_count) if recent_commit_count else None
    per_page = min(max(int(config.get("github_per_page", limit or 100)), 1), 100)
    ref = config.get("github_ref", config.get("github_branch", "main"))
    timeout = int(config.get("github_timeout", 60))
    page = 1
    yielded = 0

    while True:
        params = {"sha": ref, "per_page": per_page, "page": page}
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        if config.get("github_path"):
            params["path"] = config["github_path"]

        commits = github_get_json(
            session,
            f"{GITHUB_API_ROOT}/repos/{owner}/{repo}/commits",
            timeout,
            params=params,
        )

        if not commits:
            break

        for commit in commits:
            yield commit
            yielded += 1
            if limit and yielded >= limit:
                return

        if len(commits) < per_page:
            break
        page += 1


def get_github_commit_detail(config, session, owner, repo, sha):
    timeout = int(config.get("github_timeout", 60))
    return github_get_json(
        session,
        f"{GITHUB_API_ROOT}/repos/{owner}/{repo}/commits/{sha}",
        timeout,
    )


def fetch_github_json_file(config, session, owner, repo, path, ref):
    timeout = int(config.get("github_timeout", 60))
    response_json = github_get_json(
        session,
        f"{GITHUB_API_ROOT}/repos/{owner}/{repo}/contents/{path}",
        timeout,
        params={"ref": ref},
    )

    encoded_content = response_json.get("content", "")
    if response_json.get("encoding") != "base64" or not encoded_content:
        logging.warning("[OGMWisc] GitHub content response for %s was not base64.", path)
        return None

    try:
        raw_content = base64.b64decode(encoded_content).decode("utf-8", errors="ignore")
        return json.loads(raw_content)
    except (ValueError, json.JSONDecodeError) as exc:
        logging.warning("[OGMWisc] Failed to decode GitHub JSON at %s: %s", path, exc)
        return None


def fetch_previous_github_json_file(config, session, owner, repo, path, ref, status):
    if status not in {"modified", "renamed"}:
        return None
    if not ref or not is_json_path(path):
        return None

    try:
        return fetch_github_json_file(config, session, owner, repo, path, ref)
    except requests.HTTPError as exc:
        logging.warning(
            "[OGMWisc] Failed to fetch previous GitHub JSON at %s ref %s: %s",
            path,
            ref,
            exc,
        )
        return None


def github_get_json(session, url, timeout, params=None):
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def get_github_repo_config(config):
    owner = config.get("github_owner")
    repo = config.get("github_repo")
    if owner and repo:
        return owner, repo

    raise ValueError("GitHub source modes require github_owner and github_repo.")


def build_changed_file_event(changed_file, commit_sha, commit_date):
    file_path = changed_file.get("filename", "")
    previous_file_path = changed_file.get("previous_filename", "")
    return {
        "file_path": file_path,
        "previous_file_path": previous_file_path,
        "inferred_id": infer_id_from_json_path(previous_file_path or file_path),
        "status": changed_file.get("status", ""),
        "commit_sha": commit_sha,
        "commit_date": commit_date,
    }


def get_first_parent_sha(commit_detail):
    parents = commit_detail.get("parents", [])
    if not parents:
        return ""
    return parents[0].get("sha", "")


def is_json_path(path):
    return isinstance(path, str) and path.lower().endswith(".json")


def infer_id_from_json_path(path):
    if not path:
        return ""
    return os.path.splitext(os.path.basename(path))[0]


def build_reference_uri_variable_lookup(distribution_types):
    uri_to_vars = {}
    for dist in distribution_types or []:
        uri = dist.get("reference_uri")
        variables = dist.get("variables", [])
        if uri:
            uri_to_vars[uri] = variables
    return uri_to_vars


def flatten_ogm_wisc_records(harvested_metadata, uri_to_vars):
    flattened = []

    for record in harvested_metadata:
        new_record = record.copy()
        raw_refs = record.get("dct_references_s")

        if isinstance(raw_refs, str):
            try:
                references = json.loads(raw_refs.replace('""', '"'))
                for ref_uri, url in references.items():
                    for variable in uri_to_vars.get(ref_uri, []):
                        new_record[variable] = url
            except json.JSONDecodeError as exc:
                logging.warning(
                    "[OGMWisc] Invalid JSON in dct_references_s for record %s: %s",
                    record.get("layer_slug_s"),
                    exc,
                )

        flattened.append(new_record)

    return flattened


def write_commit_delta_outputs(primary_df, distributions_df, harvester):
    today = time.strftime("%Y-%m-%d")
    output_dir = "outputs"
    upload_dir = os.path.join(output_dir, "to_upload")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(upload_dir, exist_ok=True)

    primary_filename = os.path.join(
        output_dir,
        f"{today}_{commit_delta_basename(harvester.config['output_primary_csv'], 'primary')}",
    )
    distributions_filename = os.path.join(
        output_dir,
        f"{today}_"
        f"{commit_delta_basename(harvester.config['output_distributions_csv'], 'distributions')}",
    )
    deletions_filename = os.path.join(output_dir, f"{today}_ogmWisc_commit_deletions.csv")
    distributions_new_filename = os.path.join(
        upload_dir,
        f"{today}_ogmWisc_distributions_new.csv",
    )
    distributions_delete_filename = os.path.join(
        upload_dir,
        f"{today}_ogmWisc_distributions_delete.csv",
    )

    primary_out = primary_df.reindex(
        columns=[col for col in PRIMARY_FIELD_ORDER if col in primary_df.columns]
    )
    primary_out.to_csv(primary_filename, index=False, encoding="utf-8")

    if distributions_df.empty:
        distributions_df = empty_distribution_delta_df()
    distributions_df.to_csv(distributions_filename, index=False, encoding="utf-8")

    dist_new_df, dist_delete_df, changed_distribution_ids = (
        build_commit_distribution_delta_tables(distributions_df, harvester)
    )
    dist_new_df.to_csv(distributions_new_filename, index=False, encoding="utf-8")
    dist_delete_df.to_csv(distributions_delete_filename, index=False, encoding="utf-8")

    deletions_df = pd.DataFrame(
        harvester.deleted_github_files,
        columns=COMMIT_DELETION_COLUMNS,
    )
    deletions_df.to_csv(deletions_filename, index=False, encoding="utf-8")

    return {
        "primary_csv": primary_filename,
        "distributions_csv": distributions_filename,
        "distributions_new_csv": distributions_new_filename,
        "distributions_delete_csv": distributions_delete_filename,
        "deleted_files_csv": deletions_filename,
        "processed_count": len(primary_df),
        "deleted_count": len(deletions_df),
        "distribution_new_count": len(dist_new_df),
        "distribution_delete_count": len(dist_delete_df),
        "changed_distribution_ids": sorted(changed_distribution_ids),
    }


def build_commit_distribution_delta_tables(current_distributions_df, harvester):
    current_distributions_df = ensure_distribution_delta_columns(current_distributions_df)
    previous_distributions_df = build_previous_commit_distributions(harvester)

    current_ids = set(current_distributions_df["friendlier_id"].astype(str).str.strip())
    previous_ids = set(previous_distributions_df["friendlier_id"].astype(str).str.strip())
    new_ids = current_ids - previous_ids
    shared_ids = current_ids.intersection(previous_ids)

    return build_distribution_delta_files(
        current_distributions_df,
        previous_distributions_df,
        new_ids=new_ids,
        shared_ids=shared_ids,
    )


def build_previous_commit_distributions(harvester):
    if not harvester.previous_github_records:
        return empty_distribution_delta_df()

    previous_flat = harvester.flatten(harvester.previous_github_records)
    previous_primary_df = harvester.build_dataframe(previous_flat)
    previous_distributions_df = generate_secondary_table(
        previous_primary_df.copy(),
        harvester.distribution_types,
    )
    return ensure_distribution_delta_columns(previous_distributions_df)


def ensure_distribution_delta_columns(df):
    if df is None or df.empty:
        return empty_distribution_delta_df()

    df = df.copy()
    for column in DISTRIBUTION_DELTA_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    return df[DISTRIBUTION_DELTA_COLUMNS]


def empty_distribution_delta_df():
    return pd.DataFrame(columns=DISTRIBUTION_DELTA_COLUMNS)


def commit_delta_basename(configured_path, output_kind):
    basename = os.path.basename(configured_path)
    expected_suffix = f"_{output_kind}.csv"
    replacement_suffix = f"_commit_delta_{output_kind}.csv"

    if basename.endswith(expected_suffix):
        return basename[: -len(expected_suffix)] + replacement_suffix

    return basename.replace(".csv", replacement_suffix)


def ogm_wisc_format_temporal_coverage(df):
    def format_temporal(temporal):
        if pd.notna(temporal) and re.match(r"\d{4}-\d{4}", str(temporal)):
            return temporal
        if pd.notna(temporal):
            return f"{temporal}-{temporal}"
        return ""

    if "Temporal Coverage" in df.columns:
        df["Date Range"] = df["Temporal Coverage"].apply(format_temporal)
    return df


def ogm_wisc_flag_georeferenced(df):
    if "Format" in df.columns:
        df["Georeferenced"] = df["Format"].apply(
            lambda x: "true" if pd.notna(x) and "GeoTIFF" in x else "false"
        )
    return df


def ogm_wisc_generate_identifier(df):
    if "ID" in df.columns:
        df["Identifier"] = "https://geodata.wisc.edu/catalog/" + df["ID"]
    return df


def ogm_wisc_reorder_bbox(df):
    def extract_bbox(value):
        if value.startswith("ENVELOPE(") and value.endswith(")"):
            try:
                coords = value[len("ENVELOPE(") : -1].split(",")
                west, east, north, south = [coord.strip() for coord in coords]
                return f"{west},{south},{east},{north}"
            except Exception:
                return None
        return None

    if "solr_geom" in df.columns:
        df["Bounding Box"] = df["solr_geom"].apply(
            lambda x: extract_bbox(x) if isinstance(x, str) else None
        )
    return df


def ogm_wisc_map_theme_from_subject(df):
    theme_map = {
        "Farming": "Agriculture",
        "Biota": "Biology",
        "Atmospheric Sciences": "Climate",
        "Geoscientific Information": "Geology",
        "Imagery and Base Maps": "Imagery",
        "Planning and Cadastral": "Property",
        "Utilities and Communication": "Utilities",
    }

    def map_theme_multivalued(subject):
        if not isinstance(subject, str) or subject.strip() == "":
            return subject
        parts = subject.split("|")
        mapped = [theme_map.get(part.strip(), part.strip()) for part in parts]
        return "|".join(mapped)

    if "Subject" in df.columns:
        df["Theme"] = df["Subject"].apply(map_theme_multivalued)
    return df


def ogm_wisc_build_display_note(df):
    def map_display_note(notice, supplemental):
        parts = []
        if isinstance(notice, str) and notice.strip():
            parts.append(notice.strip())
        if isinstance(supplemental, str) and supplemental.strip():
            parts.append(f"Info: {supplemental.strip()}")
        return "|".join(parts) if parts else ""

    if "uw_notice_s" in df.columns or "uw_supplemental_s" in df.columns:
        notice_values = (
            df["uw_notice_s"] if "uw_notice_s" in df.columns else [""] * len(df)
        )
        supplemental_values = (
            df["uw_supplemental_s"] if "uw_supplemental_s" in df.columns else [""] * len(df)
        )
        df["Display Note"] = [
            map_display_note(notice, supplemental)
            for notice, supplemental in zip(notice_values, supplemental_values)
        ]
    return df


def ogm_wisc_add_resource_class(df):
    if "dc_type_s" in df.columns:
        df["Resource Class"] = df["dc_type_s"].apply(
            lambda x: "Imagery" if x == "Image" else "Datasets"
        )
    return df


def ogm_wisc_add_resource_type(df):
    if "layer_geom_type_s" in df.columns:
        df["Resource Type"] = df["layer_geom_type_s"].astype(str).apply(
            lambda value: match_resource_type(f"{value} data")
        )
    return df


def ogm_wisc_clean_creator_values(df, county_spatial_lookup, county_spatial_alias_lookup):
    if "Creator" not in df.columns:
        return df

    def normalize_creator(value):
        if not isinstance(value, str) or not value.strip():
            return value

        text = value.strip().strip("|- ")

        if text.endswith(" County"):
            county_match = match_county_spatial(
                [f"Wisconsin--{text}", text],
                county_spatial_lookup,
                county_spatial_alias_lookup,
            )
            if county_match["full_name"]:
                return county_match["full_name"]

        if text.startswith("City of "):
            city_name = text.replace("City of ", "", 1).strip()
            return f"Wisconsin--{city_name}"

        return text

    df["Creator"] = df["Creator"].apply(normalize_creator)

    creator_matches = df["Creator"].apply(
        lambda value: match_county_spatial(
            [value],
            county_spatial_lookup,
            county_spatial_alias_lookup,
        )
    )

    df["Geometry"] = creator_matches.apply(lambda match: match["geometry"])
    df["GeoNames"] = creator_matches.apply(lambda match: match["geonames"])

    return df
