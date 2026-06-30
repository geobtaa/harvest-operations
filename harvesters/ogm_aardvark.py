import csv
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
from utils.dataframe_cleaner import (
    clean_date_ranges,
    clean_descriptions,
    deduplicate_rows_and_columns,
    strip_text_fields,
)
from utils.distribution_writer import generate_secondary_table, load_distribution_types
from utils.field_order import FIELD_ORDER, PRIMARY_FIELD_ORDER
from utils.spatial_cleaner import spatial_cleaning


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


class OgmAardvarkHarvester(BaseHarvester):
    PRIMARY_OUTPUT_EXCLUDED_FIELDS = {"dct_references_s", "gbl_mdVersion_s"}

    def __init__(self, config):
        super().__init__(config)
        self.repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.source_mode = self.config.get("source_mode", "local_json")
        self.json_path = self._resolve_path(self.config.get("json_path", ""))
        self.source_field_map = {}
        self.schema_field_names = set()
        self.field_separators = {}
        self.distribution_variables = set()
        self.reference_uri_to_variables = {}
        self.extra_output_columns = []
        self.deleted_github_files = []
        self.repo_defaults = {}
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
        self.repo_defaults = load_repo_defaults(
            self._resolve_path(self.config.get("repo_defaults_csv", "config/ogm-repos.csv"))
        )

    def fetch(self):
        if self.source_mode == "local_json":
            return fetch_local_json_tree(self.json_path)

        if self.source_mode == "github_tarball":
            session = build_github_session(self.config)
            return fetch_github_tarball_json(self.config, session)

        if self.source_mode == "github_commits":
            session = build_github_session(self.config)
            records, deleted_files = fetch_github_commit_json(self.config, session)
            self.deleted_github_files = deleted_files
            return records

        raise ValueError(f"Unsupported ogm_aardvark source_mode: {self.source_mode}")

    def flatten(self, harvested_metadata):
        if not self.reference_uri_to_variables:
            self.load_reference_data()

        flattened = []
        for record in harvested_metadata:
            new_record = record.copy()
            references = self._parse_references(
                record.get("dct_references_s"),
                record.get("id", ""),
            )
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

        defaults = repo_defaults_for_config(self.config, self.repo_defaults)
        df = apply_repo_defaults(df, defaults)

        return df

    def add_provenance(self, df):
        df = super().add_provenance(df)
        if df.empty:
            return df

        today = time.strftime("%Y-%m-%d")
        if self.source_mode.startswith("github_"):
            endpoint_url = github_repo_url(self.config) or self.config.get("endpoint_url")
        else:
            endpoint_url = self.config.get("endpoint_url")
        if not endpoint_url:
            endpoint_url = self.json_path
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

        if self.source_mode == "github_commits":
            return write_commit_delta_outputs(primary_df, distributions_df, self)

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
            f"{today}_{output_basename(self.config, 'primary', self.source_mode)}",
        )
        primary_df.to_csv(primary_filename, index=False, encoding="utf-8")

        results = {"primary_csv": primary_filename}

        if self.config.get("output_distributions_csv"):
            distributions_filename = os.path.join(
                output_dir,
                f"{today}_{output_basename(self.config, 'distributions', self.source_mode)}",
            )
            distributions_df.to_csv(distributions_filename, index=False, encoding="utf-8")
            results["distributions_csv"] = distributions_filename

        return results

    def build_uploads(self, results: dict) -> dict | None:
        if self.source_mode == "github_commits":
            return None
        return super().build_uploads(results)

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
                    "[OGM Aardvark] Failed to parse JSON at %s: %s",
                    file_path,
                    exc,
                )

    return dataset


def load_repo_defaults(csv_path):
    if not csv_path or not os.path.exists(csv_path):
        return {}

    defaults = {}
    with open(csv_path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            repository = str(row.get("Repository", "")).strip()
            if not repository:
                continue

            values = {
                "Code": str(row.get("Code", "")).strip(),
                "Member Of": str(row.get("Member Of", "")).strip(),
            }
            values = {key: value for key, value in values.items() if value}
            if values:
                defaults[repository.lower()] = values

    return defaults


def repo_defaults_for_config(config, repo_defaults):
    repo = str(config.get("github_repo", "")).strip().lower()
    if not repo:
        return {}
    return repo_defaults.get(repo, {})


def apply_repo_defaults(df, defaults):
    for column, value in defaults.items():
        if column not in df.columns:
            df[column] = value
            continue

        existing = df[column].fillna("").astype(str).str.strip()
        df[column] = df[column].where(existing != "", value)

    return df


def fetch_github_tarball_json(config, session):
    owner, repo = get_github_repo_config(config)
    ref = config.get("github_ref", config.get("github_branch", "main"))
    timeout = int(config.get("github_timeout", 60))
    path_filter = normalize_github_path(config.get("github_path", ""))
    url = f"{GITHUB_API_ROOT}/repos/{owner}/{repo}/tarball/{ref}"

    response = session.get(url, timeout=timeout)
    response.raise_for_status()

    dataset = []
    with tarfile.open(fileobj=BytesIO(response.content), mode="r:gz") as archive:
        for member in archive.getmembers():
            relative_path = strip_tarball_root(member.name)
            if not member.isfile() or not is_json_path(relative_path):
                continue
            if path_filter and not path_is_under_filter(relative_path, path_filter):
                continue

            extracted = archive.extractfile(member)
            if extracted is None:
                continue

            try:
                dataset.append(json.loads(extracted.read().decode("utf-8", errors="ignore")))
            except json.JSONDecodeError as exc:
                logging.warning(
                    "[OGM Aardvark] Failed to parse JSON in tarball at %s: %s",
                    member.name,
                    exc,
                )

    return dataset


def fetch_github_commit_json(config, session):
    owner, repo = get_github_repo_config(config)
    path_filter = normalize_github_path(config.get("github_path", ""))
    records = []
    deleted_files = []
    seen_paths = set()

    for commit in iter_selected_github_commits(config, session, owner, repo):
        detail = get_github_commit_detail(config, session, owner, repo, commit["sha"])
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
            if path_filter and not (
                path_is_under_filter(new_path, path_filter)
                or path_is_under_filter(previous_path, path_filter)
            ):
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

            seen_paths.add(new_path)
            if previous_path:
                seen_paths.add(previous_path)

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
    path_filter = normalize_github_path(config.get("github_path", ""))
    page = 1
    yielded = 0

    while True:
        params = {"sha": ref, "per_page": per_page, "page": page}
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        if path_filter:
            params["path"] = path_filter

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
        logging.warning("[OGM Aardvark] GitHub content response for %s was not base64.", path)
        return None

    try:
        raw_content = base64.b64decode(encoded_content).decode("utf-8", errors="ignore")
        return json.loads(raw_content)
    except (ValueError, json.JSONDecodeError) as exc:
        logging.warning("[OGM Aardvark] Failed to decode GitHub JSON at %s: %s", path, exc)
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


def github_repo_url(config):
    owner = config.get("github_owner")
    repo = config.get("github_repo")
    if owner and repo:
        return f"https://github.com/{owner}/{repo}"
    return ""


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


def normalize_github_path(path):
    if not isinstance(path, str):
        return ""
    return path.strip().strip("/")


def path_is_under_filter(path, path_filter):
    if not path:
        return False
    normalized_path = normalize_github_path(path)
    normalized_filter = normalize_github_path(path_filter)
    return normalized_path == normalized_filter or normalized_path.startswith(
        f"{normalized_filter}/"
    )


def strip_tarball_root(path):
    parts = path.split("/", 1)
    return parts[1] if len(parts) == 2 else path


def is_json_path(path):
    return isinstance(path, str) and path.lower().endswith(".json")


def infer_id_from_json_path(path):
    if not path:
        return ""
    return os.path.splitext(os.path.basename(path))[0]


def write_commit_delta_outputs(primary_df, distributions_df, harvester):
    today = time.strftime("%Y-%m-%d")
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)

    primary_filename = os.path.join(
        output_dir,
        f"{today}_{output_basename(harvester.config, 'primary', harvester.source_mode, True)}",
    )
    distributions_filename = os.path.join(
        output_dir,
        f"{today}_"
        f"{output_basename(harvester.config, 'distributions', harvester.source_mode, True)}",
    )
    deletions_filename = os.path.join(
        output_dir,
        f"{today}_{output_basename(harvester.config, 'deletions', harvester.source_mode, True)}",
    )

    primary_columns = [
        col
        for col in PRIMARY_FIELD_ORDER
        if col in primary_df.columns and col not in harvester.PRIMARY_OUTPUT_EXCLUDED_FIELDS
    ]
    primary_columns.extend(
        col
        for col in primary_df.columns
        if col not in PRIMARY_FIELD_ORDER
        and col not in harvester.PRIMARY_OUTPUT_EXCLUDED_FIELDS
        and col not in harvester.distribution_variables
        and col not in primary_columns
    )
    primary_df.reindex(columns=primary_columns).to_csv(
        primary_filename,
        index=False,
        encoding="utf-8",
    )

    if distributions_df.empty:
        distributions_df = pd.DataFrame(
            columns=["friendlier_id", "reference_type", "distribution_url", "label"]
        )
    distributions_df.to_csv(distributions_filename, index=False, encoding="utf-8")

    deletions_df = pd.DataFrame(
        harvester.deleted_github_files,
        columns=COMMIT_DELETION_COLUMNS,
    )
    deletions_df.to_csv(deletions_filename, index=False, encoding="utf-8")

    return {
        "primary_csv": primary_filename,
        "distributions_csv": distributions_filename,
        "deleted_files_csv": deletions_filename,
        "processed_count": len(primary_df),
        "deleted_count": len(deletions_df),
    }


def output_basename(config, output_kind, source_mode, commit_delta=False):
    repo_slug = github_repo_slug(config)
    if source_mode.startswith("github_") and repo_slug:
        if output_kind == "deletions":
            suffix = "commit_deletions"
        elif commit_delta:
            suffix = f"commit_delta_{output_kind}"
        else:
            suffix = output_kind
        return f"ogm_{repo_slug}_{suffix}.csv"

    configured_key = (
        "output_distributions_csv"
        if output_kind == "distributions"
        else "output_primary_csv"
    )
    if output_kind == "deletions":
        configured_path = "ogm_aardvark_deletions.csv"
    else:
        configured_path = config[configured_key]

    if commit_delta:
        return commit_delta_basename(configured_path, output_kind)
    return os.path.basename(configured_path)


def github_repo_slug(config):
    repo = config.get("github_repo", "")
    if not isinstance(repo, str):
        return ""

    slug = re.sub(r"[^A-Za-z0-9]+", "-", repo.strip()).strip("-").lower()
    return slug


def commit_delta_basename(configured_path, output_kind):
    basename = os.path.basename(configured_path)
    expected_suffix = f"_{output_kind}.csv"
    replacement_suffix = f"_commit_delta_{output_kind}.csv"

    if basename.endswith(expected_suffix):
        return basename[: -len(expected_suffix)] + replacement_suffix

    return basename.replace(".csv", replacement_suffix)
