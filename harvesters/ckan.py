import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import pandas as pd
import requests

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
from utils.temporal_fields import create_date_range, infer_temporal_coverage_from_title


class CkanHarvester(BaseHarvester):
    def __init__(self, config):
        config = dict(config)
        config.setdefault("build_uploads", True)
        config.setdefault("output_report_csv", "reports/ckan/ckan_report.csv")
        super().__init__(config)
        self.workflow_input_path = self.config.get("input_csv", "")
        self.hub_metadata_path = self.config.get("hub_metadata_csv", "")
        self.report_output_path = self.config["output_report_csv"]
        self._hub_metadata_lookup = None
        self._harvest_report_rows = []
        if self.workflow_input_path:
            required_config_path(self.config, "input_csv", "CKAN")
            self.base_url = str(self.config.get("base_url", "")).rstrip("/")
            self.endpoint_url = self.config.get("endpoint_url", "")
        else:
            self.base_url = self.config["base_url"].rstrip("/")
            self.endpoint_url = self.config.get(
                "endpoint_url",
                f"{self.base_url}/api/3/action/package_search",
            )
        self.rows = int(self.config.get("rows", 100))
        self.timeout = int(self.config.get("timeout", 60))

    def load_reference_data(self):
        super().load_reference_data()
        if self.hub_metadata_path:
            self._hub_metadata_lookup = load_metadata_lookup(self.hub_metadata_path)

    def fetch(self):
        session = requests.Session()
        session.headers.update(
            {"User-Agent": self.config.get("user_agent", "harvester-api ckan")}
        )

        if self.workflow_input_path:
            if self._hub_metadata_lookup is None and self.hub_metadata_path:
                self._hub_metadata_lookup = load_metadata_lookup(self.hub_metadata_path)

            workflow_df = pd.read_csv(self.workflow_input_path, dtype=str).fillna("")
            for harvest_record in workflow_df.to_dict(orient="records"):
                hub_defaults = match_metadata_defaults(
                    harvest_record,
                    self._hub_metadata_lookup or {},
                )
                endpoint_url = ckan_package_search_endpoint(harvest_record.get("Endpoint URL", ""))
                base_url = ckan_base_url_from_endpoint(endpoint_url)
                site_title = first_non_empty(
                    hub_defaults.get("Title", ""),
                    harvest_record.get("Title", ""),
                    base_url,
                )
                site_code = first_non_empty(
                    hub_defaults.get("Code", ""),
                    harvest_record.get("Code", ""),
                    harvest_record.get("Identifier", ""),
                    harvest_record.get("ID", ""),
                )

                source = {
                    "workflow": harvest_record,
                    "hub_defaults": hub_defaults,
                    "base_url": base_url,
                    "endpoint_url": endpoint_url,
                    "site_title": site_title,
                }

                yield f"[CKAN] Starting {site_code or site_title} - {site_title}."
                try:
                    yield from ckan_fetch_package_search_records(
                        session=session,
                        endpoint_url=endpoint_url,
                        rows=self.rows,
                        timeout=self.timeout,
                        config=self.config,
                        source=source,
                    )
                    total_found = int(source.get("total_found", 0))
                    message = (
                        f"[CKAN] Fetched {total_found} package records from "
                        f"{site_code or site_title}."
                    )
                    self._harvest_report_rows.append(
                        build_ckan_harvest_report_run_row(
                            harvest_record,
                            "success",
                            message,
                            total_found,
                        )
                    )
                except Exception as exc:
                    message = f"[CKAN] Error fetching {site_code or site_title}: {exc}"
                    self._harvest_report_rows.append(
                        build_ckan_harvest_report_run_row(
                            harvest_record,
                            "error",
                            message,
                            0,
                        )
                    )
                    yield message
            return

        endpoint_url = ckan_package_search_endpoint(self.endpoint_url)
        start = 0
        total = None

        while True:
            params = {"rows": self.rows, "start": start}
            if self.config.get("q"):
                params["q"] = self.config["q"]
            if self.config.get("fq"):
                params["fq"] = self.config["fq"]

            response = session.get(endpoint_url, params=params, timeout=self.timeout)
            response.raise_for_status()

            payload = response.json()
            if not payload.get("success"):
                raise ValueError(f"CKAN API returned unsuccessful response for {endpoint_url}")

            result = payload.get("result", {})
            records = result.get("results", [])
            total = result.get("count", start + len(records))

            yield f"[CKAN] Fetched {len(records)} package records (start={start}, total={total})."

            if not records:
                break

            for record in records:
                yield record

            start += len(records)
            if total is not None and start >= total:
                break

    def flatten(self, harvested_records):
        return [record for record in harvested_records if isinstance(record, dict)]

    def build_dataframe(self, flattened_items):
        df = pd.DataFrame(flattened_items)
        if df.empty:
            return pd.DataFrame()
        return df.pipe(ckan_map_to_schema, config=self.config, base_url=self.base_url)

    def derive_fields(self, df):
        df = super().derive_fields(df)
        if df.empty:
            return df

        df = (
            df.pipe(ckan_title_from_alternative, title_suffix=self.config.get("title_suffix", ""))
            .pipe(ckan_fill_spatial_fields)
            .pipe(ckan_temporal_coverage)
            .pipe(ckan_format_date_ranges)
        )

        return df

    def add_defaults(self, df):
        existing_access_rights = df["Access Rights"].copy() if "Access Rights" in df.columns else None
        df = super().add_defaults(df)
        if df.empty:
            return df

        if existing_access_rights is not None:
            df["Access Rights"] = existing_access_rights.where(
                existing_access_rights.astype(str).str.strip() != "",
                df["Access Rights"],
            )

        df["Language"] = self.config.get("language", "eng")
        df["Resource Class"] = self.config.get("resource_class", "Datasets")

        optional_defaults = {
            "Member Of": self.config.get("member_of", ""),
            "Is Part Of": self.config.get("is_part_of", ""),
            "Code": self.config.get("code", ""),
            "Publisher": self.config.get("publisher", self.config.get("site_title", "")),
            "Spatial Coverage": self.config.get("spatial_coverage", ""),
            "Bounding Box": self.config.get("bounding_box", ""),
        }
        for field, value in optional_defaults.items():
            if value:
                df[field] = df[field].where(df[field].astype(str).str.strip() != "", value)

        return df

    def add_provenance(self, df):
        df = super().add_provenance(df)
        if df.empty:
            return df

        today = time.strftime("%Y-%m-%d")
        site_title = self.config.get("site_title", self.base_url)

        df["Website Platform"] = "CKAN"
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = df.apply(
            lambda row: ckan_source_value(
                row,
                "Harvest Workflow",
                self.config.get("harvest_workflow", "py_ckan"),
                source_section="workflow",
            ),
            axis=1,
        )
        df["Endpoint URL"] = df.apply(
            lambda row: ckan_source_endpoint(row, self.endpoint_url),
            axis=1,
        )
        df["Endpoint Description"] = "CKAN API (package_search)"
        df["Provenance"] = df.apply(
            lambda row: (
                "The metadata for this resource was last retrieved from "
                f"{ckan_source_site_title(row, site_title)} on {today}."
            ),
            axis=1,
        )

        if self.workflow_input_path:
            harvest_record_df = build_updated_harvest_record_rows(self.workflow_input_path, today)
            if not harvest_record_df.empty:
                df = pd.concat([df, harvest_record_df], ignore_index=True)

        return df

    def clean(self, df):
        return super().clean(df)

    def validate(self, df):
        return super().validate(df)

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        results = super().write_outputs(primary_df, distributions_df)
        report_path = write_ckan_harvest_report(
            self._harvest_report_rows,
            results["primary_csv"],
            self.config["output_primary_csv"],
            self.report_output_path,
        )
        results["report_csv"] = report_path
        return results

    def build_uploads(self, results: dict) -> dict | None:
        return super().build_uploads(results)


# Custom functions for this harvester


def ckan_map_to_schema(df: pd.DataFrame, config: dict, base_url: str) -> pd.DataFrame:
    schema_df = pd.DataFrame()
    empty_series = pd.Series([""] * len(df), index=df.index, dtype="object")

    schema_df["Alternative Title"] = df.get("title", "")
    schema_df["Description"] = df.get("notes", "").apply(ckan_strip_markup)
    schema_df["Creator"] = df.apply(ckan_get_creator, axis=1)
    schema_df["Publisher"] = df.apply(
        lambda row: ckan_source_value(
            row,
            "Title",
            config.get("publisher", config.get("site_title", "")),
            include_workflow=False,
        ),
        axis=1,
    )
    schema_df["Keyword"] = df.apply(
        lambda row: ckan_join_values(ckan_extract_named_values(row.get("tags"))),
        axis=1,
    )
    schema_df["Subject"] = df.apply(
        lambda row: ckan_join_values(ckan_extract_named_values(row.get("groups"))),
        axis=1,
    )
    schema_df["Date Issued"] = df.get("metadata_created", empty_series).apply(ckan_split_date)
    schema_df["Date Modified"] = df.get("metadata_modified", empty_series).apply(ckan_split_date)
    schema_df["License"] = df.apply(
        lambda row: row.get("license_url") or row.get("license_title") or "",
        axis=1,
    )
    schema_df["Rights"] = df.get("license_title", empty_series)
    schema_df["Access Rights"] = df.apply(ckan_get_access_rights, axis=1)
    schema_df["ID"] = df.get("id", empty_series)
    schema_df["Identifier"] = df.get("name", empty_series)
    schema_df["Member Of"] = df.apply(lambda row: ckan_source_value(row, "Member Of", ""), axis=1)
    schema_df["Is Part Of"] = df.apply(lambda row: ckan_source_value(row, "Is Part Of", ""), axis=1)
    schema_df["Code"] = df.apply(lambda row: ckan_source_value(row, "Code", ""), axis=1)
    schema_df["Spatial Coverage"] = df.apply(
        lambda row: ckan_source_value(row, "Spatial Coverage", ""),
        axis=1,
    )
    schema_df["Bounding Box"] = df.apply(
        lambda row: ckan_get_bounding_box(row) or ckan_source_value(row, "Bounding Box", ""),
        axis=1,
    )
    schema_df["information"] = df.apply(
        lambda row: ckan_build_information_url(row, base_url),
        axis=1,
    )
    schema_df["thumbnail"] = df.apply(
        lambda row: row.get("image_display_url") or ckan_get_extra(row, "image_url") or "",
        axis=1,
    )
    schema_df["_ckan_source"] = df.get("_ckan_source", empty_series)

    resource_columns = df.apply(ckan_extract_resource_columns, axis=1, result_type="expand")
    return pd.concat([schema_df, resource_columns], axis=1)


def ckan_title_from_alternative(df: pd.DataFrame, title_suffix: str = "") -> pd.DataFrame:
    df["Title"] = df["Alternative Title"].fillna("")
    suffix = str(title_suffix or "").strip()
    if suffix:
        df["Title"] = df["Title"].apply(lambda value: f"{value}{suffix}" if value else value)
    return df


def ckan_fill_spatial_fields(df: pd.DataFrame) -> pd.DataFrame:
    creator_places = df["Creator"].apply(ckan_derive_place_from_creator)

    df["Spatial Coverage"] = df["Spatial Coverage"].where(
        df["Spatial Coverage"].astype(str).str.strip() != "",
        creator_places.apply(ckan_place_to_spatial_coverage),
    )
    df["Bounding Box"] = df.apply(
        lambda row: row["Bounding Box"]
        if isinstance(row.get("Bounding Box"), str) and row.get("Bounding Box").strip()
        else ckan_place_to_bounding_box(
            ckan_derive_place_from_spatial_coverage(row.get("Spatial Coverage", ""))
        ),
        axis=1,
    )
    return df


def ckan_temporal_coverage(df: pd.DataFrame) -> pd.DataFrame:
    df["Temporal Coverage"] = df.apply(infer_temporal_coverage_from_title, axis=1)
    return df


def ckan_format_date_ranges(df: pd.DataFrame) -> pd.DataFrame:
    df["Date Range"] = df.apply(
        lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
        axis=1,
    )
    return df


def ckan_build_information_url(row: pd.Series, fallback_base_url: str) -> str:
    name = row.get("name", "")
    if not name:
        return ""
    source = ckan_source(row)
    base_url = source.get("base_url") or fallback_base_url
    if not base_url:
        return ""
    return urljoin(f"{base_url}/", f"dataset/{name}")


def ckan_get_creator(row: pd.Series) -> str:
    organization = row.get("organization")
    if isinstance(organization, dict) and organization.get("title"):
        return organization["title"]

    for key in ("author", "maintainer", "data_steward_name"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return ""


def ckan_get_access_rights(row: pd.Series) -> str:
    if row.get("private") is True:
        return "Restricted"

    access_level = str(
        row.get("access_level") or ckan_get_extra(row, "access_level") or ""
    ).strip().lower()
    if access_level and access_level != "public":
        return "Restricted"

    return "Public"


def ckan_get_bounding_box(row: pd.Series) -> str:
    raw_spatial = row.get("spatial") or ckan_get_extra(row, "spatial")
    if not isinstance(raw_spatial, str) or not raw_spatial.strip():
        return ""

    numbers = [float(value) for value in re.findall(r"-?\d+(?:\.\d+)?", raw_spatial)]
    if len(numbers) < 4:
        return ""

    xs = numbers[0::2]
    ys = numbers[1::2]
    if not xs or not ys:
        return ""

    return f"{min(xs)},{min(ys)},{max(xs)},{max(ys)}"


def ckan_derive_place_from_creator(creator: Any) -> str:
    lowered_creator = str(creator or "").lower()
    if "allegheny county" in lowered_creator:
        return "allegheny_county"
    if "pittsburgh" in lowered_creator:
        return "pittsburgh"
    if "pennsylvania" in lowered_creator:
        return "pennsylvania"
    return ""


def ckan_place_to_spatial_coverage(place: str) -> str:
    if place == "allegheny_county":
        return "Pennsylvania--Allegheny County|Pennsylvania"
    if place == "pittsburgh":
        return "Pennsylvania--Pittsburgh|Pennsylvania"
    if not place:
        return ""
    return "Pennsylvania"


def ckan_place_to_bounding_box(place: str) -> str:
    if place == "pittsburgh":
        return "-80.0955,40.3615,-79.8657,40.5012"
    if not place:
        return ""
    return "-80.36,40.19,-79.69,40.67"


def ckan_derive_place_from_spatial_coverage(spatial_coverage: Any) -> str:
    lowered_spatial = str(spatial_coverage or "").lower()
    if "allegheny county" in lowered_spatial:
        return "allegheny_county"
    if "pittsburgh" in lowered_spatial:
        return "pittsburgh"
    if "pennsylvania" in lowered_spatial:
        return "pennsylvania"
    return ""


def ckan_get_extra(row: pd.Series, key: str) -> Any:
    extras = row.get("extras")
    if not isinstance(extras, list):
        return None

    for extra in extras:
        if isinstance(extra, dict) and extra.get("key") == key:
            return extra.get("value")
    return None


def ckan_extract_resource_columns(row: pd.Series) -> pd.Series:
    downloads = []
    geo_json = ""
    feature_service = ""
    map_service = ""
    image_service = ""
    wfs = ""
    wms = ""
    documentation = ""

    for resource in row.get("resources", []) or []:
        if not isinstance(resource, dict):
            continue

        url = (resource.get("url") or "").strip()
        if not url:
            continue

        resource_format = (resource.get("format") or "").strip()
        lowered_url = url.lower()
        lowered_format = resource_format.lower()
        label = resource_format or resource.get("name") or ""

        if "featureserver" in lowered_url or lowered_format == "featureserver":
            feature_service = feature_service or url
        elif "mapserver" in lowered_url or lowered_format == "mapserver":
            map_service = map_service or url
        elif "imageserver" in lowered_url or lowered_format == "imageserver":
            image_service = image_service or url
        elif lowered_format == "geojson" or lowered_url.endswith(".geojson"):
            geo_json = geo_json or url
        elif lowered_format == "wfs":
            wfs = wfs or url
        elif lowered_format == "wms":
            wms = wms or url
        elif lowered_format == "pdf":
            documentation = documentation or url
        else:
            downloads.append({"url": url, "label": label})

    return pd.Series(
        {
            "download": downloads,
            "geo_json": geo_json,
            "featureService": feature_service,
            "mapService": map_service,
            "imageService": image_service,
            "wfs": wfs,
            "wms": wms,
            "documentation": documentation,
        }
    )


def ckan_extract_named_values(values) -> list[str]:
    names = []
    if not isinstance(values, list):
        return names

    for value in values:
        if isinstance(value, dict):
            name = value.get("display_name") or value.get("title") or value.get("name") or ""
            if name:
                names.append(str(name).strip())
        elif isinstance(value, str) and value.strip():
            names.append(value.strip())

    return names


def ckan_join_values(values: list[str]) -> str:
    cleaned = [value for value in values if value]
    return "|".join(cleaned)


def ckan_split_date(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.split("T", 1)[0]


def ckan_strip_markup(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    value = re.sub(r"<!--.*?-->", "", value, flags=re.DOTALL)
    return value.strip()


def ckan_package_search_endpoint(endpoint_url: str) -> str:
    parsed = urlparse(str(endpoint_url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return str(endpoint_url or "").strip()

    path = parsed.path.rstrip("/")
    if path.endswith("/api/3/action/package_search"):
        return urlunparse(parsed._replace(path=path))
    if path.endswith("/api/3/action/package_list"):
        return urlunparse(parsed._replace(path=path.removesuffix("package_list") + "package_search"))
    if "/api/3/action/" not in path:
        path = f"{path}/api/3/action/package_search" if path else "/api/3/action/package_search"
        return urlunparse(parsed._replace(path=path))
    return urlunparse(parsed._replace(path=path))


def ckan_base_url_from_endpoint(endpoint_url: str) -> str:
    parsed = urlparse(str(endpoint_url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return ""

    api_marker = "/api/3/action/"
    path = parsed.path
    if api_marker in path:
        path = path.split(api_marker, 1)[0]
    return urlunparse(parsed._replace(path=path.rstrip("/"), params="", query="", fragment="")).rstrip("/")


def ckan_fetch_package_search_records(
    session: requests.Session,
    endpoint_url: str,
    rows: int,
    timeout: int,
    config: dict,
    source: dict,
):
    start = 0
    total = None

    while True:
        params = {"rows": rows, "start": start}
        if config.get("q"):
            params["q"] = config["q"]
        if config.get("fq"):
            params["fq"] = config["fq"]

        response = session.get(endpoint_url, params=params, timeout=timeout)
        response.raise_for_status()

        payload = response.json()
        if not payload.get("success"):
            raise ValueError(f"CKAN API returned unsuccessful response for {endpoint_url}")

        result = payload.get("result", {})
        records = result.get("results", [])
        total = result.get("count", start + len(records))
        source["total_found"] = total

        yield (
            f"[CKAN] Fetched {len(records)} package records "
            f"(start={start}, total={total}) from {source.get('site_title', endpoint_url)}."
        )

        if not records:
            break

        for record in records:
            record = dict(record)
            record["_ckan_source"] = source
            yield record

        start += len(records)
        if total is not None and start >= total:
            break


def ckan_source(row: pd.Series) -> dict:
    source = row.get("_ckan_source")
    return source if isinstance(source, dict) else {}


def ckan_source_value(
    row: pd.Series,
    key: str,
    fallback: str = "",
    source_section: str = "hub_defaults",
    include_workflow: bool = True,
) -> str:
    source = ckan_source(row)
    values = []
    section = source.get(source_section)
    if isinstance(section, dict):
        values.append(section.get(key, ""))
    if include_workflow and source_section != "workflow":
        workflow = source.get("workflow")
        if isinstance(workflow, dict):
            values.append(workflow.get(key, ""))
    values.append(fallback)
    return first_non_empty(*values)


def ckan_source_endpoint(row: pd.Series, fallback: str = "") -> str:
    source = ckan_source(row)
    return first_non_empty(source.get("endpoint_url", ""), fallback)


def ckan_source_site_title(row: pd.Series, fallback: str = "") -> str:
    source = ckan_source(row)
    return first_non_empty(source.get("site_title", ""), ckan_source_value(row, "Title", ""), fallback)


def build_ckan_harvest_report_run_row(
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


def write_ckan_harvest_report(
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
        else Path("reports/ckan")
    )
    report_path = report_dir / f"{today}_{configured_report_path.name}"
    report_df = build_ckan_harvest_report_dataframe(
        report_rows,
        Path(current_primary_csv),
        configured_primary_csv,
    )
    os.makedirs(report_path.parent, exist_ok=True)
    report_df.to_csv(report_path, index=False, encoding="utf-8")
    print(f"[CKAN] Wrote harvest report: {report_path}")
    return str(report_path)


def build_ckan_harvest_report_dataframe(
    report_rows: list[dict],
    current_primary_path: Path,
    configured_primary_csv: str,
) -> pd.DataFrame:
    current_df = load_ckan_report_primary(current_primary_path)
    previous_df = load_previous_ckan_primary_for_report(current_primary_path, configured_primary_csv)
    counts_by_code = build_ckan_report_counts_by_code(current_df, previous_df)

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
        "Harvest Run": build_ckan_harvest_run_tally(report_df),
        "Harvest Message": "",
        "Total Records Found": int(report_df["Total Records Found"].sum()) if not report_df.empty else 0,
        "New Records": int(report_df["New Records"].sum()) if not report_df.empty else 0,
        "Unpublished Records": int(report_df["Unpublished Records"].sum()) if not report_df.empty else 0,
    }
    return pd.concat([report_df, pd.DataFrame([total_row])], ignore_index=True)


def load_ckan_report_primary(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")


def load_previous_ckan_primary_for_report(
    current_primary_path: Path,
    configured_primary_csv: str,
) -> pd.DataFrame:
    outputs_dir = current_primary_path.resolve().parent
    source = infer_upload_source_prefix(configured_primary_csv)
    candidates = discover_dated_files(outputs_dir, build_filename_regex(source, "primary"))
    previous = most_recent_file_before(candidates, current_primary_path)
    if previous is None:
        return pd.DataFrame()
    return load_ckan_report_primary(previous[1])


def build_ckan_report_counts_by_code(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
) -> dict[str, dict[str, int]]:
    current_records = ckan_report_dataset_rows(current_df)
    previous_records = ckan_report_dataset_rows(previous_df)
    counts: dict[str, dict[str, int]] = {}

    current_ids = set(current_records["ID"]) if "ID" in current_records.columns else set()
    previous_ids = set(previous_records["ID"]) if "ID" in previous_records.columns else set()

    new_records = current_records[current_records["ID"].isin(current_ids - previous_ids)]
    unpublished_records = previous_records[previous_records["ID"].isin(previous_ids - current_ids)]

    for code, count in new_records.groupby("__parent_code").size().items():
        counts.setdefault(str(code), {})["new"] = int(count)
    for code, count in unpublished_records.groupby("__parent_code").size().items():
        counts.setdefault(str(code), {})["unpublished"] = int(count)
    return counts


def ckan_report_dataset_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "ID" not in df.columns:
        return pd.DataFrame(columns=["ID", "__parent_code"])
    work = df.copy()
    for column in ["ID", "Code", "Is Part Of", "Resource Class"]:
        if column not in work.columns:
            work[column] = ""
        work[column] = work[column].astype(str).str.strip()
    work["__parent_code"] = work["Code"].where(work["Code"].ne(""), work["Is Part Of"])
    return work[
        work["ID"].ne("")
        & ~work["ID"].str.startswith("harvest_")
        & work["__parent_code"].ne("")
        & work["Resource Class"].str.lower().ne("websites")
        & work["Resource Class"].str.lower().ne("series")
    ].copy()


def build_ckan_harvest_run_tally(report_df: pd.DataFrame) -> str:
    if report_df.empty or "Harvest Run" not in report_df.columns:
        return "success: 0; error: 0"
    counts = report_df["Harvest Run"].value_counts()
    return f"success: {int(counts.get('success', 0))}; error: {int(counts.get('error', 0))}"
