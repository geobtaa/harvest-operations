import re
import time
from datetime import date, datetime

import pandas as pd
import requests

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table
from utils.field_order import FIELD_ORDER
from utils.language_codes import convert_languages_to_iso, load_language_mapping
from utils.marc_coordinates import first_marc_bbox


class UmediaHarvester(BaseHarvester):
    """Harvest map records from the University of Minnesota uMedia JSON API."""

    def __init__(self, config):
        config = dict(config)
        config.setdefault("build_uploads", True)
        if config.get("date_added_on_or_after"):
            config["build_uploads"] = False
        super().__init__(config)

        self.base_url = self.config.get(
            "base_url",
            "https://umedia.lib.umn.edu/search.json",
        )
        self.facets = self.config.get(
            "facets",
            {
                "contributing_organization_name_s": (
                    "University of Minnesota Libraries, John R. Borchert Map Library."
                )
            },
        )
        self.max_items = int(self.config.get("max_items", 10000))
        self.page_size = int(self.config.get("page_size", 20))
        self.page_start = int(self.config.get("page_start", 0))
        self.timeout = float(self.config.get("timeout", 60))
        self.date_added_on_or_after = umedia_filter_date(
            self.config.get("date_added_on_or_after")
        )
        self.language_mapping = {}

    def load_reference_data(self):
        super().load_reference_data()
        self.language_mapping = load_language_mapping(
            self.config.get("language_vocabulary_csv")
        )
        print(
            f"[uMedia] Loaded {len(self.language_mapping)} language mappings."
        )

    def fetch(self):
        records = []
        page = self.page_start

        while len(records) < self.max_items:
            params = umedia_request_params(self.config, self.facets, page)
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            page_records = response.json()

            if not isinstance(page_records, list):
                raise ValueError(
                    "[uMedia] Expected the search endpoint to return a JSON list."
                )
            if not page_records:
                break

            remaining = self.max_items - len(records)
            records.extend(page_records[:remaining])
            print(
                f"[uMedia] Fetched page {page}: {len(page_records)} record(s); "
                f"{len(records)} total."
            )

            if len(page_records) < self.page_size:
                break
            page += 1

        print(f"[uMedia] Fetched {len(records)} record(s) from {self.base_url}.")
        return records

    def flatten(self, harvested_metadata):
        records = [record for record in harvested_metadata if isinstance(record, dict)]
        if self.date_added_on_or_after is not None:
            before_count = len(records)
            records = [
                record
                for record in records
                if umedia_record_added_on_or_after(
                    record,
                    self.date_added_on_or_after,
                )
            ]
            print(
                f"[uMedia] Kept {len(records)} of {before_count} record(s) with "
                f"date_added on or after {self.date_added_on_or_after.isoformat()}."
            )
        print(f"[uMedia] Prepared {len(records)} record(s) for schema mapping.")
        return records

    def build_dataframe(self, flattened_items):
        language_mapping = self.language_mapping or load_language_mapping(
            self.config.get("language_vocabulary_csv")
        )
        return pd.DataFrame(flattened_items).pipe(
            umedia_map_to_schema,
            self.config,
            language_mapping,
        )

    def derive_fields(self, df):
        return super().derive_fields(df)

    def add_defaults(self, df):
        df = super().add_defaults(df)
        df["Code"] = self.config.get("code", "05d-01")
        df["Is Part Of"] = self.config.get("is_part_of", "05d-01")
        df["Member Of"] = self.config.get(
            "member_of",
            "64bd8c4c-8e60-4956-b43d-bdc3f93db488",
        )
        df["Resource Class"] = self.config.get("resource_class", "Maps")
        df["Resource Type"] = self.config.get("resource_type", "")
        df["Format"] = self.config.get("format", "JPEG")
        return df

    def add_provenance(self, df):
        df = super().add_provenance(df)
        today = time.strftime("%Y-%m-%d")
        source_name = self.config.get(
            "source_name",
            "University of Minnesota Libraries",
        )

        df["Website Platform"] = self.config.get("website_platform", "uMedia")
        df["Accrual Method"] = self.config.get("accrual_method", "JSON API")
        df["Accrual Periodicity"] = self.config.get("accrual_periodicity", "Irregular")
        df["Harvest Workflow"] = self.config.get("harvest_workflow", "py_umedia")
        df["Endpoint Description"] = self.config.get(
            "endpoint_description",
            "uMedia JSON API",
        )
        df["Endpoint URL"] = self.base_url
        df["Provenance"] = df.apply(
            lambda row: umedia_provenance(
                source_name,
                today,
                row.get("Provenance", ""),
            ),
            axis=1,
        )
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        if distributions_df is None:
            distributions_df = generate_secondary_table(
                primary_df.copy(),
                self.distribution_types,
            )
        return super().write_outputs(primary_df, distributions_df)


# Custom functions for this harvester


def umedia_request_params(config, facets, page):
    params = dict(config.get("api_params", {}))
    for field, value in facets.items():
        params[f"facets[{field}][]"] = value
    params["page"] = page
    return params


def umedia_filter_date(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise ValueError(
            "[uMedia] date_added_on_or_after must use YYYY-MM-DD format."
        ) from exc


def umedia_record_added_on_or_after(record, cutoff_date):
    source_date = umedia_source_date(record.get("date_added"))
    return source_date is not None and source_date >= cutoff_date


def umedia_source_date(value):
    clean_value = umedia_scalar(value)
    if not clean_value:
        return None

    try:
        return datetime.fromisoformat(clean_value.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(clean_value[:10])
        except ValueError:
            return None


def umedia_map_to_schema(df, config, language_mapping):
    rows = [
        umedia_build_schema_row(record, config, language_mapping)
        for record in df.to_dict(orient="records")
    ]
    return pd.DataFrame(rows).reindex(columns=FIELD_ORDER, fill_value="")


def umedia_build_schema_row(record, config, language_mapping):
    item_id = umedia_scalar(record.get("id"))
    set_spec = umedia_scalar(record.get("set_spec"))
    parent_id = umedia_scalar(record.get("parent_id"))

    item_base_url = config.get(
        "item_base_url",
        "https://umedia.lib.umn.edu/item",
    ).rstrip("/")
    contentdm_base_url = config.get(
        "contentdm_base_url",
        "https://cdm16022.contentdm.oclc.org",
    ).rstrip("/")

    information_url = f"{item_base_url}/{item_id}" if item_id else ""
    download_url = ""
    manifest_url = ""
    if set_spec and parent_id:
        download_url = (
            f"{contentdm_base_url}/utils/getfile/collection/{set_spec}/id/{parent_id}"
            "/filename/print/page/download/fparams/forcedownload"
        )
        manifest_url = (
            f"{contentdm_base_url}/iiif/info/{set_spec}/{parent_id}/manifest.json"
        )

    title = umedia_scalar(record.get("title"))
    date_issued = umedia_join(record.get("date_created"), separator=";")
    temporal_coverage = umedia_join(record.get("date_created"))
    date_range = umedia_date_range(
        record.get("date_created_sort") or record.get("date_created")
    )
    thumbnail_url = umedia_scalar(record.get("thumb_url"))
    source_coordinates = record.get("coordinates")
    date_added = umedia_scalar(record.get("date_added"))
    provider = umedia_scalar(record.get("contributing_organization")) or config.get(
        "provider",
        "",
    )

    return {
        "ID": item_id,
        "Title": title,
        "Alternative Title": title,
        "Description": umedia_join_fields(
            record,
            ("description", "notes", "dimensions"),
        ),
        "Language": convert_languages_to_iso(
            record.get("language"),
            language_mapping,
        ),
        "Creator": umedia_join(record.get("creator")),
        "Publisher": umedia_join(record.get("publisher")),
        "Provider": provider,
        "Keyword": umedia_join(record.get("subject")),
        "Local Collection": umedia_join(record.get("collection_name")),
        "Temporal Coverage": temporal_coverage,
        "Date Issued": date_issued,
        "Index Year": date_range.split("-", 1)[0] if date_range else "",
        "Date Range": date_range,
        "Spatial Coverage": umedia_spatial_coverage(record),
        "Bounding Box": first_marc_bbox(source_coordinates),
        "Spatial Resolution as Text": umedia_join(record.get("scale")),
        "Rights": umedia_join(record.get("local_rights")),
        "B1G Image": thumbnail_url,
        "Identifier": umedia_join(record.get("persistent_url")) or information_url,
        "Provenance": umedia_date_added_provenance(date_added),
        "information": information_url,
        "download": download_url,
        "manifest": manifest_url,
        "thumbnail": thumbnail_url,
        "Admin Note": umedia_coordinates_admin_note(source_coordinates),
    }


def umedia_join_fields(record, fields):
    values = []
    for field in fields:
        values.extend(umedia_values(record.get(field)))
    return "|".join(umedia_unique(values))


def umedia_spatial_coverage(record):
    countries = umedia_place_values(record.get("country"))
    states = umedia_place_values(record.get("state"))
    cities = umedia_place_values(record.get("city"))
    regions = umedia_place_values(record.get("region"))
    fast_terms = []

    if states:
        for state in states:
            if not cities and not regions:
                fast_terms.append(state)
            fast_terms.extend(f"{state}--{city}" for city in cities)
            fast_terms.extend(f"{state}--{region}" for region in regions)
    else:
        for country in countries:
            if not cities and not regions:
                fast_terms.append(country)
            fast_terms.extend(f"{country}--{city}" for city in cities)
            fast_terms.extend(f"{country}--{region}" for region in regions)

    fast_terms.extend(states)
    fast_terms.extend(countries)
    if not countries and not states:
        fast_terms.extend(regions)

    normalized_terms = [term.replace("St.", "Saint") for term in fast_terms]
    return "|".join(umedia_unique(normalized_terms))


def umedia_place_values(value):
    values = []
    for raw_value in umedia_values(value):
        values.extend(
            part.strip()
            for part in re.split(r"\s*[;|]\s*", raw_value)
            if part.strip()
        )
    return umedia_unique(values)


def umedia_coordinates_admin_note(source_coordinates):
    return "|".join(
        f"Draft source coordinates; cleanup required: {coordinate}"
        for coordinate in umedia_unique(umedia_values(source_coordinates))
    )


def umedia_date_added_provenance(date_added):
    if not date_added:
        return ""
    return f"The source metadata date_added value is {date_added}."


def umedia_provenance(source_name, retrieval_date, source_provenance):
    provenance = (
        f"The metadata for this resource was last retrieved from {source_name} "
        f"on {retrieval_date}."
    )
    if source_provenance:
        provenance += f" {source_provenance}"
    return provenance


def umedia_date_range(value):
    years = []
    for part in umedia_values(value):
        years.extend(int(year) for year in re.findall(r"\b\d{4}\b", part))
    if not years:
        return ""
    return f"{min(years)}-{max(years)}"


def umedia_join(value, separator="|"):
    return separator.join(umedia_unique(umedia_values(value)))


def umedia_values(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        values = []
        for item in value:
            values.extend(umedia_values(item))
        return values
    if isinstance(value, dict):
        return umedia_values(value.get("label") or value.get("name") or value.get("id"))
    if pd.isna(value):
        return []

    clean_value = str(value).strip()
    return [clean_value] if clean_value else []


def umedia_scalar(value):
    values = umedia_values(value)
    return values[0] if values else ""


def umedia_unique(values):
    seen = set()
    unique_values = []
    for value in values:
        clean_value = str(value).strip()
        if not clean_value or clean_value in seen:
            continue
        seen.add(clean_value)
        unique_values.append(clean_value)
    return unique_values
