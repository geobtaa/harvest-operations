import json
import re
import time
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table
from utils.temporal_fields import create_date_range


class HyraxHarvester(BaseHarvester):
    """
    Harvester for locally downloaded Hyrax/Samvera JSON records.

    This scaffold is intentionally conservative: it crosswalks the common Hyrax
    fields into the GeoBTAA schema, preserves unresolved authority values as raw
    strings, and leaves room for site-specific refinements later.
    """

    ROLE_FIELDS = [
        "creator_display",
        "creator",
        "author",
        "cartographer",
        "contributor",
        "photographer",
        "publisher",
    ]

    WORK_TYPE_LABELS = {
        "http://vocab.getty.edu/aat/300028094": "Maps",
    }

    RESOURCE_CLASS_BY_TYPE = {
        "collection": "Collections",
        "dataset": "Datasets",
        "image": "Imagery",
        "movingimage": "Imagery",
        "service": "Web services",
        "software": "Websites",
        "interactiveresource": "Websites",
    }

    EMPTY_PRIMARY_COLUMNS = [
        "ID",
        "Title",
        "Alternative Title",
        "Description",
        "Language",
        "Creator",
        "Creator ID",
        "Publisher",
        "Provider",
        "Resource Class",
        "Resource Type",
        "Subject",
        "Keyword",
        "Local Collection",
        "Temporal Coverage",
        "Date Issued",
        "Index Year",
        "Date Range",
        "Spatial Coverage",
        "Bounding Box",
        "GeoNames",
        "Provenance",
        "Rights",
        "License",
        "Rights Holder",
        "Access Rights",
        "Identifier",
        "Code",
        "Member Of",
        "Is Part Of",
        "information",
        "manifest",
        "thumbnail",
        "iiif",
    ]

    def __init__(self, config):
        super().__init__(config)
        self.input_json_root = Path(self.config["input_json_root"])
        self.input_json_glob = self.config.get("input_json_glob", "**/*.json")
        self.base_url = self.config.get("base_url", "").rstrip("/")
        self.source_name = self.config.get("source_name", self.config.get("name", "Hyrax source"))
        self.provider = self.config.get("provider", "")
        self.publisher = self.config.get("publisher", self.provider)
        self.source_id_prefix = self.config.get("source_id_prefix", "")
        self.default_language = self.config.get("default_language", "eng")
        self.default_resource_class = self.config.get("default_resource_class", "Other")
        self.default_resource_type = self.config.get("default_resource_type", "")
        self.default_spatial_coverage = self.config.get("default_spatial_coverage", "")
        self.default_bbox = self.config.get("default_bbox", "")
        self.default_geonames = self.config.get("default_geonames", "")
        self.default_code = self.config.get("code", "")
        self.default_member_of = self.config.get("member_of", "")
        self.default_is_part_of = self.config.get("is_part_of", self.default_code)
        self.manifest_url_template = self.config.get("manifest_url_template", "")
        self.iiif_url_template = self.config.get("iiif_url_template", "")
        self.thumbnail_url_template = self.config.get("thumbnail_url_template", "")
        self.resolve_linked_labels = self.config.get(
            "resolve_linked_labels",
            self.config.get("resolve_creator_labels", True),
        )
        self.linked_data_timeout = int(self.config.get("linked_data_timeout", 15))
        cache_path = self.config.get("linked_data_cache_json")
        self.linked_data_cache_path = Path(cache_path) if cache_path else None
        self.linked_data_cache = {}

    def load_reference_data(self):
        super().load_reference_data()
        self.hyrax_load_linked_data_cache()

    def fetch(self):
        root = self.input_json_root.expanduser()
        if not root.exists():
            raise FileNotFoundError(f"[HYRAX] input_json_root not found: {root}")

        json_files = sorted(path for path in root.glob(self.input_json_glob) if path.is_file())
        if not json_files:
            raise FileNotFoundError(
                f"[HYRAX] No JSON files matched {self.input_json_glob} under {root}"
            )

        print(f"[HYRAX] Found {len(json_files)} local JSON records under {root}")

        for json_path in json_files:
            try:
                with json_path.open("r", encoding="utf-8") as handle:
                    record = json.load(handle)
            except Exception as exc:
                yield f"[HYRAX] Error reading {json_path}: {exc}"
                continue

            if not isinstance(record, dict):
                yield f"[HYRAX] Skipping non-object JSON at {json_path}"
                continue

            relative_path = json_path.relative_to(root).as_posix()
            record["_source_file"] = json_path.name
            record["_relative_path"] = relative_path
            record["_json_url"] = self.hyrax_build_url(relative_path)
            record["_landing_url"] = record["_json_url"][:-5] if record["_json_url"].endswith(".json") else ""
            yield record

    def parse(self, raw_data):
        return [item for item in raw_data if isinstance(item, dict)]

    def flatten(self, harvested_metadata):
        return harvested_metadata

    def build_dataframe(self, records):
        if not records:
            print("[HYRAX] No records found. Returning an empty dataframe.")
            return pd.DataFrame(columns=self.EMPTY_PRIMARY_COLUMNS)

        mapped_rows = [self.hyrax_build_schema_row(record) for record in records]
        return pd.DataFrame(mapped_rows)

    def derive_fields(self, df):
        df = super().derive_fields(df)
        if "Temporal Coverage" not in df.columns:
            df["Temporal Coverage"] = ""

        df["Index Year"] = df["Temporal Coverage"].apply(self.hyrax_extract_index_year)
        df["Date Range"] = df.apply(
            lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
            axis=1,
        )
        return df

    def add_defaults(self, df):
        df = super().add_defaults(df)

        if "Language" not in df.columns:
            df["Language"] = self.default_language
        else:
            df["Language"] = df["Language"].replace("", self.default_language)

        if "Provider" not in df.columns:
            df["Provider"] = self.provider
        else:
            df["Provider"] = df["Provider"].replace("", self.provider)

        if "Publisher" not in df.columns:
            df["Publisher"] = self.publisher
        else:
            df["Publisher"] = df["Publisher"].replace("", self.publisher)

        if "Resource Class" not in df.columns:
            df["Resource Class"] = self.default_resource_class
        else:
            df["Resource Class"] = df["Resource Class"].replace("", self.default_resource_class)

        if "Resource Type" not in df.columns and self.default_resource_type:
            df["Resource Type"] = self.default_resource_type
        elif self.default_resource_type:
            df["Resource Type"] = df["Resource Type"].replace("", self.default_resource_type)

        df["Code"] = df.get("Code", "").replace("", self.default_code)
        df["Member Of"] = df.get("Member Of", "").replace("", self.default_member_of)
        df["Is Part Of"] = df.get("Is Part Of", "").replace("", self.default_is_part_of)
        df["Spatial Coverage"] = df.get("Spatial Coverage", "").replace("", self.default_spatial_coverage)
        df["Bounding Box"] = df.get("Bounding Box", "").replace("", self.default_bbox)
        df["GeoNames"] = df.get("GeoNames", "").replace("", self.default_geonames)

        return df

    def add_provenance(self, df):
        df = super().add_provenance(df)

        today = time.strftime("%Y-%m-%d")
        df["Website Platform"] = "Hyrax"
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = "py_hyrax"
        df["Endpoint Description"] = "Hyrax JSON"
        df["Endpoint URL"] = self.base_url
        df["Provenance"] = (
            f"The metadata for this resource was last retrieved from {self.source_name} on {today}."
        )

        return df

    def clean(self, df):
        df = super().clean(df)
        return df

    def validate(self, df):
        df = super().validate(df)
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        self.hyrax_save_linked_data_cache()
        return super().write_outputs(primary_df, distributions_df)

    def hyrax_build_schema_row(self, record):
        title_values = self.hyrax_extract_values(record.get("title"))
        alternative_title_values = self.hyrax_extract_values(record.get("alternative"))
        description_values = self.hyrax_extract_values(record.get("description"))
        abstract_values = self.hyrax_extract_values(record.get("abstract"))
        keyword_values = self.hyrax_extract_values(record.get("keyword"))
        subject_ids = self.hyrax_extract_values(record.get("subject"))
        subject_labels = self.hyrax_resolve_linked_values(subject_ids)
        temporal_values = self.hyrax_extract_values(record.get("date"))
        identifier_values = self.hyrax_extract_values(record.get("identifier"))
        language_values = self.hyrax_extract_values(record.get("language"))
        location_values = self.hyrax_extract_values(record.get("location"))
        publication_place_values = self.hyrax_extract_values(record.get("publication_place"))
        local_collection_values = self.hyrax_extract_values(record.get("local_collection_name"))
        license_values = self.hyrax_extract_values(record.get("license"))
        rights_statement_values = self.hyrax_extract_values(record.get("rights_statement"))
        rights_holder_values = self.hyrax_extract_values(record.get("rights_holder"))
        use_restrictions_values = self.hyrax_extract_values(record.get("use_restrictions"))
        work_type_values = self.hyrax_extract_values(record.get("workType"))
        resource_type_value = self.hyrax_normalize_resource_type(record.get("resource_type"))
        creator_ids = self.hyrax_role_values(record)
        creator_labels = self.hyrax_resolve_linked_values(creator_ids)

        title = title_values[0] if title_values else ""
        extra_titles = title_values[1:] + alternative_title_values
        language_codes = [self.hyrax_language_code(value) for value in language_values if value]
        geonames_values = self.hyrax_unique(location_values + publication_place_values)
        landing_url = record.get("_landing_url", "")
        replaces_url = self.hyrax_clean_scalar(record.get("replaces_url"))
        info_urls = self.hyrax_unique([landing_url or replaces_url])
        manifest_url = self.hyrax_render_template(self.manifest_url_template, record)
        if not manifest_url and landing_url:
            manifest_url = f"{landing_url}/manifest.json"
        identifier_output = self.hyrax_unique(identifier_values + ([landing_url] if landing_url else []))

        resource_class, resource_type = self.hyrax_resource_labels(
            work_type_values=work_type_values,
            resource_type=resource_type_value,
        )

        return {
            "ID": self.hyrax_record_id(record),
            "Title": title,
            "Alternative Title": "|".join(extra_titles),
            "Description": "|".join(description_values + abstract_values),
            "Language": "|".join(self.hyrax_unique(language_codes)),
            "Creator": "|".join(creator_labels),
            "Creator ID": "|".join(creator_ids),
            "Publisher": self.publisher,
            "Provider": self.provider,
            "Resource Class": resource_class,
            "Resource Type": resource_type,
            "Subject": "|".join(subject_labels),
            "Keyword": "|".join(keyword_values),
            "Local Collection": "|".join(local_collection_values),
            "Temporal Coverage": "|".join(temporal_values),
            "Date Issued": self.hyrax_first_date(record),
            "Spatial Coverage": self.default_spatial_coverage,
            "Bounding Box": self.default_bbox,
            "GeoNames": "|".join(geonames_values),
            "Rights": "|".join(rights_statement_values + use_restrictions_values),
            "License": "|".join(license_values),
            "Rights Holder": "|".join(rights_holder_values),
            "Identifier": "|".join(identifier_output),
            "Code": self.default_code,
            "Member Of": self.default_member_of,
            "Is Part Of": self.default_is_part_of,
            "information": info_urls,
            "manifest": manifest_url,
            "thumbnail": self.hyrax_render_template(self.thumbnail_url_template, record),
            "iiif": self.hyrax_render_template(self.iiif_url_template, record),
        }

    def hyrax_record_id(self, record):
        record_id = self.hyrax_clean_scalar(record.get("id"))
        if self.source_id_prefix:
            return f"{self.source_id_prefix}-{record_id}"
        return record_id

    def hyrax_build_url(self, relative_path):
        if not self.base_url:
            return ""
        return urljoin(f"{self.base_url}/", relative_path)

    def hyrax_extract_values(self, value):
        values = []
        if value is None:
            return values

        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    raw = item.get("label") or item.get("name") or item.get("id")
                else:
                    raw = item
                clean = self.hyrax_clean_scalar(raw)
                if clean:
                    values.append(clean)
            return self.hyrax_unique(values)

        if isinstance(value, dict):
            raw = value.get("label") or value.get("name") or value.get("id")
            clean = self.hyrax_clean_scalar(raw)
            return [clean] if clean else []

        clean = self.hyrax_clean_scalar(value)
        return [clean] if clean else []

    def hyrax_role_values(self, record):
        values = []
        for field in self.ROLE_FIELDS:
            values.extend(self.hyrax_extract_values(record.get(field)))
        return self.hyrax_unique(values)

    def hyrax_resource_labels(self, work_type_values, resource_type):
        for value in work_type_values:
            if value in self.WORK_TYPE_LABELS:
                label = self.WORK_TYPE_LABELS[value]
                return label, label

        if resource_type:
            key = resource_type.lower().replace(" ", "")
            resource_class = self.RESOURCE_CLASS_BY_TYPE.get(key, self.default_resource_class)
            return resource_class, resource_type

        return self.default_resource_class, self.default_resource_type

    def hyrax_normalize_resource_type(self, value):
        clean = self.hyrax_clean_scalar(value)
        if not clean:
            return ""
        tail = clean.rstrip("/").rsplit("/", 1)[-1]
        words = re.sub(r"([a-z])([A-Z])", r"\1 \2", tail).replace("_", " ")
        return words.strip().title()

    def hyrax_first_date(self, record):
        issued_values = self.hyrax_extract_values(record.get("issued"))
        if issued_values:
            return issued_values[0]

        temporal_values = self.hyrax_extract_values(record.get("date"))
        if temporal_values:
            return temporal_values[0]

        return self.hyrax_clean_scalar(record.get("date_uploaded")).split("T")[0]

    def hyrax_extract_index_year(self, value):
        match = re.search(r"(1[0-9]{3}|20[0-9]{2})", str(value or ""))
        return match.group(1) if match else ""

    def hyrax_language_code(self, value):
        clean = self.hyrax_clean_scalar(value)
        if not clean:
            return ""
        if re.fullmatch(r"[a-z]{3}", clean):
            return clean
        tail = clean.rstrip("/").rsplit("/", 1)[-1].lower()
        return tail if re.fullmatch(r"[a-z]{3}", tail) else clean

    def hyrax_render_template(self, template, record):
        if not template:
            return ""
        return template.format(
            id=self.hyrax_clean_scalar(record.get("id")),
            json_url=record.get("_json_url", ""),
            landing_url=record.get("_landing_url", ""),
            relative_path=record.get("_relative_path", ""),
            source_file=record.get("_source_file", ""),
        )

    def hyrax_clean_scalar(self, value):
        if value is None:
            return ""
        return str(value).strip()

    def hyrax_unique(self, values):
        seen = set()
        unique = []
        for value in values:
            clean = self.hyrax_clean_scalar(value)
            if not clean or clean in seen:
                continue
            seen.add(clean)
            unique.append(clean)
        return unique

    def hyrax_load_linked_data_cache(self):
        if not self.linked_data_cache_path or not self.linked_data_cache_path.exists():
            return

        try:
            with self.linked_data_cache_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception as exc:
            print(f"[HYRAX] Warning: could not load linked data cache: {exc}")
            return

        if isinstance(payload, dict):
            self.linked_data_cache = {
                str(key): self.hyrax_clean_scalar(value) for key, value in payload.items()
            }

    def hyrax_save_linked_data_cache(self):
        if not self.linked_data_cache_path:
            return

        try:
            self.linked_data_cache_path.parent.mkdir(parents=True, exist_ok=True)
            with self.linked_data_cache_path.open("w", encoding="utf-8") as handle:
                json.dump(self.linked_data_cache, handle, indent=2, ensure_ascii=False, sort_keys=True)
        except Exception as exc:
            print(f"[HYRAX] Warning: could not save linked data cache: {exc}")

    def hyrax_resolve_linked_values(self, values):
        resolved = []
        for value in values:
            label = self.hyrax_resolve_linked_data_label(value)
            resolved.append(label or value)
        return self.hyrax_unique(resolved)

    def hyrax_resolve_linked_data_label(self, uri):
        clean_uri = self.hyrax_clean_scalar(uri)
        if not clean_uri:
            return ""

        if clean_uri in self.linked_data_cache:
            return self.linked_data_cache[clean_uri]

        if not self.resolve_linked_labels or not clean_uri.startswith(("http://", "https://")):
            self.linked_data_cache[clean_uri] = ""
            return ""

        candidate_urls = self.hyrax_linked_data_urls(clean_uri)
        headers = {
            "Accept": "application/ld+json, application/json;q=0.9, text/plain;q=0.1",
            "User-Agent": "harvester-api hyrax linked-data resolver",
        }

        label = ""
        for candidate_url in candidate_urls:
            try:
                response = requests.get(candidate_url, headers=headers, timeout=self.linked_data_timeout)
                response.raise_for_status()
                payload = response.json()
            except Exception:
                continue

            label = self.hyrax_extract_label_from_payload(payload, clean_uri)
            if label:
                break

        self.linked_data_cache[clean_uri] = label
        return label

    def hyrax_linked_data_urls(self, uri):
        urls = []
        if uri.endswith(".json") or uri.endswith(".jsonld"):
            urls.append(uri)
        else:
            urls.append(f"{uri}.json")
            urls.append(uri)
        return urls

    def hyrax_extract_label_from_payload(self, payload, target_uri):
        nodes = []
        if isinstance(payload, list):
            nodes = [item for item in payload if isinstance(item, dict)]
        elif isinstance(payload, dict):
            if isinstance(payload.get("@graph"), list):
                nodes = [item for item in payload["@graph"] if isinstance(item, dict)]
            else:
                nodes = [payload]

        preferred_nodes = [
            node for node in nodes if self.hyrax_clean_scalar(node.get("@id")) == target_uri
        ]

        for node_group in (preferred_nodes, nodes):
            for node in node_group:
                label = self.hyrax_extract_label_from_node(node)
                if label:
                    return label

        return ""

    def hyrax_extract_label_from_node(self, node):
        label_keys = [
            "http://www.loc.gov/mads/rdf/v1#authoritativeLabel",
            "http://www.w3.org/2004/02/skos/core#prefLabel",
            "http://www.w3.org/2000/01/rdf-schema#label",
            "madsrdf:authoritativeLabel",
            "skos:prefLabel",
            "rdfs:label",
            "authoritativeLabel",
            "prefLabel",
            "label",
        ]

        for key in label_keys:
            if key not in node:
                continue

            values = node[key]
            if not isinstance(values, list):
                values = [values]

            for value in values:
                if isinstance(value, dict):
                    text = (
                        value.get("@value")
                        or value.get("label")
                        or value.get("name")
                        or value.get("value")
                    )
                else:
                    text = value

                clean = self.hyrax_clean_scalar(text)
                if clean:
                    return clean

        return ""
