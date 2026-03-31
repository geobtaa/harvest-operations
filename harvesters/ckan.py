import re
import time
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table
from utils.temporal_fields import create_date_range, infer_temporal_coverage_from_title


class CkanHarvester(BaseHarvester):
    def __init__(self, config):
        super().__init__(config)
        self.base_url = self.config["base_url"].rstrip("/")
        self.endpoint_url = self.config.get(
            "endpoint_url",
            f"{self.base_url}/api/3/action/package_search",
        )
        self.rows = int(self.config.get("rows", 100))
        self.timeout = int(self.config.get("timeout", 60))

    def fetch(self):
        session = requests.Session()
        session.headers.update(
            {"User-Agent": self.config.get("user_agent", "harvester-api ckan")}
        )

        start = 0
        total = None

        while True:
            params = {"rows": self.rows, "start": start}
            if self.config.get("q"):
                params["q"] = self.config["q"]
            if self.config.get("fq"):
                params["fq"] = self.config["fq"]

            response = session.get(self.endpoint_url, params=params, timeout=self.timeout)
            response.raise_for_status()

            payload = response.json()
            if not payload.get("success"):
                raise ValueError(f"CKAN API returned unsuccessful response for {self.endpoint_url}")

            result = payload.get("result", {})
            records = result.get("results", [])
            total = result.get("count", total if total is not None else 0)

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
        return self.ckan_map_to_schema(df)

    def derive_fields(self, df):
        df = super().derive_fields(df)
        if df.empty:
            return df

        df["Title"] = df["Alternative Title"].fillna("")
        title_suffix = self.config.get("title_suffix", "").strip()
        if title_suffix:
            df["Title"] = df["Title"].apply(lambda value: f"{value}{title_suffix}" if value else value)

        df = (
            df.pipe(self.ckan_fill_spatial_fields)
            .pipe(self.ckan_temporal_coverage)
            .pipe(self.ckan_format_date_ranges)
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
        df["Harvest Workflow"] = self.config.get("harvest_workflow", "py_ckan")
        df["Endpoint URL"] = self.endpoint_url
        df["Endpoint Description"] = "CKAN API (package_search)"
        df["Provenance"] = (
            f"The metadata for this resource was last retrieved from {site_title} on {today}."
        )

        return df

    def clean(self, df):
        return super().clean(df)

    def validate(self, df):
        return super().validate(df)

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        return super().write_outputs(primary_df, distributions_df)

    def ckan_map_to_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        schema_df = pd.DataFrame()
        empty_series = pd.Series([""] * len(df), index=df.index, dtype="object")

        schema_df["Alternative Title"] = df.get("title", "")
        schema_df["Description"] = df.get("notes", "").apply(self._strip_markup)
        schema_df["Creator"] = df.apply(self._get_creator, axis=1)
        schema_df["Publisher"] = self.config.get("publisher", self.config.get("site_title", ""))
        schema_df["Keyword"] = df.apply(
            lambda row: self._join_values(self._extract_named_values(row.get("tags"))),
            axis=1,
        )
        schema_df["Subject"] = df.apply(
            lambda row: self._join_values(self._extract_named_values(row.get("groups"))),
            axis=1,
        )
        schema_df["Date Issued"] = df.get("metadata_created", empty_series).apply(self._split_date)
        schema_df["Date Modified"] = df.get("metadata_modified", empty_series).apply(self._split_date)
        schema_df["License"] = df.apply(
            lambda row: row.get("license_url") or row.get("license_title") or "",
            axis=1,
        )
        schema_df["Rights"] = df.get("license_title", empty_series)
        schema_df["Access Rights"] = df.apply(self._get_access_rights, axis=1)
        schema_df["ID"] = df.get("id", empty_series)
        schema_df["Identifier"] = df.get("name", empty_series)
        schema_df["Bounding Box"] = df.apply(self._get_bounding_box, axis=1)
        schema_df["information"] = df.apply(self._build_information_url, axis=1)
        schema_df["thumbnail"] = df.apply(
            lambda row: row.get("image_display_url") or self._get_extra(row, "image_url") or "",
            axis=1,
        )

        resource_columns = df.apply(self._extract_resource_columns, axis=1, result_type="expand")
        schema_df = pd.concat([schema_df, resource_columns], axis=1)

        return schema_df

    def ckan_temporal_coverage(self, df):
        df["Temporal Coverage"] = df.apply(infer_temporal_coverage_from_title, axis=1)
        return df

    def ckan_format_date_ranges(self, df):
        df["Date Range"] = df.apply(
            lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
            axis=1,
        )
        return df

    def _build_information_url(self, row: pd.Series) -> str:
        name = row.get("name", "")
        if not name:
            return ""
        return urljoin(f"{self.base_url}/", f"dataset/{name}")

    def _get_creator(self, row: pd.Series) -> str:
        organization = row.get("organization")
        if isinstance(organization, dict) and organization.get("title"):
            return organization["title"]

        for key in ("author", "maintainer", "data_steward_name"):
            value = row.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        return ""

    def _get_access_rights(self, row: pd.Series) -> str:
        if row.get("private") is True:
            return "Restricted"

        access_level = str(
            row.get("access_level") or self._get_extra(row, "access_level") or ""
        ).strip().lower()
        if access_level and access_level != "public":
            return "Restricted"

        return "Public"

    def ckan_fill_spatial_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        creator_places = df["Creator"].apply(self._derive_place_from_creator)

        df["Spatial Coverage"] = creator_places.apply(self._place_to_spatial_coverage)
        df["Bounding Box"] = df.apply(
            lambda row: row["Bounding Box"]
            if isinstance(row.get("Bounding Box"), str) and row.get("Bounding Box").strip()
            else self._place_to_bounding_box(self._derive_place_from_spatial_coverage(row.get("Spatial Coverage", ""))),
            axis=1,
        )
        return df

    def _get_bounding_box(self, row: pd.Series) -> str:
        raw_spatial = row.get("spatial") or self._get_extra(row, "spatial")
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

    @staticmethod
    def _derive_place_from_creator(creator: Any) -> str:
        lowered_creator = str(creator or "").lower()
        if "allegheny county" in lowered_creator:
            return "allegheny_county"
        if "pittsburgh" in lowered_creator:
            return "pittsburgh"
        return "pennsylvania"

    @staticmethod
    def _place_to_spatial_coverage(place: str) -> str:
        if place == "allegheny_county":
            return "Pennsylvania--Allegheny County|Pennsylvania"
        if place == "pittsburgh":
            return "Pennsylvania--Pittsburgh|Pennsylvania"
        return "Pennsylvania"

    @staticmethod
    def _place_to_bounding_box(place: str) -> str:
        if place == "pittsburgh":
            return "-80.0955,40.3615,-79.8657,40.5012"
        return "-80.36,40.19,-79.69,40.67"

    @staticmethod
    def _derive_place_from_spatial_coverage(spatial_coverage: Any) -> str:
        lowered_spatial = str(spatial_coverage or "").lower()
        if "allegheny county" in lowered_spatial:
            return "allegheny_county"
        if "pittsburgh" in lowered_spatial:
            return "pittsburgh"
        return "pennsylvania"

    def _get_extra(self, row: pd.Series, key: str) -> Any:
        extras = row.get("extras")
        if not isinstance(extras, list):
            return None

        for extra in extras:
            if isinstance(extra, dict) and extra.get("key") == key:
                return extra.get("value")
        return None

    def _extract_resource_columns(self, row: pd.Series) -> pd.Series:
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

    @staticmethod
    def _extract_named_values(values) -> list[str]:
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

    @staticmethod
    def _join_values(values: list[str]) -> str:
        cleaned = [value for value in values if value]
        return "|".join(cleaned)

    @staticmethod
    def _split_date(value: Any) -> str:
        if not isinstance(value, str):
            return ""
        return value.split("T", 1)[0]

    @staticmethod
    def _strip_markup(value: Any) -> str:
        if not isinstance(value, str):
            return ""
        value = re.sub(r"<!--.*?-->", "", value, flags=re.DOTALL)
        return value.strip()
