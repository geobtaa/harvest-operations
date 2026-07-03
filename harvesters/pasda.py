import csv
import hashlib
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin
from xml.etree import ElementTree as ET

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from harvesters.base import BaseHarvester


LOGGER = logging.getLogger(__name__)


class PasdaHarvester(BaseHarvester):
    def __init__(self, config):
        config = dict(config)
        config.setdefault("build_uploads", True)
        config.setdefault("metadata_base_url", "https://www.pasda.psu.edu/metadata/")
        config.setdefault("download_base_url", "https://www.pasda.psu.edu/download/")
        config.setdefault("source_manifest", "metadata_directory")
        config.setdefault("cache_dir", "inputs/pasda/metadata_xml")
        config.setdefault("output_dir", "outputs/pasda")
        config.setdefault("incremental", True)
        config.setdefault("timeout", 30)
        config.setdefault("user_agent", "harvest-operations PASDA metadata harvester")
        super().__init__(config)
        self.manifest_rows = []
        self.normalized_records = []
        self.error_rows = []
        self.profile_summary = []

    def fetch(self):
        session = build_pasda_session(self.config.get("user_agent", "harvest-operations"))
        metadata_base_url = self.config["metadata_base_url"]
        timeout = int(self.config.get("timeout", 30))
        harvested_at = utc_now()

        LOGGER.info("Fetching PASDA metadata directory listing: %s", metadata_base_url)
        response = session.get(metadata_base_url, timeout=timeout)
        response.raise_for_status()

        manifest_rows = parse_metadata_directory_listing(
            response.text,
            metadata_base_url,
            harvested_at=harvested_at,
        )
        max_records = self.config.get("max_records")
        if max_records:
            manifest_rows = manifest_rows[: int(max_records)]

        cache_dir = Path(self.config["cache_dir"])
        cache_dir.mkdir(parents=True, exist_ok=True)

        fetched_rows = []
        for row in manifest_rows:
            fetched_rows.append(
                fetch_and_cache_metadata_xml(
                    row,
                    session=session,
                    cache_dir=cache_dir,
                    timeout=timeout,
                    incremental=bool(self.config.get("incremental", True)),
                )
            )

        LOGGER.info("Prepared PASDA manifest with %s XML records", len(fetched_rows))
        return fetched_rows

    def parse(self, raw_data):
        normalized_records = []
        manifest_rows = []
        error_rows = []

        for row in raw_data:
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

        manifest_path = output_dir / f"{today}_pasda_metadata_manifest.csv"
        normalized_jsonl_path = output_dir / f"{today}_pasda_normalized_records.jsonl"
        normalized_csv_path = output_dir / f"{today}_pasda_normalized_records.csv"
        errors_path = output_dir / f"{today}_pasda_error_report.csv"
        profile_summary_path = output_dir / f"{today}_pasda_profile_summary.csv"

        write_csv_rows(manifest_path, self.manifest_rows)
        write_jsonl(normalized_jsonl_path, self.normalized_records)
        primary_df.to_csv(normalized_csv_path, index=False, encoding="utf-8")
        write_csv_rows(errors_path, self.error_rows)
        write_csv_rows(profile_summary_path, self.profile_summary)

        results = {
            "manifest_csv": str(manifest_path),
            "normalized_jsonl": str(normalized_jsonl_path),
            "normalized_csv": str(normalized_csv_path),
            "error_report_csv": str(errors_path),
            "profile_summary_csv": str(profile_summary_path),
        }
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


def fetch_and_cache_metadata_xml(
    row: dict[str, Any],
    session: requests.Session,
    cache_dir: Path,
    timeout: int,
    incremental: bool,
) -> dict[str, Any]:
    row = dict(row)
    cache_path = cache_dir / safe_metadata_filename(row["metadata_filename"])
    cached_content = read_cached_xml_if_usable(cache_path, row, incremental)

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
) -> bytes | None:
    if not incremental or not cache_path.exists():
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
            "title": first_text(root, ["idinfo/citation/citeinfo/title"]),
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
            "spatial_reference": first_text(root, ["spref/horizsys/planar/mapproj/mapprojn"]),
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
            "data_format": first_text(root, ["distinfo/stdorder/digform/digtinfo/formname"]),
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
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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
