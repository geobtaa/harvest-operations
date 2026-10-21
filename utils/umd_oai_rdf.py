"""Use UMD's companion OAI-RDF feed to select collections and build IIIF URLs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import xml.etree.ElementTree as ET
from urllib.parse import unquote, urlparse

from utils.oai_pmh import slugify


NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dcterms": "http://purl.org/dc/terms/",
    "ore": "http://www.openarchives.org/ore/terms/",
}
RDF_RESOURCE = f"{{{NS['rdf']}}}resource"
RDF_ABOUT = f"{{{NS['rdf']}}}about"
UMD_ITEM_TYPE = "http://vocab.lib.umd.edu/model#Item"
FCREPO_REST_PREFIX = "/fcrepo/rest/"


class UmdOaiRdfError(ValueError):
    """Raised when the companion RDF cannot safely identify the OAI records."""


@dataclass(frozen=True)
class UmdRdfRecord:
    oai_identifier: str
    fcrepo_uri: str
    aggregation_uris: tuple[str, ...]


def parse_umd_rdf_page(xml_text: str) -> dict[str, UmdRdfRecord]:
    """Parse one OAI ListRecords/GetRecord RDF response, keyed by OAI identifier."""
    root = ET.fromstring(xml_text)
    parsed: dict[str, UmdRdfRecord] = {}

    for record_el in root.findall(".//oai:record", NS):
        header_el = record_el.find("oai:header", NS)
        if header_el is None or header_el.attrib.get("status") == "deleted":
            continue

        identifier_el = header_el.find("oai:identifier", NS)
        oai_identifier = (identifier_el.text or "").strip() if identifier_el is not None else ""
        if not oai_identifier:
            raise UmdOaiRdfError("UMD RDF record is missing its OAI identifier.")

        rdf_root = record_el.find("oai:metadata/rdf:RDF", NS)
        if rdf_root is None:
            raise UmdOaiRdfError(
                f"UMD RDF record {oai_identifier!r} has no rdf:RDF metadata."
            )

        description = _select_item_description(rdf_root)
        fcrepo_uri = description.attrib.get(RDF_ABOUT, "") if description is not None else ""
        aggregation_uris = tuple(
            dict.fromkeys(
                value
                for element in (
                    description.findall("ore:isAggregatedBy", NS)
                    if description is not None
                    else []
                )
                if (value := element.attrib.get(RDF_RESOURCE, "").strip())
            )
        )
        candidate = UmdRdfRecord(
            oai_identifier=oai_identifier,
            fcrepo_uri=fcrepo_uri,
            aggregation_uris=aggregation_uris,
        )
        existing = parsed.get(oai_identifier)
        if existing is not None and existing != candidate:
            raise UmdOaiRdfError(
                f"Conflicting UMD RDF records found for {oai_identifier!r}."
            )
        parsed[oai_identifier] = candidate

    return parsed


def load_umd_rdf_records(
    download_dir: str | Path,
    set_specs: list[str],
) -> dict[str, UmdRdfRecord]:
    """Load all locally staged RDF pages for the configured OAI sets."""
    root = Path(download_dir)
    records: dict[str, UmdRdfRecord] = {}
    missing_sets: list[str] = []

    for set_spec in set_specs:
        set_dir = root / slugify(set_spec)
        xml_paths = sorted(set_dir.glob("*.xml")) if set_dir.is_dir() else []
        if not xml_paths:
            missing_sets.append(set_spec)
            continue

        for xml_path in xml_paths:
            page_records = parse_umd_rdf_page(xml_path.read_text(encoding="utf-8"))
            for identifier, candidate in page_records.items():
                existing = records.get(identifier)
                if existing is not None and existing != candidate:
                    raise UmdOaiRdfError(
                        f"Conflicting UMD RDF records found for {identifier!r}."
                    )
                records[identifier] = candidate

    if missing_sets:
        raise FileNotFoundError(
            "UMD companion RDF XML was not found for set(s): "
            f"{', '.join(missing_sets)}. Expected files under {root}/<set-spec>/. "
            "Run the OAI download stage before harvesting."
        )
    return records


def filter_and_enrich_umd_records(
    records: list[dict],
    rdf_records: dict[str, UmdRdfRecord],
    *,
    aggregation_uri: str,
    manifest_base_url: str,
) -> list[dict]:
    """Keep one UMD aggregation and attach a derived IIIF manifest to each item."""
    missing = sorted(
        {
            str(record.get("oai_identifier", "")).strip()
            for record in records
            if str(record.get("oai_identifier", "")).strip() not in rdf_records
        }
    )
    if missing:
        preview = ", ".join(missing[:5])
        suffix = "..." if len(missing) > 5 else ""
        raise UmdOaiRdfError(
            f"Companion RDF is missing {len(missing)} Dublin Core record(s): "
            f"{preview}{suffix}"
        )

    selected: list[dict] = []
    for record in records:
        identifier = str(record.get("oai_identifier", "")).strip()
        rdf_record = rdf_records[identifier]
        if aggregation_uri not in rdf_record.aggregation_uris:
            continue
        fcrepo_id = fcrepo_id_from_uri(rdf_record.fcrepo_uri)
        if not fcrepo_id:
            raise UmdOaiRdfError(
                f"Maryland Maps RDF record {identifier!r} has no usable Fedora item URI."
            )
        enriched = dict(record)
        enriched["umd_fcrepo_uri"] = rdf_record.fcrepo_uri
        enriched["umd_fcrepo_id"] = fcrepo_id
        enriched["manifest"] = umd_manifest_url(fcrepo_id, manifest_base_url)
        selected.append(enriched)
    return selected


def fcrepo_id_from_uri(uri: str) -> str:
    """Convert a UMD Fedora REST item URI into its public ``fcrepo:...`` ID."""
    path = unquote(urlparse(str(uri or "").strip()).path)
    if not path.startswith(FCREPO_REST_PREFIX):
        return ""
    segments = [segment for segment in path[len(FCREPO_REST_PREFIX) :].split("/") if segment]
    if not segments:
        return ""
    return "fcrepo:" + ":".join(segments)


def umd_manifest_url(fcrepo_id: str, manifest_base_url: str) -> str:
    base_url = str(manifest_base_url or "").strip().rstrip("/")
    return f"{base_url}/{fcrepo_id}/manifest" if base_url and fcrepo_id else ""


def _select_item_description(rdf_root: ET.Element) -> ET.Element | None:
    descriptions = rdf_root.findall("rdf:Description", NS)
    item_descriptions = [
        description
        for description in descriptions
        if any(
            type_el.attrib.get(RDF_RESOURCE, "") == UMD_ITEM_TYPE
            for type_el in description.findall("rdf:type", NS)
        )
    ]
    if len(item_descriptions) > 1:
        raise UmdOaiRdfError("UMD RDF metadata contains multiple item descriptions.")
    if item_descriptions:
        return item_descriptions[0]

    # Collection-like records may not use the Item class. Prefer the unfragmented
    # Fedora resource carrying a source identifier so these records can still be
    # indexed and safely rejected by the aggregation filter.
    candidates = [
        description
        for description in descriptions
        if "#" not in description.attrib.get(RDF_ABOUT, "")
        and description.find("dcterms:identifier", NS) is not None
    ]
    if len(candidates) > 1:
        raise UmdOaiRdfError("UMD RDF metadata contains multiple primary descriptions.")
    return candidates[0] if candidates else None
