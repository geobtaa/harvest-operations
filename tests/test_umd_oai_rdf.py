from pathlib import Path

import pytest

from utils.umd_oai_rdf import (
    UmdOaiRdfError,
    filter_and_enrich_umd_records,
    fcrepo_id_from_uri,
    load_umd_rdf_records,
    parse_umd_rdf_page,
)


MAPS_AGGREGATION = "http://vocab.lib.umd.edu/set#marylandMaps"
FCREPO_URI = (
    "https://fcrepo.lib.umd.edu/fcrepo/rest/dc/2023/2/c1/91/18/3e/"
    "c191183e-76b0-43ee-8221-4108b3521c3a"
)
OAI_IDENTIFIER = "oai:fcrepo.lib.umd.edu:1903.1/12533"


def rdf_page(*, aggregation_uri: str = MAPS_AGGREGATION) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
      xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
      xmlns:dcterms="http://purl.org/dc/terms/"
      xmlns:ore="http://www.openarchives.org/ore/terms/">
      <ListRecords>
        <record>
          <header><identifier>{OAI_IDENTIFIER}</identifier></header>
          <metadata>
            <rdf:RDF>
              <rdf:Description rdf:about="{FCREPO_URI}#subject">
                <rdf:type rdf:resource="http://example.com/Subject" />
              </rdf:Description>
              <rdf:Description rdf:about="{FCREPO_URI}">
                <dcterms:identifier>hdl:1903.1/12533</dcterms:identifier>
                <rdf:type rdf:resource="http://vocab.lib.umd.edu/model#Item" />
                <ore:isAggregatedBy rdf:resource="{aggregation_uri}" />
              </rdf:Description>
            </rdf:RDF>
          </metadata>
        </record>
      </ListRecords>
    </OAI-PMH>"""


def test_parse_umd_rdf_page_selects_main_item_and_aggregation() -> None:
    records = parse_umd_rdf_page(rdf_page())

    assert records[OAI_IDENTIFIER].fcrepo_uri == FCREPO_URI
    assert records[OAI_IDENTIFIER].aggregation_uris == (MAPS_AGGREGATION,)


def test_fcrepo_id_from_uri_builds_public_identifier() -> None:
    assert fcrepo_id_from_uri(FCREPO_URI) == (
        "fcrepo:dc:2023:2:c1:91:18:3e:c191183e-76b0-43ee-8221-4108b3521c3a"
    )


def test_filter_and_enrich_umd_records_keeps_maps_and_builds_manifest() -> None:
    rdf_records = parse_umd_rdf_page(rdf_page())
    records = [
        {"oai_identifier": OAI_IDENTIFIER, "fields": {"dc:title": ["Maryland"]}},
    ]

    selected = filter_and_enrich_umd_records(
        records,
        rdf_records,
        aggregation_uri=MAPS_AGGREGATION,
        manifest_base_url="https://iiif.lib.umd.edu/manifests",
    )

    assert selected == [
        {
            "oai_identifier": OAI_IDENTIFIER,
            "fields": {"dc:title": ["Maryland"]},
            "umd_fcrepo_uri": FCREPO_URI,
            "umd_fcrepo_id": (
                "fcrepo:dc:2023:2:c1:91:18:3e:"
                "c191183e-76b0-43ee-8221-4108b3521c3a"
            ),
            "manifest": (
                "https://iiif.lib.umd.edu/manifests/fcrepo:dc:2023:2:c1:91:18:3e:"
                "c191183e-76b0-43ee-8221-4108b3521c3a/manifest"
            ),
        }
    ]


def test_filter_and_enrich_umd_records_rejects_incomplete_companion_snapshot() -> None:
    with pytest.raises(UmdOaiRdfError, match="Companion RDF is missing 1"):
        filter_and_enrich_umd_records(
            [{"oai_identifier": OAI_IDENTIFIER}],
            {},
            aggregation_uri=MAPS_AGGREGATION,
            manifest_base_url="https://iiif.lib.umd.edu/manifests",
        )


def test_load_umd_rdf_records_reads_set_snapshot(tmp_path: Path) -> None:
    set_dir = tmp_path / "maryland_and_historical_collections"
    set_dir.mkdir()
    (set_dir / "0001.xml").write_text(rdf_page(), encoding="utf-8")

    records = load_umd_rdf_records(
        tmp_path,
        ["maryland_and_historical_collections"],
    )

    assert list(records) == [OAI_IDENTIFIER]
