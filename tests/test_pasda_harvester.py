import json
from pathlib import Path

import harvesters.pasda as pasda_module
import pandas as pd
from harvesters.base import BaseHarvester
from harvesters.pasda import (
    PasdaHarvester,
    build_pasda_aardvark_draft_dataframe,
    build_pasda_aardvark_draft_records,
    build_pasda_county_lookup,
    detect_metadata_profile,
    parse_metadata_directory_listing,
    parse_pasda_manifest_row,
    select_metadata_sample,
)


FGDC_XML = """<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <idinfo>
    <citation>
      <citeinfo>
        <origin>Pennsylvania Geological Survey</origin>
        <pubdate>20240115</pubdate>
        <title>Pennsylvania Bedrock Geology</title>
        <geoform>vector digital data</geoform>
        <onlink>https://www.pasda.psu.edu/download/geology/bedrock.zip</onlink>
      </citeinfo>
    </citation>
    <descript>
      <abstract>Statewide bedrock geology polygons.</abstract>
      <purpose>Planning and research.</purpose>
    </descript>
    <spdom>
      <bounding>
        <westbc>-80.52</westbc>
        <eastbc>-74.69</eastbc>
        <northbc>42.27</northbc>
        <southbc>39.72</southbc>
      </bounding>
    </spdom>
    <keywords>
      <theme><themekey>geology</themekey></theme>
      <place><placekey>Pennsylvania</placekey></place>
    </keywords>
    <accconst>None</accconst>
    <useconst>Use with citation.</useconst>
  </idinfo>
  <distinfo>
    <distrib><cntinfo><cntorgp><cntorg>PASDA</cntorg></cntorgp></cntinfo></distrib>
    <stdorder><digform><digtinfo><formname>Shapefile</formname></digtinfo></digform></stdorder>
  </distinfo>
  <spref>
    <horizsys>
      <cordsysn>
        <geogcsn>GCS_North_American_1983</geogcsn>
        <projcsn>NAD_1983_StatePlane_Pennsylvania_South_FIPS_3702_Feet</projcsn>
      </cordsysn>
      <planar><planci><plandu>survey feet</plandu></planci></planar>
      <geodetic>
        <horizdn>North American Datum of 1983</horizdn>
        <ellips>Geodetic Reference System 80</ellips>
      </geodetic>
    </horizsys>
  </spref>
  <metainfo>
    <metd>20240201</metd>
    <metstdn>FGDC Content Standard for Digital Geospatial Metadata</metstdn>
    <metstdv>FGDC-STD-001-1998</metstdv>
  </metainfo>
</metadata>
"""


ISO_XML = """<?xml version="1.0" encoding="UTF-8"?>
<gmd:MD_Metadata
  xmlns:gmd="http://www.isotc211.org/2005/gmd"
  xmlns:gco="http://www.isotc211.org/2005/gco">
  <gmd:fileIdentifier><gco:CharacterString>iso-1</gco:CharacterString></gmd:fileIdentifier>
  <gmd:dateStamp><gco:Date>2024-02-01</gco:Date></gmd:dateStamp>
  <gmd:metadataStandardName><gco:CharacterString>ISO 19115</gco:CharacterString></gmd:metadataStandardName>
  <gmd:identificationInfo>
    <gmd:MD_DataIdentification>
      <gmd:citation>
        <gmd:CI_Citation>
          <gmd:title><gco:CharacterString>Pennsylvania Imagery 2023</gco:CharacterString></gmd:title>
          <gmd:date>
            <gmd:CI_Date>
              <gmd:date><gco:Date>2023-06-01</gco:Date></gmd:date>
              <gmd:dateType><gmd:CI_DateTypeCode codeListValue="publication"/></gmd:dateType>
            </gmd:CI_Date>
          </gmd:date>
        </gmd:CI_Citation>
      </gmd:citation>
      <gmd:abstract><gco:CharacterString>Leaf-off imagery.</gco:CharacterString></gmd:abstract>
      <gmd:descriptiveKeywords>
        <gmd:MD_Keywords><gmd:keyword><gco:CharacterString>imagery</gco:CharacterString></gmd:keyword></gmd:MD_Keywords>
      </gmd:descriptiveKeywords>
      <gmd:extent>
        <gmd:EX_Extent>
          <gmd:geographicElement>
            <gmd:EX_GeographicBoundingBox>
              <gmd:westBoundLongitude><gco:Decimal>-80.5</gco:Decimal></gmd:westBoundLongitude>
              <gmd:eastBoundLongitude><gco:Decimal>-74.7</gco:Decimal></gmd:eastBoundLongitude>
              <gmd:southBoundLatitude><gco:Decimal>39.7</gco:Decimal></gmd:southBoundLatitude>
              <gmd:northBoundLatitude><gco:Decimal>42.3</gco:Decimal></gmd:northBoundLatitude>
            </gmd:EX_GeographicBoundingBox>
          </gmd:geographicElement>
        </gmd:EX_Extent>
      </gmd:extent>
    </gmd:MD_DataIdentification>
  </gmd:identificationInfo>
  <gmd:distributionInfo>
    <gmd:MD_Distribution>
      <gmd:transferOptions>
        <gmd:MD_DigitalTransferOptions>
          <gmd:onLine>
            <gmd:CI_OnlineResource>
              <gmd:linkage><gmd:URL>https://example.org/arcgis/rest/services/imagery/ImageServer</gmd:URL></gmd:linkage>
            </gmd:CI_OnlineResource>
          </gmd:onLine>
        </gmd:MD_DigitalTransferOptions>
      </gmd:transferOptions>
    </gmd:MD_Distribution>
  </gmd:distributionInfo>
</gmd:MD_Metadata>
"""


ARCGIS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <Esri><ArcGISFormat>1.0</ArcGISFormat></Esri>
  <dataIdInfo>
    <idCitation><resTitle>County Parcels</resTitle></idCitation>
    <idAbs>Parcel boundaries for a county.</idAbs>
    <searchKeys><keyword>parcels</keyword></searchKeys>
    <idPurp>Assessment mapping.</idPurp>
  </dataIdInfo>
  <DataProperties><lineage>Converted from CAD.</lineage></DataProperties>
  <distInfo><onlink>https://www.pasda.psu.edu/download/parcels/parcels.zip</onlink></distInfo>
</metadata>
"""


UNKNOWN_XML = """<?xml version="1.0"?><record><title>Mystery Layer</title><description>Still useful.</description></record>"""
MALFORMED_XML = """<?xml version="1.0"?><metadata><idinfo></metadata>"""
FGDC_CUSTOM_PROJECTED_XML = """<?xml version="1.0"?>
<metadata>
  <idinfo>
    <citation><citeinfo><title>Custom Albers</title></citeinfo></citation>
    <descript><abstract>Custom projected CRS example.</abstract></descript>
  </idinfo>
  <spref>
    <horizsys>
      <cordsysn>
        <geogcsn>GCS_North_American_1983</geogcsn>
        <projcsn>albers_dep</projcsn>
      </cordsysn>
      <planar><planci><plandu>meters</plandu></planci></planar>
      <geodetic><horizdn>North American Datum of 1983</horizdn></geodetic>
    </horizsys>
  </spref>
  <distinfo/>
  <metainfo/>
</metadata>
"""
FGDC_DISTOR_FORMAT_XML = """<?xml version="1.0"?>
<metadata>
  <idinfo>
    <citation><citeinfo><title>Distor Format Example</title></citeinfo></citation>
    <descript><abstract>Distribution format example.</abstract></descript>
  </idinfo>
  <distInfo>
    <distributor>
      <distorFormat>
        <formatName Sync="TRUE">Shapefile</formatName>
      </distorFormat>
    </distributor>
  </distInfo>
  <distinfo/>
  <metainfo/>
</metadata>
"""


def _config(tmp_path: Path) -> dict:
    return {
        "metadata_base_url": "https://www.pasda.psu.edu/metadata/",
        "cache_dir": str(tmp_path / "cache"),
        "output_dir": str(tmp_path / "outputs"),
    }


def _manifest_row(tmp_path: Path, filename: str, xml: str) -> dict:
    raw_path = tmp_path / filename
    raw_path.write_text(xml, encoding="utf-8")
    return {
        "source_system": "PASDA",
        "source_manifest": "metadata_directory",
        "metadata_filename": filename,
        "metadata_url": f"https://www.pasda.psu.edu/metadata/{filename}",
        "metadata_file_stem": Path(filename).stem,
        "metadata_last_modified": "",
        "metadata_size_bytes": raw_path.stat().st_size,
        "metadata_extension": ".xml",
        "harvested_at": "2026-07-03T00:00:00Z",
        "xml_fetch_status": "fetched",
        "xml_parse_status": "pending",
        "metadata_profile": "",
        "metadata_profile_confidence": "",
        "parse_error": "",
        "xml_sha256": "abc123",
        "raw_xml_path": str(raw_path),
    }


def test_pasda_harvester_enables_build_uploads_by_default(tmp_path: Path) -> None:
    harvester = PasdaHarvester(_config(tmp_path))

    assert harvester.config["build_uploads"] is True


def test_pasda_harvester_allows_build_uploads_to_be_disabled(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config["build_uploads"] = False

    harvester = PasdaHarvester(config)

    assert harvester.config["build_uploads"] is False


def test_pasda_harvester_keeps_base_method_surface(tmp_path: Path) -> None:
    PasdaHarvester(_config(tmp_path))
    allowed_methods = {
        "__init__",
        "load_reference_data",
        "fetch",
        "parse",
        "flatten",
        "build_dataframe",
        "derive_fields",
        "add_defaults",
        "add_provenance",
        "clean",
        "validate",
        "write_outputs",
        "build_uploads",
        "harvest_pipeline",
    }

    pasda_methods = {
        name
        for name, value in PasdaHarvester.__dict__.items()
        if callable(value) and not name.startswith("__")
    }
    base_methods = {
        name
        for name, value in BaseHarvester.__dict__.items()
        if callable(value) and not name.startswith("__")
    }

    assert pasda_methods <= allowed_methods
    assert pasda_methods <= base_methods | {"build_uploads"}


def test_parse_metadata_directory_listing_apache_style() -> None:
    html = """
    <html><body><pre>
    <a href="county_roads.xml">county_roads.xml</a> 2024-01-12 10:30 12K
    <a href="imagery.xml">imagery.xml</a> 2024-02-01 09:00 2048
    <a href="notes.txt">notes.txt</a> 2024-02-01 09:00 10K
    </pre></body></html>
    """

    rows = parse_metadata_directory_listing(
        html,
        "https://www.pasda.psu.edu/metadata/",
        harvested_at="2026-07-03T00:00:00Z",
    )

    assert [row["metadata_filename"] for row in rows] == ["county_roads.xml", "imagery.xml"]
    assert rows[0]["metadata_url"] == "https://www.pasda.psu.edu/metadata/county_roads.xml"
    assert rows[0]["metadata_size_bytes"] == 12288
    assert rows[0]["source_manifest"] == "metadata_directory"


def test_fetch_creates_test_directories_and_honors_max_records(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class FakeResponse:
        def __init__(self, text: str = "", content: bytes = b"") -> None:
            self.text = text
            self.content = content

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def get(self, url: str, timeout: int):
            if url.endswith("/metadata/"):
                return FakeResponse(
                    """
                    <pre>
                    <a href="one.xml">one.xml</a> 2024-01-01 10:00 1K
                    <a href="two.xml">two.xml</a> 2024-01-01 10:00 1K
                    </pre>
                    """
                )
            return FakeResponse(content=FGDC_XML.encode("utf-8"))

    config = _config(tmp_path)
    config["max_records"] = 1
    monkeypatch.setattr(pasda_module, "build_pasda_session", lambda user_agent: FakeSession())

    harvester = PasdaHarvester(config)
    rows = harvester.fetch()

    assert (tmp_path / "cache").is_dir()
    assert (tmp_path / "outputs").is_dir()
    assert len(rows) == 1
    assert rows[0]["metadata_filename"] == "one.xml"
    assert len(harvester.inventory_rows) == 2
    assert harvester.inventory_rows[0]["selected_for_download"] == "yes"
    assert harvester.inventory_rows[1]["selected_for_download"] == "no"


def test_fetch_and_parse_reuse_unchanged_metadata_registry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    metadata_registry_path = tmp_path / "registry" / "pasda_metadata_registry.csv"
    normalized_registry_path = tmp_path / "registry" / "pasda_normalized_registry.jsonl"
    metadata_registry_path.parent.mkdir()
    pd.DataFrame(
        [
            {
                "metadata_filename": "one.xml",
                "metadata_url": "https://www.pasda.psu.edu/metadata/one.xml",
                "source_record_id": "one",
                "pasda_record_id": "pasda-one",
                "metadata_last_modified": "2024-01-01 10:00",
                "metadata_size_bytes": "1024",
                "xml_sha256": "abc123",
                "metadata_profile": "fgdc_csdgm",
                "metadata_profile_confidence": "high",
                "xml_fetch_status": "fetched",
                "xml_parse_status": "parsed",
                "parse_error": "",
                "first_seen": "2026-07-01",
                "last_seen": "2026-07-01",
                "last_parsed": "2026-07-01",
                "registry_version": "1",
            }
        ]
    ).to_csv(metadata_registry_path, index=False)
    normalized_registry_path.write_text(
        json.dumps(
            {
                "source_system": "PASDA",
                "source_record_id": "one",
                "metadata_filename": "one.xml",
                "metadata_url": "https://www.pasda.psu.edu/metadata/one.xml",
                "title": "Registry Title",
                "metadata_profile": "fgdc_csdgm",
                "metadata_profile_confidence": "high",
                "xml_parse_status": "parsed",
                "xml_sha256": "abc123",
                "place_keywords": [],
                "theme_keywords": [],
                "iso_topic_categories": [],
                "online_links": [],
                "distribution_links": [],
                "download_links_found_in_metadata": [],
                "service_links_found_in_metadata": [],
                "parse_warnings": [],
                "registry_version": "1",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeResponse:
        text = """
        <pre>
        <a href="one.xml">one.xml</a> 2024-01-01 10:00 1K
        </pre>
        """

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def __init__(self) -> None:
            self.metadata_get_count = 0

        def get(self, url: str, timeout: int):
            if url.endswith("/metadata/"):
                return FakeResponse()
            self.metadata_get_count += 1
            raise AssertionError("Registry reuse should skip XML download.")

    fake_session = FakeSession()
    monkeypatch.setattr(pasda_module, "build_pasda_session", lambda user_agent: fake_session)

    config = _config(tmp_path)
    config["metadata_registry_path"] = str(metadata_registry_path)
    config["normalized_registry_path"] = str(normalized_registry_path)

    harvester = PasdaHarvester(config)
    rows = harvester.fetch()
    records = harvester.parse(rows)

    assert fake_session.metadata_get_count == 0
    assert rows[0]["xml_fetch_status"] == "registry"
    assert rows[0]["registry_reuse_status"] == "reused"
    assert records[0]["title"] == "Registry Title"
    assert records[0]["registry_reuse_status"] == "reused"


def test_mixed_sample_strategy_selects_across_inventory() -> None:
    rows = [{"metadata_filename": f"{index:02d}.xml"} for index in range(10)]

    sample = select_metadata_sample(rows, sample_size=4, sample_strategy="mixed")

    assert [row["metadata_filename"] for row in sample] == [
        "00.xml",
        "03.xml",
        "06.xml",
        "09.xml",
    ]


def test_metadata_profile_detection() -> None:
    assert detect_metadata_profile(FGDC_XML)["metadata_profile"] == "fgdc_csdgm"
    assert detect_metadata_profile(ISO_XML)["metadata_profile"] == "iso_19139"
    assert detect_metadata_profile(ARCGIS_XML)["metadata_profile"] == "arcgis_metadata"
    assert detect_metadata_profile(UNKNOWN_XML)["metadata_profile"] == "unknown_xml"
    assert detect_metadata_profile(MALFORMED_XML)["metadata_profile"] == "malformed_xml"
    assert detect_metadata_profile("not xml")["metadata_profile"] == "empty_or_non_xml"


def test_fgdc_field_extraction(tmp_path: Path) -> None:
    manifest, record = parse_pasda_manifest_row(_manifest_row(tmp_path, "bedrock.xml", FGDC_XML))

    assert manifest["xml_parse_status"] == "parsed"
    assert record["metadata_profile"] == "fgdc_csdgm"
    assert record["title"] == "Pennsylvania Bedrock Geology"
    assert record["creator"] == "Pennsylvania Geological Survey"
    assert record["west_bbox"] == "-80.52"
    assert record["spatial_reference"] == "https://spatialreference.org/ref/epsg/2272/"
    assert record["theme_keywords"] == ["geology"]
    assert record["download_links_found_in_metadata"] == [
        "https://www.pasda.psu.edu/download/geology/bedrock.zip"
    ]


def test_fgdc_custom_projected_crs_keeps_descriptive_reference(tmp_path: Path) -> None:
    _, record = parse_pasda_manifest_row(
        _manifest_row(tmp_path, "custom-albers.xml", FGDC_CUSTOM_PROJECTED_XML)
    )

    assert record["spatial_reference"].startswith("Projected CRS: albers_dep")
    assert "https://spatialreference.org/ref/epsg/4269/" not in record["spatial_reference"]


def test_fgdc_data_format_reads_distor_format(tmp_path: Path) -> None:
    _, record = parse_pasda_manifest_row(
        _manifest_row(tmp_path, "distor-format.xml", FGDC_DISTOR_FORMAT_XML)
    )

    assert record["data_format"] == "Shapefile"


def test_iso_field_extraction(tmp_path: Path) -> None:
    manifest, record = parse_pasda_manifest_row(_manifest_row(tmp_path, "imagery.xml", ISO_XML))

    assert manifest["xml_parse_status"] == "parsed"
    assert record["metadata_profile"] == "iso_19139"
    assert record["title"] == "Pennsylvania Imagery 2023"
    assert record["abstract"] == "Leaf-off imagery."
    assert record["publication_date"] == "2023-06-01"
    assert record["north_bbox"] == "42.3"
    assert record["service_links_found_in_metadata"] == [
        "https://example.org/arcgis/rest/services/imagery/ImageServer"
    ]


def test_arcgis_partial_field_extraction(tmp_path: Path) -> None:
    manifest, record = parse_pasda_manifest_row(_manifest_row(tmp_path, "parcels.xml", ARCGIS_XML))

    assert manifest["xml_parse_status"] == "parsed"
    assert record["metadata_profile"] == "arcgis_metadata"
    assert record["title"] == "County Parcels"
    assert record["abstract"] == "Parcel boundaries for a county."
    assert "arcgis_metadata_parser_is_heuristic" in record["parse_warnings"]


def test_malformed_xml_handling(tmp_path: Path) -> None:
    manifest, record = parse_pasda_manifest_row(_manifest_row(tmp_path, "broken.xml", MALFORMED_XML))

    assert manifest["xml_parse_status"] == "malformed"
    assert manifest["metadata_profile"] == "malformed_xml"
    assert record["parse_error"]


def test_unknown_xml_normalized_record_shape(tmp_path: Path) -> None:
    manifest, record = parse_pasda_manifest_row(_manifest_row(tmp_path, "unknown.xml", UNKNOWN_XML))

    assert manifest["xml_parse_status"] == "partial"
    assert record["metadata_profile"] == "unknown_xml"
    assert record["title"] == "Mystery Layer"
    assert isinstance(record["online_links"], list)
    assert "source_system" in record


def test_pasda_parse_collects_errors_and_summary(tmp_path: Path) -> None:
    harvester = PasdaHarvester(_config(tmp_path))
    rows = [
        _manifest_row(tmp_path, "good.xml", FGDC_XML),
        _manifest_row(tmp_path, "broken.xml", MALFORMED_XML),
    ]

    records = harvester.parse(rows)

    assert len(records) == 2
    assert len(harvester.error_rows) == 1
    assert any(row["metadata_profile"] == "fgdc_csdgm" for row in harvester.profile_summary)


def test_pasda_aardvark_draft_crosswalk_leaves_provider_blank(tmp_path: Path) -> None:
    _, record = parse_pasda_manifest_row(_manifest_row(tmp_path, "bedrock.xml", FGDC_XML))
    record["lineage"] = "Test lineage."

    rows = build_pasda_aardvark_draft_records([record], accession_date="2026-07-03")

    assert rows[0]["Provider"] == ""
    assert rows[0]["ID"] == "pasda-bedrock"
    assert rows[0]["Title"] == "Pennsylvania Bedrock Geology"
    assert rows[0]["Alternative Title"] == "Pennsylvania Bedrock Geology"
    assert rows[0]["Description"].startswith("Statewide bedrock geology polygons.")
    assert rows[0]["Publisher"] == "Pennsylvania Spatial Data Access (PASDA)"
    assert rows[0]["Resource Class"] == "Datasets"
    assert rows[0]["Date Issued"] == "2024-01-15"
    assert rows[0]["Temporal Coverage"] == "2024-01-15"
    assert "Index Year" not in rows[0]
    assert rows[0]["Resource Type"] == ""
    assert rows[0]["Format"] == "Shapefile"
    assert rows[0]["Coordinate Reference System"] == "https://spatialreference.org/ref/epsg/2272/"
    assert rows[0]["Bounding Box"] == "-80.52,39.72,-74.69,42.27"
    assert rows[0]["Keyword"] == "geology|Pennsylvania"
    assert rows[0]["Spatial Coverage"] == "Pennsylvania"
    assert rows[0]["Rights"] == "Use with citation."
    assert rows[0]["Source"] == ""
    assert rows[0]["Is Harvested"] == ""
    assert rows[0]["Last Harvested"] == ""
    assert rows[0]["Endpoint Description"] == ""
    assert rows[0]["Endpoint URL"] == ""
    assert rows[0]["Member Of"] == ""
    assert rows[0]["Is Part Of"] == ""
    assert rows[0]["Provenance"] == (
        "Harvested from https://www.pasda.psu.edu/metadata/bedrock.xml on 2026-07-03. "
        "Pasda lineage text: Test lineage."
    )
    assert rows[0]["Admin Note"] == "PASDA metadata profile: fgdc_csdgm"
    assert "pasda_metadata_profile" not in rows[0]
    assert "pasda_metadata_filename" not in rows[0]
    assert "pasda_metadata_url" not in rows[0]


def test_pasda_aardvark_draft_does_not_default_spatial_coverage() -> None:
    rows = build_pasda_aardvark_draft_records(
        [
            {
                "source_record_id": "no-place",
                "metadata_url": "https://www.pasda.psu.edu/metadata/no-place.xml",
                "title": "No Place",
                "publication_date": "20240101",
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            }
        ],
        accession_date="2026-07-03",
    )

    assert rows[0]["Spatial Coverage"] == ""


def test_pasda_aardvark_spatial_coverage_matches_pa_county_reference_format() -> None:
    county_lookup = build_pasda_county_lookup(
        pd.DataFrame(
            {
                "County": [
                    "Pennsylvania--Allegheny County",
                    "Pennsylvania--Chester County",
                    "Pennsylvania--Montgomery County",
                    "New York--Montgomery County",
                    "Virginia--Alleghany County",
                ]
            }
        )
    )
    rows = build_pasda_aardvark_draft_records(
        [
            {
                "source_record_id": "allegheny",
                "metadata_url": "https://www.pasda.psu.edu/metadata/allegheny.xml",
                "title": "Allegheny",
                "place_keywords": ["PA", "Allegheny County", "Pittsburgh"],
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "virginia",
                "metadata_url": "https://www.pasda.psu.edu/metadata/virginia.xml",
                "title": "Virginia County",
                "place_keywords": ["Virginia", "Alleghany County"],
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "chester",
                "metadata_url": "https://www.pasda.psu.edu/metadata/chester.xml",
                "title": "Chester",
                "place_keywords": ["Chester County"],
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "montgomery-ny",
                "metadata_url": "https://www.pasda.psu.edu/metadata/montgomery-ny.xml",
                "title": "Montgomery New York",
                "place_keywords": ["New York", "Montgomery County"],
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
        ],
        accession_date="2026-07-03",
        county_lookup=county_lookup,
    )

    assert rows[0]["Spatial Coverage"] == "Pennsylvania--Allegheny County|Pennsylvania|Pittsburgh"
    assert rows[1]["Spatial Coverage"] == "Virginia|Alleghany County"
    assert rows[2]["Spatial Coverage"] == "Pennsylvania--Chester County|Pennsylvania"
    assert rows[3]["Spatial Coverage"] == "New York|Montgomery County"


def test_pasda_aardvark_resource_class_identifies_imagery_records() -> None:
    rows = build_pasda_aardvark_draft_records(
        [
            {
                "source_record_id": "orthoimagery",
                "metadata_url": "https://www.pasda.psu.edu/metadata/orthoimagery.xml",
                "title": "DVRPC 2000 Digital Orthoimagery",
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "orthophotos",
                "metadata_url": "https://www.pasda.psu.edu/metadata/orthophotos.xml",
                "title": "PAMAP Program 2005 Color Orthophotos",
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "aerial",
                "metadata_url": "https://www.pasda.psu.edu/metadata/aerial.xml",
                "title": "Historic Aerial Photos 1937",
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "satellite-imagery",
                "metadata_url": "https://www.pasda.psu.edu/metadata/satellite-imagery.xml",
                "title": "Satellite Imagery Mosaic",
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "building-footprints",
                "metadata_url": "https://www.pasda.psu.edu/metadata/building-footprints.xml",
                "title": "Allegheny County Building Footprints",
                "abstract": "Building footprint polygons digitized from imagery.",
                "data_format": "Vector data",
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
        ],
        accession_date="2026-07-03",
    )

    assert rows[0]["Resource Class"] == "Imagery"
    assert rows[1]["Resource Class"] == "Imagery"
    assert rows[2]["Resource Class"] == "Imagery"
    assert rows[3]["Resource Class"] == "Imagery"
    assert rows[4]["Resource Class"] == "Datasets"


def test_pasda_aardvark_draft_dataframe_derives_theme_from_keywords() -> None:
    draft_df = build_pasda_aardvark_draft_dataframe(
        [
            {
                "source_record_id": "roads",
                "metadata_url": "https://www.pasda.psu.edu/metadata/roads.xml",
                "title": "Road Centerlines",
                "theme_keywords": ["roads", "transportation"],
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "parcels",
                "metadata_url": "https://www.pasda.psu.edu/metadata/parcels.xml",
                "title": "County Parcels",
                "theme_keywords": ["parcel", "planningCadastre"],
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
        ],
        accession_date="2026-07-03",
        theme_map={
            "roads": "Transportation",
            "transportation": "Transportation",
            "parcel": "Property",
            "planningcadastre": "Property",
        },
    )

    assert draft_df.loc[0, "Theme"] == "Transportation"
    assert draft_df.loc[1, "Theme"] == "Property"


def test_pasda_aardvark_temporal_coverage_uses_readable_range_separator() -> None:
    rows = build_pasda_aardvark_draft_records(
        [
            {
                "source_record_id": "date-range",
                "metadata_url": "https://www.pasda.psu.edu/metadata/date-range.xml",
                "title": "Date Range",
                "temporal_start": "19940101",
                "temporal_end": "20050101",
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "same-date",
                "metadata_url": "https://www.pasda.psu.edu/metadata/same-date.xml",
                "title": "Same Date",
                "temporal_start": "2012",
                "temporal_end": "2012",
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            }
        ],
        accession_date="2026-07-03",
    )

    assert rows[0]["Temporal Coverage"] == "1994-01-01 to 2005-01-01"
    assert rows[1]["Temporal Coverage"] == "2012"


def test_pasda_aardvark_title_refinement_preserves_dates_and_adds_place_when_helpful() -> None:
    rows = build_pasda_aardvark_draft_records(
        [
            {
                "source_record_id": "active-boundaries",
                "metadata_url": "https://www.pasda.psu.edu/metadata/active.xml",
                "title": "Active Underground Permit Boundaries 202410",
                "publication_date": "202410",
                "place_keywords": ["Pennsylvania"],
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "allegheny-address",
                "metadata_url": "https://www.pasda.psu.edu/metadata/address.xml",
                "title": "Allegheny County Address Points 201812",
                "publication_date": "201812",
                "place_keywords": ["Pennsylvania", "Allegheny County"],
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "anf-road",
                "metadata_url": "https://www.pasda.psu.edu/metadata/road.xml",
                "title": "Road",
                "publication_date": "20121204",
                "place_keywords": ["Allegheny National Forest", "ANF", "Pennsylvania", "PA"],
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
            {
                "source_record_id": "anf-road-comma",
                "metadata_url": "https://www.pasda.psu.edu/metadata/road-comma.xml",
                "title": "Road",
                "publication_date": "20121204",
                "place_keywords": ["Allegheny National Forest, ANF, Pennsylvania, PA"],
                "xml_parse_status": "parsed",
                "metadata_profile": "fgdc_csdgm",
            },
        ],
        accession_date="2026-07-03",
    )

    assert rows[0]["Title"] == "Active Underground Permit Boundaries 2024-10 [Pennsylvania]"
    assert rows[0]["Alternative Title"] == "Active Underground Permit Boundaries 202410"
    assert rows[1]["Title"] == "Allegheny County Address Points 2018-12"
    assert rows[1]["Alternative Title"] == "Allegheny County Address Points 201812"
    assert rows[2]["Title"] == "Road [Allegheny National Forest]"
    assert rows[2]["Alternative Title"] == "Road"
    assert rows[3]["Title"] == "Road [Allegheny National Forest]"
    assert rows[3]["Alternative Title"] == "Road"
    assert rows[3]["Spatial Coverage"] == "Pennsylvania|Allegheny National Forest|ANF"
