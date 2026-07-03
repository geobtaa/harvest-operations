from pathlib import Path

from harvesters.base import BaseHarvester
from harvesters.pasda import (
    PasdaHarvester,
    detect_metadata_profile,
    parse_metadata_directory_listing,
    parse_pasda_manifest_row,
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
    assert record["theme_keywords"] == ["geology"]
    assert record["download_links_found_in_metadata"] == [
        "https://www.pasda.psu.edu/download/geology/bedrock.zip"
    ]


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
