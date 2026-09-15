from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "curation" / "src"))

from curation.fgdc_metadata import normalize_fgdc_date, parse_fgdc_metadata  # noqa: E402


def test_parse_fgdc_keeps_publication_and_content_dates_separate(
    tmp_path: Path,
) -> None:
    xml_path = tmp_path / "metadata.xml"
    xml_path.write_text(
        """<?xml version="1.0"?>
        <metadata>
          <idinfo>
            <citation><citeinfo>
              <origin>Example Agency</origin>
              <origin>Example Division</origin>
              <pubdate>20170104</pubdate>
              <title>Example waterbodies</title>
              <pubinfo><publish>Example Publisher</publish></pubinfo>
            </citeinfo></citation>
            <descript>
              <abstract>Abstract &lt;strong&gt;text&lt;/strong&gt;.
                &lt;ul&gt;&lt;li&gt;First item&lt;/li&gt;&lt;li&gt;Second item&lt;/li&gt;&lt;/ul&gt;
              </abstract>
              <purpose>Research &lt;em&gt;and teaching&lt;/em&gt;.</purpose>
            </descript>
            <timeperd><timeinfo><sngdate><caldate>20240919</caldate></sngdate></timeinfo>
              <current>Ground condition</current></timeperd>
            <keywords><theme><themekey>water</themekey></theme>
              <place><placekey>Minnesota</placekey></place></keywords>
            <spdom><bounding><westbc>-97.2</westbc><eastbc>-89.4</eastbc>
              <northbc>49.4</northbc><southbc>43.5</southbc></bounding></spdom>
            <accconst>None</accconst><useconst>Public domain</useconst>
          </idinfo>
          <metainfo><metd>20170224</metd></metainfo>
          <eainfo><detailed><attr><attrlabl>NAME</attrlabl>
            <attrdef>Waterbody name</attrdef><attrdefs>Agency</attrdefs>
            <attrdomv><edom><edomv>A</edomv><edomvd>Active</edomvd></edom></attrdomv>
          </attr></detailed></eainfo>
        </metadata>
        """,
        encoding="utf-8",
    )

    parsed = parse_fgdc_metadata(xml_path)

    assert parsed.title == "Example waterbodies"
    assert parsed.creator == "Example Agency|Example Division"
    assert parsed.description == (
        "Abstract text. First item Second item|Purpose: Research and teaching."
    )
    assert "<" not in parsed.description
    assert parsed.publication_date == "2017-01-04"
    assert parsed.calendar_dates == ("2024-09-19",)
    assert parsed.temporal_coverage == "2024-09-19"
    assert parsed.temporal_source == "FGDC calendar date"
    assert parsed.date_range == "2024-2024"
    assert parsed.bounding_box == "-97.2,43.5,-89.4,49.4"
    assert parsed.geometry.startswith("POLYGON((-97.2 49.4")
    assert parsed.centroid == "46.45,-93.30000000000001"
    assert parsed.attributes[0].label == "NAME"
    assert parsed.attributes[0].domain == "A | Active"


def test_parse_fgdc_range_dates_and_namespaces(tmp_path: Path) -> None:
    xml_path = tmp_path / "range.xml"
    xml_path.write_text(
        """<fgdc:metadata xmlns:fgdc="urn:test"><fgdc:idinfo>
          <fgdc:citation><fgdc:citeinfo><fgdc:title>Roads</fgdc:title>
            <fgdc:pubdate>2020</fgdc:pubdate></fgdc:citeinfo></fgdc:citation>
          <fgdc:timeperd><fgdc:timeinfo><fgdc:rngdates>
            <fgdc:begdate>201801</fgdc:begdate><fgdc:enddate>20191231</fgdc:enddate>
          </fgdc:rngdates></fgdc:timeinfo></fgdc:timeperd>
        </fgdc:idinfo></fgdc:metadata>""",
        encoding="utf-8",
    )

    parsed = parse_fgdc_metadata(xml_path)

    assert parsed.begin_date == "2018-01"
    assert parsed.end_date == "2019-12-31"
    assert parsed.temporal_coverage == "2018-01 to 2019-12-31"
    assert parsed.date_range == "2018-2019"
    assert normalize_fgdc_date("Present") == "Present"
    assert normalize_fgdc_date("20241340") == "20241340"


def test_parse_fgdc_free_text_attribute_overview(tmp_path: Path) -> None:
    xml_path = tmp_path / "overview.xml"
    xml_path.write_text(
        """<metadata><eainfo><overview><eaover>
        FIELDS ASSOCIATED WITH: Example.shp:

        FIELD_ONE = First field definition

        FIELD_TWO - Second field definition.
        </eaover><eadetcit>1) FirstLayer; tract-level observations

        2) SecondLayer; dissolved outline</eadetcit></overview></eainfo></metadata>""",
        encoding="utf-8",
    )

    parsed = parse_fgdc_metadata(xml_path)

    assert [(row.label, row.definition) for row in parsed.attributes] == [
        ("FIELD_ONE", "First field definition"),
        ("FIELD_TWO", "Second field definition."),
    ]
    assert all(
        row.definition_source == "FGDC entity and attribute overview"
        for row in parsed.attributes
    )
    assert parsed.entity_attribute_detail == (
        "1) FirstLayer; tract-level observations 2) SecondLayer; dissolved outline"
    )
