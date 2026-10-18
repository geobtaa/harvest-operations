from pathlib import Path

import pandas as pd

from harvesters.oai_qdc import OaiQdcHarvester


def _config(sets_csv: Path, **overrides) -> dict:
    config = {
        "name": "iowa-library",
        "oai_base_url": "https://digital.lib.uiowa.edu/oai/request",
        "provider": "University of Iowa",
        "sets_csv": str(sets_csv),
        "output_primary_csv": "outputs/iowa-library_primary.csv",
        "output_distributions_csv": "outputs/iowa-library_distributions.csv",
        "identifier_prefix_field_map": {
            "B1G Image": ["https://digital.lib.uiowa.edu/_foxml/datastream/"],
        },
    }
    config.update(overrides)
    return config


def _write_spatial_lookup_csvs(tmp_path: Path) -> dict[str, str]:
    cities_csv = tmp_path / "spatial_cities.csv"
    cities_csv.write_text(
        "\n".join(
            [
                "City,Bounding Box,Geometry,GeoNames",
                "Iowa--Iowa City,CITY_BBOX,CITY_GEOM,CITY_GEONAMES",
                "Iowa--Des Moines,DES_MOINES_CITY_BBOX,DES_MOINES_CITY_GEOM,DES_MOINES_CITY_GEONAMES",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    counties_csv = tmp_path / "spatial_counties.csv"
    counties_csv.write_text(
        "\n".join(
            [
                "County,Bounding Box,Geometry,GeoNames",
                "Iowa--Iowa County,IOWA_COUNTY_BBOX,IOWA_COUNTY_GEOM,IOWA_COUNTY_GEONAMES",
                "Iowa--Johnson County,COUNTY_BBOX,COUNTY_GEOM,COUNTY_GEONAMES",
                "Iowa--Des Moines County,DES_MOINES_COUNTY_BBOX,DES_MOINES_COUNTY_GEOM,DES_MOINES_COUNTY_GEONAMES",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    states_csv = tmp_path / "spatial_states.csv"
    states_csv.write_text(
        "\n".join(
            [
                "Label,GeoNames ID,Bounding Box,Geometry,GEOID,STUSPS",
                "Iowa,STATE_GEONAMES,STATE_BBOX,STATE_GEOM,19,IA",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    plss_csv = tmp_path / "spatial_plss.csv"
    plss_csv.write_text(
        "\n".join(
            [
                "Township,Township Direction,Range,Range Direction,STATEABBR,PRINMERCD,PRINMER,PLSSID,Bounding Box",
                '1,N,1,W,IA,,,,"1,2,3,4"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "spatial_match_state": "Iowa",
        "spatial_append_state": "Iowa",
        "spatial_plss_state_abbr": "IA",
        "spatial_cities_csv": str(cities_csv),
        "spatial_counties_csv": str(counties_csv),
        "spatial_states_csv": str(states_csv),
        "spatial_plss_csv": str(plss_csv),
    }


def test_oai_qdc_routes_iowa_foxml_identifier_values_to_b1g_image(tmp_path) -> None:
    sets_csv = tmp_path / "sets.csv"
    sets_csv.write_text("set,title\nnode:1,Sample Set\n", encoding="utf-8")

    harvester = OaiQdcHarvester(_config(sets_csv))

    df = pd.DataFrame(
        [
            {
                "Identifier": (
                    "oai:test:123|https://digital.lib.uiowa.edu/_foxml/datastream/abc/full/full/0/default.jpg"
                    "|https://digital.lib.uiowa.edu/node/123"
                ),
                "B1G Image": "",
            }
        ]
    )

    routed_df = harvester.oai_route_identifier_values(df)
    row = routed_df.iloc[0]

    assert row["Identifier"] == "oai:test:123|https://digital.lib.uiowa.edu/node/123"
    assert (
        row["B1G Image"]
        == "https://digital.lib.uiowa.edu/_foxml/datastream/abc/full/full/0/default.jpg"
    )


def test_oai_qdc_builds_iowa_ids_with_b1g_prefix_and_without_node(tmp_path) -> None:
    sets_csv = tmp_path / "sets.csv"
    sets_csv.write_text("set,title\nnode:244482,Sample Set\n", encoding="utf-8")

    harvester = OaiQdcHarvester(
        _config(
            sets_csv,
            source_id_prefix="b1g_iowa",
            id_set_spec_prefixes_to_strip=["node"],
        )
    )

    record_id = harvester.oai_build_id(
        {
            "set_spec": "node:244482",
            "oai_identifier": "oai:digital.lib.uiowa.edu:node-38132",
        },
        "https://digital.lib.uiowa.edu/node/38132",
    )

    assert record_id == "b1g_iowa_244482_38132"


def test_oai_qdc_prefers_plss_bbox_over_city_match(tmp_path) -> None:
    sets_csv = tmp_path / "sets.csv"
    sets_csv.write_text("set,title\nnode:1,Sample Set\n", encoding="utf-8")

    harvester = OaiQdcHarvester(
        _config(sets_csv, **_write_spatial_lookup_csvs(tmp_path))
    )
    harvester.load_reference_data()

    df = pd.DataFrame(
        [
            {
                "Spatial Coverage": "T1N|R1W|United States -- Iowa -- Johnson County -- Iowa City",
                "Bounding Box": "",
                "Geometry": "",
                "GeoNames": "",
            }
        ]
    )

    enriched_df = harvester.oai_enrich_spatial_fields(df)
    row = enriched_df.iloc[0]

    assert row["Bounding Box"] == "1,2,3,4"
    assert row["Geometry"] == "CITY_GEOM"
    assert row["GeoNames"] == "CITY_GEONAMES"


def test_oai_qdc_does_not_confuse_des_moines_city_with_des_moines_county(
    tmp_path,
) -> None:
    sets_csv = tmp_path / "sets.csv"
    sets_csv.write_text("set,title\nnode:1,Sample Set\n", encoding="utf-8")

    harvester = OaiQdcHarvester(
        _config(sets_csv, **_write_spatial_lookup_csvs(tmp_path))
    )
    harvester.load_reference_data()

    df = pd.DataFrame(
        [
            {
                "Spatial Coverage": "United States -- Iowa -- Des Moines County",
                "Bounding Box": "",
                "Geometry": "",
                "GeoNames": "",
            }
        ]
    )

    enriched_df = harvester.oai_enrich_spatial_fields(df)
    row = enriched_df.iloc[0]

    assert row["Bounding Box"] == "DES_MOINES_COUNTY_BBOX"
    assert row["Geometry"] == "DES_MOINES_COUNTY_GEOM"
    assert row["GeoNames"] == "DES_MOINES_COUNTY_GEONAMES"


def test_oai_qdc_distinguishes_iowa_state_county_and_city_matches(tmp_path) -> None:
    sets_csv = tmp_path / "sets.csv"
    sets_csv.write_text("set,title\nnode:1,Sample Set\n", encoding="utf-8")

    harvester = OaiQdcHarvester(
        _config(sets_csv, **_write_spatial_lookup_csvs(tmp_path))
    )
    harvester.load_reference_data()

    df = pd.DataFrame(
        [
            {
                "Spatial Coverage": "Iowa",
                "Bounding Box": "",
                "Geometry": "",
                "GeoNames": "",
            },
            {
                "Spatial Coverage": "Iowa--Iowa County",
                "Bounding Box": "",
                "Geometry": "",
                "GeoNames": "",
            },
            {
                "Spatial Coverage": "Iowa--Iowa City",
                "Bounding Box": "",
                "Geometry": "",
                "GeoNames": "",
            },
        ]
    )

    enriched_df = harvester.oai_enrich_spatial_fields(df)

    state_row = enriched_df.iloc[0]
    county_row = enriched_df.iloc[1]
    city_row = enriched_df.iloc[2]

    assert state_row["Bounding Box"] == "STATE_BBOX"
    assert state_row["Geometry"] == "STATE_GEOM"
    assert state_row["GeoNames"] == "STATE_GEONAMES"

    assert county_row["Bounding Box"] == "IOWA_COUNTY_BBOX"
    assert county_row["Geometry"] == "IOWA_COUNTY_GEOM"
    assert county_row["GeoNames"] == "IOWA_COUNTY_GEONAMES"

    assert city_row["Bounding Box"] == "CITY_BBOX"
    assert city_row["Geometry"] == "CITY_GEOM"
    assert city_row["GeoNames"] == "CITY_GEONAMES"


def test_oai_qdc_accepts_inline_sets_without_a_sidecar_csv(tmp_path) -> None:
    config = _config(tmp_path / "unused.csv")
    config.pop("sets_csv")
    config["sets"] = [
        {"set": "maps", "title": "Map Collection"},
        "aerials",
    ]

    harvester = OaiQdcHarvester(config)

    assert harvester.sets_csv is None
    assert harvester.oai_load_sets() == [
        {"set_spec": "maps", "set_title": "Map Collection"},
        {"set_spec": "aerials", "set_title": ""},
    ]


def test_oai_qdc_parses_standard_oai_dc_records(tmp_path) -> None:
    config = _config(tmp_path / "unused.csv")
    config.pop("sets_csv")
    config["sets"] = [{"set": "maps", "title": "Map Collection"}]
    harvester = OaiQdcHarvester(config)
    xml_text = """<?xml version="1.0" encoding="UTF-8"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
      xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
      xmlns:dc="http://purl.org/dc/elements/1.1/">
      <ListRecords>
        <record>
          <header>
            <identifier>oai:example.edu:maps-42</identifier>
            <datestamp>2026-08-01</datestamp>
          </header>
          <metadata>
            <oai_dc:dc>
              <dc:title>Campus map</dc:title>
              <dc:identifier>https://example.edu/maps/42</dc:identifier>
              <dc:type>Map</dc:type>
            </oai_dc:dc>
          </metadata>
        </record>
      </ListRecords>
    </OAI-PMH>
    """

    records = harvester.oai_parse_xml(
        xml_text,
        {"set_spec": "maps", "set_title": "Map Collection"},
    )

    assert records == [
        {
            "oai_identifier": "oai:example.edu:maps-42",
            "datestamp": "2026-08-01",
            "set_spec": "maps",
            "set_title": "Map Collection",
            "source_xml_path": "",
            "fields": {
                "dc:title": ["Campus map"],
                "dc:identifier": ["https://example.edu/maps/42"],
                "dc:type": ["Map"],
            },
        }
    ]


def test_oai_qdc_parses_dspace_qdc_and_uses_header_identifier_for_id(
    tmp_path,
) -> None:
    config = _config(
        tmp_path / "unused.csv",
        source_id_prefix="umn_udc",
    )
    config.pop("sets_csv")
    config["sets"] = [
        {"set": "col_11299_57196", "title": "County Atlas Series"}
    ]
    harvester = OaiQdcHarvester(config)
    xml_text = """<?xml version="1.0" encoding="UTF-8"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
      xmlns:qdc="http://dspace.org/qualifieddc/"
      xmlns:dc="http://purl.org/dc/elements/1.1/"
      xmlns:dcterms="http://purl.org/dc/terms/">
      <ListRecords>
        <record>
          <header>
            <identifier>oai:conservancy.umn.edu:11299/173781</identifier>
            <datestamp>2022-09-06T16:38:59Z</datestamp>
          </header>
          <metadata>
            <qdc:qualifieddc>
              <dc:title>Surficial Geology Map, Wadena County.</dc:title>
              <dcterms:abstract>Only the surficial geology map is available.</dcterms:abstract>
              <dcterms:issued>2016</dcterms:issued>
              <dc:type>Map</dc:type>
              <dc:identifier>http://hdl.handle.net/11299/173781
                http://hdl.handle.net/11299/183206</dc:identifier>
            </qdc:qualifieddc>
          </metadata>
        </record>
      </ListRecords>
    </OAI-PMH>
    """

    records = harvester.oai_parse_xml(
        xml_text,
        {"set_spec": "col_11299_57196", "set_title": "County Atlas Series"},
    )
    df = harvester.build_dataframe(records)

    assert records[0]["fields"]["dcterms:abstract"] == [
        "Only the surficial geology map is available."
    ]
    assert df.loc[0, "ID"] == "umn_udc_col_11299_57196_173781"
    assert df.loc[0, "Description"] == "Only the surficial geology map is available."
    assert df.loc[0, "Date Issued"] == "2016"


def test_oai_qdc_filters_with_umd_rdf_and_emits_manifest_distribution(tmp_path) -> None:
    primary_dir = tmp_path / "primary" / "maryland_and_historical_collections"
    rdf_dir = tmp_path / "rdf" / "maryland_and_historical_collections"
    primary_dir.mkdir(parents=True)
    rdf_dir.mkdir(parents=True)
    oai_identifier = "oai:fcrepo.lib.umd.edu:1903.1/12533"
    fcrepo_uri = (
        "https://fcrepo.lib.umd.edu/fcrepo/rest/dc/2023/2/c1/91/18/3e/"
        "c191183e-76b0-43ee-8221-4108b3521c3a"
    )
    (primary_dir / "0001.xml").write_text(
        f"""<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
          xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
          xmlns:dc="http://purl.org/dc/elements/1.1/">
          <ListRecords><record><header><identifier>{oai_identifier}</identifier></header>
          <metadata><oai_dc:dc><dc:title>Maryland</dc:title>
          <dc:identifier>https://hdl.handle.net/1903.1/12533</dc:identifier>
          <dc:type>Image</dc:type></oai_dc:dc></metadata></record></ListRecords>
        </OAI-PMH>""",
        encoding="utf-8",
    )
    (rdf_dir / "0001.xml").write_text(
        f"""<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
          xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
          xmlns:dcterms="http://purl.org/dc/terms/"
          xmlns:ore="http://www.openarchives.org/ore/terms/">
          <ListRecords><record><header><identifier>{oai_identifier}</identifier></header>
          <metadata><rdf:RDF><rdf:Description rdf:about="{fcrepo_uri}">
          <dcterms:identifier>hdl:1903.1/12533</dcterms:identifier>
          <rdf:type rdf:resource="http://vocab.lib.umd.edu/model#Item" />
          <ore:isAggregatedBy rdf:resource="http://vocab.lib.umd.edu/set#marylandMaps" />
          </rdf:Description></rdf:RDF></metadata></record></ListRecords>
        </OAI-PMH>""",
        encoding="utf-8",
    )
    config = _config(
        tmp_path / "unused.csv",
        name="umd-library",
        source_id_prefix="umd",
        oai_download_dir=str(tmp_path / "primary"),
        sets=[
            {
                "set": "maryland_and_historical_collections",
                "title": "Maryland and Historical Collections",
            }
        ],
        rdf_companion={
            "metadata_prefix": "rdf",
            "download_dir": str(tmp_path / "rdf"),
            "aggregation_uri": "http://vocab.lib.umd.edu/set#marylandMaps",
            "manifest_base_url": "https://iiif.lib.umd.edu/manifests",
        },
    )
    config.pop("sets_csv")
    harvester = OaiQdcHarvester(config)
    harvester.load_reference_data()

    records = harvester.flatten(harvester.parse(harvester.fetch()))
    primary_df = harvester.build_dataframe(records)
    distributions_df = harvester.oai_build_distributions(primary_df)

    assert primary_df["Title"].tolist() == ["Maryland"]
    manifest_rows = distributions_df.loc[
        distributions_df["reference_type"].eq("iiif_manifest")
    ]
    assert manifest_rows["distribution_url"].tolist() == [
        "https://iiif.lib.umd.edu/manifests/fcrepo:dc:2023:2:c1:91:18:3e:"
        "c191183e-76b0-43ee-8221-4108b3521c3a/manifest"
    ]


def test_oai_qdc_reconciles_existing_handle_inventory_during_write(
    monkeypatch,
    tmp_path,
) -> None:
    existing_primary_path = tmp_path / "existing_primary.csv"
    pd.DataFrame(
        [
            {
                "ID": "canonical-id",
                "Identifier": "canonical-id",
                "Title": "Curated Maryland title",
                "Website Platform": "PHP",
                "Created At": "2021-04-27",
            }
        ]
    ).to_csv(existing_primary_path, index=False)
    existing_distributions_path = tmp_path / "existing_distributions.csv"
    pd.DataFrame(
        [
            {
                "friendlier_id": "canonical-id",
                "reference_type": "documentation_external",
                "distribution_url": "http://hdl.handle.net/1903.1/12533",
                "label": "",
            }
        ]
    ).to_csv(existing_distributions_path, index=False)
    config = _config(
        tmp_path / "unused.csv",
        metadata_reconciliation={
            "identity": "handle",
            "existing_primary_csv": str(existing_primary_path),
            "existing_distributions_csv": str(existing_distributions_path),
            "ignore_unknown_fields": True,
            "exclude_fields": ["Identifier", "Website Platform"],
        },
    )
    captured = {}

    def capture_outputs(self, primary_df, distributions_df=None):
        captured["primary"] = primary_df.copy()
        captured["distributions"] = distributions_df.copy()
        return {
            "primary_csv": str(tmp_path / "2026-10-17_umd_primary.csv"),
            "distributions_csv": str(tmp_path / "2026-10-17_umd_distributions.csv"),
        }

    monkeypatch.setattr("harvesters.base.BaseHarvester.write_outputs", capture_outputs)
    harvester = OaiQdcHarvester(config)
    harvester.load_reference_data()
    harvested = pd.DataFrame(
        [
            {
                "ID": "harvested-id",
                "Identifier": "hdl:1903.1/12533",
                "Title": "Harvested title",
                "Website Platform": "Fedora",
                "information": "https://hdl.handle.net/1903.1/12533",
                "manifest": "https://iiif.example/12533/manifest",
            },
            {
                "ID": "new-id",
                "Identifier": "hdl:1903.1/99999",
                "Title": "New map",
                "Website Platform": "Fedora",
                "information": "https://hdl.handle.net/1903.1/99999",
                "manifest": "https://iiif.example/99999/manifest",
            },
        ]
    )

    results = harvester.write_outputs(harvested)

    reconciled = captured["primary"].set_index("ID")
    assert list(reconciled.index) == ["canonical-id", "new-id"]
    assert reconciled.loc["canonical-id", "Title"] == "Curated Maryland title"
    assert reconciled.loc["canonical-id", "Identifier"] == "hdl:1903.1/12533"
    assert reconciled.loc["canonical-id", "Website Platform"] == "Fedora"
    assert set(captured["distributions"]["friendlier_id"]) == {
        "canonical-id",
        "new-id",
    }
    assert Path(results["reconciliation_crosswalk_csv"]).is_file()
    ignored = pd.read_csv(
        results["reconciliation_ignored_enrichment_fields_csv"],
        dtype=str,
    )
    assert ignored.to_dict("records") == [
        {"field": "Created At", "nonblank_value_count": "1"}
    ]
