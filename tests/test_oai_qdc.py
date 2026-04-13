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
                "1,N,1,W,IA,,,,\"1,2,3,4\"",
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

    harvester = OaiQdcHarvester(_config(sets_csv, **_write_spatial_lookup_csvs(tmp_path)))
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


def test_oai_qdc_does_not_confuse_des_moines_city_with_des_moines_county(tmp_path) -> None:
    sets_csv = tmp_path / "sets.csv"
    sets_csv.write_text("set,title\nnode:1,Sample Set\n", encoding="utf-8")

    harvester = OaiQdcHarvester(_config(sets_csv, **_write_spatial_lookup_csvs(tmp_path)))
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

    harvester = OaiQdcHarvester(_config(sets_csv, **_write_spatial_lookup_csvs(tmp_path)))
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
