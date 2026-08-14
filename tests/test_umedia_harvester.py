from unittest.mock import patch

import pytest

from harvesters.base import BaseHarvester
from harvesters.umedia import UmediaHarvester
from utils.distribution_writer import generate_secondary_table
from utils.field_order import FIELD_ORDER


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def _config(**overrides):
    config = {
        "base_url": "https://umedia.example/search.json",
        "facets": {"organization_s": "Borchert Map Library"},
        "max_items": 100,
        "page_size": 2,
        "timeout": 15,
        "output_primary_csv": "outputs/umedia_primary.csv",
        "output_distributions_csv": "outputs/umedia_distributions.csv",
    }
    config.update(overrides)
    return config


def _record(item_id="maps:265", parent_id="265"):
    return {
        "id": item_id,
        "set_spec": "maps",
        "parent_id": parent_id,
        "title": "Map of Example County",
        "description": "Relief shown by hachures.",
        "notes": "Includes an index.",
        "dimensions": "51 x 33 centimeters",
        "creator": ["Example, Alex", "Cartographer, Casey"],
        "publisher": ["Example Press", "1909"],
        "subject": ["Example County", "Maps"],
        "date_created": ["1909", "1911"],
        "date_created_sort": "1909 1911",
        "date_added": "2024-05-15T12:34:56Z",
        "language": ["English", "French"],
        "state": ["Minnesota"],
        "city": ["Minneapolis", "St. Paul"],
        "region": ["Middle West"],
        "country": ["United States"],
        "continent": ["North America"],
        "scale": "Scale 1:24,000",
        "coordinates": [
            "W0931600 W0931500 N0450100 N0445900",
            "(W 93°16'--W 93°15'/N 45°01'--N 44°59')",
        ],
        "local_rights": "Copyright status unknown.",
        "collection_name": "City Maps",
        "contributing_organization": "University of Minnesota Libraries",
        "thumb_url": "https://images.example/maps/265.jpg",
    }


def test_umedia_fetches_paginated_faceted_results_up_to_limit():
    harvester = UmediaHarvester(_config(max_items=3))
    responses = [
        FakeResponse([_record("maps:1", "1"), _record("maps:2", "2")]),
        FakeResponse([_record("maps:3", "3"), _record("maps:4", "4")]),
    ]

    with patch("harvesters.umedia.requests.get", side_effect=responses) as mock_get:
        records = harvester.fetch()

    assert [record["id"] for record in records] == ["maps:1", "maps:2", "maps:3"]
    assert mock_get.call_count == 2
    assert mock_get.call_args_list[0].kwargs == {
        "params": {
            "facets[organization_s][]": "Borchert Map Library",
            "page": 0,
        },
        "timeout": 15.0,
    }
    assert mock_get.call_args_list[1].kwargs["params"]["page"] == 1


def test_umedia_maps_notebook_fields_and_builds_distribution_urls():
    harvester = UmediaHarvester(_config())
    df = harvester.build_dataframe([_record()])
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)
    row = df.iloc[0]

    assert row["ID"] == "maps:265"
    assert row["Title"] == "Map of Example County"
    assert row["Alternative Title"] == "Map of Example County"
    assert row["Description"] == (
        "Relief shown by hachures.|Includes an index.|51 x 33 centimeters"
    )
    assert row["Creator"] == "Example, Alex|Cartographer, Casey"
    assert row["Publisher"] == "Example Press|1909"
    assert row["Keyword"] == "Example County|Maps"
    assert row["Date Issued"] == "1909;1911"
    assert row["Temporal Coverage"] == "1909|1911"
    assert row["Date Range"] == "1909-1911"
    assert row["Index Year"] == "1909"
    assert row["Language"] == "eng|fre"
    assert row["Spatial Coverage"] == (
        "Minnesota--Minneapolis|Minnesota--Saint Paul|Minnesota--Middle West|"
        "Minnesota|United States"
    )
    assert row["Spatial Resolution as Text"] == "Scale 1:24,000"
    assert row["Bounding Box"] == "-93.266667,44.983333,-93.25,45.016667"
    assert row["Admin Note"] == (
        "Draft source coordinates; cleanup required: "
        "W0931600 W0931500 N0450100 N0445900|"
        "Draft source coordinates; cleanup required: "
        "(W 93°16'--W 93°15'/N 45°01'--N 44°59')"
    )
    assert row["Rights"] == "Copyright status unknown."
    assert row["Resource Class"] == "Maps"
    assert row["Format"] == "JPEG"
    assert row["Code"] == "05d-01"
    assert row["Identifier"] == "https://umedia.lib.umn.edu/item/maps:265"
    assert row["information"] == "https://umedia.lib.umn.edu/item/maps:265"
    assert row["download"] == (
        "https://cdm16022.contentdm.oclc.org/utils/getfile/collection/maps/id/265"
        "/filename/print/page/download/fparams/forcedownload"
    )
    assert row["manifest"] == (
        "https://cdm16022.contentdm.oclc.org/iiif/info/maps/265/manifest.json"
    )
    assert row["thumbnail"] == "https://images.example/maps/265.jpg"
    assert row["Website Platform"] == "uMedia"
    assert row["Harvest Workflow"] == "py_umedia"
    assert "date_added value is 2024-05-15T12:34:56Z" in row["Provenance"]
    assert "umedia_date_added" not in df.columns

    harvester.load_reference_data()
    distributions = generate_secondary_table(df, harvester.distribution_types)
    assert set(distributions["reference_type"]) == {
        "documentation_external",
        "download",
        "iiif_manifest",
        "thumbnail",
    }


def test_umedia_handles_missing_optional_fields_and_empty_results():
    harvester = UmediaHarvester(_config())
    row = harvester.build_dataframe(
        [{"id": "maps:1", "title": "Untitled map"}]
    ).iloc[0]

    assert row["Description"] == ""
    assert row["Language"] == ""
    assert row["Date Range"] == ""
    assert row["download"] == ""
    assert row["manifest"] == ""
    assert row["Bounding Box"] == ""

    empty_df = harvester.build_dataframe([])
    assert empty_df.empty
    assert {"ID", "Title", "Bounding Box", "information"}.issubset(empty_df.columns)
    assert empty_df.columns.tolist() == FIELD_ORDER


def test_umedia_spatial_coverage_uses_country_when_state_is_absent():
    harvester = UmediaHarvester(_config())
    row = harvester.build_dataframe(
        [
            {
                "id": "maps:2",
                "title": "European map",
                "country": ["Sweden; Finland"],
                "city": ["Stockholm"],
            }
        ]
    ).iloc[0]

    assert row["Spatial Coverage"] == (
        "Sweden--Stockholm|Finland--Stockholm|Sweden|Finland"
    )


def test_umedia_filters_records_using_date_added_on_or_after():
    harvester = UmediaHarvester(
        _config(date_added_on_or_after="2024-05-15", build_uploads=True)
    )
    records = [
        {"id": "before", "date_added": "2024-05-14T23:59:59Z"},
        {"id": "same-day", "date_added": "2024-05-15T00:00:00Z"},
        {"id": "after", "date_added": "2024-06-01"},
        {"id": "missing"},
        {"id": "invalid", "date_added": "not-a-date"},
    ]

    filtered = harvester.flatten(records)

    assert [record["id"] for record in filtered] == ["same-day", "after"]
    assert harvester.config["build_uploads"] is False


def test_umedia_rejects_invalid_date_added_cutoff():
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        UmediaHarvester(_config(date_added_on_or_after="05/15/2024"))


def test_umedia_class_only_defines_base_harvester_lifecycle_methods():
    custom_methods = {
        name
        for name, value in UmediaHarvester.__dict__.items()
        if callable(value) and not name.startswith("__")
    }
    base_methods = {
        name
        for name, value in BaseHarvester.__dict__.items()
        if callable(value)
    }

    assert custom_methods <= base_methods
