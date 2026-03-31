from unittest.mock import patch

from harvesters.ckan import CkanHarvester


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_ckan_fetch_paginates_and_maps_core_fields() -> None:
    config = {
        "base_url": "https://data.example.org",
        "site_title": "Example CKAN",
        "endpoint_url": "https://data.example.org/api/3/action/package_search",
        "rows": 1,
        "timeout": 30,
        "output_primary_csv": "outputs/ckan_primary.csv",
        "output_distributions_csv": "outputs/ckan_distributions.csv",
    }
    harvester = CkanHarvester(config)

    package_one = {
        "id": "pkg-1",
        "name": "roads",
        "title": "Road Centerlines 2024",
        "notes": "Centerline data",
        "metadata_created": "2024-01-01T10:00:00",
        "metadata_modified": "2024-02-02T11:00:00",
        "license_title": "CC0",
        "license_url": "https://example.com/license",
        "organization": {"title": "City of Pittsburgh"},
        "tags": [{"display_name": "roads"}, {"display_name": "transportation"}],
        "groups": [{"title": "Transportation"}],
        "resources": [
            {"url": "https://data.example.org/download/roads.geojson", "format": "GeoJSON"},
            {"url": "https://data.example.org/arcgis/rest/services/Roads/FeatureServer/0?f=pjson", "format": "FeatureServer"},
            {"url": "https://data.example.org/arcgis/rest/services/Roads/MapServer/2?f=pjson", "format": "Esri REST"},
        ],
        "private": False,
        "spatial": '{"type":"Polygon","coordinates":[[[-89.5,40.0],[-88.0,40.0],[-88.0,41.0],[-89.5,41.0],[-89.5,40.0]]]}',
        "geographic_unit": "Neighborhood",
    }
    package_two = {
        "id": "pkg-2",
        "name": "bridges",
        "title": "Bridge Locations",
        "notes": "Bridge inventory",
        "metadata_created": "2023-05-10T08:00:00",
        "metadata_modified": "2024-03-12T09:00:00",
        "license_title": "CC BY",
        "resources": [],
        "private": True,
        "organization": {"title": "Allegheny County"},
    }

    responses = [
        FakeResponse({"success": True, "result": {"count": 2, "results": [package_one]}}),
        FakeResponse({"success": True, "result": {"count": 2, "results": [package_two]}}),
    ]

    with patch("requests.Session.get", side_effect=responses) as mock_get:
        fetched = list(harvester.fetch())

    records = [item for item in fetched if isinstance(item, dict)]
    assert len(records) == 2
    assert mock_get.call_count == 2
    assert mock_get.call_args_list[0].kwargs["params"] == {"rows": 1, "start": 0}
    assert mock_get.call_args_list[1].kwargs["params"] == {"rows": 1, "start": 1}

    df = harvester.build_dataframe(records)
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)

    first = df.loc[df["ID"] == "pkg-1"].iloc[0]
    second = df.loc[df["ID"] == "pkg-2"].iloc[0]

    assert first["Title"] == "Road Centerlines 2024"
    assert first["Creator"] == "City of Pittsburgh"
    assert first["Keyword"] == "roads|transportation"
    assert first["Subject"] == "Transportation"
    assert first["Access Rights"] == "Public"
    assert first["Spatial Coverage"] == "Pennsylvania--Pittsburgh|Pennsylvania"
    assert first["Bounding Box"] == "-89.5,40.0,-88.0,41.0"
    assert first["information"] == "https://data.example.org/dataset/roads"
    assert first["featureService"] == "https://data.example.org/arcgis/rest/services/Roads/FeatureServer/0?f=pjson"
    assert first["mapService"] == "https://data.example.org/arcgis/rest/services/Roads/MapServer/2?f=pjson"
    assert first["geo_json"] == "https://data.example.org/download/roads.geojson"
    assert first["download"] == []
    assert first["Website Platform"] == "CKAN"
    assert first["Endpoint Description"] == "CKAN API (package_search)"

    assert second["Access Rights"] == "Restricted"
    assert second["Publisher"] == "Example CKAN"
    assert second["Spatial Coverage"] == "Pennsylvania--Allegheny County|Pennsylvania"
    assert second["Bounding Box"] == "-80.36,40.19,-79.69,40.67"


def test_ckan_spatial_coverage_defaults_to_pennsylvania() -> None:
    harvester = CkanHarvester(
        {
            "base_url": "https://data.example.org",
            "site_title": "Example CKAN",
            "output_primary_csv": "outputs/ckan_primary.csv",
            "output_distributions_csv": "outputs/ckan_distributions.csv",
        }
    )

    df = harvester.build_dataframe(
        [
            {
                "id": "pkg-3",
                "name": "stormwater-outfalls",
                "title": "Stormwater Outfalls",
                "notes": "",
                "organization": {"title": "Pennsylvania Department of Environmental Protection"},
                "resources": [],
            }
        ]
    )

    df = harvester.derive_fields(df)

    assert df.loc[df["ID"] == "pkg-3", "Spatial Coverage"].iloc[0] == "Pennsylvania"
    assert df.loc[df["ID"] == "pkg-3", "Bounding Box"].iloc[0] == "-80.36,40.19,-79.69,40.67"
