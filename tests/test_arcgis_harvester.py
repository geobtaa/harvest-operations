from pathlib import Path
from unittest.mock import patch

from harvesters.arcgis import ArcGISHarvester


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_CSV = ROOT / "tests" / "fixtures" / "arcgis_test_hubs.csv"


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def _config() -> dict:
    return {
        "input_csv": str(FIXTURE_CSV),
        "output_primary_csv": "outputs/arcgis_primary.csv",
        "output_distributions_csv": "outputs/arcgis_distributions.csv",
    }


def test_arcgis_harvester_processes_fixture_hubs_and_appends_hub_status_rows() -> None:
    harvester = ArcGISHarvester(_config())

    payload_by_url = {
        "https://opendata.minneapolismn.gov/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "Street Centerlines 2024",
                    "description": "Centerline features for Minneapolis streets.",
                    "publisher": {"name": "City of Minneapolis"},
                    "keyword": ["roads", "transportation"],
                    "issued": "2024-01-10T00:00:00",
                    "modified": "2024-05-01T12:00:00",
                    "license": "Open Data",
                    "identifier": "https://hub.arcgis.com/datasets?id=roads123&sublayer=2",
                    "landingPage": "https://example.org/datasets/roads123",
                    "spatial": "-93.30,44.90,-93.20,45.00",
                    "distribution": [
                        {
                            "title": "Shapefile",
                            "accessURL": "https://downloads.example.org/roads.zip",
                        },
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Roads/FeatureServer/2",
                        },
                    ],
                }
            ]
        },
        "https://holmes-county-gis-holmesgis.hub.arcgis.com/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "County Imagery 2019",
                    "description": "Current county imagery tiles.",
                    "publisher": "Holmes County GIS",
                    "keyword": ["imagery"],
                    "issued": "",
                    "modified": "2023-02-02T09:30:00",
                    "license": "Public Domain",
                    "identifier": "https://hub.arcgis.com/datasets?id=imagery999",
                    "landingPage": "https://example.org/datasets/imagery999",
                    "spatial": "-81.90,40.50,-81.90,40.60",
                    "distribution": [
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Imagery/ImageServer",
                        }
                    ],
                }
            ]
        },
        "https://gisdata-jeffcowa.opendata.arcgis.com/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "Parcels",
                    "description": "County parcel boundaries.",
                    "publisher": "Jefferson County Assessor",
                    "keyword": ["parcel", "polygon"],
                    "issued": "",
                    "modified": "2022-07-15T00:00:00",
                    "license": "Open",
                    "identifier": "https://hub.arcgis.com/datasets?id=parcel456",
                    "landingPage": "https://example.org/datasets/parcel456",
                    "spatial": "-122.600,48.370,-124.720,47.520",
                    "distribution": [
                        {
                            "title": "Shapefile",
                            "accessURL": "https://downloads.example.org/parcels.zip",
                        },
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Parcels/MapServer",
                        },
                    ],
                }
            ]
        },
    }

    def fake_get(url, timeout):
        assert timeout == 30
        return FakeResponse(payload_by_url[url])

    with patch("requests.get", side_effect=fake_get) as mock_get:
        fetched = list(harvester.fetch())

    status_messages = [item for item in fetched if isinstance(item, str)]
    fetched_rows = [item for item in fetched if isinstance(item, dict)]

    assert len(status_messages) == 3
    assert len(fetched_rows) == 3
    assert mock_get.call_count == 3

    df = harvester.flatten(fetched)
    df = harvester.build_dataframe(df)
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)
    df = harvester.clean(df)
    df = harvester.validate(df)

    street_row = df.loc[df["ID"] == "roads123_2"].iloc[0]
    imagery_row = df.loc[df["ID"] == "imagery999"].iloc[0]
    parcels_row = df.loc[df["ID"] == "parcel456"].iloc[0]

    assert street_row["Title"] == "Street Centerlines 2024 [Minnesota--Minneapolis]"
    assert street_row["Identifier"] == "https://hub.arcgis.com/datasets/roads123_2"
    assert street_row["Creator"] == "City of Minneapolis"
    assert street_row["Date Range"] == "2024-2024"
    assert street_row["featureService"] == "https://services.example.org/arcgis/rest/services/Roads/FeatureServer/2"
    assert street_row["Format"] == "ArcGIS FeatureLayer"
    assert street_row["Bounding Box"] == "-93.300,44.900,-93.200,45.000"

    assert imagery_row["Title"] == "County Imagery 2019 [Ohio--Holmes County]"
    assert imagery_row["Identifier"] == "https://hub.arcgis.com/datasets/imagery999"
    assert imagery_row["Date Range"] == "2019-2019"
    assert imagery_row["imageService"] == "https://services.example.org/arcgis/rest/services/Imagery/ImageServer"
    assert imagery_row["Format"] == "ArcGIS ImageMapLayer"
    assert imagery_row["Bounding Box"] == "-82.221,40.444,-81.649,40.668"

    assert parcels_row["Title"] == "Parcels [Washington (State)--Jefferson County]"
    assert parcels_row["Date Range"] == "2022-2022"
    assert parcels_row["mapService"] == "https://services.example.org/arcgis/rest/services/Parcels/MapServer"
    assert parcels_row["Format"] == "ArcGIS DynamicMapLayer"
    assert parcels_row["Bounding Box"] == "-124.720,47.520,-122.600,48.370"
    assert parcels_row["Resource Type"] == "Polygon data"

    hub_rows = df.loc[df["Resource Class"] == "Websites"].copy()

    assert len(hub_rows) == 3
    assert set(hub_rows["ID"]) == {"05c-01", "11b-39075", "16b-53031"}
    assert set(hub_rows["Is Harvested"]) == {"True"}
    assert all(hub_rows["Last Harvested"].astype(str).str.match(r"\d{4}-\d{2}-\d{2}"))


def test_arcgis_harvester_reads_workflow_inputs_and_metadata_defaults(tmp_path) -> None:
    workflow_csv = tmp_path / "py-arcgis-hub.csv"
    metadata_csv = tmp_path / "arcHub_metadata.csv"

    workflow_csv.write_text(
        "\n".join(
            [
                "Title,Provenance,Endpoint URL,Endpoint Description,Is Harvested,Last Harvested,Accrual Method,Accrual Periodicity,Harvest Workflow,ID,Identifier,Code,Tags,Admin Note",
                "Harvest record for Open Data Minneapolis,2026-03-28 / harvest,https://opendata.minneapolismn.gov/api/feed/dcat-us/1.1.json,DCAT API,,2026-03-28,Automated retrieval,Weekly,py_arcgis_hub,harvest_05c-01,05c-01,05c-01,,",
                "Harvest record for Holmes County GIS Open Data Portal,2026-03-22 / harvest,https://holmes-county-gis-holmesgis.hub.arcgis.com/api/feed/dcat-us/1.1.json,DCAT API,,2026-03-22,Automated retrieval,Weekly,py_arcgis_hub,harvest_11b-39075,11b-39075,11b-39075,,",
                "Harvest record for Jefferson County Washington Open Data Site,2026-03-28 / harvest,https://gisdata-jeffcowa.opendata.arcgis.com/api/feed/dcat-us/1.1.json,DCAT API,,2026-03-28,Automated retrieval,Weekly,py_arcgis_hub,harvest_16b-53031,16b-53031,16b-53031,,",
            ]
        ),
        encoding="utf-8",
    )
    metadata_csv.write_text(
        "\n".join(
            [
                "Title,Creator,Publisher,Subject,Spatial Coverage,Bounding Box,Member Of,ID,Identifier,Code",
                'Open Data Minneapolis,,Minnesota--Minneapolis,Municipal government records,Minnesota--Minneapolis|Minnesota,"-93.329,44.890,-93.194,45.051",ba5cc745-21c5-4ae9-954b-72dd8db6815a,05c-01,https://opendata.minneapolismn.gov,05c-01',
                'Holmes County GIS Open Data Portal,,Ohio--Holmes County,County government records,Ohio--Holmes County|Ohio,"-82.221,40.444,-81.649,40.668",ba5cc745-21c5-4ae9-954b-72dd8db6815a,11b-39075,https://holmes-county-gis-holmesgis.hub.arcgis.com,11b-39075',
                'Jefferson County Washington Open Data Site,,Washington (State)--Jefferson County,County government records,Washington (State)--Jefferson County|Washington (State),"-124.720,47.520,-122.600,48.370",ba5cc745-21c5-4ae9-954b-72dd8db6815a,16b-53031,https://gisdata-jeffcowa.opendata.arcgis.com,16b-53031',
            ]
        ),
        encoding="utf-8",
    )

    harvester = ArcGISHarvester(
        {
            "input_csv": str(workflow_csv),
            "hub_metadata_csv": str(metadata_csv),
            "output_primary_csv": "outputs/arcgis_primary.csv",
            "output_distributions_csv": "outputs/arcgis_distributions.csv",
        }
    )

    payload_by_url = {
        "https://opendata.minneapolismn.gov/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "Street Centerlines 2024",
                    "description": "Centerline features for Minneapolis streets.",
                    "publisher": {"name": "City of Minneapolis"},
                    "keyword": ["roads", "transportation"],
                    "issued": "2024-01-10T00:00:00",
                    "modified": "2024-05-01T12:00:00",
                    "license": "Open Data",
                    "identifier": "https://hub.arcgis.com/datasets?id=roads123&sublayer=2",
                    "landingPage": "https://example.org/datasets/roads123",
                    "spatial": "-93.30,44.90,-93.20,45.00",
                    "distribution": [
                        {
                            "title": "Shapefile",
                            "accessURL": "https://downloads.example.org/roads.zip",
                        },
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Roads/FeatureServer/2",
                        },
                    ],
                }
            ]
        },
        "https://holmes-county-gis-holmesgis.hub.arcgis.com/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "County Imagery 2019",
                    "description": "Current county imagery tiles.",
                    "publisher": "Holmes County GIS",
                    "keyword": ["imagery"],
                    "issued": "",
                    "modified": "2023-02-02T09:30:00",
                    "license": "Public Domain",
                    "identifier": "https://hub.arcgis.com/datasets?id=imagery999",
                    "landingPage": "https://example.org/datasets/imagery999",
                    "spatial": "-81.90,40.50,-81.90,40.60",
                    "distribution": [
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Imagery/ImageServer",
                        }
                    ],
                }
            ]
        },
        "https://gisdata-jeffcowa.opendata.arcgis.com/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "Parcels",
                    "description": "County parcel boundaries.",
                    "publisher": "Jefferson County Assessor",
                    "keyword": ["parcel", "polygon"],
                    "issued": "",
                    "modified": "2022-07-15T00:00:00",
                    "license": "Open",
                    "identifier": "https://hub.arcgis.com/datasets?id=parcel456",
                    "landingPage": "https://example.org/datasets/parcel456",
                    "spatial": "-122.600,48.370,-124.720,47.520",
                    "distribution": [
                        {
                            "title": "Shapefile",
                            "accessURL": "https://downloads.example.org/parcels.zip",
                        },
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Parcels/MapServer",
                        },
                    ],
                }
            ]
        },
    }

    def fake_get(url, timeout):
        assert timeout == 30
        return FakeResponse(payload_by_url[url])

    with patch("requests.get", side_effect=fake_get):
        fetched = list(harvester.fetch())

    df = harvester.flatten(fetched)
    df = harvester.build_dataframe(df)
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)
    df = harvester.clean(df)
    df = harvester.validate(df)

    street_row = df.loc[df["ID"] == "roads123_2"].iloc[0]
    minneapolis_hub = df.loc[df["ID"] == "05c-01"].iloc[0]

    assert street_row["Is Part Of"] == "05c-01"
    assert street_row["Code"] == "05c-01"
    assert street_row["Publisher"] == "Open Data Minneapolis"
    assert street_row["Endpoint Description"] == "DCAT API"
    assert street_row["Accrual Periodicity"] == "Weekly"

    assert minneapolis_hub["Title"] == "Open Data Minneapolis"
    assert minneapolis_hub["Resource Class"] == "Websites"
    assert minneapolis_hub["Resource Type"] == "Data portals"
    assert minneapolis_hub["Endpoint URL"] == "https://opendata.minneapolismn.gov/api/feed/dcat-us/1.1.json"
    assert minneapolis_hub["Provenance"] == "2026-03-28 / harvest"
    assert minneapolis_hub["Harvest Workflow"] == "py_arcgis_hub"
