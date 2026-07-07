import csv

from scripts.query_psu_distribution_metadata import (
    build_metadata_url,
    extract_extent,
    extract_name,
    extract_spatial_reference,
    format_extent,
    format_spatial_reference,
    query_distribution_urls,
    query_source_rows,
    read_distribution_urls,
    read_source_rows,
    write_rows,
)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payloads_by_url):
        self.payloads_by_url = payloads_by_url
        self.requested_urls = []

    def get(self, url, timeout=20):
        self.requested_urls.append((url, timeout))
        return FakeResponse(self.payloads_by_url[url])


def test_build_metadata_url_replaces_existing_format_query() -> None:
    assert build_metadata_url(
        "https://example.com/arcgis/rest/services/demo/MapServer/0?f=lyr&v=9.3"
    ) == "https://example.com/arcgis/rest/services/demo/MapServer/0?f=pjson"


def test_extracts_name_extent_and_spatial_reference() -> None:
    payload = {
        "name": "Layer name",
        "extent": {
            "xmin": -80.5,
            "ymin": 39.25,
            "xmax": -74,
            "ymax": 42,
            "spatialReference": {
                "wkid": 102100,
                "latestWkid": 3857,
            },
        },
    }

    extent = extract_extent(payload)
    spatial_reference = extract_spatial_reference(payload)

    assert extract_name(payload) == "Layer name"
    assert format_extent(extent) == "-80.5,39.25,-74,42"
    assert format_spatial_reference(spatial_reference) == "EPSG:3857 (wkid 102100)"


def test_query_distribution_urls_writes_requested_output_columns(tmp_path) -> None:
    input_csv = tmp_path / "psu.csv"
    input_csv.write_text(
        "friendlier_id,reference_type,distribution_url,label\n"
        "row-1,arcgis_dynamic_map_layer,https://example.com/rest/services/demo/MapServer/0,\n",
        encoding="utf-8",
    )
    metadata_url = "https://example.com/rest/services/demo/MapServer/0?f=pjson"
    session = FakeSession(
        {
            metadata_url: {
                "name": "Parcels",
                "extent": {
                    "xmin": -80,
                    "ymin": 39,
                    "xmax": -74,
                    "ymax": 42,
                    "spatialReference": {"wkid": 4326},
                },
            }
        }
    )

    rows, counts = query_source_rows(
        read_source_rows(input_csv),
        session=session,
        timeout=7,
    )
    output_csv = tmp_path / "metadata.csv"
    write_rows(output_csv, rows)

    with output_csv.open("r", newline="", encoding="utf-8") as handle:
        written_rows = list(csv.DictReader(handle))

    assert counts == {
        "urls_processed": 1,
        "arcgis_urls_seen": 1,
        "errors": 0,
        "cache_hits": 0,
    }
    assert session.requested_urls == [(metadata_url, 7)]
    assert written_rows == [
        {
            "friendlier_id": "row-1",
            "distribution_url": "https://example.com/rest/services/demo/MapServer/0",
            "name": "Parcels",
            "extent": "-80,39,-74,42",
            "spatial_reference": "EPSG:4326",
        }
    ]


def test_read_distribution_urls_remains_available(tmp_path) -> None:
    input_csv = tmp_path / "psu.csv"
    input_csv.write_text(
        "friendlier_id,reference_type,distribution_url,label\n"
        "row-1,arcgis_dynamic_map_layer,https://example.com/rest/services/demo/MapServer/0,\n",
        encoding="utf-8",
    )

    assert read_distribution_urls(input_csv) == [
        "https://example.com/rest/services/demo/MapServer/0"
    ]
    rows, _ = query_distribution_urls(
        ["https://example.com/about"],
        session=FakeSession({}),
    )
    assert rows[0]["friendlier_id"] == ""
