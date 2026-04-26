import csv

from scripts.enrich_arcgis_service_projections import (
    build_metadata_url,
    enrich_rows_with_projections,
    extract_spatial_reference,
    format_projection,
    get_output_fieldnames,
    is_arcgis_service_url,
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


def test_build_metadata_url_replaces_arcmap_query_with_pjson() -> None:
    assert build_metadata_url(
        "https://example.com/arcgis/rest/services/demo/MapServer?f=lyr&v=9.3"
    ) == "https://example.com/arcgis/rest/services/demo/MapServer?f=pjson"


def test_is_arcgis_service_url_handles_service_and_layer_endpoints() -> None:
    assert is_arcgis_service_url("https://example.com/rest/services/demo/MapServer")
    assert is_arcgis_service_url("https://example.com/rest/services/demo/FeatureServer/0")
    assert is_arcgis_service_url("https://example.com/rest/services/demo/ImageServer")
    assert not is_arcgis_service_url("https://example.com/about")


def test_extract_spatial_reference_and_format_projection_prefer_latest_wkid() -> None:
    payload = {
        "fullExtent": {
            "spatialReference": {
                "wkid": 102100,
                "latestWkid": 3857,
            }
        }
    }

    spatial_reference = extract_spatial_reference(payload)

    assert spatial_reference == {"wkid": 102100, "latestWkid": 3857}
    assert format_projection(spatial_reference) == "EPSG:3857 (wkid 102100)"


def test_enrich_rows_with_projections_updates_service_rows_and_caches_requests(tmp_path) -> None:
    rows = [
        {
            "friendlier_id": "row-1",
            "reference_type": "arcgis_dynamic_map_layer",
            "distribution_url": "https://example.com/rest/services/demo/MapServer?f=lyr&v=9.3",
            "label": "",
        },
        {
            "friendlier_id": "row-2",
            "reference_type": "arcgis_dynamic_map_layer",
            "distribution_url": "https://example.com/rest/services/demo/MapServer?f=lyr&v=9.3",
            "label": "",
        },
        {
            "friendlier_id": "row-3",
            "reference_type": "documentation_external",
            "distribution_url": "https://example.com/about",
            "label": "",
        },
    ]
    metadata_url = "https://example.com/rest/services/demo/MapServer?f=pjson"
    session = FakeSession(
        {
            metadata_url: {
                "spatialReference": {
                    "wkid": 4326,
                }
            }
        }
    )

    enriched_rows, counts = enrich_rows_with_projections(rows, session=session, timeout=9)

    assert [row.get("projection", "") for row in enriched_rows] == [
        "EPSG:4326",
        "EPSG:4326",
        "",
    ]
    assert counts == {
        "rows_processed": 3,
        "service_rows_seen": 2,
        "rows_updated": 2,
        "errors": 0,
        "cache_hits": 1,
    }
    assert session.requested_urls == [(metadata_url, 9)]

    output_csv = tmp_path / "projection_output.csv"
    fieldnames = get_output_fieldnames(
        ["friendlier_id", "reference_type", "distribution_url", "label"]
    )
    write_rows(output_csv, fieldnames, enriched_rows)

    with output_csv.open("r", newline="", encoding="utf-8") as handle:
        written_rows = list(csv.DictReader(handle))

    assert written_rows[0]["projection"] == "EPSG:4326"
    assert written_rows[2]["projection"] == ""
