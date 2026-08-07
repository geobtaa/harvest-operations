import csv
from pathlib import Path

from scripts.inventory_gdrs import (
    extract_metadata,
    inventory_gdrs,
    normalize_fgdc_date,
    resolve_pub_root,
    select_preferred_format_rows,
    write_inventory,
)


FGDC_XML = """\
<?xml version="1.0"?>
<metadata>
  <idinfo>
    <citation>
      <citeinfo>
        <origin>Example Agency</origin>
        <origin>Example Department</origin>
        <pubdate>20171206</pubdate>
        <title>Example roads</title>
      </citeinfo>
    </citation>
  </idinfo>
</metadata>
"""


def _make_resource(pub: Path, organization: str, resource: str) -> Path:
    resource_dir = pub / organization / resource
    (resource_dir / "metadata").mkdir(parents=True)
    (resource_dir / "metadata" / "metadata.xml").write_text(FGDC_XML, encoding="utf-8")
    return resource_dir


def test_inventory_counts_composite_datasets_once_and_skips_external(tmp_path) -> None:
    pub = tmp_path / "archive" / "data" / "pub"
    resource = _make_resource(pub, "example_org", "roads")

    geodatabase = resource / "fgdb" / "roads.gdb"
    geodatabase.mkdir(parents=True)
    (geodatabase / "table.gdbtable").write_bytes(b"1234")
    (geodatabase / "table.gdbtablx").write_bytes(b"12")
    (resource / "fgdb" / "empty.gdb").mkdir()

    shapefile_dir = resource / "shp"
    shapefile_dir.mkdir()
    (shapefile_dir / "roads.shp").write_bytes(b"123")
    (shapefile_dir / "roads.dbf").write_bytes(b"12345")
    (shapefile_dir / "roads.lyr").write_bytes(b"not data")
    (shapefile_dir / "roads.shp.xml").write_bytes(b"metadata")

    external_resource = _make_resource(pub, "example_org", "external_app")
    (external_resource / "external").mkdir()

    unfiled_resource = _make_resource(pub, "example_org", "unfiled")
    unfiled_geodatabase = unfiled_resource / "unfiled.gdb"
    unfiled_geodatabase.mkdir()
    (unfiled_geodatabase / "table.gdbtable").write_bytes(b"123")

    rows, stats = inventory_gdrs(tmp_path / "archive")

    assert len(rows) == 3
    assert stats.resources == 3
    assert stats.resources_with_stored_data == 2
    assert stats.resources_without_stored_data == 1

    rows_by_format = {row.format_folder: row for row in rows if row.resource == "roads"}
    assert rows_by_format["fgdb"].dataset_filename == "roads.gdb"
    assert rows_by_format["fgdb"].filesize_bytes == 6
    assert rows_by_format["fgdb"].component_count == 2
    assert rows_by_format["shp"].dataset_filename == "roads.shp"
    assert rows_by_format["shp"].filesize_bytes == 8
    assert rows_by_format["shp"].component_count == 2
    assert rows_by_format["shp"].title == "Example roads"
    assert rows_by_format["shp"].creator_originator == (
        "Example Agency; Example Department"
    )
    assert rows_by_format["shp"].publication_date == "2017-12-06"
    assert rows_by_format["shp"].date_source == "FGDC pubdate"
    unfiled_row = next(row for row in rows if row.resource == "unfiled")
    assert unfiled_row.format_folder == "fgdb"
    assert unfiled_row.storage_location == "resource_root"

    preferred_rows = select_preferred_format_rows(rows)
    preferred_formats = {(row.resource, row.format_folder) for row in preferred_rows}
    assert preferred_formats == {("roads", "fgdb"), ("unfiled", "fgdb")}


def test_preferred_formats_use_requested_fallback_order(tmp_path) -> None:
    pub = tmp_path / "pub"
    resource = _make_resource(pub, "org", "places")
    for folder, filename in (
        ("csv", "places.csv"),
        ("geojson", "places.geojson"),
        ("kmz", "places.kml"),
    ):
        format_dir = resource / folder
        format_dir.mkdir()
        (format_dir / filename).write_text("data", encoding="utf-8")

    spreadsheet_resource = _make_resource(pub, "org", "spreadsheet_only")
    xlsx_dir = spreadsheet_resource / "xlsx"
    xlsx_dir.mkdir()
    (xlsx_dir / "data.xlsx").write_bytes(b"spreadsheet")

    rows, _ = inventory_gdrs(pub)
    preferred_rows = select_preferred_format_rows(rows)

    selected_formats = {row.resource: row.format_folder for row in preferred_rows}
    assert selected_formats == {"places": "kmz", "spreadsheet_only": "xlsx"}


def test_metadata_falls_back_to_data_resource_fields(tmp_path) -> None:
    pub = tmp_path / "pub"
    resource = pub / "org" / "resource"
    resource.mkdir(parents=True)
    (resource / "dataResource.xml").write_text(
        "<dataResource><descriptiveName>Fallback title</descriptiveName>"
        "<publisher>Fallback publisher</publisher>"
        "<currentAsofDate>2016-04-03</currentAsofDate></dataResource>",
        encoding="utf-8",
    )

    metadata, missing, parse_error = extract_metadata(resource, pub)

    assert missing is True
    assert parse_error is False
    assert metadata.title == "Fallback title"
    assert metadata.creator_originator == "Fallback publisher"
    assert metadata.publication_date == "2016-04-03"
    assert metadata.date_source == "GDRS currentAsofDate"


def test_csv_text_files_exclude_documentation(tmp_path) -> None:
    pub = tmp_path / "pub"
    resource = _make_resource(pub, "org", "transit")
    csv_dir = resource / "csv"
    csv_dir.mkdir()
    (csv_dir / "stops.txt").write_text("stop_id,stop_name\n1,Here\n", encoding="utf-8")
    (csv_dir / "readme.txt").write_text("documentation", encoding="utf-8")

    rows, _ = inventory_gdrs(pub)

    assert [row.dataset_filename for row in rows] == ["stops.txt"]


def test_write_inventory_uses_expected_columns(tmp_path) -> None:
    pub = tmp_path / "pub"
    resource = _make_resource(pub, "org", "places")
    geojson_dir = resource / "geojson"
    geojson_dir.mkdir()
    (geojson_dir / "places.geojson").write_text("{}", encoding="utf-8")
    rows, _ = inventory_gdrs(pub)
    output = tmp_path / "inventory.csv"

    write_inventory(output, rows)

    with output.open(encoding="utf-8", newline="") as handle:
        written = list(csv.DictReader(handle))
    assert written[0]["dataset_filename"] == "places.geojson"
    assert written[0]["format"] == "GeoJSON"
    assert written[0]["filesize_bytes"] == "2"


def test_resolve_pub_root_and_date_normalization(tmp_path) -> None:
    pub = tmp_path / "data" / "pub"
    pub.mkdir(parents=True)

    assert resolve_pub_root(tmp_path) == pub.resolve()
    assert resolve_pub_root(pub) == pub.resolve()
    assert normalize_fgdc_date("20171102") == "2017-11-02"
    assert normalize_fgdc_date("20171340") == "20171340"
    assert normalize_fgdc_date("Unknown") == "Unknown"
