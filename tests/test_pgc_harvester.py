from collections import Counter
from pathlib import Path

import pandas as pd

from harvesters.pgc import (
    PgcHarvester,
    apply_pgc_bbox_inferences,
    apply_pgc_legacy_enrichments,
    clean_pgc_spatial_fields,
    format_pgc_bounding_box,
    load_pgc_bbox_inferences,
    load_pgc_legacy_metadata,
    pgc_bounding_box,
    pgc_centroid,
    pgc_downloads,
    pgc_preview,
    pgc_record_to_schema,
)
from utils.distribution_writer import generate_secondary_table
from utils.validation import geometry_validation_error


def _record(**overrides):
    record = {
        "_pgc_region_slug": "antarctica",
        "map_id": 2,
        "reference_id": "ANT REF-MT2503-002",
        "title": "Argentina Range",
        "region_name": "Antarctica",
        "area_name": "Argentina Range",
        "auth_year": 1967,
        "auth_person": "",
        "org_name": "United States Geological Survey",
        "projection": "Polar Stereographic Projection",
        "series_name": "USGS Topographic Reconnaissance Series",
        "theme_name": "General Purpose",
        "purpose_name": "Reference",
        "type_name": "Topographic",
        "lat_lower_left": -83,
        "lat_upper_right": -82,
        "lon_lower_left": -45,
        "lon_upper_right": -37.5,
        "scale": 250000,
        "filename": "Argentina Range",
        "url_base": "https://data.pgc.umn.edu/maps/antarctica/usgs/03/",
        "is_geo": 1,
        "is_crop": 1,
        "is_tif": 1,
        "is_pdf": 1,
        "is_preview": 1,
        "status": 1,
    }
    record.update(overrides)
    return record


def test_load_legacy_crosswalk_and_metadata(tmp_path: Path) -> None:
    primary = tmp_path / "primary.csv"
    distributions = tmp_path / "distributions.csv"
    pd.DataFrame(
        [
            {
                "ID": "05d-06",
                "Creator": "",
                "Creator ID": "",
                "Language": "eng",
                "Spatial Coverage": "",
                "Keyword": "",
                "Tags": "",
                "Date Accessioned": "2019-05-03",
                "Bounding Box": "",
                "Geometry": "",
                "Centroid": "",
            },
            {
                "ID": "ANT-REF-MT2503-002",
                "Creator": "Curated creator",
                "Creator ID": "authority-1",
                "Language": "eng",
                "Spatial Coverage": "Antarctica",
                "Keyword": "legacy term",
                "Tags": "legacy-tag",
                "Date Accessioned": "2019-05-03",
                "Bounding Box": "-45,-83,-37.5,-82",
                "Geometry": "ENVELOPE(-45,-37.5,-82,-83)",
                "Centroid": "-82.5,-41.25",
            },
        ]
    ).to_csv(primary, index=False)
    pd.DataFrame(
        [
            {
                "friendlier_id": "ANT-REF-MT2503-002",
                "reference_type": "documentation_external",
                "distribution_url": "https://maps.apps.pgc.umn.edu/id/2",
            }
        ]
    ).to_csv(distributions, index=False)

    crosswalk, metadata = load_pgc_legacy_metadata(primary, distributions)

    assert crosswalk == {"2": "ANT-REF-MT2503-002"}
    assert metadata["ANT-REF-MT2503-002"]["Creator ID"] == "authority-1"
    assert "05d-06" not in metadata


def test_record_mapping_preserves_legacy_id_and_builds_current_links() -> None:
    record = _record(map_description="Source narrative.")

    row = pgc_record_to_schema(
        record,
        legacy_id_by_map_id={"2": "ANT-REF-MT2503-002"},
        reference_counts=Counter({"ANT-REF-MT2503-002": 1}),
        catalog_base_url="https://maps.apps.pgc.umn.edu",
    )

    assert row["ID"] == "ANT-REF-MT2503-002"
    assert row["Title"] == "Argentina Range: Antarctica"
    assert row["Description"] == "Source narrative"
    assert row["Local Collection"] == "USGS Topographic Reconnaissance Series"
    assert row["Rights Holder"] == ""
    assert "National Science Foundation" in row["Rights"]
    assert "United States Geological Survey" in row["Rights"]
    assert "acknowledgement-policy" in row["Rights"]
    assert "Map copyright" not in row["Rights"]
    assert row["B1G Image"] == (
        "https://data.pgc.umn.edu/maps/antarctica/usgs/03/preview/"
        "Argentina%20Range.jpg"
    )
    assert row["Identifier"] == "PGC Map ID 2"
    assert row["Source"] == ""
    assert row["Bounding Box"] == "-45,-83,-37.5,-82"
    assert row["Centroid"] == "-82.5,-41.25"
    assert row["information"].endswith("/mapinfo/antarctica/2")
    assert row["download"] == [
        {
            "url": "https://data.pgc.umn.edu/maps/antarctica/usgs/03/crop/Argentina%20Range.zip",
            "label": "Cropped GeoTIFF",
        },
        {
            "url": "https://data.pgc.umn.edu/maps/antarctica/usgs/03/geo/Argentina%20Range.zip",
            "label": "GeoTIFF",
        },
        {
            "url": "https://data.pgc.umn.edu/maps/antarctica/usgs/03/tif/Argentina%20Range.tif",
            "label": "TIFF",
        },
        {
            "url": "https://data.pgc.umn.edu/maps/antarctica/usgs/03/pdf/Argentina%20Range.pdf",
            "label": "PDF",
        },
    ]


def test_duplicate_reference_ids_receive_stable_map_id_suffixes() -> None:
    record = _record(map_id=699, reference_id="ANT REF-MT1601-001")

    row = pgc_record_to_schema(
        record,
        legacy_id_by_map_id={},
        reference_counts=Counter({"ANT-REF-MT1601-001": 3}),
        catalog_base_url="https://maps.apps.pgc.umn.edu",
    )

    assert row["ID"] == "ANT-REF-MT1601-001-699"


def test_non_georeferenced_record_has_no_geotiff_download() -> None:
    record = _record(is_geo=0, is_crop=0, is_tif=0, is_pdf=0)

    assert pgc_downloads(record) == []


def test_preview_requires_source_availability_flag() -> None:
    assert pgc_preview(_record(is_preview=0)) == ""


def test_antimeridian_bbox_and_centroid_are_preserved() -> None:
    record = _record(
        lon_lower_left=170,
        lon_upper_right=-170,
        lat_lower_left=-75,
        lat_upper_right=-70,
    )

    bbox = pgc_bounding_box(record)

    assert bbox == "170,-75,-170,-70"
    assert pgc_centroid(bbox) == "-72.5,180"


def test_spatial_cleaning_uses_multipolygon_for_antimeridian_bbox() -> None:
    frame = pd.DataFrame(
        [{"Bounding Box": "178.000,-78.000,-100.000,-73.500", "Geometry": ""}]
    )

    cleaned = clean_pgc_spatial_fields(frame)

    assert cleaned.loc[0, "Bounding Box"] == "178.000,-78.000,-100.000,-73.500"
    assert cleaned.loc[0, "Geometry"].startswith("MULTIPOLYGON")
    assert (
        geometry_validation_error(
            cleaned.loc[0, "Bounding Box"], cleaned.loc[0, "Geometry"]
        )
        == ""
    )


def test_bbox_inference_fills_only_blank_records_and_appends_note() -> None:
    frame = pd.DataFrame(
        [
            {
                "ID": "missing",
                "Bounding Box": "",
                "Geometry": "",
                "Centroid": "",
                "Admin Note": "Existing note.",
            },
            {
                "ID": "existing",
                "Bounding Box": "-10,60,-9,61",
                "Geometry": "ENVELOPE(-10,-9,61,60)",
                "Centroid": "60.5,-9.5",
                "Admin Note": "",
            },
        ]
    )
    decisions = {
        "missing": {
            "bounding_box": "140,20,-120,70",
            "note": "Approximate search extent inferred from the title.",
        },
        "existing": {
            "bounding_box": "0,0,1,1",
            "note": "This must not overwrite source coordinates.",
        },
    }

    result = apply_pgc_bbox_inferences(frame, decisions)

    assert result.loc[0, "Bounding Box"] == "140,20,-120,70"
    assert result.loc[0, "Centroid"] == "45,-170"
    assert result.loc[0, "Admin Note"] == (
        "Existing note. Approximate search extent inferred from the title."
    )
    assert result.loc[1, "Bounding Box"] == "-10,60,-9,61"


def test_load_bbox_inferences_normalizes_and_validates_yaml(tmp_path: Path) -> None:
    path = tmp_path / "bbox.yaml"
    path.write_text(
        "records:\n"
        "  map-1:\n"
        "    bounding_box: '-130.000,-77.000,-124.000,-76.000'\n"
        "    note: Approximate search extent.\n",
        encoding="utf-8",
    )

    assert load_pgc_bbox_inferences(path) == {
        "map-1": {
            "bounding_box": "-130,-77,-124,-76",
            "note": "Approximate search extent.",
        }
    }


def test_legacy_enrichments_preserve_curated_values_and_merge_keywords() -> None:
    frame = pd.DataFrame(
        [
            {
                "ID": "ANT-REF-MT2503-002",
                "Creator": "Source creator",
                "Keyword": "General Purpose|Reference",
                "Language": "eng",
                "Spatial Coverage": "Antarctica--Argentina Range|Antarctica",
            }
        ]
    )
    legacy = {
        "ANT-REF-MT2503-002": {
            "Creator": "Curated creator",
            "Creator ID": "authority-1",
            "Language": "eng|spa",
            "Spatial Coverage": "Antarctica",
            "Keyword": "legacy term|Reference",
            "Tags": "legacy-tag",
            "Bounding Box": "-45,-83,-37.5,-82",
            "Geometry": "ENVELOPE(-45,-37.5,-82,-83)",
            "Centroid": "-82.5,-41.25",
        }
    }

    result = apply_pgc_legacy_enrichments(frame, legacy).iloc[0]

    assert result["Creator"] == "Curated creator"
    assert result["Creator ID"] == "authority-1"
    assert result["Language"] == "eng|spa"
    assert result["Spatial Coverage"] == "Antarctica"
    assert result["Keyword"] == "legacy term|Reference|General Purpose"


def test_legacy_spatial_fields_take_precedence_for_known_records() -> None:
    frame = pd.DataFrame(
        [
            {
                "ID": "legacy-blank",
                "Bounding Box": "",
                "Geometry": "",
                "Centroid": "",
            },
            {
                "ID": "current-spatial",
                "Bounding Box": "1,2,3,4",
                "Geometry": "",
                "Centroid": "3,2",
            },
        ]
    )
    legacy = {
        record_id: {
            "Bounding Box": "-45,-83,-37.5,-82",
            "Geometry": "ENVELOPE(-45,-37.5,-82,-83)",
            "Centroid": "-82.5,-41.25",
        }
        for record_id in ("legacy-blank", "current-spatial")
    }

    result = apply_pgc_legacy_enrichments(frame, legacy)

    assert result.iloc[0]["Bounding Box"] == "-45,-83,-37.5,-82"
    assert result.iloc[1]["Bounding Box"] == "-45,-83,-37.5,-82"
    assert result.iloc[1]["Centroid"] == "-82.5,-41.25"


def test_positive_antarctic_latitudes_are_corrected_to_southern_hemisphere() -> None:
    record = _record(lat_lower_left=68.916, lat_upper_right=67.66)

    assert pgc_bounding_box(record) == "-45,-68.916,-37.5,-67.66"


def test_pgc_bbox_precision_uses_integers_or_three_decimal_places() -> None:
    assert format_pgc_bounding_box("-130.000,-77.000,-124.000,-76.000") == (
        "-130,-77,-124,-76"
    )
    assert format_pgc_bounding_box(
        "-147.119329,64.043962,-145.860241,64.684493"
    ) == "-147.119,64.044,-145.860,64.684"


def test_pgc_spatial_cleaning_preserves_polar_limits_and_updates_geometry() -> None:
    result = clean_pgc_spatial_fields(
        pd.DataFrame(
            [{"Bounding Box": "-180.000000,-90.000000,180.000000,-60.000000"}]
        )
    ).iloc[0]

    assert result["Bounding Box"] == "-180,-90,180,-60"
    assert result["Geometry"] == "ENVELOPE(-180,180,-60,-90)"


def test_distribution_table_contains_landing_and_all_download_variants() -> None:
    harvester = PgcHarvester(
        {
            "collections": {"antarctica": "https://example.test/items"},
            "build_uploads": False,
        }
    )
    harvester.load_reference_data()
    record = _record()
    primary = harvester.build_dataframe([record])
    distributions = generate_secondary_table(
        primary,
        harvester.distribution_types,
    )

    assert list(distributions["reference_type"]) == [
        "documentation_external",
        "download",
        "download",
        "download",
        "download",
    ]
    assert list(distributions["label"]) == [
        "",
        "Cropped GeoTIFF",
        "GeoTIFF",
        "TIFF",
        "PDF",
    ]
