from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import convert_shapefiles_to_geopackage as converter  # noqa: E402


def test_nested_duplicate_names_and_uppercase_extensions(tmp_path, monkeypatch):
    source = tmp_path / "source"
    for name in ("a/roads.shp", "b/roads.SHP"):
        path = source / name
        path.parent.mkdir(parents=True)
        path.touch()
    outputs = []
    monkeypatch.setattr(
        converter, "convert_shapefile", lambda src, dst, **kw: outputs.append(dst)
    )
    output = tmp_path / "output"
    assert converter.convert_directory(source, output, ogr2ogr="ogr2ogr") == 0
    assert outputs == [output / "a/roads.gpkg", output / "b/roads.gpkg"]


def test_existing_outputs_require_explicit_policy(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "roads.shp").touch()
    output = tmp_path / "output"
    output.mkdir()
    destination = output / "roads.gpkg"
    destination.write_bytes(b"existing")
    calls = []
    monkeypatch.setattr(
        converter, "convert_shapefile", lambda *a, **kw: calls.append(a)
    )
    assert converter.convert_directory(source, output, ogr2ogr="ogr2ogr") == 1
    assert (
        converter.convert_directory(
            source, output, ogr2ogr="ogr2ogr", skip_existing=True
        )
        == 0
    )
    assert calls == []
    destination.write_bytes(b"")
    assert (
        converter.convert_directory(
            source, output, ogr2ogr="ogr2ogr", skip_existing=True
        )
        == 1
    )
    assert (
        converter.convert_directory(source, output, ogr2ogr="ogr2ogr", overwrite=True)
        == 0
    )
    assert len(calls) == 1


def test_failed_overwrite_preserves_existing_output(tmp_path, monkeypatch):
    destination = tmp_path / "roads.gpkg"
    destination.write_bytes(b"original")

    def fail(command, **kwargs):
        Path(command[3]).write_bytes(b"partial output")
        return subprocess.CompletedProcess(command, 1, "", "broken source")

    monkeypatch.setattr(converter.subprocess, "run", fail)
    with pytest.raises(RuntimeError, match="broken source"):
        converter.convert_shapefile(
            tmp_path / "roads.shp", destination, ogr2ogr="ogr2ogr"
        )
    assert destination.read_bytes() == b"original"
    assert list(tmp_path.iterdir()) == [destination]


def test_batch_continues_after_failure(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    for name in ("bad.shp", "good.shp"):
        (source / name).touch()
    calls = []

    def convert(src, dst, **kwargs):
        calls.append(src.name)
        if src.name == "bad.shp":
            raise RuntimeError("invalid input")

    monkeypatch.setattr(converter, "convert_shapefile", convert)
    assert (
        converter.convert_directory(source, tmp_path / "output", ogr2ogr="ogr2ogr") == 1
    )
    assert calls == ["bad.shp", "good.shp"]


def test_empty_input_is_failure(tmp_path):
    assert (
        converter.convert_directory(tmp_path, tmp_path / "output", ogr2ogr="ogr2ogr")
        == 1
    )


def test_gdal_conversion_preserves_features_fields_and_crs(tmp_path):
    ogr2ogr = shutil.which("ogr2ogr")
    if not ogr2ogr:
        pytest.skip("GDAL is not installed")
    import geopandas as gpd
    from shapely.geometry import Point

    source = tmp_path / "source"
    source.mkdir()
    frame = gpd.GeoDataFrame(
        {"label": ["test"]}, geometry=[Point(-89, 40)], crs="EPSG:4269"
    )
    frame.to_file(source / "points.shp")
    output = tmp_path / "output"
    assert (
        converter.main(["--input-dir", str(source), "--output-dir", str(output)]) == 0
    )
    result = gpd.read_file(output / "points.gpkg", layer="points")
    assert result["label"].tolist() == ["test"]
    assert result.geometry.iloc[0].equals(Point(-89, 40))
    assert result.crs.to_epsg() == 4269
