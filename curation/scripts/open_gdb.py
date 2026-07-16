#!/usr/bin/env python3
"""Inventory feature classes in a File Geodatabase using OpenFileGDB.

Example:
  python scripts/open_gdb.py /path/to.gdb
  python scripts/open_gdb.py /path/to.gdb --json inventory.json
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---- User config (optional) ----
# If you leave CLI args blank, these defaults are used.
DEFAULT_GDB_PATH = "BTAA_GIN_Baltimore_City_base_layers.gdb"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_RUN_INVENTORY_CSV = True
DEFAULT_RUN_FIELDS_CSV = True
DEFAULT_ID_PREFIX = "b1g_"
# --------------------------------


def _field_def_to_dict(field_def) -> Dict[str, Any]:
    field_info = {
        "name": field_def.GetName(),
        "type": field_def.GetTypeName(),
        "width": field_def.GetWidth(),
        "precision": field_def.GetPrecision(),
        "nullable": bool(field_def.IsNullable()),
        "default": field_def.GetDefault(),
    }

    description = None
    if hasattr(field_def, "GetComment"):
        try:
            description = field_def.GetComment()
        except Exception:
            description = None
    if not description and hasattr(field_def, "GetDescription"):
        try:
            description = field_def.GetDescription()
        except Exception:
            description = None
    if description:
        field_info["description"] = description

    if hasattr(field_def, "GetDomainName"):
        try:
            domain_name = field_def.GetDomainName()
        except Exception:
            domain_name = None
        if domain_name:
            field_info["domain"] = domain_name

    if hasattr(field_def, "GetAlternativeName"):
        try:
            alt_name = field_def.GetAlternativeName()
        except Exception:
            alt_name = None
        if alt_name:
            field_info["alias"] = alt_name

    return field_info


def _extract_metadata_xml_from_gdb(gdb_path: str) -> List[str]:
    xml_chunks: List[str] = []
    try:
        files = [
            os.path.join(gdb_path, name)
            for name in os.listdir(gdb_path)
            if name.endswith(".gdbtable")
        ]
    except Exception:
        return xml_chunks

    pattern = re.compile(r"<metadata.*?</metadata>", re.DOTALL)

    for path in files:
        try:
            with open(path, "rb") as f:
                data = f.read()
        except Exception:
            continue

        try:
            text = data.decode("utf-8", errors="ignore")
        except Exception:
            continue

        xml_chunks.extend(pattern.findall(text))

    return xml_chunks


def _first_text(element) -> Optional[str]:
    if element is None:
        return None
    text = (element.text or "").strip()
    return _strip_html(text) or None


def _strip_html(text: str) -> str:
    if not text:
        return text
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _xml_attribute_map(xml_texts: Iterable[str]) -> Dict[str, Dict[str, Dict[str, str]]]:
    from xml.etree import ElementTree as ET

    layer_map: Dict[str, Dict[str, Dict[str, str]]] = {}

    for xml_text in xml_texts:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            continue

        for detailed in root.findall(".//detailed"):
            layer_name = detailed.get("Name") or _first_text(detailed.find("./enttyp/enttypl"))
            if not layer_name:
                continue

            layer_entry = layer_map.setdefault(layer_name, {})
            for attr in detailed.findall(".//attr"):
                field_name = _first_text(attr.find("./attrlabl"))
                if not field_name:
                    continue

                meta: Dict[str, str] = {}
                description = _first_text(attr.find("./attrdef"))
                definition_source = _first_text(attr.find("./attrdefs"))
                alias = _first_text(attr.find("./attalias"))
                domain = _first_text(attr.find("./attrdomv/udom"))

                if description:
                    meta["description"] = description
                if definition_source:
                    meta["definition_source"] = definition_source
                if alias:
                    meta["alias"] = alias
                if domain:
                    meta["domain_description"] = domain

                if meta:
                    existing = layer_entry.setdefault(field_name, {})
                    for key, value in meta.items():
                        if key not in existing:
                            existing[key] = value

    return layer_map


def _xml_layer_descriptions(xml_texts: Iterable[str]) -> Dict[str, str]:
    from xml.etree import ElementTree as ET

    layer_descriptions: Dict[str, str] = {}

    for xml_text in xml_texts:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            continue

        layer_name = None
        detailed = root.find(".//detailed")
        if detailed is not None:
            layer_name = detailed.get("Name") or _first_text(detailed.find("./enttyp/enttypl"))

        if not layer_name:
            layer_name = _first_text(root.find(".//dataIdInfo/idCitation/resTitle"))
        if not layer_name:
            layer_name = _first_text(root.find(".//idinfo/citation/citeinfo/title"))

        if not layer_name:
            continue

        description = (
            _first_text(root.find(".//dataIdInfo/idAbs"))
            or _first_text(root.find(".//idinfo/descript/abstract"))
            or _first_text(root.find(".//idinfo/descript/purpose"))
        )

        if description and layer_name not in layer_descriptions:
            layer_descriptions[layer_name] = description

    return layer_descriptions


def _xml_layer_rights(xml_texts: Iterable[str]) -> Dict[str, str]:
    from xml.etree import ElementTree as ET

    layer_rights: Dict[str, str] = {}

    for xml_text in xml_texts:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            continue

        layer_name = None
        detailed = root.find(".//detailed")
        if detailed is not None:
            layer_name = detailed.get("Name") or _first_text(detailed.find("./enttyp/enttypl"))

        if not layer_name:
            layer_name = _first_text(root.find(".//dataIdInfo/idCitation/resTitle"))
        if not layer_name:
            layer_name = _first_text(root.find(".//idinfo/citation/citeinfo/title"))

        if not layer_name:
            continue

        rights_values: List[str] = []

        # FGDC-style
        for path in [
            ".//idinfo/accconst",
            ".//idinfo/useconst",
            ".//idinfo/secinfo/secclass",
        ]:
            value = _first_text(root.find(path))
            if value:
                rights_values.append(value)

        # ISO-style
        for path in [
            ".//dataIdInfo/resConst/Consts/useLimitation",
            ".//dataIdInfo/resConst/Consts/otherConstraints",
            ".//dataIdInfo/resConst/Consts/accessConstraints",
            ".//dataIdInfo/resConst/Consts/useConstraints",
        ]:
            value = _first_text(root.find(path))
            if value:
                rights_values.append(value)

        if rights_values:
            uniq = []
            for item in rights_values:
                if item not in uniq:
                    uniq.append(item)
            layer_rights[layer_name] = " | ".join(uniq)

    return layer_rights


def _xml_layer_themes(xml_texts: Iterable[str]) -> Dict[str, str]:
    from xml.etree import ElementTree as ET

    layer_themes: Dict[str, str] = {}

    for xml_text in xml_texts:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            continue

        layer_name = None
        detailed = root.find(".//detailed")
        if detailed is not None:
            layer_name = detailed.get("Name") or _first_text(detailed.find("./enttyp/enttypl"))

        if not layer_name:
            layer_name = _first_text(root.find(".//dataIdInfo/idCitation/resTitle"))
        if not layer_name:
            layer_name = _first_text(root.find(".//idinfo/citation/citeinfo/title"))

        if not layer_name:
            continue

        topics: List[str] = []
        for node in root.findall(".//dataIdInfo/tpCat"):
            value = _first_text(node)
            if value:
                topics.append(value)
        for node in root.findall(".//idinfo/keywords/theme/themekey"):
            value = _first_text(node)
            if value and value.lower() in (
                "farming",
                "biota",
                "boundaries",
                "climatology/meteorology/atmosphere",
                "economy",
                "elevation",
                "environment",
                "geoscientificinformation",
                "health",
                "imagerybasemapsearthcover",
                "intelligencemilitary",
                "inlandwaters",
                "location",
                "oceans",
                "planningcadastre",
                "society",
                "structure",
                "transportation",
                "utilitiescommunication",
            ):
                topics.append(value)

        if topics:
            uniq = []
            for item in topics:
                if item not in uniq:
                    uniq.append(item)
            layer_themes[layer_name] = " | ".join(uniq)

    return layer_themes


def _srs_to_dict(srs) -> Optional[Dict[str, Any]]:
    if srs is None:
        return None

    try:
        auth_name = srs.GetAuthorityName(None)
        auth_code = srs.GetAuthorityCode(None)
    except Exception:
        auth_name = None
        auth_code = None

    srs_dict = {
        "authority_name": auth_name,
        "authority_code": auth_code,
        "wkt": srs.ExportToWkt() if hasattr(srs, "ExportToWkt") else None,
    }
    return srs_dict


def _geom_type_name(layer_def) -> str:
    try:
        return layer_def.GetGeomTypeName()
    except Exception:
        pass

    try:
        from osgeo import ogr  # type: ignore
    except Exception:
        return str(layer_def.GetGeomType())

    try:
        return ogr.GeometryTypeToName(layer_def.GetGeomType())
    except Exception:
        return str(layer_def.GetGeomType())


def _layer_to_dict(layer) -> Dict[str, Any]:
    layer_def = layer.GetLayerDefn()

    try:
        feature_count = layer.GetFeatureCount(True)
    except Exception:
        feature_count = None

    try:
        extent = layer.GetExtent()
        extent_dict = {
            "min_x": extent[0],
            "max_x": extent[1],
            "min_y": extent[2],
            "max_y": extent[3],
        }
    except Exception:
        extent_dict = None

    fields = [_field_def_to_dict(layer_def.GetFieldDefn(i)) for i in range(layer_def.GetFieldCount())]

    layer_info = {
        "id": _generate_layer_id(prefix=DEFAULT_ID_PREFIX),
        "name": layer.GetName(),
        "geometry_type": layer_def.GetGeomType(),
        "geometry_type_name": _geom_type_name(layer_def),
        "feature_count": feature_count,
        "extent": extent_dict,
        "extent_bbox": _layer_bounding_box(layer),
        "epsg": _layer_epsg(layer),
        "srs": _srs_to_dict(layer.GetSpatialRef()),
        "fields": fields,
        "metadata": layer.GetMetadata() or {},
    }
    return layer_info


def _open_gdb(path: str):
    try:
        from osgeo import gdal, ogr  # type: ignore
    except Exception as exc:
        raise SystemExit(
            "GDAL Python bindings not available. Install GDAL or ensure osgeo is on PYTHONPATH."
        ) from exc

    gdal.UseExceptions()
    ogr.UseExceptions()

    driver = ogr.GetDriverByName("OpenFileGDB")
    if driver is None:
        raise SystemExit("OpenFileGDB driver not available in this GDAL build.")

    try:
        if hasattr(gdal, "OpenEx"):
            ds = gdal.OpenEx(path, gdal.OF_VECTOR)
        else:
            ds = ogr.Open(path, 0)
    except Exception as exc:
        raise SystemExit(f"Failed to open geodatabase: {exc}") from exc

    if ds is None:
        raise SystemExit("Failed to open geodatabase (dataset is None).")

    return ds


def _format_crs_uri(crs_string: Optional[str]) -> Optional[str]:
    if crs_string and crs_string.startswith("EPSG:"):
        epsg_code = crs_string.split(":")[1]
        return f"https://epsg.io/{epsg_code}"
    return crs_string


def _layer_epsg(layer) -> Optional[str]:
    srs = layer.GetSpatialRef()
    if srs is None:
        return None

    try:
        srs = srs.Clone()
    except Exception:
        pass

    try:
        srs.AutoIdentifyEPSG()
    except Exception:
        pass

    try:
        auth_name = srs.GetAuthorityName(None)
        auth_code = srs.GetAuthorityCode(None)
    except Exception:
        return None

    if auth_name and auth_code:
        return f"{auth_name}:{auth_code}"
    return None


def _transform_bbox_to_wgs84(
    bbox: Tuple[float, float, float, float], srs
) -> Optional[Tuple[float, float, float, float]]:
    try:
        from osgeo import osr  # type: ignore
    except Exception:
        return None

    try:
        wgs84 = osr.SpatialReference()
        wgs84.ImportFromEPSG(4326)
        if hasattr(wgs84, "SetAxisMappingStrategy") and hasattr(
            osr, "OAMS_TRADITIONAL_GIS_ORDER"
        ):
            wgs84.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        if hasattr(srs, "SetAxisMappingStrategy") and hasattr(
            osr, "OAMS_TRADITIONAL_GIS_ORDER"
        ):
            srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        transform = osr.CoordinateTransformation(srs, wgs84)
    except Exception:
        return None

    min_x, max_x, min_y, max_y = bbox
    corners = [
        (min_x, min_y),
        (min_x, max_y),
        (max_x, min_y),
        (max_x, max_y),
    ]

    try:
        transformed = [transform.TransformPoint(x, y) for x, y in corners]
    except Exception:
        return None

    xs = [pt[0] for pt in transformed]
    ys = [pt[1] for pt in transformed]
    return (min(xs), min(ys), max(xs), max(ys))


def _layer_bounding_box(layer, decimal_places: int = 4) -> str:
    srs = layer.GetSpatialRef()
    if srs is None:
        return "Unknown"

    try:
        extent = layer.GetExtent()
    except Exception:
        return "Unknown"

    bbox = (extent[0], extent[1], extent[2], extent[3])

    epsg = _layer_epsg(layer)
    if epsg == "EPSG:4326":
        transformed_bbox = (bbox[0], bbox[2], bbox[1], bbox[3])
    else:
        transformed = _transform_bbox_to_wgs84(bbox, srs)
        if transformed is None:
            return "Unknown"
        transformed_bbox = transformed

    min_x, min_y, max_x, max_y = transformed_bbox
    rounded = [round(coord, decimal_places) for coord in (min_x, min_y, max_x, max_y)]
    return f"{rounded[0]},{rounded[1]},{rounded[2]},{rounded[3]}"


def _sanitize_filename(name: str) -> str:
    safe = name.replace(os.sep, "_")
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in safe)


def _generate_layer_id(length: int = 12, prefix: str = "") -> str:
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    try:
        from nanoid import generate  # type: ignore

        return f"{prefix}{generate(alphabet, length)}"
    except Exception:
        import secrets

        return f"{prefix}{''.join(secrets.choice(alphabet) for _ in range(length))}"


def _resource_type(geom_name: Optional[str]) -> str:
    if not geom_name:
        return "Unknown"
    parts = [part for part in geom_name.split() if part not in ("Multi", "String")]
    if not parts:
        return "Unknown"
    return f"{' '.join(parts)} data"


def _write_layer_csv(path: str, inventory: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "ID",
        "name",
        "Description",
        "Rights",
        "Theme",
        "geometry_type",
        "geometry_type_name",
        "Resource Type",
        "feature_count",
        "Bounding Box",
        "Coordinate Reference System",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in inventory:
            writer.writerow(
                {
                    "ID": item.get("id"),
                    "name": item.get("name"),
                    "Description": item.get("description"),
                    "Rights": item.get("rights"),
                    "Theme": item.get("theme"),
                    "geometry_type": item.get("geometry_type"),
                    "geometry_type_name": item.get("geometry_type_name"),
                    "Resource Type": _resource_type(item.get("geometry_type_name")),
                    "feature_count": item.get("feature_count"),
                    "Bounding Box": item.get("extent_bbox"),
                    "Coordinate Reference System": _format_crs_uri(item.get("epsg")),
                }
            )


def _write_field_csvs(directory: str, inventory: List[Dict[str, Any]]) -> None:
    os.makedirs(directory, exist_ok=True)
    fieldnames = [
        "friendlier_id",
        "field_name",
        "field_type",
        # "width",
        # "precision",
        # "nullable",
        # "default",
        "values",
        # "alias",
        "definition",
        "definition_source",
        "domain_description",
        "parent_field_name",
        "position"
    ]
    for item in inventory:
        layer_name = item.get("name") or "layer"
        layer_id = item.get("id") or ""
        filename = _sanitize_filename(layer_name) + ".csv"
        path = os.path.join(directory, filename)
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for field in item.get("fields") or []:
                alias = field.get("alias") or ""
                field_name = field.get("name") or ""
                if alias and field_name and alias == field_name:
                    alias = ""
                definition = field.get("description") or ""
                if alias and definition:
                    definition = f"{alias}. {definition}"
                elif alias and not definition:
                    definition = alias
                writer.writerow(
                    {
                        "friendlier_id": layer_id,
                        "field_name": field.get("name"),
                        "field_type": field.get("type"),
                        "definition": definition,
                        "definition_source": field.get("definition_source"),
                        # "width": field.get("width"),
                        # "precision": field.get("precision"),
                        # "nullable": field.get("nullable"),
                        # "default": field.get("default"),
                        "values": field.get("domain"),
                        # "alias": field.get("alias"),
                        "domain_description": field.get("domain_description"),
                        "parent_field_name": "",
                        "position": "",
                    }
                )


def _print_table(inventory: List[Dict[str, Any]]) -> None:
    headers = ["feature_class", "geom", "features", "fields"]
    rows: List[List[str]] = []

    for item in inventory:
        rows.append(
            [
                item["name"],
                item.get("geometry_type_name") or "Unknown",
                str(item.get("feature_count") if item.get("feature_count") is not None else ""),
                str(len(item.get("fields") or [])),
            ]
        )

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(values: List[str]) -> str:
        return "  ".join(values[i].ljust(widths[i]) for i in range(len(values)))

    print(fmt_row(headers))
    print(fmt_row(["-" * w for w in widths]))
    for row in rows:
        print(fmt_row(row))


def main() -> int:
    parser = argparse.ArgumentParser(description="Inventory feature classes in a File Geodatabase.")
    parser.add_argument("gdb", nargs="?", help="Path to .gdb directory")
    parser.add_argument("--out-dir", dest="out_dir", help="Output directory for CSVs/JSON")
    parser.add_argument("--json", dest="json_path", help="Write full inventory to JSON file")
    parser.add_argument("--csv", dest="csv_path", help="Write layer inventory to CSV file")
    parser.add_argument(
        "--fields-dir",
        dest="fields_dir",
        help="Write one CSV per layer for field definitions",
    )
    parser.add_argument(
        "--no-inventory",
        action="store_true",
        help="Disable writing the layer inventory CSV",
    )
    parser.add_argument(
        "--no-fields",
        action="store_true",
        help="Disable writing per-layer field CSVs",
    )
    args = parser.parse_args()

    gdb_path = args.gdb or DEFAULT_GDB_PATH
    if not gdb_path:
        raise SystemExit("No geodatabase path provided. Set DEFAULT_GDB_PATH or pass as an argument.")

    out_dir = args.out_dir or DEFAULT_OUTPUT_DIR
    os.makedirs(out_dir, exist_ok=True)

    run_inventory = DEFAULT_RUN_INVENTORY_CSV if not args.no_inventory else False
    run_fields = DEFAULT_RUN_FIELDS_CSV if not args.no_fields else False

    ds = _open_gdb(gdb_path)
    xml_texts = _extract_metadata_xml_from_gdb(gdb_path)
    xml_map = _xml_attribute_map(xml_texts)
    xml_layer_desc = _xml_layer_descriptions(xml_texts)
    xml_layer_rights = _xml_layer_rights(xml_texts)
    xml_layer_themes = _xml_layer_themes(xml_texts)

    inventory: List[Dict[str, Any]] = []
    for i in range(ds.GetLayerCount()):
        layer = ds.GetLayerByIndex(i)
        if layer is None:
            continue
        layer_info = _layer_to_dict(layer)

        layer_name = layer_info.get("name") or ""
        layer_meta = xml_map.get(layer_name)
        if layer_meta is None and layer_name:
            for key, value in xml_map.items():
                if key.endswith(f".{layer_name}") or layer_name.endswith(f".{key}"):
                    layer_meta = value
                    break
        layer_desc = xml_layer_desc.get(layer_name)
        if layer_desc is None and layer_name:
            for key, value in xml_layer_desc.items():
                if key.endswith(f".{layer_name}") or layer_name.endswith(f".{key}"):
                    layer_desc = value
                    break
        layer_rights = xml_layer_rights.get(layer_name)
        if layer_rights is None and layer_name:
            for key, value in xml_layer_rights.items():
                if key.endswith(f".{layer_name}") or layer_name.endswith(f".{key}"):
                    layer_rights = value
                    break
        layer_theme = xml_layer_themes.get(layer_name)
        if layer_theme is None and layer_name:
            for key, value in xml_layer_themes.items():
                if key.endswith(f".{layer_name}") or layer_name.endswith(f".{key}"):
                    layer_theme = value
                    break

        if layer_desc:
            layer_info["description"] = layer_desc
        if layer_rights:
            layer_info["rights"] = layer_rights
        if layer_theme:
            layer_info["theme"] = layer_theme

        if layer_meta:
            for field in layer_info.get("fields") or []:
                field_name = field.get("name")
                if not field_name:
                    continue
                meta = layer_meta.get(field_name)
                if not meta:
                    continue
                for key, value in meta.items():
                    if key not in field or not field.get(key):
                        field[key] = value

        inventory.append(layer_info)

    if args.json_path:
        with open(args.json_path, "w", encoding="utf-8") as f:
            json.dump(inventory, f, indent=2, ensure_ascii=False)

    if args.csv_path:
        _write_layer_csv(args.csv_path, inventory)
    elif run_inventory:
        _write_layer_csv(os.path.join(out_dir, "layers.csv"), inventory)

    if args.fields_dir:
        _write_field_csvs(args.fields_dir, inventory)
    elif run_fields:
        _write_field_csvs(os.path.join(out_dir, "fields"), inventory)

    if not args.json_path and not args.csv_path and not args.fields_dir and not run_inventory and not run_fields:
        _print_table(inventory)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
