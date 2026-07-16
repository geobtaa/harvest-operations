#!/usr/bin/env python3
"""Extract field descriptions/definitions from GDB layer metadata XML.

This script reads metadata XML embedded in a File Geodatabase (if present)
and attempts to extract FGDC-style attribute definitions (attrdef/attrdefs).

Example:
  python scripts/parse_gdb_metadata_xml.py /path/to.gdb --out-dir temp/field_metadata
"""
from __future__ import annotations

import argparse
import csv
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree as ET


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


def _sanitize_filename(name: str) -> str:
    safe = name.replace(os.sep, "_")
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in safe)


def _field_def_to_dict(field_def) -> Dict[str, Any]:
    field_info = {
        "name": field_def.GetName(),
        "type": field_def.GetTypeName(),
        "width": field_def.GetWidth(),
        "precision": field_def.GetPrecision(),
        "nullable": bool(field_def.IsNullable()),
        "default": field_def.GetDefault(),
    }

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


def _get_xml_strings(layer) -> List[Tuple[str, str]]:
    xml_strings: List[Tuple[str, str]] = []
    domains = layer.GetMetadataDomainList() or []
    if not domains:
        return xml_strings

    for domain in domains:
        try:
            md = layer.GetMetadata(domain)
        except Exception:
            continue

        if isinstance(md, dict):
            for value in md.values():
                if isinstance(value, str) and value.strip().startswith("<"):
                    xml_strings.append((domain, value))
        elif isinstance(md, list):
            for value in md:
                if isinstance(value, str) and value.strip().startswith("<"):
                    xml_strings.append((domain, value))
        elif isinstance(md, str) and md.strip().startswith("<"):
            xml_strings.append((domain, md))

    return xml_strings


def _first_text(element: Optional[ET.Element]) -> Optional[str]:
    if element is None:
        return None
    text = (element.text or "").strip()
    return text or None


def _extract_fgdc_attributes(xml_text: str) -> Dict[str, Dict[str, str]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return {}

    attr_info: Dict[str, Dict[str, str]] = {}

    # FGDC attribute section: eainfo/detailed/attr
    for attr in root.findall(".//attr"):
        label = _first_text(attr.find("./attrlabl"))
        if not label:
            continue
        description = _first_text(attr.find("./attrdef"))
        source = _first_text(attr.find("./attrdefs"))
        attr_info[label] = {}
        if description:
            attr_info[label]["description"] = description
        if source:
            attr_info[label]["definition_source"] = source

    return attr_info


def _extract_esri_attributes(xml_text: str) -> Dict[str, Dict[str, str]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return {}

    attr_info: Dict[str, Dict[str, str]] = {}

    # ESRI metadata variants often store field info under eainfo/detailed/attr,
    # but some store under eainfo/overview or dataIdInfo. We'll try attr nodes first.
    for attr in root.findall(".//attr"):
        label = _first_text(attr.find("./attrlabl"))
        if not label:
            continue
        description = _first_text(attr.find("./attrdef")) or _first_text(attr.find("./attrdescr"))
        source = _first_text(attr.find("./attrdefs"))
        attr_info[label] = {}
        if description:
            attr_info[label]["description"] = description
        if source:
            attr_info[label]["definition_source"] = source

    return attr_info


def _merge_attr_info(aggregated: Dict[str, Dict[str, str]], incoming: Dict[str, Dict[str, str]]):
    for key, value in incoming.items():
        if key not in aggregated:
            aggregated[key] = value
            continue
        for subkey, subval in value.items():
            if subkey not in aggregated[key]:
                aggregated[key][subkey] = subval


def _attribute_metadata(layer) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
    attr_info: Dict[str, Dict[str, str]] = {}
    domains_used: List[str] = []
    for domain, xml_text in _get_xml_strings(layer):
        domains_used.append(domain)
        _merge_attr_info(attr_info, _extract_fgdc_attributes(xml_text))
        _merge_attr_info(attr_info, _extract_esri_attributes(xml_text))
    return attr_info, sorted(set(domains_used))


def _write_field_csv(path: str, fields: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "name",
        "type",
        "width",
        "precision",
        "nullable",
        "default",
        "domain",
        "alias",
        "description",
        "definition_source",
        "metadata_domains",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for field in fields:
            writer.writerow({key: field.get(key) for key in fieldnames})


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract field metadata from GDB layer XML metadata."
    )
    parser.add_argument("gdb", help="Path to .gdb directory")
    parser.add_argument("--out-dir", required=True, help="Directory for per-layer CSVs")
    args = parser.parse_args()

    ds = _open_gdb(args.gdb)
    os.makedirs(args.out_dir, exist_ok=True)

    for i in range(ds.GetLayerCount()):
        layer = ds.GetLayerByIndex(i)
        if layer is None:
            continue

        layer_def = layer.GetLayerDefn()
        fields = [_field_def_to_dict(layer_def.GetFieldDefn(j)) for j in range(layer_def.GetFieldCount())]

        attr_info, domains_used = _attribute_metadata(layer)
        for field in fields:
            name = field.get("name")
            if not name:
                continue
            meta = attr_info.get(name, {})
            if meta:
                field.update(meta)
            field["metadata_domains"] = ";".join(domains_used) if domains_used else ""

        filename = _sanitize_filename(layer.GetName() or "layer") + ".csv"
        path = os.path.join(args.out_dir, filename)
        _write_field_csv(path, fields)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
