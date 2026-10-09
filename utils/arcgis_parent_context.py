"""Opt-in DCAT parent context; never infer a parent from a layer name."""

import copy
import json
from collections import defaultdict
from urllib.parse import parse_qs, urlparse


def item_layer(resource):
    query = parse_qs(urlparse(resource.get("identifier", "")).query)
    return query.get("id", [""])[0], query.get("sublayer", [None])[0]


def service_urls(resource):
    return sorted({
        d.get("accessURL", "") for d in resource.get("distribution", [])
        if d.get("title") in {"ArcGIS GeoService", "Service URL"}
        and urlparse(d.get("accessURL", "")).scheme in {"http", "https"}
        and urlparse(d.get("accessURL", "")).netloc
    })


def prepare_parent_context(resources, filter_rows):
    """Return copies with context and narrow parent eligibility markers.

    Conflicting repeated identifiers fail explicitly instead of choosing a record
    according to feed ordering. Identical entries are emitted once.
    """
    import pandas as pd

    unique = {}
    groups = defaultdict(list)
    for original in resources:
        resource = copy.deepcopy(original)
        identifier = resource.get("identifier", "")
        if identifier in unique:
            if unique[identifier] != resource:
                raise ValueError(f"Conflicting ArcGIS identifier: {identifier}")
            continue
        unique[identifier] = resource
        item, layer = item_layer(resource)
        if item:
            groups[item].append(resource)

    for group in groups.values():
        parents = [r for r in group if item_layer(r)[1] is None]
        if len(parents) != 1:
            continue
        parent = parents[0]
        title = str(parent.get("title", "")).strip()
        if not title or title.startswith("{{"):
            continue
        roots = [u.rstrip("/") for u in service_urls(parent)
                 if urlparse(u).path.rstrip("/").endswith(("/FeatureServer", "/MapServer"))]
        children = [r for r in group if item_layer(r)[1] is not None]
        matched = []
        for child in children:
            layer = item_layer(child)[1]
            if any(u.rstrip("/") == f"{root}/{layer}" for u in service_urls(child) for root in roots):
                matched.append(child)
        eligible = filter_rows(pd.DataFrame([{"resource": r} for r in matched])) if matched else []
        parent["_include_parent"] = bool(roots) and len(eligible) > 0
        for child in matched:
            original_title = str(child.get("title", "")).strip()
            if not original_title or original_title.startswith("{{"):
                continue
            child["_context_title"] = (
                original_title if title.casefold() in original_title.casefold()
                else f"{original_title} - {title}"
            )
        # Equal parent/child or sibling titles still need a stable distinction.
        counts = defaultdict(int)
        counts[title.casefold()] += 1
        for child in matched:
            counts[child.get("_context_title", child.get("title", "")).casefold()] += 1
        for child in matched:
            name = child.get("_context_title")
            if name and counts[name.casefold()] > 1:
                child["_context_title"] = f"{name} - Layer {item_layer(child)[1]}"
    for resource in unique.values():
        resource["_preserve_links"] = True
    return list(unique.values())


def distribution_fields(distributions, existing):
    """Use the existing writer's list support; keep links on their own record."""
    services = {"FeatureServer": "featureService", "MapServer": "mapService",
                "ImageServer": "imageService", "TileServer": "tileService"}
    downloads = []
    collected = defaultdict(set)
    formats = {"CSV", "Shapefile", "GeoJSON", "KML", "File Geodatabase", "Geopackage", "GeoPackage",
               "Feature Collection", "Excel", "SQLite Geodatabase"}
    for dist in distributions:
        url = dist.get("downloadURL") or dist.get("accessURL") or ""
        if urlparse(url).scheme not in {"http", "https"} or not urlparse(url).netloc:
            continue
        if dist.get("title") in {"ArcGIS GeoService", "Service URL"}:
            for token, column in services.items():
                if token in urlparse(url).path.split("/"):
                    collected[column].add(url)
        elif dist.get("downloadURL") or dist.get("title") in formats:
            downloads.append({"url": url, "label": dist.get("title") or dist.get("format") or "Download"})
    for column in services.values():
        existing[column] = sorted(collected[column])
    existing["download"] = sorted({json.dumps(d, sort_keys=True) for d in downloads})
    existing["download"] = [json.loads(d) for d in existing["download"]]
    return existing
