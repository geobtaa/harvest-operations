"""Add title context only for explicit parent/sublayer pairs in a DCAT catalog.

Adapted from the mngeo-harvest-tests experiment. Service names alone never
establish a parent, and parent context does not change harvest eligibility.
"""

from collections import defaultdict
from urllib.parse import parse_qs, urlparse


def item_layer(resource: dict) -> tuple[str, str | None]:
    query = parse_qs(urlparse(resource.get("identifier", "")).query)
    return query.get("id", [""])[0], query.get("sublayer", [None])[0]


def service_urls(resource: dict) -> set[str]:
    """Normalize service links, including ArcGIS/arcgis capitalization variants."""
    urls = set()
    for distribution in resource.get("distribution", []) or []:
        if distribution.get("title") not in {"ArcGIS GeoService", "Service URL"}:
            continue
        parsed = urlparse(distribution.get("accessURL", ""))
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            urls.add(f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/").casefold())
    return urls


def prepare_parent_context(resources: list[dict]) -> list[dict]:
    """Copy records and add context where item ID and service hierarchy agree.

    Inspect the complete feed before filtering so ineligible parent records can
    still supply titles. Missing or ambiguous parents leave child titles alone.
    Preserve all records and links for the normal pipeline and duplicate policy.
    """
    parents = defaultdict(list)
    for resource in resources:
        item, layer = item_layer(resource)
        if item and layer is None:
            parents[item].append(resource)

    prepared = []
    for resource in resources:
        result = dict(resource)
        item, layer = item_layer(resource)
        title = str(resource.get("title", "")).strip()
        if item and layer is not None and layer.isdigit() and title and not title.startswith("{{"):
            child_urls = service_urls(resource)
            matching_titles = set()
            for parent in parents[item]:
                parent_title = str(parent.get("title", "")).strip()
                if not parent_title or parent_title.startswith("{{"):
                    continue
                roots = {
                    url for url in service_urls(parent)
                    if url.endswith(("/featureserver", "/mapserver"))
                }
                if any(f"{root}/{layer}" in child_urls for root in roots):
                    matching_titles.add(parent_title)
            if len(matching_titles) == 1:
                parent_title = matching_titles.pop()
                # Avoid repeating context already present in the supplied title.
                if parent_title.casefold() not in title.casefold():
                    result["_context_title"] = f"{title} (sublayer of {parent_title})"
        prepared.append(result)
    return prepared
