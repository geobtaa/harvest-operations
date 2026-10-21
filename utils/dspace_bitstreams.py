"""Download and join DSpace ORIGINAL-bundle bitstream inventories."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import tempfile


class DspaceBitstreamError(ValueError):
    """Raised when a DSpace inventory cannot be built or joined safely."""


def download_dspace_bitstream_inventory(
    session,
    *,
    rest_base_url: str,
    collection_handle: str,
    output_path: Path,
    timeout: int = 60,
) -> dict:
    """Download every public ORIGINAL-bundle bitstream in one collection."""
    base_url = rest_base_url.rstrip("/")
    collection = _find_collection(
        session,
        base_url=base_url,
        handle=collection_handle,
        timeout=timeout,
    )
    items = []
    page = 0
    while True:
        payload = _get_json(
            session,
            f"{base_url}/discover/search/objects",
            timeout,
            params={"scope": collection["uuid"], "size": 100, "page": page},
        )
        search_result = payload.get("_embedded", {}).get("searchResult", {})
        for result in search_result.get("_embedded", {}).get("objects", []):
            item = result.get("_embedded", {}).get("indexableObject", {})
            if item.get("type") != "item" or not item.get("handle"):
                continue
            items.append(
                _inventory_item(session, base_url, item, timeout=timeout)
            )
        page_info = search_result.get("page", {})
        if page + 1 >= int(page_info.get("totalPages", 1)):
            break
        page += 1

    handles = [item["handle"] for item in items]
    if len(handles) != len(set(handles)):
        raise DspaceBitstreamError("DSpace collection returned duplicate item Handles.")
    items.sort(key=lambda item: item["handle"])
    inventory = {
        "collection_handle": collection_handle,
        "collection_uuid": collection["uuid"],
        "downloaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "items": items,
    }
    _write_json_atomic(output_path, inventory)
    return inventory


def load_dspace_bitstream_inventory(path: Path) -> dict[str, dict]:
    """Load a staged inventory keyed by normalized Handle."""
    if not path.is_file():
        raise FileNotFoundError(
            f"DSpace bitstream inventory was not found: {path}. "
            "Run the OAI download stage before harvesting."
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    result = {}
    for item in payload.get("items", []):
        handle = str(item.get("handle", "")).strip().strip("/")
        if not handle or handle in result:
            raise DspaceBitstreamError(
                "DSpace bitstream inventory contains a blank or duplicate Handle."
            )
        result[handle] = item
    return result


def enrich_records_with_dspace_bitstreams(
    records: list[dict], inventory: dict[str, dict]
) -> list[dict]:
    """Attach canonical landing pages and labeled download links by OAI Handle."""
    enriched = []
    missing = []
    for record in records:
        handle = _handle_from_oai_identifier(record.get("oai_identifier", ""))
        item = inventory.get(handle)
        if item is None:
            missing.append(handle or str(record.get("oai_identifier", "")))
            continue
        downloads = []
        for bitstream in item.get("files", []):
            url = str(bitstream.get("download_url", "")).strip()
            if not url:
                continue
            downloads.append(
                {
                    "url": url,
                    "label": str(bitstream.get("description", "")).strip()
                    or str(bitstream.get("name", "")).strip(),
                }
            )
        candidate = dict(record)
        candidate["information"] = f"https://hdl.handle.net/{handle}"
        candidate["download"] = downloads
        candidate["thumbnail"] = str(item.get("thumbnail_url", "")).strip()
        enriched.append(candidate)
    if missing:
        raise DspaceBitstreamError(
            f"DSpace inventory is missing {len(missing)} OAI record(s): "
            + ", ".join(missing[:5])
            + ("..." if len(missing) > 5 else "")
        )
    return enriched


def _find_collection(session, *, base_url: str, handle: str, timeout: int) -> dict:
    payload = _get_json(
        session,
        f"{base_url}/discover/search/objects",
        timeout,
        params={"query": f"handle:{handle}", "size": 10},
    )
    objects = payload.get("_embedded", {}).get("searchResult", {}).get(
        "_embedded", {}
    ).get("objects", [])
    matches = [
        result.get("_embedded", {}).get("indexableObject", {})
        for result in objects
        if result.get("_embedded", {}).get("indexableObject", {}).get("type")
        == "collection"
        and result.get("_embedded", {}).get("indexableObject", {}).get("handle")
        == handle
    ]
    if len(matches) != 1:
        raise DspaceBitstreamError(
            f"Expected one DSpace collection for Handle {handle!r}; found {len(matches)}."
        )
    return matches[0]


def _inventory_item(session, base_url: str, item: dict, *, timeout: int) -> dict:
    thumbnail = _get_optional_json(
        session,
        f"{base_url}/core/items/{item['uuid']}/thumbnail",
        timeout,
    )
    bundles = _get_json(
        session,
        f"{base_url}/core/items/{item['uuid']}/bundles",
        timeout,
        params={"size": 100},
    )
    originals = [
        bundle
        for bundle in bundles.get("_embedded", {}).get("bundles", [])
        if bundle.get("name") == "ORIGINAL"
    ]
    if len(originals) != 1:
        raise DspaceBitstreamError(
            f"Item {item['handle']} has {len(originals)} ORIGINAL bundles."
        )
    thumbnail_url = str(
        thumbnail.get("_links", {}).get("content", {}).get("href", "")
        if thumbnail
        else ""
    ).strip()
    if not thumbnail_url:
        thumbnail_bundles = [
            bundle
            for bundle in bundles.get("_embedded", {}).get("bundles", [])
            if bundle.get("name") == "THUMBNAIL"
        ]
        if thumbnail_bundles:
            thumbnail_bitstreams = _get_json(
                session,
                f"{base_url}/core/bundles/{thumbnail_bundles[0]['uuid']}/bitstreams",
                timeout,
                params={"size": 1000},
            ).get("_embedded", {}).get("bitstreams", [])
            thumbnail_candidates = sorted(
                (
                    bitstream
                    for bitstream in thumbnail_bitstreams
                    if bitstream.get("_links", {}).get("content", {}).get("href")
                ),
                key=lambda bitstream: str(bitstream.get("name", "")).casefold(),
            )
            if thumbnail_candidates:
                thumbnail_url = thumbnail_candidates[0]["_links"]["content"]["href"]
    bitstreams = _get_json(
        session,
        f"{base_url}/core/bundles/{originals[0]['uuid']}/bitstreams",
        timeout,
        params={"size": 1000},
    )
    files = []
    for bitstream in bitstreams.get("_embedded", {}).get("bitstreams", []):
        descriptions = [
            str(value.get("value", "")).strip()
            for value in bitstream.get("metadata", {}).get("dc.description", [])
            if str(value.get("value", "")).strip()
        ]
        uuid = bitstream.get("uuid", "")
        files.append(
            {
                "uuid": uuid,
                "name": bitstream.get("name", ""),
                "description": " | ".join(descriptions),
                "size_bytes": bitstream.get("sizeBytes"),
                "download_url": f"{_ui_base(base_url)}/bitstreams/{uuid}/download",
            }
        )
    return {
        "handle": item["handle"],
        "uuid": item["uuid"],
        "title": item.get("name", ""),
        "thumbnail_url": thumbnail_url,
        "files": files,
    }


def _get_json(session, url: str, timeout: int, *, params: dict) -> dict:
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _get_optional_json(session, url: str, timeout: int) -> dict:
    """Return a linked DSpace resource, treating a missing relation as blank."""
    response = session.get(url, timeout=timeout)
    if response.status_code in {204, 404} or not response.content.strip():
        return {}
    response.raise_for_status()
    return response.json()


def _handle_from_oai_identifier(value: str) -> str:
    match = re.search(r":(\d+(?:\.\d+)?/[^:|/?#\s]+)$", str(value or "").strip())
    return match.group(1) if match else ""


def _ui_base(rest_base_url: str) -> str:
    return rest_base_url.removesuffix("/server/api")


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=path.parent,
            prefix=path.name + ".",
            suffix=".tmp",
            delete=False,
            encoding="utf-8",
        ) as handle:
            temporary = Path(handle.name)
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
