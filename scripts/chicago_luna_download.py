#!/usr/bin/env python3
"""
Download a University of Chicago LUNA IIIF collection and its manifests to a local JSON file.

The output bundle is designed to be consumed by harvesters/chicago_luna.py.
"""

import argparse
import json
import time
from pathlib import Path

import requests
import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "chicago-luna.yaml"


def resolve_path(path_value: str) -> Path:
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (PROJECT_ROOT / candidate).resolve()


def load_config(config_path: Path) -> dict:
    with config_path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_cookie_value(config: dict) -> str:
    cookie_value = str(config.get("cookie", "")).strip()
    if cookie_value:
        return cookie_value

    cookie_file = str(config.get("cookie_file", "")).strip()
    if cookie_file:
        return resolve_path(cookie_file).read_text(encoding="utf-8").strip()

    return ""


def build_session(config: dict) -> requests.Session:
    session = requests.Session()
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        ),
    }

    cookie_value = load_cookie_value(config)
    if cookie_value:
        headers["Cookie"] = cookie_value

    extra_headers = config.get("headers", {})
    if isinstance(extra_headers, dict):
        headers.update({str(key): str(value) for key, value in extra_headers.items()})

    session.headers.update(headers)
    return session


def get_json(session: requests.Session, url: str, timeout: int, delay: float) -> dict | None:
    print(f"[Chicago Luna Download] Fetching {url}")
    response = session.get(url, timeout=timeout)
    response.raise_for_status()

    content_type = response.headers.get("content-type", "")
    if "html" in content_type.lower() or "Verify Access" in response.text[:500]:
        print(
            "[Chicago Luna Download] The endpoint returned an access verification page "
            f"for {url}. Skipping this response."
        )
        return None

    if delay > 0:
        time.sleep(delay)

    return response.json()


def extract_label(value):
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        for item in value:
            text = extract_label(item)
            if text:
                return text
        return ""
    if isinstance(value, dict):
        for candidate in value.values():
            text = extract_label(candidate)
            if text:
                return text
    return ""


def extract_resource_id(value):
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        if value.get("id"):
            return str(value["id"]).strip()
        if value.get("@id"):
            return str(value["@id"]).strip()
    return ""


def extract_manifest_refs(page_json: dict) -> list[dict]:
    manifest_refs = []
    manifest_entries = []

    for key in ("manifests", "members", "items"):
        candidate = page_json.get(key, [])
        if isinstance(candidate, list):
            manifest_entries.extend(candidate)

    for entry in manifest_entries:
        manifest_url = extract_resource_id(entry)
        if not manifest_url:
            continue

        entry_type = ""
        if isinstance(entry, dict):
            entry_type = entry.get("@type") or entry.get("type") or ""

        if entry_type and "manifest" not in str(entry_type).lower():
            continue

        manifest_refs.append(
            {
                "id": manifest_url,
                "type": entry_type or "sc:Manifest",
                "label": extract_label(entry.get("label")) if isinstance(entry, dict) else "",
            }
        )

    return manifest_refs


def infer_next_page_url(collection_url: str, page_json: dict, root_json: dict, page_manifest_count: int, page_size: int) -> str:
    try:
        current_start = int(page_json.get("startIndex", 0))
        total = int(page_json.get("total") or root_json.get("total") or 0)
    except (TypeError, ValueError):
        return ""

    effective_page_size = page_manifest_count or page_size
    next_start = current_start + effective_page_size
    if effective_page_size <= 0 or next_start >= total:
        return ""

    collection_base_url = (
        extract_resource_id(page_json.get("within"))
        or extract_resource_id(root_json.get("within"))
        or extract_resource_id(root_json)
        or collection_url
    )

    return f"{collection_base_url.rstrip('/')}/{next_start}"


def collect_manifest_links(session: requests.Session, config: dict, root_json: dict) -> list[dict]:
    manifest_refs = []
    seen_manifest_urls = set()
    seen_page_urls = set()
    queued_pages = []

    root_page_manifests = extract_manifest_refs(root_json)
    if root_page_manifests:
        queued_pages.append((config["collection_url"], root_json))
    else:
        first_page_url = extract_resource_id(root_json.get("first")) or config["collection_url"]
        queued_pages.append((first_page_url, None))

    while queued_pages:
        page_url, prefetched_json = queued_pages.pop(0)
        if not page_url or page_url in seen_page_urls:
            continue

        seen_page_urls.add(page_url)
        page_json = prefetched_json or get_json(
            session=session,
            url=page_url,
            timeout=int(config.get("timeout", 60)),
            delay=float(config.get("delay", 0)),
        )
        if page_json is None:
            continue

        page_manifests = extract_manifest_refs(page_json)
        for manifest_ref in page_manifests:
            manifest_url = manifest_ref["id"]
            if manifest_url and manifest_url not in seen_manifest_urls:
                seen_manifest_urls.add(manifest_url)
                manifest_refs.append(manifest_ref)

        print(
            f"[Chicago Luna Download] Page {page_url} yielded {len(page_manifests)} manifests "
            f"({len(manifest_refs)} total)."
        )

        next_page_url = extract_resource_id(page_json.get("next"))
        if not next_page_url:
            next_page_url = infer_next_page_url(
                collection_url=config["collection_url"],
                page_json=page_json,
                root_json=root_json,
                page_manifest_count=len(page_manifests),
                page_size=int(config.get("page_size", 0) or 0),
            )

        if next_page_url and next_page_url not in seen_page_urls:
            queued_pages.append((next_page_url, None))

    return manifest_refs


def download_bundle(config: dict) -> dict:
    session = build_session(config)
    timeout = int(config.get("timeout", 60))
    delay = float(config.get("delay", 0))

    root_json = get_json(session, config["collection_url"], timeout, delay)
    if root_json is None:
        return {
            "id": config["collection_url"],
            "label": config.get("name", "Chicago Luna"),
            "attribution": "",
            "manifest_refs": [],
            "manifests": [],
        }

    manifest_refs = collect_manifest_links(session, config, root_json)
    manifests = []

    for index, manifest_ref in enumerate(manifest_refs, start=1):
        manifest_url = manifest_ref["id"]
        manifest_json = get_json(session, manifest_url, timeout, delay)
        if manifest_json is None:
            continue

        manifests.append(manifest_json)
        print(
            f"[Chicago Luna Download] Downloaded manifest {index}/{len(manifest_refs)}: {manifest_url}"
        )

    return {
        "id": root_json.get("@id") or root_json.get("id") or config["collection_url"],
        "label": root_json.get("label") or config.get("name", "Chicago Luna"),
        "attribution": root_json.get("attribution", ""),
        "manifest_refs": manifest_refs,
        "manifests": manifests,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to the Chicago Luna YAML config file.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config_path = resolve_path(args.config)
    config = load_config(config_path)

    output_json = resolve_path(config["input_json"])
    output_json.parent.mkdir(parents=True, exist_ok=True)

    bundle = download_bundle(config)
    with output_json.open("w", encoding="utf-8") as handle:
        json.dump(bundle, handle, indent=2, ensure_ascii=False)

    print(
        f"[Chicago Luna Download] Saved {len(bundle['manifests'])} manifests to {output_json}"
    )


if __name__ == "__main__":
    main()
