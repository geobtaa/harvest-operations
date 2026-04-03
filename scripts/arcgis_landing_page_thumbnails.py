#!/usr/bin/env python3
"""
Scan ArcGIS Hub landing pages and extract dataset thumbnail URLs.

The default input is `inputs/arcgisLandingPages.csv`, which is expected to
contain a `landingPage` column. The output preserves the original columns and
adds:

- `thumbnailUrl`
- `thumbnailItemId`
- `thumbnailSource`
- `status`
- `error`
"""

from __future__ import annotations

import argparse
import csv
import html
import re
import time
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_INPUT_CANDIDATES = (
    PROJECT_ROOT / "inputs" / "arcgisLandingPages.csv",
    PROJECT_ROOT / "input" / "arcgisLandingPages.csv",
)
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "outputs" / "arcgis_landing_page_thumbnails.csv"

LANDING_PAGE_COLUMNS = ("landingPage", "landing_page", "url", "URL")
THUMBNAIL_FIELDS = [
    "thumbnailUrl",
    "thumbnailItemId",
    "thumbnailSource",
    "status",
    "error",
]

THUMBNAIL_URL_PATTERN = re.compile(
    r"https?://[^\"'\s<>]+/sharing/rest/content/items/(?P<item_id>[^/?\"'<>]+)/info/"
    r"thumbnail/[^\"'\s<>]+(?:\?[^\"'\s<>]+)?",
    re.IGNORECASE,
)
ITEM_ID_PATTERN = re.compile(
    r"/sharing/rest/content/items/(?P<item_id>[^/?#]+)/info/thumbnail/",
    re.IGNORECASE,
)
CANONICAL_ITEM_ID_PATTERNS = (
    re.compile(r"https?://www\.arcgis\.com/home/item\.html\?id=(?P<item_id>[^\"'&\s<>]+)"),
    re.compile(
        r"https?://www\.arcgis\.com/sharing/rest/content/items/"
        r"(?P<item_id>[^/?\"'&<>]+)/info/metadata/",
        re.IGNORECASE,
    ),
    re.compile(r"[?&]layers=(?P<item_id>[^\"'&\s<>]+)"),
)


class PlaywrightThumbnailFetcher:
    def __init__(self, page_timeout_ms: int, wait_after_load_ms: int):
        self.page_timeout_ms = page_timeout_ms
        self.wait_after_load_ms = wait_after_load_ms
        self._playwright = None
        self._browser = None
        self._page = None

    def __enter__(self) -> "PlaywrightThumbnailFetcher":
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError(
                "Playwright is not installed. Install it with `pip install playwright` "
                "and then run `playwright install chromium`."
            ) from exc

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=True)
        self._page = self._browser.new_page()
        self._page.set_default_timeout(self.page_timeout_ms)
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        if self._page is not None:
            self._page.close()
        if self._browser is not None:
            self._browser.close()
        if self._playwright is not None:
            self._playwright.stop()

    def load_page(self, url: str) -> None:
        assert self._page is not None
        self._page.goto(url, wait_until="domcontentloaded")
        if self.wait_after_load_ms > 0:
            self._page.wait_for_timeout(self.wait_after_load_ms)

    def fetch_html(self, url: str) -> str:
        self.load_page(url)
        return self._page.content()

    def fetch_dom_images(self, url: str) -> list[dict[str, str]]:
        self.load_page(url)
        assert self._page is not None
        try:
            self._page.wait_for_function(
                """
                () => Array.from(document.images).some((img) => {
                  const src = img.currentSrc || img.src || "";
                  return src.includes("/sharing/rest/content/items/")
                    && src.includes("/info/thumbnail/");
                })
                """,
                timeout=min(self.page_timeout_ms, 5000),
            )
        except Exception:  # noqa: BLE001
            pass
        return self._page.evaluate(
            """
            Array.from(document.images).map((img) => ({
              alt: img.alt || "",
              src: img.currentSrc || img.src || ""
            }))
            """
        )


def resolve_default_input_csv() -> Path:
    for candidate in DEFAULT_INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    return DEFAULT_INPUT_CANDIDATES[0]


def resolve_path(path_value: str | None, *, default: Path | None = None) -> Path:
    if path_value is None:
        if default is None:
            raise ValueError("A path value is required.")
        return default

    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (PROJECT_ROOT / candidate).resolve()


def looks_like_url(value: str) -> bool:
    parsed = urlparse(value.strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def find_landing_page_column(fieldnames: Iterable[str] | None) -> str | None:
    if fieldnames is None:
        return None

    for field in fieldnames:
        for candidate in LANDING_PAGE_COLUMNS:
            if field.strip().casefold() == candidate.casefold():
                return field
    return None


def load_input_rows(csv_path: Path) -> tuple[list[str], list[dict[str, str]], str]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        landing_page_column = find_landing_page_column(reader.fieldnames)

        if landing_page_column is None:
            raise ValueError(
                f"Could not find a landing page column in {csv_path}. "
                f"Expected one of: {', '.join(LANDING_PAGE_COLUMNS)}."
            )

        rows: list[dict[str, str]] = []
        for row_number, raw_row in enumerate(reader, start=2):
            row = {str(key): str(value or "").strip() for key, value in raw_row.items() if key}
            url = row.get(landing_page_column, "").strip()
            if not url:
                continue
            if not looks_like_url(url):
                raise ValueError(f"Row {row_number} has an invalid URL: {url}")
            rows.append(row)

    if not rows:
        raise ValueError(f"No landing pages found in {csv_path}")

    fieldnames = list(reader.fieldnames or [])
    return fieldnames, rows, landing_page_column


def normalize_thumbnail_url(value: str) -> str:
    url = html.unescape(value.strip()).replace("\\/", "/")
    if url.startswith("/sharing/rest/"):
        return urljoin("https://www.arcgis.com", url)
    return url


def is_thumbnail_url(value: str) -> bool:
    return bool(THUMBNAIL_URL_PATTERN.search(normalize_thumbnail_url(value)))


def extract_item_id(thumbnail_url: str) -> str:
    match = ITEM_ID_PATTERN.search(normalize_thumbnail_url(thumbnail_url))
    return match.group("item_id") if match else ""


def extract_canonical_item_id(html_text: str) -> str:
    normalized_html = html.unescape(html_text).replace("\\/", "/")
    matches: list[str] = []
    for pattern in CANONICAL_ITEM_ID_PATTERNS:
        matches.extend(match.group("item_id") for match in pattern.finditer(normalized_html))

    for item_id in matches:
        if item_id and not item_id.endswith("_0"):
            return item_id
    return matches[0] if matches else ""


def set_thumbnail_width(thumbnail_url: str, width: int) -> str:
    split_url = urlsplit(thumbnail_url)
    query_pairs = [(key, value) for key, value in parse_qsl(split_url.query, keep_blank_values=True)]
    filtered_pairs = [(key, value) for key, value in query_pairs if key != "w"]
    filtered_pairs.append(("w", str(width)))
    return urlunsplit(
        (
            split_url.scheme,
            split_url.netloc,
            split_url.path,
            urlencode(filtered_pairs),
            split_url.fragment,
        )
    )


def maybe_rewrite_meta_thumbnail_url(thumbnail_url: str, thumbnail_source: str, html_text: str) -> str:
    if not thumbnail_url or not thumbnail_source.startswith("meta:"):
        return thumbnail_url

    canonical_item_id = extract_canonical_item_id(html_text)
    if not canonical_item_id:
        return thumbnail_url

    current_item_id = extract_item_id(thumbnail_url)
    if not current_item_id or current_item_id == canonical_item_id:
        return thumbnail_url

    rewritten_url = ITEM_ID_PATTERN.sub(
        f"/sharing/rest/content/items/{canonical_item_id}/info/thumbnail/",
        normalize_thumbnail_url(thumbnail_url),
        count=1,
    )
    return set_thumbnail_width(rewritten_url, width=800)


def extract_thumbnail_from_dom_images(image_records: Iterable[dict[str, str]]) -> tuple[str, str]:
    candidates: list[tuple[str, str]] = []
    for record in image_records:
        src = normalize_thumbnail_url(str(record.get("src", "")).strip())
        if not is_thumbnail_url(src):
            continue

        alt_text = str(record.get("alt", "")).strip()
        if alt_text:
            candidates.append((src, "img:dom-thumbnail-alt"))
        else:
            candidates.append((src, "img:dom"))

    if not candidates:
        return "", ""

    return candidates[0]


def mark_playwright_source(source: str) -> str:
    if not source:
        return source
    if source.endswith("+playwright"):
        return source
    return f"{source}+playwright"


def extract_thumbnail_candidates_from_html(html_text: str) -> list[tuple[str, str]]:
    normalized_html = html.unescape(html_text).replace("\\/", "/")
    soup = BeautifulSoup(normalized_html, "html.parser")
    candidates: list[tuple[str, str]] = []

    for element in soup.select("div.content-media img, img[alt*='thumbnail' i], img[src]"):
        src = element.get("src", "").strip()
        if is_thumbnail_url(src):
            alt_text = element.get("alt", "").strip()
            if element.find_parent(class_="content-media") is not None:
                source = "img:content-media"
            elif alt_text:
                source = "img:thumbnail-alt"
            else:
                source = "img"
            candidates.append((normalize_thumbnail_url(src), source))

    meta_selectors = (
        ("meta[property='og:image']", "meta:og:image"),
        ("meta[name='twitter:image']", "meta:twitter:image"),
        ("meta[itemprop='image']", "meta:itemprop:image"),
    )
    for selector, source in meta_selectors:
        element = soup.select_one(selector)
        if element is None:
            continue
        content = element.get("content", "").strip()
        if is_thumbnail_url(content):
            candidates.append((normalize_thumbnail_url(content), source))

    for match in THUMBNAIL_URL_PATTERN.finditer(normalized_html):
        candidates.append((normalize_thumbnail_url(match.group(0)), "regex"))

    deduped: list[tuple[str, str]] = []
    seen: set[str] = set()
    for url, source in candidates:
        if url in seen:
            continue
        seen.add(url)
        deduped.append((url, source))

    return deduped


def extract_thumbnail_url(html_text: str) -> tuple[str, str]:
    candidates = extract_thumbnail_candidates_from_html(html_text)
    if not candidates:
        return "", ""

    thumbnail_url, thumbnail_source = candidates[0]
    thumbnail_url = maybe_rewrite_meta_thumbnail_url(thumbnail_url, thumbnail_source, html_text)
    return thumbnail_url, thumbnail_source


def fetch_html(
    session: requests.Session,
    url: str,
    timeout: int,
    retries: int,
    retry_wait: float,
) -> str:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            if attempt == retries:
                break
            wait_seconds = retry_wait * attempt
            print(
                f"Attempt {attempt}/{retries} failed for {url}: {exc}. "
                f"Retrying in {wait_seconds:.1f}s..."
            )
            time.sleep(wait_seconds)

    assert last_error is not None
    raise last_error


def build_result_row(
    row: dict[str, str],
    thumbnail_url: str,
    thumbnail_source: str,
    status: str,
    error: str = "",
) -> dict[str, str]:
    result = dict(row)
    result["thumbnailUrl"] = thumbnail_url
    result["thumbnailItemId"] = extract_item_id(thumbnail_url) if thumbnail_url else ""
    result["thumbnailSource"] = thumbnail_source
    result["status"] = status
    result["error"] = error
    return result


def should_try_rendered_page(thumbnail_url: str, thumbnail_source: str, render_missing: bool) -> bool:
    if not render_missing:
        return False
    if not thumbnail_url:
        return True
    return thumbnail_source.startswith("meta:")


def scan_landing_pages(
    rows: list[dict[str, str]],
    landing_page_column: str,
    timeout: int,
    retries: int,
    retry_wait: float,
    delay: float,
    render_missing: bool,
    page_wait_ms: int,
    user_agent: str,
) -> list[dict[str, str]]:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    results: list[dict[str, str]] = []
    renderer: PlaywrightThumbnailFetcher | None = None
    renderer_error: str | None = None

    try:
        for index, row in enumerate(rows, start=1):
            landing_page = row[landing_page_column]
            print(f"[{index}/{len(rows)}] Scanning {landing_page}")

            try:
                html_text = fetch_html(
                    session=session,
                    url=landing_page,
                    timeout=timeout,
                    retries=retries,
                    retry_wait=retry_wait,
                )
                thumbnail_url, thumbnail_source = extract_thumbnail_url(html_text)

                if should_try_rendered_page(thumbnail_url, thumbnail_source, render_missing):
                    if renderer is None and renderer_error is None:
                        try:
                            renderer = PlaywrightThumbnailFetcher(
                                page_timeout_ms=timeout * 1000,
                                wait_after_load_ms=page_wait_ms,
                            ).__enter__()
                        except RuntimeError as exc:
                            renderer_error = str(exc)

                    if renderer is not None:
                        rendered_images = renderer.fetch_dom_images(landing_page)
                        rendered_url, rendered_source = extract_thumbnail_from_dom_images(
                            rendered_images
                        )
                        if not rendered_url:
                            rendered_html = renderer.fetch_html(landing_page)
                            rendered_url, rendered_source = extract_thumbnail_url(rendered_html)

                        if rendered_url and (
                            not thumbnail_url
                            or thumbnail_source.startswith("meta:")
                            or rendered_source.startswith("img:")
                        ):
                            thumbnail_url = rendered_url
                            thumbnail_source = mark_playwright_source(rendered_source)

                        if thumbnail_url:
                            results.append(
                                build_result_row(
                                    row=row,
                                    thumbnail_url=thumbnail_url,
                                    thumbnail_source=thumbnail_source,
                                    status="ok",
                                )
                            )
                        else:
                            results.append(
                                build_result_row(
                                    row=row,
                                    thumbnail_url="",
                                    thumbnail_source="",
                                    status="missing",
                                    error="Thumbnail URL not found in HTML or rendered page.",
                                )
                            )
                    else:
                        if thumbnail_url:
                            results.append(
                                build_result_row(
                                    row=row,
                                    thumbnail_url=thumbnail_url,
                                    thumbnail_source=thumbnail_source,
                                    status="ok",
                                    error=renderer_error or "",
                                )
                            )
                        else:
                            results.append(
                                build_result_row(
                                    row=row,
                                    thumbnail_url="",
                                    thumbnail_source="",
                                    status="missing",
                                    error=f"Thumbnail URL not found in HTML. {renderer_error}",
                                )
                            )
                elif thumbnail_url:
                    results.append(
                        build_result_row(
                            row=row,
                            thumbnail_url=thumbnail_url,
                            thumbnail_source=thumbnail_source,
                            status="ok",
                        )
                    )
                else:
                    results.append(
                        build_result_row(
                            row=row,
                            thumbnail_url="",
                            thumbnail_source="",
                            status="missing",
                            error="Thumbnail URL not found in HTML.",
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                results.append(
                    build_result_row(
                        row=row,
                        thumbnail_url="",
                        thumbnail_source="",
                        status="error",
                        error=str(exc),
                    )
                )

            if delay > 0:
                time.sleep(delay)
    finally:
        if renderer is not None:
            renderer.__exit__(None, None, None)

    return results


def write_results_csv(fieldnames: list[str], rows: list[dict[str, str]], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_fieldnames = list(fieldnames)
    for field in THUMBNAIL_FIELDS:
        if field not in output_fieldnames:
            output_fieldnames.append(field)

    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-csv",
        default=str(resolve_default_input_csv()),
        help="Input CSV of ArcGIS Hub landing pages.",
    )
    parser.add_argument(
        "--output-csv",
        default=str(DEFAULT_OUTPUT_CSV),
        help="Output CSV for landing page thumbnail results.",
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds.")
    parser.add_argument("--retries", type=int, default=3, help="HTTP retry attempts per page.")
    parser.add_argument(
        "--retry-wait",
        type=float,
        default=1.5,
        help="Base wait time between HTTP retries in seconds.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Optional delay between landing-page scans in seconds.",
    )
    parser.add_argument(
        "--page-wait-ms",
        type=int,
        default=1500,
        help="Extra wait after Playwright loads a page before scraping rendered HTML.",
    )
    parser.add_argument(
        "--render-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use Playwright only when the plain HTML response does not expose a thumbnail URL.",
    )
    parser.add_argument(
        "--user-agent",
        default=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        help="User-Agent header for HTTP requests.",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    input_csv = resolve_path(args.input_csv)
    output_csv = resolve_path(args.output_csv)

    fieldnames, rows, landing_page_column = load_input_rows(input_csv)
    results = scan_landing_pages(
        rows=rows,
        landing_page_column=landing_page_column,
        timeout=args.timeout,
        retries=args.retries,
        retry_wait=args.retry_wait,
        delay=args.delay,
        render_missing=args.render_missing,
        page_wait_ms=args.page_wait_ms,
        user_agent=args.user_agent,
    )
    write_results_csv(fieldnames=fieldnames, rows=results, output_csv=output_csv)

    ok_count = sum(1 for row in results if row["status"] == "ok")
    missing_count = sum(1 for row in results if row["status"] == "missing")
    error_count = sum(1 for row in results if row["status"] == "error")
    print(
        f"Wrote {len(results)} rows to {output_csv} "
        f"({ok_count} ok, {missing_count} missing, {error_count} error)."
    )
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
