#!/usr/bin/env python3
"""
Download JSON documents from a CSV-style URL list.

The default input is inputs/oregon-maps.csv, which contains one quoted URL per
line. Each response is saved under inputs/oregon-maps-json/ using a stable path
derived from the source URL, and a manifest is written at the end of the run.
"""

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import parse_qsl, urlparse

import requests


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_INPUT_CSV = PROJECT_ROOT / "inputs" / "oregon-maps.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "inputs" / "oregon-maps-json"


class InvalidJsonResponseError(RuntimeError):
    pass


def optional_playwright_import():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is not installed. Install it with `pip install playwright` "
            "and then run `playwright install chromium`."
        ) from exc

    return sync_playwright


def resolve_path(path_value: str) -> Path:
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (PROJECT_ROOT / candidate).resolve()


def slugify_segment(value: str) -> str:
    clean = "".join(char if char.isalnum() or char in "._-" else "-" for char in value.strip())
    clean = clean.strip(".-")
    while "--" in clean:
        clean = clean.replace("--", "-")
    return clean or "item"


def looks_like_url(value: str) -> bool:
    parsed = urlparse(value.strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def load_urls(csv_path: Path) -> list[str]:
    urls: list[str] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row_number, row in enumerate(reader, start=1):
            values = [str(value).strip() for value in row if str(value).strip()]
            if not values:
                continue

            value = values[0]
            if value.startswith("#"):
                continue

            if row_number == 1 and not looks_like_url(value):
                normalized = value.casefold()
                if normalized in {"url", "urls", "json_url", "json_urls"}:
                    continue

            if not looks_like_url(value):
                raise ValueError(f"Row {row_number} is not a valid URL: {value}")

            urls.append(value)

    if not urls:
        raise ValueError(f"No URLs found in {csv_path}")

    return urls


def build_relative_output_path(url: str) -> Path:
    parsed = urlparse(url)
    host = slugify_segment(parsed.netloc)
    path_segments = [slugify_segment(part) for part in parsed.path.split("/") if part]

    if not path_segments:
        path_segments = ["index.json"]
    elif not path_segments[-1].endswith(".json"):
        path_segments[-1] = f"{path_segments[-1]}.json"

    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    if query_items:
        suffix = "-".join(
            slugify_segment(f"{key}-{value}") if value else slugify_segment(key)
            for key, value in query_items
        )
        path = Path(host, *path_segments)
        return path.with_name(f"{path.stem}-{suffix}{path.suffix}")

    return Path(host, *path_segments)


def uniquify_paths(paths: Iterable[Path]) -> list[Path]:
    seen: dict[Path, int] = {}
    unique_paths: list[Path] = []

    for path in paths:
        count = seen.get(path, 0)
        seen[path] = count + 1
        if count == 0:
            unique_paths.append(path)
            continue

        unique_paths.append(path.with_name(f"{path.stem}-{count + 1}{path.suffix}"))

    return unique_paths


def summarize_response_body(response: requests.Response, limit: int = 240) -> str:
    text = response.text.strip()
    if not text:
        return "<empty body>"

    snippet = " ".join(text.split())
    if len(snippet) > limit:
        snippet = f"{snippet[:limit]}..."
    return snippet


def extract_json_text_from_html(html_text: str) -> Optional[str]:
    marker = "<pre"
    start = html_text.find(marker)
    if start == -1:
        return None

    start = html_text.find(">", start)
    if start == -1:
        return None

    end = html_text.find("</pre>", start)
    if end == -1:
        return None

    return html_text[start + 1 : end].strip()


def validate_json_text(text: str) -> bytes:
    json.loads(text)
    return text.encode("utf-8")


def load_cookie_value(cookie: Optional[str], cookie_file: Optional[str]) -> Optional[str]:
    if cookie:
        return cookie.strip()
    if cookie_file:
        return resolve_path(cookie_file).read_text(encoding="utf-8").strip()
    return None


def parse_extra_headers(header_values: Optional[list[str]]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for value in header_values or []:
        if ":" not in value:
            raise ValueError(f"Invalid header format: {value!r}. Use 'Name: value'.")
        name, header_value = value.split(":", 1)
        headers[name.strip()] = header_value.strip()
    return headers


def fetch_json_bytes(
    session: requests.Session,
    url: str,
    timeout: int,
    retries: int,
    retry_wait: float,
) -> tuple[bytes, requests.Response, int]:
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            payload = response.content
            try:
                json.loads(payload)
            except json.JSONDecodeError as exc:
                content_type = response.headers.get("Content-Type", "")
                extracted = extract_json_text_from_html(response.text)
                if extracted is not None:
                    return validate_json_text(extracted), response, attempt
                body_preview = summarize_response_body(response)
                raise InvalidJsonResponseError(
                    f"Invalid JSON response. HTTP {response.status_code}, "
                    f"Content-Type: {content_type or '<missing>'}, "
                    f"body preview: {body_preview}"
                ) from exc
            return payload, response, attempt
        except (requests.RequestException, InvalidJsonResponseError) as exc:
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


def fetch_json_bytes_playwright(
    page,
    url: str,
    timeout_ms: int,
    retries: int,
    retry_wait: float,
) -> tuple[bytes, int]:
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            if response is None:
                raise RuntimeError("No response received from page navigation.")

            page.wait_for_load_state("networkidle", timeout=timeout_ms)

            pre = page.locator("pre").first
            if pre.count():
                pre_text = pre.text_content() or ""
                if pre_text.strip():
                    return validate_json_text(pre_text), attempt

            body_text = page.locator("body").inner_text(timeout=timeout_ms).strip()
            if body_text:
                try:
                    return validate_json_text(body_text), attempt
                except json.JSONDecodeError:
                    pass

            html_text = page.content()
            extracted = extract_json_text_from_html(html_text)
            if extracted is not None:
                return validate_json_text(extracted), attempt

            raise InvalidJsonResponseError(
                f"Invalid browser response. Final URL: {page.url}, "
                f"HTTP {response.status}, body preview: {' '.join(body_text.split())[:240] or '<empty body>'}"
            )
        except Exception as exc:
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download JSON documents from a CSV-style URL list."
    )
    parser.add_argument(
        "--input-csv",
        default=str(DEFAULT_INPUT_CSV),
        help="CSV or line-delimited file containing one URL per row.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where downloaded JSON files will be written.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=180,
        help="HTTP timeout in seconds for each request. Default: 180.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        help="Number of attempts per URL before marking it failed. Default: 4.",
    )
    parser.add_argument(
        "--retry-wait",
        type=float,
        default=5.0,
        help="Base seconds to wait before retrying a failed request. Default: 5.0.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Seconds to sleep between successful requests. Default: 0.25.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only process the first N URLs from the input list.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip files that already exist in the output directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned downloads without making HTTP requests.",
    )
    parser.add_argument(
        "--backend",
        choices=("requests", "playwright"),
        default="playwright",
        help="Downloader backend to use. Default: playwright.",
    )
    parser.add_argument(
        "--browser",
        choices=("chromium", "firefox", "webkit"),
        default="chromium",
        help="Browser engine for the Playwright backend. Default: chromium.",
    )
    parser.add_argument(
        "--chrome-channel",
        action="store_true",
        help="Use the installed Google Chrome channel with Playwright instead of bundled Chromium.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Playwright in headless mode. Off by default so challenge pages are visible.",
    )
    parser.add_argument(
        "--user-data-dir",
        help=(
            "Persistent browser profile directory for the Playwright backend. "
            "Using a stable profile can help keep challenge/session state."
        ),
    )
    parser.add_argument(
        "--cookie",
        help="Raw Cookie header value to send with the requests backend.",
    )
    parser.add_argument(
        "--cookie-file",
        help="Path to a text file containing a raw Cookie header value.",
    )
    parser.add_argument(
        "--header",
        action="append",
        help="Extra request header for the requests backend. Repeat as needed. Format: 'Name: value'.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    input_csv = resolve_path(args.input_csv)
    output_dir = resolve_path(args.output_dir)

    if not input_csv.exists():
        parser.error(f"Input file not found: {input_csv}")
    if args.retries < 1:
        parser.error("--retries must be at least 1.")
    if args.timeout < 1:
        parser.error("--timeout must be at least 1.")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1 when provided.")

    try:
        cookie_value = load_cookie_value(args.cookie, args.cookie_file)
        extra_headers = parse_extra_headers(args.header)
    except ValueError as exc:
        parser.error(str(exc))

    urls = load_urls(input_csv)
    if args.limit:
        urls = urls[: args.limit]

    relative_paths = uniquify_paths(build_relative_output_path(url) for url in urls)

    if args.dry_run:
        print(f"Input file: {input_csv}")
        print(f"Output directory: {output_dir}")
        print(f"URLs to download: {len(urls)}")
        print(f"Timeout: {args.timeout}s")
        print(f"Retries: {args.retries}")
        for url, relative_path in zip(urls[:10], relative_paths):
            print(f"- {url} -> {relative_path}")
        if len(urls) > 10:
            print(f"... {len(urls) - 10} more URL(s)")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    started_at = time.strftime("%Y-%m-%d %H:%M:%S")
    manifest_entries: list[dict] = []
    downloaded_count = 0
    skipped_count = 0
    failed_count = 0

    def process_one(index: int, url: str, relative_path: Path, session=None, page=None) -> None:
        nonlocal downloaded_count, skipped_count, failed_count

        target_path = output_dir / relative_path

        if args.skip_existing and target_path.exists():
            skipped_count += 1
            manifest_entries.append(
                {
                    "url": url,
                    "output_path": str(relative_path),
                    "status": "skipped",
                    "attempts": 0,
                    "bytes": target_path.stat().st_size,
                }
            )
            print(f"[{index}/{len(urls)}] Skipped existing file {relative_path}")
            return

        target_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if args.backend == "playwright":
                payload, attempts = fetch_json_bytes_playwright(
                    page=page,
                    url=url,
                    timeout_ms=args.timeout * 1000,
                    retries=args.retries,
                    retry_wait=args.retry_wait,
                )
                http_status = None
                content_type = "browser-extracted"
            else:
                payload, response, attempts = fetch_json_bytes(
                    session=session,
                    url=url,
                    timeout=args.timeout,
                    retries=args.retries,
                    retry_wait=args.retry_wait,
                )
                http_status = response.status_code
                content_type = response.headers.get("Content-Type", "")

            target_path.write_bytes(payload)
            downloaded_count += 1
            manifest_entries.append(
                {
                    "url": url,
                    "output_path": str(relative_path),
                    "status": "downloaded",
                    "attempts": attempts,
                    "bytes": len(payload),
                    "http_status": http_status,
                    "content_type": content_type,
                }
            )
            print(f"[{index}/{len(urls)}] Downloaded {relative_path}")
        except Exception as exc:
            failed_count += 1
            manifest_entries.append(
                {
                    "url": url,
                    "output_path": str(relative_path),
                    "status": "failed",
                    "error": str(exc),
                }
            )
            print(f"[{index}/{len(urls)}] Failed {url}: {exc}")

        if args.delay > 0 and index < len(urls):
            time.sleep(args.delay)

    if args.backend == "playwright":
        sync_playwright = optional_playwright_import()
        with sync_playwright() as pw:
            launch_kwargs = {
                "headless": args.headless,
            }
            context_kwargs = {
                "locale": "en-US",
                "user_agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/146.0.0.0 Safari/537.36"
                ),
                "viewport": {"width": 1440, "height": 960},
            }
            if args.chrome_channel:
                launch_kwargs["channel"] = "chrome"

            if args.user_data_dir:
                context = getattr(pw, args.browser).launch_persistent_context(
                    user_data_dir=str(resolve_path(args.user_data_dir)),
                    **launch_kwargs,
                    **context_kwargs,
                )
                pages = context.pages
                page = pages[0] if pages else context.new_page()
                browser = None
            else:
                browser = getattr(pw, args.browser).launch(**launch_kwargs)
                context = browser.new_context(**context_kwargs)
                page = context.new_page()

            page.add_init_script(
                """
                Object.defineProperty(navigator, 'webdriver', {
                  get: () => undefined,
                });
                """
            )

            for index, (url, relative_path) in enumerate(zip(urls, relative_paths), start=1):
                process_one(index=index, url=url, relative_path=relative_path, page=page)

            context.close()
            if browser is not None:
                browser.close()
    else:
        session = requests.Session()
        request_headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
        }
        if cookie_value:
            request_headers["Cookie"] = cookie_value
        request_headers.update(extra_headers)
        session.headers.update(request_headers)

        for index, (url, relative_path) in enumerate(zip(urls, relative_paths), start=1):
            process_one(index=index, url=url, relative_path=relative_path, session=session)

    manifest = {
        "input_csv": str(input_csv),
        "output_dir": str(output_dir),
        "started_at": started_at,
        "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "backend": args.backend,
        "timeout": args.timeout,
        "retries": args.retries,
        "retry_wait": args.retry_wait,
        "delay": args.delay,
        "url_count": len(urls),
        "downloaded_count": downloaded_count,
        "skipped_count": skipped_count,
        "failed_count": failed_count,
        "files": manifest_entries,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Manifest written to {manifest_path}")
    print(
        f"Done. Downloaded: {downloaded_count}, skipped: {skipped_count}, failed: {failed_count}"
    )

    if failed_count:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
