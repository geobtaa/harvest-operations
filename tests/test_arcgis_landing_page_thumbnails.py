from pathlib import Path

from scripts.arcgis_landing_page_thumbnails import (
    extract_canonical_item_id,
    extract_item_id,
    extract_thumbnail_from_dom_images,
    extract_thumbnail_candidates_from_html,
    extract_thumbnail_url,
    load_input_rows,
    maybe_rewrite_meta_thumbnail_url,
)


SAMPLE_HTML = """
<div class="content-media">
  <div aria-hidden="true" class="aspect-ratio-box">
    <img
      alt="Postsecondary School Locations - Current thumbnail"
      src="https://www.arcgis.com/sharing/rest/content/items/a15e8731a17a46aabc452ea607f172c0/info/thumbnail/thumbnail1763664561383.png?w=800"
      class="ember-view"
    >
  </div>
</div>
"""


def test_extract_thumbnail_url_from_content_media_image() -> None:
    thumbnail_url, source = extract_thumbnail_url(SAMPLE_HTML)

    assert (
        thumbnail_url
        == "https://www.arcgis.com/sharing/rest/content/items/"
        "a15e8731a17a46aabc452ea607f172c0/info/thumbnail/thumbnail1763664561383.png?w=800"
    )
    assert source == "img:content-media"


def test_extract_thumbnail_candidates_unescapes_meta_and_json_urls() -> None:
    html = """
    <html>
      <head>
        <meta property="og:image"
          content="https://www.arcgis.com/sharing/rest/content/items/abc123/info/thumbnail/thumb.png?w=800&amp;h=600">
      </head>
      <body>
        <script>
          window.__DATA__ = {
            "thumbnailUrl": "https:\\/\\/www.arcgis.com\\/sharing\\/rest\\/content\\/items\\/abc123\\/info\\/thumbnail\\/thumb.png?w=800&h=600"
          };
        </script>
      </body>
    </html>
    """

    candidates = extract_thumbnail_candidates_from_html(html)

    assert candidates[0] == (
        "https://www.arcgis.com/sharing/rest/content/items/abc123/info/thumbnail/thumb.png?w=800&h=600",
        "meta:og:image",
    )


def test_extract_canonical_item_id_prefers_non_share_item_id() -> None:
    html = """
    <a href="https://www.arcgis.com/home/item.html?id=a15e8731a17a46aabc452ea607f172c0">AGO</a>
    <meta property="og:image"
      content="https://www.arcgis.com/sharing/rest/content/items/a15e8731a17a46aabc452ea607f172c0_0/info/thumbnail/thumb.png?w=500">
    """

    assert extract_canonical_item_id(html) == "a15e8731a17a46aabc452ea607f172c0"


def test_rewrite_meta_thumbnail_url_uses_canonical_item_id_and_width() -> None:
    html = """
    <a href="https://www.arcgis.com/home/item.html?id=a15e8731a17a46aabc452ea607f172c0">AGO</a>
    """
    meta_thumbnail_url = (
        "https://www.arcgis.com/sharing/rest/content/items/"
        "a15e8731a17a46aabc452ea607f172c0_0/info/thumbnail/thumbnail1763664561383.png?w=500"
    )

    rewritten = maybe_rewrite_meta_thumbnail_url(meta_thumbnail_url, "meta:og:image", html)

    assert rewritten == (
        "https://www.arcgis.com/sharing/rest/content/items/"
        "a15e8731a17a46aabc452ea607f172c0/info/thumbnail/thumbnail1763664561383.png?w=800"
    )


def test_extract_thumbnail_from_dom_images_prefers_thumbnail_image() -> None:
    images = [
        {
            "alt": "Postsecondary School Locations - Current thumbnail",
            "src": (
                "https://www.arcgis.com/sharing/rest/content/items/"
                "a15e8731a17a46aabc452ea607f172c0/info/thumbnail/"
                "thumbnail1763664561383.png?w=800"
            ),
        },
        {
            "alt": "",
            "src": "https://www.arcgis.com/sharing/rest/community/users/OpenDataMgr_NCES/info/NCES.png",
        },
    ]

    assert extract_thumbnail_from_dom_images(images) == (
        "https://www.arcgis.com/sharing/rest/content/items/"
        "a15e8731a17a46aabc452ea607f172c0/info/thumbnail/thumbnail1763664561383.png?w=800",
        "img:dom-thumbnail-alt",
    )


def test_extract_item_id_from_thumbnail_url() -> None:
    thumbnail_url = (
        "https://www.arcgis.com/sharing/rest/content/items/"
        "a15e8731a17a46aabc452ea607f172c0/info/thumbnail/thumbnail1763664561383.png?w=800"
    )

    assert extract_item_id(thumbnail_url) == "a15e8731a17a46aabc452ea607f172c0"


def test_load_input_rows_accepts_landing_page_header(tmp_path: Path) -> None:
    csv_path = tmp_path / "arcgisLandingPages.csv"
    csv_path.write_text(
        "landingPage\n"
        "https://data-nces.opendata.arcgis.com/datasets/nces::example/about\n",
        encoding="utf-8",
    )

    fieldnames, rows, landing_page_column = load_input_rows(csv_path)

    assert fieldnames == ["landingPage"]
    assert landing_page_column == "landingPage"
    assert rows == [
        {"landingPage": "https://data-nces.opendata.arcgis.com/datasets/nces::example/about"}
    ]
