import csv
from pathlib import Path

from scripts.compare_gdrs_archives import compare_archives, write_comparison


def _make_resource(
    pub: Path,
    organization: str,
    resource: str,
    guid: str,
    content: bytes,
) -> None:
    resource_dir = pub / organization / resource
    metadata_dir = resource_dir / "metadata"
    metadata_dir.mkdir(parents=True)
    (metadata_dir / "metadata.xml").write_text(
        "<metadata><idinfo><citation><citeinfo>"
        "<origin>Example</origin><pubdate>20200101</pubdate>"
        f"<title>{resource}</title>"
        "</citeinfo></citation></idinfo></metadata>",
        encoding="utf-8",
    )
    (resource_dir / "dataResource.xml").write_text(
        "<dataResource>"
        f"<baseName>{resource}</baseName><publisherID>{organization}</publisherID>"
        f"<resourceIdentifiers><resourceGUID>{{{guid}}}</resourceGUID>"
        "</resourceIdentifiers></dataResource>",
        encoding="utf-8",
    )
    geodatabase = resource_dir / "fgdb" / "dataset.gdb"
    geodatabase.mkdir(parents=True)
    (geodatabase / "a00000001.gdbtable").write_bytes(content)


def test_checksum_matches_by_guid_and_detects_same_size_changes(tmp_path) -> None:
    old_pub = tmp_path / "old" / "data" / "pub"
    new_pub = tmp_path / "new" / "data" / "pub"
    _make_resource(old_pub, "old_org", "old_name", "same-guid", b"abc")
    _make_resource(old_pub, "old_org", "removed", "removed-guid", b"old")
    _make_resource(new_pub, "new_org", "renamed", "same-guid", b"xyz")
    _make_resource(new_pub, "new_org", "brand_new", "new-guid", b"new")

    checksum_rows = compare_archives(tmp_path / "old", tmp_path / "new")
    rows_by_resource = {row.resource_2026: row for row in checksum_rows}

    assert rows_by_resource["renamed"].status == "changed"
    assert rows_by_resource["renamed"].match_method == "resourceGUID"
    assert rows_by_resource["renamed"].change_reason == "content checksums differ"
    assert rows_by_resource["renamed"].archive_2026 == "yes"
    assert rows_by_resource["brand_new"].status == "new"
    removed_row = next(row for row in checksum_rows if row.status == "removed")
    assert removed_row.resource_2017 == "removed"
    assert removed_row.resource_2026 == ""
    assert removed_row.archive_2026 == "no"

    size_rows = compare_archives(tmp_path / "old", tmp_path / "new", method="size")
    size_by_resource = {row.resource_2026: row for row in size_rows}
    assert size_by_resource["renamed"].status == "unchanged"
    assert sum(row.status == "removed" for row in size_rows) == 1


def test_unchanged_checksum_and_candidate_csv(tmp_path) -> None:
    old_pub = tmp_path / "old" / "data" / "pub"
    new_pub = tmp_path / "new" / "data" / "pub"
    _make_resource(old_pub, "org", "same", "same-guid", b"same")
    _make_resource(new_pub, "org", "same", "same-guid", b"same")

    rows = compare_archives(tmp_path / "old", tmp_path / "new")
    output = tmp_path / "comparison.csv"
    write_comparison(output, rows)

    assert rows[0].status == "unchanged"
    assert rows[0].archive_2026 == "no"
    assert rows[0].content_sha256_2017 == rows[0].content_sha256_2026
    with output.open(encoding="utf-8", newline="") as handle:
        written = list(csv.DictReader(handle))
    assert written[0]["status"] == "unchanged"
