#!/usr/bin/env python3
"""Compare preferred datasets between two GDRS archive snapshots.

Resources are matched by resourceGUID, then publisherID plus baseName, and
finally their organization/resource path. Dataset content is compared after
the preferred-format selection performed by ``inventory_gdrs.py``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path

if __package__:
    from scripts.inventory_gdrs import (
        SHAPEFILE_COMPONENT_EXTENSIONS,
        InventoryRow,
        inventory_gdrs,
        resolve_pub_root,
        select_preferred_format_rows,
    )
else:
    from inventory_gdrs import (  # type: ignore[import-not-found]
        SHAPEFILE_COMPONENT_EXTENSIONS,
        InventoryRow,
        inventory_gdrs,
        resolve_pub_root,
        select_preferred_format_rows,
    )


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OLD_ROOT = REPOSITORY_ROOT / "inputs" / "GDRS-December-2017"
DEFAULT_NEW_ROOT = REPOSITORY_ROOT / "inputs" / "GDRS-January-2026"
DEFAULT_OUTPUT = REPOSITORY_ROOT / "outputs" / "gdrs_2017_to_2026_comparison.csv"

IGNORED_CONTENT_NAMES = {".ds_store", "thumbs.db"}
COMPARISON_FIELDS = [
    "status",
    "archive_2026",
    "match_method",
    "change_reason",
    "organization_2017",
    "resource_2017",
    "organization_2026",
    "resource_2026",
    "resource_guid_2017",
    "resource_guid_2026",
    "resource_ids_2017",
    "resource_ids_2026",
    "publisher_id_2017",
    "publisher_id_2026",
    "base_name_2017",
    "base_name_2026",
    "current_as_of_2017",
    "current_as_of_2026",
    "format_2017",
    "format_2026",
    "dataset_filenames_2017",
    "dataset_filenames_2026",
    "filesize_bytes_2017",
    "filesize_bytes_2026",
    "filesize_delta_bytes",
    "content_sha256_2017",
    "content_sha256_2026",
]


@dataclass(frozen=True)
class ResourceInfo:
    organization: str
    resource: str
    resource_guid: str = ""
    resource_ids: str = ""
    publisher_id: str = ""
    base_name: str = ""
    current_as_of: str = ""

    @property
    def path_key(self) -> tuple[str, str]:
        return self.organization.casefold(), self.resource.casefold()

    @property
    def publisher_base_key(self) -> tuple[str, str] | None:
        if not self.publisher_id or not self.base_name:
            return None
        return self.publisher_id.casefold(), self.base_name.casefold()


@dataclass(frozen=True)
class ComparisonRow:
    status: str
    archive_2026: str
    match_method: str
    change_reason: str
    organization_2017: str
    resource_2017: str
    organization_2026: str
    resource_2026: str
    resource_guid_2017: str
    resource_guid_2026: str
    resource_ids_2017: str
    resource_ids_2026: str
    publisher_id_2017: str
    publisher_id_2026: str
    base_name_2017: str
    base_name_2026: str
    current_as_of_2017: str
    current_as_of_2026: str
    format_2017: str
    format_2026: str
    dataset_filenames_2017: str
    dataset_filenames_2026: str
    filesize_bytes_2017: int | str
    filesize_bytes_2026: int | str
    filesize_delta_bytes: int | str
    content_sha256_2017: str
    content_sha256_2026: str


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _first_text(root: ET.Element, element_name: str) -> str:
    for element in root.iter():
        if _local_name(element.tag) == element_name.lower():
            text = " ".join((element.text or "").split())
            if text:
                return text
    return ""


def _normalize_guid(value: str) -> str:
    return value.strip().strip("{}").casefold()


def _resource_ids(root: ET.Element) -> str:
    values: list[str] = []
    for element in root.iter():
        if _local_name(element.tag) != "resourceid":
            continue
        value = " ".join((element.text or "").split())
        if not value:
            continue
        id_type = element.attrib.get("type", "").strip()
        values.append(f"{id_type}:{value}" if id_type else value)
    return "; ".join(dict.fromkeys(values))


def read_resource_info(resource_dir: Path) -> ResourceInfo:
    metadata_file = resource_dir / "dataResource.xml"
    if not metadata_file.is_file():
        candidates = sorted(resource_dir.glob("dataResource*.xml"))
        metadata_file = candidates[0] if candidates else metadata_file

    root: ET.Element | None = None
    if metadata_file.is_file():
        try:
            root = ET.parse(metadata_file).getroot()
        except (ET.ParseError, OSError):
            pass

    return ResourceInfo(
        organization=resource_dir.parent.name,
        resource=resource_dir.name,
        resource_guid=(
            _normalize_guid(_first_text(root, "resourceguid"))
            if root is not None
            else ""
        ),
        resource_ids=_resource_ids(root) if root is not None else "",
        publisher_id=_first_text(root, "publisherid") if root is not None else "",
        base_name=_first_text(root, "basename") if root is not None else "",
        current_as_of=(
            _first_text(root, "currentasofdate") if root is not None else ""
        ),
    )


def read_resources(pub_root: Path) -> dict[tuple[str, str], ResourceInfo]:
    pub_root = resolve_pub_root(pub_root)
    resources: dict[tuple[str, str], ResourceInfo] = {}
    for organization_dir in sorted(
        path for path in pub_root.iterdir() if path.is_dir()
    ):
        for resource_dir in sorted(
            path for path in organization_dir.iterdir() if path.is_dir()
        ):
            info = read_resource_info(resource_dir)
            resources[info.path_key] = info
    return resources


def _unique_index(
    resources: Iterable[ResourceInfo], key_name: str
) -> dict[object, ResourceInfo]:
    candidates: dict[object, list[ResourceInfo]] = {}
    for resource in resources:
        key = getattr(resource, key_name)
        if key:
            candidates.setdefault(key, []).append(resource)
    return {key: matches[0] for key, matches in candidates.items() if len(matches) == 1}


def match_old_resource(
    current: ResourceInfo,
    old_by_guid: dict[object, ResourceInfo],
    old_by_publisher_base: dict[object, ResourceInfo],
    old_by_path: dict[object, ResourceInfo],
) -> tuple[ResourceInfo | None, str]:
    if current.resource_guid and current.resource_guid in old_by_guid:
        return old_by_guid[current.resource_guid], "resourceGUID"
    if (
        current.publisher_base_key
        and current.publisher_base_key in old_by_publisher_base
    ):
        return old_by_publisher_base[current.publisher_base_key], "publisherID+baseName"
    if current.path_key in old_by_path:
        return old_by_path[current.path_key], "organization/resource path"
    return None, ""


def _group_inventory_rows(
    rows: Iterable[InventoryRow],
) -> dict[tuple[str, str], list[InventoryRow]]:
    grouped: dict[tuple[str, str], list[InventoryRow]] = {}
    for row in rows:
        key = row.organization.casefold(), row.resource.casefold()
        grouped.setdefault(key, []).append(row)
    return grouped


def _format_name(rows: list[InventoryRow] | None) -> str:
    if not rows:
        return ""
    return rows[0].format_folder


def _filenames(rows: list[InventoryRow] | None) -> str:
    if not rows:
        return ""
    return "; ".join(sorted(row.dataset_filename for row in rows))


def _total_size(rows: list[InventoryRow] | None) -> int:
    return sum(row.filesize_bytes for row in (rows or []))


def _quick_signature(
    rows: list[InventoryRow],
) -> tuple[str, tuple[tuple[str, int], ...]]:
    return (
        _format_name(rows),
        tuple(sorted((row.dataset_filename, row.filesize_bytes) for row in rows)),
    )


def _ignored_content_file(path: Path) -> bool:
    name = path.name.casefold()
    return name in IGNORED_CONTENT_NAMES or name.endswith(".lock")


def _dataset_component_paths(dataset_path: Path) -> list[Path]:
    if dataset_path.is_dir():
        return sorted(
            path
            for path in dataset_path.rglob("*")
            if path.is_file() and not _ignored_content_file(path)
        )

    suffix = dataset_path.suffix.casefold()
    if suffix == ".shp":
        return sorted(
            path
            for path in dataset_path.parent.iterdir()
            if path.is_file()
            and path.stem.casefold() == dataset_path.stem.casefold()
            and path.suffix.casefold() in SHAPEFILE_COMPONENT_EXTENSIONS
        )
    if suffix in {".tif", ".tiff"}:
        stem = dataset_path.stem.casefold()
        filename = dataset_path.name.casefold()
        companion_names = {
            f"{stem}.aux",
            f"{stem}.tfw",
            f"{stem}.wld",
            f"{filename}.aux.xml",
            f"{filename}.ovr",
            f"{filename}.xml",
        }
        return sorted(
            path
            for path in dataset_path.parent.iterdir()
            if path.is_file()
            and (path == dataset_path or path.name.casefold() in companion_names)
        )
    if suffix == ".asc":
        return sorted(
            path
            for path in dataset_path.parent.iterdir()
            if path.is_file()
            and path.stem.casefold() == dataset_path.stem.casefold()
            and path.suffix.casefold() in {".asc", ".prj"}
        )
    return [dataset_path]


def _hash_file(path: Path, digest: hashlib._Hash) -> None:
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)


def content_signature(pub_root: Path, rows: list[InventoryRow]) -> str:
    """Hash selected dataset paths and meaningful component files."""
    pub_root = resolve_pub_root(pub_root)
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda value: value.dataset_path.casefold()):
        dataset_path = pub_root / row.dataset_path
        digest.update(row.dataset_filename.encode("utf-8", errors="surrogateescape"))
        digest.update(b"\0")
        component_root = dataset_path if dataset_path.is_dir() else dataset_path.parent
        for component in _dataset_component_paths(dataset_path):
            relative_path = component.relative_to(component_root).as_posix()
            digest.update(relative_path.encode("utf-8", errors="surrogateescape"))
            digest.update(b"\0")
            _hash_file(component, digest)
            digest.update(b"\0")
    return digest.hexdigest()


def _comparison_row(
    *,
    status: str,
    reason: str,
    match_method: str,
    old_info: ResourceInfo | None,
    new_info: ResourceInfo,
    old_rows: list[InventoryRow] | None,
    new_rows: list[InventoryRow],
    old_checksum: str = "",
    new_checksum: str = "",
) -> ComparisonRow:
    old_size = _total_size(old_rows)
    new_size = _total_size(new_rows)
    return ComparisonRow(
        status=status,
        archive_2026="yes" if status in {"new", "changed"} else "no",
        match_method=match_method,
        change_reason=reason,
        organization_2017=old_info.organization if old_info else "",
        resource_2017=old_info.resource if old_info else "",
        organization_2026=new_info.organization,
        resource_2026=new_info.resource,
        resource_guid_2017=old_info.resource_guid if old_info else "",
        resource_guid_2026=new_info.resource_guid,
        resource_ids_2017=old_info.resource_ids if old_info else "",
        resource_ids_2026=new_info.resource_ids,
        publisher_id_2017=old_info.publisher_id if old_info else "",
        publisher_id_2026=new_info.publisher_id,
        base_name_2017=old_info.base_name if old_info else "",
        base_name_2026=new_info.base_name,
        current_as_of_2017=old_info.current_as_of if old_info else "",
        current_as_of_2026=new_info.current_as_of,
        format_2017=_format_name(old_rows),
        format_2026=_format_name(new_rows),
        dataset_filenames_2017=_filenames(old_rows),
        dataset_filenames_2026=_filenames(new_rows),
        filesize_bytes_2017=old_size if old_info else "",
        filesize_bytes_2026=new_size,
        filesize_delta_bytes=new_size - old_size if old_info else "",
        content_sha256_2017=old_checksum,
        content_sha256_2026=new_checksum,
    )


def _removed_comparison_row(
    old_info: ResourceInfo, old_rows: list[InventoryRow]
) -> ComparisonRow:
    return ComparisonRow(
        status="removed",
        archive_2026="no",
        match_method="",
        change_reason="no matching 2026 resource with a stored dataset",
        organization_2017=old_info.organization,
        resource_2017=old_info.resource,
        organization_2026="",
        resource_2026="",
        resource_guid_2017=old_info.resource_guid,
        resource_guid_2026="",
        resource_ids_2017=old_info.resource_ids,
        resource_ids_2026="",
        publisher_id_2017=old_info.publisher_id,
        publisher_id_2026="",
        base_name_2017=old_info.base_name,
        base_name_2026="",
        current_as_of_2017=old_info.current_as_of,
        current_as_of_2026="",
        format_2017=_format_name(old_rows),
        format_2026="",
        dataset_filenames_2017=_filenames(old_rows),
        dataset_filenames_2026="",
        filesize_bytes_2017=_total_size(old_rows),
        filesize_bytes_2026="",
        filesize_delta_bytes="",
        content_sha256_2017="",
        content_sha256_2026="",
    )


def compare_archives(
    old_root: Path,
    new_root: Path,
    *,
    method: str = "checksum",
    progress: bool = False,
) -> list[ComparisonRow]:
    if method not in {"checksum", "size"}:
        raise ValueError("method must be 'checksum' or 'size'")

    old_pub = resolve_pub_root(old_root)
    new_pub = resolve_pub_root(new_root)
    old_resources = read_resources(old_pub)
    new_resources = read_resources(new_pub)
    old_inventory = _group_inventory_rows(
        select_preferred_format_rows(inventory_gdrs(old_pub)[0])
    )
    new_inventory = _group_inventory_rows(
        select_preferred_format_rows(inventory_gdrs(new_pub)[0])
    )

    old_values = list(old_resources.values())
    old_by_guid = _unique_index(old_values, "resource_guid")
    old_by_publisher_base = _unique_index(old_values, "publisher_base_key")
    old_by_path = _unique_index(old_values, "path_key")

    comparison_rows: list[ComparisonRow] = []
    matched_old_keys: set[tuple[str, str]] = set()
    checksum_candidates = 0
    for new_key, new_rows in sorted(new_inventory.items()):
        new_info = new_resources[new_key]
        old_info, match_method = match_old_resource(
            new_info, old_by_guid, old_by_publisher_base, old_by_path
        )
        if old_info is None:
            comparison_rows.append(
                _comparison_row(
                    status="new",
                    reason="no matching 2017 resource",
                    match_method="",
                    old_info=None,
                    new_info=new_info,
                    old_rows=None,
                    new_rows=new_rows,
                )
            )
            continue

        old_rows = old_inventory.get(old_info.path_key)
        if not old_rows:
            comparison_rows.append(
                _comparison_row(
                    status="new",
                    reason="matched resource has no stored 2017 dataset",
                    match_method=match_method,
                    old_info=old_info,
                    new_info=new_info,
                    old_rows=None,
                    new_rows=new_rows,
                )
            )
            continue

        matched_old_keys.add(old_info.path_key)

        old_quick = _quick_signature(old_rows)
        new_quick = _quick_signature(new_rows)
        if old_quick[0] != new_quick[0]:
            status, reason = "changed", "preferred format changed"
            old_checksum = new_checksum = ""
        elif tuple(name for name, _ in old_quick[1]) != tuple(
            name for name, _ in new_quick[1]
        ):
            status, reason = "changed", "dataset filenames changed"
            old_checksum = new_checksum = ""
        elif old_quick[1] != new_quick[1]:
            status, reason = "changed", "dataset filesizes changed"
            old_checksum = new_checksum = ""
        elif method == "size":
            status = "unchanged"
            reason = "format, filenames, and filesizes match; content not hashed"
            old_checksum = new_checksum = ""
        else:
            checksum_candidates += 1
            if progress and checksum_candidates % 25 == 0:
                print(
                    f"Checksum comparison {checksum_candidates}: "
                    f"{new_info.organization}/{new_info.resource}",
                    file=sys.stderr,
                    flush=True,
                )
            old_checksum = content_signature(old_pub, old_rows)
            new_checksum = content_signature(new_pub, new_rows)
            if old_checksum == new_checksum:
                status, reason = "unchanged", "content checksums match"
            else:
                status, reason = "changed", "content checksums differ"

        comparison_rows.append(
            _comparison_row(
                status=status,
                reason=reason,
                match_method=match_method,
                old_info=old_info,
                new_info=new_info,
                old_rows=old_rows,
                new_rows=new_rows,
                old_checksum=old_checksum,
                new_checksum=new_checksum,
            )
        )

    for old_key, old_rows in sorted(old_inventory.items()):
        if old_key not in matched_old_keys:
            comparison_rows.append(
                _removed_comparison_row(old_resources[old_key], old_rows)
            )

    return comparison_rows


def write_comparison(path: Path, rows: Iterable[ComparisonRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COMPARISON_FIELDS)
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)


def default_candidates_path(comparison_path: Path) -> Path:
    return comparison_path.with_name(f"{comparison_path.stem}_archive_candidates.csv")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("old", nargs="?", type=Path, default=DEFAULT_OLD_ROOT)
    parser.add_argument("new", nargs="?", type=Path, default=DEFAULT_NEW_ROOT)
    parser.add_argument("-o", "--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--candidates-output",
        type=Path,
        help="new/changed-only CSV (default: derived from --output)",
    )
    parser.add_argument(
        "--method",
        choices=("checksum", "size"),
        default="checksum",
        help="comparison method after resource matching (default: %(default)s)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        rows = compare_archives(args.old, args.new, method=args.method, progress=True)
        write_comparison(args.output, rows)
        candidates_path = args.candidates_output or default_candidates_path(args.output)
        candidates = [row for row in rows if row.archive_2026 == "yes"]
        write_comparison(candidates_path, candidates)
    except (
        FileNotFoundError,
        NotADirectoryError,
        PermissionError,
        ValueError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    counts = Counter(row.status for row in rows)
    print(f"Wrote {len(rows):,} comparisons to {args.output.resolve()}")
    print(
        f"Wrote {len(candidates):,} archive candidates to {candidates_path.resolve()}"
    )
    for status in ("new", "changed", "unchanged", "removed"):
        print(f"{status.title()}: {counts[status]:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
