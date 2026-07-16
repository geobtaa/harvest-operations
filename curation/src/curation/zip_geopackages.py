"""Create one ZIP archive per GeoPackage."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


LOGGER = logging.getLogger(__name__)


@dataclass
class ZipSummary:
    """Summary of a GeoPackage zipping run."""

    created_archives: list[Path] = field(default_factory=list)
    skipped_archives: list[Path] = field(default_factory=list)


def iter_geopackages(input_dir: Path, *, recursive: bool) -> list[Path]:
    pattern = "**/*.gpkg" if recursive else "*.gpkg"
    return sorted(path for path in input_dir.glob(pattern) if path.is_file())


def build_archive_path(gpkg_path: Path, input_dir: Path, output_dir: Path) -> Path:
    relative_parent = gpkg_path.parent.relative_to(input_dir)
    archive_dir = output_dir / relative_parent
    archive_name = f"{gpkg_path.name}.zip"
    return archive_dir / archive_name


def zip_one_geopackage(
    gpkg_path: Path,
    input_dir: Path,
    output_dir: Path,
    *,
    overwrite: bool,
    delete_original: bool,
) -> Path | None:
    archive_path = build_archive_path(gpkg_path, input_dir, output_dir)
    archive_path.parent.mkdir(parents=True, exist_ok=True)

    if archive_path.exists() and not overwrite:
        LOGGER.info("Skipping existing archive %s", archive_path)
        return None

    with ZipFile(archive_path, mode="w", compression=ZIP_DEFLATED) as archive:
        archive.write(gpkg_path, arcname=gpkg_path.name)

    if delete_original:
        gpkg_path.unlink()

    LOGGER.info("Created %s", archive_path)
    return archive_path


def zip_geopackages(
    input_dir: Path,
    *,
    output_dir: Path | None = None,
    recursive: bool = False,
    overwrite: bool = False,
    delete_original: bool = False,
) -> ZipSummary:
    if not input_dir.is_dir():
        raise NotADirectoryError(input_dir)

    resolved_output_dir = output_dir or input_dir
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    summary = ZipSummary()
    for gpkg_path in iter_geopackages(input_dir, recursive=recursive):
        archive_path = build_archive_path(gpkg_path, input_dir, resolved_output_dir)
        result = zip_one_geopackage(
            gpkg_path,
            input_dir,
            resolved_output_dir,
            overwrite=overwrite,
            delete_original=delete_original,
        )
        if result is None:
            summary.skipped_archives.append(archive_path)
        else:
            summary.created_archives.append(result)

    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create one ZIP archive per GeoPackage in a directory."
    )
    parser.add_argument("input_dir", type=Path, help="Directory containing .gpkg files")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory for the .gpkg.zip files. Defaults to the input directory.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Include GeoPackages in subdirectories.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing .gpkg.zip files instead of skipping them.",
    )
    parser.add_argument(
        "--delete-originals",
        action="store_true",
        help="Delete each .gpkg after its archive is created.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    summary = zip_geopackages(
        args.input_dir,
        output_dir=args.output_dir,
        recursive=args.recursive,
        overwrite=args.overwrite,
        delete_original=args.delete_originals,
    )
    LOGGER.info(
        "Created %s archive(s), skipped %s",
        len(summary.created_archives),
        len(summary.skipped_archives),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
