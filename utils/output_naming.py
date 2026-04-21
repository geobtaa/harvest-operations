from pathlib import Path


def infer_upload_source_prefix(primary_output_csv: str) -> str:
    """
    Infer the source prefix used by build_uploads.py from a configured primary
    output filename, e.g. outputs/ogmWisc_primary.csv -> ogmWisc.
    """
    stem = Path(primary_output_csv).stem
    if not stem.endswith("_primary"):
        raise ValueError(
            "output_primary_csv must end with '_primary.csv' to support build_uploads."
        )
    return stem[: -len("_primary")]
