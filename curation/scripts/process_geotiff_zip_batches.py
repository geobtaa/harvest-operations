"""CLI wrapper for processing zipped GeoTIFF batches."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from curation.process_geotiff_zip_batches import main


if __name__ == "__main__":
    raise SystemExit(main())
