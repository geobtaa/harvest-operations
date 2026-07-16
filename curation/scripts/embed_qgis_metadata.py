#!/usr/bin/env python3
"""CLI wrapper for embedding QGIS-style metadata into GeoPackages."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from curation.embed_qgis_metadata import main


if __name__ == "__main__":
    raise SystemExit(main())
