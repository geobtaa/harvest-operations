#!/usr/bin/env python3
"""CLI wrapper for the staged ArcGIS curation pipeline."""

from __future__ import annotations

import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CURATION_SRC = REPO_ROOT / "curation" / "src"
sys.path.insert(0, str(CURATION_SRC))
sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

from curation.arcgis_curation_pipeline import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
