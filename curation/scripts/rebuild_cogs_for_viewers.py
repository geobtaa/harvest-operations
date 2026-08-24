"""CLI wrapper for rebuilding viewer-compatible COGs."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from curation.rebuild_cogs_for_viewers import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
