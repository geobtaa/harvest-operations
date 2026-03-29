# harvest-operations
Tracking and planning harvests for the BTAA Geoportal

## Python setup

This repo uses [uv](https://docs.astral.sh/uv/) for Python environment management instead of Anaconda.

### Initial setup

```bash
uv sync
```

This will create a local `.venv` using Python `3.12`.

### Running scripts

```bash
uv run triage/triage_harvest_records.py
uv run codework/compare_codes.py
```

You can also pass arguments through `uv run`, for example:

```bash
uv run triage/triage_harvest_records.py --today 2026-03-29
```
