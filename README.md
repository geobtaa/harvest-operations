# harvest-operations

`harvest-operations` is a Python project for running metadata harvests, related data-processing jobs, and supporting reports for GeoBTAA workflows.

At the center of the repository is a small FastAPI application that serves browser-based admin pages for launching harvesters and reviewing a few supporting tools. Around that app is a larger collection of reusable harvester modules plus many one-off scripts for source-specific maintenance, downloads, comparisons, transformations, and report generation.

## Project Structure

### Main code structure

- `main.py` starts the FastAPI application, mounts the static admin pages, and exposes browser-triggered harvester endpoints.
- `routers/` contains FastAPI router modules for job execution and schema-related endpoints.
- `harvesters/` contains the source-specific harvester classes. Most harvesters inherit from `harvesters/base.py`, which defines the shared pipeline for fetching, parsing, flattening, building dataframes, cleaning, validating, and writing outputs.
- `utils/` contains shared Python helpers used by harvesters and scripts.

The repository is therefore not just a web app and not just a script collection. It is primarily:

1. A Python/FastAPI admin surface for running harvest jobs in the browser.
2. A library of reusable harvester classes and utilities.
3. A working repository of standalone scripts for one-off or periodic operational tasks.

### Directory guide

| Path | Purpose |
| --- | --- |
| `main.py` | FastAPI entry point. Serves the admin UI, mounts `static/`, and exposes streaming or job-based run endpoints. |
| `harvesters/` | Source-specific harvester implementations such as ArcGIS, CKAN, Socrata, PASDA, HDX, OAI-QDC, and others. |
| `harvesters/base.py` | Shared base class for the common harvest pipeline. |
| `routers/` | FastAPI routers, including job execution endpoints such as `/jobs/{job_id}/run`. |
| `static/` | HTML pages used as the browser admin interface for launching harvesters and related tools. |
| `config/` | YAML job definitions. These connect a job id to a harvester type plus the input and output file paths it should use. |
| `schemas/` | Metadata schema files, schema maps, controlled value files, and other schema-support assets. |
| `utils/` | Reusable helpers for field derivation, cleaning, validation, file IO, spatial matching, and output writing. |
| `scripts/` | Standalone Python scripts for one-off jobs and operational tasks that are not all wired into FastAPI. This includes download helpers, comparison tools, report builders, and data transforms. |
| `inputs/` | Local working inputs used by harvesters and scripts, usually CSVs or other source files supplied before a run. |
| `reference_data/` | Lookup tables and enrichment data used during normalization and matching. |
| `reports/` | Generated dashboard reports and related HTML/CSV report outputs. |
| `tests/` | Pytest coverage for harvesters, scripts, report builders, and static admin pages. |
| `requirements.txt` | Pip-installable dependency list for setting up a local Python environment. |
| `pyproject.toml` and `uv.lock` | Project metadata and `uv`-based dependency management files used by the local launcher workflow. |
| `start-fastapi.command` | Convenience launcher that starts the FastAPI app locally and sources `.secrets.local` if present. |

## How Harvesters Run

There are two common ways jobs are run in this repository.

### Browser-driven harvesters

The FastAPI app serves a lightweight admin UI from `/` using the files in `static/`. From that landing page, a user can open a source page such as the ArcGIS or Socrata harvester page and start a run in the browser.

Depending on the page, the browser either:

- calls a dedicated streaming endpoint such as `/run-arcgis-stream`, or
- calls a more generic job endpoint such as `/jobs/{job_id}/run`.

In either case, the Python code reads the corresponding YAML in `config/`, creates the correct harvester class, runs the harvest pipeline, and writes outputs to the configured destination files.

For example, the ArcGIS browser workflow is:

1. Start the FastAPI app locally.
2. Open `http://localhost:8000/`.
3. Open the ArcGIS harvester page.
4. Upload the ArcGIS hubs CSV input if needed.
5. Click the run button in the browser.
6. Watch progress stream back to the page while the server runs the harvester.

### One-off scripts

Not every task belongs in the browser UI. The `scripts/` directory contains many standalone scripts for tasks such as:

- downloading source data for local processing,
- generating dashboard or upload files,
- comparing outputs,
- cleaning or transforming data,
- supporting one-time operational work.

Some scripts are recurring operational tools, and some are ad hoc utilities kept in the repository because they are useful for ongoing metadata work. The README in [`scripts/readme.md`](scripts/readme.md) documents several of the main ones.

## Local Setup

### Requirements

- Python `3.12` is the expected local version in this repository (`.python-version`).
- `pip` is needed to install `requirements.txt`.
- `uv` is also recommended because `start-fastapi.command` uses `uv run` to launch the FastAPI server.

### Set up a local environment

Create and activate a virtual environment, then install the Python dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If `uv` is not already installed on your machine, install it as well so the launcher script works:

```bash
pip install uv
```

### Start the FastAPI app

The simplest way to start the project locally is:

```bash
./start-fastapi.command
```

That script:

- changes into the repository root,
- sources `.secrets.local` if the file exists,
- runs `uv run uvicorn main:app --reload`.

After startup, the main local URLs are:

- `http://localhost:8000/` for the browser admin pages
- `http://localhost:8000/docs` for FastAPI's autogenerated API docs

If you prefer to start the app yourself, run:

```bash
uv run uvicorn main:app --reload
```

## Configuration and Inputs

- Job definitions live in `config/*.yaml`.
- Those YAML files point to the harvester type plus the input and output paths for that job.
- Many harvesters expect local files in `inputs/` before they are run.
- Output locations vary by job and are usually defined directly in the job YAML, commonly under `outputs/` or `reports/`.

## Local Secrets

Some local workflows use environment variables, including the harvest task dashboard's optional GitHub issue lookup.

To provide local secrets:

1. Create `.secrets.local` in the repository root.
2. Add exports such as:

```zsh
export GEOBTAA_PROJECTS_TOKEN='YOUR_TOKEN_HERE'
```

3. Start the app with `./start-fastapi.command`.

Because `start-fastapi.command` sources `.secrets.local` before launching FastAPI, changes to that file require a full server restart.

## Development Notes

- Run tests with `pytest`.
- Add new harvesters under `harvesters/` and define their job configuration in `config/`.
- If a task does not fit the reusable harvester model, it may belong in `scripts/` instead.

## Publishing Dashboard Reports On GitHub Pages

This repository includes a GitHub Pages workflow that publishes generated dashboard HTML files from `reports/` as a static site.

How it works:

1. Generate dashboard reports locally so new dated `reports/*.html` files exist.
2. Commit and push those report files to `main`.
3. GitHub Actions runs `.github/workflows/publish-dashboard-pages.yml`.
4. The workflow publishes the latest views plus dated archives.

You can also build the site locally with:

```bash
uv run python scripts/build_dashboard_pages_site.py --reports-dir reports --output-dir site
```

That command writes a deployable static site into `site/`.
