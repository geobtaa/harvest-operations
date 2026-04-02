# Metadata Harvester API Toolkit

This repository contains an API-driven metadata harvesting toolkit.

- Built on **FastAPI** for orchestration  
- Implements a modular **harvesting architecture** 
- Centralizes the metadata schema and distribution keys in YAML files 
- Provides a lightweight **admin interface** for running jobs via a web browser  

---

## Directory Overview


| Folder/File | Description |
| ----- | ----- |
| `main.py` | Entry point for running harvesting routines manually or via scripts |
|  |  |
| `harvesters/` | Contains source-specific harvester modules, each subclassing the base harvester class |
| `harvesters/base.py` | Defines the `BaseHarvester` class with the standard pipeline: fetch → parse → flatten |
| `utils/` | Shared utility functions used across harvesters (e.g., title formatting, spatial/temporal cleaning) |
| `routers/` | FastAPI endpoints for running harvesters via HTTP routes or background jobs |
| `schemas/` | YAML metadata schemas used for field validation and formatting |
| `reference_data/` | External controlled vocabularies, lookup tables, or enrichment data (e.g., spatial or organization info) |
| `inputs/` | Source-specific configuration or input files, such as CSVs or cached HTML pages |
| `outputs/` | Processed metadata outputs, typically saved as CSV or JSON |
| `config/` | Optional config files for customizing runtime parameters or deployment settings |
| `static/` | Static HTML pages or assets for lightweight documentation or interface testing |
| `pyproject.toml` | Project metadata and dependency definitions (managed with `uv`) |
| `uv.lock` | Locked dependency versions for reproducible installs |
| `requirements.txt` | Legacy dependency list (use `pyproject.toml` going forward) |


## Setup instructions

1. Clone the repository and change into this directory
2. Create the local environment and install dependencies: `uv sync`
3. Start the FastAPI Server: `uv run uvicorn main:app --reload`
4. Review the API documentation (Swagger UI) at http://localhost:8000/docs
5. For a list of runnable jobs, go to http://localhost:8000/


**Notes:**

* The --reload flag automatically restarts the server when you edit code.
* Jobs are configured in YAML files inside the jobs/ directory.
* Outputs from harvests will be saved in the outputs/ folder.


## Adding jobs

To create new harvesters, here are the basic steps:

1. Add a new Python file in the `harvesters/` directory
2. Create a job config YAML in `config/`
3. In `routers/jobs.py`, update the run endpoint for the new harvester type
4. Test the new harvester

*More details tbd*

Current generic source types include `arcgis`, `ckan`, and `socrata`.

## Publishing Dashboard Reports On GitHub Pages

This repository now includes a GitHub Pages workflow that publishes the generated dashboard HTML files in `reports/` as a simple static site.

How it works:

1. Generate the dashboard reports locally so new dated `reports/*.html` files exist.
2. Commit and push those report files to `main`.
3. GitHub Actions runs `.github/workflows/publish-dashboard-pages.yml`.
4. The workflow builds a small site with:
   - `latest/` for the newest full dashboard
   - `latest/due/` for the newest due-only report
   - `latest/retrospective/` for the newest retrospective report
   - `latest/workflows/py-arcgis-hub/` for the newest ArcGIS Hubs view
   - dated archive pages for each published report date

Setup in GitHub:

1. Open the repository Settings page in GitHub.
2. Go to Pages.
3. Set the source to GitHub Actions.
4. Push report updates to `main` whenever you want to refresh the published site.

You can also build the site locally with:

```bash
uv run python scripts/build_dashboard_pages_site.py --reports-dir reports --output-dir site
```

That command writes a deployable static site into `site/`.
