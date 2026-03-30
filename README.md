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
