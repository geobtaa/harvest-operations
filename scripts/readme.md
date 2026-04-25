The `scripts` folder contains standalone python scripts that are not integrated into the harvester modules.

`oai_download.py` downloads raw OAI-PMH XML into a local folder so parser development can happen offline.

`oai_list_sets.py` queries an OAI-PMH `ListSets` endpoint, filters the set list by one or more
keywords such as `atlas` or `plat book`, and can write a CSV of discovered set specs.

`harvest_task_dashboard.py` builds a due-date dashboard from `inputs/harvest-records.csv`
and `reference_data/websites.csv`, writes per-workflow CSVs under
`inputs/harvest-workflow-inputs/`, and is also wired into the FastAPI job UI.

`arcgis_landing_page_thumbnails.py` scans ArcGIS Hub landing pages from
`inputs/arcgisLandingPages.csv` and writes thumbnail URLs to
`outputs/arcgis_landing_page_thumbnails.csv`.
