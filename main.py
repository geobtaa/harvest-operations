import asyncio
import csv
import os
import random
import tempfile

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
import yaml

from harvesters.oai_qdc import OaiQdcHarvester
from routers import jobs as jobs_router
from routers import schema as schema_router

# Initialize app
app = FastAPI()

OAI_QDC_UI_JOB_IDS = ("iowa-library", "university-washington")


def resolve_config_path(path_value: str, config_path: str | None = None) -> str:
    candidate = os.path.expanduser(path_value)
    if os.path.isabs(candidate):
        return candidate

    project_candidate = os.path.abspath(candidate)
    if os.path.exists(project_candidate):
        return project_candidate

    if config_path:
        config_candidate = os.path.abspath(
            os.path.join(os.path.dirname(config_path), candidate)
        )
        if os.path.exists(config_candidate):
            return config_candidate

    return project_candidate


def load_yaml_config(config_path: str) -> dict:
    with open(config_path, encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def create_arcgis_test_input_csv(source_csv: str, sample_size: int = 3) -> tuple[str, int]:
    """
    Create a temporary ArcGIS workflow input CSV containing a random subset of rows.
    Returns the temp file path and the number of sampled rows written.
    """
    with open(source_csv, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    if not fieldnames:
        raise ValueError(f"ArcGIS input CSV is missing headers: {source_csv}")
    if not rows:
        raise ValueError(f"ArcGIS input CSV has no rows to sample: {source_csv}")

    selected_rows = random.sample(rows, k=min(sample_size, len(rows)))
    temp_handle = tempfile.NamedTemporaryFile(
        mode="w",
        newline="",
        encoding="utf-8",
        suffix=".csv",
        delete=False,
    )
    try:
        writer = csv.DictWriter(temp_handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(selected_rows)
    finally:
        temp_handle.close()

    return temp_handle.name, len(selected_rows)


def load_sets_from_csv(
    csv_path: str,
    set_column: str = "set",
    title_column: str = "title",
) -> list[dict]:
    sets: list[dict] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            set_spec = str(row.get(set_column, "")).strip()
            set_title = str(row.get(title_column, "")).strip()
            if not set_spec:
                continue
            sets.append({"set_spec": set_spec, "set_title": set_title})
    return sets


def build_oai_qdc_ui_sources() -> list[dict]:
    sources: list[dict] = []

    for job_id in OAI_QDC_UI_JOB_IDS:
        config_path = os.path.join("config", f"{job_id}.yaml")
        config = load_yaml_config(config_path)
        sets_csv = resolve_config_path(config["sets_csv"], config_path)
        set_column = config.get("sets_csv_set_column", "set")
        title_column = config.get("sets_csv_title_column", "title")
        sets = load_sets_from_csv(
            csv_path=sets_csv,
            set_column=set_column,
            title_column=title_column,
        )

        sources.append(
            {
                "job_id": job_id,
                "label": config.get("source_name") or config.get("name") or job_id,
                "base_url": config.get("oai_base_url", ""),
                "metadata_prefix": config.get("metadata_prefix", "oai_qdc"),
                "download_dir": config.get("oai_download_dir", ""),
                "sets": sets,
            }
        )

    return sources

# Register routers
app.include_router(schema_router.router)
app.include_router(jobs_router.router)

# Mount static files at /static
app.mount("/static", StaticFiles(directory="static"), name="static")

# Serve index.html manually at root
@app.get("/", response_class=FileResponse)
async def root():
    return FileResponse(os.path.join("static", "index.html"))


@app.get("/oai-qdc-sources")
async def oai_qdc_sources():
    return {"sources": build_oai_qdc_ui_sources()}

# Manual trigger for ArcGIS harvester
@app.post("/run-arcgis")
async def run_arcgis_harvester(test_run: bool = Query(default=False)):
    from harvesters.arcgis import ArcGISHarvester

    config_path = "config/arcgis.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    temp_input_path = None
    test_run_message = ""
    upload_message = ""
    if test_run:
        temp_input_path, selected_count = create_arcgis_test_input_csv(config["input_csv"])
        config = {**config, "input_csv": temp_input_path}
        test_run_message = f"<p>Test run used {selected_count} randomly selected hubs.</p>"

    try:
        harvester = ArcGISHarvester(config)
        harvester.load_reference_data()

        records = harvester.fetch()
        parsed = harvester.parse(records)
        flat = harvester.flatten(parsed)
        df = harvester.build_dataframe(flat)
        df = harvester.derive_fields(df)
        df = harvester.add_defaults(df)
        df = harvester.add_provenance(df)
        df = harvester.clean(df)
        harvester.validate(df)
        results = harvester.write_outputs(df)
        upload_summary = harvester.build_uploads(results)
        if upload_summary is not None:
            results["upload_summary"] = upload_summary
            if upload_summary.get("status") == "created":
                upload_message = (
                    "<p>Built upload files: "
                    f"{upload_summary['primary_upload_csv']}, "
                    f"{upload_summary['distributions_new_csv']}, "
                    f"{upload_summary['distributions_delete_csv']}.</p>"
                )
            else:
                upload_message = (
                    "<p>Upload files not built: "
                    f"{upload_summary.get('reason', 'No reason provided.')}.</p>"
                )
    finally:
        if temp_input_path and os.path.exists(temp_input_path):
            os.unlink(temp_input_path)

    return HTMLResponse(content=f"""
        <html>
          <head><title>Harvester Run Complete</title></head>
          <body>
            <h2>Harvester completed!</h2>
            {test_run_message}
            {upload_message}
            <p>Check the output folder for results.</p>
            <p><a href="/static/arcgis.html">Back</a></p>
          </body>
        </html>
    """, status_code=200)

@app.get("/run-arcgis-stream")
async def run_arcgis_stream(test_run: bool = Query(default=False)):
    from harvesters.arcgis import ArcGISHarvester
    import yaml

    async def event_stream():
        config_path = "config/arcgis.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        temp_input_path = None
        if test_run:
            temp_input_path, selected_count = create_arcgis_test_input_csv(config["input_csv"])
            config = {**config, "input_csv": temp_input_path}
            yield (
                f"data: Test run enabled. Using {selected_count} randomly selected hubs "
                f"from py-arcgis-hub.csv.\n\n"
            )

        try:
            harvester = ArcGISHarvester(config)
            harvester.load_reference_data()

            fetched_records = []
            for item in harvester.fetch():
                if isinstance(item, str):
                    # Just yield the message — it was already formatted in arcgis.py
                    yield f"data: {item}\n\n"
                else:
                    fetched_records.append(item)

                await asyncio.sleep(0.1)  # <— allow the event loop to yield control


            # Proceed with the remaining steps
            yield f"data: Finished fetching {len(fetched_records)} records. Now parsing...\n\n"
            parsed = harvester.parse(fetched_records)
            flat = harvester.flatten(parsed)
            df = harvester.build_dataframe(flat)
            df = harvester.derive_fields(df)
            df = harvester.add_defaults(df)
            df = harvester.add_provenance(df)
            df = harvester.clean(df)
            harvester.validate(df)
            results = harvester.write_outputs(df)
            upload_summary = harvester.build_uploads(results)
            if upload_summary is not None:
                results["upload_summary"] = upload_summary
                if upload_summary.get("status") == "created":
                    yield (
                        "data: Built upload files: "
                        f"{upload_summary['primary_upload_csv']}, "
                        f"{upload_summary['distributions_new_csv']}, "
                        f"{upload_summary['distributions_delete_csv']}.\n\n"
                    )
                else:
                    yield (
                        "data: Upload files not built: "
                        f"{upload_summary.get('reason', 'No reason provided.')}.\n\n"
                    )

            yield f"data: Harvester complete! Check the output folder.\n\n"
            yield "data: DONE\n\n"
        finally:
            if temp_input_path and os.path.exists(temp_input_path):
                os.unlink(temp_input_path)

    return StreamingResponse(event_stream(), media_type="text/event-stream")\
    
@app.get("/run-socrata-stream")
async def run_socrata_stream():
    from harvesters.socrata import SocrataHarvester
    import yaml

    async def event_stream():
        config_path = "config/socrata.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        harvester = SocrataHarvester(config)
        harvester.load_reference_data()

        fetched_records = []
        for item in harvester.fetch():
            if isinstance(item, str):
                # Just yield the message — it was already formatted in arcgis.py
                yield f"data: {item}\n\n"
            else:
                fetched_records.append(item)

            await asyncio.sleep(0.1)  # <— allow the event loop to yield control


        # Proceed with the remaining steps
        yield f"data: Finished fetching {len(fetched_records)} records. Now parsing...\n\n"
        parsed = harvester.parse(fetched_records)
        flat = harvester.flatten(parsed)
        df = harvester.build_dataframe(flat)
        df = harvester.derive_fields(df)
        df = harvester.add_defaults(df)
        df = harvester.add_provenance(df)
        df = harvester.clean(df)
        harvester.validate(df)
        harvester.write_outputs(df)

        yield f"data: Harvester complete! Check the output folder.\n\n"
        yield "data: DONE\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")

@app.get("/run-pasda-stream")
async def run_pasda_stream():
    from harvesters.pasda import PasdaHarvester

    async def event_stream():
        config_path = "config/pasda.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        harvester = PasdaHarvester(config)
        harvester.load_reference_data()

        yield "data: Starting PASDA harvest...\n\n"
        raw_html = harvester.fetch()
        yield "data: Fetched HTML, now parsing...\n\n"

        parsed = harvester.parse(raw_html)
        flat = harvester.flatten(parsed)
        df = harvester.build_dataframe(flat)
        df = harvester.derive_fields(df)
        df = harvester.add_defaults(df)
        df = harvester.add_provenance(df)
        df = harvester.clean(df)
        harvester.validate(df)
        harvester.write_outputs(df)

        yield "data: PASDA harvest complete. Check output folder.\n\n"
        yield "data: DONE\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/run-hdx-stream")
async def run_hsx_stream():
    from harvesters.hdx import HdxHarvester

    async def event_stream():
        config_path = "config/hdx.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        harvester = HdxHarvester(config)
        harvester.load_reference_data()

        yield "data: Starting HDX harvest...\n\n"
        raw_html = harvester.fetch()
        yield "data: Fetched JSON, now parsing...\n\n"

        parsed = harvester.parse(raw_html)
        flat = harvester.flatten(parsed)
        df = harvester.build_dataframe(flat)
        df = harvester.derive_fields(df)
        df = harvester.add_defaults(df)
        df = harvester.add_provenance(df)
        df = harvester.clean(df)
        harvester.validate(df)
        harvester.write_outputs(df)

        yield "data: HDX harvest complete. Check output folder.\n\n"
        yield "data: DONE\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")

@app.get("/run-isgs-stream")
async def run_isgs_stream():
    from harvesters.isgs import IsgsHarvester
    import yaml

    async def event_stream():
        config_path = "config/isgs.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        harvester = IsgsHarvester(config)
        harvester.load_reference_data()

        fetched_records = []
        # This loop works because fetch() is a generator
        for item in harvester.fetch():
            # The ISGS harvester only yields data tuples, not status strings,
            # but we keep the structure for consistency.
            if isinstance(item, str):
                yield f"data: {item}\n\n"
            else:
                fetched_records.append(item)

            # A small sleep helps keep the stream responsive
            await asyncio.sleep(0.01)


        # Proceed with the remaining steps, which now work correctly
        yield f"data: Finished fetching {len(fetched_records)} records. Now parsing...\n\n"
        
        # The parse method now correctly accepts the full list
        parsed = harvester.parse(fetched_records)
        
        flat = harvester.flatten(parsed)
        df = harvester.build_dataframe(flat)
        df = harvester.derive_fields(df)
        df = harvester.add_defaults(df)
        df = harvester.add_provenance(df)
        df = harvester.clean(df)
        harvester.validate(df)
        harvester.write_outputs(df)

        yield f"data: Harvester complete! Check the output folder.\n\n"
        yield "data: DONE\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/run-oai-qdc-stream")
async def run_oai_qdc_stream(
    source: str = Query(..., description="OAI QDC job id."),
    set_spec: str = Query(..., description="OAI set spec to harvest, or '__all__'."),
):
    source_map = {item["job_id"]: item for item in build_oai_qdc_ui_sources()}
    selected_source = source_map.get(source)
    if not selected_source:
        raise HTTPException(status_code=404, detail=f"Unknown OAI QDC source '{source}'.")

    run_all_sets = set_spec == "__all__"
    selected_set = None if run_all_sets else next(
        (item for item in selected_source["sets"] if item["set_spec"] == set_spec),
        None,
    )
    if not run_all_sets and not selected_set:
        raise HTTPException(
            status_code=404,
            detail=f"Set '{set_spec}' was not found for source '{source}'.",
        )

    async def event_stream():
        temp_sets_path = None

        try:
            config_path = os.path.join("config", f"{source}.yaml")
            config = load_yaml_config(config_path)
            config = dict(config)

            if not run_all_sets:
                set_column = config.get("sets_csv_set_column", "set")
                title_column = config.get("sets_csv_title_column", "title")

                fd, temp_sets_path = tempfile.mkstemp(
                    prefix=f"{source}-",
                    suffix="-sets.csv",
                )
                os.close(fd)

                with open(temp_sets_path, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=[set_column, title_column])
                    writer.writeheader()
                    writer.writerow(
                        {
                            set_column: selected_set["set_spec"],
                            title_column: selected_set["set_title"],
                        }
                    )

                config["sets_csv"] = temp_sets_path

            harvester = OaiQdcHarvester(config)
            harvester.load_reference_data()

            if run_all_sets:
                yield (
                    f"data: Running OAI QDC harvest for {selected_source['label']} "
                    f"across all configured sets ({len(selected_source['sets'])}).\n\n"
                )
            else:
                set_label = selected_set["set_title"] or selected_set["set_spec"]
                yield f"data: Running OAI QDC harvest for {selected_source['label']} -> {set_label}.\n\n"
            yield (
                "data: This job reads previously downloaded local XML. "
                "If files are missing, run scripts/oai_download.py first.\n\n"
            )
            await asyncio.sleep(0.01)

            raw = harvester.fetch()
            yield f"data: Loaded {len(raw)} local XML page(s). Now parsing...\n\n"
            await asyncio.sleep(0.01)

            parsed = harvester.parse(raw)
            flat = harvester.flatten(parsed)
            yield f"data: Prepared {len(flat)} record(s). Now building outputs...\n\n"
            await asyncio.sleep(0.01)

            df = harvester.build_dataframe(flat)
            df = harvester.derive_fields(df)
            df = harvester.add_defaults(df)
            df = harvester.add_provenance(df)
            df = harvester.clean(df)
            harvester.validate(df)
            results = harvester.write_outputs(df)

            yield f"data: Wrote primary CSV to {results['primary_csv']}\n\n"
            if "distributions_csv" in results:
                yield f"data: Wrote distributions CSV to {results['distributions_csv']}\n\n"
            yield "data: OAI QDC harvest complete.\n\n"
            yield "data: DONE\n\n"
        except Exception as exc:
            yield f"data: ERROR: {exc}\n\n"
            yield "data: DONE\n\n"
        finally:
            if temp_sets_path and os.path.exists(temp_sets_path):
                os.remove(temp_sets_path)

    return StreamingResponse(event_stream(), media_type="text/event-stream")
