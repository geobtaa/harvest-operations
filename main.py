from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from routers import schema as schema_router
from routers import jobs as jobs_router
import shutil
import os
import yaml
import asyncio
from fastapi.responses import StreamingResponse
import time
from harvesters.arcgis import ArcGISHarvester


import os

# Initialize app
app = FastAPI()

# Register routers
app.include_router(schema_router.router)
app.include_router(jobs_router.router)

# Mount static files at /static
app.mount("/static", StaticFiles(directory="static"), name="static")

# Serve index.html manually at root
@app.get("/", response_class=FileResponse)
async def root():
    return FileResponse(os.path.join("static", "index.html"))

# CSV file upload endpoint
@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if file.filename.endswith(".csv"):
        save_path = os.path.join("inputs", "arcHubs.csv")
        with open(save_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return RedirectResponse(url="/static/arcgis.html?upload=success", status_code=303)
    return {"error": "Only CSV files are allowed."}

# Manual trigger for ArcGIS harvester
@app.post("/run-arcgis")
async def run_arcgis_harvester():
    from harvesters.arcgis import ArcGISHarvester

    config_path = "config/arcgis.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    harvester = ArcGISHarvester(config)
    harvester.load_schema()

    records = harvester.fetch()
    parsed = harvester.parse(records)
    flat = harvester.flatten(parsed)
    df = harvester.build_dataframe(flat)
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)
    df = harvester.clean(df)
    harvester.validate(df)
    harvester.write_outputs(df)

    return HTMLResponse(content="""
        <html>
          <head><title>Harvester Run Complete</title></head>
          <body>
            <h2>Harvester completed!</h2>
            <p>Check the output folder for results.</p>
            <p><a href="/static/arcgis.html">Back</a></p>
          </body>
        </html>
    """, status_code=200)

@app.get("/run-arcgis-stream")
async def run_arcgis_stream():
    from harvesters.arcgis import ArcGISHarvester
    import yaml

    async def event_stream():
        config_path = "config/arcgis.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

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
        harvester.write_outputs(df)

        yield f"data: Harvester complete! Check the output folder.\n\n"
        yield "data: DONE\n\n"

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


