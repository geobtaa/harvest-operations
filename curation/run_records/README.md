# Curation run records

This tracked directory contains portable, immutable records of completed
curation runs. Each snapshot is stored at:

```text
<job-id>/<UTC-run-id>/
├── job.yaml
├── manifest.json
└── metadata.csv
```

The manifest contains repository- or work-relative paths plus sizes and
SHA-256 checksums for generated artifacts. GeoPackages, FlatGeoBuf, PMTiles,
thumbnails, dictionaries, and reports remain under ignored `curation/work/`
and are not copied here.

Saved manifests are also an ID registry. If a local work manifest is absent,
the ArcGIS curation pipeline searches saved run records by source ID before
generating a new curated NanoID.
