# Local Scripts

This folder contains scripts that run on a **developer's local machine**, not in Databricks.

These are typically data preparation/ETL scripts that:
- Download data from external sources
- Process and transform data locally
- Upload results to S3 for Databricks to consume

## Scripts

### `nih_exporter_to_s3.py` (retired 2026-11)
Replaced by openalex-ingest's `nih_awards.py` (weekly on Heroku Scheduler: ExPORTER FY1985-2025 plus the RePORTER API from
FY2026, the same 48 columns), read by the Awards job's `nih_exporter` row in `notebooks/awards/sources.csv`. The script is in
git history; its last object, `s3://openalex-ingest/awards/nih/nih_projects_combined.parquet` (2026-10-06), stays where it is.

### `gtr_to_s3.py`
Downloads UK Research Council grant data from Gateway to Research (GtR) API, processes it, and uploads to S3.

**What it does:**
1. Downloads all projects from the GtR API (171K+ projects)
2. Parses XML responses to extract project metadata
3. Deduplicates by `grant_reference`
4. Converts to parquet format
5. Uploads to `s3://openalex-ingest/awards/gtr/`

**Output:** ~171K UK Research Council grants

**Requirements:**
```bash
pip install pandas pyarrow requests lxml
# AWS CLI must be configured with credentials for s3://openalex-ingest/
```

**Usage:**
```bash
cd scripts/local
python gtr_to_s3.py

# Resume interrupted download:
python gtr_to_s3.py --resume
```

### NWO
NWOpen is fetched weekly by openalex-ingest `nwo_awards.py` into `s3://openalex-ingest/awards/nwopen/`; the Awards job reads the newest object. The laptop script `nwo_to_s3.py` is retired.

## After Running Local Scripts

After data is uploaded to S3, run the corresponding Databricks SQL/notebooks:
- For NIH: Run `notebooks/awards/CreateNIHAwards.ipynb` in Databricks
- For GTR: Run `notebooks/awards/CreateGTRProjectAwards.ipynb` in Databricks
