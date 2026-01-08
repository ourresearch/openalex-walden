# Local Scripts

This folder contains scripts that run on a **developer's local machine**, not in Databricks.

These are typically data preparation/ETL scripts that:
- Download data from external sources
- Process and transform data locally
- Upload results to S3 for Databricks to consume

## Scripts

### `nih_exporter_to_s3.py`
Downloads NIH grant data from NIH ExPORTER, processes it, and uploads to S3.

**What it does:**
1. Downloads NIH ExPORTER project files (FY1985-2024) from reporter.nih.gov
2. Extracts and combines all CSVs
3. Deduplicates by `full_project_num` (keeping most recent fiscal year)
4. Converts to parquet format
5. Uploads to `s3://openalex-ingest/awards/nih/`

**Output:** ~2.28M unique NIH awards

**Requirements:**
```bash
pip install pandas pyarrow requests
# AWS CLI must be configured with credentials for s3://openalex-ingest/
```

**Usage:**
```bash
cd scripts/local
python nih_exporter_to_s3.py
```

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

### `nwo_to_s3.py`
Downloads Dutch research grant data from the NWOpen API (NWO - Dutch Research Council), processes it, and uploads to S3.

**What it does:**
1. Downloads all projects from the NWOpen API (paginated JSON)
2. Extracts project metadata including title, abstract, amount, dates, PI, organization
3. Captures Products and summary_updates as JSON strings for later parsing
4. Deduplicates by `project_id`
5. Converts to parquet format
6. Uploads to `s3://openalex-ingest/awards/nwo/`

**Requirements:**
```bash
pip install pandas pyarrow requests
# AWS CLI must be configured with credentials for s3://openalex-ingest/
```

**Usage:**
```bash
cd scripts/local
python nwo_to_s3.py

# Resume interrupted download:
python nwo_to_s3.py --resume

# Test with limited pages:
python nwo_to_s3.py --max-pages 5 --skip-upload
```

## After Running Local Scripts

After data is uploaded to S3, run the corresponding Databricks SQL/notebooks:
- For NIH: Run `notebooks/awards/CreateNIHAwards.ipynb` in Databricks
- For GTR: Run `notebooks/awards/CreateGTRProjectAwards.ipynb` in Databricks
- For NWO: Run `notebooks/awards/CreateNWOAwards.ipynb` in Databricks
