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

## After Running Local Scripts

After data is uploaded to S3, run the corresponding Databricks SQL/notebooks:
- For NIH: Run `notebooks/awards/CreateNIHAwards.ipynb` in Databricks
