# Databricks notebook source
# MAGIC %md
# MAGIC # Export Unsub institution-metrics sources
# MAGIC
# MAGIC Unsub (jump-api) counts each customer institution's authorships and citations per journal from three
# MAGIC work-level datasets, keyed on OpenAlex institution IDs with lineage (oxjob #1717). This notebook
# MAGIC writes them as Parquet; jump-api's `load_institution_metrics_sources.py` COPYs them into Unsub's
# MAGIC Redshift and `institution_metrics.py --all` rebuilds the counts.
# MAGIC
# MAGIC | dataset | columns | rows |
# MAGIC |---|---|---|
# MAGIC | `oa_work` | doi, issn_l, year | every work with a DOI and a primary-source ISSN-L (also the cited side) |
# MAGIC | `oa_work_institution` | doi, openalex_id, is_direct | works from the last `YEARS_BACK` years: each authorship institution plus its lineage |
# MAGIC | `oa_work_refs` | citing_doi, cited_doi | references from those works (with at least one institution) to works in `oa_work` |
# MAGIC
# MAGIC Lineage is the works' own `authorships.institutions.lineage` (super systems already removed), the same
# MAGIC thing the API's `authorships.institutions.lineage` filter matches. `is_direct` = an author is affiliated
# MAGIC with `openalex_id` itself, not only with one of its descendants.
# MAGIC
# MAGIC **Source**: `openalex.works.openalex_works` (read only) · **Target**: `s3://openalex-sandbox/unsub/institution_metrics/<run_date>/`
# MAGIC · **Runs**: monthly. Full replacement; `LATEST` (the run date) is written last, so a reader never sees a half-written run.

# COMMAND ----------

import json
from datetime import datetime, timezone

OUT = "s3://openalex-sandbox/unsub/institution_metrics"
YEARS_BACK = 7  # Unsub reads the last 5 complete years; keep a margin
KEEP_RUNS = 2   # older run folders are deleted at the end

run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
run_path = f"{OUT}/{run_date}"
min_year = datetime.now(timezone.utc).year - YEARS_BACK

# COMMAND ----------

# fail fast if the fields this export relies on have moved
works_schema = spark.table("openalex.works.openalex_works").schema
institution_fields = works_schema["authorships"].dataType.elementType["institutions"].dataType.elementType.fieldNames()
assert {"id", "lineage"} <= set(institution_fields), institution_fields
assert "issn_l" in works_schema["primary_location"].dataType["source"].dataType.fieldNames()
assert "referenced_works" in works_schema.fieldNames()

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TEMP VIEW unsub_works AS
    SELECT
        id AS work_id,
        LOWER(REPLACE(doi, 'https://doi.org/', '')) AS doi,
        primary_location.source.issn_l AS issn_l,
        CAST(publication_year AS INT) AS year,
        authorships,
        referenced_works
    FROM openalex.works.openalex_works
    WHERE doi IS NOT NULL
      AND primary_location.source.issn_l IS NOT NULL
      AND LENGTH(doi) <= 1000
""")

# one row per DOI (a DOI on two works keeps the lower work id)
spark.sql("""
    CREATE OR REPLACE TEMP VIEW unsub_works_by_doi AS
    SELECT * FROM unsub_works
    QUALIFY ROW_NUMBER() OVER (PARTITION BY doi ORDER BY work_id) = 1
""")

# COMMAND ----------

oa_work = spark.sql("SELECT doi, issn_l, year FROM unsub_works_by_doi")

oa_work_institution = spark.sql(f"""
    SELECT DISTINCT
        w.doi,
        CAST(REPLACE(lineage_id, 'https://openalex.org/I', '') AS BIGINT) AS openalex_id,
        lineage_id = inst.id AS is_direct
    FROM unsub_works_by_doi w
    LATERAL VIEW EXPLODE(w.authorships) a AS authorship
    LATERAL VIEW EXPLODE(authorship.institutions) i AS inst
    LATERAL VIEW EXPLODE(inst.lineage) l AS lineage_id
    WHERE w.year >= {min_year}
""")

oa_work_refs = spark.sql(f"""
    WITH citing AS (
        SELECT doi AS citing_doi, EXPLODE(referenced_works) AS cited_work_id
        FROM unsub_works_by_doi
        WHERE year >= {min_year}
          AND EXISTS(authorships, a -> SIZE(a.institutions) > 0)
    )
    SELECT DISTINCT c.citing_doi, cited.doi AS cited_doi
    FROM citing c
    JOIN unsub_works cited ON cited.work_id = c.cited_work_id
""")

# COMMAND ----------

counts = {}
for name, df in [("oa_work", oa_work), ("oa_work_institution", oa_work_institution), ("oa_work_refs", oa_work_refs)]:
    df.write.mode("overwrite").parquet(f"{run_path}/{name}/")
    counts[name] = spark.read.parquet(f"{run_path}/{name}/").count()
    print(f"{name}: {counts[name]:,} rows")

dbutils.fs.put(f"{run_path}/_MANIFEST.json", json.dumps({"run_date": run_date, "min_year": min_year, "counts": counts}), overwrite=True)
dbutils.fs.put(f"{OUT}/LATEST", run_date, overwrite=True)
print(f"exported {run_path}")

# COMMAND ----------

run_folders = sorted(f.name.rstrip("/") for f in dbutils.fs.ls(OUT) if f.isDir())
for old in run_folders[:-KEEP_RUNS]:
    dbutils.fs.rm(f"{OUT}/{old}", recurse=True)
    print(f"removed {old}")
