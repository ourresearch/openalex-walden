# Databricks notebook source
# MAGIC %md
# MAGIC # Strip invalid GROBID XMLs from `openalex.pdf.pdf_works`
# MAGIC
# MAGIC One-shot cleanup: for every pdf_works row whose `ids` array contains a
# MAGIC `docs.parsed-pdf` entry pointing at a known-bad `grobid_uuid` (per
# MAGIC `openalex.pdf.invalid_grobids`), strip just that `docs.parsed-pdf` entry.
# MAGIC The `docs.pdf` entry is left intact — the PDF is still valid, only the
# MAGIC GROBID parse is bad.
# MAGIC
# MAGIC Pairs with `notebooks/ingest/PDF.py`'s `grobid_raw` stream-static
# MAGIC anti-join filter (Track 2 of #202). The filter prevents *future* bad
# MAGIC grobid_uuids from entering the pipeline; this notebook handles the
# MAGIC *existing* ~8M rows already in pdf_works.
# MAGIC
# MAGIC **Source**: `openalex.pdf.invalid_grobids` (seeded from grobid_xml_classification)
# MAGIC **Target**: `openalex.pdf.pdf_works` (Delta MERGE, in-place UPDATE of ids)
# MAGIC
# MAGIC ## What propagates
# MAGIC
# MAGIC 1. CDF emits `update_postimage` on the touched pdf_works rows.
# MAGIC 2. `UnionAllWorksIntoLocationsParsed` consumes the CDF → apply_changes
# MAGIC    updates `locations_parsed` (same native_id, ids array minus bad parsed-pdf).
# MAGIC 3. Next `CreateSuperLocations` run: grobid_s3_id NULL for that row.
# MAGIC 4. Next `CreateLocationsMapped` MERGE: IS DISTINCT FROM → UPDATE
# MAGIC    `locations_mapped.grobid_s3_id` to NULL.
# MAGIC 5. Next `CreateWorksBase`: has_content.grobid_xml false.
# MAGIC 6. Next `sync_works`: API reflects.
# MAGIC
# MAGIC ## Multi-XML works
# MAGIC
# MAGIC 1.7M work_ids have multiple grobid_s3_id rows in locations_mapped. Each
# MAGIC row corresponds to a different native_id (PDF source). We strip
# MAGIC docs.parsed-pdf per-native_id only when its grobid_uuid is in
# MAGIC invalid_grobids. Other native_id rows (potentially with good XMLs)
# MAGIC stay untouched. After the strip, the next CreateLocationsMapped run
# MAGIC NULLs out only the bad-uuid row's grobid_s3_id; the good sibling row
# MAGIC stays valid; CreateWorksBase rolls up across siblings so
# MAGIC has_content.grobid_xml stays true if any sibling survives.
# MAGIC
# MAGIC ## Re-scrape coexistence
# MAGIC
# MAGIC A successful re-scrape (taxicab → grobid_processing_results) produces a
# MAGIC NEW grobid_uuid (different UUID) NOT in invalid_grobids. apply_changes
# MAGIC treats the new event as an UPDATE keyed by native_id and replaces the
# MAGIC (stripped) ids array with a fresh one — naturally re-adding the good
# MAGIC docs.parsed-pdf entry.
# MAGIC
# MAGIC Widgets:
# MAGIC   - `pilot_limit`: 0 = full cohort. >0 = strip only first N grobid_uuids.
# MAGIC   - `dry_run`: "true" = compute counts + preview only, no MERGE.

# COMMAND ----------

dbutils.widgets.text("pilot_limit", "0", "Pilot limit (0 = full cohort)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (no MERGE)")

PILOT_LIMIT = int(dbutils.widgets.get("pilot_limit"))
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

print(f"pilot_limit={PILOT_LIMIT:,}  dry_run={DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the cohort

# COMMAND ----------

pilot_clause = f"LIMIT {PILOT_LIMIT}" if PILOT_LIMIT > 0 else ""

spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW _invalid_grobid_cohort AS
  SELECT
    grobid_uuid,
    CONCAT(grobid_uuid, '.xml.gz') AS bad_xml_id
  FROM openalex.pdf.invalid_grobids
  {pilot_clause}
""")

cohort_count = spark.sql("SELECT COUNT(*) AS n FROM _invalid_grobid_cohort").collect()[0]["n"]
print(f"Cohort size: {cohort_count:,} grobid_uuids")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find victims + collect the specific docs.parsed-pdf ids to drop per native_id

# COMMAND ----------

spark.sql("""
  CREATE OR REPLACE TEMP VIEW _exploded_pdf_works AS
  SELECT native_id, EXPLODE(ids) AS i
  FROM openalex.pdf.pdf_works
""")

spark.sql("""
  CREATE OR REPLACE TEMP VIEW _strip_plan AS
  SELECT
    e.native_id,
    COLLECT_SET(e.i.id) AS bad_xml_ids_to_drop
  FROM _exploded_pdf_works e
  JOIN _invalid_grobid_cohort c
    ON e.i.namespace = 'docs.parsed-pdf'
   AND e.i.id        = c.bad_xml_id
  GROUP BY e.native_id
""")

victim_count = spark.sql("SELECT COUNT(*) AS n FROM _strip_plan").collect()[0]["n"]
print(f"Victim rows in pdf_works: {victim_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview the change on 5 sample rows

# COMMAND ----------

spark.sql("""
  SELECT
    p.native_id,
    SIZE(p.ids) AS ids_before_count,
    SIZE(FILTER(
      p.ids,
      x -> NOT (x.namespace = 'docs.parsed-pdf' AND ARRAY_CONTAINS(s.bad_xml_ids_to_drop, x.id))
    )) AS ids_after_count,
    TRANSFORM(
      FILTER(
        p.ids,
        x -> x.namespace = 'docs.parsed-pdf' AND ARRAY_CONTAINS(s.bad_xml_ids_to_drop, x.id)
      ),
      x -> STRUCT(x.namespace, x.id)
    ) AS ids_being_dropped
  FROM openalex.pdf.pdf_works p
  JOIN _strip_plan s ON p.native_id = s.native_id
  LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE — strip only the bad docs.parsed-pdf entries

# COMMAND ----------

if DRY_RUN:
    print("DRY RUN — skipping MERGE. Set dry_run=false to apply.")
else:
    spark.sql("""
      MERGE INTO openalex.pdf.pdf_works AS t
      USING _strip_plan                  AS s
      ON t.native_id = s.native_id
      WHEN MATCHED THEN UPDATE SET
        t.ids = FILTER(
          t.ids,
          x -> NOT (x.namespace = 'docs.parsed-pdf' AND ARRAY_CONTAINS(s.bad_xml_ids_to_drop, x.id))
        )
    """)
    print(f"MERGE complete — stripped bad docs.parsed-pdf entries from {victim_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Post-MERGE: zero `docs.parsed-pdf` entries whose id is in invalid_grobids.

# COMMAND ----------

residual = spark.sql("""
  SELECT COUNT(*) AS n
  FROM _exploded_pdf_works e
  JOIN _invalid_grobid_cohort c
    ON e.i.namespace = 'docs.parsed-pdf'
   AND e.i.id        = c.bad_xml_id
""").collect()[0]["n"]

if DRY_RUN:
    print(f"DRY RUN — would strip {residual:,} bad docs.parsed-pdf entries")
else:
    print(f"POST-MERGE residual: docs.parsed-pdf with bad grobid_uuid = {residual:,} (expected 0)")
