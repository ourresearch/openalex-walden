# Databricks notebook source
# MAGIC %md
# MAGIC # Build the study-design tagger queue (oxjob #1312)
# MAGIC
# MAGIC Every work with an abstract in a research-carrying type that has no
# MAGIC `works_study_design_tagger` row at the current `tagger_version`. The queue
# MAGIC carries the tagger's inputs (title, venue, abstract) so the tagger never
# MAGIC joins back to `openalex_works`, and is partitioned into hash chunks of
# MAGIC ~`chunk_works` rows whose ids sort in priority order:
# MAGIC
# MAGIC | priority | works |
# MAGIC |---|---|
# MAGIC | 0 | created in the last `new_days` days (the nightly stream) |
# MAGIC | 1 | has a PMID (PubMed-vs-tagger disagreement is measured on these) |
# MAGIC | 2 | no PMID, publication year >= 2000 |
# MAGIC | 3 | the rest |
# MAGIC
# MAGIC Rebuilt from scratch on every run (CREATE OR REPLACE); the tagger table is
# MAGIC the only state. No chunk needs a global sort: chunk = priority * 1e6 +
# MAGIC hash(work_id) mod buckets(priority).

# COMMAND ----------

import math
import os
import sys
import time

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "study_design.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "study_design.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import study_design as sd  # noqa: E402

dbutils.widgets.text("schema", "openalex.works", "target schema")
dbutils.widgets.text("chunk_works", "50000", "works per chunk (one Jev wave; ~3-4 min at 250 req/s)")
dbutils.widgets.text("min_abstract_chars", "100", "skip works whose abstract is shorter")
dbutils.widgets.text("min_year", "0", "skip works published before this year (0 = no floor)")
dbutils.widgets.text("new_days", "30", "works created within this many days get priority 0")

SCHEMA = dbutils.widgets.get("schema").strip()
CHUNK = int(dbutils.widgets.get("chunk_works"))
MIN_ABS = int(dbutils.widgets.get("min_abstract_chars"))
MIN_YEAR = int(dbutils.widgets.get("min_year"))
NEW_DAYS = int(dbutils.widgets.get("new_days"))

QUEUE = f"{SCHEMA}.works_study_design_queue"
TAGGER = f"{SCHEMA}.works_study_design_tagger"
TYPES = "'article', 'review', 'preprint', 'conference-paper', 'book-chapter', 'dissertation', 'report', 'data-paper'"

# COMMAND ----------

# The tagger table must exist for the anti-join, even on the very first run.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TAGGER} (
  work_id BIGINT NOT NULL,
  tagger_values ARRAY<STRING>,
  scores MAP<STRING, FLOAT>,
  probabilities MAP<STRING, FLOAT>,
  is_rct FLOAT,
  human_subjects FLOAT,
  stated_random BOOLEAN,
  abstract_chars INT,
  input_tokens INT,
  tagger_version STRING,
  jev_model STRING,
  updated_at TIMESTAMP
) USING DELTA CLUSTER BY (work_id)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
COMMENT 'Automated study-design tagging per work (oxjob #1312). Append-only; latest updated_at per (work_id, tagger_version) wins.'
""")

CAND = f"""
SELECT w.id AS work_id, w.title, w.primary_location.source.display_name AS venue, w.abstract,
       w.publication_year, w.type,
       CAST(regexp_extract(w.ids['pmid'], '(\\\\d+)$', 1) AS BIGINT) AS pmid,
       CASE WHEN w.created_date >= date_sub(current_date(), {NEW_DAYS}) THEN 0
            WHEN w.ids['pmid'] IS NOT NULL THEN 1
            WHEN w.publication_year >= 2000 THEN 2
            ELSE 3 END AS priority
FROM openalex.works.openalex_works w
LEFT ANTI JOIN {TAGGER} t
  ON w.id = t.work_id AND t.tagger_version = '{sd.TAGGER_VERSION}'
WHERE NOT w.is_xpac
  AND w.type IN ({TYPES})
  AND w.title IS NOT NULL AND length(w.title) >= 10
  AND w.abstract IS NOT NULL AND length(w.abstract) >= {MIN_ABS}
  AND (w.publication_year IS NULL OR w.publication_year >= {MIN_YEAR})
"""

# COMMAND ----------

# Pass 1 (ids only, columnar: cheap): rows per priority -> buckets per priority.
t0 = time.time()
counts = {int(r.priority): int(r.n) for r in spark.sql(f"SELECT priority, count(*) AS n FROM ({CAND}) GROUP BY priority").collect()}
buckets = {p: max(1, math.ceil(n / CHUNK)) for p, n in counts.items()}
total = sum(counts.values())
print(f"queue candidates: {total:,} works  by priority {counts}  buckets {buckets}  ({time.time() - t0:.0f}s)")
print(f"projected Jev spend if fully tagged: ${total * 1500 * sd.JEV_USD_PER_TOKEN:,.0f} at ~1,500 tokens/work")

# COMMAND ----------

# Pass 2: materialise the queue with the tagger's inputs, partitioned by chunk.
bucket_case = " ".join(f"WHEN {p} THEN {b}" for p, b in buckets.items()) or "WHEN 0 THEN 1"
build_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
t0 = time.time()
spark.sql(f"""
CREATE OR REPLACE TABLE {QUEUE}
USING DELTA
PARTITIONED BY (chunk_id)
COMMENT 'Tagger input queue (oxjob #1312); rebuilt every run by BuildStudyDesignQueue'
AS
SELECT work_id, title, venue, abstract, pmid, publication_year, type, priority,
       '{build_id}' AS build_id,
       CAST(priority * 1000000 + pmod(xxhash64(work_id), CASE priority {bucket_case} ELSE 1 END) AS BIGINT) AS chunk_id
FROM ({CAND})
""")
print(f"queue {QUEUE} built: build_id={build_id} ({time.time() - t0:.0f}s)")
display(spark.sql(f"SELECT priority, count(DISTINCT chunk_id) AS chunks, count(*) AS works, min(chunk_id) AS first_chunk FROM {QUEUE} GROUP BY priority ORDER BY priority"))
