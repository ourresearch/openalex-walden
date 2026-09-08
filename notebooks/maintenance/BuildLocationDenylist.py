# Databricks notebook source
# MAGIC %md
# MAGIC # Build the mega-work location denylist (oxjob #946)
# MAGIC
# MAGIC A **static snapshot** of the locations that make a work enormous, so `CreateLocationsMapped`
# MAGIC can drop them and `CreateWorksBase` can stop hiding the problem behind
# MAGIC `QUALIFY row_num <= 10`.
# MAGIC
# MAGIC **The rule, one predicate:** an `(work, endpoint)` group contributing **>= 100 locations** to
# MAGIC a single work is junk — many distinct items collapsed onto one merge key (a newspaper's every
# MAGIC issue, every CCDC deposition, the 107 AraPheno phenotypes of one GWAS paper). A work whose
# MAGIC 100+ locations come from 100+ *different* endpoints is the opposite shape — one item deposited
# MAGIC in many repositories — and is left alone. That distinction is what keeps the Higgs mass
# MAGIC measurement, the GBD studies and the autophagy guidelines out of this table.
# MAGIC
# MAGIC **Deletion is not a second rule.** Nothing here deletes anything. Works whose every location
# MAGIC is denylisted simply stop appearing in `openalex_works`, and `TrackDeletedWorks` ledgers them
# MAGIC through the normal #784 path. Works with something real left (the GWAS paper keeps its
# MAGIC crossref + pubmed + 3 repo copies) survive with correct locations.
# MAGIC
# MAGIC **Static on purpose.** 94.7% of the captured population is >2 years old — a frozen legacy
# MAGIC backfill, not a live firehose. Regrowth is ~6,400 locations/month from two sources (`datacite`
# MAGIC title-key collapse, which is #880's defect to fix at the key, and one Hungarian newspaper
# MAGIC endpoint, which is #881's endpoint decision). A self-updating MV would be a standing authority
# MAGIC to auto-delete works with no review; this is a bounded, auditable list instead.
# MAGIC
# MAGIC `mode=report` (the default) writes nothing and prints what a rebuild WOULD add — run it
# MAGIC quarterly, read the diff, then run `mode=build` if the additions look right.
# MAGIC
# MAGIC Counted from the **pre-filter** sources (`locations_w_types` + `locations_stale`), never from
# MAGIC `locations_mapped` — otherwise the filter hides its own input, groups fall back under the
# MAGIC threshold, and the list oscillates.

# COMMAND ----------

dbutils.widgets.text("env_suffix", "", "Environment suffix (e.g. _dev)")
dbutils.widgets.text("threshold", "100", "Locations from one endpoint that make a group junk")
dbutils.widgets.dropdown("mode", "report", ["report", "build"], "report = print the diff only")

ENV_SUFFIX = dbutils.widgets.get("env_suffix")
THRESHOLD = int(dbutils.widgets.get("threshold"))
MODE = dbutils.widgets.get("mode")

CATALOG = f"openalex{ENV_SUFFIX}"
DENYLIST = f"{CATALOG}.works.location_denylist"

print(f"catalog:   {CATALOG}")
print(f"threshold: {THRESHOLD}")
print(f"mode:      {MODE}")

# COMMAND ----------

# The pre-filter population: live rows carrying the work_id the registry assigns them, plus the
# stale sidecar (which already carries its own work_id). Mirrors the `live`/`stale` CTEs in
# CreateLocationsMapped, including its (provenance, namespace, native_id) dedup.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW denylist_candidates AS
WITH t AS (
  SELECT provenance, native_id_namespace, native_id, endpoint_id
  FROM {CATALOG}.works.locations_w_types
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY provenance, native_id_namespace, native_id
    ORDER BY updated_date DESC) = 1
),
live_anchor AS (
  SELECT r.work_id, t.provenance, t.native_id_namespace, t.native_id,
         COALESCE(t.endpoint_id, t.provenance) AS src
  FROM t
  LEFT JOIN {CATALOG}.works.location_work_ids r
    ON  t.provenance          = r.provenance
    AND t.native_id_namespace = r.native_id_namespace
    AND t.native_id           = r.native_id
),
-- locations_stale carries no endpoint_id (NULLed in the union) and is only consulted where no
-- live row holds the same anchor -- both mirrored from CreateLocationsMapped's `stale` CTE.
stale_anchor AS (
  SELECT s.work_id, s.provenance, s.native_id_namespace, s.native_id, s.provenance AS src
  FROM {CATALOG}.works.locations_stale s
  LEFT ANTI JOIN live_anchor l
    ON  s.provenance          = l.provenance
    AND s.native_id_namespace = l.native_id_namespace
    AND s.native_id           = l.native_id
    AND s.work_id <=> l.work_id
),
src AS (SELECT * FROM live_anchor UNION ALL SELECT * FROM stale_anchor),
grp AS (
  SELECT work_id, src, COUNT(*) AS group_n
  FROM src
  WHERE work_id IS NOT NULL
  GROUP BY work_id, src
  HAVING COUNT(*) >= {THRESHOLD}
)
SELECT s.provenance, s.native_id_namespace, s.native_id,
       s.work_id, s.src AS endpoint, g.group_n
FROM src s
JOIN grp g ON s.work_id = g.work_id AND s.src = g.src
""")

# COMMAND ----------

if spark.catalog.tableExists(DENYLIST):
    new_rows = spark.sql(f"""
        SELECT c.* FROM denylist_candidates c
        LEFT ANTI JOIN {DENYLIST} d
          ON  c.provenance          = d.provenance
          AND c.native_id_namespace = d.native_id_namespace
          AND c.native_id           = d.native_id
    """)
else:
    new_rows = spark.sql("SELECT * FROM denylist_candidates")

new_rows.createOrReplaceTempView("denylist_new")
print(f"Locations the rule captures that are not already listed: {spark.table('denylist_new').count():,}")
display(spark.sql("""
    SELECT endpoint, COUNT(*) AS locations, COUNT(DISTINCT work_id) AS works, MAX(group_n) AS biggest_group
    FROM denylist_new GROUP BY endpoint ORDER BY locations DESC LIMIT 40
"""))

# COMMAND ----------

if MODE != "build":
    print("mode=report — nothing written. Re-run with mode=build to add these rows.")
    dbutils.notebook.exit("report-only")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DENYLIST} (
    provenance          STRING NOT NULL,
    native_id_namespace STRING,
    native_id           STRING NOT NULL,
    work_id             BIGINT,
    endpoint            STRING,
    group_n             BIGINT,
    listed_at           TIMESTAMP NOT NULL
)
""")

inserted = spark.sql(f"""
    INSERT INTO {DENYLIST}
    SELECT provenance, native_id_namespace, native_id, work_id, endpoint, group_n, current_timestamp()
    FROM denylist_new
""").collect()[0].num_inserted_rows

total = spark.sql(f"SELECT COUNT(*) AS c FROM {DENYLIST}").collect()[0].c
works = spark.sql(f"SELECT COUNT(DISTINCT work_id) AS c FROM {DENYLIST}").collect()[0].c
print(f"Inserted {inserted:,} locations. Denylist now holds {total:,} locations across {works:,} works.")
