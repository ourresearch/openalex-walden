# Databricks notebook source
# MAGIC %md
# MAGIC # Freeze Aurora's SDG tags, once → `openalex.works.works_sdg_aurora_frozen` (oxjob #1300, DEPRECATED field)
# MAGIC
# MAGIC Jason, 2026-10-01: the Jev-trained head replaces Aurora in `sustainable_development_goals`; Aurora stops
# MAGIC running, and its existing tags are kept in a field clearly marked **deprecated**,
# MAGIC `sustainable_development_goals_aurora`, for about a month, then removed completely.
# MAGIC
# MAGIC This notebook makes the frozen copy that field is filled from. Run it **once**, by hand, before the
# MAGIC replacement ships. It refuses to run again: the copy is never recomputed. `CreateWorksEnriched` re-applies it
# MAGIC every night (openalex_works is re-cloned from the base nightly) and keeps it out of the content hash.
# MAGIC
# MAGIC Source: exactly what `CreateWorksEnriched` merged into `sustainable_development_goals` before the switch:
# MAGIC Aurora's backfill (`work_sdg_backfill`, old works) plus its frontfill (`works_sdg_frontfill`, latest row per
# MAGIC new work). Read from those tables rather than from `openalex_works`, so the freeze is right whether it runs
# MAGIC before or after the switch. Element shape `{id, display_name, score}`, score as served (Aurora's sigmoid
# MAGIC output); display names normalized by goal id to the canonical names the head uses ("Life on land",
# MAGIC "Quality education", "Peace, justice, and strong institutions"; ticket 06896), the only change.
# MAGIC
# MAGIC Removal (about a month after the swap): drop this table, the `sustainable_development_goals_aurora` column in
# MAGIC `CreateWorksBase`, its MERGE cell in `CreateWorksEnriched`, and its lines in `sync_works` and
# MAGIC `BuildLakebaseWorksDocs`; then leave the field out of the next index mapping.

# COMMAND ----------

TARGET = "openalex.works.works_sdg_aurora_frozen"

if spark.catalog.tableExists(TARGET):
    raise RuntimeError(f"{TARGET} already exists. It is a one-time frozen copy of Aurora's SDG tags and is never "
                       "rebuilt (oxjob #1300). Nothing to do.")

CANONICAL_NAMES = [
    "No poverty", "Zero hunger", "Good health and well-being", "Quality education", "Gender equality",
    "Clean water and sanitation", "Affordable and clean energy", "Decent work and economic growth",
    "Industry, innovation and infrastructure", "Reduced inequalities", "Sustainable cities and communities",
    "Responsible consumption and production", "Climate action", "Life below water", "Life on land",
    "Peace, justice, and strong institutions", "Partnerships for the goals",
]
names_sql = ", ".join("'" + n.replace("'", "''") + "'" for n in CANONICAL_NAMES)

spark.sql(f"""
CREATE TABLE {TARGET} CLUSTER BY (work_id)
COMMENT 'DEPRECATED (oxjob #1300): Aurora SDG tags frozen when the Jev-trained head replaced Aurora in sustainable_development_goals. One-time copy, never rebuilt; feeds openalex_works.sustainable_development_goals_aurora. Remove about a month after the swap.'
AS
WITH aurora AS (
  -- Same union as CreateWorksEnriched's pre-switch SDG MERGE (backfill: old works only; frontfill: id > 6.6B)
  SELECT paper_id AS work_id, sustainable_development_goals AS sdgs
  FROM openalex.works.work_sdg_backfill
  UNION ALL
  SELECT work_id, sdg AS sdgs
  FROM (
    SELECT work_id, sdg,
      ROW_NUMBER() OVER (PARTITION BY work_id ORDER BY created_timestamp DESC NULLS LAST) AS rn
    FROM openalex.works.works_sdg_frontfill
    WHERE work_id > 6600000000
      AND SIZE(sdg) > 0
  ) ranked
  WHERE rn = 1
)
SELECT
  work_id,
  TRANSFORM(sdgs, s -> NAMED_STRUCT(
    'id', s.id,
    'display_name', COALESCE(TRY_ELEMENT_AT(ARRAY({names_sql}), TRY_CAST(SUBSTRING_INDEX(s.id, '/', -1) AS INT)),
                             s.display_name),
    'score', CAST(s.score AS FLOAT)
  )) AS sustainable_development_goals,
  CURRENT_TIMESTAMP() AS frozen_at
FROM aurora
WHERE SIZE(sdgs) > 0
""")
print(f"wrote {TARGET}")

# COMMAND ----------

# Checks: one row per work (CreateWorksEnriched's MERGE needs it), and the count next to openalex_works.
# Before the switch the two counts match (openalex_works still carries Aurora); after it they differ.
s = spark.sql(f"""
    SELECT COUNT(*) AS n, COUNT(DISTINCT work_id) AS works,
           SUM(SIZE(sustainable_development_goals)) AS tags
    FROM {TARGET}
""").first()
served = spark.sql("""
    SELECT COUNT(*) AS n FROM openalex.works.openalex_works WHERE SIZE(sustainable_development_goals) > 0
""").first()["n"]
print(f"frozen: {s['n']:,} rows, {s['works']:,} works, {s['tags']:,} tags; "
      f"openalex_works with a non-empty sustainable_development_goals now: {served:,}")
if s["n"] != s["works"]:
    raise RuntimeError(f"{TARGET} has {s['n'] - s['works']:,} duplicate work_ids: fix before CreateWorksEnriched reads it")
display(spark.sql(f"""
    SELECT t.id, t.display_name, COUNT(*) AS works
    FROM {TARGET} LATERAL VIEW explode(sustainable_development_goals) x AS t
    GROUP BY 1, 2 ORDER BY 1
"""))
