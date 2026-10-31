# Databricks notebook source
# MAGIC %md
# MAGIC # Peer reviews showing the reviewed article's abstract: weekly refresh (oxjob #1790)
# MAGIC
# MAGIC Crossref deposits peer reviews (Copernicus referee comments, eLife and PeerJ reviews, F1000 / HRB / Gates
# MAGIC reports, Cambridge Prisms, SciPost ...) without an abstract, and the review's landing page or PDF often carries
# MAGIC the REVIEWED article's abstract, so the review showed it. Two lists stop that:
# MAGIC
# MAGIC - `BORROWED` (`openalex.abstracts.peer_review_borrowed_abstracts`): landing_page and pdf rows of Crossref
# MAGIC   peer-review DOIs whose text is word for word (letters and digits, case-free, a leading "Abstract" dropped,
# MAGIC   80+ left) another work's abstract, that work not a review or titled like one. CreateSuperLocations NULLs them
# MAGIC   (join on provenance + native_id + xxhash64(abstract) + length(abstract), so the four keys must stay unique).
# MAGIC - `BACKFILL_LIST` (`openalex.abstracts.peer_review_borrowed_backfill`): review work_ids whose legacy
# MAGIC   `abstracts_backfill` text is word for word the matched article's abstract. Copied into
# MAGIC   `abstracts_backfill_not_abstract` with reason `peer_review_borrowed` (CreateWorksBase anti-joins it;
# MAGIC   #1800's FlagBackfillNotAbstracts re-reads `BACKFILL_LIST` on every rebuild).
# MAGIC
# MAGIC Both were built once on 29-30 Oct 2026 (bar: 40 of 40 and 20 of 20 random rows the reviewed article). New reviews
# MAGIC arrive every day (~600-1,000 a week would be listed), so this notebook re-runs the same rule weekly and **adds**
# MAGIC new keys. It never updates or deletes a row: a text that stops matching stays listed (it was the article's).
# MAGIC
# MAGIC - **Guard:** stops before writing when a week would add more than `MAX_NEW_ROWS` rows, or when the keys would not
# MAGIC   be unique. A jump that size means the rule or an input changed, not a busy week: read a sample first.
# MAGIC - **Dry run:** job parameter `dry_run` = `true` counts and samples, writes nothing.
# MAGIC - **Revert one run:** every row it adds carries that run's `created_at` / `built_at`:
# MAGIC   `DELETE FROM <table> WHERE created_at = '<run ts>'` on both lists, and
# MAGIC   `DELETE FROM openalex.abstracts.abstracts_backfill_not_abstract WHERE reason = 'peer_review_borrowed' AND built_at = '<run ts>'`.
# MAGIC   Revert everything: #1790 write log (`plans/landing-pages-log.md`, rows 29 Oct 06:51 and 30 Oct 10:40).

# COMMAND ----------

import time

BORROWED = "openalex.abstracts.peer_review_borrowed_abstracts"
BACKFILL_LIST = "openalex.abstracts.peer_review_borrowed_backfill"
NOT_ABSTRACT = "openalex.abstracts.abstracts_backfill_not_abstract"
LABEL = "peer-review-borrowed-v1"
BACKFILL_LABEL = "peer-review-borrowed-backfill-v1"
MAX_NEW_ROWS = 20000  # a normal week adds ~1K; the first build was 191,786 rows
# Scratch copies of this run's matches (serverless has no CACHE TABLE; the match scans every work once).
SCRATCH = "openalex.transient.oxjob1790_pr_borrowed_weekly"
SCRATCH_BF = "openalex.transient.oxjob1790_pr_backfill_weekly"

dbutils.widgets.text("dry_run", "false")
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
RUN_TS = spark.sql("SELECT date_trunc('second', current_timestamp()) AS t").first()["t"]
print(f"run {RUN_TS} dry_run={DRY_RUN}")


def write(sql):
    """Run one write; retry a few times when another job writes the same table at the same moment."""
    for attempt in range(4):
        try:
            return spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            if "Concurrent" not in str(e) or attempt == 3:
                raise
            print(f"concurrent write, retry {attempt + 1}: {str(e)[:200]}")
            time.sleep(60 * (attempt + 1))

# COMMAND ----------

# MAGIC %md ## 1. Landing-page and PDF rows (same rule as #1790 work/pr_borrowed_build.sql)

# COMMAND ----------

spark.sql(r"""
CREATE OR REPLACE TABLE SCRATCH AS
WITH pr AS (
  SELECT DISTINCT native_id AS doi FROM openalex.works.locations_parsed
  WHERE provenance = 'crossref' AND (raw_type = 'peer-review' OR type = 'peer-review')),
rows AS (
  SELECT lp.provenance, lp.native_id, get(filter(lp.ids, x -> x.namespace = 'doi').id, 0) AS doi,
         xxhash64(lp.abstract) AS abstract_hash, length(lp.abstract) AS text_len,
         lower(regexp_replace(regexp_replace(lp.abstract, '^\\s*(?i)abstract', ''), '[^a-zA-Z0-9]', '')) AS nt
  FROM openalex.works.locations_parsed lp
  JOIN pr ON pr.doi = get(filter(lp.ids, x -> x.namespace = 'doi').id, 0)
  WHERE lp.provenance IN ('landing_page', 'pdf') AND lp.abstract IS NOT NULL AND length(trim(lp.abstract)) >= 100),
rows2 AS (SELECT * EXCEPT (nt), xxhash64(nt) AS nh FROM rows WHERE length(nt) >= 80),
others AS (
  SELECT xxhash64(nt) AS oh, min_by(id, id) AS matched_work_id, min_by(substring(doi, 17), id) AS matched_doi, count(*) AS n_matched
  FROM (SELECT id, title, doi, lower(regexp_replace(regexp_replace(abstract, '^\\s*(?i)abstract', ''), '[^a-zA-Z0-9]', '')) AS nt
        FROM openalex.works.openalex_works
        WHERE abstract IS NOT NULL AND length(trim(abstract)) >= 100 AND type <> 'peer-review'
          -- copies of a review under another DOI ("Comment on ...", "Peer Review #2 of ...") prove nothing;
          -- iThenticate reports stay: their text is boilerplate on Access Microbiology review PDFs
          AND (NOT coalesce(title, '') RLIKE '(?i)^\\s*(peer review|review of|review [0-9]|reviewer|report on|referee|comment on|reply on|reply to|author response|authors? reply|editor|decision|response to)'
               OR title LIKE 'iThenticate%'))
  WHERE length(nt) >= 80
  GROUP BY 1)
SELECT r.provenance, r.native_id, min(r.doi) AS doi, r.abstract_hash, r.text_len,
       min(o.matched_work_id) AS matched_work_id, min_by(o.matched_doi, o.matched_work_id) AS matched_doi,
       min_by(o.n_matched, o.matched_work_id) AS n_matched
FROM rows2 r JOIN others o ON o.oh = r.nh
GROUP BY r.provenance, r.native_id, r.abstract_hash, r.text_len
""".replace("SCRATCH", SCRATCH))

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW borrowed_new AS
SELECT n.* FROM {SCRATCH} n
LEFT ANTI JOIN {BORROWED} b
  ON b.provenance = n.provenance AND b.native_id = n.native_id AND b.abstract_hash = n.abstract_hash AND b.text_len = n.text_len
""")
new_rows = spark.table("borrowed_new").count()
new_dois = spark.sql("SELECT count(DISTINCT doi) AS n FROM borrowed_new").first()["n"]
print(f"{BORROWED}: {new_rows:,} new rows on {new_dois:,} DOIs")
display(spark.sql("""
  SELECT n.provenance, n.doi, n.matched_doi, w.title AS matched_title
  FROM borrowed_new n LEFT JOIN openalex.works.openalex_works w ON w.id = n.matched_work_id
  ORDER BY xxhash64(n.native_id, 1790) LIMIT 20"""))

assert new_rows <= MAX_NEW_ROWS, f"{new_rows:,} new rows > {MAX_NEW_ROWS:,}: read a sample before raising the cap"
dup = spark.sql(f"""
  SELECT count(*) AS n FROM (
    SELECT provenance, native_id, abstract_hash, text_len FROM {BORROWED}
    UNION ALL SELECT provenance, native_id, abstract_hash, text_len FROM borrowed_new)
  GROUP BY provenance, native_id, abstract_hash, text_len HAVING count(*) > 1""").count()
assert dup == 0, f"{dup} duplicate keys: CreateSuperLocations would duplicate locations"

if not DRY_RUN and new_rows:
    write(f"""
    INSERT INTO {BORROWED} (provenance, native_id, doi, abstract_hash, text_len, matched_work_id, matched_doi, n_matched, label_version, created_at)
    SELECT n.provenance, n.native_id, n.doi, n.abstract_hash, n.text_len, n.matched_work_id, n.matched_doi, n.n_matched,
           '{LABEL}', timestamp'{RUN_TS}'
    FROM borrowed_new n
    LEFT ANTI JOIN {BORROWED} b
      ON b.provenance = n.provenance AND b.native_id = n.native_id AND b.abstract_hash = n.abstract_hash AND b.text_len = n.text_len
    """)

# COMMAND ----------

# MAGIC %md ## 2. Legacy backfill half (same rule as #1790 work/pr_borrowed_backfill_build.sql)

# COMMAND ----------

# In a dry run the new rows aren't in BORROWED yet, so read them from the view as well.
spark.sql(f"""
CREATE OR REPLACE TABLE {SCRATCH_BF} AS
WITH m AS (SELECT DISTINCT doi, matched_work_id, matched_doi FROM (
             SELECT doi, matched_work_id, matched_doi FROM {BORROWED}
             UNION ALL SELECT doi, matched_work_id, matched_doi FROM borrowed_new)),
rw AS (SELECT m.doi, m.matched_work_id, m.matched_doi, w.id AS work_id
       FROM m JOIN openalex.works.openalex_works w ON w.doi = concat('https://doi.org/', m.doi)
       LEFT ANTI JOIN {BACKFILL_LIST} p ON p.work_id = w.id),
bf AS (
  SELECT bf.work_id, CASE WHEN r.work_id IS NOT NULL THEN r.abstract ELSE bf.abstract END AS abstract
  FROM openalex.abstracts.abstracts_backfill bf
  LEFT JOIN openalex.abstracts.abstracts_backfill_repair r ON bf.work_id = r.work_id
  WHERE bf.work_id IN (SELECT work_id FROM rw)),
mw AS (SELECT id, lower(regexp_replace(regexp_replace(abstract, '^\\\\s*(?i)abstract', ''), '[^a-zA-Z0-9]', '')) AS mt
       FROM openalex.works.openalex_works WHERE id IN (SELECT matched_work_id FROM m))
SELECT rw.work_id, min(rw.doi) AS doi, min(length(bf.abstract)) AS text_len,
       min(rw.matched_work_id) AS matched_work_id, min_by(rw.matched_doi, rw.matched_work_id) AS matched_doi
FROM rw JOIN bf ON bf.work_id = rw.work_id JOIN mw ON mw.id = rw.matched_work_id
WHERE length(mw.mt) >= 80
  AND lower(regexp_replace(regexp_replace(bf.abstract, '^\\\\s*(?i)abstract', ''), '[^a-zA-Z0-9]', '')) = mw.mt
GROUP BY rw.work_id
""")
new_backfill = spark.table(SCRATCH_BF).count()
print(f"{BACKFILL_LIST}: {new_backfill:,} new work_ids")
assert new_backfill <= MAX_NEW_ROWS, f"{new_backfill:,} new backfill work_ids > {MAX_NEW_ROWS:,}"

if not DRY_RUN and new_backfill:
    # The list first: FlagBackfillNotAbstracts re-derives its peer_review_borrowed rows from it on a rebuild.
    write(f"""
    INSERT INTO {BACKFILL_LIST} (work_id, doi, text_len, matched_work_id, matched_doi, label_version, created_at)
    SELECT n.work_id, n.doi, n.text_len, n.matched_work_id, n.matched_doi, '{BACKFILL_LABEL}', timestamp'{RUN_TS}'
    FROM {SCRATCH_BF} n LEFT ANTI JOIN {BACKFILL_LIST} p ON p.work_id = n.work_id
    """)

if not DRY_RUN:
    # Every listed work_id not yet in the anti-join table (also heals a rebuild that missed some).
    write(f"""
    INSERT INTO {NOT_ABSTRACT} (work_id, reason, text_len, filter_commit, built_at)
    SELECT p.work_id, 'peer_review_borrowed', p.text_len, p.label_version, timestamp'{RUN_TS}'
    FROM {BACKFILL_LIST} p
    LEFT ANTI JOIN {NOT_ABSTRACT} na ON na.work_id = p.work_id
    """)

# COMMAND ----------

summary = spark.sql(f"""
  SELECT (SELECT count(*) FROM {BORROWED}) AS borrowed_rows,
         (SELECT count(*) FROM {BORROWED} WHERE created_at = timestamp'{RUN_TS}') AS borrowed_added,
         (SELECT count(*) FROM {BACKFILL_LIST}) AS backfill_rows,
         (SELECT count(*) FROM {BACKFILL_LIST} WHERE created_at = timestamp'{RUN_TS}') AS backfill_added,
         (SELECT count(*) FROM {NOT_ABSTRACT} WHERE reason = 'peer_review_borrowed') AS not_abstract_rows,
         (SELECT count(*) FROM {NOT_ABSTRACT} WHERE reason = 'peer_review_borrowed' AND built_at = timestamp'{RUN_TS}') AS not_abstract_added
""").first().asDict()
print(summary)
dbutils.notebook.exit(str({"run_ts": str(RUN_TS), "dry_run": DRY_RUN, "new_rows": new_rows, "new_backfill": new_backfill, **summary}))
