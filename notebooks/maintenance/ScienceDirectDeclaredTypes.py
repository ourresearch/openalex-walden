# Databricks notebook source
# MAGIC %md
# MAGIC # ScienceDirect's own content-type label: daily rows for `record_declared_types` (oxjob #2574)
# MAGIC
# MAGIC ScienceDirect labels each item it hosts (`<meta name="citation_article_type">`: Abstract, Correspondence, Erratum,
# MAGIC Contents list ...), but Crossref says only `journal-article`, so the type cascade typed these items article. On 24 Nov
# MAGIC 2026 oxjob #2574 wrote 1,120,113 rows to `openalex.works.record_declared_types` (platform `sciencedirect`); the End 2
# MAGIC End's CreateWorksBase gives a work typed article the type of its leading record's row (rdt WHEN), and `is_paratext`
# MAGIC follows a paratext row. Those rows covered the works of 24 Nov. New ScienceDirect items, pages crawled for the first
# MAGIC time, and items that cross a guard later (their reference count drops) would stay article, so this notebook re-runs
# MAGIC the same rule every day and **adds** rows for keys that have none. It never updates or deletes a row.
# MAGIC
# MAGIC **The rule** (#2574 `work/build_rows.sql`, barred 24 Nov on 200 + 80 fresh rows, outside review Gemini + GPT; plus the
# MAGIC 25 Nov guards marked below, barred on 175 rows and reviewed again): the label is read from the stored crawl of the DOI's
# MAGIC landing page (`openalex.landing_page.meta_tags_for_work_type`), on works still typed article whose DOI the page names
# MAGIC (or the page names none):
# MAGIC - Abstract, <= 5 references, a known 1-2 page span: a meeting signal (S-page, or an issue named supplement / abstracts /
# MAGIC   meeting) or #1976's shadow call conference-abstract -> `conference-abstract`; the shadow call other, or no signal and
# MAGIC   no call -> `other` (an abstract of another work, RULINGS 22 Nov 2026); the shadow model calling it article keeps it.
# MAGIC - Correspondence, <= 15 references, at most 4 pages, not (3+ pages and 6+ references) -> `letter`
# MAGIC - Erratum, <= 5 references, at most 3 pages, not a removal / withdrawal notice -> `erratum`
# MAGIC - Contents list, Editorial Board, Index with no byline -> `paratext`
# MAGIC - Announcement, About a conference, Calendar, Product review, Patent report, Literature alert, Personal report,
# MAGIC   <= 5 references -> `other`; 25 Nov: an 'About a conference' item with a byline, 3+ pages or an article number, and no
# MAGIC   meeting / notice word in its title may be a full conference paper, so the job abstains (it stays article)
# MAGIC - Left out: Retraction (the label sits on the retracted article itself), Publisher's note, Discussion, Miscellaneous.
# MAGIC - Keyed on the DOI's own Crossref record leading the work; a record with another platform's row keeps it; a record
# MAGIC   whose works (or stored pages) disagree, or where any one abstains, gets none.
# MAGIC - Pages: title ends 'ScienceDirect' or a truncated ' - S...' (24 Nov build: any 'S...' suffix, which also let in ~140
# MAGIC   pages of other publishers); an Erratum titled 'RETRACTED' / 'retraction' is a retraction notice and is left out.
# MAGIC
# MAGIC **Guards:** never writes while the End 2 End runs (it reads this table); a work waits 7 days after it is created
# MAGIC (references and pages settle); stops before writing when a run would add more than `max_new_rows` rows (a normal
# MAGIC day adds ~100; a jump means the rule or an input changed, e.g. a references change moving works under a guard: read
# MAGIC a sample first) or when a key would repeat (CreateWorksBase's LEFT JOIN would repeat the work). The batch is
# MAGIC materialized once (`<scratch>_new`), so the rows counted are the rows written.
# MAGIC **Dry run:** job parameter `dry_run` = `true` counts and samples, writes nothing.
# MAGIC **Revert one run:** pause the job first (it would add the same rows again the next day), then every row it added
# MAGIC carries the run's timestamp (UTC) in `harvested_at`:
# MAGIC `DELETE FROM openalex.works.record_declared_types WHERE oxjob = 2574 AND harvested_at = '<run ts>'` (the next night types
# MAGIC those works back). Revert everything #2574 wrote: `... WHERE oxjob = 2574`.

# COMMAND ----------

import time

RDT = "openalex.works.record_declared_types"
SCRATCH = "openalex.transient.oxjob2574_sd_daily"
END2END_JOB_ID = 616701029470182
PLATFORM = "sciencedirect"
OXJOB = 2574

# A normal day adds ~100; the first run (25 Nov) added ~2.5K with max_new_rows raised for that run.
dbutils.widgets.text("max_new_rows", "2000")
MAX_NEW_ROWS = int(dbutils.widgets.get("max_new_rows"))
dbutils.widgets.text("dry_run", "false")
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
RUN_TS = spark.sql("SELECT date_trunc('second', current_timestamp()) AS t").first()["t"]
print(f"run {RUN_TS} dry_run={DRY_RUN} max_new_rows={MAX_NEW_ROWS:,}")


def end2end_active():
    from databricks.sdk import WorkspaceClient
    return [r.run_id for r in WorkspaceClient().jobs.list_runs(job_id=END2END_JOB_ID, active_only=True)]


def write(sql):
    """Run one write; retry a few times when another job writes the same table at the same moment. Never while the
    End 2 End runs: checked before every attempt (shortcut: a check, not a lock; the run starts at 03:00 UTC, 7 h after
    this job, and a Delta append is atomic, so a run that starts mid-write reads the table before or after it)."""
    for attempt in range(4):
        active = end2end_active()
        if active:
            dbutils.notebook.exit(f"skipped: Walden End 2 End started (runs {active}); the next run adds these rows.")
        try:
            return spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            if "Concurrent" not in str(e) or attempt == 3:
                raise
            print(f"concurrent write, retry {attempt + 1}: {str(e)[:200]}")
            time.sleep(60 * (attempt + 1))


active = end2end_active()
if active and not DRY_RUN:
    dbutils.notebook.exit(f"skipped: Walden End 2 End is running (runs {active}); it reads {RDT}. The next run adds these rows.")

# COMMAND ----------

# MAGIC %md ## 1. Every ScienceDirect-labelled work still typed article, with its new type (or NULL: a guard keeps it article)

# COMMAND ----------

spark.sql(r"""
CREATE OR REPLACE TABLE SCRATCH AS
WITH m AS (
  SELECT lower(native_id) doi,
    regexp_extract(array_join(filter(coalesce(tx_meta, array()), t -> lower(t) LIKE '%citation_article_type%'), ' '), 'content="([^"]{1,60})"', 1) cat,
    lower(regexp_extract(array_join(filter(coalesce(tx_meta, array()), t -> lower(t) LIKE '%citation_doi%'), ' '), 'content="([^"]{1,200})"', 1)) cdoi
  FROM openalex.landing_page.meta_tags_for_work_type
  WHERE native_id_namespace = 'doi' AND tx_page_title RLIKE '(ScienceDirect| - S(c(i(e(n(c(e(D(i(r(e(c(t)?)?)?)?)?)?)?)?)?)?)?)?)$' AND native_id LIKE '10.%'),
w AS (
  SELECT w.id, w.is_xpac, lower(w.title) tl, lower(substr(w.doi, 17)) d, w.referenced_works_count nref, size(w.authorships) na,
         w.biblio.first_page fp, w.biblio.last_page lp, w.biblio.issue iss,
         w.type_decision.type td, coalesce(w.type_decision.confidence, 0) conf,
         w.primary_location.provenance prov, w.primary_location.native_id nid
  FROM openalex.works.openalex_works w
  WHERE w.type = 'article' AND w.doi IS NOT NULL AND w.primary_location.provenance IS NOT NULL AND w.primary_location.native_id IS NOT NULL
    AND w.created_date <= date_sub(current_date(), 7)),  -- a new work waits a week: its references and pages settle first
f AS (
  SELECT w.*, m.cat, (coalesce(m.cdoi, '') IN ('', w.d)) same_doi,
    try_cast(regexp_extract(w.lp, '([0-9]+)', 1) AS INT) - try_cast(regexp_extract(w.fp, '([0-9]+)', 1) AS INT) + 1 span,
    (upper(coalesce(w.fp, '')) RLIKE '^S[0-9]' OR lower(coalesce(w.iss, '')) RLIKE 'sup|abstr|meet|^s[0-9]') meet,
    (w.td = 'conference-abstract' AND w.conf >= 0.8) m_ca, (w.td = 'other' AND w.conf >= 0.8) m_oth,
    (w.td = 'article' AND w.conf >= 0.8) m_art
  FROM w JOIN m ON m.doi = w.d),
f2 AS (
  -- 'About a conference' also sits on full conference papers in proceedings-type journals (Measurement: Sensors, Journal of
  -- Asian Economics ...). An uncertainty guard, not a paper detector: a byline, 3+ pages or an article number, and no
  -- meeting / notice word in the title -> abstain (the work stays article).
  SELECT f.*, coalesce(na > 0 AND (span >= 3 OR fp RLIKE '^[0-9]{6}$')
               AND NOT coalesce(tl, '') RLIKE '(conferen|congres|congrès|meeting|symposi|workshop|report|proceedings|program|abstract|session|forum|society|association|colloqu|summit|seminar|exhibition|council|committee|award|calendar|diary|\\bnews|\\bevents?\\b|bulletin|minutes|preface|opening|closing|welcome|tribute|in memoriam|obituar|journ[ée]es|réunion|kongress|tagung|jornada|reuni[oó]n|congreso|simposio|excursion|foray|field (meeting|trip|day|excursion)|visit|\\btours?\\b)', FALSE) paperlike
  FROM f)
SELECT f2.*,
  CASE
    WHEN NOT same_doi THEN NULL
    WHEN cat = 'Abstract' AND nref <= 5 AND span BETWEEN 1 AND 2 THEN CASE
      WHEN NOT m_oth AND (meet OR m_ca) THEN 'conference-abstract'
      WHEN m_oth THEN 'other'
      WHEN NOT meet AND NOT m_ca AND NOT m_art THEN 'other' END
    WHEN cat = 'Correspondence' AND nref <= 15 AND (span BETWEEN 1 AND 4 OR span IS NULL) AND NOT (span >= 3 AND nref >= 6) THEN 'letter'
    WHEN cat = 'Erratum' AND nref <= 5 AND (span BETWEEN 1 AND 3 OR span IS NULL)
         AND NOT coalesce(tl RLIKE '^(temporary )?removal|removal notice|withdraw|retract', FALSE) THEN 'erratum'
    WHEN cat IN ('Contents list', 'Editorial Board', 'Index') AND na = 0 THEN 'paratext'
    WHEN cat IN ('Announcement', 'About a conference', 'Calendar', 'Product review', 'Patent report', 'Literature alert',
                 'Personal report') AND nref <= 5 AND NOT (cat = 'About a conference' AND paperlike) THEN 'other'
  END new_type,
  CASE WHEN cat = 'Abstract' THEN CASE WHEN meet AND NOT m_oth THEN 'meeting-signal' WHEN m_ca THEN 'model-ca'
                                       WHEN m_oth THEN 'model-other' WHEN m_art THEN 'model-article' ELSE 'unplaced' END END abs_tier
FROM f2
""".replace("SCRATCH", SCRATCH))

# COMMAND ----------

# MAGIC %md ## 2. Rows for keys that have none yet (one per DOI record; a record whose works disagree gets none)

# COMMAND ----------

NEW = SCRATCH + "_new"
spark.sql(f"""
CREATE OR REPLACE TABLE {NEW} AS
SELECT k.provenance, k.native_id, k.type, '{PLATFORM}' AS platform, k.record_key, k.declared_code, {OXJOB} AS oxjob,
       timestamp'{RUN_TS}' AS harvested_at
FROM (
  SELECT prov AS provenance, nid AS native_id, min(new_type) AS type, min(d) AS record_key,
         concat('ScienceDirect citation_article_type: ', min(cat),
                CASE WHEN min(abs_tier) IS NOT NULL THEN concat(' (', min(abs_tier), ')') ELSE '' END) AS declared_code
  FROM {SCRATCH} WHERE prov = 'crossref' AND lower(nid) = d
  -- every work led by the record (and every stored page of its DOI) must give the same non-null type: one abstention or
  -- disagreement and the record gets no row
  GROUP BY prov, nid HAVING count(*) = count(new_type) AND count(DISTINCT new_type) = 1
) k
LEFT ANTI JOIN {RDT} l ON l.provenance = k.provenance AND l.native_id = k.native_id
""")
new_rows = spark.table(NEW).count()
print(f"{RDT}: {new_rows:,} new {PLATFORM} rows")
display(spark.sql(f"SELECT type, declared_code, count(*) AS rows FROM {NEW} GROUP BY ALL ORDER BY rows DESC"))
display(spark.sql(f"""
  SELECT n.type, n.declared_code, s.id, s.tl AS title, s.fp, s.lp, s.iss, s.nref, s.na
  FROM {NEW} n JOIN {SCRATCH} s ON s.prov = n.provenance AND s.nid = n.native_id
  ORDER BY xxhash64(n.native_id, {OXJOB}) LIMIT 30"""))

assert new_rows <= MAX_NEW_ROWS, f"{new_rows:,} new rows > {MAX_NEW_ROWS:,}: read a sample before raising the cap"
dup = spark.sql(f"""
  SELECT count(*) AS n FROM (
    SELECT provenance, native_id FROM {RDT} UNION ALL SELECT provenance, native_id FROM {NEW})
  GROUP BY provenance, native_id HAVING count(*) > 1""").count()
assert dup == 0, f"{dup} repeated keys: CreateWorksBase's LEFT JOIN would repeat those works"

# COMMAND ----------

# MAGIC %md ## 3. Append (never while the End 2 End runs: checked again just before the write)

# COMMAND ----------

if DRY_RUN or not new_rows:
    dbutils.notebook.exit(f"{'dry run' if DRY_RUN else 'nothing to add'}: {new_rows:,} new rows")

write(f"""
INSERT INTO {RDT} (provenance, native_id, type, platform, record_key, declared_code, oxjob, harvested_at)
SELECT n.provenance, n.native_id, n.type, n.platform, n.record_key, n.declared_code, n.oxjob, n.harvested_at
FROM {NEW} n
LEFT ANTI JOIN {RDT} l ON l.provenance = n.provenance AND l.native_id = n.native_id
""")
added = spark.sql(f"SELECT count(*) AS n FROM {RDT} WHERE oxjob = {OXJOB} AND harvested_at = timestamp'{RUN_TS}'").first()["n"]
print(f"added {added:,} rows (harvested_at {RUN_TS}); revert: DELETE FROM {RDT} WHERE oxjob = {OXJOB} AND harvested_at = '{RUN_TS}'")
dbutils.notebook.exit(f"added {added:,} rows at {RUN_TS}")
