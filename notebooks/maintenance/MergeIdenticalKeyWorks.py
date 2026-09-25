# Databricks notebook source
# MAGIC %md
# MAGIC # MergeIdenticalKeyWorks — collapse works that share a byte-identical `title_author` key (oxjob #1256 step four)
# MAGIC
# MAGIC The identical-key defect class: a `title_author` key (> 20 chars) carried by the pinned records of 2–3
# MAGIC works where at most ONE of them carries a DOI. The keyless side(s) minted as fresh works because the
# MAGIC map did not hold the key for the DOI-bearing work (leak closed 2026-09-22, walden `3631b877`; stock
# MAGIC 3,985,433 keys / 4,084,352 excess works on 2026-09-24, 86 % uncited, 77 % repo-only). The two-DOI
# MAGIC groups are #880's policy class (versioned DOIs, same-titled items) and are NOT in scope.
# MAGIC
# MAGIC **Winner** = the DOI-bearing work, else the lowest id. **Losers** = the rest. The merge is the
# MAGIC registries' sanctioned correction path (`RepointWorkIds` header): DELETE the loser's registry pins and
# MAGIC its `work_id_map` rows, then let the nightly `MapWorkIds` re-resolve the loser's records — keyless, so
# MAGIC the title tier binds them to the winner (after the delete the key's only id is the winner). The loser
# MAGIC ends the night with no pins → `TrackDeletedWorks` ledgers it (404, deleted_ids.csv, ES delete). The
# MAGIC morning after, `repoint_citations` moves `work_references.cited_work_id` loser → winner by id pair.
# MAGIC
# MAGIC **Rule (from a 1,570-pair blind-labelled sample, 2026-09-24; precision weighted by stratum size):** a pair
# MAGIC merges when the two records agree on a known publication year (tier 1: full normalized titles identical,
# MAGIC ~100 %; tier 2: titles differ beyond the key but share >= `title_jaccard_min` of their tokens, ~99 %), volume /
# MAGIC first page do not contradict, and none of the failure shapes the sample found applies: same primary source
# MAGIC (letters and serial items sharing a page: 77 % same), preprint / conference / thesis / report vs article
# MAGIC (a version), a book vs its chapter or a review of it, a junk-typed side, two undated books. Tier 3 (a year
# MAGIC missing, 94 %) and tier 4 (years one apart, 84 %, mostly preprints becoming articles) are staged but off by
# MAGIC default (`tiers`). No size or citation caps: every cited loser is listed in `<target>_cited_review` for the
# MAGIC record; `chained` (the loser is a winner of another key) is a mechanical hold for the second pass.
# MAGIC
# MAGIC **Waves** are assigned per KEY (all losers of a key in one wave, so MIN(id) over the key is the winner
# MAGIC the next morning), uncited keys first, `wave_size` losers per wave. Nightly budget: TrackDeletedWorks
# MAGIC refuses > 0.5 % of live works deleted (~2.38M on 2026-09-24) and Guardrails > 7.5M stamps (losers +
# MAGIC winners + normal churn) → default 1,500,000.
# MAGIC
# MAGIC Modes (all keyed on `target_table`, one row per loser):
# MAGIC - `stage`     build the class from `locations_mapped` + `openalex_works`, apply holds, assign waves.
# MAGIC - `dry_run`   `wave = N`: losers, keys, winners, pins + map rows to delete, cited losers; samples.
# MAGIC - `execute`   `confirm = yes`, `wave = N`, outside 04:00-08:00 UTC, never while End 2 End runs: freeze
# MAGIC               `<target>_wave<N>_audit` (the pins and map rows deleted, in full), the two DELETEs, stamp.
# MAGIC               Undo: re-INSERT the audit rows (pins + map rows) — the nightly has not run yet.
# MAGIC - `verify`    the morning after: every audited pin re-pinned; landed on the winner / elsewhere / NULL;
# MAGIC               losers with 0 pins; losers ledgered.
# MAGIC - `repoint_citations`  `wave = N`, `confirm = yes`, after `verify` is clean: `<target>_wave<N>_refs_audit`
# MAGIC               (before-image, both sides) then UPDATE `work_references`: `cited_work_id` loser → winner (citations
# MAGIC               TO the loser) and `citing_work_id` loser → winner (the loser's OWN reference list comes along; rows keep
# MAGIC               their location + ref_ind so they never collide with the winner's).

# COMMAND ----------

dbutils.widgets.dropdown("mode", "stage", ["stage", "dry_run", "execute", "verify", "repoint_citations", "reexecute_resurrected", "record_merges"])
dbutils.widgets.text("target_table", "openalex.works.oxjob1256_identical_key_merge_target")
dbutils.widgets.text("wave_size", "1500000")
dbutils.widgets.text("wave", "1")
dbutils.widgets.dropdown("class_mode", "title_key", ["title_key", "same_doi"])
dbutils.widgets.text("tiers", "1,2")
dbutils.widgets.text("title_jaccard_min", "0.9")
dbutils.widgets.text("abstract_jaccard_min", "0.6")
dbutils.widgets.text("preprint_is_same", "no")
dbutils.widgets.text("prior_targets", "")
dbutils.widgets.text("confirm", "no")

MODE = dbutils.widgets.get("mode")
TARGET = dbutils.widgets.get("target_table")
WAVE_SIZE = int(dbutils.widgets.get("wave_size"))
WAVE = int(dbutils.widgets.get("wave"))
CLASS_MODE = dbutils.widgets.get("class_mode")
TIERS = {int(t) for t in dbutils.widgets.get("tiers").split(",") if t.strip()}
TITLE_JACCARD_MIN = float(dbutils.widgets.get("title_jaccard_min"))
ABSTRACT_JACCARD_MIN = float(dbutils.widgets.get("abstract_jaccard_min"))
PREPRINT_IS_SAME = dbutils.widgets.get("preprint_is_same") == "yes"
# earlier targets whose executed losers must not be staged again (locations_mapped still shows them until the nightly rebuild)
PRIOR_TARGETS = [t.strip() for t in dbutils.widgets.get("prior_targets").split(",") if t.strip()]
CONFIRM = dbutils.widgets.get("confirm") == "yes"
AUDIT = f"{TARGET}_wave{WAVE}_audit"
REFS_AUDIT = f"{TARGET}_wave{WAVE}_refs_audit"
REVIEW = f"{TARGET}_cited_review"

LM = "openalex.works.locations_mapped"
REGISTRY = "openalex.works.location_work_ids"
MAP = "openalex.works.work_id_map"
WORKS = "openalex.works.openalex_works"
REFS = "openalex.works.work_references"
LEDGER = "openalex.works.deleted_works"
END2END_JOB_ID = 616701029470182
MERGED = "openalex.works.merged_work_ids"   # durable loser -> winner record; MapWorkIds redirects legacy adoption through it

import datetime, json, time

SUMMARY = {"mode": MODE, "wave": WAVE}


def note(**kw):
    """Serverless notebook tasks return no stdout through the API; everything printed is also returned via notebook.exit."""
    SUMMARY.update(kw)
    print(kw)

print(dict(mode=MODE, class_mode=CLASS_MODE, target=TARGET, wave_size=WAVE_SIZE, wave=WAVE, tiers=sorted(TIERS), title_jaccard_min=TITLE_JACCARD_MIN,
           abstract_jaccard_min=ABSTRACT_JACCARD_MIN, preprint_is_same=PREPRINT_IS_SAME, prior_targets=PRIOR_TARGETS, confirm=CONFIRM))


def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def one(sql):
    return rows(sql)[0]


def end2end_active():
    from databricks.sdk import WorkspaceClient
    return [r.run_id for r in WorkspaceClient().jobs.list_runs(job_id=END2END_JOB_ID, active_only=True)]


def class_sql():
    """One row per loser with its winner, the evidence features, the tier, and the hold (2026-09-24 labelled sample, oxjob #1256)."""
    if CLASS_MODE == "same_doi":
        return same_doi_class_sql()
    return f"""
    WITH k AS (
      SELECT merge_key.title_author AS ta, work_id,
             MAX(NULLIF(merge_key.doi, '') IS NOT NULL) AS has_doi,
             MAX(CASE WHEN provenance NOT IN ('repo', 'repo_backfill') THEN 1 ELSE 0 END) = 0 AS repo_only,
             COUNT(*) AS n_locations
      FROM {LM}
      WHERE work_id IS NOT NULL AND merge_key.title_author IS NOT NULL AND LENGTH(merge_key.title_author) > 20
        {prior_exclusion()}
      GROUP BY 1, 2
    ),
    g AS (
      SELECT ta, COUNT(*) AS n_ids, SUM(CASE WHEN has_doi THEN 1 ELSE 0 END) AS n_doi
      FROM k GROUP BY ta HAVING COUNT(*) BETWEEN 2 AND 3
    ),
    w AS (
      SELECT id, publication_year AS yr, type, title,
             regexp_replace(lower(title), '[^a-z0-9]', '') AS title_norm,
             array_distinct(filter(split(regexp_replace(lower(title), '[^a-z0-9 ]', ' '), ' +'), x -> x <> '')) AS title_tokens,
             primary_location.source.id AS src, biblio.volume AS vol, biblio.first_page AS fp,
             COALESCE(cited_by_count, 0) AS cites,
             array_distinct(filter(split(regexp_replace(lower(COALESCE(abstract, '')), '[^a-z0-9 ]', ' '), ' +'), x -> length(x) > 3)) AS abs_tokens
      FROM {WORKS}
    ),
    sides AS (
      SELECT k.ta, g.n_ids, g.n_doi, k.work_id, k.has_doi, k.repo_only, k.n_locations, w.yr, w.type, w.title_norm, w.title_tokens, w.src, w.vol, w.fp, w.cites, w.abs_tokens,
             ROW_NUMBER() OVER (PARTITION BY k.ta ORDER BY k.has_doi DESC, k.work_id) AS rn
      FROM g JOIN k ON k.ta = g.ta
      LEFT JOIN w ON w.id = k.work_id
      WHERE g.n_doi <= 1
    ),
    winners AS (SELECT * FROM sides WHERE rn = 1),
    pairs AS (
      SELECT s.ta, s.n_ids, s.n_doi, x.work_id AS winner_work_id, x.yr AS winner_yr, x.has_doi AS winner_has_doi, x.type AS winner_type,
             s.work_id AS loser_work_id, s.yr AS loser_yr, s.cites AS loser_cites, s.repo_only AS loser_repo_only,
             s.n_locations AS loser_locations, LENGTH(s.ta) AS key_len, s.type AS loser_type,
             CASE WHEN s.yr IS NULL OR x.yr IS NULL THEN 'unknown' WHEN s.yr = x.yr THEN 'same'
                  WHEN ABS(s.yr - x.yr) = 1 THEN 'pm1' ELSE 'gt1' END AS year_cls,
             (s.title_norm = x.title_norm) AS full_title_same,
             CASE WHEN size(array_union(s.title_tokens, x.title_tokens)) = 0 THEN 0.0
                  ELSE size(array_intersect(s.title_tokens, x.title_tokens)) / size(array_union(s.title_tokens, x.title_tokens)) END AS title_jaccard,
             (s.vol IS NOT NULL AND x.vol IS NOT NULL AND (s.vol <> x.vol OR (s.fp IS NOT NULL AND x.fp IS NOT NULL AND s.fp <> x.fp))) AS biblio_differs,
             (s.src IS NOT NULL AND s.src = x.src) AS src_same,
             CASE WHEN size(s.abs_tokens) >= 20 AND size(x.abs_tokens) >= 20
                  THEN size(array_intersect(s.abs_tokens, x.abs_tokens)) / size(array_union(s.abs_tokens, x.abs_tokens)) END AS abs_jaccard
      FROM sides s JOIN winners x ON x.ta = s.ta WHERE s.rn > 1
    ),
    tiered AS (
      SELECT p.*,
             CASE WHEN p.biblio_differs OR p.year_cls = 'gt1' THEN NULL
                  WHEN p.year_cls = 'same' AND p.full_title_same THEN 1
                  WHEN p.year_cls = 'same' THEN 2
                  WHEN p.year_cls = 'unknown' THEN 3
                  WHEN p.year_cls = 'pm1' THEN 4 END AS tier,
             -- the failure shapes the labelled sample found: same venue (letters / serial items on one page),
             -- preprint-or-conference-vs-article (a version, not a duplicate), a book vs its chapter or a review of it
             CASE WHEN p.loser_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other')
                    OR p.winner_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other') THEN 'junk_type'
                  WHEN (p.loser_type, p.winner_type) IN (('preprint', 'article'), ('article', 'preprint'), ('conference-paper', 'article'), ('article', 'conference-paper'),
                                                         ('dissertation', 'article'), ('article', 'dissertation'), ('report', 'article'), ('article', 'report')) THEN 'version_types'
                  WHEN (p.loser_type = 'book' AND p.winner_type IN ('book-chapter', 'article')) OR (p.winner_type = 'book' AND p.loser_type IN ('book-chapter', 'article')) THEN 'book_vs_part'
                  WHEN p.src_same THEN 'same_source'
                  WHEN p.loser_type = 'book' AND p.winner_type = 'book' AND p.year_cls = 'unknown' THEN 'undated_books' END AS exclusion
      FROM pairs p
    ),
    held AS (
      SELECT t.*,
             CASE WHEN t.exclusion IS NOT NULL THEN t.exclusion
                  WHEN t.tier IS NULL THEN CASE WHEN t.biblio_differs THEN 'biblio_differs' ELSE 'year_gap' END
                  WHEN t.tier NOT IN ({', '.join(str(x) for x in sorted(TIERS)) or 'NULL'}) THEN CONCAT('tier_', t.tier, '_off')
                  WHEN t.tier = 2 AND t.title_jaccard < {TITLE_JACCARD_MIN} THEN 'title_too_different'
                  END AS base_hold
      FROM tiered t
    )
    -- abstract rescue (2026-09-24 labelled sample, 1,103 held pairs): where both records carry an abstract and the
    -- token sets agree (Jaccard >= abstract_jaccard_min) the pair is the same work in every bucket below (42/42 each);
    -- version-type and +-1-year pairs are the same MANUSCRIPT or its preprint, so they join only under preprint_is_same
    SELECT h.*,
           CASE WHEN h.base_hold IN ('same_source', 'year_gap', 'tier_3_off', 'title_too_different') AND h.abs_jaccard >= {ABSTRACT_JACCARD_MIN} THEN 'abstract'
                WHEN h.base_hold IN ('version_types', 'tier_4_off') AND h.abs_jaccard >= {ABSTRACT_JACCARD_MIN} AND {'TRUE' if PREPRINT_IS_SAME else 'FALSE'} THEN 'abstract_preprint'
                END AS rescue,
           CASE WHEN h.base_hold IN ('same_source', 'year_gap', 'tier_3_off', 'title_too_different') AND h.abs_jaccard >= {ABSTRACT_JACCARD_MIN} THEN NULL
                WHEN h.base_hold IN ('version_types', 'tier_4_off') AND h.abs_jaccard >= {ABSTRACT_JACCARD_MIN} AND {'TRUE' if PREPRINT_IS_SAME else 'FALSE'} THEN NULL
                WHEN h.base_hold IS NOT NULL THEN h.base_hold
                WHEN EXISTS (SELECT 1 FROM winners x WHERE x.work_id = h.loser_work_id) THEN 'chained'
                END AS hold_reason
    FROM held h
    """


def same_doi_class_sql():
    """Same-DOI class: 2-3 live works whose pinned records carry one cleaned DOI. Winner = the work with a Crossref pin
    for it, else the lowest id (the DOI seed already made MIN(id) over the DOI the anchor). The loser's records re-resolve
    on the DOI tier. Tier 1: titles agree (Jaccard >= title_jaccard_min); tier 2: titles differ but abstracts agree. Held:
    year gap > 1 (`year_gap`), titles differ with disagreeing abstracts (`doi_misassigned`: the Spiegelhalter-discussion
    shape) or with no abstract (`title_differs_no_abstract`); junk / book-vs-part types as in the title class."""
    clean = "regexp_replace({c}, '[^a-zA-Z0-9\\./-]', '')"
    return f"""
    WITH k AS (
      SELECT lower(NULLIF({clean.format(c='merge_key.doi')}, '')) AS ta, work_id,
             MAX(CASE WHEN provenance = 'crossref' THEN 1 ELSE 0 END) = 1 AS has_doi,
             MAX(CASE WHEN provenance NOT IN ('repo', 'repo_backfill') THEN 1 ELSE 0 END) = 0 AS repo_only,
             COUNT(*) AS n_locations
      FROM {LM}
      WHERE work_id IS NOT NULL AND NULLIF(merge_key.doi, '') IS NOT NULL
        {prior_exclusion()}
      GROUP BY 1, 2
    ),
    live AS (SELECT DISTINCT work_id FROM {REGISTRY} WHERE work_id IS NOT NULL),
    kl AS (SELECT k.* FROM k JOIN live l ON l.work_id = k.work_id),
    g AS (SELECT ta, COUNT(*) AS n_ids, SUM(CASE WHEN has_doi THEN 1 ELSE 0 END) AS n_doi FROM kl GROUP BY ta HAVING COUNT(*) BETWEEN 2 AND 3),
    w AS (
      SELECT id, publication_year AS yr, type, title,
             regexp_replace(lower(title), '[^a-z0-9]', '') AS title_norm,
             array_distinct(filter(split(regexp_replace(lower(title), '[^a-z0-9 ]', ' '), ' +'), x -> x <> '')) AS title_tokens,
             primary_location.source.id AS src, biblio.volume AS vol, biblio.first_page AS fp,
             COALESCE(cited_by_count, 0) AS cites,
             array_distinct(filter(split(regexp_replace(lower(COALESCE(abstract, '')), '[^a-z0-9 ]', ' '), ' +'), x -> length(x) > 3)) AS abs_tokens
      FROM {WORKS}
    ),
    sides AS (
      SELECT kl.ta, g.n_ids, g.n_doi, kl.work_id, kl.has_doi, kl.repo_only, kl.n_locations, w.yr, w.type, w.title_norm, w.title_tokens, w.src, w.vol, w.fp, w.cites, w.abs_tokens,
             ROW_NUMBER() OVER (PARTITION BY kl.ta ORDER BY kl.has_doi DESC, kl.work_id) AS rn
      FROM g JOIN kl ON kl.ta = g.ta LEFT JOIN w ON w.id = kl.work_id
    ),
    winners AS (SELECT * FROM sides WHERE rn = 1),
    pairs AS (
      SELECT s.ta, s.n_ids, s.n_doi, x.work_id AS winner_work_id, x.yr AS winner_yr, x.has_doi AS winner_has_doi, x.type AS winner_type,
             s.work_id AS loser_work_id, s.yr AS loser_yr, s.cites AS loser_cites, s.repo_only AS loser_repo_only,
             s.n_locations AS loser_locations, LENGTH(s.ta) AS key_len, s.type AS loser_type,
             CASE WHEN s.yr IS NULL OR x.yr IS NULL THEN 'unknown' WHEN s.yr = x.yr THEN 'same'
                  WHEN ABS(s.yr - x.yr) = 1 THEN 'pm1' ELSE 'gt1' END AS year_cls,
             (s.title_norm = x.title_norm) AS full_title_same,
             CASE WHEN size(array_union(s.title_tokens, x.title_tokens)) = 0 THEN 0.0
                  ELSE size(array_intersect(s.title_tokens, x.title_tokens)) / size(array_union(s.title_tokens, x.title_tokens)) END AS title_jaccard,
             (s.vol IS NOT NULL AND x.vol IS NOT NULL AND (s.vol <> x.vol OR (s.fp IS NOT NULL AND x.fp IS NOT NULL AND s.fp <> x.fp))) AS biblio_differs,
             (s.src IS NOT NULL AND s.src = x.src) AS src_same,
             CASE WHEN size(s.abs_tokens) >= 20 AND size(x.abs_tokens) >= 20
                  THEN size(array_intersect(s.abs_tokens, x.abs_tokens)) / size(array_union(s.abs_tokens, x.abs_tokens)) END AS abs_jaccard
      FROM sides s JOIN winners x ON x.ta = s.ta WHERE s.rn > 1
    ),
    tiered AS (
      SELECT p.*,
             CASE WHEN p.year_cls = 'gt1' THEN NULL
                  WHEN p.title_jaccard >= {TITLE_JACCARD_MIN} THEN 1
                  WHEN p.abs_jaccard >= {ABSTRACT_JACCARD_MIN} THEN 2 END AS tier,
             CASE WHEN p.loser_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other')
                    OR p.winner_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other') THEN 'junk_type'
                  WHEN (p.loser_type = 'book' AND p.winner_type IN ('book-chapter', 'article')) OR (p.winner_type = 'book' AND p.loser_type IN ('book-chapter', 'article')) THEN 'book_vs_part' END AS exclusion
      FROM pairs p
    ),
    held AS (
      SELECT t.*,
             CASE WHEN t.exclusion IS NOT NULL THEN t.exclusion
                  WHEN t.year_cls = 'gt1' THEN 'year_gap'
                  WHEN t.tier IS NULL THEN CASE WHEN t.abs_jaccard IS NOT NULL THEN 'doi_misassigned' ELSE 'title_differs_no_abstract' END
                  WHEN t.tier NOT IN ({', '.join(str(x) for x in sorted(TIERS)) or 'NULL'}) THEN CONCAT('tier_', t.tier, '_off')
                  END AS base_hold
      FROM tiered t
    )
    SELECT h.*, CAST(NULL AS STRING) AS rescue,
           CASE WHEN h.base_hold IS NOT NULL THEN h.base_hold
                WHEN EXISTS (SELECT 1 FROM winners x WHERE x.work_id = h.loser_work_id) THEN 'chained'
                END AS hold_reason
    FROM held h
    """


def prior_exclusion():
    """anti-join every prior target's executed losers (and their winners' losers) so a re-stage never re-lists them"""
    return " ".join(f"AND NOT EXISTS (SELECT 1 FROM {t} p WHERE p.executed_at IS NOT NULL AND p.loser_work_id = work_id)" for t in PRIOR_TARGETS)


def wave_pred():
    return f"t.wave = {WAVE} AND t.hold_reason IS NULL AND t.executed_at IS NULL"


def print_waves(table):
    for r in rows(f"""SELECT wave, hold_reason, COUNT(*) AS losers, COUNT(DISTINCT ta) AS keys, COUNT(DISTINCT winner_work_id) AS winners,
                             SUM(loser_locations) AS locations, SUM(CASE WHEN loser_cites > 0 THEN 1 ELSE 0 END) AS cited_losers,
                             SUM(loser_cites) AS cites, SUM(CASE WHEN executed_at IS NOT NULL THEN 1 ELSE 0 END) AS executed
                      FROM {table} GROUP BY 1, 2 ORDER BY 1 NULLS LAST, 2 NULLS FIRST"""):
        print(r)

# COMMAND ----------

if MODE == "stage":
    t0 = time.time()
    spark.sql(f"""CREATE OR REPLACE TABLE {TARGET} AS
                  WITH c AS ({class_sql()}),
                  keys AS (
                    SELECT ta, MAX(CASE WHEN hold_reason IS NOT NULL THEN 1 ELSE 0 END) AS held,
                           MAX(loser_cites) AS max_cites, COUNT(*) AS n_losers
                    FROM c GROUP BY ta),
                  ordered AS (
                    -- uncited keys first, then by citations; all losers of a key share a wave; held keys get no wave
                    SELECT ta, held, SUM(n_losers) OVER (ORDER BY max_cites, ta ROWS UNBOUNDED PRECEDING) AS cum_losers
                    FROM keys WHERE held = 0)
                  SELECT c.*, CASE WHEN o.ta IS NOT NULL THEN CAST(CEIL(o.cum_losers / {WAVE_SIZE}) AS INT) END AS wave,
                         current_timestamp() AS staged_at, CAST(NULL AS TIMESTAMP) AS executed_at
                  FROM c LEFT JOIN ordered o ON o.ta = c.ta""")
    spark.sql(f"ALTER TABLE {TARGET} CLUSTER BY (wave, ta)")
    spark.sql(f"""CREATE OR REPLACE TABLE {REVIEW} AS
                  SELECT t.wave, t.hold_reason, t.loser_work_id, t.loser_cites, t.winner_work_id, t.ta,
                         LEFT(wl.title, 120) AS loser_title, LEFT(ww.title, 120) AS winner_title, t.loser_yr, t.winner_yr
                  FROM {TARGET} t LEFT JOIN {WORKS} wl ON wl.id = t.loser_work_id LEFT JOIN {WORKS} ww ON ww.id = t.winner_work_id
                  WHERE t.loser_cites > 0 ORDER BY t.loser_cites DESC""")
    note(staged_seconds=int(time.time() - t0), target=TARGET, cited_review=REVIEW,
         totals=one(f"""SELECT COUNT(*) AS losers, COUNT(DISTINCT ta) AS keys, SUM(CASE WHEN hold_reason IS NULL THEN 1 ELSE 0 END) AS executable,
                               MAX(wave) AS waves FROM {TARGET}"""),
         holds=rows(f"SELECT hold_reason, COUNT(*) AS losers FROM {TARGET} WHERE hold_reason IS NOT NULL GROUP BY 1 ORDER BY 2 DESC"),
         waves=rows(f"SELECT wave, COUNT(*) AS losers, SUM(loser_cites) AS cites FROM {TARGET} WHERE wave IS NOT NULL GROUP BY 1 ORDER BY 1"),
         rescued=rows(f"SELECT rescue, base_hold, COUNT(*) AS losers FROM {TARGET} WHERE rescue IS NOT NULL GROUP BY 1, 2 ORDER BY 3 DESC"))
    print_waves(TARGET)

# COMMAND ----------

if MODE in ("dry_run", "execute"):
    plan = one(f"""SELECT COUNT(*) AS losers, COUNT(DISTINCT t.ta) AS keys, COUNT(DISTINCT t.winner_work_id) AS winners,
                          SUM(t.loser_locations) AS locations, SUM(CASE WHEN t.loser_cites > 0 THEN 1 ELSE 0 END) AS cited_losers,
                          SUM(t.loser_cites) AS cites_to_move, MAX(t.loser_cites) AS max_cites
                   FROM {TARGET} t WHERE {wave_pred()}""")
    dels = one(f"""SELECT (SELECT COUNT(*) FROM {REGISTRY} r WHERE EXISTS (SELECT 1 FROM {TARGET} t WHERE {wave_pred()} AND t.loser_work_id = r.work_id)) AS pins_to_delete,
                          (SELECT COUNT(*) FROM {MAP} m WHERE EXISTS (SELECT 1 FROM {TARGET} t WHERE {wave_pred()} AND t.loser_work_id = m.id)) AS map_rows_to_delete""")
    note(**plan, **dels)
    print_waves(TARGET)
    if plan["losers"] == 0:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": f"nothing to do for wave {WAVE}"}, default=str))
    print("sample merges:")
    for r in rows(f"""SELECT t.loser_work_id, LEFT(wl.title, 50) AS loser_title, t.loser_yr, t.loser_cites, t.loser_repo_only,
                             t.winner_work_id, LEFT(ww.title, 50) AS winner_title, t.winner_yr, t.winner_has_doi
                      FROM {TARGET} t LEFT JOIN {WORKS} wl ON wl.id = t.loser_work_id LEFT JOIN {WORKS} ww ON ww.id = t.winner_work_id
                      WHERE {wave_pred()} ORDER BY RAND(1) LIMIT 10"""):
        print("  ", r)

# COMMAND ----------

if MODE == "execute":
    hour = datetime.datetime.utcnow().hour
    if 4 <= hour < 8:
        raise Exception("MapWorkIds writes the registries 05:00-07:00 UTC; run execute outside 04:00-08:00 UTC")
    active = end2end_active()
    if active:
        raise Exception(f"Walden End 2 End is running (runs {active}); wait for it to finish")
    if not CONFIRM:
        dbutils.notebook.exit("dry run only: pass confirm=yes to execute")
    if spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} exists: wave {WAVE} was already executed")

    t0 = time.time()
    spark.sql(f"""CREATE TABLE {AUDIT} AS
                  SELECT 'pin' AS kind, t.loser_work_id, t.winner_work_id, t.ta,
                         r.provenance, r.native_id_namespace, r.native_id, r.work_id_source, r.openalex_created_dt, r.openalex_updated_dt, r.seeded_dt, r.seeded_from,
                         CAST(NULL AS STRING) AS doi, CAST(NULL AS STRING) AS pmid, CAST(NULL AS STRING) AS arxiv, CAST(NULL AS STRING) AS title_author,
                         CAST(NULL AS DATE) AS created_date, CAST(NULL AS TIMESTAMP) AS updated_date, current_timestamp() AS audited_at
                  FROM {TARGET} t JOIN {REGISTRY} r ON r.work_id = t.loser_work_id WHERE {wave_pred()}
                  UNION ALL
                  SELECT 'map', t.loser_work_id, t.winner_work_id, t.ta,
                         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                         m.doi, m.pmid, m.arxiv, m.title_author, m.created_date, m.updated_date, current_timestamp()
                  FROM {TARGET} t JOIN {MAP} m ON m.id = t.loser_work_id WHERE {wave_pred()}""")
    # a loser under two keys is audited once per key; the DELETEs are by pin / id, so compare DISTINCT
    n = one(f"""SELECT COUNT(DISTINCT CASE WHEN kind = 'pin' THEN CONCAT_WS('|', provenance, native_id_namespace, native_id) END) AS pins,
                       COUNT(DISTINCT CASE WHEN kind = 'map' THEN CONCAT_WS('|', loser_work_id, doi, pmid, arxiv, title_author, created_date) END) AS map_rows
                FROM {AUDIT}""")
    pins = spark.sql(f"""DELETE FROM {REGISTRY} r WHERE EXISTS (SELECT 1 FROM {AUDIT} a WHERE a.kind = 'pin'
                         AND a.provenance = r.provenance AND a.native_id_namespace = r.native_id_namespace AND a.native_id = r.native_id)""").collect()[0].num_affected_rows
    maprows = spark.sql(f"""DELETE FROM {MAP} m WHERE EXISTS (SELECT 1 FROM {AUDIT} a WHERE a.kind = 'map' AND a.loser_work_id = m.id)""").collect()[0].num_affected_rows
    spark.sql(f"UPDATE {TARGET} t SET executed_at = current_timestamp() WHERE {wave_pred()}")
    record_merges(f"{TARGET} t WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL")
    note(executed_seconds=int(time.time() - t0), audit=AUDIT, audited_pins=n["pins"], audited_map_rows=n["map_rows"],
         pins_deleted=pins, map_rows_deleted=maprows)
    assert pins == n["pins"], f"pins deleted {pins} != audited {n['pins']}"
    print_waves(TARGET)

def record_merges(scope_sql):
    """Write (loser -> winner) into the durable merged_work_ids table (idempotent). MapWorkIds' legacy adoption
    (mag id / PMH crosswalk) redirects through it, so a merged loser can never be resurrected by its own legacy
    record (35,035 came back that way on 2026-09-25 before this existed)."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {MERGED} (loser_work_id BIGINT NOT NULL, winner_work_id BIGINT NOT NULL,
                  merged_at TIMESTAMP NOT NULL, source STRING) CLUSTER BY (loser_work_id)""")
    n = spark.sql(f"""MERGE INTO {MERGED} m
                      USING (SELECT DISTINCT t.loser_work_id, t.winner_work_id, MIN(t.executed_at) AS merged_at, '{TARGET}' AS source
                             FROM {scope_sql} GROUP BY t.loser_work_id, t.winner_work_id) s
                        ON m.loser_work_id = s.loser_work_id AND m.winner_work_id = s.winner_work_id
                      WHEN NOT MATCHED THEN INSERT *""").collect()[0].num_inserted_rows
    note(merged_work_ids_recorded=n)
    return n

# COMMAND ----------

if MODE == "record_merges":
    # backfill the durable table from every executed row of this target (all waves)
    record_merges(f"{TARGET} t WHERE t.executed_at IS NOT NULL")
    note(merged_rows_total=one(f"SELECT COUNT(*) AS n FROM {MERGED}")["n"])

# COMMAND ----------

if MODE == "reexecute_resurrected":
    # executed losers that hold registry pins again (legacy adoption re-pinned their own records before the
    # MapWorkIds redirect existed): audit + delete their pins and map rows once more so the nightly re-resolves
    # them through the redirect onto the winner. `wave` selects the wave; `confirm = yes` executes.
    hour = datetime.datetime.utcnow().hour
    if 4 <= hour < 8:
        raise Exception("run outside 04:00-08:00 UTC")
    active = end2end_active()
    if active:
        raise Exception(f"Walden End 2 End is running (runs {active}); wait for it to finish")
    REAUDIT = f"{TARGET}_wave{WAVE}_reexec_audit"
    back = f"""(SELECT DISTINCT t.loser_work_id, t.winner_work_id, t.ta FROM {TARGET} t
                WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL
                  AND EXISTS (SELECT 1 FROM {REGISTRY} r WHERE r.work_id = t.loser_work_id))"""
    plan = one(f"""SELECT COUNT(*) AS resurrected_losers,
                          (SELECT COUNT(*) FROM {REGISTRY} r WHERE EXISTS (SELECT 1 FROM {back} b WHERE b.loser_work_id = r.work_id)) AS pins_to_delete,
                          (SELECT COUNT(*) FROM {MAP} m WHERE EXISTS (SELECT 1 FROM {back} b WHERE b.loser_work_id = m.id)) AS map_rows_to_delete
                   FROM {back} b""")
    note(**plan)
    print("how the resurrected records were bound:")
    for r in rows(f"""SELECT r.work_id_source, r.provenance, COUNT(*) AS pins FROM {REGISTRY} r
                      WHERE EXISTS (SELECT 1 FROM {back} b WHERE b.loser_work_id = r.work_id) GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 8"""):
        print("  ", r)
    if plan["resurrected_losers"] == 0:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": "nothing resurrected"}, default=str))
    if not CONFIRM:
        dbutils.notebook.exit("dry run only: pass confirm=yes to re-execute")
    if spark.catalog.tableExists(REAUDIT):
        raise Exception(f"{REAUDIT} exists: already re-executed once; drop it deliberately to run again")
    record_merges(f"{TARGET} t WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL")
    spark.sql(f"""CREATE TABLE {REAUDIT} AS
                  SELECT 'pin' AS kind, b.loser_work_id, b.winner_work_id, b.ta,
                         r.provenance, r.native_id_namespace, r.native_id, r.work_id_source, r.openalex_created_dt, r.openalex_updated_dt, r.seeded_dt, r.seeded_from,
                         CAST(NULL AS STRING) AS doi, CAST(NULL AS STRING) AS pmid, CAST(NULL AS STRING) AS arxiv, CAST(NULL AS STRING) AS title_author,
                         CAST(NULL AS DATE) AS created_date, CAST(NULL AS TIMESTAMP) AS updated_date, current_timestamp() AS audited_at
                  FROM {back} b JOIN {REGISTRY} r ON r.work_id = b.loser_work_id
                  UNION ALL
                  SELECT 'map', b.loser_work_id, b.winner_work_id, b.ta, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                         m.doi, m.pmid, m.arxiv, m.title_author, m.created_date, m.updated_date, current_timestamp()
                  FROM {back} b JOIN {MAP} m ON m.id = b.loser_work_id""")
    pins = spark.sql(f"""DELETE FROM {REGISTRY} r WHERE EXISTS (SELECT 1 FROM {REAUDIT} a WHERE a.kind = 'pin'
                         AND a.provenance = r.provenance AND a.native_id_namespace = r.native_id_namespace AND a.native_id = r.native_id)""").collect()[0].num_affected_rows
    maprows = spark.sql(f"DELETE FROM {MAP} m WHERE EXISTS (SELECT 1 FROM {REAUDIT} a WHERE a.kind = 'map' AND a.loser_work_id = m.id)").collect()[0].num_affected_rows
    note(reexec_audit=REAUDIT, pins_deleted=pins, map_rows_deleted=maprows)

# COMMAND ----------

if MODE == "verify":
    if not spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} does not exist: wave {WAVE} was not executed")
    note(landed=rows(f"""
        SELECT CASE WHEN r.work_id IS NULL THEN 'not re-pinned yet / NULL'
                    WHEN r.work_id = a.winner_work_id THEN 'on the winner'
                    WHEN r.work_id = a.loser_work_id THEN 'BACK ON THE LOSER'
                    ELSE 'elsewhere' END AS landed, COUNT(*) AS pins
        FROM {AUDIT} a LEFT JOIN {REGISTRY} r ON r.provenance = a.provenance AND r.native_id_namespace = a.native_id_namespace AND r.native_id = a.native_id
        WHERE a.kind = 'pin' GROUP BY 1 ORDER BY 2 DESC"""))
    note(losers=one(f"""
        SELECT COUNT(DISTINCT t.loser_work_id) AS executed_losers,
               COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM {REGISTRY} p WHERE p.work_id = t.loser_work_id) THEN t.loser_work_id END) AS losers_still_pinned,
               COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM {LEDGER} d WHERE d.work_id = t.loser_work_id) THEN t.loser_work_id END) AS losers_ledgered
        FROM {TARGET} t WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL"""))
    print("elsewhere samples (a loser record that re-resolved to a third work):")
    for r in rows(f"""SELECT a.provenance, a.native_id, a.loser_work_id, a.winner_work_id, r.work_id AS landed_on, r.work_id_source
                      FROM {AUDIT} a JOIN {REGISTRY} r ON r.provenance = a.provenance AND r.native_id_namespace = a.native_id_namespace AND r.native_id = a.native_id
                      WHERE a.kind = 'pin' AND r.work_id IS NOT NULL AND r.work_id NOT IN (a.winner_work_id, a.loser_work_id) LIMIT 8"""):
        print("  ", r)

# COMMAND ----------

if MODE == "repoint_citations":
    if not spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} does not exist: wave {WAVE} was not executed")
    if spark.catalog.tableExists(REFS_AUDIT):
        raise Exception(f"{REFS_AUDIT} exists: citations for wave {WAVE} were already repointed")
    # losers that hold pins again (resurrected before the MapWorkIds redirect) are left for the re-execute pass
    pairs = f"""(SELECT DISTINCT loser_work_id, winner_work_id FROM {TARGET} t WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL
                 AND NOT EXISTS (SELECT 1 FROM {REGISTRY} r WHERE r.work_id = t.loser_work_id))"""
    still = one(f"""SELECT COUNT(DISTINCT t.loser_work_id) AS n FROM {TARGET} t WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL
                    AND EXISTS (SELECT 1 FROM {REGISTRY} r WHERE r.work_id = t.loser_work_id)""")["n"]
    plan = one(f"""SELECT COUNT(*) AS edges, COUNT(DISTINCT r.citing_work_id) AS citing_works, COUNT(DISTINCT r.cited_work_id) AS losers_cited
                   FROM {REFS} r JOIN {pairs} p ON r.cited_work_id = p.loser_work_id""")
    note(losers_still_pinned_and_skipped=still, **plan)
    if not CONFIRM:
        dbutils.notebook.exit("dry run only: pass confirm=yes to repoint citations")
    own = one(f"""SELECT COUNT(*) AS loser_reference_rows, COUNT(DISTINCT r.citing_work_id) AS losers_with_references
                  FROM {REFS} r JOIN {pairs} p ON r.citing_work_id = p.loser_work_id""")
    note(**own)
    # before-image of both directions: rows that CITE a loser (cited side) and the loser's OWN reference list (citing side)
    spark.sql(f"""CREATE TABLE {REFS_AUDIT} AS
                  SELECT 'cited' AS side, r.citing_work_id, r.native_id, r.native_id_namespace, r.ref_ind,
                         r.cited_work_id AS old_id, p.winner_work_id AS new_id, current_timestamp() AS audited_at
                  FROM {REFS} r JOIN {pairs} p ON r.cited_work_id = p.loser_work_id
                  UNION ALL
                  SELECT 'citing', r.citing_work_id, r.native_id, r.native_id_namespace, r.ref_ind,
                         r.citing_work_id, p.winner_work_id, current_timestamp()
                  FROM {REFS} r JOIN {pairs} p ON r.citing_work_id = p.loser_work_id""")
    moved = spark.sql(f"""MERGE INTO {REFS} r USING {pairs} p ON r.cited_work_id = p.loser_work_id
                          WHEN MATCHED THEN UPDATE SET r.cited_work_id = p.winner_work_id, r.updated_timestamp = current_timestamp()""").collect()[0].num_affected_rows
    # the loser's own reference list comes along: rows keep their (native_id, ref_ind) so they cannot collide with the winner's
    carried = spark.sql(f"""MERGE INTO {REFS} r USING {pairs} p ON r.citing_work_id = p.loser_work_id
                            WHEN MATCHED THEN UPDATE SET r.citing_work_id = p.winner_work_id, r.updated_timestamp = current_timestamp()""").collect()[0].num_affected_rows
    note(refs_audit=REFS_AUDIT, edges_repointed=moved, loser_reference_rows_carried=carried)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(SUMMARY, default=str))
