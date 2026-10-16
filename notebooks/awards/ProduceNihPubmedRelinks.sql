-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Produce NIH PubMed relinks (nightly, incremental)
-- MAGIC PubMed GrantList strings (`openalex.works.locations_mapped`, provenance pubmed) -> NIH core project -> rows for the registry-mode
-- MAGIC `relink` leg of Nightly Grants. One target: `award_relink_map`, insert-only MERGE, election tag `nih_pubmed_nightly`.
-- MAGIC
-- MAGIC - Key normalization: the 10-05 nih-key build (unchanged), election rules: P5 (chief-eng proofs/p5-nih-key) re-keyed to the NIH parent
-- MAGIC   funder 4320332161 as in NIH-key part B (proofs/p8-nihB-timing). Every kept row elects its intended project record in last night's
-- MAGIC   published election (Nightly scratch `openalex.awards.ngr_*`), is ACTIVE, is not a fold loser, and is not already linked or mapped.
-- MAGIC - Incremental: PubMed rows changed (greatest of `ingested_at`, `openalex_updated_dt`) in (watermark - 14 days, max change time]. Watermark = max(created_date) of this tag in the
-- MAGIC   map (each run stamps its rows with its source cutoff), so a rerun on the same source adds nothing and creates no table version.
-- MAGIC - Not ready (before 2026-10-19 01:00 UTC, fold not bound, item-7 class not declared, nihA/nihB missing): success, nothing written;
-- MAGIC   still not ready from 2026-10-21 01:00 UTC: fail.
-- MAGIC - Fail closed: every guard raises before the MERGE (only the postcondition runs after it); more than `max_rows` (<= 50k) rows stops the run; nothing is ever deleted.

-- COMMAND ----------

-- Parameters: relink_map (the production map, or a ce1016_nihp_ dev copy for proofs), max_rows (per-run fuse).
SELECT assert_true(:relink_map = 'openalex.awards.award_relink_map' OR :relink_map RLIKE '^openalex_dev[.]rohan_lab[.]ce1016_nihp_[a-z0-9_]+$',
    'relink_map must be openalex.awards.award_relink_map or a openalex_dev.rohan_lab.ce1016_nihp_ dev copy'),
  assert_true(try_cast(:max_rows AS INT) BETWEEN 1 AND 50000, 'max_rows must be an integer in 1..50000 (50k is the hard ceiling)')

-- COMMAND ----------

-- The target as a session variable (no parameter markers inside views or the script block below).
DECLARE OR REPLACE VARIABLE nihp_map STRING DEFAULT :relink_map

-- COMMAND ----------

SET TIME ZONE 'UTC'

-- COMMAND ----------

-- Production runs only start in 01:00-02:30 UTC (the job is scheduled 01:30; Nightly Grants binds its inputs at 03:00 UTC).
SELECT CASE WHEN nihp_map = 'openalex.awards.award_relink_map' AND NOT (hour(current_timestamp()) = 1 OR (hour(current_timestamp()) = 2 AND minute(current_timestamp()) < 30))
  THEN raise_error('OUTSIDE_WINDOW: production runs start 01:00-02:30 UTC only; nothing written') ELSE 'window ok' END run_window

-- COMMAND ----------

-- Readiness (not a breach): until every condition holds the run writes nothing and ends as a SUCCESS with one line naming the unmet
-- condition (cell `readiness`); every later guard, the row build and the MERGE are gated on nihp_ready = 'READY'. A production run that
-- is still not ready on or after 2026-10-21 01:00 UTC FAILS, so a dormant job is noticed. Conditions, in order:
--  1. NOT_BEFORE (production only): on or after 2026-10-19 01:00 UTC (Sun 18:00 PDT).
--  2. The NIH yearly fold is complete and correctly bound: each of nih-all-00/01/02 is in the production manifest with exactly the reviewed
--     fold-list count (npy1007r_all_newest_manifest v0, as literals), and every listed loser resolves, in last night's election, either to
--     itself as GONE (at most 39: losers already GONE before the fold) or to its declared target (or that target's own ACTIVE terminal);
--     none is still its own record, missing, or resolving elsewhere; NULL/unknown states fail. Before that, a project's newest-year record
--     can still become a loser and links the fold itself creates would be duplicated (dev 10-16, batch 00 only: 6,603 of 16,625 rows
--     elected a nih-all-01/02 loser, 8,529 duplicated a fold move).
--  3. grants-job's item-7 standing class for this tag is declared (openalex_dev.rohan_lab.pfold_relink_standing_classes, the table its
--     per-night acceptance reads): insert-only, max_works_per_night >= max_rows, and (production) active_from <= now.
--  4. The nihA/nihB relink sets are still in the production map (the 4 election counts at least their 10-16 values).
DECLARE OR REPLACE VARIABLE nihp_ready STRING DEFAULT 'NOT_EVALUATED'

-- COMMAND ----------

SET VAR nihp_ready = (
  SELECT CASE
    WHEN nihp_map = 'openalex.awards.award_relink_map' AND current_timestamp() < TIMESTAMP'2026-10-19 01:00:00 UTC'
      THEN 'NOT_BEFORE: production runs start on or after 2026-10-19 01:00 UTC'
    WHEN coalesce(f.b00, 0) <> 631298 OR coalesce(f.b01, 0) <> 625949 OR coalesce(f.b02, 0) <> 628072
      THEN 'FOLD_INCOMPLETE: manifest rows nih-all-00/01/02 = ' || coalesce(f.b00, 0) || '/' || coalesce(f.b01, 0) || '/' || coalesce(f.b02, 0) || ', expected 631298/625949/628072'
    WHEN coalesce(f.not_valid, 1) <> 0 OR coalesce(f.rows, 0) <> coalesce(f.losers, -1) OR coalesce(f.gone, 999) > 39
      THEN 'FOLD_NOT_BOUND: invalid ' || coalesce(f.not_valid, -1) || ', resolver rows ' || coalesce(f.rows, -1) || ' for ' || coalesce(f.losers, -1) || ' losers, GONE ' || coalesce(f.gone, -1) || ' (<= 39)'
    WHEN coalesce(c.n_ok, 0) = 0
      THEN 'ITEM7_CLASS_NOT_DECLARED: no active insert-only nih_pubmed_nightly row with max_works_per_night >= max_rows in pfold_relink_standing_classes'
    WHEN NOT coalesce(ab.ok, false)
      THEN 'NIHAB_SETS_MISSING: nihA core/ic_serial, nihB core/ic_serial = ' || coalesce(ab.counts, '?') || ', expected >= 445008/370084/946081/474786'
    ELSE 'READY' END
  FROM (SELECT 1 one) base
  LEFT JOIN (SELECT count(*) rows, count(DISTINCT m.loser_id) losers,
    count_if(m.batch_id = 'nih-all-00') b00, count_if(m.batch_id = 'nih-all-01') b01, count_if(m.batch_id = 'nih-all-02') b02,
    -- valid = GONE on itself, or ACTIVE on the declared target / that target's ACTIVE terminal; NULL or anything else is invalid
    count_if(NOT coalesce((x.status = 'GONE' AND x.stable_id = m.loser_id)
      OR (x.status = 'ACTIVE' AND x.stable_id <> m.loser_id AND (x.stable_id = m.target_id OR x.stable_id = tx.stable_id)), false)) not_valid,
    count_if(x.status = 'GONE') gone
  FROM openalex.awards.award_migration_manifest m
  LEFT JOIN openalex.awards.ngr_resolve_candidate x ON x.owner_id = m.loser_id
  LEFT JOIN openalex.awards.ngr_resolve_candidate tx ON tx.owner_id = m.target_id AND tx.status = 'ACTIVE'
  WHERE m.evidence_class = 'NIH_PROJECT_YEAR_FOLD' AND m.batch_id IN ('nih-all-00','nih-all-01','nih-all-02')) f ON true
  LEFT JOIN (SELECT count(*) n_ok FROM openalex_dev.rohan_lab.pfold_relink_standing_classes
    WHERE election = 'nih_pubmed_nightly' AND insert_only AND max_works_per_night >= CAST(:max_rows AS INT)
      AND (nihp_map <> 'openalex.awards.award_relink_map' OR active_from <= current_timestamp())) c ON true
  LEFT JOIN (SELECT count_if(election = 'ce1014_nihA:pubmed_nih_core') >= 445008 AND count_if(election = 'ce1014_nihA:pubmed_nih_ic_serial') >= 370084
      AND count_if(election = 'ce1014_nihB:pubmed_nih_core') >= 946081 AND count_if(election = 'ce1014_nihB:pubmed_nih_ic_serial') >= 474786 ok,
      count_if(election = 'ce1014_nihA:pubmed_nih_core') || '/' || count_if(election = 'ce1014_nihA:pubmed_nih_ic_serial') || '/'
        || count_if(election = 'ce1014_nihB:pubmed_nih_core') || '/' || count_if(election = 'ce1014_nihB:pubmed_nih_ic_serial') counts
    FROM openalex.awards.award_relink_map WHERE election LIKE 'ce1014_nih%') ab ON true)

-- COMMAND ----------

-- The run's one readiness line. Not ready: SUCCESS with nothing written (all later steps skip); production past the deadline: FAIL.
SELECT CASE WHEN nihp_ready <=> 'READY' THEN 'READY: writing allowed if every guard passes'
  WHEN nihp_map = 'openalex.awards.award_relink_map' AND current_timestamp() >= TIMESTAMP'2026-10-21 01:00:00 UTC'
    THEN raise_error('NOT_READY_PAST_DEADLINE (2026-10-21 01:00 UTC): ' || coalesce(nihp_ready, 'NULL') || '; nothing written')
  ELSE 'NOT READY, nothing will be written (success): ' || coalesce(nihp_ready, 'NULL') END readiness

-- COMMAND ----------

-- Last night's election must be complete and consistent: no Nightly run in progress, the served links and the scratch election tables
-- all come from the latest SUCCEEDED run (each table's last data commit inside that run's window). Otherwise skip tonight (fail closed).
SELECT CASE WHEN NOT (nihp_ready <=> 'READY') THEN 'skipped: not ready'
  WHEN n.run_id IS NULL OR n.finished_at IS NULL THEN raise_error('NO_SUCCEEDED_NIGHTLY_RUN')
  WHEN coalesce(n.open_runs, 1) <> 0 THEN raise_error('NIGHTLY_RUN_IN_PROGRESS: Nightly Grants has a run without finished_at')
  WHEN coalesce(n.good_tables, 0) <> 7 THEN raise_error('SCRATCH_NOT_FROM_LATEST_SUCCEEDED_RUN: ' || coalesce(n.bad_list, '?'))
  WHEN NOT coalesce(n.served_ok, false) THEN raise_error('SERVED_LINKS_NOT_FROM_LATEST_SUCCEEDED_RUN')
  WHEN NOT coalesce(n.parent_canon, false) THEN raise_error('NIH_PARENT_NOT_CANONICAL')
  ELSE 'nightly state ok: ' || n.run_id END nightly_state
FROM (SELECT 1 one) base LEFT JOIN (
  WITH run AS (SELECT run_id, started_at, finished_at FROM openalex.awards.award_nightly_runs WHERE status LIKE 'SUCCEEDED%' ORDER BY started_at DESC LIMIT 1),
  h AS (
    SELECT 'ngr_work_key_map' t, max(timestamp) ts FROM (DESCRIBE HISTORY openalex.awards.ngr_work_key_map) WHERE operation NOT IN ('OPTIMIZE','VACUUM START','VACUUM END','COMPUTE STATISTICS')
    UNION ALL SELECT 'ngr_normalization_lookup', max(timestamp) FROM (DESCRIBE HISTORY openalex.awards.ngr_normalization_lookup) WHERE operation NOT IN ('OPTIMIZE','VACUUM START','VACUUM END','COMPUTE STATISTICS')
    UNION ALL SELECT 'ngr_funder_canon', max(timestamp) FROM (DESCRIBE HISTORY openalex.awards.ngr_funder_canon) WHERE operation NOT IN ('OPTIMIZE','VACUUM START','VACUUM END','COMPUTE STATISTICS')
    UNION ALL SELECT 'ngr_resolve_candidate', max(timestamp) FROM (DESCRIBE HISTORY openalex.awards.ngr_resolve_candidate) WHERE operation NOT IN ('OPTIMIZE','VACUUM START','VACUUM END','COMPUTE STATISTICS')
    UNION ALL SELECT 'ngr_awards_candidate', max(timestamp) FROM (DESCRIBE HISTORY openalex.awards.ngr_awards_candidate) WHERE operation NOT IN ('OPTIMIZE','VACUUM START','VACUUM END','COMPUTE STATISTICS')
    UNION ALL SELECT 'ngr_work_survivors', max(timestamp) FROM (DESCRIBE HISTORY openalex.awards.ngr_work_survivors) WHERE operation NOT IN ('OPTIMIZE','VACUUM START','VACUUM END','COMPUTE STATISTICS')
    UNION ALL SELECT 'work_awards', max(timestamp) FROM (DESCRIBE HISTORY openalex.awards.work_awards) WHERE operation NOT IN ('OPTIMIZE','VACUUM START','VACUUM END','COMPUTE STATISTICS'))
  SELECT run.run_id, run.finished_at,
    (SELECT count(*) FROM openalex.awards.award_nightly_runs WHERE finished_at IS NULL) open_runs,
    count(DISTINCT CASE WHEN coalesce(h.ts BETWEEN run.started_at AND run.finished_at, false) THEN h.t END) good_tables,
    concat_ws(',', collect_list(CASE WHEN NOT coalesce(h.ts BETWEEN run.started_at AND run.finished_at, false) THEN h.t END)) bad_list,
    (SELECT collect_set(release_id) FROM openalex.awards.work_awards) = array(run.run_id) served_ok,
    (SELECT count(*) = 1 AND min(canonical_funder_id) = 4320332161 FROM openalex.awards.ngr_funder_canon WHERE funder_id = 4320332161) parent_canon
  FROM run CROSS JOIN h GROUP BY run.run_id, run.finished_at) n ON true

-- COMMAND ----------

-- Watermark (from the target itself; no state table). First run: the P5/nihA/nihB lists used locations_mapped v9157 (10-14 06:00 UTC);
-- the window never starts before 10-13 (older PubMed rows belong to those lists). A row's change time is greatest(ingested_at, openalex_updated_dt) (6% of a week's changed PubMed rows only move
-- openalex_updated_dt). Look back 14 days so rows mapped or changed late are reconsidered; overlap is harmless (mapped/served keys skip).
DECLARE OR REPLACE VARIABLE nihp_wm_from TIMESTAMP

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE nihp_wm_to TIMESTAMP

-- COMMAND ----------

SET VAR nihp_wm_from = (SELECT greatest(coalesce(max(created_date) - INTERVAL 14 DAYS, TIMESTAMP'2026-10-13 00:00:00'), TIMESTAMP'2026-10-13 00:00:00')
  FROM IDENTIFIER(nihp_map) WHERE election = 'nih_pubmed_nightly')

-- COMMAND ----------

SET VAR nihp_wm_to = (SELECT max(greatest(ingested_at, coalesce(openalex_updated_dt, ingested_at))) FROM openalex.works.locations_mapped WHERE provenance = 'pubmed' AND nihp_ready <=> 'READY')

-- COMMAND ----------

SELECT CASE WHEN NOT (nihp_ready <=> 'READY') THEN 'skipped: not ready'
  WHEN nihp_wm_to IS NULL THEN raise_error('SOURCE_EMPTY: no PubMed change time in locations_mapped')
  WHEN nihp_wm_to < (SELECT max(created_date) FROM IDENTIFIER(nihp_map) WHERE election = 'nih_pubmed_nightly') THEN raise_error('SOURCE_WENT_BACKWARDS: max change time below the last stamp')
  WHEN nihp_wm_to < current_timestamp() - INTERVAL 3 DAYS THEN raise_error('SOURCE_STALE: newest PubMed change is more than 3 days old')
  ELSE 'window ' || nihp_wm_from || ' .. ' || nihp_wm_to END source_window

-- COMMAND ----------

-- NIH award keys: the 10-05 nih-key build, verbatim (all ACTIVE awards of the 30 NIH funder ids; core = activity+IC+serial).
CREATE OR REPLACE TEMP VIEW nihp_award_keys AS
SELECT a.id award_id, a.funder_id, a.funder_award_id, a.provenance, e.entity_kind,
  (e.entity_kind <> 'SHELL' AND a.provenance NOT IN ('crossref_work_funders','crossref_work.grants','crossref_work','europepmc_work_funders','datacite_work_funders')) is_supplied,
  CASE WHEN regexp_extract(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(upper(coalesce(a.funder_award_id,'')), '[ \\t\\r\\n\\x0B\\f\\x{00A0}\\x{2000}-\\x{200B}\\x{3000}]+', ' '), '[\\x{2010}-\\x{2015}\\x{2212}]', '-')), '(?<![A-Z0-9])(NHLBI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HL'), '(?<![A-Z0-9])(NIAID)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AI'), '(?<![A-Z0-9])(NIDDK)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DK'), '(?<![A-Z0-9])(NIGMS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'GM'), '(?<![A-Z0-9])(NICHD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HD'), '(?<![A-Z0-9])(NINDS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'NS'), '(?<![A-Z0-9])(NIAAA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AA'), '(?<![A-Z0-9])(NIEHS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'ES'), '(?<![A-Z0-9])(NIDCR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DE'), '(?<![A-Z0-9])(NIDCD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DC'), '(?<![A-Z0-9])(NIAMS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AR'), '(?<![A-Z0-9])(NHGRI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HG'), '(?<![A-Z0-9])(NIBIB)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'EB'), '(?<![A-Z0-9])(NCATS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'TR'), '(?<![A-Z0-9])(NCCIH)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AT'), '(?<![A-Z0-9])(NIMHD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'MD'), '(?<![A-Z0-9])(NIMH)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'MH'), '(?<![A-Z0-9])(NIDA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DA'), '(?<![A-Z0-9])(NINR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'NR'), '(?<![A-Z0-9])(NCRR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'RR'), '(?<![A-Z0-9])(NCI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'CA'), '(?<![A-Z0-9])(NIA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AG'), '(?<![A-Z0-9])(NEI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'EY'), '(?<![A-Z0-9])(NLM)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'LM'), '(?<![A-Z0-9])(FIC)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'TW'), '(?<![A-Z0-9])([A-Z]{2})[ _.-]{1,3}([A-Z][0-9]{2})[ _.-]{1,3}([0-9]{4,6})(?![0-9])', '$2-$1-$3'), '^(?<![A-Z0-9])(?:[1-9][ _.-]{0,3})?[A-Z][A-Z0-9]{2}[ _.-]{0,3}[A-Z]{2}[ _.-]{0,3}(?:[0-9O]{4,6}(?![0-9])|[0-9]{6}(?=[0-9]{2}(?:[AS][0-9]+)*(?![0-9A-Z])))', 0) <> '' THEN concat(regexp_replace(substr(regexp_replace(regexp_replace(regexp_extract(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(upper(coalesce(a.funder_award_id,'')), '[ \\t\\r\\n\\x0B\\f\\x{00A0}\\x{2000}-\\x{200B}\\x{3000}]+', ' '), '[\\x{2010}-\\x{2015}\\x{2212}]', '-')), '(?<![A-Z0-9])(NHLBI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HL'), '(?<![A-Z0-9])(NIAID)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AI'), '(?<![A-Z0-9])(NIDDK)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DK'), '(?<![A-Z0-9])(NIGMS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'GM'), '(?<![A-Z0-9])(NICHD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HD'), '(?<![A-Z0-9])(NINDS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'NS'), '(?<![A-Z0-9])(NIAAA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AA'), '(?<![A-Z0-9])(NIEHS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'ES'), '(?<![A-Z0-9])(NIDCR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DE'), '(?<![A-Z0-9])(NIDCD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DC'), '(?<![A-Z0-9])(NIAMS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AR'), '(?<![A-Z0-9])(NHGRI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HG'), '(?<![A-Z0-9])(NIBIB)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'EB'), '(?<![A-Z0-9])(NCATS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'TR'), '(?<![A-Z0-9])(NCCIH)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AT'), '(?<![A-Z0-9])(NIMHD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'MD'), '(?<![A-Z0-9])(NIMH)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'MH'), '(?<![A-Z0-9])(NIDA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DA'), '(?<![A-Z0-9])(NINR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'NR'), '(?<![A-Z0-9])(NCRR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'RR'), '(?<![A-Z0-9])(NCI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'CA'), '(?<![A-Z0-9])(NIA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AG'), '(?<![A-Z0-9])(NEI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'EY'), '(?<![A-Z0-9])(NLM)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'LM'), '(?<![A-Z0-9])(FIC)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'TW'), '(?<![A-Z0-9])([A-Z]{2})[ _.-]{1,3}([A-Z][0-9]{2})[ _.-]{1,3}([0-9]{4,6})(?![0-9])', '$2-$1-$3'), '^(?<![A-Z0-9])(?:[1-9][ _.-]{0,3})?[A-Z][A-Z0-9]{2}[ _.-]{0,3}[A-Z]{2}[ _.-]{0,3}(?:[0-9O]{4,6}(?![0-9])|[0-9]{6}(?=[0-9]{2}(?:[AS][0-9]+)*(?![0-9A-Z])))', 0), '[ _.-]', ''), '^[1-9]', ''), 1, 3), '^([A-Z])O([0-9])$', '$10$2'), substr(regexp_replace(regexp_replace(regexp_extract(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(upper(coalesce(a.funder_award_id,'')), '[ \\t\\r\\n\\x0B\\f\\x{00A0}\\x{2000}-\\x{200B}\\x{3000}]+', ' '), '[\\x{2010}-\\x{2015}\\x{2212}]', '-')), '(?<![A-Z0-9])(NHLBI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HL'), '(?<![A-Z0-9])(NIAID)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AI'), '(?<![A-Z0-9])(NIDDK)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DK'), '(?<![A-Z0-9])(NIGMS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'GM'), '(?<![A-Z0-9])(NICHD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HD'), '(?<![A-Z0-9])(NINDS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'NS'), '(?<![A-Z0-9])(NIAAA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AA'), '(?<![A-Z0-9])(NIEHS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'ES'), '(?<![A-Z0-9])(NIDCR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DE'), '(?<![A-Z0-9])(NIDCD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DC'), '(?<![A-Z0-9])(NIAMS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AR'), '(?<![A-Z0-9])(NHGRI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HG'), '(?<![A-Z0-9])(NIBIB)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'EB'), '(?<![A-Z0-9])(NCATS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'TR'), '(?<![A-Z0-9])(NCCIH)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AT'), '(?<![A-Z0-9])(NIMHD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'MD'), '(?<![A-Z0-9])(NIMH)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'MH'), '(?<![A-Z0-9])(NIDA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DA'), '(?<![A-Z0-9])(NINR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'NR'), '(?<![A-Z0-9])(NCRR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'RR'), '(?<![A-Z0-9])(NCI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'CA'), '(?<![A-Z0-9])(NIA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AG'), '(?<![A-Z0-9])(NEI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'EY'), '(?<![A-Z0-9])(NLM)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'LM'), '(?<![A-Z0-9])(FIC)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'TW'), '(?<![A-Z0-9])([A-Z]{2})[ _.-]{1,3}([A-Z][0-9]{2})[ _.-]{1,3}([0-9]{4,6})(?![0-9])', '$2-$1-$3'), '^(?<![A-Z0-9])(?:[1-9][ _.-]{0,3})?[A-Z][A-Z0-9]{2}[ _.-]{0,3}[A-Z]{2}[ _.-]{0,3}(?:[0-9O]{4,6}(?![0-9])|[0-9]{6}(?=[0-9]{2}(?:[AS][0-9]+)*(?![0-9A-Z])))', 0), '[ _.-]', ''), '^[1-9]', ''), 4, 2), lpad(replace(substr(regexp_replace(regexp_replace(regexp_extract(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(upper(coalesce(a.funder_award_id,'')), '[ \\t\\r\\n\\x0B\\f\\x{00A0}\\x{2000}-\\x{200B}\\x{3000}]+', ' '), '[\\x{2010}-\\x{2015}\\x{2212}]', '-')), '(?<![A-Z0-9])(NHLBI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HL'), '(?<![A-Z0-9])(NIAID)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AI'), '(?<![A-Z0-9])(NIDDK)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DK'), '(?<![A-Z0-9])(NIGMS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'GM'), '(?<![A-Z0-9])(NICHD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HD'), '(?<![A-Z0-9])(NINDS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'NS'), '(?<![A-Z0-9])(NIAAA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AA'), '(?<![A-Z0-9])(NIEHS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'ES'), '(?<![A-Z0-9])(NIDCR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DE'), '(?<![A-Z0-9])(NIDCD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DC'), '(?<![A-Z0-9])(NIAMS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AR'), '(?<![A-Z0-9])(NHGRI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HG'), '(?<![A-Z0-9])(NIBIB)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'EB'), '(?<![A-Z0-9])(NCATS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'TR'), '(?<![A-Z0-9])(NCCIH)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AT'), '(?<![A-Z0-9])(NIMHD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'MD'), '(?<![A-Z0-9])(NIMH)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'MH'), '(?<![A-Z0-9])(NIDA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DA'), '(?<![A-Z0-9])(NINR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'NR'), '(?<![A-Z0-9])(NCRR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'RR'), '(?<![A-Z0-9])(NCI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'CA'), '(?<![A-Z0-9])(NIA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AG'), '(?<![A-Z0-9])(NEI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'EY'), '(?<![A-Z0-9])(NLM)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'LM'), '(?<![A-Z0-9])(FIC)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'TW'), '(?<![A-Z0-9])([A-Z]{2})[ _.-]{1,3}([A-Z][0-9]{2})[ _.-]{1,3}([0-9]{4,6})(?![0-9])', '$2-$1-$3'), '^(?<![A-Z0-9])(?:[1-9][ _.-]{0,3})?[A-Z][A-Z0-9]{2}[ _.-]{0,3}[A-Z]{2}[ _.-]{0,3}(?:[0-9O]{4,6}(?![0-9])|[0-9]{6}(?=[0-9]{2}(?:[AS][0-9]+)*(?![0-9A-Z])))', 0), '[ _.-]', ''), '^[1-9]', ''), 6), 'O', '0'), 6, '0')) END core,
  a.display_name, a.lead_investigator.given_name pi_given, a.lead_investigator.family_name pi_family, a.start_year, a.end_year
FROM openalex.awards.openalex_awards  a
JOIN openalex.awards.award_entities e ON e.stable_id = a.id AND e.status = 'ACTIVE'
WHERE a.funder_id IN (4320332161,4320337611,4320337356,4320337376,4320337472,4320337358,4320337593,4320337375,4320337355,4320337362,4320337363,4320337364,4320337365,4320337357,4320337361,4320337354,4320337346,4320337359,4320337353,4320337337,4320337330,4320337352,4320337347,4320337534,4320337372,4320337351,4320337374,4320337350,4320337338,4320337348)

-- COMMAND ----------

-- PubMed GrantList keys for the window only: the 10-05 nih-key build, verbatim, plus the change-time window.
CREATE OR REPLACE TEMP VIEW nihp_pubmed_keys AS
WITH g AS (
  SELECT DISTINCT lm.work_id, f.name agency, gid
  FROM openalex.works.locations_mapped  lm
  LATERAL VIEW explode(lm.funders) x AS f
  LATERAL VIEW explode(f.awards) y AS gid
  WHERE lm.provenance = 'pubmed' AND greatest(lm.ingested_at, coalesce(lm.openalex_updated_dt, lm.ingested_at)) > nihp_wm_from AND greatest(lm.ingested_at, coalesce(lm.openalex_updated_dt, lm.ingested_at)) <= nihp_wm_to AND size(lm.funders) > 0 AND lm.work_id IS NOT NULL AND trim(gid) <> ''
    AND (coalesce(f.name,'') RLIKE '(?i)(NIH HHS$|\\bNIH\\b|National Institutes of Health|National (Cancer|Eye|Human Genome Research) Institute|National Library of Medicine|National Heart, Lung|Eunice Kennedy Shriver|Fogarty|National Institute (of|on|for) (Allergy|Child|Diabetes|General Medical|Mental|Neurological|Aging|Drug|Alcohol|Environmental Health|Arthritis|Biomedical|Dental|Deafness|Nursing|Minority)|National Center for (Advancing Translational|Complementary|Research Resources))' AND NOT coalesce(f.name,'') RLIKE '(?i)(Foundation for the National Institutes|Korea|Japan|China|Chinese|CDC|FDA|AHRQ|HRSA|SAMHSA)')
), k AS (SELECT g.*, array_distinct(flatten(transform(transform(split(coalesce(gid,''), '[,;]'), part -> named_struct('p', regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(upper(coalesce(part,'')), '[ \\t\\r\\n\\x0B\\f\\x{00A0}\\x{2000}-\\x{200B}\\x{3000}]+', ' '), '[\\x{2010}-\\x{2015}\\x{2212}]', '-')), '(?<![A-Z0-9])(NHLBI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HL'), '(?<![A-Z0-9])(NIAID)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AI'), '(?<![A-Z0-9])(NIDDK)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DK'), '(?<![A-Z0-9])(NIGMS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'GM'), '(?<![A-Z0-9])(NICHD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HD'), '(?<![A-Z0-9])(NINDS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'NS'), '(?<![A-Z0-9])(NIAAA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AA'), '(?<![A-Z0-9])(NIEHS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'ES'), '(?<![A-Z0-9])(NIDCR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DE'), '(?<![A-Z0-9])(NIDCD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DC'), '(?<![A-Z0-9])(NIAMS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AR'), '(?<![A-Z0-9])(NHGRI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'HG'), '(?<![A-Z0-9])(NIBIB)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'EB'), '(?<![A-Z0-9])(NCATS)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'TR'), '(?<![A-Z0-9])(NCCIH)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AT'), '(?<![A-Z0-9])(NIMHD)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'MD'), '(?<![A-Z0-9])(NIMH)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'MH'), '(?<![A-Z0-9])(NIDA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'DA'), '(?<![A-Z0-9])(NINR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'NR'), '(?<![A-Z0-9])(NCRR)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'RR'), '(?<![A-Z0-9])(NCI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'CA'), '(?<![A-Z0-9])(NIA)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'AG'), '(?<![A-Z0-9])(NEI)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'EY'), '(?<![A-Z0-9])(NLM)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'LM'), '(?<![A-Z0-9])(FIC)(?=[ _.-]{0,3}[0-9]{4,6}(?![0-9]))', 'TW'), '(?<![A-Z0-9])([A-Z]{2})[ _.-]{1,3}([A-Z][0-9]{2})[ _.-]{1,3}([0-9]{4,6})(?![0-9])', '$2-$1-$3'))), x -> CASE WHEN size(regexp_extract_all(x.p, '(?<![A-Z0-9])(?:[1-9][ _.-]{0,3})?[A-Z][A-Z0-9]{2}[ _.-]{0,3}[A-Z]{2}[ _.-]{0,3}(?:[0-9O]{4,6}(?![0-9])|[0-9]{6}(?=[0-9]{2}(?:[AS][0-9]+)*(?![0-9A-Z])))', 0)) > 0 THEN transform(regexp_extract_all(x.p, '(?<![A-Z0-9])(?:[1-9][ _.-]{0,3})?[A-Z][A-Z0-9]{2}[ _.-]{0,3}[A-Z]{2}[ _.-]{0,3}(?:[0-9O]{4,6}(?![0-9])|[0-9]{6}(?=[0-9]{2}(?:[AS][0-9]+)*(?![0-9A-Z])))', 0), m -> named_struct('key', concat(regexp_replace(substr(regexp_replace(regexp_replace(m, '[ _.-]', ''), '^[1-9]', ''), 1, 3), '^([A-Z])O([0-9])$', '$10$2'), substr(regexp_replace(regexp_replace(m, '[ _.-]', ''), '^[1-9]', ''), 4, 2), lpad(replace(substr(regexp_replace(regexp_replace(m, '[ _.-]', ''), '^[1-9]', ''), 6), 'O', '0'), 6, '0')), 'kind', 'core')) ELSE transform(regexp_extract_all(x.p, '(?<![A-Z0-9])[A-Z]{2}[ _.-]{0,3}[0-9O]{4,6}(?![0-9])', 0), m -> named_struct('key', concat(substr(regexp_replace(m, '[ _.-]', ''), 1, 2), lpad(replace(substr(regexp_replace(m, '[ _.-]', ''), 3), 'O', '0'), 6, '0')), 'kind', 'ic_serial')) END))) ks, (regexp_replace(upper(coalesce(gid,'')), '[ _.-]', '') RLIKE '^[1-9]?(?![A-Z]O[0-9])[A-Z][A-Z0-9]{2}[A-Z]{2}[0-9]{6}([0-9]{2}([AS][0-9]+)*)?$') is_clean FROM g)
SELECT k.work_id, k.agency, k.gid, z.key, z.kind, k.is_clean, size(k.ks) keys_in_string,
  w.publication_year, coalesce(w.is_xpac, false) is_xpac, w.type,
  (w.publication_year >= 2015 AND NOT coalesce(w.is_xpac, false)) recent_main
FROM k JOIN openalex.works.openalex_works w ON w.id = k.work_id
LATERAL VIEW explode_outer(k.ks) e AS z

-- COMMAND ----------

-- Fold list in production (every NIH_PROJECT_YEAR_FOLD batch loaded so far): core -> project survivor.
CREATE OR REPLACE TEMP VIEW nihp_fold_map AS
SELECT loser_id, target_id, batch_id, get_json_object(evidence_json, '$.core') core
FROM openalex.awards.award_migration_manifest WHERE evidence_class = 'NIH_PROJECT_YEAR_FOLD'

-- COMMAND ----------

-- P5 candidates (proofs/p5-nih-key/candidates.sql.tmpl), for the window's papers only, against the served links of the latest run.
-- Unchanged: project election, ic_serial uniqueness, fold keying, in_prod / fold_creates / core_linked exclusions, date guard, 1990 rule.
-- Dropped: the move/add label (shell_keys), which nothing downstream reads.
CREATE OR REPLACE TEMP VIEW nihp_candidates AS
WITH sup AS (SELECT * FROM nihp_award_keys WHERE is_supplied AND core IS NOT NULL),
core_stats AS (
  SELECT core, count(*) n_records, min(start_year) first_year, max(end_year) last_year,
    max_by(award_id, named_struct('ns', NOT (funder_award_id RLIKE '^3' OR funder_award_id RLIKE '(?i)s[0-9]+$'),
      'n', display_name IS NOT NULL, 'e', coalesce(end_year, -1), 'i', -award_id)) elected_award_id
  FROM sup GROUP BY core),
ic_index AS (SELECT substr(core, 4) ic_serial, min(core) core FROM core_stats GROUP BY 1 HAVING count(*) = 1),
ct AS (SELECT core, min(target_id) target_id FROM nihp_fold_map GROUP BY core),
tgt AS (
  SELECT cs.*, coalesce(ct.target_id, cs.elected_award_id) target_award_id, ct.target_id IS NOT NULL in_fold
  FROM core_stats cs LEFT JOIN ct ON ct.core = cs.core),
pm AS (
  SELECT p.*, coalesce(s.survivor_work_id, p.work_id) wid
  FROM nihp_pubmed_keys p
  LEFT JOIN openalex.awards.ngr_work_survivors s ON s.loser_work_id = p.work_id
  WHERE p.key IS NOT NULL),
pairs AS (
  SELECT p.wid work_id, p.gid, p.publication_year, t.core, 'core' route
  FROM pm p JOIN tgt t ON p.kind = 'core' AND t.core = p.key
  UNION ALL
  SELECT p.wid, p.gid, p.publication_year, t.core, 'ic_serial'
  FROM pm p JOIN ic_index i ON p.kind = 'ic_serial' AND i.ic_serial = p.key JOIN tgt t ON t.core = i.core),
links AS (SELECT DISTINCT w.work_id, CAST(regexp_extract(w.award.id, 'G([0-9]+)$', 1) AS BIGINT) award_id
  FROM openalex.awards.work_awards w LEFT SEMI JOIN (SELECT DISTINCT work_id FROM pairs) x ON x.work_id = w.work_id),
plinks AS (SELECT l.work_id, l.award_id, fm.target_id post_award_id FROM links l JOIN nihp_fold_map fm ON fm.loser_id = l.award_id),
linked_cores AS (SELECT DISTINCT l.work_id, a.core FROM links l JOIN nihp_award_keys a ON a.award_id = l.award_id WHERE a.is_supplied AND a.core IS NOT NULL),
agg AS (
  SELECT work_id, core, min(CASE WHEN route = 'core' THEN 0 ELSE 1 END) route_rank, max(publication_year) py,
    slice(array_sort(collect_set(gid)), 1, 3) gids
  FROM pairs GROUP BY 1, 2),
j AS (
  SELECT a.*, CASE WHEN a.route_rank = 0 THEN 'core' ELSE 'ic_serial' END route, t.first_year, t.target_award_id, t.in_fold,
    ex.work_id IS NOT NULL in_prod_exact, fl.work_id IS NOT NULL fold_creates, lc.work_id IS NOT NULL core_linked
  FROM agg a JOIN tgt t ON t.core = a.core
  LEFT JOIN links ex ON ex.work_id = a.work_id AND ex.award_id = t.target_award_id
  LEFT JOIN (SELECT DISTINCT work_id, post_award_id FROM plinks) fl ON fl.work_id = a.work_id AND fl.post_award_id = t.target_award_id
  LEFT JOIN linked_cores lc ON lc.work_id = a.work_id AND lc.core = a.core)
SELECT j.*,
  (NOT in_prod_exact AND NOT fold_creates AND NOT core_linked AND (py IS NULL OR first_year IS NULL OR py >= first_year - 1) AND (route = 'core' OR py >= 1990)) net_new
FROM j

-- COMMAND ----------

-- Relink rows (P5 ship-input shape, re-keyed to the NIH parent as part B) and last night's election of each (verbatim work_awards.py
-- relink leg: funder_canon -> normalization_lookup, gap rows normalized as the runtime does -> generic election, then sharp).
CREATE OR REPLACE TEMP VIEW nihp_check AS
WITH r AS (
  SELECT c.work_id, CAST(4320332161 AS BIGINT) src_funder_id, c.gids[0] src_award_id, CAST(4320332161 AS BIGINT) target_funder_id,
    a.funder_award_id target_award_id, CAST(1 AS BIGINT) n_registry_awards, CAST(c.first_year AS BIGINT) target_start_year,
    c.route, c.target_award_id intended_id
  FROM nihp_candidates c JOIN nihp_award_keys a ON a.award_id = c.target_award_id
  WHERE c.net_new AND a.funder_award_id IS NOT NULL),
w AS (
  SELECT r.*, f.canonical_funder_id funder_id,
    CASE WHEN length(regexp_replace(lower(lower(r.target_award_id)), '[^a-z0-9]', '')) >= 4 THEN regexp_replace(lower(lower(r.target_award_id)), '[^a-z0-9]', '') ELSE lower(trim(lower(r.target_award_id))) END nk,
    CASE WHEN n.award_key IS NOT NULL THEN CASE WHEN n.sharp_eligible THEN n.sharp_key END
      ELSE openalex.awards.award_norm_key(f.canonical_funder_id, lower(r.target_award_id), 'registry') END sk
  FROM r LEFT JOIN openalex.awards.ngr_funder_canon f ON f.funder_id = r.target_funder_id
  LEFT JOIN openalex.awards.ngr_normalization_lookup n
    ON n.funder_id <=> f.canonical_funder_id AND n.award_key <=> lower(r.target_award_id) AND n.is_direct = true),
m AS (
  SELECT w.*, coalesce(g.stable_id, s.stable_id) elected_id
  FROM w LEFT JOIN openalex.awards.ngr_work_key_map g ON g.regime = 'g' AND g.funder_id = w.funder_id AND g.nk = w.nk
  LEFT JOIN openalex.awards.ngr_work_key_map s ON s.regime = 's' AND s.funder_id = w.funder_id AND s.nk = w.sk),
fold_target AS (SELECT loser_id, max(target_id) target_id FROM nihp_fold_map GROUP BY 1 HAVING count(DISTINCT target_id) = 1)
SELECT m.*, coalesce(sv.survivor_work_id, m.work_id) served_work_id,
  coalesce(ft.target_id, (SELECT max(x.stable_id) FROM openalex.awards.ngr_resolve_candidate x WHERE x.owner_id = m.intended_id AND x.status = 'ACTIVE')) intended_terminal,
  EXISTS (SELECT 1 FROM openalex.awards.ngr_awards_candidate d WHERE d.id = m.elected_id) has_document,
  EXISTS (SELECT 1 FROM openalex.awards.ngr_resolve_candidate x WHERE x.owner_id = m.elected_id AND x.stable_id = m.elected_id AND x.status = 'ACTIVE') active,
  EXISTS (SELECT 1 FROM openalex.awards.award_migration_manifest l WHERE l.loser_id = m.elected_id) is_loser,
  EXISTS (SELECT 1 FROM openalex.awards.work_awards s WHERE s.work_id = coalesce(sv.survivor_work_id, m.work_id)
    AND s.award.id = concat('https://openalex.org/G', m.elected_id)) already_served,
  EXISTS (SELECT 1 FROM openalex.works.openalex_works v WHERE v.id = coalesce(sv.survivor_work_id, m.work_id)) work_served
FROM m LEFT JOIN openalex.awards.ngr_work_survivors sv ON sv.loser_work_id = m.work_id
LEFT JOIN fold_target ft ON ft.loser_id = m.intended_id

-- COMMAND ----------

-- Keep (build_nihB.py rule): elects its intended record, ACTIVE with a document, not a fold loser, not already linked, work served,
-- not already in the map, and no other row on the same (work, number) wants a different record. One row per map key.
-- One pass fills both the rows (at most max_rows + 1, so the fuse can see an oversize night) and the run's funnel counts.
DECLARE OR REPLACE VARIABLE nihp_rows ARRAY<STRUCT<work_id BIGINT, src_funder_id BIGINT, src_award_id STRING, target_funder_id BIGINT,
  target_award_id STRING, n_registry_awards BIGINT, target_start_year BIGINT, route STRING, intended_id BIGINT, intended_terminal BIGINT,
  elected_id BIGINT, served_work_id BIGINT>>

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE nihp_funnel STRUCT<candidates BIGINT, elects BIGINT, elects_intended BIGINT, already_served BIGINT, in_map BIGINT,
  elects_loser BIGINT, work_not_served BIGINT, kept BIGINT, kept_core BIGINT, kept_ic_serial BIGINT>

-- COMMAND ----------

SET VAR (nihp_rows, nihp_funnel) = (
  WITH c AS (
    SELECT c.*, bool_and(c.intended_terminal <=> c.elected_id) OVER (PARTITION BY c.work_id, c.target_funder_id, lower(c.target_award_id)) key_ok,
      x.work_id IS NOT NULL in_map
    FROM nihp_check c
    LEFT JOIN (SELECT DISTINCT work_id, target_funder_id, target_award_id FROM IDENTIFIER(nihp_map)) x
      ON x.work_id = c.work_id AND x.target_funder_id = c.target_funder_id AND x.target_award_id = c.target_award_id),
  k AS (
    SELECT c.*, (key_ok AND elected_id IS NOT NULL AND elected_id = intended_terminal AND has_document AND active AND NOT is_loser
      AND NOT already_served AND work_served AND NOT in_map AND work_id IS NOT NULL AND target_award_id IS NOT NULL) keep FROM c),
  d AS (
    SELECT k.*, keep AND row_number() OVER (PARTITION BY work_id, target_funder_id, target_award_id
      ORDER BY keep DESC, route, src_award_id, target_start_year) = 1 kept FROM k),
  o AS (
    SELECT d.*, CASE WHEN kept THEN row_number() OVER (PARTITION BY kept ORDER BY work_id, target_award_id, src_award_id) END ord FROM d)
  SELECT coalesce(collect_list(struct(work_id, src_funder_id, src_award_id, target_funder_id, target_award_id, n_registry_awards, target_start_year,
      route, intended_id, intended_terminal, elected_id, served_work_id)) FILTER (WHERE ord <= CAST(:max_rows AS INT) + 1), array()),
    named_struct('candidates', count(*), 'elects', count_if(elected_id IS NOT NULL), 'elects_intended', count_if(elected_id = intended_terminal),
      'already_served', count_if(already_served), 'in_map', count_if(in_map), 'elects_loser', count_if(is_loser),
      'work_not_served', count_if(NOT work_served), 'kept', count_if(kept), 'kept_core', count_if(kept AND route = 'core'),
      'kept_ic_serial', count_if(kept AND route = 'ic_serial'))
  FROM o WHERE nihp_ready <=> 'READY')

-- COMMAND ----------

-- Run report (job output).
SELECT nihp_funnel.candidates, nihp_funnel.elects, nihp_funnel.elects_intended, nihp_funnel.already_served, nihp_funnel.in_map,
  nihp_funnel.elects_loser, nihp_funnel.work_not_served, nihp_funnel.kept, nihp_funnel.kept_core, nihp_funnel.kept_ic_serial,
  size(nihp_rows) rows_held, nihp_wm_from wm_from, nihp_wm_to wm_to

-- COMMAND ----------

-- Guards on exactly the rows the MERGE will insert (fail closed; nothing has been written yet).
SELECT CASE WHEN NOT (nihp_ready <=> 'READY') THEN 'skipped: not ready'
  WHEN nihp_funnel.kept > CAST(:max_rows AS INT) OR size(nihp_rows) > CAST(:max_rows AS INT) THEN raise_error('FUSE: ' || nihp_funnel.kept || ' rows tonight, more than ' || :max_rows || '; nothing written. Review the night (50k is the hard ceiling)')
  WHEN g.dup_keys > 0 THEN raise_error('DUPLICATE_KEYS: ' || g.dup_keys)
  WHEN g.bad_funder > 0 THEN raise_error('NOT_NIH_PARENT_ROWS: ' || g.bad_funder)
  WHEN g.not_active > 0 THEN raise_error('TARGET_NOT_ACTIVE: ' || g.not_active)
  WHEN g.fold_losers > 0 THEN raise_error('TARGET_IS_FOLD_LOSER: ' || g.fold_losers)
  WHEN g.not_intended > 0 THEN raise_error('NOT_ELECTING_INTENDED: ' || g.not_intended)
  WHEN g.served > 0 THEN raise_error('ALREADY_SERVED: ' || g.served)
  WHEN g.mapped > 0 THEN raise_error('ALREADY_IN_MAP: ' || g.mapped)
  ELSE 'guards ok: ' || size(nihp_rows) || ' rows' END guards
FROM (
  WITH f AS (SELECT r.* FROM (SELECT explode(nihp_rows) r))
  SELECT
    (SELECT count(*) - count(DISTINCT work_id, target_funder_id, target_award_id) FROM f) dup_keys,
    (SELECT count_if(NOT (src_funder_id <=> 4320332161 AND target_funder_id <=> 4320332161) OR work_id IS NULL OR target_award_id IS NULL) FROM f) bad_funder,
    (SELECT count(*) FROM f LEFT ANTI JOIN openalex.awards.ngr_resolve_candidate x ON x.owner_id = f.elected_id AND x.stable_id = f.elected_id AND x.status = 'ACTIVE') not_active,
    (SELECT count(*) FROM (SELECT elected_id id FROM f UNION ALL SELECT intended_terminal FROM f) z
       LEFT SEMI JOIN openalex.awards.award_migration_manifest l ON l.loser_id = z.id) fold_losers,
    (SELECT count_if(NOT (elected_id <=> intended_terminal) OR elected_id IS NULL) FROM f) not_intended,
    (SELECT count(*) FROM f LEFT SEMI JOIN openalex.awards.work_awards s ON s.work_id = f.served_work_id AND s.award.id = concat('https://openalex.org/G', f.elected_id)) served,
    (SELECT count(*) FROM f LEFT SEMI JOIN IDENTIFIER(nihp_map) x ON x.work_id = f.work_id AND x.target_funder_id = f.target_funder_id
       AND x.target_award_id = f.target_award_id) mapped) g

-- COMMAND ----------

-- The one write: insert-only, keyed on (work_id, target_funder_id, target_award_id). Skipped when there is nothing to add (no empty commit).
-- Right before writing: the Nightly has not started and (production) it is still before 02:40 UTC; else stop with nothing written.
BEGIN
  IF nihp_ready <=> 'READY' AND size(nihp_rows) > 0 AND (EXISTS (SELECT 1 FROM openalex.awards.award_nightly_runs WHERE finished_at IS NULL)
      OR (nihp_map = 'openalex.awards.award_relink_map' AND NOT (hour(current_timestamp()) = 1 OR (hour(current_timestamp()) = 2 AND minute(current_timestamp()) < 40)))) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'LATE_OR_NIGHTLY_STARTED: nothing written';
  END IF;
  IF nihp_ready <=> 'READY' AND size(nihp_rows) > 0 THEN
    MERGE INTO IDENTIFIER(nihp_map) t
    USING (SELECT r.work_id, r.src_funder_id, r.src_award_id, r.target_funder_id, r.target_award_id, r.n_registry_awards, r.target_start_year,
             'nih_pubmed_nightly' election, nihp_wm_to created_date FROM (SELECT explode(nihp_rows) r)) s
    ON t.work_id = s.work_id AND t.target_funder_id = s.target_funder_id AND t.target_award_id = s.target_award_id
    WHEN NOT MATCHED THEN INSERT (work_id, src_funder_id, src_award_id, target_funder_id, target_award_id, n_registry_awards, target_start_year, election, created_date)
      VALUES (s.work_id, s.src_funder_id, s.src_award_id, s.target_funder_id, s.target_award_id, s.n_registry_awards, s.target_start_year, s.election, s.created_date);
  END IF;
END

-- COMMAND ----------

-- Postcondition (after the commit): every row is in the map under this run's tag and stamp. A failure here means the MERGE committed;
-- read the map back by tag + stamp before any retry.
SELECT CASE WHEN NOT (nihp_ready <=> 'READY') THEN 'skipped: not ready, nothing written'
  WHEN p.landed <> size(nihp_rows) THEN raise_error('POSTCONDITION: ' || p.landed || ' of ' || size(nihp_rows) || ' rows landed')
  ELSE 'landed ' || p.landed END postcondition
FROM (SELECT count(*) landed FROM (SELECT explode(nihp_rows) r) f JOIN IDENTIFIER(nihp_map) x
  ON x.work_id = f.r.work_id AND x.target_funder_id = f.r.target_funder_id AND x.target_award_id = f.r.target_award_id
  AND x.election = 'nih_pubmed_nightly' AND x.created_date = nihp_wm_to) p
