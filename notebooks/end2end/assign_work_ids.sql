-- Map_Work_Ids 3/3, assign_work_ids (oxjob #764, #1668): the night's answers in __VERDICTS__ become pins. The only writer of
-- __REG__ (location_work_ids) and __MAP__ (work_id_map) in the nightly. Resolve-before-mint: every map row the first pass saw
-- predates this run, and new map rows are born carrying their verdict (an explicit id, or a generated one for a new work).
-- Map model: a row is the set of identifiers one record carried, bound to one `id`; a work may hold many rows and an identifier
-- may sit on several works (`n_ids`). Writes are insert-only plus the enrichment MERGEs, which only fill empty slots.
-- Registry discipline: insert-only; pinned anchors are never re-resolved (TrackDeletedWorks purges the identity rows of works
-- ledgered as deleted, so a dead id never resolves again).
CREATE TABLE IF NOT EXISTS __RUN_STATS__ (
  run_ts TIMESTAMP, run_dt DATE, anchors_new_resolved BIGINT, anchors_new_null BIGINT, anchors_retried_resolved BIGINT,
  anchors_retried_null BIGINT, src_doi BIGINT, src_pmid BIGINT, src_arxiv BIGINT, src_title_author BIGINT, src_mag_legacy BIGINT,
  src_pmh_legacy BIGINT, minted_range BIGINT, distinct_works BIGINT, max_work_fanin BIGINT, top_fanin_work_id BIGINT,
  registry_null_backlog BIGINT, mints_displaced BIGINT, bridge_conflicts BIGINT, converged_alias BIGINT,
  src_location_matcher BIGINT, src_new_work BIGINT, src_merged_winner BIGINT)
;
-- The location matcher's answers (oxjob #1668) for the first-sight records it decided: an existing work (followed through any
-- merge since the run's works snapshot, and only while it still holds a pin) or a new work. Its reasons are in __DECISIONS__.
CREATE OR REPLACE TABLE __VERDICTS__
CLUSTER BY (provenance, native_id_namespace, native_id)
AS
WITH m AS (SELECT loser_work_id, max_by(winner_work_id, merged_at) AS w FROM __MERGED__ GROUP BY loser_work_id),
d AS (
  SELECT d.provenance, d.native_id_namespace, d.native_id,
    CASE WHEN d.model_work IS NOT NULL THEN COALESCE(m3.w, m2.w, m1.w, d.model_work) END AS t
  FROM __DECISIONS__ d
  LEFT JOIN m m1 ON m1.loser_work_id = d.model_work
  LEFT JOIN m m2 ON m2.loser_work_id = m1.w
  LEFT JOIN m m3 ON m3.loser_work_id = m2.w
  WHERE d.night = DATE'__NIGHT__' AND d.mode = '__MODE__'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d.provenance, d.native_id_namespace, d.native_id ORDER BY d.decided_at DESC) = 1
),
live AS (
  SELECT DISTINCT r.work_id FROM __REG__ r LEFT SEMI JOIN (SELECT DISTINCT t FROM d WHERE t IS NOT NULL) x ON x.t = r.work_id
),
dl AS (SELECT d.provenance, d.native_id_namespace, d.native_id, l.work_id AS t FROM d LEFT JOIN live l ON l.work_id = d.t)
SELECT v.provenance, v.native_id_namespace, v.native_id, v.is_retry, v.first_sight,
       v.doi, v.doi_clean, v.pmid, v.arxiv, v.title_author, v.bad_title, v.has_version_doi_clean, v.version_of_doi_clean,
       v.preprint_link_dois, v.preprint_link_is_pre, v.established_conflict, v.title_doi_conflict, v.mintable, v.routing_key_unseen,
       v.title_pick,
       CASE WHEN dl.native_id IS NOT NULL AND v.first_sight AND v.work_id IS NULL THEN dl.t ELSE v.work_id END AS work_id,
       CASE WHEN dl.native_id IS NOT NULL AND v.first_sight AND v.work_id IS NULL
            THEN CASE WHEN dl.t IS NOT NULL THEN 'location_matcher' END
            ELSE v.work_id_source END AS work_id_source,
       (dl.native_id IS NOT NULL AND v.first_sight AND v.work_id IS NULL) AS decided_by_model,
       v.converged_alias, v.run_ts
FROM __VERDICTS__ v
LEFT JOIN dl ON dl.provenance = v.provenance AND dl.native_id_namespace = v.native_id_namespace AND dl.native_id = v.native_id
;
-- Map inserts: rows born with their verdict. Each insert skips the keys an earlier slice carries, so a key enters the map once
-- per run. 1. Aliases: resolved records (identifier, title tier, location matcher) whose strongest key is new: the key is born
-- pointing at the right work.
INSERT INTO __MAP__
  (id, doi, pmid, arxiv, title_author, created_date, updated_date)
SELECT s.work_id, s.doi, s.pmid, s.arxiv, s.title_author,
       current_date(), current_timestamp()
FROM __VERDICTS__ s
WHERE s.work_id IS NOT NULL AND s.routing_key_unseen
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY COALESCE(s.doi_clean, s.pmid, s.arxiv, s.title_author)
  ORDER BY (s.doi IS NULL), (s.pmid IS NULL), (s.arxiv IS NULL), s.native_id
) = 1
;
-- 2. New works (generated id), one insert per tier. A DataCite concept defers to its single version record when that version is
-- bound or minting in this run (oxjob #1540); a version record defers to its concept (oxjob #1581); a Crossref preprint declaring
-- one article defers to it (oxjob #1099). The deferring record binds in the second pass through has_version / version_of /
-- crossref_preprint, and the partner never defers itself, so a pair cannot leave both unbound.
INSERT INTO __MAP__
  (doi, pmid, arxiv, title_author, created_date, updated_date)
SELECT s.doi, s.pmid, s.arxiv, s.title_author,
       current_date(), current_timestamp()
FROM __VERDICTS__ s
WHERE s.work_id IS NULL AND s.mintable AND s.doi IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM __VERDICTS__ x
    WHERE x.work_id IS NOT NULL AND x.routing_key_unseen
      AND x.doi_clean = s.doi_clean)
  AND NOT EXISTS (
    SELECT 1 FROM __VERDICTS__ x
    WHERE s.has_version_doi_clean IS NOT NULL AND x.doi_clean = s.has_version_doi_clean
      AND x.has_version_doi_clean IS NULL
      AND (x.work_id IS NOT NULL OR (x.mintable AND x.doi IS NOT NULL)))
  AND NOT EXISTS (
    SELECT 1 FROM __VERDICTS__ x
    WHERE s.version_of_doi_clean IS NOT NULL AND s.has_version_doi_clean IS NULL AND x.doi_clean = s.version_of_doi_clean
      AND x.has_version_doi_clean IS NULL AND x.version_of_doi_clean IS NULL
      AND (x.work_id IS NOT NULL OR (x.mintable AND x.doi IS NOT NULL)))
  AND NOT EXISTS (
    SELECT 1 FROM __VERDICTS__ x
    WHERE s.preprint_link_is_pre AND SIZE(s.preprint_link_dois) = 1 AND x.doi_clean = s.preprint_link_dois[0]
      AND NOT x.preprint_link_is_pre
      AND (x.work_id IS NOT NULL OR (x.mintable AND x.doi IS NOT NULL)))
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY s.doi_clean
  ORDER BY (s.pmid IS NULL), (s.arxiv IS NULL), (s.title_author IS NULL), s.native_id
) = 1
;
INSERT INTO __MAP__
  (doi, pmid, arxiv, title_author, created_date, updated_date)
SELECT s.doi, s.pmid, s.arxiv, s.title_author,
       current_date(), current_timestamp()
FROM __VERDICTS__ s
WHERE s.work_id IS NULL AND s.mintable AND s.doi IS NULL AND s.pmid IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM __VERDICTS__ x
    WHERE x.pmid = s.pmid
      AND ((x.work_id IS NOT NULL AND x.routing_key_unseen)
           OR (x.work_id IS NULL AND x.mintable AND x.doi IS NOT NULL)))
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY s.pmid
  ORDER BY (s.arxiv IS NULL), (s.title_author IS NULL), s.native_id
) = 1
;
INSERT INTO __MAP__
  (doi, pmid, arxiv, title_author, created_date, updated_date)
SELECT s.doi, s.pmid, s.arxiv, s.title_author,
       current_date(), current_timestamp()
FROM __VERDICTS__ s
WHERE s.work_id IS NULL AND s.mintable
  AND s.doi IS NULL AND s.pmid IS NULL AND s.arxiv IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM __VERDICTS__ x
    WHERE x.arxiv = s.arxiv
      AND ((x.work_id IS NOT NULL AND x.routing_key_unseen)
           OR (x.work_id IS NULL AND x.mintable
               AND (x.doi IS NOT NULL OR x.pmid IS NOT NULL))))
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY s.arxiv
  ORDER BY (s.title_author IS NULL), s.native_id
) = 1
;
INSERT INTO __MAP__
  (doi, pmid, arxiv, title_author, created_date, updated_date)
SELECT s.doi, s.pmid, s.arxiv, s.title_author,
       current_date(), current_timestamp()
FROM __VERDICTS__ s
WHERE s.work_id IS NULL AND s.mintable
  AND s.doi IS NULL AND s.pmid IS NULL AND s.arxiv IS NULL AND s.title_author IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM __VERDICTS__ x
    WHERE x.title_author = s.title_author
      AND ((x.work_id IS NOT NULL AND x.routing_key_unseen)
           OR (x.work_id IS NULL AND x.mintable
               AND (x.doi IS NOT NULL OR x.pmid IS NOT NULL OR x.arxiv IS NOT NULL))))
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.title_author ORDER BY s.native_id) = 1
;
-- 3. Enrichment (the only UPDATE in normal operation): records resolved on an identifier gift their secondary keys to the row
-- they resolved on: single winner per key, empty slots only, `id` never touched.
MERGE INTO __MAP__ AS target
USING (
  SELECT s.doi_clean, s.pmid, s.arxiv, s.title_author
  FROM __VERDICTS__ s
  WHERE s.work_id_source = 'doi' AND s.doi IS NOT NULL
    AND (s.pmid IS NOT NULL OR s.arxiv IS NOT NULL OR s.title_author IS NOT NULL)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.doi_clean
    ORDER BY (s.pmid IS NULL), (s.arxiv IS NULL), (s.title_author IS NULL), s.native_id
  ) = 1
) AS source
ON regexp_replace(target.doi, '[^a-zA-Z0-9\./-]', '') = source.doi_clean
WHEN MATCHED AND (
  (target.pmid IS NULL AND source.pmid IS NOT NULL) OR
  (target.arxiv IS NULL AND source.arxiv IS NOT NULL) OR
  (target.title_author IS NULL AND source.title_author IS NOT NULL)
)
THEN UPDATE SET
  target.pmid = COALESCE(target.pmid, source.pmid),
  target.arxiv = COALESCE(target.arxiv, source.arxiv),
  target.title_author = COALESCE(target.title_author, source.title_author),
  target.updated_date = current_timestamp()
;
MERGE INTO __MAP__ AS target
USING (
  SELECT s.pmid, s.arxiv, s.title_author
  FROM __VERDICTS__ s
  WHERE s.work_id_source = 'pmid' AND s.pmid IS NOT NULL
    AND (s.arxiv IS NOT NULL OR s.title_author IS NOT NULL)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.pmid
    ORDER BY (s.arxiv IS NULL), (s.title_author IS NULL), s.native_id
  ) = 1
) AS source
ON target.pmid = source.pmid
WHEN MATCHED AND (
  (target.arxiv IS NULL AND source.arxiv IS NOT NULL) OR
  (target.title_author IS NULL AND source.title_author IS NOT NULL)
)
THEN UPDATE SET
  target.arxiv = COALESCE(target.arxiv, source.arxiv),
  target.title_author = COALESCE(target.title_author, source.title_author),
  target.updated_date = current_timestamp()
;
MERGE INTO __MAP__ AS target
USING (
  SELECT s.arxiv, s.title_author
  FROM __VERDICTS__ s
  WHERE s.work_id_source = 'arxiv' AND s.arxiv IS NOT NULL AND s.title_author IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.arxiv ORDER BY (s.title_author IS NULL), s.native_id
  ) = 1
) AS source
ON target.arxiv = source.arxiv
WHEN MATCHED AND target.title_author IS NULL AND source.title_author IS NOT NULL
THEN UPDATE SET
  target.title_author = source.title_author,
  target.updated_date = current_timestamp()
;
-- Second pass: records still without a work pick up tonight's rows by re-reading the lookups (now including this run's
-- inserts); everything else keeps its first-pass answer. A title key is looked up only among the rows this run inserted, so the
-- title rule never binds a record to a work that existed before the run: a record that minted finds its own new work, or a
-- same-night record's with the same key. A nightly retry matches by identifier only. `converged_alias` counts strongest-key-new records bound to existing works (duplicates
-- prevented at birth). A first-sight record the matcher decided that lands on a new id is labelled `new_work`.
CREATE OR REPLACE TABLE __VERDICTS__
CLUSTER BY (provenance, native_id_namespace, native_id)
AS
WITH __LOOKUPS__,
map_ta_tonight AS (
  SELECT m.title_author AS k, MIN(m.id) AS v, COUNT(DISTINCT m.id) AS n_ids
  FROM __MAP__ m
  LEFT SEMI JOIN (SELECT DISTINCT title_author FROM __PENDING__ WHERE title_author IS NOT NULL) pk
    ON m.title_author = pk.title_author
  WHERE m.created_date = DATE'__RUN_DATE__' AND m.updated_date >= TIMESTAMP'__RUN_START__'
    AND m.title_author IS NOT NULL AND m.title_author <> ''
  GROUP BY m.title_author
),
p2 AS (
  SELECT s.*,
         d.v AS dv, p.v AS pv, a.v AS av,
         -- oxjob #1672: same arms as the first pass
         CASE WHEN SPLIT_PART(s.doi_clean, '/', 1) = SPLIT_PART(s.version_of_doi_clean, '/', 1)
              THEN vo.v
              WHEN LENGTH(s.title_author) > 20 AND ARRAY_CONTAINS(vo.doi_row_keys, s.title_author) THEN vo.v END AS vv,
         CASE WHEN SPLIT_PART(s.doi_clean, '/', 1) = SPLIT_PART(s.has_version_doi_clean, '/', 1)
              THEN hv.v
              WHEN LENGTH(s.title_author) > 20 AND ARRAY_CONTAINS(hv.doi_row_keys, s.title_author) THEN hv.v END AS hvv,
         pl.v AS plv,
         CASE WHEN LENGTH(s.title_author) > 20 AND tn.n_ids <= 3 THEN tn.v END AS tav
  FROM __VERDICTS__ s
  LEFT JOIN map_doi_lookup d   ON s.work_id IS NULL AND s.doi_clean = d.k
  LEFT JOIN map_pmid_lookup p  ON s.work_id IS NULL AND s.pmid = p.k
  LEFT JOIN map_arxiv_lookup a ON s.work_id IS NULL AND s.arxiv = a.k
  LEFT JOIN map_ta_tonight tn  ON s.work_id IS NULL AND NOT s.is_retry AND s.title_author = tn.k
  LEFT JOIN map_version_of_lookup vo ON s.work_id IS NULL AND s.version_of_doi_clean = vo.k
  LEFT JOIN map_has_version_lookup hv ON s.work_id IS NULL AND s.has_version_doi_clean = hv.k
  LEFT JOIN map_preprint_lookup pl ON s.work_id IS NULL AND s.preprint_link_dois = pl.k
),
f AS (
  SELECT p2.*,
         COALESCE(work_id, dv, pv, av, vv, hvv, plv, tav) AS final_work_id,
         COALESCE(work_id_source,
                  CASE WHEN dv IS NOT NULL THEN 'doi'
                       WHEN pv IS NOT NULL THEN 'pmid'
                       WHEN av IS NOT NULL THEN 'arxiv'
                       WHEN vv IS NOT NULL THEN 'version_of'
                       WHEN hvv IS NOT NULL THEN 'has_version'
                       WHEN plv IS NOT NULL THEN 'crossref_preprint'
                       WHEN tav IS NOT NULL THEN 'title_author' END) AS tier_source
  FROM p2
)
SELECT provenance, native_id_namespace, native_id, is_retry, first_sight,
       doi, doi_clean, pmid, arxiv, title_author, bad_title, has_version_doi_clean, version_of_doi_clean,
       preprint_link_dois, preprint_link_is_pre, established_conflict, title_doi_conflict, mintable, routing_key_unseen,
       title_pick,
       final_work_id AS work_id,
       CASE WHEN decided_by_model AND work_id IS NULL AND final_work_id > __MAXID__ THEN 'new_work' ELSE tier_source END
         AS work_id_source,
       decided_by_model,
       (work_id IS NOT NULL AND routing_key_unseen) AS converged_alias,
       current_timestamp() AS run_ts
FROM f
;
-- oxjob #1540 / #1581 / #1099: a record that bound through has_version, version_of or crossref_preprint in the second pass
-- registers its own DOI on the winner (first-pass aliases are written above), so a later record carrying that DOI resolves on
-- the doi tier. Insert-only; one row per DOI.
INSERT INTO __MAP__
  (id, doi, pmid, arxiv, title_author, created_date, updated_date)
SELECT v.work_id, v.doi, v.pmid, v.arxiv, v.title_author,
       current_date(), current_timestamp()
FROM __VERDICTS__ v
WHERE v.work_id_source IN ('has_version', 'version_of', 'crossref_preprint') AND v.work_id IS NOT NULL AND v.doi IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM __MAP__ m
    WHERE regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '') = v.doi_clean)
QUALIFY ROW_NUMBER() OVER (PARTITION BY v.doi_clean ORDER BY v.native_id) = 1
;
MERGE INTO __REG__ AS target
USING __VERDICTS__ AS source
ON  target.provenance = source.provenance
AND target.native_id_namespace = source.native_id_namespace
AND target.native_id = source.native_id
WHEN MATCHED AND target.work_id IS NULL AND source.work_id IS NOT NULL
THEN UPDATE SET
  target.work_id = source.work_id,
  target.work_id_source = source.work_id_source,
  target.openalex_updated_dt = source.run_ts
WHEN NOT MATCHED THEN INSERT (
  provenance, native_id_namespace, native_id, work_id, work_id_source,
  openalex_created_dt, openalex_updated_dt, seeded_dt, seeded_from
) VALUES (
  source.provenance, source.native_id_namespace, source.native_id,
  source.work_id, source.work_id_source,
  current_date(), source.run_ts, NULL, NULL
)
;
-- oxjob #1256: every title key a bound record carries is registered to its work (a title-only row per missing (work, key)),
-- same gate as the title tier (length > 20, not a curated bad title); `id` never touched
INSERT INTO __MAP__
  (id, doi, pmid, arxiv, title_author, created_date, updated_date)
SELECT b.work_id, NULL, NULL, NULL, b.title_author,
       current_date(), current_timestamp()
FROM (
  SELECT DISTINCT v.work_id, v.title_author
  FROM __VERDICTS__ v
  WHERE v.work_id IS NOT NULL
    AND v.title_author IS NOT NULL
    AND LENGTH(v.title_author) > 20
    AND NOT v.bad_title
    AND NOT EXISTS (
      SELECT 1 FROM __MAP__ m
      WHERE m.id = v.work_id AND m.title_author = v.title_author)
) b
;
-- Per-run stats, idempotent on run_ts; a run with no pending records logs nothing
INSERT INTO __RUN_STATS__
  (run_ts, run_dt, anchors_new_resolved, anchors_new_null,
   anchors_retried_resolved, anchors_retried_null,
   src_doi, src_pmid, src_arxiv, src_title_author, src_mag_legacy, src_pmh_legacy,
   minted_range, distinct_works, max_work_fanin, top_fanin_work_id, registry_null_backlog,
   mints_displaced, bridge_conflicts, converged_alias, src_location_matcher, src_new_work, src_merged_winner)
WITH f AS (
  SELECT work_id, COUNT(*) AS n
  FROM __VERDICTS__
  WHERE work_id IS NOT NULL
  GROUP BY work_id ORDER BY n DESC LIMIT 1
)
SELECT
  MAX(v.run_ts),
  CAST(MAX(v.run_ts) AS DATE),
  SUM(CASE WHEN NOT v.is_retry AND v.work_id IS NOT NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN NOT v.is_retry AND v.work_id IS NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.is_retry AND v.work_id IS NOT NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.is_retry AND v.work_id IS NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.work_id_source = 'doi' THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.work_id_source = 'pmid' THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.work_id_source = 'arxiv' THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.work_id_source = 'title_author' THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.work_id_source = 'mag_legacy' THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.work_id_source = 'pmh_legacy' THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.work_id > 6600000000 THEN 1 ELSE 0 END),
  COUNT(DISTINCT v.work_id),
  MAX(f.n),
  MAX(f.work_id),
  (SELECT COUNT(*) - COUNT(work_id) FROM __REG__),
  CAST(NULL AS BIGINT),
  SUM(CASE WHEN v.established_conflict THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.converged_alias THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.work_id_source = 'location_matcher' THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.work_id_source = 'new_work' THEN 1 ELSE 0 END),
  SUM(CASE WHEN v.work_id_source = 'merged_winner' THEN 1 ELSE 0 END)
FROM __VERDICTS__ v
LEFT JOIN f ON TRUE
HAVING MAX(v.run_ts) IS NOT NULL
   AND NOT EXISTS (
     SELECT 1 FROM __RUN_STATS__ s WHERE s.run_ts = MAX(v.run_ts))
;
SELECT is_retry, first_sight, decided_by_model, work_id_source, (work_id IS NULL) AS parked, count(*) AS records
FROM __VERDICTS__ GROUP BY ALL ORDER BY records DESC
