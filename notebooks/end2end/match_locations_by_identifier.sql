-- Map_Work_Ids 1/3, match_locations_by_identifier (oxjob #764, #1668). Every record that needs a work tonight (absent from the
-- registry, or registered NULL: NULL anchors retry nightly) into __PENDING__, then its answer by identifier into __VERDICTS__,
-- the night's one answer table (AssignWorkIds adds the location matcher's answers and the new ids, then pins from it).
-- Strict tier precedence doi -> pmid -> arxiv -> version_of -> has_version -> crossref_preprint over works that already exist,
-- then `merged_winner`: a record a merge job unpinned since the previous registry MERGE (__REG_OLD__) goes to the winner its old
-- work was merged into (merged_work_ids, which every merge job writes when it runs; on 2026-11-01 all ~31K re-pinned records
-- were of this kind and 99.97% landed on the winner), then legacy adoption (a mag record's mag id, a repo record's PMH crosswalk
-- id) when nothing resolves but the record is mintable. The title rule decides nothing (Casey 2026-11-02): its pick is kept as
-- `title_pick`, an input to the location matcher, which decides the first-sight records (never pinned before) left without a
-- work. A re-pinned record with no match mints (a split job's records, as before); a nightly retry with no match stays parked.
-- doi matching is on the CLEANED form ([^a-zA-Z0-9./-] stripped) on both sides; keys are scrubbed of degenerate values here, once.
CREATE OR REPLACE TABLE __PENDING__
CLUSTER BY (provenance, native_id_namespace, native_id)
AS
WITH t AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY provenance, native_id_namespace, native_id
           ORDER BY updated_date DESC) AS rwcnt
  FROM __LOCATIONS__
  QUALIFY rwcnt = 1
),
-- oxjob #1099: Crossref-declared preprint <-> article pairs (`is-preprint-of` on the preprint or `has-preprint` on the article),
-- typed, from the one definition MergeDeclaredLinks also reads (it recreates the view before this task)
cr_pairs AS (
  SELECT pre, pub FROM openalex.works.preprint_link_pairs WHERE arm = 'crossref'
),
cr_partners AS (
  SELECT d, MAX(is_pre) AS is_pre, ARRAY_SORT(COLLECT_SET(p)) AS dois
  FROM (SELECT pre AS d, pub AS p, TRUE AS is_pre FROM cr_pairs UNION ALL SELECT pub, pre, FALSE FROM cr_pairs)
  GROUP BY d
),
old AS (
  SELECT provenance, native_id_namespace, native_id, MIN(work_id) AS old_work_id FROM __REG_OLD__ WHERE work_id IS NOT NULL
  GROUP BY provenance, native_id_namespace, native_id
)
SELECT t.provenance, t.native_id_namespace, t.native_id,
       (r.native_id IS NOT NULL) AS is_retry,
       (r.native_id IS NULL AND o.native_id IS NULL) AS first_sight,
       CASE WHEN r.native_id IS NULL THEN o.old_work_id END AS old_work_id,
       NULLIF(t.merge_key.doi, '')  AS doi,
       NULLIF(regexp_replace(t.merge_key.doi, '[^a-zA-Z0-9\./-]', ''), '') AS doi_clean,
       NULLIF(t.merge_key.pmid, '') AS pmid,
       CASE WHEN t.merge_key.arxiv IN ('arXiv:', '') THEN NULL
            ELSE t.merge_key.arxiv END AS arxiv,
       NULLIF(t.merge_key.title_author, '') AS title_author,
       -- oxjob #880: curated junk titles never merge or mint on the title tier
       (bt.title IS NOT NULL) AS bad_title,
       CAST(get(filter(t.ids, x -> x.namespace = 'mag').id, 0) AS BIGINT) AS mag_id,
       -- oxjob #1256 / #1540: a DataCite record declaring IsVersionOf exactly one other DOI (every prefix but arXiv); oxjob #1581:
       -- Figshare and its portals declare nothing, so a DataCite DOI ending `.vN` is a version of the DOI without it
       CASE WHEN t.provenance = 'datacite' AND SIZE(ARRAY_DISTINCT(TRANSFORM(FILTER(t.ids, x -> x.relationship = 'IsVersionOf'
                   AND LOWER(COALESCE(x.namespace, 'doi')) = 'doi' AND LOWER(x.id) NOT LIKE '%10.48550/%'),
                   x -> regexp_replace(regexp_replace(LOWER(TRIM(x.id)), '^(https?://(dx[.])?doi[.]org/|doi:)', ''), '[^a-zA-Z0-9\./-]', '')))) = 1
            THEN ARRAY_DISTINCT(TRANSFORM(FILTER(t.ids, x -> x.relationship = 'IsVersionOf'
                   AND LOWER(COALESCE(x.namespace, 'doi')) = 'doi' AND LOWER(x.id) NOT LIKE '%10.48550/%'),
                   x -> regexp_replace(regexp_replace(LOWER(TRIM(x.id)), '^(https?://(dx[.])?doi[.]org/|doi:)', ''), '[^a-zA-Z0-9\./-]', '')))[0]
            WHEN t.provenance = 'datacite' AND regexp_replace(t.merge_key.doi, '[^a-zA-Z0-9\./-]', '') RLIKE '[.][vV][0-9]+$'
            THEN regexp_replace(regexp_replace(t.merge_key.doi, '[^a-zA-Z0-9\./-]', ''), '[.][vV][0-9]+$', '') END AS version_of_doi_clean,
       -- oxjob #1540: a DataCite concept record declaring HasVersion exactly one DOI (InvenioRDM parent -> its single version)
       CASE WHEN t.provenance = 'datacite' AND SIZE(ARRAY_DISTINCT(TRANSFORM(FILTER(t.ids, x -> x.relationship = 'HasVersion'
                   AND LOWER(COALESCE(x.namespace, 'doi')) = 'doi' AND LOWER(x.id) NOT LIKE '%10.48550/%'),
                   x -> regexp_replace(regexp_replace(LOWER(TRIM(x.id)), '^(https?://(dx[.])?doi[.]org/|doi:)', ''), '[^a-zA-Z0-9\./-]', '')))) = 1
            THEN ARRAY_DISTINCT(TRANSFORM(FILTER(t.ids, x -> x.relationship = 'HasVersion'
                   AND LOWER(COALESCE(x.namespace, 'doi')) = 'doi' AND LOWER(x.id) NOT LIKE '%10.48550/%'),
                   x -> regexp_replace(regexp_replace(LOWER(TRIM(x.id)), '^(https?://(dx[.])?doi[.]org/|doi:)', ''), '[^a-zA-Z0-9\./-]', '')))[0] END AS has_version_doi_clean,
       -- oxjob #1099: a Crossref preprint's declared article DOI(s), or an article's declared preprint DOI(s), cleaned like doi_clean
       cp.dois AS preprint_link_dois,
       COALESCE(cp.is_pre, FALSE) AS preprint_link_is_pre
FROM t
LEFT JOIN (SELECT DISTINCT TRIM(title) AS title FROM openalex.system.bad_titles) bt ON bt.title = TRIM(t.title)
LEFT JOIN cr_partners cp ON t.provenance = 'crossref' AND cp.d = regexp_replace(t.merge_key.doi, '[^a-zA-Z0-9\./-]', '')
LEFT JOIN __REG__ r
  ON  t.provenance = r.provenance
  AND t.native_id_namespace = r.native_id_namespace
  AND t.native_id = r.native_id
LEFT JOIN old o
  ON  t.provenance = o.provenance
  AND t.native_id_namespace = o.native_id_namespace
  AND t.native_id = o.native_id
WHERE r.native_id IS NULL OR r.work_id IS NULL
;
-- The first-pass answer. `established_conflict` marks strong-tier disagreement between existing works (the bridge class);
-- `routing_key_unseen` marks records whose strongest key is new to the map (resolved ones become alias rows in AssignWorkIds).
-- Guards on the title tier: title_author length > 20, <= 3 distinct ids per key, no PRIMARY-id conflict (oxjob #880 § 8).
CREATE OR REPLACE TABLE __VERDICTS__
CLUSTER BY (provenance, native_id_namespace, native_id)
AS
WITH __LOOKUPS__,
-- the winner a re-pinned record's old work was merged into (followed through later merges), while it holds a pin
merged_winner AS (
  WITH m AS (SELECT loser_work_id, max_by(winner_work_id, merged_at) AS w FROM __MERGED__ GROUP BY loser_work_id),
  o AS (SELECT DISTINCT old_work_id FROM __PENDING__ WHERE old_work_id IS NOT NULL),
  f AS (
    SELECT o.old_work_id, COALESCE(m3.w, m2.w, m1.w) AS w
    FROM o JOIN m m1 ON m1.loser_work_id = o.old_work_id
    LEFT JOIN m m2 ON m2.loser_work_id = m1.w
    LEFT JOIN m m3 ON m3.loser_work_id = m2.w
  )
  SELECT f.old_work_id AS k, f.w AS v FROM f LEFT SEMI JOIN __REG__ r ON r.work_id = f.w
),
pmh_mapping AS (
  SELECT LOWER(pmh_id) AS pmh_id, MIN(work_id) AS legacy_work_id
  FROM openalex.works_poc.work_id_to_pmh_id_final
  GROUP BY LOWER(pmh_id)
),
rs AS (
  SELECT pd.provenance, pd.native_id_namespace, pd.native_id, pd.is_retry, pd.first_sight, mw.v AS mwv,
         pd.doi, pd.doi_clean, pd.pmid, pd.arxiv, pd.title_author,
         pd.mag_id, pd.bad_title, pd.has_version_doi_clean, pd.version_of_doi_clean,
         pd.preprint_link_dois, pd.preprint_link_is_pre,
         d.v AS dv, p.v AS pv, a.v AS av,
         -- oxjob #1672: a same-registrant declaration (Zenodo / InvenioRDM concept, Figshare `.vN`) binds on the declaration
         -- alone; a cross-registrant one (author-entered, e.g. preprint -> article) keeps the title gate
         CASE WHEN SPLIT_PART(pd.doi_clean, '/', 1) = SPLIT_PART(pd.version_of_doi_clean, '/', 1)
              THEN vo.v
              WHEN LENGTH(pd.title_author) > 20 AND NOT pd.bad_title AND ARRAY_CONTAINS(vo.doi_row_keys, pd.title_author)
              THEN vo.v END AS vv,
         CASE WHEN SPLIT_PART(pd.doi_clean, '/', 1) = SPLIT_PART(pd.has_version_doi_clean, '/', 1)
              THEN hv.v
              WHEN LENGTH(pd.title_author) > 20 AND NOT pd.bad_title AND ARRAY_CONTAINS(hv.doi_row_keys, pd.title_author)
              THEN hv.v END AS hvv,
         pl.v AS plv,
         -- oxjob #880 § 8: the title tier must not bind a record whose PRIMARY-registered id conflicts with one of the same
         -- class already on the work (crossref onto a crossref work, datacite onto a datacite work)
         CASE WHEN LENGTH(pd.title_author) > 20 AND NOT pd.bad_title AND ta.n_ids <= 3
                   AND NOT (pd.doi IS NOT NULL AND d.v IS NULL
            AND ((pd.provenance = 'crossref' AND COALESCE(wp.has_crossref, FALSE))
              OR (pd.provenance = 'datacite' AND COALESCE(wp.has_datacite, FALSE))))
              THEN ta.v END AS tav,
         (ta.v IS NOT NULL AND pd.doi IS NOT NULL AND d.v IS NULL
            AND ((pd.provenance = 'crossref' AND COALESCE(wp.has_crossref, FALSE))
              OR (pd.provenance = 'datacite' AND COALESCE(wp.has_datacite, FALSE)))) AS title_doi_conflict,
         (ta.k IS NOT NULL AND ta.n_ids > 3) AS ta_blocked,
         SIZE(ARRAY_DISTINCT(FILTER(ARRAY(d.v, p.v, a.v), x -> x IS NOT NULL))) > 1
           AS established_conflict
  FROM __PENDING__ pd
  LEFT JOIN map_doi_lookup d   ON pd.doi_clean = d.k
  LEFT JOIN map_pmid_lookup p  ON pd.pmid = p.k
  LEFT JOIN map_arxiv_lookup a ON pd.arxiv = a.k
  LEFT JOIN map_ta_lookup ta   ON pd.title_author = ta.k
  LEFT JOIN map_ta_work_prov wp ON wp.k = ta.v
  LEFT JOIN map_version_of_lookup vo ON pd.version_of_doi_clean = vo.k
  LEFT JOIN map_has_version_lookup hv ON pd.has_version_doi_clean = hv.k
  LEFT JOIN map_preprint_lookup pl ON pd.preprint_link_dois = pl.k
  LEFT JOIN merged_winner mw ON pd.old_work_id = mw.k
),
scored AS (
  SELECT rs.*,
         COALESCE(rs.dv, rs.pv, rs.av, rs.vv, rs.hvv, rs.plv, rs.mwv) AS resolved_id,
         CASE WHEN rs.dv IS NOT NULL THEN 'doi'
              WHEN rs.pv IS NOT NULL THEN 'pmid'
              WHEN rs.av IS NOT NULL THEN 'arxiv'
              WHEN rs.vv IS NOT NULL THEN 'version_of'
              WHEN rs.hvv IS NOT NULL THEN 'has_version'
              WHEN rs.plv IS NOT NULL THEN 'crossref_preprint'
              WHEN rs.mwv IS NOT NULL THEN 'merged_winner' END AS resolved_source,
         (rs.doi IS NOT NULL OR rs.pmid IS NOT NULL OR rs.arxiv IS NOT NULL
          OR (LENGTH(rs.title_author) > 20 AND NOT rs.bad_title AND NOT COALESCE(rs.ta_blocked, FALSE)))
           AS mintable,
         CASE WHEN rs.doi IS NOT NULL THEN rs.dv IS NULL
              WHEN rs.pmid IS NOT NULL THEN rs.pv IS NULL
              WHEN rs.arxiv IS NOT NULL THEN rs.av IS NULL
              WHEN rs.title_author IS NOT NULL THEN rs.tav IS NULL
              ELSE FALSE END AS routing_key_unseen
  FROM rs
)
SELECT s.provenance, s.native_id_namespace, s.native_id, s.is_retry, s.first_sight,
       s.doi, s.doi_clean, s.pmid, s.arxiv, s.title_author, s.bad_title, s.has_version_doi_clean, s.version_of_doi_clean,
       s.preprint_link_dois, s.preprint_link_is_pre,
       s.established_conflict, s.title_doi_conflict, s.mintable, s.routing_key_unseen,
       s.tav AS title_pick,
       CASE
         WHEN s.resolved_id IS NOT NULL THEN s.resolved_id
         -- oxjob #1256: a merged-away legacy id adopts its winner; a ledgered dead id is not adoptable (mint instead)
         WHEN s.mintable AND s.provenance = 'mag' AND s.mag_id IS NOT NULL
              THEN CASE WHEN rm.k IS NULL THEN s.mag_id ELSE rm.v END
         WHEN s.mintable AND s.provenance IN ('repo', 'repo_backfill')
              AND pm.legacy_work_id IS NOT NULL
              THEN CASE WHEN rp.k IS NULL THEN pm.legacy_work_id ELSE rp.v END
       END AS work_id,
       CASE
         WHEN s.resolved_id IS NOT NULL THEN s.resolved_source
         WHEN s.mintable AND s.provenance = 'mag' AND s.mag_id IS NOT NULL AND (rm.k IS NULL OR rm.v IS NOT NULL) THEN 'mag_legacy'
         WHEN s.mintable AND s.provenance IN ('repo', 'repo_backfill')
              AND pm.legacy_work_id IS NOT NULL AND (rp.k IS NULL OR rp.v IS NOT NULL) THEN 'pmh_legacy'
       END AS work_id_source,
       FALSE AS decided_by_model,
       CAST(NULL AS BOOLEAN) AS converged_alias,
       CAST(NULL AS TIMESTAMP) AS run_ts
FROM scored s
LEFT JOIN legacy_id_redirect rm ON rm.k = s.mag_id AND s.provenance = 'mag'
LEFT JOIN pmh_mapping pm
  ON s.provenance IN ('repo', 'repo_backfill') AND LOWER(s.native_id) = pm.pmh_id
LEFT JOIN legacy_id_redirect rp ON rp.k = pm.legacy_work_id AND s.provenance IN ('repo', 'repo_backfill')
;
SELECT is_retry, first_sight, work_id_source, (title_pick IS NOT NULL) AS has_title_pick, count(*) AS records
FROM __VERDICTS__ GROUP BY ALL ORDER BY records DESC
