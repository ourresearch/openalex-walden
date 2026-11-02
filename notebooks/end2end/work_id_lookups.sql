-- Map_Work_Ids (oxjob #764, #1668): the work_id_map key lookups, one definition used as CTEs by both
-- MatchLocationsByIdentifier (against the map before tonight's inserts) and AssignWorkIds (after them, so a record can find
-- a work minted tonight). Each is semi-joined to __PENDING__'s keys so the map is probed, not aggregated in full. `map_ta_lookup`
-- exposes the distinct-id count instead of hiding it in a HAVING, so blocked keys (> 3 works) are distinguishable from unseen ones.
map_doi_lookup AS (
  SELECT regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '') AS k, MIN(m.id) AS v
  FROM __MAP__ m
  LEFT SEMI JOIN (SELECT DISTINCT doi_clean FROM __PENDING__ WHERE doi_clean IS NOT NULL) pk
    ON regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '') = pk.doi_clean
  WHERE m.doi IS NOT NULL AND m.doi <> ''
  GROUP BY regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '')
),
map_pmid_lookup AS (
  SELECT m.pmid AS k, MIN(m.id) AS v
  FROM __MAP__ m
  LEFT SEMI JOIN (SELECT DISTINCT pmid FROM __PENDING__ WHERE pmid IS NOT NULL) pk ON m.pmid = pk.pmid
  WHERE m.pmid IS NOT NULL AND m.pmid <> ''
  GROUP BY m.pmid
),
map_arxiv_lookup AS (
  SELECT m.arxiv AS k, MIN(m.id) AS v
  FROM __MAP__ m
  LEFT SEMI JOIN (SELECT DISTINCT arxiv FROM __PENDING__ WHERE arxiv IS NOT NULL) pk ON m.arxiv = pk.arxiv
  WHERE m.arxiv IS NOT NULL AND m.arxiv NOT IN ('arXiv:', '')
  GROUP BY m.arxiv
),
-- oxjob #1256: the work holding a declared IsVersionOf DOI, and the title keys on the rows carrying that DOI (the gate:
-- the preprint's own title key must be on the DOI's rows, not merely somewhere on the work -- a work is a union of records,
-- and a bad declaration's preprint can already be glued to it; blind sample 2026-09-28)
-- oxjob #1672: a concept DOI with no map row (merge waves move the concept record without registering its DOI) falls back to
-- the work its DataCite record is pinned to; no title keys there, so only the same-registrant arm can bind through it
map_version_of_lookup AS (
  WITH pk AS (
    SELECT DISTINCT version_of_doi_clean AS k FROM __PENDING__ WHERE version_of_doi_clean IS NOT NULL
  ),
  m AS (
    SELECT regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '') AS k, MIN(m.id) AS v, COLLECT_SET(m.title_author) AS doi_row_keys
    FROM __MAP__ m
    LEFT SEMI JOIN pk ON regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '') = pk.k
    WHERE m.doi IS NOT NULL AND m.doi <> ''
    GROUP BY regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '')
  ),
  r AS (
    SELECT regexp_replace(r.native_id, '[^a-zA-Z0-9\./-]', '') AS k, MIN(r.work_id) AS v
    FROM __REG__ r
    LEFT SEMI JOIN pk ON regexp_replace(r.native_id, '[^a-zA-Z0-9\./-]', '') = pk.k
    WHERE r.provenance = 'datacite' AND r.work_id IS NOT NULL
    GROUP BY regexp_replace(r.native_id, '[^a-zA-Z0-9\./-]', '')
  )
  SELECT COALESCE(m.k, r.k) AS k, COALESCE(m.v, r.v) AS v, m.doi_row_keys
  FROM m FULL OUTER JOIN r ON m.k = r.k
),
-- oxjob #1540: the work holding a declared HasVersion DOI (the concept record's single version), same gate as version_of:
-- the concept's own title key must be on the rows carrying the version DOI. oxjob #1672: same fallback to the version's pin.
map_has_version_lookup AS (
  WITH pk AS (
    SELECT DISTINCT has_version_doi_clean AS k FROM __PENDING__ WHERE has_version_doi_clean IS NOT NULL
  ),
  m AS (
    SELECT regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '') AS k, MIN(m.id) AS v, COLLECT_SET(m.title_author) AS doi_row_keys
    FROM __MAP__ m
    LEFT SEMI JOIN pk ON regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '') = pk.k
    WHERE m.doi IS NOT NULL AND m.doi <> ''
    GROUP BY regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '')
  ),
  r AS (
    SELECT regexp_replace(r.native_id, '[^a-zA-Z0-9\./-]', '') AS k, MIN(r.work_id) AS v
    FROM __REG__ r
    LEFT SEMI JOIN pk ON regexp_replace(r.native_id, '[^a-zA-Z0-9\./-]', '') = pk.k
    WHERE r.provenance = 'datacite' AND r.work_id IS NOT NULL
    GROUP BY regexp_replace(r.native_id, '[^a-zA-Z0-9\./-]', '')
  )
  SELECT COALESCE(m.k, r.k) AS k, COALESCE(m.v, r.v) AS v, m.doi_row_keys
  FROM m FULL OUTER JOIN r ON m.k = r.k
),
-- oxjob #1099: the work holding a Crossref record's declared preprint partner(s). Binds when every partner DOI already in the map
-- sits on one work (an article declaring several preprint versions on different works mints). No title gate: the declaration
-- alone binds (Casey 2026-10-07).
map_preprint_lookup AS (
  WITH pk AS (
    SELECT DISTINCT preprint_link_dois AS dois FROM __PENDING__ WHERE preprint_link_dois IS NOT NULL
  ),
  dv AS (
    SELECT regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '') AS d, MIN(m.id) AS v
    FROM __MAP__ m
    LEFT SEMI JOIN (SELECT DISTINCT explode(dois) AS d FROM pk) pd ON regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '') = pd.d
    WHERE m.doi IS NOT NULL AND m.doi <> ''
    GROUP BY regexp_replace(m.doi, '[^a-zA-Z0-9\./-]', '')
  )
  SELECT x.dois AS k, MIN(dv.v) AS v
  FROM (SELECT dois, explode(dois) AS d FROM pk) x
  JOIN dv ON dv.d = x.d
  GROUP BY x.dois
  HAVING COUNT(DISTINCT dv.v) = 1
),
map_ta_lookup AS (
  SELECT m.title_author AS k, MIN(m.id) AS v, COUNT(DISTINCT m.id) AS n_ids
  FROM __MAP__ m
  LEFT SEMI JOIN (SELECT DISTINCT title_author FROM __PENDING__ WHERE title_author IS NOT NULL) pk
    ON m.title_author = pk.title_author
  WHERE m.title_author IS NOT NULL AND m.title_author <> ''
  GROUP BY m.title_author
),
-- oxjob #880 § 8: which title-tier candidate works already carry a PRIMARY-registered id. Class of a DOI = provenance of the
-- anchors holding it; a work with a crossref anchor holds a crossref-registered DOI, likewise datacite.
map_ta_work_prov AS (
  SELECT r.work_id AS k,
         MAX(CASE WHEN r.provenance = 'crossref' THEN 1 ELSE 0 END) = 1 AS has_crossref,
         MAX(CASE WHEN r.provenance = 'datacite' THEN 1 ELSE 0 END) = 1 AS has_datacite
  FROM __REG__ r
  LEFT SEMI JOIN (SELECT DISTINCT v AS work_id FROM map_ta_lookup) w ON w.work_id = r.work_id
  WHERE r.work_id IS NOT NULL
  GROUP BY r.work_id
),
-- oxjob #1256: legacy adoption (a mag record adopting its mag id, a repo record adopting its PMH crosswalk id) never resurrects a
-- merged or dead id: a merged id adopts its winner, a ledgered dead id is not adoptable (the record mints)
legacy_id_redirect AS (
  SELECT m.loser_work_id AS k, m.winner_work_id AS v
  FROM __MERGED__ m
  LEFT ANTI JOIN __DELETED__ d ON d.work_id = m.winner_work_id
  UNION ALL
  SELECT d.work_id AS k, CAST(NULL AS BIGINT) AS v
  FROM __DELETED__ d
  LEFT ANTI JOIN __MERGED__ m ON m.loser_work_id = d.work_id
)
