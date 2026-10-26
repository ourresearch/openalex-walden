-- Match_Locations_To_Works (oxjob #1668): Qwen input. Records whose top XGBoost score is >= 0.01 (below that they mint) with
-- their top 10 candidates by XGBoost (hard rules already applied), both sides in the judge format the model was trained on
-- (record = its locations_mapped row; candidate = openalex_works __V__).
CREATE OR REPLACE TABLE openalex.works.location_work_match_qwen_pairs AS
WITH e AS (
  SELECT x.seed_id, x.cid, x.rank, x.p_raw FROM openalex.works.location_work_match_xgb x
  WHERE x.rank <= 10 AND x.seed_id IN (SELECT seed_id FROM openalex.works.location_work_match_xgb WHERE rank = 1 AND p_raw >= 0.01)
),
r AS (
  SELECT s.seed_id, l.title, year(l.published_date) AS yr, l.type, l.best_doi AS doi,
    transform(slice(l.authors, 1, 8), a -> coalesce(nullif(a.name, ''), concat_ws(' ', a.given, a.family))) AS authors,
    size(l.authors) AS n_authors, l.source_name AS venue, l.provenance AS prov, l.version,
    regexp_extract(l.landing_page_url, '^https?://([^/]+)', 1) AS host,
    concat_ws(' ', l.volume, l.issue, l.first_page) AS biblio, substr(l.abstract, 1, 400) AS abstract
  FROM openalex.works.location_work_match_seeds s
  JOIN openalex.works.locations_mapped l
    ON l.provenance = s.provenance AND l.native_id_namespace = s.native_id_namespace AND l.native_id = s.native_id
  WHERE s.seed_id IN (SELECT seed_id FROM e)
  QUALIFY row_number() OVER (PARTITION BY s.seed_id ORDER BY l.openalex_updated_dt DESC) = 1
),
w AS (
  SELECT id, title, publication_year AS yr, type, doi,
    transform(slice(authorships, 1, 8), a -> a.author.display_name) AS authors, size(authorships) AS n_authors,
    coalesce(primary_location.source.display_name, primary_location.raw_source_name) AS venue,
    primary_location.provenance AS prov, primary_location.version AS version,
    regexp_extract(primary_location.landing_page_url, '^https?://([^/]+)', 1) AS host,
    concat_ws(' ', biblio.volume, biblio.issue, biblio.first_page) AS biblio,
    substr(abstract, 1, 400) AS abstract, cited_by_count AS cites, locations_count
  FROM openalex.works.openalex_works VERSION AS OF __V__
  WHERE id IN (SELECT cid FROM e)
)
SELECT c.seed_id, c.cid, c.rank, c.p_raw,
  to_json(named_struct('title', r.title, 'authors', r.authors, 'n_authors', r.n_authors, 'year', r.yr, 'type', r.type, 'doi', r.doi,
    'venue', r.venue, 'version', r.version, 'host', r.host, 'biblio', r.biblio, 'abstract', r.abstract, 'prov', r.prov)) AS rec_a,
  to_json(named_struct('title', w.title, 'authors', w.authors, 'n_authors', w.n_authors, 'year', w.yr, 'type', w.type, 'doi', w.doi,
    'venue', w.venue, 'version', w.version, 'host', w.host, 'biblio', w.biblio, 'abstract', w.abstract, 'prov', w.prov, 'cites', w.cites)) AS rec_b
FROM e c JOIN r ON r.seed_id = c.seed_id JOIN w ON w.id = c.cid
;
SELECT count(*) pairs, count(DISTINCT seed_id) records FROM openalex.works.location_work_match_qwen_pairs
