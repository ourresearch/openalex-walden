-- oxjob #1668 embedding channel: embed each seed record (the incoming location, as locations_mapped holds it) with the
-- corpus recipe of walden notebooks/vector_search/EmbedQwen3Incremental.py (Title / Abstract / Venue, 2,000 chars, bare,
-- databricks-qwen3-embedding-0-6b), so a seed vector is comparable to the works-vectors index.
CREATE OR REPLACE TABLE openalex.works.location_work_match_seed_vecs AS
WITH r AS (
  SELECT s.seed_id,
    SUBSTRING(CONCAT('Title: ', l.title,
      CASE WHEN l.abstract IS NOT NULL THEN CONCAT('\n\nAbstract: ', l.abstract) ELSE '' END,
      CASE WHEN l.source_name IS NOT NULL THEN CONCAT('\n\nVenue: ', l.source_name) ELSE '' END), 1, 2000) AS text
  FROM openalex.works.location_work_match_seeds s
  JOIN openalex.works.locations_mapped l
    ON l.provenance = s.provenance AND l.native_id_namespace = s.native_id_namespace AND l.native_id = s.native_id
  WHERE l.title IS NOT NULL AND length(trim(l.title)) > 0
  QUALIFY row_number() OVER (PARTITION BY s.seed_id ORDER BY l.openalex_updated_dt DESC) = 1
)
SELECT seed_id, text, ai_query('databricks-qwen3-embedding-0-6b', text) AS embedding FROM r
;
SELECT count(*) AS seeds, count_if(size(embedding) = 1024) AS with_vector FROM openalex.works.location_work_match_seed_vecs
