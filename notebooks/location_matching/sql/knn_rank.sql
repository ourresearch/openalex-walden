-- Match_Locations_To_Works (oxjob #1668): the embedding neighbours features.sql reads. KnnSearch's exact global top 20 per
-- seed (location_work_match_knn_raw, task tops), kept only where the work existed before the run (location_work_match_keys =
-- openalex_works __V__), ranked 1-10. score = (1 + cos) / 2, Elasticsearch's cosine score.
CREATE OR REPLACE TABLE openalex.works.location_work_match_knn AS
WITH g AS (
  SELECT qi, wid, cos, row_number() OVER (PARTITION BY qi ORDER BY cos DESC, wid) AS r FROM openalex.works.location_work_match_knn_raw
),
v AS (SELECT g.qi, g.wid, g.cos FROM g JOIN openalex.works.location_work_match_keys k ON k.id = g.wid WHERE g.r <= 20),
r AS (SELECT qi, wid, cos, row_number() OVER (PARTITION BY qi ORDER BY cos DESC, wid) AS rank FROM v)
SELECT q.seed_id, CAST(r.rank AS INT) AS rank, concat('https://openalex.org/W', r.wid) AS es_id, CAST((1 + r.cos) / 2 AS DOUBLE) AS score
FROM r JOIN openalex.works.location_work_match_knn_q q USING (qi)
WHERE r.rank <= 10
;
SELECT count(*) AS n, count(DISTINCT seed_id) AS seeds FROM openalex.works.location_work_match_knn
