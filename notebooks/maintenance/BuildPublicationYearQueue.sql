-- Databricks notebook source
-- oxjob #1644: the queue for ResolvePublicationYears. Every work with no publication year that has a repository or
-- DataCite location, with every harvested record it has (repository OAI records from the live harvest, else the
-- backfill copy; DataCite attributes), each cut at 8,000 characters. chunk_id spreads works over ~50K-work chunks.
-- One-time: the fill it feeds retires once the ingest parsers read these records properly (#1644, #1828).
-- Revert: drop the table (nothing reads it but ResolvePublicationYears).

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.works.publication_year_queue
COMMENT 'oxjob #1644: works with no publication year + all their repository / DataCite records, for ResolvePublicationYears'
AS
WITH w AS (
  SELECT w.id work_id, w.title, w.type,
    coalesce(w.primary_location.source.id, w.primary_location.endpoint_id,
             CASE WHEN w.primary_location.provenance = 'datacite' THEN regexp_extract(w.doi, '(10\\.[0-9]+)', 1) END,
             'unknown') src_key,
    w.locations
  FROM openalex.works.openalex_works w
  WHERE w.publication_year IS NULL AND exists(w.locations, l -> l.provenance IN ('repo', 'datacite'))),
rl AS (SELECT w.work_id, l.native_id nid, l.endpoint_id ep, l.source.display_name sname
       FROM w LATERAL VIEW explode(filter(w.locations, l -> l.provenance = 'repo')) x AS l),
b AS (SELECT bf.pmh_id, bf.endpoint_id, first(substr(bf.api_raw, 1, 8000)) rec
      FROM openalex.repo.repo_items_backfill bf JOIN (SELECT DISTINCT nid, ep FROM rl) k ON k.nid = bf.pmh_id AND k.ep = bf.endpoint_id
      GROUP BY 1, 2),
lv AS (SELECT i.`ns0:header`.`ns0:identifier` nid,
              max_by(substr(to_json(i.`ns0:metadata`), 1, 8000), i.`ns0:header`.`ns0:datestamp`) rec
       FROM openalex.repo.repo_items i JOIN (SELECT DISTINCT nid FROM rl) k ON k.nid = i.`ns0:header`.`ns0:identifier`
       GROUP BY 1),
rrec AS (SELECT rl.work_id, named_struct('prov', 'repo', 'nid', rl.nid, 'sname', rl.sname, 'rec', coalesce(lv.rec, b.rec)) r
         FROM rl LEFT JOIN b ON b.pmh_id = rl.nid AND b.endpoint_id = rl.ep LEFT JOIN lv ON lv.nid = rl.nid
         WHERE coalesce(lv.rec, b.rec) IS NOT NULL),
dl AS (SELECT w.work_id, lower(l.native_id) doi, l.source.display_name sname
       FROM w LATERAL VIEW explode(filter(w.locations, l -> l.provenance = 'datacite')) x AS l),
drec AS (SELECT dl.work_id, named_struct('prov', 'datacite', 'nid', dl.doi, 'sname', first(dl.sname),
                'rec', substr(to_json(max_by(i.attributes, i.attributes.updated)), 1, 8000)) r
         FROM dl JOIN openalex.datacite.datacite_items i ON lower(i.id) = dl.doi GROUP BY dl.work_id, dl.doi),
recs AS (SELECT work_id, collect_list(r) records FROM (SELECT * FROM rrec UNION ALL SELECT * FROM drec) GROUP BY 1)
SELECT w.work_id, pmod(xxhash64(w.work_id, 1644), 320) chunk_id, w.title, w.type, w.src_key, recs.records
FROM w JOIN recs USING (work_id)

-- COMMAND ----------

SELECT count(*) works, count(DISTINCT chunk_id) chunks, sum(size(records)) records,
       round(avg(aggregate(records, 0L, (acc, r) -> acc + length(r.rec)))) avg_chars
FROM openalex.works.publication_year_queue
