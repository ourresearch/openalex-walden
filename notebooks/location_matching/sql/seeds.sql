-- Map_Work_Ids 2/3, the location matcher (oxjob #1668), step 2 of the inputs: seeds (every record of the night's frame, no
-- sampling, weight 1) and their records. Candidates = works in openalex_works __V__ (the works before the run), through
-- location_work_match_keys. A seed's record is its latest locations_w_types row, which locations_mapped copies column for column
-- after the run (is_retracted coalesced the same way): tonight's records reach locations_mapped only after Map_Work_Ids.
CREATE OR REPLACE TABLE openalex.works.location_work_match_seeds AS
SELECT f.provenance, f.native_id_namespace, f.native_id, f.work_id, f.work_id_source, f.night, f.decision, f.prov, f.h AS seed_id,
  CAST(1 AS DOUBLE) AS weight
FROM openalex.works.location_work_match_frame f
;
CREATE OR REPLACE TABLE openalex.works.location_work_match_records AS
SELECT t.provenance, t.native_id_namespace, t.native_id, t.title, t.authors, t.ids, t.raw_type, t.type, t.version, t.language,
  t.published_date, t.issue, t.volume, t.first_page, COALESCE(t.is_retracted, FALSE) AS is_retracted, t.abstract, t.source_name,
  t.publisher, t.landing_page_url, t.pdf_url, t.best_doi, t.source_id, CAST(t.updated_date AS TIMESTAMP) AS openalex_updated_dt
FROM __LOCATIONS__ t
LEFT SEMI JOIN openalex.works.location_work_match_seeds s
  ON s.provenance = t.provenance AND s.native_id_namespace = t.native_id_namespace AND s.native_id = t.native_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.provenance, t.native_id_namespace, t.native_id ORDER BY t.updated_date DESC) = 1
;
SELECT s.prov, s.decision, count(*) AS seeds, count(r.native_id) AS with_record
FROM openalex.works.location_work_match_seeds s
LEFT JOIN openalex.works.location_work_match_records r USING (provenance, native_id_namespace, native_id)
GROUP BY 1, 2 ORDER BY 1, 2
