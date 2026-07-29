"""oxjob #695 — validate the Source-3-replica cohort reconstruction against #682's 27.4M."""
import sys
sys.path.insert(0, "/Users/caseymeyer/vs code projects/openalex-walden")
from utils.databricks_sql import run_query

DOI_PREFIX = r"^(doi:|https?://(dx\\.)?doi\\.org/)"

SQL = f"""
WITH fresh AS (
  SELECT
    coalesce(concat(get(filter(l.ids, x -> x.namespace = 'html.gz'), 0).id, '.html.gz'),
             get(filter(l.ids, x -> x.namespace = 'docs.html'), 0).id)          AS file_key,
    coalesce(get(filter(l.ids, x -> x.namespace = 'pmh'), 0).id,
             regexp_replace(lower(get(filter(l.ids, x -> x.namespace = 'doi'), 0).id),
                            '{DOI_PREFIX}', ''),
             l.native_id)                                                        AS work_key,
    CASE WHEN size(filter(l.ids, x -> x.namespace = 'pmh')) > 0 THEN 'pmh'
         WHEN size(filter(l.ids, x -> x.namespace = 'doi')) > 0 THEN 'doi'
         ELSE 'native' END                                                       AS work_key_ns,
    l.native_id, l.native_id_namespace,
    get(filter(l.urls, u -> u.content_type = 'pdf'), 0).url                      AS pdf_url,
    l.publisher,
    row_number() OVER (PARTITION BY
      coalesce(get(filter(l.ids, x -> x.namespace = 'pmh'), 0).id,
               regexp_replace(lower(get(filter(l.ids, x -> x.namespace = 'doi'), 0).id),
                              '{DOI_PREFIX}', ''),
               l.native_id)
      ORDER BY l.ingested_at DESC)                                               AS rn
  FROM openalex.landing_page.landing_page_works l
  WHERE l.created_date >= '2026-07-27'
    AND size(filter(l.urls, u -> u.content_type = 'pdf')) > 0
),
cand AS (SELECT * FROM fresh WHERE rn = 1 AND file_key IS NOT NULL AND pdf_url IS NOT NULL),
harvested AS (
  SELECT DISTINCT
    CASE WHEN native_id_namespace = 'doi'
         THEN regexp_replace(lower(native_id), '{DOI_PREFIX}', '')
         ELSE native_id END AS work_key
  FROM openalex.taxicab.taxicab_results
  WHERE status_code = 200 AND content_type LIKE '%pdf%' AND s3_path IS NOT NULL
),
held_works AS (
  SELECT DISTINCT work_id
  FROM openalex.works.locations_mapped
  WHERE work_id IS NOT NULL
    AND (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
),
cand_work AS (
  SELECT c.work_key, lm.work_id
  FROM cand c
  JOIN openalex.works.locations_mapped lm
    ON lm.native_id = c.native_id AND lm.native_id_namespace = c.native_id_namespace
  WHERE lm.work_id IS NOT NULL
),
attempted_urls AS (SELECT DISTINCT url FROM openalex.taxicab.taxicab_results)
SELECT count(*) AS n_cohort
FROM cand c
LEFT ANTI JOIN harvested h ON c.work_key = h.work_key
LEFT ANTI JOIN (SELECT DISTINCT cw.work_key
                FROM cand_work cw JOIN held_works hw ON cw.work_id = hw.work_id) held
  ON c.work_key = held.work_key
LEFT ANTI JOIN attempted_urls a ON c.pdf_url = a.url
"""

print(run_query(SQL, size="xlarge"))
