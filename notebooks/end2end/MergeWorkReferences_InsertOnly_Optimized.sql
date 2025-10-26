-- Optimized INSERT-only approach for work_references table
-- Strategy: Filter to only works not in target table, then explode references
-- This avoids expensive joins on ref_ind and minimizes data to explode

INSERT INTO openalex.works.work_references (
  native_id,
  native_id_namespace,
  citing_work_id,
  cited_work_id,
  ref_ind,
  doi,
  pmid,
  arxiv,
  title,
  normalized_title,
  authors,
  year,
  raw,
  parsed_doi,
  parsed_first_author,
  parsed_title,
  title_author,
  provenance,
  created_timestamp,
  updated_timestamp
)
WITH works_to_process AS (
  SELECT 
    lm.native_id,
    lm.native_id_namespace,
    lm.work_id as citing_work_id,
    lm.provenance,
    posexplode(lm.references) as (ref_ind, ref)
  FROM openalex.works.locations_mapped lm
  LEFT ANTI JOIN openalex.works.work_references wr
    ON lm.work_id = wr.citing_work_id
)
SELECT 
  native_id,
  native_id_namespace,
  citing_work_id,
  CAST(null as BIGINT), -- cited_work_id (calculated later)
  ref_ind,
  ref.doi as doi,
  ref.pmid as pmid,
  ref.arxiv as arxiv,
  ref.title as title,
  CAST(null as STRING), -- normalized_title (calculated later)
  ref.authors as authors,
  ref.year as year,
  ref.raw as raw,
  CAST(null as STRING), -- parsed_doi (calculated later)
  CAST(null as STRING), -- parsed_first_author (calculated later)
  CAST(null as STRING), -- parsed_title (calculated later)
  CAST(null as STRING), -- title_author (calculated later)
  provenance,
  current_timestamp() as created_timestamp,
  current_timestamp() as updated_timestamp
FROM works_to_process;

-- For full re-processing of specific works, run this first:
-- DELETE FROM openalex.works.work_references
-- WHERE citing_work_id IN (SELECT work_id FROM openalex.works.locations_mapped WHERE ...);

-- ============================================================
-- STEP 2: Link references to cited works via DOI matching
-- ============================================================
-- Update cited_work_id and title_author for references where DOI matches work_id_map
-- Prefer paper_id, fall back to id within work_id_map
-- Only updates records where cited_work_id is still NULL

MERGE INTO openalex.works.work_references AS target
USING (
  SELECT 
    lower(doi) as doi,
    min(paper_id) as paper_id,
    min(id) as work_id,
    min(title_author) as title_author
  FROM openalex.works.work_id_map
  WHERE doi IS NOT NULL
  GROUP BY lower(doi)
) AS source
ON lower(target.doi) = source.doi
WHEN MATCHED AND target.cited_work_id IS NULL
THEN UPDATE SET 
  target.cited_work_id = COALESCE(source.paper_id, source.work_id),
  target.title_author = source.title_author,
  target.updated_timestamp = current_timestamp();
