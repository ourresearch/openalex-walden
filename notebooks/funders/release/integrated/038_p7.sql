-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- READ-ONLY for served API/hash: materialize exact scheduled-owner SELECT into scratch
CREATE TABLE openalex.funders.funders_api_cutover_check AS
WITH
-- Base from funders array (for work-funder relationships)
wf_base AS (
  SELECT
    CAST(REPLACE(funder.id, 'https://openalex.org/F', '') AS BIGINT) AS funder_id,
    CAST(w.id AS BIGINT) AS work_id
  FROM openalex.works.openalex_works w
  LATERAL VIEW EXPLODE(w.funders) AS funder
  WHERE funder.id IS NOT NULL 
    AND w.id IS NOT NULL
    AND funder.id LIKE 'https://openalex.org/F%'
),
-- Enrich with work-level signals needed for metrics
wf_enriched AS (
  SELECT
    b.funder_id,
    b.work_id,
    COALESCE(w.publication_year, YEAR(w.publication_date)) AS pub_year,
    CAST(w.cited_by_count AS INT) AS cited_by_count,
    CAST(w.open_access.is_oa AS BOOLEAN) AS is_oa
  FROM wf_base b
  JOIN openalex.works.openalex_works w
    ON w.id = b.work_id
),
funder_unique_awards AS (
  SELECT
    funder_id,
    COUNT(*) AS unique_awards_count
  FROM openalex.awards.openalex_awards
  WHERE funder_id IS NOT NULL
  GROUP BY funder_id
),
-- Per-funder metrics (works, citations, OA, grants; plus helpers for summary_stats)
funder_metrics AS (
  SELECT
    funder_id,
    /* counts over unique works */
    CAST(COUNT(DISTINCT work_id) AS INT) AS works_count,
    CAST(SUM(cited_by_count) AS INT)     AS cited_by_count,
    CAST(SUM(CASE WHEN is_oa THEN 1 ELSE 0 END) AS INT) AS oa_works_count,
    /* summary_stats helpers */
    CAST(AVG(CASE WHEN pub_year >= YEAR(current_date()) - 2 THEN cited_by_count END) AS DOUBLE) AS two_year_mean,
    CAST(COUNT_IF(cited_by_count >= 10) AS INT) AS i10_index,
    SORT_ARRAY(
      TRANSFORM(
        FILTER(COLLECT_LIST(cited_by_count), x -> x IS NOT NULL),
        x -> CAST(x AS INT)
      ),
      false
    ) AS sorted_citations
  FROM wf_enriched
  GROUP BY funder_id
),
-- Funder Roles CTE
funder_roles AS (
  WITH entity_links_expanded AS (
    SELECT 
      f.funder_id AS funder_id,
      el.id_1,
      el.id_2
    FROM openalex.funders.funders f
    INNER JOIN openalex.mid.entity_link el 
      ON (el.id_1 = CONCAT('F', f.funder_id) OR el.id_2 = CONCAT('F', f.funder_id))
  ),
  all_entity_ids AS (
    -- Funder's own role
    SELECT 
      f.funder_id AS funder_id,
      CONCAT('F', f.funder_id) AS entity_id,
      'funder' AS role
    FROM openalex.funders.funders f
    
    UNION ALL
    
    -- Linked institution/publisher roles
    SELECT 
      funder_id,
      CASE 
        WHEN id_1 LIKE 'F%' THEN id_2 
        ELSE id_1 
      END AS entity_id,
      CASE 
        WHEN id_1 LIKE 'I%' OR id_2 LIKE 'I%' THEN 'institution'
        WHEN id_1 LIKE 'P%' OR id_2 LIKE 'P%' THEN 'publisher'
      END AS role
    FROM entity_links_expanded
    WHERE (id_1 LIKE 'I%' OR id_1 LIKE 'P%' OR id_2 LIKE 'I%' OR id_2 LIKE 'P%')
  ),
  roles_with_counts AS (
    SELECT 
      ae.funder_id,
      ae.role,
      CONCAT('https://openalex.org/', ae.entity_id) AS id,
      CASE 
        -- For funder: use works_count from funder_metrics
        WHEN ae.role = 'funder' THEN fm.works_count
        -- For institution: join to institutions_api table
        WHEN ae.role = 'institution' THEN i_api.works_count
        -- For publisher: join to publishers_api table
        WHEN ae.role = 'publisher' THEN p_api.works_count
      END AS works_count
    FROM all_entity_ids ae
    -- Join to funder_metrics for funder works_count
    LEFT JOIN funder_metrics fm 
      ON ae.role = 'funder' AND ae.funder_id = fm.funder_id
    -- Join to institutions_api for institution works_count
    LEFT JOIN openalex.institutions.institutions_api i_api
      ON ae.role = 'institution' AND ae.entity_id = CONCAT('I', i_api.id)
    -- Join to publishers_api for publisher works_count
    LEFT JOIN openalex.publishers.publishers_api p_api
      ON ae.role = 'publisher' AND ae.entity_id = CONCAT('P', p_api.id)
  ),
  deduplicated_roles AS (
    SELECT 
      funder_id,
      role,
      id,
      CAST(COALESCE(works_count, 0) AS INT) AS works_count,
      ROW_NUMBER() OVER (
        PARTITION BY funder_id, role 
        ORDER BY works_count DESC, id
      ) AS rn
    FROM roles_with_counts
  )
  SELECT 
    funder_id,
    COLLECT_LIST(
      STRUCT(role, id, works_count)
    ) AS roles
  FROM deduplicated_roles
  WHERE rn = 1  -- Keep only one per role type
  GROUP BY funder_id
)

SELECT
  f.funder_id AS id,
  f.display_name,
  from_json(f.alternate_titles, 'ARRAY<STRING>') AS alternate_titles,
  f.country_code,
  f.description,
  f.homepage_url,
  f.image_url,
  f.image_thumbnail_url,
  STRUCT(
    CONCAT('https://openalex.org/F', f.funder_id) AS openalex,
    f.ror_id       AS ror,
    f.wikidata_id  AS wikidata,
    f.crossref_id  AS crossref,
    f.doi          AS doi
  ) AS ids,

  /* totals */
  COALESCE(m.works_count, 0)      AS works_count,
  COALESCE(m.cited_by_count, 0)   AS cited_by_count,
  COALESCE(fua.unique_awards_count, 0) AS awards_count,

  -- Roles
  COALESCE(fr.roles, ARRAY()) AS roles,

  /* counts_by_year */
  (
    SELECT
      SORT_ARRAY(
        COLLECT_LIST(
          STRUCT(
            year,
            works_count,
            oa_works_count,
            cited_by_count
          )
        ),
        false
      )
    FROM (
      SELECT
        CAST(e.pub_year AS INT) AS year,
        CAST(COUNT(DISTINCT e.work_id) AS INT) AS works_count,
        CAST(SUM(CASE WHEN e.is_oa THEN 1 ELSE 0 END) AS INT) AS oa_works_count,
        CAST(SUM(e.cited_by_count) AS INT) AS cited_by_count
      FROM wf_enriched e
      WHERE e.funder_id = f.funder_id
        AND e.pub_year IS NOT NULL
      GROUP BY CAST(e.pub_year AS INT)
    )
  ) AS counts_by_year,

  /* summary_stats */
  NAMED_STRUCT(
    '2yr_mean_citedness', COALESCE(m.two_year_mean, 0.0),
    'h_index',
      CAST(
        ARRAY_MAX(
          ZIP_WITH(
            m.sorted_citations,
            SEQUENCE(1, SIZE(m.sorted_citations)),
            (citation, rank) -> IF(citation >= rank, rank, 0)
          )
        ) AS INT
      ),
    'i10_index', COALESCE(m.i10_index, 0)
  ) AS summary_stats,

  /* dates */
  DATE_TRUNC('SECOND', CAST(f.created_date AS TIMESTAMP)) AS created_date,
  CAST(NULL AS TIMESTAMP) AS updated_date

FROM openalex.funders.funders f
LEFT JOIN funder_metrics m
  ON m.funder_id = f.funder_id
LEFT JOIN funder_unique_awards fua
  ON f.funder_id = fua.funder_id
LEFT JOIN funder_roles fr ON f.funder_id = fr.funder_id
-- merge losers (MergeFunders.ipynb alias rows) stay in openalex.funders.funders so DOI/name
-- matching keeps resolving, but must not surface as API entities
WHERE f.merge_into_id IS NULL;
