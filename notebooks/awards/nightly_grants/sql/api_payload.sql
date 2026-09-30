WITH
all_strings AS (
  SELECT DISTINCT
    award_id,
    provenance,
    raw,
    CASE WHEN provenance IN ('nih_exporter', 'nsf_award_search', 'kaken') THEN 0.1 ELSE 0.3 END AS thresh
  FROM (
    SELECT id AS award_id, provenance, lead_investigator.affiliation.name AS raw
    FROM {AWARDS}
    WHERE provenance NOT IN (
      'kaken', 'crossref_work.grants', 'crossref_work_funders',
      'gateway_to_research', 'usaspending', 'rwjf_grants_explorer',
      'argentina_mincyt', 'openaire_fwf', 'researchfi', 'nobelprize_api'
    )
      AND lead_investigator.affiliation.name IS NOT NULL
    UNION ALL
    SELECT id, provenance, co_lead_investigator.affiliation.name
    FROM {AWARDS}
    WHERE provenance NOT IN (
      'kaken', 'crossref_work.grants', 'crossref_work_funders',
      'gateway_to_research', 'usaspending', 'rwjf_grants_explorer',
      'argentina_mincyt', 'openaire_fwf', 'researchfi', 'nobelprize_api'
    )
      AND co_lead_investigator.affiliation.name IS NOT NULL
    UNION ALL
    SELECT id, provenance, inv_struct.affiliation.name
    FROM {AWARDS}
    LATERAL VIEW OUTER EXPLODE(investigators) inv AS inv_struct
    WHERE provenance NOT IN (
      'kaken', 'crossref_work.grants', 'crossref_work_funders',
      'gateway_to_research', 'usaspending', 'rwjf_grants_explorer',
      'argentina_mincyt', 'openaire_fwf', 'researchfi', 'nobelprize_api'
    )
      AND inv_struct.affiliation.name IS NOT NULL
    UNION ALL
    SELECT oa.id, oa.provenance, kpr.institution
    FROM {AWARDS} oa
    JOIN kaken_projects_v kpr ON oa.funder_award_id = kpr.project_id
    WHERE oa.provenance = 'kaken' AND kpr.institution IS NOT NULL
  )
  WHERE raw IS NOT NULL
    AND raw NOT LIKE 'Institution abroad%'
    AND LOWER(TRIM(raw)) NOT IN (
      'grantee', 'autre', 'autres', 'n/a', 'na', 'null',
      'unknown', 'none', 'tbd', 'tba', 'other', 'individual',
      'data not available', 'no institution available'  -- oxjobs #123.6: NOPL junk magnet
    )
),
disambiguated AS (
  SELECT
    s.award_id,
    CASE
      WHEN SIZE(asl.institution_ids_override) > 0
           AND NOT ARRAY_CONTAINS(asl.institution_ids_override, -1)
        THEN asl.institution_ids_override
      ELSE TRANSFORM(
        FILTER(asl.model_response, x -> x.score >= s.thresh),
        x -> CAST(x.id AS BIGINT)
      )
    END AS ids
  FROM all_strings s
  JOIN affiliation_lookup_v asl
    ON s.raw = asl.raw_affiliation_string
),
exploded AS (
  SELECT d.award_id, ie.institution_id
  FROM disambiguated d
  LATERAL VIEW EXPLODE(d.ids) ie AS institution_id
  WHERE ie.institution_id IS NOT NULL
),
deduped AS (SELECT DISTINCT award_id, institution_id FROM exploded),
institution_awarded_per_award AS (
  SELECT
    d.award_id,
    COLLECT_LIST(STRUCT(
      CONCAT('https://openalex.org/I', CAST(i.id AS STRING)) AS id,
      i.display_name,
      i.ror,
      i.country_code,
      i.type,
      i.lineage
    )) AS institution_awarded
  FROM deduped d
  JOIN institutions_api_v i ON d.institution_id = i.id
  GROUP BY d.award_id
)
SELECT
  oa.id, oa.display_name, oa.description, oa.funder_id, oa.funder_award_id,
  oa.amount, oa.currency, oa.funder, oa.funding_type, oa.funder_scheme,
  oa.provenance, oa.start_date, oa.end_date, oa.start_year, oa.end_year,
  oa.lead_investigator, oa.co_lead_investigator, oa.investigators,
  oa.landing_page_url, oa.doi, oa.works_api_url,
  DATE_TRUNC('SECOND', CAST(oa.created_date AS TIMESTAMP)) AS created_date,
  CAST(NULL AS TIMESTAMP) AS updated_date,
  oa.funded_outputs,
  oa.funded_outputs_count,
  oa.primary_topic,
  oa.topics,
  COALESCE(iap.institution_awarded, ARRAY()) AS institution_awarded,
  -- Denormalized siblings (object mapping in v4) for fast term filtering — see oxjob #123.2.
  -- Same content as the rich nested versions; v4 mapping declares the rich ones as nested
  -- and the _full ones as object with explicit keyword id fields. Mirrors the works
  -- authorships/authorships_full pattern.
  COALESCE(iap.institution_awarded, ARRAY()) AS institution_awarded_full,
  oa.primary_topic AS primary_topic_full,
  oa.topics AS topics_full
FROM {AWARDS} oa
LEFT JOIN institution_awarded_per_award iap ON oa.id = iap.award_id
