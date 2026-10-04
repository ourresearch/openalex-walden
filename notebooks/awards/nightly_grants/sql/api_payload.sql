WITH
-- Country lookup (country/award_country_lookup.csv): the free-text `affiliation.country` of each source -> ISO 3166-1
-- alpha-2. `code`: 'provenance|value' or '*|value' -> the code ('' when the row gives none). `usable_source`: provenance ->
-- whether that source's country field holds the country of the named organisation (its '*' row in the file).
country_lookup AS (
  {COUNTRY_LOOKUP}
),
country_map AS (
  SELECT
    MAP_FROM_ENTRIES(COLLECT_LIST(CASE WHEN value <> '*' THEN STRUCT(
      CONCAT(provenance_scope, '|', value) AS k,
      CASE WHEN confidence IN ('high', 'medium') THEN iso2 ELSE '' END AS v) END)) AS code,
    MAP_FROM_ENTRIES(COLLECT_LIST(CASE WHEN value = '*' THEN STRUCT(
      provenance_scope AS k, confidence IN ('high', 'medium') AS v) END)) AS usable_source
  FROM country_lookup
),
-- Country code of every affiliation on the award. A row for this provenance and value decides on its own (RWJF's 'MA' is
-- Massachusetts). Otherwise the source must be usable (IDRC stores the project's recipient country, NSF the place of
-- performance, Humboldt a constant 'Germany': none is) and the code is the '*'-scoped row for the value. A source the
-- file does not describe gives none.
-- NIH intramural projects (activity codes Z01, ZIA, ZIC, ...) run inside NIH's own institutes and the source leaves their
-- country empty: US. On 2026-10-02 all 116,926 such awards had either no country (59,975) or UNITED STATES.
award_country AS (
  SELECT
    oa.id AS award_id, oa.provenance, oa.funder_award_id, oa.lead_investigator, oa.co_lead_investigator, oa.investigators,
    COALESCE(
      NULLIF(COALESCE(
        TRY_ELEMENT_AT(cm.code, CONCAT(oa.provenance, '|', LOWER(TRIM(oa.lead_investigator.affiliation.country)))),
        CASE WHEN TRY_ELEMENT_AT(cm.usable_source, oa.provenance)
             THEN TRY_ELEMENT_AT(cm.code, CONCAT('*|', LOWER(TRIM(oa.lead_investigator.affiliation.country)))) END), ''),
      CASE WHEN oa.provenance = 'nih_exporter' AND oa.funder_award_id RLIKE '^[0-9]?[Zz]'
                AND oa.lead_investigator.affiliation.name IS NOT NULL
                AND NULLIF(TRIM(oa.lead_investigator.affiliation.country), '') IS NULL THEN 'US' END
    ) AS lead_country_code,
    NULLIF(COALESCE(
        TRY_ELEMENT_AT(cm.code, CONCAT(oa.provenance, '|', LOWER(TRIM(oa.co_lead_investigator.affiliation.country)))),
        CASE WHEN TRY_ELEMENT_AT(cm.usable_source, oa.provenance)
             THEN TRY_ELEMENT_AT(cm.code, CONCAT('*|', LOWER(TRIM(oa.co_lead_investigator.affiliation.country)))) END), '') AS co_lead_country_code,
    TRANSFORM(oa.investigators, x ->
      NULLIF(COALESCE(
        TRY_ELEMENT_AT(cm.code, CONCAT(oa.provenance, '|', LOWER(TRIM(x.affiliation.country)))),
        CASE WHEN TRY_ELEMENT_AT(cm.usable_source, oa.provenance)
             THEN TRY_ELEMENT_AT(cm.code, CONCAT('*|', LOWER(TRIM(x.affiliation.country)))) END), '')
    ) AS investigator_country_codes
  FROM {AWARDS} oa
  CROSS JOIN country_map cm
),
-- Each affiliation name on the award with the country code recorded next to it.
all_strings AS (
  SELECT DISTINCT
    award_id,
    provenance,
    raw,
    record_country,
    CASE WHEN provenance IN ('nih_exporter', 'nsf_award_search', 'kaken') THEN 0.1 ELSE 0.3 END AS thresh
  FROM (
    SELECT award_id, provenance, lead_investigator.affiliation.name AS raw, lead_country_code AS record_country
    FROM award_country
    WHERE provenance NOT IN (
      'kaken', 'crossref_work.grants', 'crossref_work_funders',
      'gateway_to_research', 'usaspending', 'rwjf_grants_explorer',
      'argentina_mincyt', 'openaire_fwf', 'researchfi', 'nobelprize_api'
    )
      AND lead_investigator.affiliation.name IS NOT NULL
    UNION ALL
    SELECT award_id, provenance, co_lead_investigator.affiliation.name, co_lead_country_code
    FROM award_country
    WHERE provenance NOT IN (
      'kaken', 'crossref_work.grants', 'crossref_work_funders',
      'gateway_to_research', 'usaspending', 'rwjf_grants_explorer',
      'argentina_mincyt', 'openaire_fwf', 'researchfi', 'nobelprize_api'
    )
      AND co_lead_investigator.affiliation.name IS NOT NULL
    UNION ALL
    SELECT award_id, provenance, inv_struct.affiliation.name, investigator_country_codes[inv_pos]
    FROM award_country
    LATERAL VIEW OUTER POSEXPLODE(investigators) inv AS inv_pos, inv_struct
    WHERE provenance NOT IN (
      'kaken', 'crossref_work.grants', 'crossref_work_funders',
      'gateway_to_research', 'usaspending', 'rwjf_grants_explorer',
      'argentina_mincyt', 'openaire_fwf', 'researchfi', 'nobelprize_api'
    )
      AND inv_struct.affiliation.name IS NOT NULL
    UNION ALL
    SELECT oa.award_id, oa.provenance, kpr.institution, oa.lead_country_code
    FROM award_country oa
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
-- Institution ids for each name. First the affiliation matcher's answer for the string ({ANSWERS}: the table works have
-- read since 2026-09-28, oxjob #1386; an empty answer means "names no institution" and is final). Strings it has not
-- answered keep the legacy route: hand-rules override, else the 2023 model's candidates at or above the threshold.
-- A NaN score never passes: in Spark NaN compares above every number, which gave 318 names (mostly Japanese and
-- Chinese) the same five institutions.
disambiguated AS (
  SELECT
    s.award_id,
    s.provenance,
    s.raw,
    s.record_country,
    ans.raw_affiliation_string IS NOT NULL AS from_matcher,
    CASE
      WHEN ans.raw_affiliation_string IS NOT NULL THEN COALESCE(ans.institution_ids, ARRAY())
      WHEN SIZE(asl.institution_ids_override) > 0
           AND NOT ARRAY_CONTAINS(asl.institution_ids_override, -1)
        THEN asl.institution_ids_override
      ELSE TRANSFORM(
        FILTER(asl.model_response, x -> x.score >= s.thresh AND NOT ISNAN(x.score)),
        x -> CAST(x.id AS BIGINT)
      )
    END AS ids
  FROM all_strings s
  LEFT JOIN {ANSWERS} ans
    ON s.raw = ans.raw_affiliation_string
  LEFT JOIN affiliation_lookup_v asl
    ON s.raw = asl.raw_affiliation_string
  WHERE ans.raw_affiliation_string IS NOT NULL OR asl.raw_affiliation_string IS NOT NULL
),
exploded AS (
  SELECT d.award_id, d.provenance, d.raw, d.record_country, d.from_matcher, ie.institution_id
  FROM disambiguated d
  LATERAL VIEW EXPLODE(d.ids) ie AS institution_id
  WHERE ie.institution_id IS NOT NULL
),
-- Dependent territories count as their sovereign state for the guard: NIH records Puerto Rico's universities under
-- UNITED STATES, Erasmus+ records the Universite de La Reunion under FR. Palau, Micronesia and the Marshall Islands are
-- grouped with the US because US federal sources list them as domestic.
sovereign AS (
  SELECT * FROM VALUES
    ('PR', 'US'), ('VI', 'US'), ('GU', 'US'), ('AS', 'US'), ('MP', 'US'), ('UM', 'US'), ('PW', 'US'), ('FM', 'US'), ('MH', 'US'),
    ('GF', 'FR'), ('GP', 'FR'), ('MQ', 'FR'), ('RE', 'FR'), ('YT', 'FR'), ('NC', 'FR'), ('PF', 'FR'), ('PM', 'FR'), ('BL', 'FR'),
    ('MF', 'FR'), ('WF', 'FR'), ('TF', 'FR'),
    ('GI', 'GB'), ('BM', 'GB'), ('KY', 'GB'), ('VG', 'GB'), ('TC', 'GB'), ('MS', 'GB'), ('AI', 'GB'), ('FK', 'GB'), ('SH', 'GB'),
    ('PN', 'GB'), ('IO', 'GB'), ('GS', 'GB'), ('JE', 'GB'), ('GG', 'GB'), ('IM', 'GB'),
    ('AW', 'NL'), ('CW', 'NL'), ('SX', 'NL'), ('BQ', 'NL'),
    ('GL', 'DK'), ('FO', 'DK'), ('SJ', 'NO'), ('BV', 'NO'), ('AX', 'FI'),
    ('NF', 'AU'), ('CX', 'AU'), ('CC', 'AU'), ('HM', 'AU'), ('CK', 'NZ'), ('NU', 'NZ'), ('TK', 'NZ'),
    ('HK', 'CN'), ('MO', 'CN')
  AS sovereign(territory, state)
),
-- Country-consistency guard: when the source recorded the organisation's country next to the name, a matched
-- institution must be in that country (or in a territory of it, or the reverse). A match is kept when either side has no
-- country. An institution reached from two names on the same award is kept if either agrees. An ancestor in the right
-- country does not rescue a match: "FRED HUTCHINSON CANCER CENTER", UNITED STATES was matched to its Cape Town
-- laboratory that way.
guarded AS (
  SELECT
    e.award_id,
    e.provenance,
    e.raw,
    e.from_matcher,
    e.institution_id,
    e.record_country,
    i.country_code AS institution_country,
    (e.record_country IS NULL OR i.country_code IS NULL
      OR COALESCE(si.state, i.country_code) = COALESCE(sr.state, e.record_country)) AS country_ok
  FROM exploded e
  JOIN institutions_api_v i ON e.institution_id = i.id
  LEFT JOIN sovereign si ON si.territory = i.country_code
  LEFT JOIN sovereign sr ON sr.territory = e.record_country
),
deduped AS (SELECT DISTINCT award_id, institution_id FROM guarded WHERE country_ok),
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
  oa.topics AS topics_full,
  -- Sub-award links (award_relations.py). *_full = uncapped object siblings for search filters.
  oa.parent_awards,
  oa.sub_awards,
  oa.sub_awards_count,
  oa.parent_awards_full,
  oa.sub_awards_full
FROM {AWARDS} oa
LEFT JOIN institution_awarded_per_award iap ON oa.id = iap.award_id
