-- Mapping from CreateHRCAwards.ipynb (origin e22fdf7f): its awards-table SELECT over hrc_research_repository_input (was the
-- staging table read from s3://openalex-ingest/awards/hrc_nz/hrc_nz_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320334749
),
raw_prepped AS (
    SELECT
        *,
        TRY_CAST(amount AS DOUBLE) AS amount_double,
        CASE WHEN TRY_CAST(start_year AS INT) BETWEEN 1800 AND 2100 THEN TRY_CAST(start_year AS INT) END AS start_year_int,
        NULLIF(TRIM(host_organization), '') AS leader_affiliation_name,
        TRIM(REGEXP_REPLACE(
            REGEXP_REPLACE(
                researchers_raw,
                '^((Emeritus|Distinguished|Associate|Adjunct|Clinical|Honorary)\\s+)?(Professor|Prof\\.?|Dr\\.?|Doctor|Mr\\.?|Mrs\\.?|Ms\\.?|Miss|Sir|Dame)\\s+',
                ''
            ),
            '(\\s+(DNZM|GNZM|KNZM|CNZM|ONZM|MNZM|QSO|QSM|CRSNZ|FRSNZ|FRS|FMedSci|FRSC|FRACP|FRACS|FRCP|FNZIC|CBE|OBE|MBE|KBE|DBE|CNZOM|KC|QC|JP|[A-Z]{3,}))+$',
            ''
        )) AS leader_clean_name
    FROM hrc_research_repository_input
),
raw_named AS (
    SELECT
        *,
        CASE WHEN leader_clean_name IS NOT NULL AND length(leader_clean_name) > 0
             THEN element_at(split(leader_clean_name, ' '), -1) END AS leader_family_name,
        CASE WHEN size(split(leader_clean_name, ' ')) > 1
             THEN array_join(slice(split(leader_clean_name, ' '), 1, size(split(leader_clean_name, ' ')) - 1), ' ') END AS leader_given_name
    FROM raw_prepped
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT('4320334749:', LOWER(TRIM(rp.funder_award_id))))) % 9000000000 AS id,
    rp.display_name,
    NULLIF(TRIM(rp.description), '') AS description,
    4320334749 AS funder_id,
    rp.funder_award_id,
    rp.amount_double AS amount,
    CASE WHEN rp.amount_double IS NOT NULL THEN 'NZD' ELSE NULL END AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    CASE
        WHEN rp.proposal_type ILIKE '%fellowship%' THEN 'fellowship'
        WHEN rp.proposal_type ILIKE '%scholarship%' THEN 'fellowship'
        WHEN rp.proposal_type ILIKE '%career%' THEN 'fellowship'
        WHEN rp.proposal_type ILIKE '%training%' THEN 'training'
        ELSE 'research'
    END AS funding_type,
    NULLIF(TRIM(rp.proposal_type), '') AS funder_scheme,
    'hrc_research_repository' AS provenance,
    CAST(NULL AS DATE) AS start_date,
    CAST(NULL AS DATE) AS end_date,
    rp.start_year_int AS start_year,
    CAST(NULL AS INT) AS end_year,
    CASE
        WHEN rp.leader_family_name IS NOT NULL OR rp.leader_affiliation_name IS NOT NULL THEN
            struct(
                rp.leader_given_name AS given_name,
                rp.leader_family_name AS family_name,
                CAST(NULL AS STRING) AS orcid,
                CAST(NULL AS DATE) AS role_start,
                struct(
                    rp.leader_affiliation_name AS name,
                    CASE WHEN rp.leader_affiliation_name IS NOT NULL THEN 'NZ' ELSE NULL END AS country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                ) AS affiliation
            )
        ELSE NULL
    END AS lead_investigator,
    CAST(NULL AS STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >) AS co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >>) AS investigators,
    rp.landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT('4320334749:', LOWER(TRIM(rp.funder_award_id))))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM raw_named rp
CROSS JOIN funder_resolved f
WHERE rp.funder_award_id IS NOT NULL
  AND rp.display_name IS NOT NULL
)
