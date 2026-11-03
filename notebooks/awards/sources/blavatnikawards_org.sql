-- Mapping from CreateBlavatnikAwards.ipynb (origin e22fdf7f): its awards-table SELECT over blavatnikawards_org_input (was the
-- staging table read from s3://openalex-ingest/awards/blavatnik/blavatnik_awards.parquet); created_date and updated_date are the run timestamp.
WITH
 blavatnik_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320312914
), normalized AS (
    SELECT
        *,
        TRY_CAST(award_year AS INT) AS award_year_int,
        TRY_CAST(amount_usd AS DOUBLE) AS amount_double,
        NULLIF(TRIM(currency), '') AS currency_norm,
        NULLIF(TRIM(citation), '') AS citation_norm,
        NULLIF(TRIM(institution), '') AS institution_norm,
        NULLIF(TRIM(current_position), '') AS current_position_norm,
        NULLIF(TRIM(laureate_given_name), '') AS given_name_norm,
        NULLIF(TRIM(laureate_family_name), '') AS family_name_norm,
        NULLIF(TRIM(laureate_name), '') AS laureate_name_norm,
        NULLIF(TRIM(region), '') AS region_norm,
        NULLIF(TRIM(role), '') AS role_norm,
        NULLIF(TRIM(status), '') AS status_norm
    FROM blavatnikawards_org_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':blavatnik:', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT(
        CAST(n.award_year_int AS STRING), ' Blavatnik ',
        COALESCE(n.region_norm, 'Unknown'), ' Award ',
        COALESCE(n.status_norm, 'Honoree'),
        CASE WHEN n.role_norm IS NOT NULL THEN CONCAT(' - ', n.role_norm) ELSE '' END,
        ' - ', n.laureate_name_norm
    ) AS display_name,
    CASE
        WHEN TRY_CAST(n.declined AS BOOLEAN) AND n.citation_norm IS NOT NULL
            THEN CONCAT('Declined the prize. ', n.citation_norm)
        WHEN TRY_CAST(n.declined AS BOOLEAN) THEN 'Declined the prize.'
        ELSE n.citation_norm
    END AS description,
    f.funder_id,
    n.funder_award_id,
    n.amount_double AS amount,
    n.currency_norm AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name, f.ror_id, f.doi
    ) AS funder,
    'prize' AS funding_type,
    CONCAT(
        'Blavatnik ', COALESCE(n.region_norm, 'Unknown'), ' Award - ',
        COALESCE(n.role_norm, 'Honoree'), ' ',
        COALESCE(n.status_norm, 'Honoree')
    ) AS funder_scheme,
    'blavatnikawards_org' AS provenance,
    TRY_TO_DATE(CONCAT(CAST(n.award_year_int AS STRING), '-09-01'), 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(CONCAT(CAST(n.award_year_int AS STRING), '-09-30'), 'yyyy-MM-dd') AS end_date,
    n.award_year_int AS start_year,
    n.award_year_int AS end_year,
    struct(
        n.given_name_norm AS given_name,
        n.family_name_norm AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            n.institution_norm AS name,
            CAST(NULL AS STRING) AS country,
            CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
        ) AS affiliation
    ) AS lead_investigator,
    CAST(NULL AS STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >) AS co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >>) AS investigators,
    n.profile_url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':blavatnik:', LOWER(n.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM normalized n
CROSS JOIN blavatnik_funder f
WHERE n.funder_award_id IS NOT NULL
  AND n.award_year_int IS NOT NULL
  AND n.laureate_name_norm IS NOT NULL
)
