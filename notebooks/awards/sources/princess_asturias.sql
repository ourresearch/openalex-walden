-- Mapping from CreatePrincessAsturiasAwards.ipynb (origin e22fdf7f): its awards-table SELECT over princess_asturias_input (was the
-- staging table read from s3://openalex-ingest/awards/princess_asturias/princess_asturias_laureates.parquet); created_date and updated_date are the run timestamp.
WITH
 princess_asturias_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320323780
), normalized AS (
    SELECT
        *,
        TRY_CAST(award_year AS INT) AS award_year_int,
        NULLIF(TRIM(prize_title), '') AS prize_title_norm,
        NULLIF(TRIM(citation), '') AS citation_norm,
        NULLIF(TRIM(laureate_given_name), '') AS given_name_norm,
        NULLIF(TRIM(laureate_family_name), '') AS family_name_norm,
        NULLIF(TRIM(laureate_name), '') AS laureate_name_norm,
        NULLIF(TRIM(currency), '') AS currency_norm,
        NULLIF(TRIM(landing_page_url), '') AS landing_page_url_norm
    FROM princess_asturias_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':princess-asturias:', LOWER(n.funder_award_id)))) % 9000000000 AS id,
    CONCAT(CAST(n.award_year_int AS STRING), ' ', n.prize_title_norm, ' - ', n.laureate_name_norm) AS display_name,
    n.citation_norm AS description,
    f.funder_id,
    n.funder_award_id,
    CAST(NULL AS DOUBLE) AS amount, -- prize-pattern waiver: historical per-laureate amount not exposed by source
    n.currency_norm AS currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'prize' AS funding_type,
    n.prize_title_norm AS funder_scheme,
    'princess_asturias' AS provenance,
    TRY_TO_DATE(CONCAT(CAST(n.award_year_int AS STRING), '-01-01'), 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(CONCAT(CAST(n.award_year_int AS STRING), '-12-31'), 'yyyy-MM-dd') AS end_date,
    n.award_year_int AS start_year,
    n.award_year_int AS end_year,
    struct(
        n.given_name_norm AS given_name,
        n.family_name_norm AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            CAST(NULL AS STRING) AS name,
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
    n.landing_page_url_norm AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':princess-asturias:', LOWER(n.funder_award_id)))) % 9000000000) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM normalized n
CROSS JOIN princess_asturias_funder f
WHERE n.funder_award_id IS NOT NULL
  AND n.award_year_int IS NOT NULL
  AND n.prize_title_norm IS NOT NULL
  AND n.laureate_name_norm IS NOT NULL
)
