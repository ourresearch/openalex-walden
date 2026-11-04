-- Mapping from CreateTWASAwards.ipynb (origin e22fdf7f): its awards-table SELECT over twas_awards_input (was the
-- staging table read from s3://openalex-ingest/awards/twas_awards/twas_awards.parquet); created_date and updated_date are the run timestamp.
WITH
 twas_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321078
),
normalized AS (
    SELECT
        r.*,
        TRY_CAST(r.award_year AS INT) AS award_year_int,
        TRY_CAST(r.amount_per_laureate AS DOUBLE) AS amount_double,
        NULLIF(TRIM(r.currency), '') AS currency_norm,
        NULLIF(TRIM(r.award_name), '') AS award_name_norm,
        NULLIF(TRIM(r.award_field), '') AS award_field_norm,
        NULLIF(TRIM(r.citation), '') AS citation_norm,
        NULLIF(TRIM(r.description), '') AS description_norm,
        NULLIF(TRIM(r.laureate_name), '') AS laureate_name_norm,
        NULLIF(TRIM(r.laureate_given_name), '') AS given_name_norm,
        NULLIF(TRIM(r.laureate_family_name), '') AS family_name_norm,
        NULLIF(TRIM(r.laureate_affiliation_or_context), '') AS affiliation_norm,
        NULLIF(TRIM(r.laureate_country_or_nationality), '') AS country_norm,
        NULLIF(TRIM(r.landing_page_url), '') AS landing_page_url_norm
    FROM twas_awards_input r
),
awards AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':twas-awards:', LOWER(n.funder_award_id)))) % 9000000000 AS id,
        CONCAT(CAST(n.award_year_int AS STRING), ' ', n.award_name_norm, ' - ', n.laureate_name_norm) AS display_name,
        COALESCE(n.citation_norm, n.description_norm) AS description,
        f.funder_id,
        n.funder_award_id,
        n.amount_double AS amount,
        n.currency_norm AS currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,
        'prize' AS funding_type,
        n.award_name_norm AS funder_scheme,
        'twas_awards' AS provenance,
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
                n.affiliation_norm AS name,
                n.country_norm AS country,
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
        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM normalized n
    CROSS JOIN twas_funder f
    WHERE n.funder_award_id IS NOT NULL
      AND n.award_year_int IS NOT NULL
      AND n.award_name_norm IS NOT NULL
      AND n.award_field_norm IS NOT NULL
      AND n.laureate_name_norm IS NOT NULL
),

awards_transformed AS (
SELECT
    id,
    display_name,
    description,
    funder_id,
    funder_award_id,
    amount,
    currency,
    funder,
    funding_type,
    funder_scheme,
    provenance,
    start_date,
    end_date,
    start_year,
    end_year,
    lead_investigator,
    co_lead_investigator,
    investigators,
    landing_page_url,
    doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G', CAST(id AS STRING)) AS works_api_url,
    created_date,
    updated_date
FROM awards
)
