-- Mapping from CreateCanadaCouncilPrizesAwards.ipynb (origin e22fdf7f): its awards-table SELECT over canada_council_prizes_input (was the
-- staging table read from s3://openalex-ingest/awards/canada_council_prizes/canada_council_prizes.parquet); created_date and updated_date are the run timestamp.
WITH
 canada_council_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320319951
), normalized AS (
    SELECT
        *,
        TRY_CAST(year AS INT) AS award_year_int,
        NULLIF(TRIM(funder_award_id), '') AS funder_award_id_norm,
        NULLIF(TRIM(prize_name), '') AS prize_name_norm,
        NULLIF(TRIM(category), '') AS category_norm,
        NULLIF(TRIM(language), '') AS language_norm,
        NULLIF(TRIM(laureate_name), '') AS laureate_name_norm,
        NULLIF(TRIM(laureate_given_name), '') AS given_name_norm,
        NULLIF(TRIM(laureate_family_name), '') AS family_name_norm,
        LOWER(NULLIF(TRIM(is_organization_like), '')) = 'true' AS is_organization_like_bool,
        NULLIF(TRIM(work_title), '') AS work_title_norm,
        NULLIF(TRIM(source_description), '') AS source_description_norm,
        NULLIF(TRIM(raw_entry_text), '') AS raw_entry_text_norm,
        NULLIF(TRIM(prize_url), '') AS prize_url_norm,
        NULLIF(TRIM(currency), '') AS currency_norm
    FROM canada_council_prizes_input
), awards AS (
    SELECT
        abs(xxhash64(CONCAT(
            TRY_CAST(f.funder_id AS STRING), ':canada-council:', LOWER(n.funder_award_id_norm)
        ))) % 9000000000 AS id,
        CONCAT(
            CAST(n.award_year_int AS STRING), ' ', n.prize_name_norm, ' - ', n.laureate_name_norm,
            CASE WHEN n.work_title_norm IS NOT NULL THEN CONCAT(' (', n.work_title_norm, ')') ELSE '' END
        ) AS display_name,
        NULLIF(CONCAT_WS(
            ' ',
            n.source_description_norm,
            CASE WHEN n.category_norm IS NOT NULL THEN CONCAT('Category: ', n.category_norm, '.') END,
            CASE WHEN n.language_norm IS NOT NULL THEN CONCAT('Language: ', n.language_norm, '.') END,
            CASE WHEN n.work_title_norm IS NOT NULL THEN CONCAT('Winning work: ', n.work_title_norm, '.') END,
            CASE WHEN n.raw_entry_text_norm IS NOT NULL THEN CONCAT('Source entry: ', n.raw_entry_text_norm) END
        ), '') AS description,
        f.funder_id,
        n.funder_award_id_norm AS funder_award_id,
        CAST(NULL AS DOUBLE) AS amount,
        COALESCE(n.currency_norm, 'CAD') AS currency,
        struct(
            CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
            f.display_name, f.ror_id, f.doi
        ) AS funder,
        'prize' AS funding_type,
        CONCAT_WS(' - ', n.prize_name_norm, n.category_norm, n.language_norm) AS funder_scheme,
        'canada_council_prizes' AS provenance,
        TRY_TO_DATE(CONCAT(CAST(n.award_year_int AS STRING), '-01-01'), 'yyyy-MM-dd') AS start_date,
        TRY_TO_DATE(CONCAT(CAST(n.award_year_int AS STRING), '-12-31'), 'yyyy-MM-dd') AS end_date,
        n.award_year_int AS start_year,
        n.award_year_int AS end_year,
        struct(
            CASE
                WHEN n.is_organization_like_bool THEN n.laureate_name_norm
                ELSE COALESCE(n.given_name_norm, n.laureate_name_norm)
            END AS given_name,
            CASE
                WHEN n.is_organization_like_bool THEN CAST(NULL AS STRING)
                ELSE n.family_name_norm
            END AS family_name,
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
        n.prize_url_norm AS landing_page_url,
        CAST(NULL AS STRING) AS doi,
        CONCAT('https://api.openalex.org/works?filter=awards.id:G',
            TRY_CAST(abs(xxhash64(CONCAT(
                TRY_CAST(f.funder_id AS STRING), ':canada-council:', LOWER(n.funder_award_id_norm)
            ))) % 9000000000 AS STRING)
        ) AS works_api_url,
        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM normalized n
    CROSS JOIN canada_council_funder f
    WHERE n.funder_award_id_norm IS NOT NULL
      AND n.award_year_int IS NOT NULL
      AND n.prize_name_norm IS NOT NULL
      AND n.laureate_name_norm IS NOT NULL
),

awards_transformed AS (
SELECT *
FROM awards
)
