-- Mapping from CreateGruberPrizesAwards.ipynb (origin e22fdf7f): its awards-table SELECT over gruber_prizes_input (was the
-- staging table read from s3://openalex-ingest/awards/gruber_prizes/gruber_prizes_awards.parquet); created_date and updated_date are the run timestamp.
WITH
 gruber_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320312392
),
normalized AS (
    SELECT
        g.*,
        TRY_CAST(g.award_year AS INT) AS parsed_award_year,
        TRY_CAST(g.source_total_award_amount AS DOUBLE) AS parsed_total_amount,
        TRY_CAST(g.laureate_count AS DOUBLE) AS parsed_laureate_count
    FROM gruber_prizes_input g
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':gruber_prizes:', LOWER(TRIM(g.funder_award_id))))) % 9000000000 AS id,
    CONCAT(g.source_title, ' - ', g.laureate_name) AS display_name,
    COALESCE(NULLIF(g.source_teaser, ''), NULLIF(g.prize_profile_text, ''), NULLIF(g.recipient_bio_text, '')) AS description,
    f.funder_id,
    g.funder_award_id AS funder_award_id,
    CASE
        WHEN g.parsed_total_amount IS NOT NULL AND g.parsed_laureate_count > 0
            THEN g.parsed_total_amount / g.parsed_laureate_count
        ELSE NULL
    END AS amount,
    NULLIF(g.source_currency, '') AS currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) AS id,
        f.display_name, f.ror_id, f.doi
    ) AS funder,
    'prize' AS funding_type,
    CONCAT('Gruber ', g.prize_category, ' Prize') AS funder_scheme,
    'gruber_prizes' AS provenance,
    TRY_TO_DATE(CONCAT(CAST(g.parsed_award_year AS STRING), '-01-01'), 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(CONCAT(CAST(g.parsed_award_year AS STRING), '-12-31'), 'yyyy-MM-dd') AS end_date,
    g.parsed_award_year AS start_year,
    g.parsed_award_year AS end_year,
    struct(
        NULLIF(g.laureate_given_name, '') AS given_name,
        NULLIF(g.laureate_family_name, '') AS family_name,
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
    COALESCE(NULLIF(g.recipient_profile_url, ''), NULLIF(g.prize_detail_url, '')) AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':gruber_prizes:', LOWER(TRIM(g.funder_award_id))))) % 9000000000) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM normalized g
CROSS JOIN gruber_funder f
WHERE g.funder_award_id IS NOT NULL
  AND g.laureate_name IS NOT NULL
  AND g.prize_category IN ('Cosmology', 'Genetics', 'Neuroscience')
  AND g.parsed_award_year IS NOT NULL
)
