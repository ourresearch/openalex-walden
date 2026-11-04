-- Mapping from CreateNobelAwards.ipynb (origin e22fdf7f): its awards-table SELECT over nobelprize_api_input (was the
-- staging table read from s3://openalex-ingest/awards/nobel/nobel_prizes.parquet); created_date and updated_date are the run timestamp.
WITH
 cat_funder_map AS (
    SELECT * FROM (VALUES
        ('phy', 4320320936),
        ('che', 4320320936),
        ('eco', 4320320936),
        ('med', 4320322315)
    ) AS t(category_code, funder_id)
),
funders_resolved AS (
    SELECT m.category_code, f.funder_id, f.display_name, f.ror_id, f.doi
    FROM cat_funder_map m
    JOIN openalex.funders.funders f USING (funder_id)
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        f.funder_id, ':nobel:', n.category_code, ':', n.award_year, ':', n.laureate_id
    ))) % 9000000000 AS id,
    CONCAT(
        n.category_full_en, ' ', n.award_year,
        ' — ', n.laureate_full_name
    ) AS display_name,
    n.motivation_en AS description,
    f.funder_id,
    CONCAT(n.category_code, '-', n.award_year, '-', n.laureate_id) AS funder_award_id,
    CASE
        WHEN n.portion = '1'   THEN TRY_CAST(n.prize_amount_sek AS DOUBLE)
        WHEN n.portion = '1/2' THEN TRY_CAST(n.prize_amount_sek AS DOUBLE) * 0.5
        WHEN n.portion = '1/3' THEN TRY_CAST(n.prize_amount_sek AS DOUBLE) / 3.0
        WHEN n.portion = '1/4' THEN TRY_CAST(n.prize_amount_sek AS DOUBLE) * 0.25
        ELSE TRY_CAST(n.prize_amount_sek AS DOUBLE)
    END AS amount,
    'SEK' AS currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'prize' AS funding_type,
    n.category_full_en AS funder_scheme,
    'nobelprize_api' AS provenance,
    COALESCE(
        TRY_TO_DATE(n.date_awarded, 'yyyy-MM-dd'),
        TRY_TO_DATE(CONCAT(n.award_year, '-12-10'), 'yyyy-MM-dd')
    ) AS start_date,
    COALESCE(
        TRY_TO_DATE(n.date_awarded, 'yyyy-MM-dd'),
        TRY_TO_DATE(CONCAT(n.award_year, '-12-10'), 'yyyy-MM-dd')
    ) AS end_date,
    TRY_CAST(n.award_year AS INT) AS start_year,
    TRY_CAST(n.award_year AS INT) AS end_year,
    struct(
        n.laureate_given_name AS given_name,
        n.laureate_family_name AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            n.affiliation_name AS name,
            n.affiliation_country AS country,
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
    CONCAT('https://www.nobelprize.org/prizes/', n.category_code, '/', n.award_year, '/summary/') AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(
               f.funder_id, ':nobel:', n.category_code, ':', n.award_year, ':', n.laureate_id
           ))) % 9000000000) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM nobelprize_api_input n
JOIN funders_resolved f ON n.category_code = f.category_code
WHERE n.laureate_id IS NOT NULL
)
