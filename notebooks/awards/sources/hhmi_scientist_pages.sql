-- Mapping from CreateHHMIAwards.ipynb (origin e22fdf7f): its awards-table SELECT over hhmi_scientist_pages_input (was the
-- staging table read from s3://openalex-ingest/awards/hhmi/hhmi_investigators.parquet); created_date and updated_date are the run timestamp.
WITH
 hhmi_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306082
),
src AS (
    SELECT
        url,
        regexp_extract(url, '/scientists/([^/]+)$', 1) AS slug,
        name_full,
        role,
        term,
        start_year,
        end_year,
        institution,
        disciplines,
        bio,
        regexp_replace(
            regexp_replace(name_full, ',?\\s*(PhD|MD|DPhil|DSc|ScD)\\.?\\s*$', ''),
            ',?\\s+(Jr|Sr|II|III|IV)\\.?\\s*$', ''
        ) AS name_no_degree
    FROM hhmi_scientist_pages_input
    WHERE name_full IS NOT NULL
),
parsed_names AS (
    SELECT
        *,
        CASE
            WHEN array_size(split(trim(name_no_degree), '\\s+')) >= 2
            THEN element_at(split(trim(name_no_degree), '\\s+'), -1)
            ELSE NULL
        END AS family_name_tok,
        CASE
            WHEN array_size(split(trim(name_no_degree), '\\s+')) >= 2
            THEN array_join(slice(split(trim(name_no_degree), '\\s+'), 1, array_size(split(trim(name_no_degree), '\\s+')) - 1), ' ')
            ELSE trim(name_no_degree)
        END AS given_name_tok
    FROM src
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(p.slug)))) % 9000000000 AS id,
    CONCAT(
        COALESCE(p.name_no_degree, p.name_full), ' — HHMI ',
        COALESCE(REGEXP_REPLACE(NULLIF(p.role, 'HHMI'), '^HHMI ', ''), 'Scientist'),
        CASE WHEN p.term IS NOT NULL THEN CONCAT(' (', p.term, ')') ELSE '' END
    ) AS display_name,
    NULLIF(p.bio, '') AS description,
    f.funder_id,
    p.slug AS funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,
    'USD' AS currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    CASE
        WHEN LOWER(p.role) LIKE '%fellow%' THEN 'fellowship'
        ELSE 'research'
    END AS funding_type,
    NULLIF(p.role, 'HHMI') AS funder_scheme,
    'hhmi_scientist_pages' AS provenance,
    CASE WHEN p.start_year IS NOT NULL
         THEN TRY_TO_DATE(CONCAT(p.start_year, '-01-01'), 'yyyy-MM-dd') END AS start_date,
    CASE WHEN p.end_year IS NOT NULL
         THEN TRY_TO_DATE(CONCAT(p.end_year, '-12-31'), 'yyyy-MM-dd') END AS end_date,
    TRY_CAST(p.start_year AS INT) AS start_year,
    TRY_CAST(p.end_year AS INT) AS end_year,
    struct(
        p.given_name_tok AS given_name,
        p.family_name_tok AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            p.institution AS name,
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
    p.url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(p.slug)))) % 9000000000) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM parsed_names p
CROSS JOIN hhmi_funder f
WHERE p.slug IS NOT NULL AND TRIM(p.slug) != ''
)
