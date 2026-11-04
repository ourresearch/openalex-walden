-- Mapping from CreateGuggenheimAwards.ipynb (origin e22fdf7f): its awards-table SELECT over guggenheim_fellowship_input (was the
-- staging table read from s3://openalex-ingest/awards/guggenheim/guggenheim_fellows.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320308774  -- John Simon Guggenheim Memorial Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT('Guggenheim Fellowship - ', n.fellow_name,
           CASE WHEN n.start_year IS NOT NULL THEN CONCAT(' (', n.start_year, ')') ELSE '' END) AS display_name,
    CAST(NULL AS STRING) AS description,
    f.funder_id,
    n.funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,        -- §6.7 fellowship waiver: no per-fellow stipend published
    CAST(NULL AS STRING) AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'fellowship' AS funding_type,
    CAST(NULL AS STRING) AS funder_scheme,  -- field/discipline is client-side only, never guessed
    'guggenheim_fellowship' AS provenance,
    CAST(NULL AS DATE) AS start_date,
    CAST(NULL AS DATE) AS end_date,
    TRY_CAST(n.start_year AS INT) AS start_year,
    CAST(NULL AS INT) AS end_year,
    CASE
        WHEN n.family_name IS NOT NULL OR n.given_name IS NOT NULL THEN struct(
            n.given_name AS given_name,
            n.family_name AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            struct(
                CAST(NULL AS STRING) AS name,     -- public directory lists no institution, never guessed
                CAST(NULL AS STRING) AS country,
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
    n.landing_page_url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM guggenheim_fellowship_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
)
