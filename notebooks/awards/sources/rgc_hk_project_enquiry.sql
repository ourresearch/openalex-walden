-- Mapping from CreateRGCHKAwards.ipynb (origin e22fdf7f): its awards-table SELECT over rgc_hk_project_enquiry_input (was the
-- staging table read from s3://openalex-ingest/awards/rgc_hk/rgc_hk_projects.parquet); created_date and updated_date are the run timestamp.
WITH
rgc_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321592
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        TRY_CAST(start_year AS INT) AS parsed_start_year,
        TRY_CAST(end_year AS INT) AS parsed_end_year,
        TRY_CAST(completion_date AS DATE) AS parsed_completion_date,
        from_json(
            co_investigators_struct_json,
            'ARRAY<STRUCT<raw_name:STRING,given_name:STRING,family_name:STRING>>'
        ) AS co_people
    FROM rgc_hk_project_enquiry_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND title IS NOT NULL
      AND TRIM(title) != ''
),

mapped_awards AS (
    SELECT
        abs(xxhash64(CONCAT(CAST(f.funder_id AS STRING), ':', g.native_award_id))) % 9000000000 AS id,
        TRIM(g.title) AS display_name,
        NULLIF(TRIM(g.description), '') AS description,
        f.funder_id,
        g.native_award_id AS funder_award_id,
        CASE WHEN g.parsed_amount > 0 THEN g.parsed_amount ELSE NULL END AS amount,
        CASE WHEN g.parsed_amount > 0 THEN 'HKD' ELSE NULL END AS currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,
        COALESCE(NULLIF(TRIM(g.funding_type_hint), ''), 'research') AS funding_type,
        NULLIF(TRIM(g.funding_scheme), '') AS funder_scheme,
        'rgc_hk_project_enquiry' AS provenance,
        CAST(NULL AS DATE) AS start_date,
        g.parsed_completion_date AS end_date,
        g.parsed_start_year AS start_year,
        g.parsed_end_year AS end_year,
        struct(
            NULLIF(TRIM(g.pi_given_name), '') AS given_name,
            NULLIF(TRIM(g.pi_family_name), '') AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            struct(
                NULLIF(TRIM(g.institution), '') AS name,
                CAST(NULL AS STRING) AS country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
            ) AS affiliation
        ) AS lead_investigator,
        CASE
            WHEN g.co_people IS NOT NULL AND size(g.co_people) > 0 THEN struct(
                NULLIF(TRIM(g.co_people[0].given_name), '') AS given_name,
                NULLIF(TRIM(g.co_people[0].family_name), '') AS family_name,
                CAST(NULL AS STRING) AS orcid,
                CAST(NULL AS DATE) AS role_start,
                struct(
                    CAST(NULL AS STRING) AS name,
                    CAST(NULL AS STRING) AS country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                ) AS affiliation
            )
            ELSE CAST(NULL AS STRUCT<
                given_name:STRING,
                family_name:STRING,
                orcid:STRING,
                role_start:DATE,
                affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
            >)
        END AS co_lead_investigator,
        CASE
            WHEN g.co_people IS NOT NULL AND size(g.co_people) > 1 THEN transform(
                slice(g.co_people, 2, size(g.co_people) - 1),
                x -> struct(
                    NULLIF(TRIM(x.given_name), '') AS given_name,
                    NULLIF(TRIM(x.family_name), '') AS family_name,
                    CAST(NULL AS STRING) AS orcid,
                    CAST(NULL AS DATE) AS role_start,
                    struct(
                        CAST(NULL AS STRING) AS name,
                        CAST(NULL AS STRING) AS country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                    ) AS affiliation
                )
            )
            ELSE CAST(NULL AS ARRAY<STRUCT<
                given_name:STRING,
                family_name:STRING,
                orcid:STRING,
                role_start:DATE,
                affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
            >>)
        END AS investigators,
        NULLIF(TRIM(g.source_url), '') AS landing_page_url,
        CAST(NULL AS STRING) AS doi,
        CONCAT('https://api.openalex.org/works?filter=awards.id:G',
               CAST(abs(xxhash64(CONCAT(CAST(f.funder_id AS STRING), ':', g.native_award_id))) % 9000000000 AS STRING)) AS works_api_url,
        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM raw_prepared g
    CROSS JOIN rgc_funder f
),

awards_transformed AS (
SELECT
    * EXCEPT(start_year, end_year),
    CASE WHEN start_year > YEAR(current_date()) + 1 THEN NULL ELSE start_year END AS start_year,
    CASE WHEN start_year > YEAR(current_date()) + 1 THEN NULL ELSE end_year END AS end_year
FROM mapped_awards
)
