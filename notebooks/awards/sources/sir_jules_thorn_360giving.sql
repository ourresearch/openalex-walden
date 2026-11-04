-- Mapping from CreateSirJulesThornAwards.ipynb (origin e22fdf7f): its awards-table SELECT over sir_jules_thorn_360giving_input (was the
-- staging table read from s3://openalex-ingest/awards/sir_jules_thorn/sir_jules_thorn_grants.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320000  -- Sir Jules Thorn Charitable Trust
), mapped_awards AS (
    SELECT
        abs(xxhash64(CONCAT(
            TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
        ))) % 9000000000 AS id,
        COALESCE(n.title, CONCAT('Sir Jules Thorn Charitable Trust grant ', n.funder_award_id)) AS display_name,
        n.description,
        f.funder_id,
        n.funder_award_id,
        CASE WHEN TRY_CAST(n.amount AS DOUBLE) > 0 THEN TRY_CAST(n.amount AS DOUBLE) ELSE NULL END AS amount,
        CASE WHEN TRY_CAST(n.amount AS DOUBLE) > 0 THEN n.currency ELSE NULL END AS currency,
        struct(
            CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,
        'grant' AS funding_type,
        CAST(NULL AS STRING) AS funder_scheme,
        'sir_jules_thorn_360giving' AS provenance,
        TRY_CAST(n.award_date AS DATE) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        TRY_CAST(n.start_year AS INT) AS start_year,
        CAST(NULL AS INT) AS end_year,
        CASE
            WHEN n.recipient_org IS NOT NULL THEN struct(
                CAST(NULL AS STRING) AS given_name,
                CAST(NULL AS STRING) AS family_name,
                CAST(NULL AS STRING) AS orcid,
                CAST(NULL AS DATE) AS role_start,
                struct(
                    n.recipient_org AS name,
                    CAST(NULL AS STRING) AS country,
                    CASE
                        WHEN n.recipient_org_identifier IS NOT NULL THEN array(struct(
                            n.recipient_org_identifier AS id,
                            CAST('360Giving Recipient Org:Identifier' AS STRING) AS type,
                            CAST('source' AS STRING) AS asserted_by
                        ))
                        ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                    END AS ids
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
        CAST(NULL AS STRING) AS landing_page_url,
        CAST(NULL AS STRING) AS doi,
        CONCAT('https://api.openalex.org/works?filter=awards.id:G',
               TRY_CAST(abs(xxhash64(CONCAT(
                   TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
               ))) % 9000000000 AS STRING)) AS works_api_url,
        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM sir_jules_thorn_360giving_input n
    CROSS JOIN funder_resolved f
    WHERE n.funder_award_id IS NOT NULL
),

awards_transformed AS (
SELECT
    * EXCEPT(start_year, end_year),
    CASE WHEN start_year > YEAR(current_date()) + 1 THEN NULL ELSE start_year END AS start_year,
    CASE WHEN start_year > YEAR(current_date()) + 1 THEN NULL ELSE end_year END AS end_year
FROM mapped_awards
)
