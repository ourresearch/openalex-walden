-- Mapping from CreateNSFSriLankaAwards.ipynb (origin e22fdf7f): its awards-table SELECT over nsf_sri_lanka_gmis_input (was the
-- staging table read from s3://openalex-ingest/awards/nsf_sri_lanka/nsf_sri_lanka_grants.parquet); created_date and updated_date are the run timestamp.
WITH
nsf_sri_lanka_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320335353
),

raw_prepared AS (
    SELECT
        *,
        TRIM(grant_number) AS native_award_id,  -- citable ref (RG/87/MAB/006), source case
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        CASE WHEN TRY_CAST(amount AS DOUBLE) IS NOT NULL THEN currency ELSE NULL END AS parsed_currency,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date,
        TRY_CAST(source_year AS INT) AS parsed_year,
        CONCAT_WS(
            ' | ',
            NULLIF(TRIM(abstract), ''),
            NULLIF(TRIM(key_research_findings), '')
        ) AS source_description
    FROM nsf_sri_lanka_gmis_input
    WHERE grant_number IS NOT NULL
      AND TRIM(grant_number) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(grant_number)) ORDER BY detail_id) = 1
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.native_award_id)))) % 9000000000 as id,

        TRIM(g.display_name) as display_name,

        CASE
            WHEN NULLIF(TRIM(g.source_description), '') IS NOT NULL THEN NULLIF(TRIM(g.source_description), '')
            WHEN NULLIF(TRIM(g.keywords), '') IS NOT NULL THEN CONCAT('Keywords: ', TRIM(g.keywords))
            ELSE NULL
        END as description,

        f.funder_id,
        g.native_award_id as funder_award_id,

        g.parsed_amount as amount,
        g.parsed_currency as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        'grant' as funding_type,

        NULLIF(TRIM(g.subject), '') as funder_scheme,

        'nsf_sri_lanka_gmis' as provenance,

        g.parsed_start_date as start_date,
        g.parsed_end_date as end_date,
        COALESCE(YEAR(g.parsed_start_date), g.parsed_year) as start_year,
        COALESCE(YEAR(g.parsed_end_date), g.parsed_year) as end_year,

        struct(
            NULLIF(TRIM(g.lead_investigator_given_name), '') as given_name,
            NULLIF(TRIM(g.lead_investigator_family_name), '') as family_name,
            CAST(NULL AS STRING) as orcid,
            g.parsed_start_date as role_start,
            struct(
                CAST(NULL AS STRING) as name,
                'LK' as country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
            ) as affiliation
        ) as lead_investigator,

        CAST(NULL AS STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,

        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,

        NULLIF(TRIM(g.landing_page_url), '') as landing_page_url,
        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.native_award_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_prepared g
    CROSS JOIN nsf_sri_lanka_funder f
)
