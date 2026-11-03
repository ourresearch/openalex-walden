-- Mapping from CreateARCAwards.ipynb (origin e22fdf7f): its awards-table SELECT over arc_input (was the
-- staging table read from s3://openalex-ingest/awards/arc/arc_projects.parquet); created_date and updated_date are the run timestamp.
WITH
arc_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320334704  -- Australian Research Council
),

pi_parsed AS (
    SELECT
        *,
        CASE
            WHEN lead_investigator IS NULL OR TRIM(lead_investigator) = '' THEN NULL
            WHEN lead_investigator LIKE '%,%' THEN 
                TRIM(SUBSTRING_INDEX(lead_investigator, ',', -1))
            ELSE
                TRIM(SPLIT(
                    TRIM(REGEXP_REPLACE(lead_investigator, '^(Dr|Prof|Professor|Associate Professor|A/Prof|Asst Prof|Assoc Prof|Mr|Ms|Mrs|Miss|Emeritus Professor|Emeritus Prof|Adj/Prof|Adjunct Professor)\\s+', '')),
                    ' '
                )[0])
        END as pi_given_name,
        CASE
            WHEN lead_investigator IS NULL OR TRIM(lead_investigator) = '' THEN NULL
            WHEN lead_investigator LIKE '%,%' THEN 
                TRIM(SUBSTRING_INDEX(lead_investigator, ',', 1))
            ELSE
                TRIM(ELEMENT_AT(
                    SPLIT(
                        TRIM(REGEXP_REPLACE(lead_investigator, '^(Dr|Prof|Professor|Associate Professor|A/Prof|Asst Prof|Assoc Prof|Mr|Ms|Mrs|Miss|Emeritus Professor|Emeritus Prof|Adj/Prof|Adjunct Professor)\\s+', '')),
                        ' '
                    ),
                    -1
                ))
        END as pi_family_name
    FROM arc_input
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.code)))) % 9000000000 as id,

        g.title as display_name,

        g.summary as description,

        f.funder_id,
        g.code as funder_award_id,

        TRY_CAST(g.grant_value AS DOUBLE) as amount,
        'AUD' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.scheme_name) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(g.scheme_name) LIKE '%decra%' OR LOWER(g.scheme_name) LIKE '%early career%' THEN 'fellowship'
            WHEN LOWER(g.scheme_name) LIKE '%laureate%' THEN 'fellowship'
            WHEN LOWER(g.scheme_name) LIKE '%infrastructure%' THEN 'infrastructure'
            WHEN LOWER(g.scheme_name) LIKE '%equipment%' THEN 'infrastructure'
            WHEN LOWER(g.scheme_name) LIKE '%facilities%' THEN 'infrastructure'
            WHEN LOWER(g.scheme_name) LIKE '%centre%' THEN 'infrastructure'
            WHEN LOWER(g.scheme_name) LIKE '%project%' THEN 'research'
            WHEN LOWER(g.scheme_name) LIKE '%discovery%' THEN 'research'
            WHEN LOWER(g.scheme_name) LIKE '%linkage%' THEN 'research'
            WHEN LOWER(g.scheme_name) LIKE '%industrial%' THEN 'research'
            WHEN LOWER(g.scheme_name) LIKE '%intelligence%' THEN 'research'
            WHEN LOWER(g.scheme_name) LIKE '%indigenous%' THEN 'research'
            ELSE 'grant'
        END as funding_type,

        g.scheme_name as funder_scheme,

        'arc' as provenance,

        CASE 
            WHEN TRY_CAST(g.start_year AS INT) IS NOT NULL 
            THEN TO_DATE(CONCAT(g.start_year, '-01-01')) 
            ELSE NULL 
        END as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        TRY_CAST(g.start_year AS INT) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN g.lead_investigator IS NOT NULL AND TRIM(g.lead_investigator) != '' THEN
                struct(
                    g.pi_given_name as given_name,
                    g.pi_family_name as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.grantee as name,
                        'Australia' as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,

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

        CONCAT('https://dataportal.arc.gov.au/RGS/Web/Grants/', g.code) as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.code)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM pi_parsed g
    CROSS JOIN arc_funder f
    WHERE g.code IS NOT NULL
      AND TRIM(g.code) != ''
)
