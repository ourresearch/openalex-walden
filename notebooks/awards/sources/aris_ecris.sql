-- Mapping from CreateARISAwards.ipynb (origin e22fdf7f): its awards-table SELECT over aris_ecris_input (was the
-- staging table read from s3://openalex-ingest/awards/aris_ecris/aris_ecris_projects.parquet); created_date and updated_date are the run timestamp.
WITH
aris_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322554  -- ARIS (Javna agencija za znanstvenoraziskovalno in inovacijsko dejavnost RS)
),
mapped_awards AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.code)))) % 9000000000 as id,
        COALESCE(NULLIF(TRIM(g.title_en), ''), g.title_sl) as display_name,
        NULLIF(TRIM(g.keywords), '') as description,
        f.funder_id,
        g.code as funder_award_id,
        CAST(NULL AS DOUBLE) as amount,      -- §6.7 waiver: eCRIS publishes no amounts
        CAST(NULL AS STRING) as currency,    -- §6.7 waiver
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,
        CASE
            WHEN g.mstid_prg = 'Z' THEN 'fellowship'
            WHEN g.mstid_prg IN ('P', 'I') THEN 'research'
            ELSE 'research'
        END as funding_type,
        CASE g.mstid_prg
            WHEN 'P' THEN 'P — research programme'
            WHEN 'I' THEN 'I — infrastructure programme'
            WHEN 'J' THEN 'J — basic research project'
            WHEN 'L' THEN 'L — applied research project'
            WHEN 'V' THEN 'V — target research project'
            WHEN 'Z' THEN 'Z — postdoctoral research project'
            WHEN 'M' THEN 'M — CRP MIR'
            WHEN 'N' THEN 'N — European research project (ARIS-cofunded)'
            WHEN 'H' THEN 'H — European research project (ERA)'
            WHEN 'R' THEN 'R — development research project'
            WHEN 'T' THEN 'T — natural and cultural heritage project'
            WHEN 'NI' THEN 'NI — bilateral research project (Israel)'
            WHEN 'NC' THEN 'NC — bilateral research project (CEA)'
            WHEN 'NK' THEN 'NK — bilateral research project (China)'
            WHEN 'NJ' THEN 'NJ — bilateral research project (Japan)'
            WHEN 'BI' THEN 'BI — bilateral project'
            WHEN 'GC' THEN 'GC — Gravitation'
            WHEN 'STR' THEN 'STR — strategic project'
            WHEN 'MN' THEN 'RRP — mobility project'
            WHEN 'TN' THEN 'RRP — TRL 3-6 project'
            WHEN 'O' THEN 'O — citizen science project'
            ELSE g.mstid_prg
        END as funder_scheme,
        'aris_ecris' as provenance,
        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,
        CASE
            WHEN g.lead_family_name IS NOT NULL OR g.lead_org_name IS NOT NULL THEN
                struct(
                    g.lead_given_name as given_name,
                    g.lead_family_name as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    CASE WHEN g.lead_org_name IS NOT NULL THEN
                        struct(
                            g.lead_org_name as name,
                            'Slovenia' as country,
                            CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                        )
                    ELSE CAST(NULL AS STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>)
                    END as affiliation
                )
            ELSE NULL
        END as lead_investigator,
        CAST(NULL AS STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING,
            role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING,
            role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,
        g.landing_page_url,
        CAST(NULL AS STRING) as doi
    FROM aris_ecris_input g
    CROSS JOIN aris_funder f
),

awards_transformed AS (
SELECT *,
    concat('https://api.openalex.org/works?filter=awards.id:G', id) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM mapped_awards
)
