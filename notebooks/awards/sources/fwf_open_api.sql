-- FWF (Austrian Science Fund) from its own Open API (openalex-ingest fwf_awards.py, weekly, CC0), at p51: above
-- openaire_fwf (p29, an OpenAIRE copy with no people, kept in raw so no award id goes away), so FWF's own row is the one
-- Nightly serves wherever both name a grant (priority DESC). Award id = the FWF project code as FWF and Crossref write
-- it (P22135), the form the award registry already holds for 20,594 of FWF's 20,599 grants (measured 11-05).
-- People switch: serve_people below. true = the principal investigator (names, ORCID, institution, ROR) is served;
-- false = no people at all (one line). Rohan 11-05: serve them.
-- Team lists (key researchers, partners, consortium) stay in the S3 file for a later change.
WITH
fwf_people_switch AS (
    SELECT true AS serve_people
),

fwf_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321181  -- Austrian Science Fund (FWF)
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.code)))) % 9000000000 as id,

        COALESCE(g.title_en, g.title_de) as display_name,

        -- FWF's public summary; else its keywords, as openaire_fwf served them, joined by "; " (888 grants have no summary).
        COALESCE(g.summary_en, g.summary_de, NULLIF(array_join(g.keywords, concat(char(59), ' ')), '')) as description,

        f.funder_id,
        g.code as funder_award_id,

        g.amount as amount,
        'EUR' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        -- The openaire_fwf rule on FWF's English programme names, plus FWF's newer career programmes.
        CASE
            WHEN LOWER(g.program_en) LIKE '%start%' THEN 'fellowship'
            WHEN LOWER(g.program_en) LIKE '%hertha firnberg%' THEN 'fellowship'
            WHEN LOWER(g.program_en) LIKE '%elise richter%' THEN 'fellowship'
            WHEN LOWER(g.program_en) LIKE '%lise meitner%' THEN 'fellowship'
            WHEN LOWER(g.program_en) LIKE '%schr%dinger%' THEN 'fellowship'
            WHEN LOWER(g.program_en) LIKE '%esprit%' THEN 'fellowship'
            WHEN LOWER(g.program_en) LIKE '%charlotte b%hler%' THEN 'fellowship'
            WHEN LOWER(g.program_en) LIKE '%astra%' THEN 'fellowship'
            WHEN LOWER(g.program_en) LIKE '%doctoral%' THEN 'training'
            WHEN LOWER(g.program_en) LIKE 'doc.funds%' THEN 'training'
            WHEN LOWER(g.program_en) LIKE '%wittgenstein%' THEN 'prize'
            WHEN LOWER(g.program_en) LIKE '%special research%' THEN 'center'
            WHEN LOWER(g.program_en) LIKE '%cluster%' THEN 'center'
            WHEN LOWER(g.program_en) LIKE '%infrastructure%' THEN 'infrastructure'
            ELSE 'grant'
        END as funding_type,

        g.program_en as funder_scheme,

        'fwf_open_api' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN s.serve_people AND g.pi_last IS NOT NULL THEN
                struct(
                    g.pi_first as given_name,
                    g.pi_last as family_name,
                    CASE WHEN g.pi_orcid IS NOT NULL THEN CONCAT('https://orcid.org/', g.pi_orcid) END as orcid,
                    CAST(NULL AS DATE) as role_start,
                    CASE
                        WHEN g.pi_inst IS NOT NULL OR g.pi_ror IS NOT NULL THEN
                            struct(
                                g.pi_inst as name,
                                CAST(NULL AS STRING) as country,
                                CASE
                                    WHEN g.pi_ror LIKE 'https://ror.org/%' THEN
                                        array(struct(g.pi_ror as id, 'ror' as type, 'funder' as asserted_by))
                                    ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                                END as ids
                            )
                    END as affiliation
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

        COALESCE(g.research_radar_url, CONCAT('https://fwf.ac.at/forschungsradar/10.55776/', g.code)) as landing_page_url,

        g.grant_doi as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.code)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM fwf_open_api_input g
    CROSS JOIN fwf_funder f
    CROSS JOIN fwf_people_switch s
    WHERE g.code IS NOT NULL
      AND TRIM(g.code) != ''
)
