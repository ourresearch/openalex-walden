-- NWO (Dutch Research Council) projects from NWOpen, read from the newest object under s3://openalex-ingest/awards/nwopen/
-- (openalex-ingest nwo_awards.py, weekly; CC0 1.0). Same mapping as CreateNWOAwards.ipynb (origin e22fdf7f) for the
-- columns it had, so ids and keys are unchanged; added: NWO's amount (EUR), grant DOI, the lead's ROR, and the team.
-- People switch: nwo_people.serve_people. true (default) serves the lead and the team; set it to false (one line) to serve
-- no NWO people at all (lead_investigator and investigators NULL). Which roles are investigators: nwo_people.roles
-- (NWO's 'Researcher' rows are staff hired on the project, not applicants: they stay in the S3 file, as GtR's do).
-- created_date and updated_date are the run timestamp.
WITH
nwo_people AS (
    SELECT true AS serve_people, array('Main Applicant', 'Project leader', 'Co-applicant') AS roles
),

nwo_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321800  -- NWO (Nederlandse Organisatie voor Wetenschappelijk Onderzoek)
),

nwo_team AS (
    -- lead: the laptop parser's lead (NWO's first Main Applicant, else Project leader, else first member), as pi_member_id.
    -- team: the other applicants and project leaders, not the lead (investigators = the people after the lead, as NSF
    -- and ARC), in the fetcher's order (role, family name, given name, member id). A Dutch name's prefix (van, de, ...)
    -- belongs to the family name: 'van Alphen', not 'Alphen'.
    SELECT
        g.*,
        try_element_at(filter(g.members, m -> m.member_id <=> g.pi_member_id), 1) AS lead,
        filter(g.members, m -> NOT (m.member_id <=> g.pi_member_id) AND exists(m.roles, r -> array_contains(p.roles, r))) AS team
    FROM nwopen_input g
    CROSS JOIN nwo_people p
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000 as id,

        g.title as display_name,

        g.abstract as description,

        f.funder_id,
        g.project_id as funder_award_id,

        TRY_CAST(g.award_amount AS DOUBLE) as amount,
        'EUR' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.grant_category) LIKE '%veni%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%vidi%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%vici%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%promotiebeurs%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%onderzoekstalent%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%graduate%' THEN 'training'
            WHEN LOWER(g.grant_category) LIKE '%spinoza%' THEN 'prize'
            WHEN LOWER(g.grant_category) LIKE '%stevin%' THEN 'prize'
            WHEN LOWER(g.grant_category) LIKE '%gravitation%' THEN 'research'
            WHEN LOWER(g.grant_category) LIKE '%zwaartekracht%' THEN 'research'
            WHEN LOWER(g.grant_category) LIKE '%infrastructuur%' THEN 'infrastructure'
            WHEN LOWER(g.grant_category) LIKE '%infrastructure%' THEN 'infrastructure'
            ELSE 'grant'
        END as funding_type,

        g.grant_category as funder_scheme,

        'nwopen' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN p.serve_people AND coalesce(g.lead.last_name, g.pi_family_name) IS NOT NULL THEN
                struct(
                    coalesce(g.lead.first_name, g.lead.initials, g.pi_given_name) as given_name,
                    coalesce(nullif(trim(concat_ws(' ', g.lead.prefix, g.lead.last_name)), ''), g.pi_family_name) as family_name,
                    CASE WHEN g.pi_id RLIKE '[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]'
                         THEN concat('https://orcid.org/', regexp_extract(g.pi_id, '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])', 1)) END as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.lead_org_name as name,
                        'Netherlands' as country,
                        CASE WHEN g.pi_ror LIKE 'https://ror.org/%'
                             THEN array(named_struct('id', g.pi_ror, 'type', 'ror', 'asserted_by', 'funder'))
                             ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) END as ids
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

        -- The other applicants and project leaders, each with their own organisation (top level) and its ROR; NULL when
        -- the lead is the only one named
        CASE
            WHEN p.serve_people AND size(g.team) > 0 THEN
                transform(g.team, m -> struct(
                    coalesce(m.first_name, m.initials) as given_name,
                    nullif(trim(concat_ws(' ', m.prefix, m.last_name)), '') as family_name,
                    CASE WHEN m.orcid RLIKE '[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]'
                         THEN concat('https://orcid.org/', regexp_extract(m.orcid, '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])', 1)) END as orcid,
                    CAST(NULL AS DATE) as role_start,
                    CASE WHEN m.organisation IS NOT NULL OR m.ror IS NOT NULL THEN struct(
                        nullif(trim(split(m.organisation, '[|][|]')[0]), '') as name,
                        CAST(NULL AS STRING) as country,
                        CASE WHEN m.ror LIKE 'https://ror.org/%'
                             THEN array(named_struct('id', m.ror, 'type', 'ror', 'asserted_by', 'funder'))
                             ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) END as ids
                    ) END as affiliation
                ))
            ELSE NULL
        END as investigators,

        CONCAT('https://www.nwo.nl/projecten/', REPLACE(g.project_id, '.', '-')) as landing_page_url,

        CASE WHEN g.grant_doi LIKE '10.%' THEN lower(g.grant_doi) END as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM nwo_team g
    CROSS JOIN nwo_funder f
    CROSS JOIN nwo_people p
    WHERE g.project_id IS NOT NULL
      AND TRIM(g.project_id) != ''
)
