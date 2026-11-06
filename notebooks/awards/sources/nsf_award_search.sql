-- Legacy mapping from CreateNSFAwards.ipynb, origin c3db0a32.
-- _change is runner metadata, never a raw column. Compare all 22 other columns null-safely.
WITH nsf_funder AS (
    -- NSF funder from OpenAlex (update funder_id after running Step 2)
    SELECT
        f.funder_id,
        f.display_name,
        f.ror_id,
        f.doi
    FROM openalex.funders.funders f
    JOIN openalex.funders.funder_match_policy p
      ON p.funder_id = f.funder_id
     AND p.matcher = 'datacite_name_v1' AND p.eligible
    WHERE ror_id = 'https://ror.org/021nxhr62'  -- National Science Foundation
),

nsf_award_people AS (
    -- pi_all: NSF's whole people list as JSON, written by the fetcher only when an award names two or more people
    SELECT g.*, from_json(g.pi_all, 'ARRAY<STRUCT<pi_first_name:STRING, pi_last_name:STRING, nsf_id:STRING>>') as pi_list
    FROM nsf_award_search_input g
),

awards_transformed AS (
    SELECT
        -- Generate unique ID using xxhash64 of funder + award_id
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.awd_id)))) % 9000000000 as id,

        -- Display name = award title
        g.awd_titl_txt as display_name,

        -- Description = abstract
        g.awd_abstract_narration as description,

        -- Funder info
        f.funder_id,
        LOWER(g.awd_id) as funder_award_id,

        -- Funding amount
        CAST(g.awd_amount AS DOUBLE) as amount,
        'USD' as currency,

        -- Funder struct
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        -- Funding type based on transaction type and instrument
        CASE
            WHEN LOWER(g.awd_istr_txt) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(g.awd_istr_txt) LIKE '%cooperative%' THEN 'cooperative_agreement'
            WHEN LOWER(g.awd_istr_txt) LIKE '%contract%' THEN 'contract'
            WHEN LOWER(g.awd_istr_txt) LIKE '%continuing%' THEN 'grant'
            WHEN LOWER(g.awd_istr_txt) LIKE '%standard%' THEN 'grant'
            ELSE 'grant'
        END as funding_type,

        -- Funder scheme = award instrument type
        g.awd_istr_txt as funder_scheme,

        -- Provenance
        'nsf_award_search' as provenance,

        -- Dates (NSF dates are in yyyy-MM-dd format)
        TRY_TO_DATE(g.awd_eff_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.awd_exp_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.awd_eff_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.awd_exp_date, 'yyyy-MM-dd')) as end_year,

        -- Lead investigator
        CASE
            WHEN g.pi_last_name IS NOT NULL THEN
                struct(
                    g.pi_first_name as given_name,
                    g.pi_last_name as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.inst_name as name,
                        g.inst_country_name as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,

        -- NSF has no co-lead role; co-PIs are listed in investigators
        CAST(NULL AS STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,

        -- Everyone after the first person in NSF's list, in NSF's order: co-PIs, and former PIs and co-PIs.
        -- A person NSF lists twice (a role change) appears once, at their first place. NSF names no institution per co-PI.
        transform(
            filter(g.pi_list, (p, i) -> i > 0 AND p.pi_last_name IS NOT NULL
                AND NOT exists(slice(g.pi_list, 1, i), q -> coalesce(q.nsf_id = p.nsf_id, false))),
            p -> struct(
                p.pi_first_name as given_name,
                p.pi_last_name as family_name,
                CAST(NULL AS STRING) as orcid,
                CAST(NULL AS DATE) as role_start,
                CAST(NULL AS STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>) as affiliation
            )
        ) as investigators,

        -- Landing page URL (NSF Award Search page)
        CONCAT('https://www.nsf.gov/awardsearch/showAward?AWD_ID=', g.awd_id) as landing_page_url,

        -- No DOI for NSF grants typically
        CAST(NULL AS STRING) as doi,

        -- Works API URL
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.awd_id)))) % 9000000000) as works_api_url,

        -- Timestamps
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM nsf_award_people g
    CROSS JOIN nsf_funder f
    WHERE g.awd_id IS NOT NULL
      AND TRIM(g.awd_id) != ''
)
