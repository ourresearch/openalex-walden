-- Legacy mapping from CreateKAKENAwards.ipynb, origin c3db0a32.
-- _change is runner metadata, never a raw column. Compare all 22 other columns null-safely.
WITH
-- Get JSPS funder from OpenAlex
jsps_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320334764  -- Japan Society for the Promotion of Science
),

-- KAKEN's English pages print some principal investigators in kanji. The same researcher number (pi_nrid) carries
-- KAKEN's own romaji name on the researcher's other grants in this file, so those rows take it: the most frequent
-- romaji spelling for the number, then the latest start date, then a fixed tie-break on the spelling. Rows already
-- in Latin script, and kanji rows whose number has no romaji grant, are unchanged.
kaken_romaji AS (
    SELECT pi_nrid,
        max_by(named_struct('family_name', f, 'given_name', g),
               named_struct('n', n, 'last_start', last_start, 'f', f, 'g', coalesce(g, ''), 'g_set', g IS NOT NULL)) AS romaji
    FROM (
        SELECT pi_nrid, pi_family_name AS f, pi_given_name AS g, count(*) AS n, max(coalesce(start_date, '')) AS last_start
        FROM kaken_input
        WHERE pi_nrid IS NOT NULL AND pi_family_name IS NOT NULL
          AND NOT concat(pi_family_name, coalesce(pi_given_name, '')) RLIKE '[\\p{IsHan}\\p{IsHiragana}\\p{IsKatakana}]'
        GROUP BY pi_nrid, pi_family_name, pi_given_name
    )
    GROUP BY pi_nrid
),

awards_transformed AS (
    SELECT
        -- Generate unique ID using xxhash64 of funder_id:project_id
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000 as id,

        -- Display name = project title
        g.title as display_name,

        -- Description = abstract
        g.abstract as description,

        -- Funder info
        f.funder_id,
        g.project_id as funder_award_id,

        -- Amount in JPY
        g.amount as amount,
        'JPY' as currency,

        -- Funder struct
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        -- Funding type - map KAKEN categories to types
        CASE
            WHEN LOWER(g.category) LIKE '%fellow%' THEN 'fellowship'
            WHEN LOWER(g.category) LIKE '%young%' THEN 'fellowship'
            WHEN LOWER(g.category) LIKE '%early-career%' THEN 'fellowship'
            WHEN LOWER(g.category) LIKE '%encouragement%' THEN 'fellowship'
            WHEN LOWER(g.category) LIKE '%start%' THEN 'fellowship'
            WHEN LOWER(g.category) LIKE '%specially promoted%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%scientific research%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%challenging%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%transformative%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%pioneering%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%international%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%publication%' THEN 'other'
            ELSE 'grant'
        END as funding_type,

        -- Funder scheme = research category
        g.category as funder_scheme,

        -- Provenance
        'kaken' as provenance,

        -- Dates
        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        -- Lead investigator
        CASE
            WHEN g.pi_family_name IS NOT NULL THEN
                struct(
                    -- romaji from the same researcher number when KAKEN printed this grant's name in kanji (kaken_romaji)
                    CASE WHEN r.romaji IS NOT NULL THEN r.romaji.given_name ELSE g.pi_given_name END as given_name,
                    CASE WHEN r.romaji IS NOT NULL THEN r.romaji.family_name ELSE g.pi_family_name END as family_name,
                    -- NRID is not an ORCID, so we leave orcid NULL
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        COALESCE(g.institution, g.pi_affiliation) as name,
                        'Japan' as country,
                        -- Include NRID as an identifier if available
                        CASE
                            WHEN g.pi_nrid IS NOT NULL THEN
                                array(struct(
                                    CONCAT('https://nrid.nii.ac.jp/nrid/', g.pi_nrid) as id,
                                    'nrid' as type,
                                    'kaken' as asserted_by
                                ))
                            ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                        END as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,

        -- Co-lead and other investigators (not available in scraped data)
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

        -- Landing page URL
        g.landing_page_url,

        -- No DOI for KAKEN grants
        CAST(NULL AS STRING) as doi,

        -- Works API URL
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000) as works_api_url,

        -- Timestamps
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM kaken_input g
    CROSS JOIN jsps_funder f
    LEFT JOIN kaken_romaji r
      ON r.pi_nrid = g.pi_nrid
     AND concat(g.pi_family_name, coalesce(g.pi_given_name, '')) RLIKE '[\\p{IsHan}\\p{IsHiragana}\\p{IsKatakana}]'
    WHERE g.project_id IS NOT NULL
      AND TRIM(g.project_id) != ''
)
