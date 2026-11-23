-- Legacy mapping from CreateKAKENAwards.ipynb, origin c3db0a32. Reads openalex-ingest kaken_awards.py (the KAKEN API, monthly)
-- from s3://openalex-ingest/awards/kaken_api/ since 2026-11; the 2026-07-08 page scrape stays at awards/kaken/, unread.
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

-- KAKEN's API lists every member of a grant (members_json, from openalex-ingest kaken_awards.py), in KAKEN's order.
-- Co-investigators (role co_investigator_buntan, Kenkyu-buntansha; co_investigator_renkei, Renkei-kenkyusha) become investigators,
-- in that order. Left out: the principal investigator (the lead slot holds them), research collaborators and every other role
-- (not grant holders), a member with no family name, and a second listing of the same researcher number. A member printed in
-- kanji takes kaken_romaji's name for their number, as the lead does. A grant with no co-investigator keeps investigators NULL.
kaken_coinvestigators AS (
    SELECT project_id, transform(array_sort(collect_list(struct(pos, person)), (a, b) -> a.pos - b.pos), x -> x.person) AS investigators
    FROM (
        SELECT k.project_id, k.pos, struct(
                CASE WHEN r.romaji IS NOT NULL THEN r.romaji.given_name ELSE k.given_name END as given_name,
                CASE WHEN r.romaji IS NOT NULL THEN r.romaji.family_name ELSE k.family_name END as family_name,
                CAST(NULL AS STRING) as orcid,
                CAST(NULL AS DATE) as role_start,
                struct(
                    k.affiliation as name,
                    'Japan' as country,
                    CASE WHEN k.nrid IS NOT NULL
                         THEN array(struct(CONCAT('https://nrid.nii.ac.jp/nrid/', k.nrid) as id, 'nrid' as type, 'kaken' as asserted_by))
                         ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) END as ids
                ) as affiliation
            ) AS person
        FROM (
            SELECT g.project_id, g.pi_nrid, x.pos, mem.role, mem.family_name, mem.given_name, mem.affiliation,
                -- NRID = '1000' + the researcher number zero-padded to 9 digits (as pi_nrid)
                CASE WHEN mem.researcher_number RLIKE '^[0-9]{1,9}$' THEN concat('1000', lpad(mem.researcher_number, 9, '0')) END AS nrid,
                row_number() OVER (PARTITION BY g.project_id, coalesce(mem.researcher_number, concat('pos', x.pos)) ORDER BY x.pos) AS nth
            FROM kaken_input g
            LATERAL VIEW posexplode(from_json(g.members_json,
                'ARRAY<STRUCT<role:STRING, family_name:STRING, given_name:STRING, researcher_number:STRING, affiliation:STRING>>')) x AS pos, mem
            -- filtered before nth is numbered, so an unnamed or other-role listing of a number never hides the co-investigator's own
            WHERE mem.role IN ('co_investigator_buntan', 'co_investigator_renkei') AND mem.family_name IS NOT NULL
        ) k
        LEFT JOIN kaken_romaji r
          ON r.pi_nrid = k.nrid
         AND concat(k.family_name, coalesce(k.given_name, '')) RLIKE '[\\p{IsHan}\\p{IsHiragana}\\p{IsKatakana}]'
        WHERE k.nth = 1 AND NOT coalesce(k.nrid = k.pi_nrid, false)
    )
    GROUP BY project_id
),

-- Award keys of the 2026-07-08 page scrape. It keyed pre-1982 projects by the tail of KAKEN's number ('302014' for
-- 'X00022----302014'), and KAKEN reuses a tail across years and categories; the API gives the full number. The 86,532 awards we
-- already served under a tail keep it as funder_award_id, so each stays the same award (same id, same links) and is refreshed
-- from the API. openalex.awards.kaken_scrape_keys is that list, frozen (version 0; made once on 2026-11-16 from the scrape's
-- landing pages, which name the full number): record_id = KAKEN's number, award_key = the key we serve. A tail that is itself a
-- project number on KAKEN is not in it and belongs to that project. Every other project is keyed by KAKEN's full number.
kaken_scrape_keys AS (
    SELECT record_id, award_key FROM openalex.awards.kaken_scrape_keys VERSION AS OF 0
),

awards_transformed AS (
    SELECT
        -- Generate unique ID using xxhash64 of funder_id:project_id
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(COALESCE(sk.award_key, g.project_id))))) % 9000000000 as id,

        -- Display name = project title
        g.title as display_name,

        -- Description = abstract
        g.abstract as description,

        -- Funder info
        f.funder_id,
        -- the scrape's key where we already serve the project under it (kaken_scrape_keys), else KAKEN's number
        COALESCE(sk.award_key, g.project_id) as funder_award_id,

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

        -- KAKEN has no co-lead role
        CAST(NULL AS STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,

        -- KAKEN's co-investigators (kaken_coinvestigators)
        CAST(c.investigators AS ARRAY<STRUCT<
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
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(COALESCE(sk.award_key, g.project_id))))) % 9000000000) as works_api_url,

        -- Timestamps
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM kaken_input g
    CROSS JOIN jsps_funder f
    LEFT JOIN kaken_romaji r
      ON r.pi_nrid = g.pi_nrid
     AND concat(g.pi_family_name, coalesce(g.pi_given_name, '')) RLIKE '[\\p{IsHan}\\p{IsHiragana}\\p{IsKatakana}]'
    LEFT JOIN kaken_coinvestigators c ON c.project_id = g.project_id
    LEFT JOIN kaken_scrape_keys sk ON sk.record_id = g.project_id
    WHERE g.project_id IS NOT NULL
      AND TRIM(g.project_id) != ''
)
