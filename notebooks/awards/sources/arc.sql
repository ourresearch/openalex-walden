-- ARC (Australian Research Council), from openalex-ingest arc_awards.py: the newest monthly object under
-- s3://openalex-ingest/awards/arc_rgs/ (RGS list + one detail record per grant). Replaces the January laptop file
-- (scripts/local/arc_to_s3.py, s3://openalex-ingest/awards/arc/arc_projects.parquet), whose 20 columns the object keeps.
-- Keys are unchanged (id and funder_award_id from the grant code), so the arc p20 rows update in place.
-- People (feeds-a, 11-05): lead_investigator and investigators come from ARC's detail record: given and family name as
-- separate fields (the title is its own field, so no title parsing), ORCID, role order with the lead first. investigators
-- carries everyone but the lead, in ARC's order (as the 11-06 co-investigators change served them from the list string).
-- A grant whose detail record is missing falls back to the list string, parsed as main + arc-coinv + names-c do.
-- created_date and updated_date are the run timestamp.
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

-- THE SWITCH (one line). true: people from ARC's detail records (names, ORCIDs, every investigator). false: the people
-- served before this change, parsed from the list strings (lead + co-investigators, no ORCID).
arc_people_switch AS (SELECT true AS from_detail),

-- RGS also lists grants another agency funds and ARC administers (11-05: Office of National Intelligence 39, Department of
-- Defence 9, all served under ARC today). Set arc_only to true to stop adding new ones (rows already in raw are retained).
arc_scope AS (SELECT false AS arc_only),

arc_parsed AS (
    SELECT
        g.*,
        -- the list string's lead, names-c title list (Em/Prof, A/Pr, stacked titles); 'Family, Given' or 'Given ... Family'
        CASE
            WHEN g.lead_investigator IS NULL OR TRIM(g.lead_investigator) = '' THEN NULL
            WHEN g.lead_investigator LIKE '%,%' THEN TRIM(SUBSTRING_INDEX(g.lead_investigator, ',', -1))
            ELSE TRIM(SPLIT(TRIM(REGEXP_REPLACE(g.lead_investigator, '^((Dr|Prof|Professor|Associate Professor|A/Prof|A/Pr|Asst Prof|Assoc Prof|Mr|Ms|Mrs|Miss|Emeritus Professor|Emeritus Prof|Em/Prof|E/Prof|E/Pr|Adj/Prof|Adjunct Professor|Hon)\\.?\\s+)+', '')), ' ')[0])
        END as pi_given_name,
        CASE
            WHEN g.lead_investigator IS NULL OR TRIM(g.lead_investigator) = '' THEN NULL
            WHEN g.lead_investigator LIKE '%,%' THEN TRIM(SUBSTRING_INDEX(g.lead_investigator, ',', 1))
            ELSE TRIM(ELEMENT_AT(SPLIT(TRIM(REGEXP_REPLACE(g.lead_investigator, '^((Dr|Prof|Professor|Associate Professor|A/Prof|A/Pr|Asst Prof|Assoc Prof|Mr|Ms|Mrs|Miss|Emeritus Professor|Emeritus Prof|Em/Prof|E/Prof|E/Pr|Adj/Prof|Adjunct Professor|Hon)\\.?\\s+)+', '')), ' '), -1))
        END as pi_family_name,
        -- the list string's team (arc-coinv): the lead, then ARC's semicolon list ('\\x3B': the generator allows no semicolon)
        transform(concat(array(g.lead_investigator), split(g.investigators, '\\x3B')), x -> named_struct('name', trim(x),
            'words', filter(split(regexp_replace(trim(x), '^((Dr|Prof|Professor|Associate Professor|A/Prof|A/Pr|Asst Prof|Assoc Prof|Mr|Ms|Mrs|Miss|Emeritus Professor|Emeritus Prof|Em/Prof|E/Prof|E/Pr|Adj/Prof|Adjunct Professor|Hon)\\.?\\s+)+', ''), ' '), t -> t <> ''))) as team,
        -- the detail record's people: the lead is first (the fetcher moves it there)
        s.from_detail AND coalesce(g.detail_status = 'ok' AND size(g.people) > 0, false) as use_detail,
        transform(g.people, p -> struct(
            nullif(trim(p.given_name), '') as given_name,
            nullif(trim(p.family_name), '') as family_name,
            CASE WHEN regexp_extract(upper(p.orcid), '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])$', 1) <> ''
                 THEN concat('https://orcid.org/', regexp_extract(upper(p.orcid), '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])$', 1)) END as orcid
        )) as detail_people
    FROM arc_input g
    CROSS JOIN arc_people_switch s
    CROSS JOIN arc_scope sc
    WHERE NOT sc.arc_only OR coalesce(g.grant_funder, 'Australian Research Council') = 'Australian Research Council'
),

arc_team AS (
    SELECT
        *,
        -- detail: everyone after the lead, a name listed twice (or the lead again) once, at its first place
        transform(
            filter(detail_people, (p, i) -> i > 0 AND (p.given_name IS NOT NULL OR p.family_name IS NOT NULL)
                AND NOT exists(slice(detail_people, 1, i), q -> coalesce(lower(concat_ws(' ', q.given_name, q.family_name)) = lower(concat_ws(' ', p.given_name, p.family_name)), false))),
            p -> struct(
                p.given_name as given_name,
                p.family_name as family_name,
                p.orcid as orcid,
                CAST(NULL AS DATE) as role_start,
                CAST(NULL AS STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>) as affiliation
            )
        ) as detail_co_investigators,
        -- list string (arc-coinv): everyone after the lead, first word given, last word family, no ORCID
        transform(
            filter(team, (p, i) -> i > 0 AND size(p.words) > 0
                AND NOT exists(slice(team, 1, i), q -> coalesce(lower(array_join(q.words, ' ')) = lower(array_join(p.words, ' ')), false))),
            p -> struct(
                try_element_at(p.words, 1) as given_name,
                try_element_at(p.words, -1) as family_name,
                CAST(NULL AS STRING) as orcid,
                CAST(NULL AS DATE) as role_start,
                CAST(NULL AS STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>) as affiliation
            )
        ) as string_co_investigators
    FROM arc_parsed
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

        -- ARC's funding commencement year, as before (the detail record's project-start-date stays in the S3 file)
        CASE
            WHEN TRY_CAST(g.start_year AS INT) IS NOT NULL
            THEN TO_DATE(CONCAT(g.start_year, '-01-01'))
            ELSE NULL
        END as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        TRY_CAST(g.start_year AS INT) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN g.use_detail THEN
                struct(
                    g.detail_people[0].given_name as given_name,
                    g.detail_people[0].family_name as family_name,
                    g.detail_people[0].orcid as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.grantee as name,
                        'Australia' as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
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

        -- NULL, not [], when the lead is the only person
        CASE
            WHEN g.use_detail THEN CASE WHEN size(g.detail_co_investigators) > 0 THEN g.detail_co_investigators END
            WHEN size(g.string_co_investigators) > 0 THEN g.string_co_investigators
        END as investigators,

        CONCAT('https://dataportal.arc.gov.au/RGS/Web/Grants/', g.code) as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.code)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM arc_team g
    CROSS JOIN arc_funder f
    WHERE g.code IS NOT NULL
      AND TRIM(g.code) != ''
)
