-- Mapping from CreateFCTAwards.ipynb (main 322e8280), moved into the Awards job with openalex-ingest fct_awards.py (SciPROJ weekly dump,
-- s3://openalex-ingest/awards/fct_sciproj/). SciPROJ is Portugal's national registry: FCT plus ANI, EC and ERDF projects ride in the same
-- dump; funder_key routes each row and every row keeps provenance 'fct'. The object is FCT's public weekly file as published: fellowship
-- amounts and programmes are blank because FCT keeps them private (no fill-ins from the 2026-07 private/QA file or the 2026-06 fct.pt
-- list, both retired). amount is the project total: the fetcher sums the record's funding streams (FCT + FEDER co-funding).
-- Changes from the notebook, each declared in the feeds-d gate:
--   1. EC rows: a fixed rule instead of the anti-join on live raw. SciPROJ's EC rows ship for FP1-FP6 only (CORDIS, p27, carries FP7 and
--      later). The notebook's raw read made the output depend on whatever raw held that day.
--   2. Names card 5: Portuguese titles ('Prof. Doutor', 'Doutora', 'Dra.', 'Eng.') leave given names (lead and team).
--   3. The lead's ORCID, and the project team as investigators (SciPROJ order, PI and repeats left out by the fetcher).
-- The two switches below are the one-line on/off for the team. Research-unit members (refs UID, UIDB, UIDP, PEST, LA: a unit's strategic
-- funding lists every researcher in the unit, ~403k people on 2,115 awards) stay off until Rohan decides.
-- Freshness: the fetcher runs weekly and names its object YYYY-MM-DD.parquet. If the pinned object is more than 10 days old the source
-- maps no rows, its row floor skips it and the Awards run fails at the end naming fct, after every other source is written.
WITH
fct_switches AS (
    SELECT true AS serve_project_team,
           false AS serve_unit_members
),
fct_fresh AS (
    SELECT coalesce(datediff(to_date(session.awards_timestamp),
        try_to_date(regexp_extract(try_element_at(session.awards_objects, 'fct'), '([0-9]{4}-[0-9]{2}-[0-9]{2})[.]parquet$', 1), 'yyyy-MM-dd')) <= 10, true) AS fresh
),
fndr AS (
    SELECT k.funder_key, k.label, CAST(f.funder_id AS BIGINT) as funder_id, f.display_name, f.ror_id, f.doi
    FROM VALUES ('fct', 'FCT', 4320334779), ('ani', 'ANI', 4320324421),
                ('ec', 'EC', 4320320300), ('erdf', 'ERDF', 4320335322)
         AS k(funder_key, label, fid)
    JOIN openalex.funders.funders f ON f.funder_id = k.fid
),
filtered AS (
    SELECT g.*,
        upper(regexp_extract(g.funder_award_id, '^([A-Za-z]+)', 1)) IN ('UID', 'UIDB', 'UIDP', 'PEST', 'LA') AS is_unit,
        filter(from_json(g.team_json, 'ARRAY<STRUCT<person_id:STRING, name:STRING, given:STRING, family:STRING, orcid:STRING, ciencia_id:STRING>>'),
               p -> p.family IS NOT NULL) AS team
    FROM fct_input g
    CROSS JOIN fct_fresh fr
    WHERE fr.fresh
      AND g.funder_award_id IS NOT NULL
      AND NOT (COALESCE(g.funder_key, 'fct') = 'ec' AND COALESCE(g.scheme, '') NOT IN ('FP1', 'FP2', 'FP3', 'FP4', 'FP5', 'FP6'))
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
        COALESCE(NULLIF(TRIM(g.title), ''), CONCAT(f.label, ' grant ', g.funder_award_id)) as display_name,
        g.description as description,
        f.funder_id,
        g.funder_award_id as funder_award_id,
        CASE WHEN TRY_CAST(g.amount AS DECIMAL(18,2)) > 0 THEN TRY_CAST(g.amount AS DECIMAL(18,2)) ELSE NULL END as amount,
        CASE WHEN TRY_CAST(g.amount AS DECIMAL(18,2)) > 0 THEN g.currency ELSE NULL END as currency,
        struct(CONCAT('https://openalex.org/F', f.funder_id) as id, f.display_name, f.ror_id, f.doi) as funder,
        COALESCE(g.funding_type, 'grant') as funding_type,
        g.scheme as funder_scheme,
        'fct' as provenance,
        TRY_TO_DATE(g.start_date_raw, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date_raw, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date_raw, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date_raw, 'yyyy-MM-dd')) as end_year,
        CASE
            WHEN g.pi_family IS NOT NULL THEN
                struct(NULLIF(TRIM(regexp_replace(g.pi_given, '(?i)^((prof|professor|professora|doutor|doutora|dr|dra|eng|engª|mestre|lic)\\.?\\s+)+', '')), '') as given_name, g.pi_family as family_name,
                    CASE WHEN g.pi_orcid RLIKE '^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]$' THEN CONCAT('https://orcid.org/', g.pi_orcid) END as orcid, CAST(NULL AS DATE) as role_start,
                    struct(g.institution as name, COALESCE(g.institution_country, CASE WHEN g.funder_key = 'fct' THEN 'Portugal' END) as country, CASE WHEN g.institution_ror IS NOT NULL THEN array(struct(g.institution_ror as id, 'ror' as type, 'fct' as asserted_by)) ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) END as ids) as affiliation)
            WHEN g.institution IS NOT NULL THEN
                struct(CAST(NULL AS STRING) as given_name, CAST(NULL AS STRING) as family_name, CAST(NULL AS STRING) as orcid, CAST(NULL AS DATE) as role_start,
                    struct(g.institution as name, COALESCE(g.institution_country, CASE WHEN g.funder_key = 'fct' THEN 'Portugal' END) as country, CASE WHEN g.institution_ror IS NOT NULL THEN array(struct(g.institution_ror as id, 'ror' as type, 'fct' as asserted_by)) ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) END as ids) as affiliation)
            ELSE NULL
        END as lead_investigator,
        CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
        CASE
            WHEN size(g.team) > 0 AND ((NOT g.is_unit AND s.serve_project_team) OR (g.is_unit AND s.serve_unit_members)) THEN
                transform(g.team, p -> struct(
                    NULLIF(TRIM(regexp_replace(p.given, '(?i)^((prof|professor|professora|doutor|doutora|dr|dra|eng|engª|mestre|lic)\\.?\\s+)+', '')), '') as given_name,
                    p.family as family_name,
                    CASE WHEN p.orcid RLIKE '^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]$' THEN CONCAT('https://orcid.org/', p.orcid) END as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(CAST(NULL AS STRING) as name, CAST(NULL AS STRING) as country, CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids) as affiliation))
            ELSE CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>)
        END as investigators,
        g.landing_page_url,
        CAST(NULL AS STRING) as doi,
        CAST(NULL AS STRING) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM filtered g
    JOIN fndr f ON f.funder_key = COALESCE(g.funder_key, 'fct')
    CROSS JOIN fct_switches s
)
