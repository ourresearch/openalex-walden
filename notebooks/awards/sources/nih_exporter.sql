-- Mapping from CreateNIHAwards.ipynb (origin 322e8280): its awards-table SELECT over nih_exporter_input (was the staging
-- table openalex.awards.nih_grants_raw, read from s3://openalex-ingest/awards/nih/nih_projects_combined.parquet), now
-- the weekly object of openalex-ingest's nih_awards.py (ExPORTER FY1985-2025 + RePORTER API FY2026 on, same 48 columns).
-- Edits (feeds-c, 11-05; ship/tools/make_nih_mapping.py): the year-collision hold is a frozen list, not a read of served
-- awards; the two asserts emit an invalid row instead of failing the run; chr(59) for ';'; run timestamp for the dates.
-- The keys are unchanged: funder from administering_ic, number lower(full_project_num) or core[-a<appl>]:sub-project,
-- id xxhash64(funder:number) (the NIH fold keys on them).
WITH ic_funder_map AS (
    -- NIH ExPORTER / RePORTER is the shared grants-reporting system for HHS, so the
    -- download bundles grants administered by NON-NIH agencies (CDC, FDA, SAMHSA,
    -- HRSA, AHRQ, ATSDR, IHS, ACF, ONC) plus VA. The administering_ic column tells
    -- us the real funder. Map each non-NIH IC code to its OpenAlex funder here;
    -- anything not listed defaults to NIH (4320332161) below.
    -- Ambiguous low-volume codes (AD "Office of the Administrator", MN/FP OASH) are
    -- intentionally left as NIH — no clean canonical funder. See how-to-add-a-funder-v2.md.
    SELECT * FROM VALUES
        ('VA',4320306127),                                                                                 -- U.S. Department of Veterans Affairs
        ('HS',4320332177),                                                                                 -- AHRQ
        ('SU',4320332164),('SP',4320332164),('SM',4320332164),('TI',4320332164),                           -- SAMHSA (incl. CSAP/CSAT/CMHS)
        ('FD',4320332163),('BK',4320332163),('BJ',4320332163),('BA',4320332163),('BN',4320332163),         -- FDA (FD + CBER "B" branch codes)
        ('BB',4320332163),('BQ',4320332163),('BR',4320332163),('BD',4320332163),('BL',4320332163),
        ('BI',4320332163),('BM',4320332163),('BF',4320332163),('BO',4320332163),('BH',4320332163),
        ('BG',4320332163),('BE',4320332163),('BS',4320332163),('BT',4320332163),('BU',4320332163),
        ('OH',4320337382),                                                                                 -- NIOSH (CDC)
        ('PS',4320332162),('DP',4320332162),('CE',4320332162),('GH',4320332162),('DD',4320332162),         -- CDC (all other CDC center/office codes)
        ('IP',4320332162),('EH',4320332162),('CI',4320332162),('TP',4320332162),('CK',4320332162),
        ('CD',4320332162),('SO',4320332162),('HM',4320332162),('EP',4320332162),('GD',4320332162),
        ('SH',4320332162),('PH',4320332162),('PR',4320332162),('WC',4320332162),('HK',4320332162),
        ('CC',4320332162),
        ('PE',4320332175),('NU',4320332175),('MB',4320332175),('AH',4320332175),('DH',4320332175),         -- HRSA
        ('BP',4320332175),('NC',4320332175),('HP',4320332175),
        ('TS',4320337419),                                                                                 -- ATSDR
        ('IS',4320332207),                                                                                 -- Indian Health Service
        ('AF',4320332208),                                                                                 -- Administration for Children and Families
        ('OC',4320332334)                                                                                  -- Office of the National Coordinator for Health IT
        AS t(ic, funder_id)
),

funder_lookup AS (
    -- Funder display fields for NIH + every agency referenced in ic_funder_map.
    -- All are F4320* funders, so they are present in openalex.funders.funders.
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id IN (
        4320332161,  -- National Institutes of Health (default)
        4320332162,  -- Centers for Disease Control and Prevention
        4320332163,  -- U.S. Food and Drug Administration
        4320332164,  -- Substance Abuse and Mental Health Services Administration
        4320332175,  -- Health Resources and Services Administration
        4320332177,  -- Agency for Healthcare Research and Quality
        4320306127,  -- U.S. Department of Veterans Affairs
        4320337382,  -- National Institute for Occupational Safety and Health
        4320337419,  -- Agency for Toxic Substances and Disease Registry
        4320332207,  -- Indian Health Service
        4320332208,  -- Administration for Children and Families
        4320332334   -- Office of the National Coordinator for Health IT
    )
),

-- RePORTER's cost for a project year ExPORTER publishes without one: ExPORTER's FY1985-1999 project files carry no cost (0 of
-- 840,057 such rows in the 18 Nov object). scripts/local/nih_reporter_costs_to_s3.py fetched it once by application id
-- (sub-projects excluded); only a positive cost is used (oxjob #2326, NIH totals C, carried over from CreateNIHAwards). One row
-- per application id: a repeated id would repeat a project year, and the guard would hold the source on its duplicate keys.
nih_reporter_costs AS (
    SELECT CAST(application_id AS STRING) AS application_id, CAST(award_amount AS DOUBLE) AS award_amount
    FROM read_files('s3://openalex-ingest/awards/nih/nih_reporter_costs.parquet', format => 'parquet')
    WHERE award_amount > 0
),

-- Parse PI names (format: "LASTNAME, FIRSTNAME (contact); LASTNAME2, FIRSTNAME2")
-- and resolve the true funder from administering_ic (defaulting to NIH).
parsed_all AS (
    SELECT
        r.*,
        rc.award_amount as reporter_cost,
        -- Resolve true funder: non-NIH agency if mapped, else NIH
        COALESCE(m.funder_id, 4320332161) as resolved_funder_id,
        -- Award key: the full project number for an award (and for a center); a center's sub-projects carry the center's
        -- number plus their sub-project id, so each is its own record and the center keeps its own number and id.
        CASE WHEN r.subproject_id IS NULL OR TRIM(r.subproject_id) = '' THEN LOWER(r.full_project_num)
             ELSE CONCAT(LOWER(r.full_project_num), ':', TRIM(r.subproject_id)) END as award_key,
        -- Extract first PI as lead investigator (before any semicolon)
        CASE
            WHEN r.pi_names IS NOT NULL AND r.pi_names != ''
            -- chr(59) is the semicolon between PIs (the generator allows none in mapping code)
            THEN TRIM(split(r.pi_names, chr(59))[0])
            ELSE NULL
        END as first_pi_name
    FROM nih_exporter_input r
    LEFT JOIN ic_funder_map m
        ON UPPER(TRIM(r.administering_ic)) = m.ic
    -- never for a sub-project (its yearly cost is not its total)
    LEFT JOIN nih_reporter_costs rc
        ON rc.application_id = CAST(r.application_id AS STRING) AND (r.subproject_id IS NULL OR TRIM(r.subproject_id) = '')
),

-- One record per NIH sub-project study, not one per fiscal year (Rohan 10-06/10-07, option A, identity rule C after two reviews).
-- A sub-project is (core project number, sub-project id); resource centers (P41/P51 above all) reuse an id for different studies.
-- Within one sub-project, two rows are the same study when their titles share enough words for who ran them: >= 10% with the same PI,
-- >= 50% with different PIs (a core handed to a new director), >= 60% when a PI is unknown. A study = a connected group of rows (rows are
-- compared with each other, not only with the first row, so a title that drifts over the years stays one study). The group holding the
-- sub-project's first row is keyed core:sid; any other group core-a<application id of its earliest row>:sid (unique per group, stable).
-- PI = NIH's contact PI profile id from pi_ids (1.2% of rows have none). Each record shows its latest row (title, people, description,
-- institute) and the span of its rows' project dates. No amount: a yearly cost is not the sub-project's total.
sub_in AS (
    SELECT p.*, UPPER(TRIM(p.core_project_num)) AS sp_core, TRIM(p.subproject_id) AS sp_id, CAST(p.fy AS INT) AS sp_fy,
        array_sort(array_distinct(split(trim(regexp_replace(lower(coalesce(p.project_title, '')), '[^a-z0-9 ]', ' ')), ' +'))) AS sp_words,
        COALESCE(NULLIF(regexp_extract(coalesce(p.pi_ids, ''), '([0-9]+) *\\(contact\\)', 1), ''), NULLIF(regexp_extract(coalesce(p.pi_ids, ''), '([0-9]+)', 1), '')) AS sp_pi,
        COALESCE(TRY_TO_DATE(p.project_start, 'yyyy-MM-dd'), TRY_TO_DATE(p.project_start, 'MM/dd/yyyy'), TRY_TO_DATE(p.project_start, 'M/d/yyyy')) AS sp_start,
        COALESCE(TRY_TO_DATE(p.project_end, 'yyyy-MM-dd'), TRY_TO_DATE(p.project_end, 'MM/dd/yyyy'), TRY_TO_DATE(p.project_end, 'M/d/yyyy')) AS sp_end,
        ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(p.core_project_num)), TRIM(p.subproject_id)
                           ORDER BY CAST(p.fy AS INT), p.full_project_num, CAST(p.application_id AS STRING)) - 1 AS sp_rid
    FROM parsed_all p
    WHERE NOT (p.subproject_id IS NULL OR TRIM(p.subproject_id) = '')
),
sub_arr AS (
    SELECT sp_core, sp_id, transform(array_sort(collect_list(struct(sp_rid AS rid, sp_words AS w, sp_pi AS pi, CAST(application_id AS STRING) AS app))), x -> x) AS rs
    FROM sub_in GROUP BY sp_core, sp_id
),
sub_lab AS (
    -- connected groups by min-label propagation over each sub-project's rows (at most a few dozen), 12 rounds; round 13 must change nothing
    SELECT sp_core, sp_id, rs, aggregate(sequence(1, 12), transform(rs, r -> r.rid), (labs, i) -> transform(rs, (r, k) -> array_min(concat(array(labs[k]), filter(transform(rs, (o, j) -> IF((size(array_union(r.w, o.w)) = 0 OR 20 * size(array_intersect(r.w, o.w)) >= (CASE WHEN r.pi IS NOT NULL AND o.pi IS NOT NULL AND r.pi = o.pi THEN 2 WHEN r.pi IS NOT NULL AND o.pi IS NOT NULL THEN 10 ELSE 12 END) * size(array_union(r.w, o.w))), labs[j], NULL)), x -> x IS NOT NULL))))) AS labs12
    FROM sub_arr
),
sub_lab_ok AS (
    SELECT *, aggregate(sequence(1, 1), labs12, (labs, i) -> transform(rs, (r, k) -> array_min(concat(array(labs[k]), filter(transform(rs, (o, j) -> IF((size(array_union(r.w, o.w)) = 0 OR 20 * size(array_intersect(r.w, o.w)) >= (CASE WHEN r.pi IS NOT NULL AND o.pi IS NOT NULL AND r.pi = o.pi THEN 2 WHEN r.pi IS NOT NULL AND o.pi IS NOT NULL THEN 10 ELSE 12 END) * size(array_union(r.w, o.w))), labs[j], NULL)), x -> x IS NOT NULL))))) AS labs13 FROM sub_lab
),
sub_hold AS (
    -- HOLD (not built, no id minted) a whole sub-project whose grouping is ambiguous (Rohan 10-07: ship the clear ones):
    --   a group whose rows are not all directly linked (joined only through a bridge row), or
    --   any pair of rows whose overlap is within 0.05 of its threshold, inclusive, in exact integer arithmetic (linked or not), or
    --   a 1-2 digit sub-project id no larger than its center's latest support year whose number a paper-only NIH record already
    --   carries (core:38 vs a paper citing core-38).
    -- Overlap and thresholds use exact integers: shared words x 20 against threshold x 20 (2 / 10 / 12) x all words.
    SELECT *,
        exists(rs, r -> exists(rs, o -> o.rid > r.rid AND labs12[r.rid] = labs12[o.rid] AND NOT (size(array_union(r.w, o.w)) = 0 OR 20 * size(array_intersect(r.w, o.w)) >= (CASE WHEN r.pi IS NOT NULL AND o.pi IS NOT NULL AND r.pi = o.pi THEN 2 WHEN r.pi IS NOT NULL AND o.pi IS NOT NULL THEN 10 ELSE 12 END) * size(array_union(r.w, o.w))))) AS sp_chained,
        exists(rs, r -> exists(rs, o -> o.rid > r.rid AND size(array_union(r.w, o.w)) > 0 AND abs(20 * size(array_intersect(r.w, o.w)) - (CASE WHEN r.pi IS NOT NULL AND o.pi IS NOT NULL AND r.pi = o.pi THEN 2 WHEN r.pi IS NOT NULL AND o.pi IS NOT NULL THEN 10 ELSE 12 END) * size(array_union(r.w, o.w))) <= size(array_union(r.w, o.w)))) AS sp_near
    FROM sub_lab_ok
),
sub_year_collision AS (
    -- Frozen (feeds-c, 11-05): the sub-projects this hold kept out of raw's NIH slice when it was last built (10-09). It read
    -- served openalex.awards.openalex_awards for paper-only NIH records (core:38 vs a paper citing core-38), so the
    -- mapping moved with Nightly. Measured 11-05: none of the 4 is in raw; today the live rule would hold only the last.
    -- Releasing one is a key decision for the NIH fold (Jason), made by editing this list.
    SELECT * FROM VALUES ('P30CA014195', '38'), ('P30CA014195', '47'), ('P30CA014051', '49'), ('P50GM021700', '1')
    AS t(sp_core, sp_id)
),
sub_label AS (
    SELECT l.sp_core, l.sp_id, x.pos AS sp_rid, l.labs12[x.pos] AS sp_label, (l.labs12 = l.labs13) AS sp_converged,
        (l.sp_chained OR l.sp_near OR yc.sp_core IS NOT NULL) AS sp_hold
    FROM sub_hold l LEFT JOIN sub_year_collision yc ON yc.sp_core = l.sp_core AND yc.sp_id = l.sp_id
    LATERAL VIEW posexplode(l.rs) x AS pos, r
),
sub_grp AS (
    SELECT s.*, CASE WHEN g.sp_label = 0 THEN ''
                     ELSE CONCAT('-a', CAST(f.application_id AS STRING)) END AS sp_grp,
        g.sp_label, g.sp_hold,
        -- not converged: the row is emitted without a number, so the Awards job skips NIH (invalid rows) and names it
        g.sp_converged AS sp_ok
    FROM sub_in s
    JOIN sub_label g ON g.sp_core = s.sp_core AND g.sp_id = s.sp_id AND g.sp_rid = s.sp_rid
    JOIN sub_in f ON f.sp_core = s.sp_core AND f.sp_id = s.sp_id AND f.sp_rid = g.sp_label
),
sub_one AS (
    SELECT *, MIN(sp_start) OVER w AS sp_min_start, MAX(sp_end) OVER w AS sp_max_end,
        ROW_NUMBER() OVER (PARTITION BY sp_core, sp_id, sp_grp ORDER BY sp_fy DESC, full_project_num DESC) AS sp_rn,
        -- not one key per group: emitted without a number, as above
        size(collect_set(sp_grp) OVER (PARTITION BY sp_core, sp_id)) = size(collect_set(sp_label) OVER (PARTITION BY sp_core, sp_id)) AS sp_ok2
    FROM sub_grp
    WINDOW w AS (PARTITION BY sp_core, sp_id, sp_grp)
),
parsed_grants AS (
    SELECT project_title, phr, total_cost, total_cost_sub_project, subproject_id, activity, project_start, project_end, first_pi_name,
        org_name, org_country, application_id, full_project_num, award_key, resolved_funder_id, true AS key_ok, pi_names, reporter_cost
    FROM parsed_all
    WHERE subproject_id IS NULL OR TRIM(subproject_id) = ''
    UNION ALL
    SELECT project_title, phr, total_cost, total_cost_sub_project, subproject_id, activity,
        date_format(sp_min_start, 'yyyy-MM-dd') AS project_start, date_format(sp_max_end, 'yyyy-MM-dd') AS project_end, first_pi_name,
        org_name, org_country, application_id, full_project_num,
        CONCAT(LOWER(sp_core), sp_grp, ':', sp_id) AS award_key,
        resolved_funder_id, sp_ok AND sp_ok2 AS key_ok, pi_names, CAST(NULL AS DOUBLE) AS reporter_cost
    FROM sub_one
    WHERE sp_rn = 1 AND NOT sp_hold
),

awards_transformed AS (
    SELECT
        -- Generate unique ID using xxhash64 of funder + award_id
        abs(xxhash64(CONCAT(f.funder_id, ':', g.award_key))) % 9000000000 as id,

        -- Display name = project title
        g.project_title as display_name,

        -- Description = PHR (Public Health Relevance statement)
        g.phr as description,

        -- Funder info (resolved from administering_ic)
        f.funder_id,
        CASE WHEN g.key_ok THEN g.award_key END as funder_award_id,

        -- Funding amount (total cost for this fiscal year). Sub-project records have none (see sub_in above): one record spans
        -- several years, and a sub-project row's yearly cost is not its total.
        -- ExPORTER's cost first; where it publishes none (its FY1985-1999 project files), RePORTER's for the same application.
        CAST(CASE WHEN g.subproject_id IS NULL OR TRIM(g.subproject_id) = '' THEN COALESCE(TRY_CAST(g.total_cost AS DOUBLE), g.reporter_cost) END AS DOUBLE) as amount,
        'USD' as currency,

        -- Funder struct
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        -- Funding type based on activity code
        CASE
            WHEN g.activity LIKE 'R%' THEN 'research'
            WHEN g.activity LIKE 'K%' THEN 'career_development'
            WHEN g.activity LIKE 'T%' THEN 'training'
            WHEN g.activity LIKE 'F%' THEN 'fellowship'
            WHEN g.activity LIKE 'P%' THEN 'program'
            WHEN g.activity LIKE 'U%' THEN 'cooperative_agreement'
            WHEN g.activity LIKE 'N%' THEN 'contract'
            WHEN g.activity LIKE 'Z%' THEN 'intramural'
            ELSE 'grant'
        END as funding_type,

        -- Funder scheme = activity code (R01, K08, etc)
        g.activity as funder_scheme,

        -- Provenance
        'nih_exporter' as provenance,

        -- Dates (format varies, try multiple patterns)
        COALESCE(
            TRY_TO_DATE(g.project_start, 'yyyy-MM-dd'),
            TRY_TO_DATE(g.project_start, 'MM/dd/yyyy'),
            TRY_TO_DATE(g.project_start, 'M/d/yyyy')
        ) as start_date,
        COALESCE(
            TRY_TO_DATE(g.project_end, 'yyyy-MM-dd'),
            TRY_TO_DATE(g.project_end, 'MM/dd/yyyy'),
            TRY_TO_DATE(g.project_end, 'M/d/yyyy')
        ) as end_date,
        YEAR(COALESCE(
            TRY_TO_DATE(g.project_start, 'yyyy-MM-dd'),
            TRY_TO_DATE(g.project_start, 'MM/dd/yyyy'),
            TRY_TO_DATE(g.project_start, 'M/d/yyyy')
        )) as start_year,
        YEAR(COALESCE(
            TRY_TO_DATE(g.project_end, 'yyyy-MM-dd'),
            TRY_TO_DATE(g.project_end, 'MM/dd/yyyy'),
            TRY_TO_DATE(g.project_end, 'M/d/yyyy')
        )) as end_year,

        -- Lead investigator (parse "LASTNAME, FIRSTNAME" or "LASTNAME, FIRSTNAME (contact)")
        CASE
            WHEN g.first_pi_name IS NOT NULL THEN
                struct(
                    -- Extract given name (after comma, before any parenthetical)
                    CASE
                        WHEN CONTAINS(g.first_pi_name, ',')
                        THEN TRIM(REGEXP_REPLACE(split(g.first_pi_name, ',')[1], '\\s*\\(.*\\)\\s*', ''))
                        ELSE NULL
                    END as given_name,
                    -- Extract family name (before comma)
                    CASE
                        WHEN CONTAINS(g.first_pi_name, ',')
                        THEN TRIM(split(g.first_pi_name, ',')[0])
                        ELSE TRIM(g.first_pi_name)
                    END as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.org_name as name,
                        g.org_country as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,

        -- Co-lead and other investigators (NULL for now, could be enriched from pi_names)
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

        -- Landing page URL (NIH RePORTER project details page; covers all ExPORTER agencies)
        CONCAT('https://reporter.nih.gov/project-details/', g.application_id) as landing_page_url,

        -- No DOI for NIH grants typically
        CAST(NULL AS STRING) as doi,

        -- Works API URL
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', g.award_key))) % 9000000000) as works_api_url,

        -- Timestamps
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM parsed_grants g
    JOIN funder_lookup f
        ON f.funder_id = g.resolved_funder_id
    WHERE g.full_project_num IS NOT NULL
      AND TRIM(g.full_project_num) != ''
)
