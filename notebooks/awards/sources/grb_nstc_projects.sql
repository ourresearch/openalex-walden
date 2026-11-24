-- Taiwan NSTC (grb_nstc_projects, p202): GRB's open per-year XML (data.gov.tw 18707, Government Open Data License v1),
-- written monthly by openalex-ingest grb_awards.py to s3://openalex-ingest/awards/grb_nstc/ (one row per cited plan
-- number, NSTC's own plans from ROC 111). Replaces the hand-run CreateNSTCGRBAwards.ipynb (laptop export
-- nstc_grb/nstc_grb_projects.parquet): same keys, ids and columns, so the Awards MERGE updates the rows in place.
-- Lead = the PI; co_lead_investigator = the first researcher and investigators = the rest, in GRB's order, as before.
-- start_date / end_date stay NULL (GRB publishes ROC year-months); a start year after next year is dropped, as before.
-- Names (names-orcid card 3, carried here so its hand run is not needed): a Chinese name printed whole in family_name is
-- split into family + given. The surname is the first character, or the first two when they form one of the listed
-- two-character surnames; only 2-4 character all-Han names are split (a 4-character name only with a listed surname);
-- anything else stays as printed, and a name that already has a given name is untouched.
-- Descriptions: the open files carry no abstract text, only a link. carry_saved_abstracts keeps the abstracts the laptop
-- export saved (nstc_grb_projects.parquet, 2026-06-03, read-only) for the plans it held; plans added since have none.
WITH
nstc_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 2461203286  -- National Science and Technology Council
),
switches AS (
    SELECT
        true AS serve_people,           -- lead, co-lead and investigator names (false: no people)
        true AS carry_saved_abstracts   -- false: no description
),
saved_abstracts AS (
    SELECT lower(funder_award_id) AS award_key, min(description) AS description
    FROM read_files('s3://openalex-ingest/awards/nstc_grb/nstc_grb_projects.parquet', format => 'parquet')
    WHERE description IS NOT NULL
    GROUP BY lower(funder_award_id)
),
cleaned AS (
    SELECT
        g.*,
        CASE WHEN TRY_CAST(g.start_year AS INT) > YEAR(current_date()) + 1 THEN NULL ELSE TRY_CAST(g.start_year AS INT) END AS parsed_start_year,
        CASE WHEN TRY_CAST(g.start_year AS INT) > YEAR(current_date()) + 1 THEN NULL ELSE TRY_CAST(g.end_year AS INT) END AS parsed_end_year,
        transform(array(coalesce(g.pi, named_struct('name', CAST(NULL AS STRING), 'given_name', CAST(NULL AS STRING), 'family_name', CAST(NULL AS STRING)))), p -> named_struct('given', p.given_name, 'family', TRIM(p.family_name), 'k', CASE WHEN NULLIF(TRIM(p.given_name), '') IS NULL AND TRIM(p.family_name) RLIKE '^\\p{IsHan}{2,4}$' THEN CASE WHEN length(TRIM(p.family_name)) > 2 AND instr(' 欧阳 歐陽 司马 司馬 诸葛 諸葛 上官 东方 東方 皇甫 尉迟 尉遲 公孙 公孫 慕容 令狐 长孙 長孫 宇文 司徒 夏侯 轩辕 軒轅 端木 申屠 澹台 澹臺 太史 闻人 聞人 南宫 南宮 呼延 西门 西門 左丘 濮阳 濮陽 淳于 单于 單于 公冶 赫连 赫連 钟离 鍾離 宗政 司空 鲜于 鮮于 闾丘 閭丘 万俟 拓跋 百里 东郭 東郭 张简 張簡 范姜 张廖 張廖 ', concat(' ', substr(TRIM(p.family_name), 1, 2), ' ')) > 0 THEN 2 WHEN length(TRIM(p.family_name)) <= 3 THEN 1 END END))[0] AS lead_k,
        transform(g.researchers, p -> named_struct('given', p.given_name, 'family', TRIM(p.family_name), 'k', CASE WHEN NULLIF(TRIM(p.given_name), '') IS NULL AND TRIM(p.family_name) RLIKE '^\\p{IsHan}{2,4}$' THEN CASE WHEN length(TRIM(p.family_name)) > 2 AND instr(' 欧阳 歐陽 司马 司馬 诸葛 諸葛 上官 东方 東方 皇甫 尉迟 尉遲 公孙 公孫 慕容 令狐 长孙 長孫 宇文 司徒 夏侯 轩辕 軒轅 端木 申屠 澹台 澹臺 太史 闻人 聞人 南宫 南宮 呼延 西门 西門 左丘 濮阳 濮陽 淳于 单于 單于 公冶 赫连 赫連 钟离 鍾離 宗政 司空 鲜于 鮮于 闾丘 閭丘 万俟 拓跋 百里 东郭 東郭 张简 張簡 范姜 张廖 張廖 ', concat(' ', substr(TRIM(p.family_name), 1, 2), ' ')) > 0 THEN 2 WHEN length(TRIM(p.family_name)) <= 3 THEN 1 END END)) AS researchers_k,
        NULLIF(TRIM(g.executing_institution), '') AS institution,
        NULLIF(TRIM(g.researchers_institution), '') AS researchers_inst
    FROM grb_nstc_projects_input g
    WHERE g.funder_award_id IS NOT NULL
      AND TRIM(g.funder_award_id) != ''
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 AS id,
        g.display_name AS display_name,
        CASE WHEN s.carry_saved_abstracts THEN a.description END AS description,
        f.funder_id,
        g.funder_award_id,
        g.amount AS amount,
        g.currency AS currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,
        'research' AS funding_type,
        COALESCE(NULLIF(TRIM(g.research_type), ''), 'NSTC GRB project') AS funder_scheme,
        'grb_nstc_projects' AS provenance,
        CAST(NULL AS DATE) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        g.parsed_start_year AS start_year,
        g.parsed_end_year AS end_year,
        CASE WHEN s.serve_people THEN
            struct(CASE WHEN g.lead_k.k IS NOT NULL THEN substr(g.lead_k.family, g.lead_k.k + 1) ELSE NULLIF(TRIM(g.lead_k.given), '') END AS given_name,
                CASE WHEN g.lead_k.k IS NOT NULL THEN substr(g.lead_k.family, 1, g.lead_k.k) ELSE NULLIF(g.lead_k.family, '') END AS family_name,
                CAST(NULL AS STRING) AS orcid, CAST(NULL AS DATE) AS role_start,
                struct(g.institution AS name, CAST(NULL AS STRING) AS country, CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids) AS affiliation)
        END AS lead_investigator,
        CASE WHEN s.serve_people AND size(g.researchers_k) > 0 THEN
            struct(CASE WHEN g.researchers_k[0].k IS NOT NULL THEN substr(g.researchers_k[0].family, g.researchers_k[0].k + 1) ELSE NULLIF(TRIM(g.researchers_k[0].given), '') END AS given_name,
                CASE WHEN g.researchers_k[0].k IS NOT NULL THEN substr(g.researchers_k[0].family, 1, g.researchers_k[0].k) ELSE NULLIF(g.researchers_k[0].family, '') END AS family_name,
                CAST(NULL AS STRING) AS orcid, CAST(NULL AS DATE) AS role_start,
                struct(g.researchers_inst AS name, CAST(NULL AS STRING) AS country, CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids) AS affiliation)
        END AS co_lead_investigator,
        CASE WHEN s.serve_people AND size(g.researchers_k) > 1 THEN
            transform(slice(g.researchers_k, 2, size(g.researchers_k) - 1), x ->
            struct(CASE WHEN x.k IS NOT NULL THEN substr(x.family, x.k + 1) ELSE NULLIF(TRIM(x.given), '') END AS given_name,
                CASE WHEN x.k IS NOT NULL THEN substr(x.family, 1, x.k) ELSE NULLIF(x.family, '') END AS family_name,
                CAST(NULL AS STRING) AS orcid, CAST(NULL AS DATE) AS role_start,
                struct(g.researchers_inst AS name, CAST(NULL AS STRING) AS country, CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids) AS affiliation))
        END AS investigators,
        g.landing_page_url AS landing_page_url,
        CAST(NULL AS STRING) AS doi,
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000) AS works_api_url,
        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM cleaned g
    CROSS JOIN nstc_funder f
    CROSS JOIN switches s
    LEFT JOIN saved_abstracts a ON a.award_key = lower(g.funder_award_id)
)
