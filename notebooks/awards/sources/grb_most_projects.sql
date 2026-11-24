-- Taiwan MOST (grb_most_projects, p210), the ministry NSTC replaced in 2022: frozen. Reads the GRB export the hand-run
-- CreateTaiwanMOSTGRBAwards.ipynb loaded (s3://openalex-ingest/awards/taiwan_most_grb/taiwan_most_grb_projects.parquet,
-- 2026-06-18, laptop scripts/local/taiwan_most_grb_to_s3.py); that notebook's SELECT, unchanged but for the names.
-- created_date and updated_date are the run timestamp.
-- Names (names-orcid card 3, carried here so its hand run is not needed): a Chinese name printed whole in family_name is
-- split into family + given. The surname is the first character, or the first two when they form one of the listed
-- two-character surnames; only 2-4 character all-Han names are split (a 4-character name only with a listed surname);
-- anything else stays as printed, and a name that already has a given name is untouched.
WITH
most_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322795  -- Ministry of Science and Technology, Taiwan
),
switches AS (
    SELECT true AS serve_people   -- lead, co-lead and investigator names (false: no people)
),
raw_prepared AS (
    SELECT
        *,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        TRY_CAST(start_year AS INT) AS raw_start_year,
        TRY_CAST(end_year AS INT) AS raw_end_year,
        FROM_JSON(
            co_lead_json,
            'STRUCT<given_name:STRING,family_name:STRING,orcid:STRING,role_start_year:STRING,affiliation_name:STRING,affiliation_country:STRING>'
        ) AS parsed_co_lead,
        FROM_JSON(
            investigators_json,
            'ARRAY<STRUCT<given_name:STRING,family_name:STRING,orcid:STRING,role_start_year:STRING,affiliation_name:STRING,affiliation_country:STRING>>'
        ) AS parsed_investigators
    FROM grb_most_projects_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
),
cleaned AS (
    SELECT
        *,
        CASE WHEN raw_start_year > YEAR(current_date()) + 1 THEN NULL ELSE raw_start_year END AS parsed_start_year,
        CASE WHEN raw_start_year > YEAR(current_date()) + 1 THEN NULL ELSE raw_end_year END AS parsed_end_year,
        transform(array(named_struct('given_name', lead_given_name, 'family_name', lead_family_name)), p -> named_struct('given', p.given_name, 'family', TRIM(p.family_name), 'k', CASE WHEN NULLIF(TRIM(p.given_name), '') IS NULL AND TRIM(p.family_name) RLIKE '^\\p{IsHan}{2,4}$' THEN CASE WHEN length(TRIM(p.family_name)) > 2 AND instr(' 欧阳 歐陽 司马 司馬 诸葛 諸葛 上官 东方 東方 皇甫 尉迟 尉遲 公孙 公孫 慕容 令狐 长孙 長孫 宇文 司徒 夏侯 轩辕 軒轅 端木 申屠 澹台 澹臺 太史 闻人 聞人 南宫 南宮 呼延 西门 西門 左丘 濮阳 濮陽 淳于 单于 單于 公冶 赫连 赫連 钟离 鍾離 宗政 司空 鲜于 鮮于 闾丘 閭丘 万俟 拓跋 百里 东郭 東郭 张简 張簡 范姜 张廖 張廖 ', concat(' ', substr(TRIM(p.family_name), 1, 2), ' ')) > 0 THEN 2 WHEN length(TRIM(p.family_name)) <= 3 THEN 1 END END))[0] AS lead_k,
        CASE WHEN parsed_co_lead IS NOT NULL THEN transform(array(parsed_co_lead), p -> named_struct('given', p.given_name, 'family', TRIM(p.family_name), 'k', CASE WHEN NULLIF(TRIM(p.given_name), '') IS NULL AND TRIM(p.family_name) RLIKE '^\\p{IsHan}{2,4}$' THEN CASE WHEN length(TRIM(p.family_name)) > 2 AND instr(' 欧阳 歐陽 司马 司馬 诸葛 諸葛 上官 东方 東方 皇甫 尉迟 尉遲 公孙 公孫 慕容 令狐 长孙 長孫 宇文 司徒 夏侯 轩辕 軒轅 端木 申屠 澹台 澹臺 太史 闻人 聞人 南宫 南宮 呼延 西门 西門 左丘 濮阳 濮陽 淳于 单于 單于 公冶 赫连 赫連 钟离 鍾離 宗政 司空 鲜于 鮮于 闾丘 閭丘 万俟 拓跋 百里 东郭 東郭 张简 張簡 范姜 张廖 張廖 ', concat(' ', substr(TRIM(p.family_name), 1, 2), ' ')) > 0 THEN 2 WHEN length(TRIM(p.family_name)) <= 3 THEN 1 END END))[0] END AS co_lead_k,
        transform(parsed_investigators, p -> named_struct('given', p.given_name, 'family', TRIM(p.family_name), 'k', CASE WHEN NULLIF(TRIM(p.given_name), '') IS NULL AND TRIM(p.family_name) RLIKE '^\\p{IsHan}{2,4}$' THEN CASE WHEN length(TRIM(p.family_name)) > 2 AND instr(' 欧阳 歐陽 司马 司馬 诸葛 諸葛 上官 东方 東方 皇甫 尉迟 尉遲 公孙 公孫 慕容 令狐 长孙 長孫 宇文 司徒 夏侯 轩辕 軒轅 端木 申屠 澹台 澹臺 太史 闻人 聞人 南宫 南宮 呼延 西门 西門 左丘 濮阳 濮陽 淳于 单于 單于 公冶 赫连 赫連 钟离 鍾離 宗政 司空 鲜于 鮮于 闾丘 閭丘 万俟 拓跋 百里 东郭 東郭 张简 張簡 范姜 张廖 張廖 ', concat(' ', substr(TRIM(p.family_name), 1, 2), ' ')) > 0 THEN 2 WHEN length(TRIM(p.family_name)) <= 3 THEN 1 END END)) AS investigators_k
    FROM raw_prepared
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 AS id,
        g.display_name AS display_name,
        g.description AS description,
        f.funder_id,
        g.funder_award_id,
        g.parsed_amount AS amount,
        g.currency AS currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,
        'research' AS funding_type,
        COALESCE(NULLIF(TRIM(g.research_method), ''), 'MOST GRB project') AS funder_scheme,
        'grb_most_projects' AS provenance,
        CAST(NULL AS DATE) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        g.parsed_start_year AS start_year,
        g.parsed_end_year AS end_year,
        CASE WHEN s.serve_people THEN
            struct(CASE WHEN g.lead_k.k IS NOT NULL THEN substr(g.lead_k.family, g.lead_k.k + 1) ELSE NULLIF(TRIM(g.lead_k.given), '') END AS given_name,
                CASE WHEN g.lead_k.k IS NOT NULL THEN substr(g.lead_k.family, 1, g.lead_k.k) ELSE NULLIF(g.lead_k.family, '') END AS family_name,
                CAST(NULL AS STRING) AS orcid, CAST(NULL AS DATE) AS role_start,
                struct(NULLIF(TRIM(g.executing_institution), '') AS name, CAST(NULL AS STRING) AS country, CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids) AS affiliation)
        END AS lead_investigator,
        CASE WHEN s.serve_people AND g.parsed_co_lead IS NOT NULL THEN
            struct(CASE WHEN g.co_lead_k.k IS NOT NULL THEN substr(g.co_lead_k.family, g.co_lead_k.k + 1) ELSE NULLIF(TRIM(g.co_lead_k.given), '') END AS given_name,
                CASE WHEN g.co_lead_k.k IS NOT NULL THEN substr(g.co_lead_k.family, 1, g.co_lead_k.k) ELSE NULLIF(g.co_lead_k.family, '') END AS family_name,
                CAST(NULL AS STRING) AS orcid, CAST(NULL AS DATE) AS role_start,
                struct(NULLIF(TRIM(g.parsed_co_lead.affiliation_name), '') AS name, CAST(NULL AS STRING) AS country, CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids) AS affiliation)
        END AS co_lead_investigator,
        CASE WHEN s.serve_people AND size(g.parsed_investigators) > 0 THEN
            transform(arrays_zip(g.investigators_k, g.parsed_investigators), z ->
            struct(CASE WHEN z.investigators_k.k IS NOT NULL THEN substr(z.investigators_k.family, z.investigators_k.k + 1) ELSE NULLIF(TRIM(z.investigators_k.given), '') END AS given_name,
                CASE WHEN z.investigators_k.k IS NOT NULL THEN substr(z.investigators_k.family, 1, z.investigators_k.k) ELSE NULLIF(z.investigators_k.family, '') END AS family_name,
                CAST(NULL AS STRING) AS orcid, CAST(NULL AS DATE) AS role_start,
                struct(NULLIF(TRIM(z.parsed_investigators.affiliation_name), '') AS name, CAST(NULL AS STRING) AS country, CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids) AS affiliation))
        END AS investigators,
        g.landing_page_url AS landing_page_url,
        CAST(NULL AS STRING) AS doi,
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000) AS works_api_url,
        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM cleaned g
    CROSS JOIN most_funder f
    CROSS JOIN switches s
)
