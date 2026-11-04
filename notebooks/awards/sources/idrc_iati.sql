-- Mapping from CreateIDRCAwards.ipynb (origin e22fdf7f): its awards-table SELECT over idrc_iati_input (was the
-- staging table read from s3://openalex-ingest/awards/idrc/idrc_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 idrc_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320319949  -- International Development Research Centre
),
parsed AS (
    SELECT
        iati_identifier,
        title_en, title_fr, title_es,
        description_en, description_fr, description_es,
        actual_start, planned_start, actual_end, planned_end,
        activity_status_code,
        from_json(budgets_json,
            'array<struct<status:string,type:string,period_start:string,period_end:string,value:string,currency:string,value_date:string>>') as budgets,
        from_json(participating_orgs_json,
            'array<struct<role:string,type:string,ref:string,name_en:string,name_fr:string,name_es:string>>') as orgs,
        from_json(recipient_countries_json,
            'array<struct<code:string,percentage:string>>') as recipient_countries,
        source_xml_url
    FROM idrc_iati_input
),
aggregated AS (
    SELECT
        iati_identifier,
        COALESCE(title_en, title_fr, title_es) as display_name,
        COALESCE(description_en, description_fr, description_es) as description,
        AGGREGATE(
            COALESCE(budgets, ARRAY()),
            0.0D,
            (acc, b) -> acc + COALESCE(TRY_CAST(b.value AS DOUBLE), 0.0)
        ) as total_amount,
        try_element_at(FILTER(COALESCE(budgets, ARRAY()), b -> b.currency IS NOT NULL), 1).currency as currency,
        try_element_at(FILTER(COALESCE(orgs, ARRAY()), o -> o.role = '4'), 1) as implementing_org,
        try_element_at(COALESCE(recipient_countries, ARRAY()), 1).code as recipient_country,
        COALESCE(TRY_TO_DATE(actual_start, 'yyyy-MM-dd'),
                 TRY_TO_DATE(planned_start, 'yyyy-MM-dd')) as start_date,
        COALESCE(TRY_TO_DATE(actual_end, 'yyyy-MM-dd'),
                 TRY_TO_DATE(planned_end, 'yyyy-MM-dd')) as end_date,
        activity_status_code,
        source_xml_url
    FROM parsed
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(a.iati_identifier)))) % 9000000000 as id,
    a.display_name,
    a.description,
    f.funder_id,
    a.iati_identifier as funder_award_id,
    CASE WHEN a.total_amount > 0 THEN a.total_amount ELSE NULL END as amount,
    a.currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'grant' as funding_type,
    CAST(NULL AS STRING) as funder_scheme,
    'idrc_iati' as provenance,
    a.start_date,
    a.end_date,
    YEAR(a.start_date) as start_year,
    YEAR(a.end_date) as end_year,
    struct(
        CAST(NULL AS STRING) as given_name,
        CAST(NULL AS STRING) as family_name,
        CAST(NULL AS STRING) as orcid,
        CAST(NULL AS DATE) as role_start,
        struct(
            COALESCE(a.implementing_org.name_en, a.implementing_org.name_fr, a.implementing_org.name_es) as name,
            a.recipient_country as country,
            CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
        ) as affiliation
    ) as lead_investigator,
    CAST(NULL AS STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >) as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >>) as investigators,
    a.source_xml_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(a.iati_identifier)))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM aggregated a
CROSS JOIN idrc_funder f
WHERE a.iati_identifier IS NOT NULL
  AND TRIM(a.iati_identifier) != ''
)
