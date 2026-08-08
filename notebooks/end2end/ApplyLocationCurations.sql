-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Apply location curations to `locations_mapped`
-- MAGIC Moved out of CreateLocationsMapped (matching only) — oxjob #745.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Curation - new locations

-- COMMAND ----------

WITH curation_locations AS (
  SELECT
    -- Extract work_id and convert from OpenAlex URL to numeric ID
    CASE 
      WHEN get_json_object(property_value, '$.work_id') LIKE 'https://openalex.org/W%'
      THEN CAST(SUBSTRING(get_json_object(property_value, '$.work_id'), 23) AS BIGINT)
      ELSE NULL
    END AS work_id,
    
    NULL as work_id_source,
    TRY_TO_TIMESTAMP(moderated_date) AS moderated_ts,
    
    -- Create merge_key structure with minimal data
    STRUCT(
      NULL as doi,
      NULL as pmid, 
      NULL as arxiv,
      CASE 
        WHEN get_json_object(property_value, '$.title') IS NOT NULL 
        THEN CONCAT(
          get_json_object(property_value, '$.title'), 
          '_', 
          CAST(
            CASE 
              WHEN get_json_object(property_value, '$.work_id') LIKE 'https://openalex.org/W%'
              THEN SUBSTRING(get_json_object(property_value, '$.work_id'), 23)
              ELSE id
            END AS STRING
          )
        )
        ELSE NULL 
      END as title_author
    ) as merge_key,
    
    NULL as key_lineage,
    'curation' as provenance,
    SUBSTRING(entity_id, LOCATE(':', entity_id) + 1) as native_id,
    'openalex_curation' as native_id_namespace,
    get_json_object(property_value, '$.title') as title,
    NULL as normalized_title,
    CAST(NULL AS ARRAY<STRUCT<given: STRING, family: STRING, name: STRING, orcid: STRING, affiliations: ARRAY<STRUCT<name: STRING, department: STRING, ror_id: STRING>>, is_corresponding: BOOLEAN, author_key: STRING>>) as authors,
    CAST(NULL AS ARRAY<STRUCT<id: STRING, namespace: STRING, relationship: STRING>>) as ids,
    get_json_object(property_value, '$.type') as type,
    COALESCE(get_json_object(property_value, '$.version'), 'submittedVersion') as version,
    get_json_object(property_value, '$.license') as license,
    NULL as language,
    NULL as published_date,
    NULL as created_date,
    CAST(moderated_ts AS DATE) as updated_date,
    NULL as issue,
    NULL as volume,
    NULL as first_page,
    NULL as last_page,
    NULL as is_retracted,
    NULL as abstract,
    NULL as source_name,
    NULL as publisher,
    CAST(NULL AS ARRAY<STRUCT<doi: STRING, ror: STRING, name: STRING, awards: ARRAY<STRING>>>) as funders,
    CAST(NULL AS ARRAY<STRUCT<doi: STRING, pmid: STRING, arxiv: STRING, title: STRING, authors: STRING, year: STRING, raw: STRING>>) as references,
    CAST(NULL AS ARRAY<STRUCT<url: STRING, content_type: STRING>>) as urls,
    get_json_object(property_value, '$.pdf_url') as pdf_url,
    get_json_object(property_value, '$.landing_page_url') as landing_page_url,
    NULL as pdf_s3_id,
    NULL as grobid_s3_id,
    NULL as mesh,
    CAST(get_json_object(property_value, '$.is_oa') AS BOOLEAN) as is_oa,
    NULL as is_oa_source,
    NULL as referenced_works_count,
    CAST(NULL AS ARRAY<BIGINT>) as referenced_works,
    NULL as abstract_inverted_index,
    NULL as authors_exist,
    NULL as affiliations_exist,
    NULL as is_corresponding_exists,
    NULL as best_doi,
    
    -- Extract source_id and convert from OpenAlex URL to numeric ID
    CASE 
      WHEN get_json_object(property_value, '$.source_id') LIKE 'https://openalex.org/S%'
      THEN CAST(SUBSTRING(get_json_object(property_value, '$.source_id'), 23) AS BIGINT)
      ELSE NULL
    END AS source_id,
    
    CAST(moderated_ts AS DATE)  as openalex_created_dt,
    moderated_ts as openalex_updated_dt
    
  FROM openalex.curations.approved_curations
  WHERE entity = 'locations'
    AND status = 'approved'
    AND create_new = true
)

MERGE INTO identifier('openalex' || :env_suffix || '.works.locations_mapped') AS target
USING curation_locations AS source
ON target.native_id = source.native_id 
   AND target.native_id_namespace = source.native_id_namespace
   AND target.provenance = source.provenance

WHEN MATCHED AND (
   (source.title            IS NOT NULL AND source.title            IS DISTINCT FROM target.title) OR
   (source.pdf_url          IS NOT NULL AND source.pdf_url          IS DISTINCT FROM target.pdf_url) OR
   (source.landing_page_url IS NOT NULL AND source.landing_page_url IS DISTINCT FROM target.landing_page_url) OR
   (source.license          IS NOT NULL AND source.license          IS DISTINCT FROM target.license) OR
   (source.is_oa            IS NOT NULL AND source.is_oa            IS DISTINCT FROM target.is_oa) OR
   (source.source_id        IS NOT NULL AND source.source_id        IS DISTINCT FROM target.source_id) OR
   (source.type             IS NOT NULL AND source.type             IS DISTINCT FROM target.type)
)
THEN UPDATE SET
  target.title               = COALESCE(source.title,            target.title),
  target.pdf_url             = COALESCE(source.pdf_url,          target.pdf_url),
  target.landing_page_url    = COALESCE(source.landing_page_url, target.landing_page_url),
  target.license             = COALESCE(source.license,          target.license),
  target.is_oa               = COALESCE(source.is_oa,            target.is_oa),
  target.source_id           = COALESCE(source.source_id,        target.source_id),
  target.type                = COALESCE(source.type,             target.type),
  target.updated_date        = CAST(source.openalex_updated_dt AS DATE),
  target.openalex_updated_dt = source.openalex_updated_dt

WHEN NOT MATCHED THEN INSERT (
    work_id,
    work_id_source,
    merge_key,
    key_lineage,
    provenance,
    native_id,
    native_id_namespace,
    title,
    normalized_title,
    authors,
    ids,
    type,
    version,
    license,
    language,
    published_date,
    created_date,
    updated_date,
    issue,
    volume,
    first_page,
    last_page,
    is_retracted,
    abstract,
    source_name,
    publisher,
    funders,
    references,
    urls,
    pdf_url,
    landing_page_url,
    pdf_s3_id,
    grobid_s3_id,
    mesh,
    is_oa,
    is_oa_source,
    referenced_works_count,
    referenced_works,
    abstract_inverted_index,
    authors_exist,
    affiliations_exist,
    is_corresponding_exists,
    best_doi,
    source_id,
    openalex_created_dt,
    openalex_updated_dt
) VALUES (
    source.work_id,
    source.work_id_source,
    source.merge_key,
    source.key_lineage,
    source.provenance,
    source.native_id,
    source.native_id_namespace,
    source.title,
    source.normalized_title,
    source.authors,
    source.ids,
    source.type,
    source.version,
    source.license,
    source.language,
    source.published_date,
    source.created_date,
    source.updated_date,
    source.issue,
    source.volume,
    source.first_page,
    source.last_page,
    source.is_retracted,
    source.abstract,
    source.source_name,
    source.publisher,
    source.funders,
    source.references,
    source.urls,
    source.pdf_url,
    source.landing_page_url,
    source.pdf_s3_id,
    source.grobid_s3_id,
    source.mesh,
    source.is_oa,
    source.is_oa_source,
    source.referenced_works_count,
    source.referenced_works,
    source.abstract_inverted_index,
    source.authors_exist,
    source.affiliations_exist,
    source.is_corresponding_exists,
    source.best_doi,
    source.source_id,
    source.openalex_created_dt,
    source.openalex_updated_dt
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Curation - location overrides

-- COMMAND ----------

-- 1) Latest approved curation per (entity_id, property)
WITH latest_per_field AS (
  SELECT
    SUBSTRING(entity_id, LOCATE(':', entity_id) + 1) AS native_id,
    split(entity_id, ':')[0] AS native_id_namespace,
    property,
    NULLIF(TRIM(property_value), 'null') AS property_value,
    TRY_TO_TIMESTAMP(moderated_date) AS moderated_ts,
    ROW_NUMBER() OVER (
      PARTITION BY entity_id, property
      ORDER BY TRY_TO_TIMESTAMP(moderated_date) DESC NULLS LAST, id DESC
    ) AS rn
  FROM openalex.curations.approved_curations
  WHERE entity = 'locations'
    AND status = 'approved'
    AND create_new = false
),
dedup AS (
  SELECT native_id, native_id_namespace, property, property_value, moderated_ts
  FROM latest_per_field
  WHERE rn = 1
),

-- 2) Pivot with a value and an "apply" flag per field, so we can update values to null
curation_overrides_pivoted AS (
  SELECT
    native_id,
    native_id_namespace,

    -- values
    MAX(CASE WHEN property='title'             THEN property_value END)                                  AS title_val,
    MAX(CASE WHEN property='pdf_url'           THEN property_value END)                                  AS pdf_url_val,
    MAX(CASE WHEN property='landing_page_url'  THEN property_value END)                                  AS landing_page_url_val,
    MAX(CASE WHEN property='license'           THEN LOWER(property_value) END)                           AS license_val,
    MAX(CASE WHEN property='is_oa'             THEN CAST(LOWER(property_value) IN ('true','t','1','yes') AS BOOLEAN) END) AS is_oa_val,

    MAX(CASE WHEN property='type'              THEN LOWER(property_value) END)                           AS type_val,
    MAX(CASE WHEN property='version'           THEN property_value END)                                   AS version_val,
    MAX(CASE WHEN property='language'          THEN LOWER(property_value) END)                            AS language_val,
    MAX(CASE WHEN property='issue'             THEN property_value END)                                   AS issue_val,
    MAX(CASE WHEN property='volume'            THEN property_value END)                                   AS volume_val,
    MAX(CASE WHEN property='first_page'        THEN property_value END)                                   AS first_page_val,
    MAX(CASE WHEN property='last_page'         THEN property_value END)                                   AS last_page_val,
    MAX(CASE WHEN property='is_retracted'      THEN CAST(LOWER(property_value) IN ('true','t','1','yes') AS BOOLEAN) END) AS is_retracted_val,

    -- apply flags (1 if curator set the field, even if to NULL)
    MAX(CASE WHEN property='title'             THEN 1 ELSE 0 END) AS title_apply,
    MAX(CASE WHEN property='pdf_url'           THEN 1 ELSE 0 END) AS pdf_url_apply,
    MAX(CASE WHEN property='landing_page_url'  THEN 1 ELSE 0 END) AS landing_page_url_apply,
    MAX(CASE WHEN property='license'           THEN 1 ELSE 0 END) AS license_apply,
    MAX(CASE WHEN property='is_oa'             THEN 1 ELSE 0 END) AS is_oa_apply,
    MAX(CASE WHEN property='type'              THEN 1 ELSE 0 END) AS type_apply,
    MAX(CASE WHEN property='version'           THEN 1 ELSE 0 END) AS version_apply,
    MAX(CASE WHEN property='language'          THEN 1 ELSE 0 END) AS language_apply,
    MAX(CASE WHEN property='issue'             THEN 1 ELSE 0 END) AS issue_apply,
    MAX(CASE WHEN property='volume'            THEN 1 ELSE 0 END) AS volume_apply,
    MAX(CASE WHEN property='first_page'        THEN 1 ELSE 0 END) AS first_page_apply,
    MAX(CASE WHEN property='last_page'         THEN 1 ELSE 0 END) AS last_page_apply,
    MAX(CASE WHEN property='is_retracted'      THEN 1 ELSE 0 END) AS is_retracted_apply,

    MAX(moderated_ts) AS latest_moderated_dt
  FROM dedup
  GROUP BY native_id, native_id_namespace
)

-- 3) MERGE: use apply flags, allow NULL values, and only update on real changes
MERGE INTO identifier('openalex' || :env_suffix || '.works.locations_mapped') AS target
USING curation_overrides_pivoted AS source
ON  target.native_id = source.native_id
AND target.native_id_namespace = source.native_id_namespace

WHEN MATCHED AND (
  (source.title_apply            = 1 AND source.title_val            IS DISTINCT FROM target.title)            OR
  (source.pdf_url_apply          = 1 AND source.pdf_url_val          IS DISTINCT FROM target.pdf_url)          OR
  (source.pdf_url_apply = 1 AND source.pdf_url_val IS NULL AND
     EXISTS(target.urls, x -> lower(x.content_type) IN ('pdf','application/pdf'))
  ) OR
  (source.landing_page_url_apply = 1 AND source.landing_page_url_val IS DISTINCT FROM target.landing_page_url) OR
  (source.license_apply          = 1 AND source.license_val          IS DISTINCT FROM target.license)          OR
  (source.is_oa_apply            = 1 AND source.is_oa_val            IS DISTINCT FROM target.is_oa)            OR
  (source.type_apply             = 1 AND source.type_val             IS DISTINCT FROM target.`type`)           OR
  (source.version_apply          = 1 AND source.version_val          IS DISTINCT FROM target.version)          OR
  (source.language_apply         = 1 AND source.language_val         IS DISTINCT FROM target.language)         OR
  (source.issue_apply            = 1 AND source.issue_val            IS DISTINCT FROM target.issue)            OR
  (source.volume_apply           = 1 AND source.volume_val           IS DISTINCT FROM target.volume)           OR
  (source.first_page_apply       = 1 AND source.first_page_val       IS DISTINCT FROM target.first_page)       OR
  (source.last_page_apply        = 1 AND source.last_page_val        IS DISTINCT FROM target.last_page)        OR
  (source.is_retracted_apply     = 1 AND source.is_retracted_val     IS DISTINCT FROM target.is_retracted)
)
THEN UPDATE SET
  target.title            = CASE WHEN source.title_apply            = 1 THEN source.title_val            ELSE target.title            END,
  target.pdf_url          = CASE WHEN source.pdf_url_apply          = 1 THEN source.pdf_url_val          ELSE target.pdf_url          END,
  -- keep the urls array in sync with the scalar overrides: replaced URLs are rewritten in place,
  -- nullified URLs are removed (pdf-null also removes by content_type), or they will be picked up in works. CDM
  target.urls = CASE
                  WHEN (source.pdf_url_apply = 1 OR source.landing_page_url_apply = 1) AND target.urls IS NOT NULL THEN
                    FILTER(
                      TRANSFORM(
                        target.urls,
                        x -> STRUCT(
                          CASE
                            WHEN source.pdf_url_apply = 1 AND source.pdf_url_val IS NOT NULL AND target.pdf_url IS NOT NULL
                                 AND REGEXP_REPLACE(LOWER(x.url), '^https?://', '') = REGEXP_REPLACE(LOWER(target.pdf_url), '^https?://', '')
                              THEN source.pdf_url_val
                            WHEN source.landing_page_url_apply = 1 AND source.landing_page_url_val IS NOT NULL AND target.landing_page_url IS NOT NULL
                                 AND REGEXP_REPLACE(LOWER(x.url), '^https?://', '') = REGEXP_REPLACE(LOWER(target.landing_page_url), '^https?://', '')
                              THEN source.landing_page_url_val
                            ELSE x.url
                          END AS url,
                          x.content_type AS content_type
                        )
                      ),
                      x -> NOT (
                        (source.pdf_url_apply = 1 AND source.pdf_url_val IS NULL AND (
                          lower(x.content_type) IN ('pdf','application/pdf') OR
                          (target.pdf_url IS NOT NULL
                           AND REGEXP_REPLACE(LOWER(x.url), '^https?://', '') = REGEXP_REPLACE(LOWER(target.pdf_url), '^https?://', ''))
                        )) OR
                        (source.landing_page_url_apply = 1 AND source.landing_page_url_val IS NULL
                         AND target.landing_page_url IS NOT NULL
                         AND REGEXP_REPLACE(LOWER(x.url), '^https?://', '') = REGEXP_REPLACE(LOWER(target.landing_page_url), '^https?://', ''))
                      )
                    )
                  ELSE target.urls
                END,
  target.landing_page_url = CASE WHEN source.landing_page_url_apply = 1 THEN source.landing_page_url_val ELSE target.landing_page_url END,
  target.license          = CASE WHEN source.license_apply          = 1 THEN source.license_val          ELSE target.license          END,
  target.is_oa            = CASE WHEN source.is_oa_apply            = 1 THEN source.is_oa_val            ELSE target.is_oa            END,
  target.`type`           = CASE WHEN source.type_apply             = 1 THEN source.type_val             ELSE target.`type`           END,
  target.version          = CASE WHEN source.version_apply          = 1 THEN source.version_val          ELSE target.version          END,
  target.language         = CASE WHEN source.language_apply         = 1 THEN source.language_val         ELSE target.language         END,
  target.issue            = CASE WHEN source.issue_apply            = 1 THEN source.issue_val            ELSE target.issue            END,
  target.volume           = CASE WHEN source.volume_apply           = 1 THEN source.volume_val           ELSE target.volume           END,
  target.first_page       = CASE WHEN source.first_page_apply       = 1 THEN source.first_page_val       ELSE target.first_page       END,
  target.last_page        = CASE WHEN source.last_page_apply        = 1 THEN source.last_page_val        ELSE target.last_page        END,
  target.is_retracted     = CASE WHEN source.is_retracted_apply     = 1 THEN source.is_retracted_val     ELSE target.is_retracted     END,

  target.openalex_updated_dt = source.latest_moderated_dt
;

