-- Databricks notebook source
CREATE OR REPLACE TABLE openalex.works.locations_mapped (
  work_id BIGINT,
  merge_key STRUCT<doi: STRING, pmid: STRING, arxiv: STRING, title_author: STRING>,
  key_lineage STRING,
  provenance STRING,
  native_id STRING,
  true_native_id STRING,
  native_id_namespace STRING,
  title STRING,
  normalized_title STRING,
  authors ARRAY<STRUCT<given: STRING, family: STRING, name: STRING, orcid: STRING, affiliations: ARRAY<STRUCT<name: STRING, department: STRING, ror_id: STRING>>, is_corresponding: BOOLEAN, author_key: STRING>>,
  ids ARRAY<STRUCT<id: STRING, namespace: STRING, relationship: STRING>>,
  type STRING,
  version STRING,
  license STRING,
  language STRING,
  published_date DATE,
  created_date DATE,
  updated_date DATE,
  issue STRING,
  volume STRING,
  first_page STRING,
  last_page STRING,
  is_retracted BOOLEAN,
  abstract STRING,
  source_name STRING,
  publisher STRING,
  funders ARRAY<STRUCT<doi: STRING, ror: STRING, name: STRING, awards: ARRAY<STRING>>>,
  references ARRAY<STRUCT<doi: STRING, pmid: STRING, arxiv: STRING, title: STRING, authors: STRING, year: STRING, raw: STRING>>,
  urls ARRAY<STRUCT<url: STRING, content_type: STRING>>,
  pdf_url STRING,
  landing_page_url STRING,
  pdf_s3_id STRING,
  grobid_s3_id STRING,
  mesh STRING,
  is_oa BOOLEAN,
  is_oa_source BOOLEAN,
  abstract_inverted_index STRING,
  authors_exist BOOLEAN,
  affiliations_exist BOOLEAN,
  is_corresponding_exists BOOLEAN,
  best_doi STRING,
  source_id BIGINT,
  openalex_created_dt DATE,
  openalex_updated_dt TIMESTAMP)
USING delta
CLUSTER BY (merge_key.doi, merge_key.pmid, merge_key.arxiv, merge_key.title_author)
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.feature.v2Checkpoint' = 'supported');

-- COMMAND ----------

-- Merge DOI's into locations_mapped 
WITH counted_works AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY merge_key, native_id, native_id_namespace, provenance ORDER BY updated_date DESC) AS rwcnt
    FROM openalex.works.sources_combined
),
distinct_works AS (
    SELECT *
    FROM counted_works
    WHERE rwcnt = 1
)
MERGE INTO openalex.works.locations_mapped AS target
USING distinct_works AS source
ON target.merge_key IS NOT DISTINCT FROM source.merge_key
    AND target.native_id IS NOT DISTINCT FROM source.native_id
    AND target.native_id_namespace IS NOT DISTINCT FROM source.native_id_namespace
    AND target.provenance IS NOT DISTINCT FROM source.provenance
    AND (
      target.merge_key.doi IS NOT NULL OR
      target.merge_key.pmid IS NOT NULL OR 
      target.merge_key.arxiv IS NOT NULL OR
      target.merge_key.title_author IS NOT NULL OR
      target.merge_key.title_author <> ''
    )
WHEN MATCHED 
AND (
    (target.provenance <> source.provenance                       AND target.provenance IS NOT NULL) OR
    (target.native_id <> source.native_id                         AND target.native_id IS NOT NULL) OR
    (target.true_native_id <> source.true_native_id               AND target.true_native_id IS NOT NULL) OR
    (target.native_id_namespace <> source.native_id_namespace     AND target.native_id_namespace IS NOT NULL) OR
    (target.title <> source.title                                 AND target.title IS NOT NULL) OR
    (target.normalized_title <> source.normalized_title           AND target.normalized_title IS NOT NULL) OR
    
    -- sort arrays before comparing
    -- ids
    -- (array_sort(transform(target.ids, i -> struct(
    --     coalesce(i.id, '') as id,
    --     coalesce(i.namespace, '') as namespace,
    --     coalesce(i.relationship, '') as relationship
    -- ))) IS DISTINCT FROM array_sort(transform(source.ids, i -> struct(
    --     coalesce(i.id, '') as id,
    --     coalesce(i.namespace, '') as namespace,
    --     coalesce(i.relationship, '') as relationship
    -- ))) AND target.ids IS NOT NULL) OR
    
    -- urls
    -- (array_sort(transform(target.urls, u -> struct(
    --     coalesce(u.url, '') as url,
    --     coalesce(u.content_type, '') as content_type
    -- ))) IS DISTINCT FROM array_sort(transform(source.urls, u -> struct(
    --     coalesce(u.url, '') as url,
    --     coalesce(u.content_type, '') as content_type
    -- ))) AND target.urls IS NOT NULL) OR

    -- -- funders
    -- (array_sort(transform(target.funders, f -> struct(
    --     coalesce(f.doi, '') as doi,
    --     coalesce(f.ror, '') as ror,
    --     coalesce(f.name, '') as name
    -- ))) IS DISTINCT FROM array_sort(transform(source.funders, f -> struct(
    --     coalesce(f.doi, '') as doi,
    --     coalesce(f.ror, '') as ror,
    --     coalesce(f.name, '') as name
    -- ))) AND target.funders IS NOT NULL) OR
    
    -- references
    -- (array_sort(transform(target.references, r -> struct(
    --     coalesce(r.doi, '') as doi,
    --     coalesce(r.pmid, '') as pmid,
    --     coalesce(r.arxiv, '') as arxiv,
    --     coalesce(r.title, '') as title,
    --     coalesce(r.authors, '') as authors,
    --     coalesce(r.year, '') as year,
    --     coalesce(r.raw, '') as raw
    -- ))) IS DISTINCT FROM array_sort(transform(source.references, r -> struct(
    --     coalesce(r.doi, '') as doi,
    --     coalesce(r.pmid, '') as pmid,
    --     coalesce(r.arxiv, '') as arxiv,
    --     coalesce(r.title, '') as title,
    --     coalesce(r.authors, '') as authors,
    --     coalesce(r.year, '') as year,
    --     coalesce(r.raw, '') as raw
    -- ))) AND target.references IS NOT NULL) OR
    
    -- authors
    -- (array_sort(transform(target.authors, a -> struct(
    --     coalesce(a.given, '') as given,
    --     coalesce(a.family, '') as family,
    --     coalesce(a.name, '') as name,
    --     coalesce(a.orcid, '') as orcid,
    --     coalesce(a.is_corresponding, false) as is_corresponding,
    --     coalesce(a.author_key, '') as author_key
    -- ))) IS DISTINCT FROM array_sort(transform(source.authors, a -> struct(
    --     coalesce(a.given, '') as given,
    --     coalesce(a.family, '') as family,
    --     coalesce(a.name, '') as name,
    --     coalesce(a.orcid, '') as orcid,
    --     coalesce(a.is_corresponding, false) as is_corresponding,
    --     coalesce(a.author_key, '') as author_key
    -- ))) AND target.authors IS NOT NULL) OR
    
    (target.type <> source.type                                   AND target.type IS NOT NULL) OR
    (target.version <> source.version                             AND target.version IS NOT NULL) OR
    (target.license <> source.license                             AND target.license IS NOT NULL) OR
    (target.language <> source.language                           AND target.language IS NOT NULL) OR
    (target.published_date <> source.published_date               AND target.published_date IS NOT NULL) OR
    (target.created_date <> source.created_date                   AND target.created_date IS NOT NULL) OR
    -- (target.updated_date <> source.updated_date                   AND target.updated_date IS NOT NULL) OR
    (target.issue <> source.issue                                 AND target.issue IS NOT NULL) OR
    (target.volume <> source.volume                               AND target.volume IS NOT NULL) OR
    (target.first_page <> source.first_page                       AND target.first_page IS NOT NULL) OR
    (target.last_page <> source.last_page                         AND target.last_page IS NOT NULL) OR
    (target.is_retracted <> source.is_retracted                   AND target.is_retracted IS NOT NULL) OR
    (target.abstract <> source.abstract                           AND target.abstract IS NOT NULL) OR
    (target.source_name <> source.source_name                     AND target.source_name IS NOT NULL) OR
    (target.publisher <> source.publisher                         AND target.publisher IS NOT NULL) OR
    (target.pdf_url <> source.pdf_url                             AND target.pdf_url IS NOT NULL) OR
    (target.landing_page_url <> source.landing_page_url           AND target.landing_page_url IS NOT NULL) OR
    (target.pdf_s3_id <> source.pdf_s3_id                         AND target.pdf_s3_id IS NOT NULL) OR
    (target.grobid_s3_id <> source.grobid_s3_id                   AND target.grobid_s3_id IS NOT NULL) OR
    (target.mesh <> source.mesh                                   AND target.mesh IS NOT NULL) OR
    (target.is_oa <> source.is_oa                                 AND target.is_oa IS NOT NULL) OR
    (target.is_oa_source <> source.is_oa_source                   AND target.is_oa_source IS NOT NULL) OR
    -- (target.abstract_inverted_index <> source.abstract_inverted_index  AND target.abstract_inverted_index IS NOT NULL) OR
    (target.authors_exist <> source.authors_exist                 AND target.authors_exist IS NOT NULL) OR
    (target.affiliations_exist <> source.affiliations_exist       AND target.affiliations_exist IS NOT NULL) OR
    (target.is_corresponding_exists <> source.is_corresponding_exists AND target.is_corresponding_exists IS NOT NULL) OR
    (target.best_doi <> source.best_doi                           AND target.best_doi IS NOT NULL) OR
    (target.source_id <> source.source_id                         AND target.source_id IS NOT NULL)
)  
THEN UPDATE SET 
    target.provenance = source.provenance,
    target.native_id = source.native_id,
    target.true_native_id = source.true_native_id,
    target.native_id_namespace = source.native_id_namespace,
    target.title = source.title,
    target.normalized_title = source.normalized_title,
    target.authors = array_union(coalesce(source.authors,array()),coalesce(target.authors,array())),
    target.ids = array_union(coalesce(source.ids,array()),coalesce(target.ids,array())),
    target.type = source.type,
    target.version = source.version,
    target.license = source.license,
    target.language = source.language,
    target.published_date = source.published_date,
    target.created_date = source.created_date,
    target.updated_date = source.updated_date,
    target.issue = source.issue,
    target.volume = source.volume,
    target.first_page = source.first_page,
    target.last_page = source.last_page,
    target.is_retracted = source.is_retracted,
    target.abstract = source.abstract,
    target.source_name = source.source_name,
    target.publisher = source.publisher,
    target.funders = array_union(coalesce(source.funders,array()),coalesce(target.funders,array())),
    target.references = source.references,
    target.urls = array_union(coalesce(source.urls,array()),coalesce(target.urls,array())),
    target.pdf_url = source.pdf_url,
    target.landing_page_url = source.landing_page_url,
    target.pdf_s3_id = source.pdf_s3_id,
    target.grobid_s3_id = source.grobid_s3_id,
    target.mesh = source.mesh,
    target.is_oa = source.is_oa,
    target.is_oa_source = source.is_oa_source,
    target.abstract_inverted_index = source.abstract_inverted_index,
    target.authors_exist = source.authors_exist,
    target.affiliations_exist = source.affiliations_exist,
    target.is_corresponding_exists = source.is_corresponding_exists,
    target.best_doi = source.best_doi,
    target.source_id = source.source_id,
    target.openalex_updated_dt = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    work_id,
    merge_key,
    provenance,
    native_id,
    true_native_id,
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
    abstract_inverted_index,
    authors_exist,
    affiliations_exist,
    is_corresponding_exists,
    best_doi,
    source_id,
    openalex_created_dt,
    openalex_updated_dt
) VALUES (
    null,
    source.merge_key,
    source.provenance,
    source.native_id,
    source.true_native_id,
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
    source.abstract_inverted_index,
    source.authors_exist,
    source.affiliations_exist,
    source.is_corresponding_exists,
    source.best_doi,
    source.source_id,
    current_date(),
    current_timestamp()
);

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.works.id_map (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 6600000001 INCREMENT BY 1),
  paper_id STRING,
  merge_key STRUCT<doi: STRING, pmid: STRING, arxiv: STRING, title_author: STRING>,
  doi STRING,
  pmid STRING,
  arxiv STRING,
  title_author STRING,
  openalex_created_dt DATE,
  openalex_updated_dt TIMESTAMP
)
CLUSTER BY (merge_key.doi, merge_key.pmid, merge_key.arxiv, merge_key.title_author)
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.feature.v2Checkpoint' = 'supported');

-- COMMAND ----------

-- Merge in non-mapped values
WITH unique_unmapped AS (
  SELECT DISTINCT
    merge_key,
    merge_key.doi as doi,
    merge_key.pmid as pmid,
    merge_key.arxiv as arxiv,
    merge_key.title_author as title_author,
    MIN(openalex_created_dt) as openalex_created_dt,
    MAX(openalex_updated_dt) as openalex_updated_dt,
    row_number() over(partition by merge_key order by max(openalex_updated_dt) DESC) as rwcnt
  FROM openalex.works.locations_mapped
  WHERE work_id IS NULL
    AND openalex_updated_dt > coalesce((select max(openalex_updated_dt) FROM openalex.works.id_map), '1970-01-01')
  GROUP BY
    merge_key,
    merge_key.doi,
    merge_key.pmid,
    merge_key.arxiv,
    merge_key.title_author
),
non_mapped_works AS (
  select *
  from unique_unmapped
  where rwcnt = 1
)
MERGE into openalex.works.id_map AS target
USING non_mapped_works AS source 
  ON target.merge_key.doi = source.merge_key.doi
  OR target.merge_key.pmid = source.merge_key.pmid
  OR target.merge_key.arxiv = source.merge_key.arxiv
  OR target.merge_key.title_author = source.merge_key.title_author
WHEN MATCHED
AND (
  target.merge_key.doi IS NOT NULL OR
  target.merge_key.pmid IS NOT NULL OR 
  target.merge_key.arxiv IS NOT NULL OR
  target.merge_key.title_author IS NOT NULL OR
  target.merge_key.title_author <> ''
)
THEN UPDATE SET
  target.merge_key.doi = COALESCE(source.merge_key.doi, target.merge_key.doi),
  target.merge_key.pmid = COALESCE(source.merge_key.pmid, target.merge_key.pmid),
  target.merge_key.arxiv = COALESCE(source.merge_key.arxiv, target.merge_key.arxiv),
  target.merge_key.title_author = COALESCE(source.merge_key.title_author, target.merge_key.title_author),
  target.doi = COALESCE(source.doi, target.doi),
  target.pmid = COALESCE(source.pmid, target.pmid),
  target.arxiv = COALESCE(source.arxiv, target.arxiv),
  target.title_author = COALESCE(source.title_author, target.title_author),
  target.openalex_created_dt = source.openalex_created_dt,
  target.openalex_updated_dt = source.openalex_updated_dt
WHEN NOT MATCHED THEN INSERT (
  merge_key, 
  doi, 
  pmid, 
  arxiv, 
  title_author, 
  openalex_created_dt, 
  openalex_updated_dt
) 
VALUES (
  source.merge_key,
  source.merge_key.doi,
  source.merge_key.pmid,
  source.merge_key.arxiv,
  source.merge_key.title_author,
  source.openalex_created_dt,
  source.openalex_updated_dt
);

-- COMMAND ----------

-- Merge in legacy paper_id's for DOI
with legacy_doi as (
  SELECT DISTINCT regexp_replace(doi_lower, '[^a-zA-Z0-9.]', '') as doi_lower 
  FROM openalex.works_poc.postgres_mid
  WHERE doi_lower IS NOT NULL
)
, paper_ids as (
  select 
    min(m.paper_id) as paper_id,
    max(m.created_date) as created_dt,
    max(m.updated_date) as updated_dt,
    l.doi_lower
  from legacy_doi l
  inner join openalex.works_poc.postgres_mid m 
    on regexp_replace(
        l.doi_lower,
        '[^a-zA-Z0-9.]',
        ''
      ) = regexp_replace(
        m.doi_lower,
        '[^a-zA-Z0-9.]',
        ''
      ) 
  group by l.doi_lower
)
MERGE INTO openalex.works.id_map AS target
USING paper_ids AS source
  ON LOWER(regexp_replace(target.merge_key.doi, '[^a-zA-Z0-9.]', '')) = LOWER(source.doi_lower)
  AND target.paper_id IS NULL
WHEN MATCHED THEN 
UPDATE SET 
  target.paper_id = source.paper_id,
  target.openalex_created_dt = source.created_dt,
  target.openalex_updated_dt = source.updated_dt
;

-- COMMAND ----------

-- Merge in legacy paper_id's for PMID
with legacy_pmid AS (
  select
    w.paper_id as paper_id,
    x.attribute_value as pmid
  from openalex.works_poc.postgres_mid w
  inner join openalex.works_poc.postgres_work_extra_ids x
    on w.paper_id = x.paper_id
  where 
  -- (w.doi is null or w.doi = '') 
  --   and
     x.attribute_type = 2
)
, paper_ids as (
  select 
    min(m.paper_id) as paper_id,
    max(m.created_date) as created_dt,
    max(m.updated_date) as updated_dt, 
    l.pmid
  from legacy_pmid l
  inner join openalex.works_poc.postgres_mid m 
    on l.paper_id = m.paper_id 
  group by l.pmid
)
MERGE INTO openalex.works.id_map AS target
USING paper_ids AS source
  ON LOWER(TRIM(target.merge_key.pmid)) = LOWER(TRIM(source.pmid))
  AND target.paper_id IS NULL
WHEN MATCHED THEN 
UPDATE SET 
  target.paper_id = source.paper_id,
  target.openalex_created_dt = source.created_dt,
  target.openalex_updated_dt = source.updated_dt
;

-- COMMAND ----------

-- Merge in legacy paper_id's for ARXIV
with legacy_arxiv AS (
  select
    w.paper_id as paper_id,
    w.arxiv_id as arxiv_id
  from openalex.works_poc.postgres_mid w
  inner join openalex.works_poc.postgres_work_extra_ids x
    on w.paper_id = x.paper_id 
  where w.arxiv_id is not null 
    -- and (w.doi_lower is null or w.doi_lower = '')
)
, paper_ids as (
  select 
    min(m.paper_id) as paper_id,
    max(m.created_date) as created_dt,
    max(m.updated_date) as updated_dt,
    l.arxiv_id
  from legacy_arxiv l
  inner join openalex.works_poc.postgres_mid m 
    on l.paper_id = m.paper_id 
  group by l.arxiv_id
)
MERGE INTO openalex.works.id_map AS target
USING paper_ids AS source
  ON LOWER(TRIM(target.merge_key.arxiv)) = LOWER(TRIM(source.arxiv_id))
  AND target.paper_id IS NULL
WHEN MATCHED THEN 
UPDATE SET 
  target.paper_id = source.paper_id,
  target.openalex_created_dt = source.created_dt,
  target.openalex_updated_dt = source.updated_dt
;

-- COMMAND ----------

-- Merge in legacy paper_id's for TITLES
with legacy_titles AS (
  select
    w.paper_id as paper_id,
    w.unpaywall_normalize_title as unpaywall_normalize_title
  from openalex.works_poc.postgres_mid w
  inner join openalex.works_poc.postgres_work_extra_ids x
    on w.paper_id = x.paper_id 
  where w.arxiv_id is null 
    and w.doi_lower is null 
    and w.unpaywall_normalize_title is not null
)
, paper_ids as (
  select 
    min(m.paper_id) as paper_id,
    max(m.created_date) as created_dt,
    max(m.updated_date) as updated_dt,
    l.unpaywall_normalize_title
  from legacy_titles l
  inner join openalex.works_poc.postgres_mid m 
    on l.paper_id = m.paper_id 
  group by l.unpaywall_normalize_title
)
MERGE INTO openalex.works.id_map AS target
USING paper_ids AS source
  ON LOWER(TRIM(target.merge_key.title_author)) = LOWER(TRIM(source.unpaywall_normalize_title))
  AND target.paper_id IS NULL
WHEN MATCHED THEN 
UPDATE SET 
  target.paper_id = source.paper_id,
  target.openalex_created_dt = source.created_dt,
  target.openalex_updated_dt = source.updated_dt
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Load Values that have a `paper_id`

-- COMMAND ----------

-- Merge on DOI matches
WITH ids AS (
  SELECT DISTINCT
    merge_key.doi as doi,
    MIN(paper_id) AS paper_id,
    MAX(openalex_created_dt) AS openalex_created_dt,
    MAX(openalex_updated_dt) AS openalex_updated_dt
  FROM openalex.works.id_map
  WHERE paper_id IS NOT NULL
    AND merge_key.doi IS NOT NULL
  GROUP BY 
    merge_key.doi
)
MERGE INTO openalex.works.locations_mapped AS target
USING ids AS source 
  ON target.merge_key.doi = source.doi
  AND target.work_id IS NULL
WHEN MATCHED THEN UPDATE SET 
  target.work_id = source.paper_id,
  target.openalex_created_dt = source.openalex_created_dt,
  target.openalex_updated_dt = source.openalex_updated_dt
;

-- COMMAND ----------

-- Merge on PMID matches
WITH ids AS (
  SELECT DISTINCT
    merge_key.pmid as pmid,
    MIN(paper_id) AS paper_id,
    MAX(openalex_created_dt) AS openalex_created_dt,
    MAX(openalex_updated_dt) AS openalex_updated_dt
  FROM openalex.works.id_map
  WHERE paper_id IS NOT NULL
    AND merge_key.pmid IS NOT NULL
  GROUP BY 
    merge_key.pmid
)
MERGE INTO openalex.works.locations_mapped AS target
USING ids AS source 
  ON target.merge_key.pmid = source.pmid
  AND target.work_id IS NULL
WHEN MATCHED THEN UPDATE SET 
  target.work_id = source.paper_id,
  target.openalex_created_dt = source.openalex_created_dt,
  target.openalex_updated_dt = source.openalex_updated_dt
;

-- COMMAND ----------

-- Merge on ARXIV matches
WITH ids AS (
  SELECT DISTINCT
    merge_key.arxiv as arxiv,
    MIN(paper_id) AS paper_id,
    MAX(openalex_created_dt) AS openalex_created_dt,
    MAX(openalex_updated_dt) AS openalex_updated_dt
  FROM openalex.works.id_map
  WHERE paper_id IS NOT NULL
    AND merge_key.arxiv IS NOT NULL
  GROUP BY 
    merge_key.arxiv
)
MERGE INTO openalex.works.locations_mapped AS target
USING ids AS source 
  ON target.merge_key.arxiv = source.arxiv
  AND target.work_id IS NULL
WHEN MATCHED THEN UPDATE SET 
  target.work_id = source.paper_id,
  target.openalex_created_dt = source.openalex_created_dt,
  target.openalex_updated_dt = source.openalex_updated_dt
;

-- COMMAND ----------

-- Merge on TITLE_AUTHOR matches
WITH ids AS (
  SELECT DISTINCT
    merge_key.title_author as title_author,
    MIN(paper_id) AS paper_id,
    MAX(openalex_created_dt) AS openalex_created_dt,
    MAX(openalex_updated_dt) AS openalex_updated_dt
  FROM openalex.works.id_map
  WHERE paper_id IS NOT NULL
    AND (merge_key.doi IS NULL AND merge_key.pmid IS NULL AND merge_key.arxiv IS NULL)
    AND merge_key.title_author IS NOT NULL
  GROUP BY 
    merge_key.title_author
)
MERGE INTO openalex.works.locations_mapped AS target
USING ids AS source 
  ON target.merge_key.title_author = source.title_author
  AND target.work_id IS NULL
WHEN MATCHED THEN UPDATE SET 
  target.work_id = source.paper_id,
  target.openalex_created_dt = source.openalex_created_dt,
  target.openalex_updated_dt = source.openalex_updated_dt
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Load values with newly generated ID's

-- COMMAND ----------

-- Merge on DOI matches
WITH ids AS (
  SELECT DISTINCT
    merge_key.doi as doi,
    MIN(id) AS id,
    MAX(openalex_created_dt) AS openalex_created_dt,
    MAX(openalex_updated_dt) AS openalex_updated_dt
  FROM openalex.works.id_map
  WHERE paper_id IS NULL
    AND merge_key.doi IS NOT NULL
  GROUP BY 
    merge_key.doi
)
MERGE INTO openalex.works.locations_mapped AS target
USING ids AS source 
  ON target.merge_key.doi = source.doi
  AND target.work_id IS NULL
WHEN MATCHED THEN UPDATE SET 
  target.work_id = source.id,
  target.openalex_created_dt = source.openalex_created_dt,
  target.openalex_updated_dt = source.openalex_updated_dt
;

-- COMMAND ----------

-- Merge on PMID matches
WITH ids AS (
  SELECT DISTINCT
    merge_key.pmid as pmid,
    MIN(id) AS id,
    MAX(openalex_created_dt) AS openalex_created_dt,
    MAX(openalex_updated_dt) AS openalex_updated_dt
  FROM openalex.works.id_map
  WHERE paper_id IS NULL
    AND merge_key.pmid IS NOT NULL
  GROUP BY 
    merge_key.pmid
)
MERGE INTO openalex.works.locations_mapped AS target
USING ids AS source 
  ON target.merge_key.pmid = source.pmid
  AND target.work_id IS NULL
WHEN MATCHED THEN UPDATE SET 
  target.work_id = source.id,
  target.openalex_created_dt = source.openalex_created_dt,
  target.openalex_updated_dt = source.openalex_updated_dt
;

-- COMMAND ----------

-- Merge on ARXIV matches
WITH ids AS (
  SELECT DISTINCT
    merge_key.arxiv as arxiv,
    MIN(id) AS id,
    MAX(openalex_created_dt) AS openalex_created_dt,
    MAX(openalex_updated_dt) AS openalex_updated_dt
  FROM openalex.works.id_map
  WHERE paper_id IS NULL
    AND merge_key.arxiv IS NOT NULL
  GROUP BY 
    merge_key.arxiv
)
MERGE INTO openalex.works.locations_mapped AS target
USING ids AS source 
  ON target.merge_key.arxiv = source.arxiv
  AND target.work_id IS NULL
WHEN MATCHED THEN UPDATE SET 
  target.work_id = source.id,
  target.openalex_created_dt = source.openalex_created_dt,
  target.openalex_updated_dt = source.openalex_updated_dt
;

-- COMMAND ----------

-- Merge on TITLE_AUTHOR matches
WITH ids AS (
  SELECT DISTINCT
    merge_key.title_author as title_author,
    MIN(id) AS id,
    MAX(openalex_created_dt) AS openalex_created_dt,
    MAX(openalex_updated_dt) AS openalex_updated_dt
  FROM openalex.works.id_map
  WHERE paper_id IS NULL
    AND (merge_key.doi IS NULL AND merge_key.pmid IS NULL AND merge_key.arxiv IS NULL)
    AND merge_key.title_author IS NOT NULL
  GROUP BY 
    merge_key.title_author
)
MERGE INTO openalex.works.locations_mapped AS target
USING ids AS source 
  ON target.merge_key.title_author = source.title_author
  AND target.work_id IS NULL
WHEN MATCHED THEN UPDATE SET 
  target.work_id = source.id,
  target.openalex_created_dt = source.openalex_created_dt,
  target.openalex_updated_dt = source.openalex_updated_dt
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insert legacy paper_id's based on mag id
-- MAGIC todo: use an UPDATE clause instead of create or replace, still need to test that.

-- COMMAND ----------

create or replace table openalex.works.locations_mapped as (
with mag_walden_works as (
  select
    get(filter(ids, x -> x.namespace = 'mag').id, 0) as legacy_work_id,
    max(get(filter(ids, x -> x.namespace = 'mag').id, 0)) over (
        partition by work_id
      ) as assigned_work_id,
    *
  from
    openalex.works.locations_mapped
  where
    work_id in (
      select
        work_id
      from
        openalex.works.locations_mapped
      where
        provenance = 'mag'
    )
),
updated_mag_walden_works as (
  select
    coalesce(legacy_work_id, assigned_work_id) as work_id,
    * except (legacy_work_id, work_id, assigned_work_id)
  from
    mag_walden_works
),
unioned as (
  select
    *
  from
    updated_mag_walden_works
  union all
  select
    *
  from
    openalex.works.locations_mapped
  where
    work_id not in (
      select
        work_id
      from
        openalex.works.locations_mapped
      where
        provenance = 'mag'
    )
)
select
  *
from
  unioned
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insert legacy paper_id's based on pmh_id
-- MAGIC todo: use an UPDATE clause instead of create or replace, still need to test that.

-- COMMAND ----------

create or replace table openalex.works.locations_mapped as (
with pmh_mapping as (
  -- select the lowest work_id (since we have multiple PMH IDs for the same work and a legacy work_id is always lower than a newly minted work_id, which increments starting at 6600000001).
  select
    lower(pmh_id) as pmh_id,
    min(work_id) as legacy_work_id
  from
    openalex.works_poc.work_id_to_pmh_id_final
  group by
    pmh_id
),
repo_walden_works as (
  select
    *,
    case
      when provenance in ('repo', 'repo_backfill') then lower(true_native_id)
    end as pmh_id_walden
  from
    openalex.works.locations_mapped
  where
    work_id in (
  select
        work_id
      from
        openalex.works.locations_mapped
      where
        provenance in ('repo', 'repo_backfill')
        and work_id > 6600000000
)
),
updated_repo_walden_works as (
  select
     coalesce(max(legacy_work_id) over (partition by work_id), work_id) as work_id, * except (work_id, pmh_id, pmh_id_walden, legacy_work_id)
  from
    repo_walden_works
    left join pmh_mapping on repo_walden_works.pmh_id_walden = pmh_mapping.pmh_id
)
,
unioned as (
  select * from updated_repo_walden_works
  union all
  select * from openalex.works.locations_mapped where work_id not in (
  select
        work_id
      from
        openalex.works.locations_mapped
      where
        provenance in ('repo', 'repo_backfill')
        and work_id > 6600000000
)
)
select
  *
from
  unioned 
)
