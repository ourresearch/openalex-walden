-- Databricks SQL: Parse OAI-PMH XML using from_xml function
-- This SQL extracts all Dublin Core metadata fields from repository XML records

-- Define the XML schema for from_xml parsing
-- Based on OAI-PMH + Dublin Core structure with multi-valued field support

WITH parsed_xml AS (
  SELECT 
    *,
    -- Clean XML by removing namespaces for easier parsing
    regexp_replace(
      regexp_replace(
        regexp_replace(api_raw, '<[^>]*xmlns[^>]*>', '<'),
        '(oai_dc:|dc:|ns0:|ns1:)', ''
      ),
      '<(/?)([^:>]+:)([^>]+)>', '<$1$3>'
    ) AS cleaned_xml
  FROM dev.repo_api_responses_raw
),

extracted_fields AS (
  SELECT
    *,
    -- Parse XML into structured format
    from_xml(
      cleaned_xml,
      'struct<
        header:struct<
          identifier:string,
          datestamp:string,
          setSpec:array<string>
        >,
        metadata:struct<
          dc:struct<
            title:string,
            creator:array<string>,
            subject:array<string>,
            description:array<string>,
            publisher:array<string>,
            contributor:array<string>,
            date:array<string>,
            type:array<string>,
            format:array<string>,
            identifier:array<string>,
            source:array<string>,
            language:string,
            relation:array<string>,
            coverage:array<string>,
            rights:array<string>
          >
        >
      >',
      map('mode', 'PERMISSIVE')
    ) AS parsed
  FROM parsed_xml
)

-- Extract all fields into final table structure
SELECT
  -- Header fields
  parsed.header.identifier AS native_id,
  parsed.header.datestamp AS header_datestamp,
  parsed.header.setSpec AS set_specs,
  
  -- Basic metadata
  SUBSTRING(parsed.metadata.dc.title, 0, 1000) AS title,
  parsed.metadata.dc.creator AS creators,
  parsed.metadata.dc.contributor AS contributors,
  
  -- Type handling - extract first type and map to OpenAlex taxonomy
  element_at(parsed.metadata.dc.type, 1) AS raw_native_type,
  
  -- Publication info
  element_at(parsed.metadata.dc.publisher, 1) AS publisher,
  element_at(parsed.metadata.dc.source, 1) AS source_name,
  
  -- Dates - try multiple formats
  COALESCE(
    to_date(to_timestamp(element_at(parsed.metadata.dc.date, 1), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')),
    to_date(to_timestamp(element_at(parsed.metadata.dc.date, 1), 'yyyy-MM-dd\'T\'HH:mm:ss')),
    to_date(element_at(parsed.metadata.dc.date, 1), 'yyyy-MM-dd'),
    to_date(element_at(parsed.metadata.dc.date, 1), 'yyyy-MM'),
    to_date(regexp_replace(element_at(parsed.metadata.dc.date, 1), '\\.', '-'), 'yyyy-MM-dd'),
    to_date(
      CASE 
        WHEN length(trim(element_at(parsed.metadata.dc.date, 1))) = 4 
        THEN concat(element_at(parsed.metadata.dc.date, 1), '-01-01')
        ELSE NULL
      END,
      'yyyy-MM-dd'
    )
  ) AS published_date,
  
  -- Abstract handling
  element_at(parsed.metadata.dc.description, 1) AS abstract_raw,
  CASE
    WHEN length(element_at(parsed.metadata.dc.description, 1)) >= 100
    THEN SUBSTRING(element_at(parsed.metadata.dc.description, 1), 0, 4000)
    ELSE NULL
  END AS abstract,
  
  -- Language
  parsed.metadata.dc.language AS language,
  
  -- Subjects/keywords
  parsed.metadata.dc.subject AS subjects,
  
  -- Identifiers (URLs, DOIs, URNs, etc.)
  parsed.metadata.dc.identifier AS identifiers,
  
  -- Extract URLs from identifiers
  filter(parsed.metadata.dc.identifier, x -> x RLIKE '^http') AS urls,
  
  -- Extract DOIs from identifiers
  COALESCE(
    -- From doi.org URLs
    element_at(
      transform(
        filter(parsed.metadata.dc.identifier, x -> lower(x) LIKE '%doi.org%'),
        x -> regexp_extract(x, 'doi\\.org/(.+)$', 1)
      ),
      1
    ),
    -- From doi: prefix
    element_at(
      transform(
        filter(parsed.metadata.dc.identifier, x -> lower(x) LIKE 'doi:%'),
        x -> regexp_replace(x, '(?i)^doi:', '')
      ),
      1
    )
  ) AS doi,
  
  -- Rights/License info
  parsed.metadata.dc.rights AS rights,
  element_at(parsed.metadata.dc.rights, 1) AS raw_license,
  
  -- Check for open access indicators
  CASE
    WHEN array_contains(parsed.metadata.dc.rights, 'info:eu-repo/semantics/openAccess') THEN true
    WHEN exists(parsed.metadata.dc.rights, x -> lower(x) LIKE '%creativecommons%') THEN true
    WHEN exists(parsed.metadata.dc.rights, x -> lower(x) LIKE '%open access%') THEN true
    ELSE false
  END AS is_open_access,
  
  -- Relations
  parsed.metadata.dc.relation AS relations,
  
  -- Coverage (geographic/temporal)
  parsed.metadata.dc.coverage AS coverage,
  
  -- Format/MIME types
  parsed.metadata.dc.format AS formats,
  
  -- Original cleaned XML for debugging
  cleaned_xml
  
FROM extracted_fields
WHERE parsed IS NOT NULL
  -- Filter out deleted types
  AND NOT lower(element_at(parsed.metadata.dc.type, 1)) IN (
    'person', 'image', 'newspaper', 'info:eu-repo/semantics/lecture', 
    'photograph', 'bildband', 'dvd-video', 'video', 'fotografia', 
    'cd', 'sound recording', 'text and image', 'moving image', 
    'photographs', 'cd-rom', 'blu-ray-disc', 'stillimage', 
    'image; text', 'image;stillimage', 'still image', 'image;', 
    'ilustraciones y fotos', 'fotografie', 'fotografía'
  );

-- ============================================================
-- ALTERNATIVE: Simpler version with fewer transformations
-- ============================================================

/*
SELECT
  native_id,
  title,
  raw_native_type,
  publisher,
  published_date,
  abstract,
  language,
  identifiers,
  urls,
  doi,
  is_open_access
FROM (
  SELECT
    from_xml(
      cleaned_xml,
      'struct<header:struct<identifier:string>, metadata:struct<dc:struct<title:string, type:array<string>, identifier:array<string>>>>'
    ) AS parsed
  FROM cleaned_records
)
WHERE parsed.header.identifier IS NOT NULL;
*/

