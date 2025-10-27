-- Diagnostic query for missing record in locations_parsed
-- Check CDF history for the record in pdf_works

SET native_id_to_check = 'https://aip.scitation.org/doi/pdf/10.1063/5.0025526';

-- Check current state in pdf_works
SELECT 'Current state in pdf_works' as check_type, * 
FROM openalex.pdf.pdf_works 
WHERE native_id = ${native_id_to_check};

-- Check CDF history for this record
SELECT 
  'CDF History in pdf_works' as check_type,
  _change_type,
  _commit_version,
  _commit_timestamp,
  title,
  updated_date,
  created_date
FROM table_changes('openalex.pdf.pdf_works', 0)
WHERE native_id = ${native_id_to_check}
ORDER BY _commit_timestamp;

-- Check if it ever appeared in locations_parsed
SELECT 
  'CDF History in locations_parsed' as check_type,
  _change_type,
  _commit_version,
  _commit_timestamp,
  title,
  updated_date,
  provenance
FROM table_changes('openalex.works.locations_parsed', 0)
WHERE native_id = ${native_id_to_check}
ORDER BY _commit_timestamp;

-- Check if there are multiple provenances for this native_id
SELECT 
  'Multiple provenances' as check_type,
  provenance,
  COUNT(*) as count,
  MAX(updated_date) as latest_update
FROM openalex.works.locations_parsed
WHERE native_id = ${native_id_to_check}
GROUP BY provenance;

