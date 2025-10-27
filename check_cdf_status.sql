-- Check CDF status for all upstream tables in locations_parsed pipeline
-- Look for 'delta.enableChangeDataFeed' property in the output

-- Check table properties for CDF (look for 'delta.enableChangeDataFeed' = 'true')
SHOW TBLPROPERTIES openalex.crossref.crossref_works;
SHOW TBLPROPERTIES openalex.datacite.datacite_works;
SHOW TBLPROPERTIES openalex.pdf.pdf_works;
SHOW TBLPROPERTIES openalex.pubmed.pubmed_works;
SHOW TBLPROPERTIES openalex.repo.repo_works;
SHOW TBLPROPERTIES openalex.landing_page.landing_page_works;
SHOW TBLPROPERTIES openalex.mag.mag_dlt_works;

-- Check if any recent changes exist in CDF
SELECT 
  'crossref' as source,
  COUNT(*) as change_count,
  MAX(_commit_version) as latest_version
FROM table_changes('openalex.crossref.crossref_works', 0)
WHERE _commit_version > (SELECT MAX(version) - 10 FROM (DESCRIBE HISTORY openalex.crossref.crossref_works))

UNION ALL

SELECT 
  'datacite',
  COUNT(*),
  MAX(_commit_version)
FROM table_changes('openalex.datacite.datacite_works', 0)
WHERE _commit_version > (SELECT MAX(version) - 10 FROM (DESCRIBE HISTORY openalex.datacite.datacite_works))

UNION ALL

SELECT 
  'pdf',
  COUNT(*),
  MAX(_commit_version)
FROM table_changes('openalex.pdf.pdf_works', 0)
WHERE _commit_version > (SELECT MAX(version) - 10 FROM (DESCRIBE HISTORY openalex.pdf.pdf_works))

UNION ALL

SELECT 
  'pubmed',
  COUNT(*),
  MAX(_commit_version)
FROM table_changes('openalex.pubmed.pubmed_works', 0)
WHERE _commit_version > (SELECT MAX(version) - 10 FROM (DESCRIBE HISTORY openalex.pubmed.pubmed_works))

UNION ALL

SELECT 
  'repo',
  COUNT(*),
  MAX(_commit_version)
FROM table_changes('openalex.repo.repo_works', 0)
WHERE _commit_version > (SELECT MAX(version) - 10 FROM (DESCRIBE HISTORY openalex.repo.repo_works))

UNION ALL

SELECT 
  'landing_page',
  COUNT(*),
  MAX(_commit_version)
FROM table_changes('openalex.landing_page.landing_page_works', 0)
WHERE _commit_version > (SELECT MAX(version) - 10 FROM (DESCRIBE HISTORY openalex.landing_page.landing_page_works))

UNION ALL

SELECT 
  'mag',
  COUNT(*),
  MAX(_commit_version)
FROM table_changes('openalex.mag.mag_dlt_works', 0)
WHERE _commit_version > (SELECT MAX(version) - 10 FROM (DESCRIBE HISTORY openalex.mag.mag_dlt_works));

