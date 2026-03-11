# Acceptance Criteria: PubMed Affiliations Fix

## Test DOIs

These DOIs have affiliations in PubMed but not in Crossref. After the fix, they should have affiliations in the final `openalex_works` table.

| DOI | PMID | Expected Affiliations |
|-----|------|----------------------|
| 10.7759/cureus.97342 | 41426802 | 7 (all authors) |
| 10.1016/j.bioadv.2025.214692 | 41499861 | >0 |
| 10.1016/j.jtct.2025.12.991 | 41475518 | >0 |
| 10.1016/j.spsy.2025.11.011 | 41482435 | >0 |
| 10.1016/j.jhepr.2025.101629 | 41480237 | >0 |

## Acceptance Tests

### Test 1: Sample DOIs Have Affiliations

**Query:**
```sql
SELECT
    doi,
    size(flatten(transform(authorships, x -> x.raw_affiliation_strings))) as affiliation_count
FROM openalex.works.openalex_works
WHERE doi IN (
    'https://doi.org/10.7759/cureus.97342',
    'https://doi.org/10.1016/j.bioadv.2025.214692',
    'https://doi.org/10.1016/j.jtct.2025.12.991'
);
```

**Expected:** `affiliation_count > 0` for all rows

### Test 2: Overall Coverage Improvement

**Query:**
```sql
WITH pubmed_with_affil AS (
    SELECT get(filter(ids, x -> x.namespace = "doi").id, 0) as doi
    FROM openalex.works.locations_parsed
    WHERE provenance = "pubmed"
      AND affiliations_exist = true
      AND size(filter(ids, x -> x.namespace = "doi")) > 0
),
final_no_affil AS (
    SELECT REGEXP_REPLACE(doi, 'https://doi.org/', '') as doi
    FROM openalex.works.openalex_works
    WHERE size(flatten(transform(authorships, x -> x.raw_affiliation_strings))) = 0
)
SELECT COUNT(*) as works_missing_pubmed_affiliations
FROM pubmed_with_affil p
JOIN final_no_affil f ON p.doi = f.doi;
```

**Expected:** Count significantly lower than pre-fix baseline (~930K for recent works)

### Test 3: PubMed Affiliations Appear in crossref_super_authorships

**Query:**
```sql
SELECT COUNT(*) as pubmed_affiliation_records
FROM openalex.works.crossref_super_authorships csa
JOIN openalex.works.locations_parsed lp
  ON csa.doi = lp.native_id
  AND lp.provenance = 'pubmed'
WHERE size(flatten(transform(csa.authorships, x -> x.affiliations))) > 0;
```

**Expected:** Count > 0, indicating PubMed affiliations are being used

## Pre-fix Baseline (2026-01-12)

- Works with PubMed affiliations but zero in final table (recent): ~930,000
- Sample DOI `10.7759/cureus.97342`: 0 affiliations in final table
