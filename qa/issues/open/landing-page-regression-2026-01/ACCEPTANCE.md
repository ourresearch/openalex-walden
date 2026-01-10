# Acceptance Criteria

## Definition of Done

This issue is resolved when ALL of the following tests pass.

---

### Test 1: Elsevier Author Count Increased

**Purpose**: Verify that Elsevier records from the affected period now have authors.

**Query**:
```sql
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN size(parser_response.authors) > 0 THEN 1 ELSE 0 END) as with_authors,
    ROUND(100.0 * SUM(CASE WHEN size(parser_response.authors) > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct
FROM openalex.landing_page.taxicab_enriched_new
WHERE processed_date BETWEEN '2025-12-27' AND '2026-01-03'
  AND url NOT LIKE '%/pdf%'
  AND native_id LIKE '10.1016/%';
```

**Expected Result**: `pct >= 70` (at least 70% of records have authors)

**Actual Result**: PENDING

**Status**: PENDING

---

### Test 2: Specific DOI Verification

**Purpose**: Verify a known affected DOI now has affiliations in the API.

**Command**:
```bash
curl -s "https://api.openalex.org/works/https://doi.org/10.1016/j.enbuild.2024.114587" \
  | jq '.authorships | length'
```

**Expected Result**: Value > 0 (at least one authorship returned)

**Actual Result**: PENDING

**Status**: PENDING

---

### Test 3: No Regression in New Records

**Purpose**: Ensure the fix didn't break records processed after Jan 4.

**Query**:
```sql
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN size(parser_response.authors) > 0 THEN 1 ELSE 0 END) as with_authors,
    ROUND(100.0 * SUM(CASE WHEN size(parser_response.authors) > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct
FROM openalex.landing_page.taxicab_enriched_new
WHERE processed_date >= '2026-01-04'
  AND url NOT LIKE '%/pdf%'
  AND native_id LIKE '10.1016/%';
```

**Expected Result**: `pct >= 70` (success rate maintained or improved)

**Actual Result**: PENDING

**Status**: PENDING

---

### Test 4: NEES Sample Improvement

**Purpose**: Verify improvement on the known-problem NEES dataset.

**Command**:
```bash
# Run verification script on NEES Elsevier DOIs
python qa/exploration/scripts/verify_nees_elsevier.py --sample 50
```

**Expected Result**: At least 50% of sampled DOIs return affiliations from API

**Actual Result**: PENDING

**Status**: PENDING

---

## Verification Log

| Date | Tester | Tests Run | Results | Notes |
|------|--------|-----------|---------|-------|
| | | | | |

---

## Notes

- Tests 1 and 3 require Databricks SQL access
- Tests 2 and 4 can be run from command line
- Wait for DLT pipeline refresh (typically ~1 hour) after fix notebook completes before running API tests
