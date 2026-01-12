# Acceptance Criteria

## Definition of Done

This issue is resolved when ALL of the following tests pass.

---

## Primary Acceptance Test: Year-over-Year Affiliation Trend

This is the main acceptance test because it directly validates the trend Nees highlighted in his email: 2024 and 2025 should not "stick out" as having lower affiliation coverage than surrounding years.

### Test 1: Raw Affiliation String Coverage by Year (All Works)

**Purpose**: Verify that 2024 and 2025 raw affiliation string percentages are in line with other recent years (not anomalously low).

**Before Query** (run before fix):
```sql
SELECT
    publication_year,
    COUNT(*) as total_works,
    SUM(CASE WHEN exists(authorships, a -> size(a.raw_affiliation_strings) > 0) THEN 1 ELSE 0 END) as with_raw_aff,
    ROUND(100.0 * SUM(CASE WHEN exists(authorships, a -> size(a.raw_affiliation_strings) > 0) THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_raw_aff
FROM openalex.works.openalex_works
WHERE publication_year BETWEEN 2020 AND 2025
  AND type IN ('article', 'review')
GROUP BY publication_year
ORDER BY publication_year;
```

**Expected Before**: 2024 and 2025 show noticeably lower `pct_raw_aff` than 2022-2023

**After Query** (run after fix):
Same query as above.

**Expected After**: 2024 and 2025 `pct_raw_aff` should be comparable to 2022-2023 (within ~5% points)

| Year | Before Fix | After Fix | Status |
|------|------------|-----------|--------|
| 2020 | | | |
| 2021 | | | |
| 2022 | | | |
| 2023 | | | |
| 2024 | | | PENDING |
| 2025 | | | PENDING |

**Status**: PENDING

---

### Test 2: Assigned Institution ID Coverage by Year (All Works)

**Purpose**: Verify that 2024 and 2025 institution assignment percentages are in line with other recent years.

**Before Query** (run before fix):
```sql
SELECT
    publication_year,
    COUNT(*) as total_works,
    SUM(CASE WHEN exists(authorships, a -> size(a.institutions) > 0) THEN 1 ELSE 0 END) as with_inst,
    ROUND(100.0 * SUM(CASE WHEN exists(authorships, a -> size(a.institutions) > 0) THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_inst
FROM openalex.works.openalex_works
WHERE publication_year BETWEEN 2020 AND 2025
  AND type IN ('article', 'review')
GROUP BY publication_year
ORDER BY publication_year;
```

**Expected Before**: 2024 and 2025 show noticeably lower `pct_inst` than 2022-2023

**After Query** (run after fix):
Same query as above.

**Expected After**: 2024 and 2025 `pct_inst` should be comparable to 2022-2023 (within ~5% points)

| Year | Before Fix | After Fix | Status |
|------|------------|-----------|--------|
| 2020 | | | |
| 2021 | | | |
| 2022 | | | |
| 2023 | | | |
| 2024 | | | PENDING |
| 2025 | | | PENDING |

**Status**: PENDING

---

### Test 3: Elsevier-Specific Affiliation Coverage by Year

**Purpose**: Since the fix targets Elsevier first, verify Elsevier works specifically show improvement.

**Before Query** (run before fix):
```sql
SELECT
    publication_year,
    COUNT(*) as total_works,
    SUM(CASE WHEN exists(authorships, a -> size(a.raw_affiliation_strings) > 0) THEN 1 ELSE 0 END) as with_raw_aff,
    ROUND(100.0 * SUM(CASE WHEN exists(authorships, a -> size(a.raw_affiliation_strings) > 0) THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_raw_aff,
    SUM(CASE WHEN exists(authorships, a -> size(a.institutions) > 0) THEN 1 ELSE 0 END) as with_inst,
    ROUND(100.0 * SUM(CASE WHEN exists(authorships, a -> size(a.institutions) > 0) THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_inst
FROM openalex.works.openalex_works
WHERE publication_year BETWEEN 2020 AND 2025
  AND type IN ('article', 'review')
  AND doi LIKE '10.1016/%'
GROUP BY publication_year
ORDER BY publication_year;
```

**Expected Before**: 2024 and 2025 Elsevier works show anomalously low affiliation coverage

**After Query** (run after fix):
Same query as above.

**Expected After**: 2024 and 2025 Elsevier coverage should align with historical trends

| Year | Before pct_raw_aff | After pct_raw_aff | Before pct_inst | After pct_inst | Status |
|------|-------------------|-------------------|-----------------|----------------|--------|
| 2020 | | | | | |
| 2021 | | | | | |
| 2022 | | | | | |
| 2023 | | | | | |
| 2024 | | | | | PENDING |
| 2025 | | | | | PENDING |

**Status**: PENDING

---

## Secondary Tests

### Test 4: Affected Period Parser Response Recovery

**Purpose**: Verify that Elsevier records from the specific regression period (Dec 27 - Jan 3) now have authors in the landing page table.

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

### Test 5: Specific DOI Verification

**Purpose**: Verify a known affected DOI from the NEES dataset now has affiliations in the API.

**Command**:
```bash
curl -s "https://api.openalex.org/works/https://doi.org/10.1016/j.enbuild.2024.114587" \
  | jq '.authorships[0].raw_affiliation_strings'
```

**Expected Result**: Non-empty array of affiliation strings

**Actual Result**: PENDING

**Status**: PENDING

---

### Test 6: No Regression in New Records

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

### Test 7: NEES Sample Improvement

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

- **Primary tests (1-3)** validate the year-over-year trend that Nees reported - this is the main success metric
- **Secondary tests (4-7)** validate specific technical aspects of the fix
- Tests 1-4 and 6 require Databricks SQL access
- Tests 5 and 7 can be run from command line
- Wait for DLT pipeline refresh (typically ~1 hour) after fix notebook completes before running API tests
- Run "Before" queries and record results BEFORE running the fix notebook

## Schema Reference

The `openalex.works.openalex_works` table structure for affiliations:
```
authorships: array<struct<
    raw_affiliation_strings: array<string>,  -- Raw text affiliations
    institutions: array<struct<              -- Resolved institution IDs
        id: string,
        display_name: string,
        ...
    >>,
    ...
>>
```
