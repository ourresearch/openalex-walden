# Plan: Remove Trailing Periods from Raw Affiliation Strings

## Problem Statement

Raw affiliation strings are duplicated in `openalex_works` when the same affiliation appears with and without a trailing period. This affects data quality and inflates affiliation counts.

## Fix Approach

### Step 1: Modify CreateWorksBase.ipynb (Prevent Future Issues) ✅

Update the affiliation string cleanup in `notebooks/end2end/CreateWorksBase.ipynb` in the `authors_with_corresponding` CTE.

**Current code:**
```sql
s -> TRIM(REPLACE(s, '\\n', ''))
```

**New code:**
```sql
s -> TRIM(TRAILING '.' FROM TRIM(REPLACE(s, '\\n', '')))
```

**File**: `notebooks/end2end/CreateWorksBase.ipynb`
**Location**: `authors_with_corresponding` CTE, in the TRANSFORM for raw_affiliation_strings

### Step 2: Clean work_authors Table

Update raw_affiliation_strings to remove trailing periods and deduplicate:

```sql
UPDATE openalex.works.work_authors
SET
  raw_affiliation_strings = ARRAY_DISTINCT(
    TRANSFORM(raw_affiliation_strings, s -> TRIM(TRAILING '.' FROM TRIM(REPLACE(s, '\\n', ''))))
  ),
  updated_at = current_timestamp()
WHERE EXISTS(raw_affiliation_strings, s -> s LIKE '%.');
```

### Step 3: Re-run Downstream Pipeline Steps

1. Run `UpdateWorkAuthorships.ipynb` - picks up work_authors changes via `updated_at`
2. Run `CreateWorksEnriched.ipynb` - merges work_authorships into openalex_works

### Step 4: Full Sync to Elasticsearch

Run a full sync of data to Elasticsearch (rather than incremental based on updated_date).

## Test Queries for work_authors

### Pre-Cleanup Counts

```sql
-- Count: Records with any trailing period in affiliations
-- EXPECTED BEFORE: ~98M | AFTER: 0
SELECT 'work_authors with trailing period' as metric, COUNT(*) as count
FROM openalex.works.work_authors
WHERE EXISTS(raw_affiliation_strings, s -> s LIKE '%.' AND LENGTH(s) > 1);

-- Count: Records with potential duplicates
SELECT 'records with period duplicates' as metric, COUNT(*) as count
FROM openalex.works.work_authors
WHERE SIZE(raw_affiliation_strings) > SIZE(ARRAY_DISTINCT(
    TRANSFORM(raw_affiliation_strings, s -> TRIM(TRAILING '.' FROM s))
));
```

### Examples That SHOULD Change

```sql
-- Sample records with trailing periods (periods will be removed)
SELECT work_id, author_sequence, raw_affiliation_strings
FROM openalex.works.work_authors
WHERE EXISTS(raw_affiliation_strings, s -> s LIKE '%.' AND LENGTH(s) > 10)
LIMIT 5;

-- Example work (should have duplicates before, deduplicated after)
SELECT work_id, author_sequence, raw_affiliation_strings
FROM openalex.works.work_authors
WHERE work_id = 4414994979
LIMIT 3;
```

### Examples That Should NOT Change

```sql
-- Records with abbreviations in middle (St., Dr.) but no trailing period
-- These should be preserved
SELECT work_id, author_sequence, raw_affiliation_strings
FROM openalex.works.work_authors
WHERE EXISTS(raw_affiliation_strings, s -> (s LIKE '%St.%' OR s LIKE '%Dr.%'))
AND NOT EXISTS(raw_affiliation_strings, s -> s LIKE '%.' AND LENGTH(s) > 1 AND SUBSTRING(s, LENGTH(s), 1) = '.')
LIMIT 5;

-- Records ending with numbers (no trailing period to remove)
SELECT work_id, author_sequence, raw_affiliation_strings
FROM openalex.works.work_authors
WHERE EXISTS(raw_affiliation_strings, s -> s RLIKE '[0-9]$')
LIMIT 5;
```

### Post-Cleanup Verification

```sql
-- Should return 0
SELECT 'work_authors with trailing period' as metric, COUNT(*) as count
FROM openalex.works.work_authors
WHERE EXISTS(raw_affiliation_strings, s -> s LIKE '%.' AND LENGTH(s) > 1);

-- Verify abbreviations in middle are preserved
SELECT work_id, author_sequence, raw_affiliation_strings
FROM openalex.works.work_authors
WHERE EXISTS(raw_affiliation_strings, s -> s LIKE '%St.%' OR s LIKE '%Dr.%')
LIMIT 5;
```

## Expected Results Summary

| Metric | Before | After |
|--------|--------|-------|
| Records with trailing period | ~98M | 0 |
| Records with period duplicates | >0 | 0 |
| Work 4414994979 affiliations | Has duplicates | Deduplicated, no periods |
| Abbreviations (St., Dr.) in middle | Preserved | Preserved |

## Notes

- **affiliation_strings_lookup**: Skipped cleanup (54M strings with periods, but 0 duplicates - no harm leaving as-is)
- **openalex_works sync**: Using full sync instead of incremental updated_date approach

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Large UPDATE may be slow | Run during low-traffic window |
| Abbreviations like "Inc." lose period | Accepted behavior for consistency |
| Pipeline interruption | Run steps sequentially, verify each before proceeding |
