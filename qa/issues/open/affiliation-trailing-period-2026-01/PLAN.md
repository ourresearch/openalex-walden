# Plan: Remove Trailing Periods from Raw Affiliation Strings

## Problem Statement

Raw affiliation strings are duplicated in `openalex_works` when the same affiliation appears with and without a trailing period. This affects data quality and inflates affiliation counts.

## Fix Approach

### Step 1: Modify CreateWorksBase.ipynb (Prevent Future Issues)

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

### Step 2: Clean affiliation_strings_lookup Table

Merge duplicates and delete period-ending versions:

```sql
-- Step 2a: Update non-period versions to include institution_ids from period versions
UPDATE openalex.institutions.affiliation_strings_lookup target
SET
    institution_ids = COALESCE(
        source.institution_ids,
        target.institution_ids
    ),
    institution_ids_override = COALESCE(
        target.institution_ids_override,
        source.institution_ids_override
    )
FROM openalex.institutions.affiliation_strings_lookup source
WHERE source.raw_affiliation_string = CONCAT(target.raw_affiliation_string, '.')
  AND source.raw_affiliation_string LIKE '%.';

-- Step 2b: Delete period-ending duplicates
DELETE FROM openalex.institutions.affiliation_strings_lookup
WHERE raw_affiliation_string LIKE '%.'
  AND RTRIM(raw_affiliation_string, '.') IN (
      SELECT raw_affiliation_string
      FROM openalex.institutions.affiliation_strings_lookup
  );
```

### Step 3: Clean work_authors Table

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

### Step 4: Re-run Downstream Pipeline Steps

1. Run `UpdateWorkAuthorships.ipynb` - picks up work_authors changes via `updated_at`
2. Run `CreateWorksEnriched.ipynb` - merges work_authorships into openalex_works

### Step 5: Touch updated_date for Elasticsearch Sync

```sql
-- Touch updated_date for affected works so sync_works picks them up
UPDATE openalex.works.openalex_works
SET updated_date = current_timestamp()
WHERE id IN (
  SELECT DISTINCT work_id
  FROM openalex.works.work_authors
  WHERE updated_at > [cleanup_timestamp]
);
```

### Step 6: Run sync_works

Elasticsearch sync will pick up records where `updated_date > last_sync_date`.

## Rollout Strategy

1. **Test in dev**: Run fix on development environment first
2. **Validate**: Run acceptance queries to confirm fix works
3. **Production**: Apply to production after validation
4. **Monitor**: Check a sample of work_ids in Elasticsearch API

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Large UPDATE may be slow | Run during low-traffic window |
| Abbreviations like "Inc." lose period | Accepted behavior for consistency |
| Pipeline interruption | Run steps sequentially, verify each before proceeding |
