# Plan: Fix Invalid Institution IDs (-1) in Authorships

**Status**: ready to implement
**Created**: 2026-01-20

## Summary

Fix the sentinel value `-1` (meaning "no institution match found") leaking into `work_authorships` through `raw_affiliation_strings_institutions_mv` due to incomplete filtering.

## Root Cause Analysis

### Current filtering logic (incomplete):

```sql
CASE
  WHEN institution_ids_override != array()
    THEN FILTER(institution_ids_override, x -> x IS NOT NULL)  -- Only filters NULL, not -1
  ELSE
    CASE
      WHEN institution_ids = array(-1) THEN array()  -- Filters ONLY when exactly array(-1)
      WHEN SIZE(institution_ids) > 0 AND institution_ids[0] IS NULL THEN array()
      ELSE institution_ids  -- Mixed arrays like array(123, -1) pass through
    END
END AS institution_ids
```

### Gaps identified:

1. **PRIMARY CAUSE**: `-1` in `institution_ids_override` is NOT filtered (only NULLs are)
   - When override is `array([-1])` (used to mark "no institution" override), it passes through
2. `-1` mixed with valid IDs like `array(123, -1, 456)` would pass through unchanged
   - Not currently occurring in data, but defensive fix warranted

### Evidence:

- 750,157 records in MV have -1 in `institution_ids`
- ALL come from `institution_ids_override = array([-1])` (used to mark "no institution" override)
- Zero come from mixed model arrays

## Fix Approach

Update `raw_affiliation_strings_institutions_mv` to filter -1 from all cases using a single FILTER at the end:

```sql
FILTER(
  CASE
    WHEN institution_ids_override != array() THEN institution_ids_override
    WHEN SIZE(institution_ids) > 0 AND institution_ids[0] IS NULL THEN array()
    ELSE COALESCE(institution_ids, array())
  END,
  x -> x IS NOT NULL AND x != -1
) AS institution_ids
```

This applies the FILTER once at the end, removing both NULL and -1 from whichever source is used.

## File to Modify

- `notebooks/works/CreateRawAffiliationStringsInstitutionsMV.ipynb` - cell-1

## Verification Query

After refreshing the MV and re-running the authorships pipeline:

```sql
-- Should return 0
SELECT COUNT(*) as count
FROM openalex.works.work_authorships
WHERE EXISTS(
    authorships,
    a -> EXISTS(a.institutions, inst -> inst.id = "https://openalex.org/I-1")
)
```

## Backfill Strategy

The pipeline is incremental (`WHERE updated_date > max_updated_date`), so existing affected records won't be automatically fixed.

### Option A: Targeted backfill notebook (recommended)

Create a maintenance notebook that:
1. Identifies affected work_ids from `work_authorships` where institutions contain I-1
2. Re-processes only those ~1.68M works by running the authorships enrichment logic for just those IDs

```sql
-- Get affected work_ids
CREATE OR REPLACE TEMP VIEW affected_works AS
SELECT work_id
FROM openalex.works.work_authorships
WHERE EXISTS(
    authorships,
    a -> EXISTS(a.institutions, inst -> inst.id = "https://openalex.org/I-1")
);

-- Then run the same CTE logic from UpdateWorkAuthorships but filtered to affected_works
```

### Option B: Force full reprocess

Set `max_updated_date` to a date before the affected records were created, triggering a full reprocess. This is slower but simpler.

## Execution Steps

1. [x] Create `qa/issues/open/invalid-institution-ids-2026-01/PLAN.md` with fix documentation
2. [x] Update `CreateRawAffiliationStringsInstitutionsMV.ipynb` to filter -1 from overrides
3. [ ] Refresh the MV on Databricks
4. [x] Create fix notebook (`notebooks/maintenance/FixInvalidInstitutionIds.ipynb`)
5. [ ] Run backfill
6. [ ] Verify with acceptance query
7. [ ] Close issue

---

## Change Details

### Before (CreateRawAffiliationStringsInstitutionsMV.ipynb cell-1):

```sql
CREATE OR REPLACE MATERIALIZED VIEW openalex.institutions.raw_affiliation_strings_institutions_mv AS
SELECT
  raw_affiliation_string,
  CASE
    WHEN institution_ids_override != array()
      THEN FILTER(institution_ids_override, x -> x IS NOT NULL)
    ELSE
      CASE
        WHEN institution_ids = array(-1) THEN array()
        WHEN SIZE(institution_ids) > 0 AND institution_ids[0] IS NULL THEN array()
        ELSE institution_ids
      END
  END AS institution_ids,
  ...
```

### After:

```sql
CREATE OR REPLACE MATERIALIZED VIEW openalex.institutions.raw_affiliation_strings_institutions_mv AS
SELECT
  raw_affiliation_string,
  FILTER(
    CASE
      WHEN institution_ids_override != array() THEN institution_ids_override
      WHEN SIZE(institution_ids) > 0 AND institution_ids[0] IS NULL THEN array()
      ELSE COALESCE(institution_ids, array())
    END,
    x -> x IS NOT NULL AND x != -1
  ) AS institution_ids,
  ...
```
