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

### Approach Used: Modify UpdateWorkAuthorships

Modified `UpdateWorkAuthorships.ipynb` to include affected records in the `base_works` CTE:

```sql
WHERE (updated_date > max_updated_date
       OR id IN (
           SELECT work_id FROM identifier('openalex' || :env_suffix || '.works.work_authorships')
           WHERE EXISTS(authorships, a -> EXISTS(a.institutions, inst -> inst.id = 'https://openalex.org/I-1'))
       ))
```

This processes both new/updated records AND existing records with I-1 institution IDs.

## Execution Steps

1. [x] Create `qa/issues/open/invalid-institution-ids-2026-01/PLAN.md` with fix documentation
2. [x] Update `CreateRawAffiliationStringsInstitutionsMV.ipynb` to filter -1 from overrides
3. [x] Refresh `raw_affiliation_strings_institutions_mv` on Databricks
4. [x] Refresh `work_author_affiliations_mv` on Databricks
5. [x] Modify `UpdateWorkAuthorships.ipynb` to include affected records
6. [x] Run UpdateWorkAuthorships pipeline (fixed ~1.68M records)
7. [x] Delete stale records with I-1 institutions (3,324 deleted)
8. [x] Verify with acceptance query (returns 0)
9. [x] Revert `UpdateWorkAuthorships.ipynb` to normal incremental behavior
10. [ ] Sync deleted work_ids to Elasticsearch
11. [ ] Close issue

## Delete Stale Records

After the fix, 3,324 works still have I-1 institutions. Investigation showed:
- 16 works: not in `openalex_works_base` at all
- 3,308 works: have empty authorships (`SIZE(authorships) = 0`) in the base table

These are stale records - the source no longer has authorships data, but `work_authorships` retained old data with I-1 institutions.

### Delete query:

```sql
DELETE FROM openalex.works.work_authorships
WHERE EXISTS(authorships, a -> EXISTS(a.institutions, inst -> inst.id = 'https://openalex.org/I-1'))
```

### Sync deleted work_ids to Elasticsearch:

After deletion, sync these work_ids to remove/update them in Elasticsearch:

```sql
SELECT work_id
FROM openalex.works.work_authorships VERSION AS OF 160
WHERE EXISTS(authorships, a -> EXISTS(a.institutions, inst -> inst.id = 'https://openalex.org/I-1'))
```

### Table history reference:
- Version 160 (2026-01-20 18:45:05): Fix applied via MERGE
- Affected records span versions before this fix

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
