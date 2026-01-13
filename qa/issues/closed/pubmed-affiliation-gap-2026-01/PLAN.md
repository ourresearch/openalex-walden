# Plan: Add PubMed as Source for Raw Affiliation Strings

## Summary

Modify `notebooks/end2end/CreateCrossrefSuperAuthorships.ipynb` to include `pubmed` in the provenance filter for affiliation sources.

## Root Cause

The `crossref_base` CTE in `CreateCrossrefSuperAuthorships.ipynb` filters affiliations to:
```sql
WHERE authors_exist
  AND provenance IN ('crossref', 'pdf', 'landing_page')
```

PubMed is excluded, despite being priority 3 in the system priority table.

## Fix

### File to Modify
`notebooks/end2end/CreateCrossrefSuperAuthorships.ipynb`

### Change
In cell `cell-1`, modify the `crossref_base` CTE:

**Before:**
```sql
WHERE authors_exist
  AND provenance IN ('crossref', 'pdf', 'landing_page')
```

**After:**
```sql
WHERE authors_exist
  AND provenance IN ('crossref', 'pubmed', 'pdf', 'landing_page')
```

### Priority Ordering

PubMed is already priority 3 in `openalex.system.priority_table`:

| provenance | priority |
|------------|----------|
| crossref | 1 |
| datacite | 2 |
| pubmed | 3 |
| landing_page | 7 |
| pdf | 9 |

Adding `pubmed` to the filter means:
- Crossref affiliations take precedence (priority 1)
- PubMed affiliations used as fallback (priority 3)
- Landing page and PDF used if neither has affiliations

## Verification

After pipeline runs:
1. Run validation queries in `evidence/validation_queries.sql`
2. Verify sample DOIs now have affiliations
3. Check overall affiliation coverage improvement

## Rollback

Remove `'pubmed'` from the provenance filter to restore original behavior.
