# Issue: Invalid Institution IDs (-1) Leaking into Authorships

**Status**: open
**Discovered**: 2026-01-20
**Severity**: medium
**Component**: pipeline

## Summary

The sentinel value `-1` used in `affiliation_strings_lookup` to indicate "no institution match found" is leaking through to the final `work_authorships` table. These appear as `https://openalex.org/I-1` with null display_name, country_code, ror, and type fields.

## Impact

| Metric | Value |
|--------|-------|
| Records affected | ~1.68 million works |
| Affiliation strings with -1 | ~19 million |
| Coverage impact | Invalid institution entries in API responses |
| User-visible symptoms | Institution objects with null display_name and id ending in "I-1" |
| Time range | Ongoing |

## Files in This Issue

| File | Status | Description |
|------|--------|-------------|
| `INVESTIGATION.md` | pending | Root cause analysis |
| `PLAN.md` | complete | Fix approach |
| `notebooks/maintenance/FixInvalidInstitutionIds.ipynb` | ready | Fix notebook for affected works |
| `ACCEPTANCE.md` | pending | Verification tests |
| `evidence/` | | Supporting queries |

## Quick Links

- Related Databricks tables:
  - `openalex.institutions.affiliation_strings_lookup`
  - `openalex.institutions.raw_affiliation_strings_institutions_mv`
  - `openalex.works.work_author_affiliations_mv`
  - `openalex.works.work_authorships`
  - `openalex.institutions.institutions`
- Related notebooks:
  - `notebooks/end2end/UpdateWorkAuthorships.ipynb`
  - `notebooks/institutions/institution_batch_inference.ipynb`

---

## Evidence

### Query: Count of works with I-1 institution
```sql
SELECT COUNT(*) as count
FROM openalex.works.work_authorships
WHERE EXISTS(
    authorships,
    a -> EXISTS(a.institutions, inst -> inst.id = "https://openalex.org/I-1")
)
-- Result: 1,678,284
```

### Query: Count of affiliation strings with -1
```sql
SELECT COUNT(*) as count
FROM openalex.institutions.affiliation_strings_lookup
WHERE ARRAY_CONTAINS(institution_ids, -1)
   OR ARRAY_CONTAINS(institution_ids_override, -1)
-- Result: 19,015,024
```

### Example of -1 in final output
```json
{
  "country_code": null,
  "display_name": null,
  "id": "https://openalex.org/I-1",
  "lineage": ["https://openalex.org/I-1"],
  "ror": null,
  "type": null
}
```

---

## Next Steps

- [x] Complete root cause analysis (where should -1 be filtered?)
- [x] Write fix plan
- [ ] Define acceptance criteria
- [x] Implement fix (`CreateRawAffiliationStringsInstitutionsMV.ipynb`)
- [ ] Refresh materialized view on Databricks
- [ ] Run fix notebook (`notebooks/maintenance/FixInvalidInstitutionIds.ipynb`)
- [ ] Run acceptance tests
- [ ] Close issue

---

## Log

| Date | Who | Action |
|------|-----|--------|
| 2026-01-20 | Claude (AI agent) | Issue created |
| 2026-01-20 | Claude (AI agent) | Created PLAN.md, updated CreateRawAffiliationStringsInstitutionsMV.ipynb, created BackfillInvalidInstitutionIds.ipynb |
