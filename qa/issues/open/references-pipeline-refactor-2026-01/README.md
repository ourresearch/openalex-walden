# References Pipeline Refactor

**Status:** Open
**Type:** Architectural Improvement
**Priority:** Low (enhancement, not a bug)
**Created:** 2026-01

## Summary

The `parse_work_references` job reads directly from `locations_mapped` instead of `openalex_works_base`, breaking the clean pattern followed by all other enrichment steps in the pipeline. This issue proposes refactoring to:

1. Move raw `references` collection into `CreateWorksBase`
2. Update `parse_work_references` to read from `openalex_works_base`
3. Maintain the clean `locations_mapped → works_base → enrichment` pattern

## Impact Assessment

- **Severity:** Low - current pipeline works correctly
- **Type:** Technical debt / architectural consistency
- **Risk:** None - this is an improvement, not a fix

## Current State

The pipeline processes references through multiple paths:
- **Raw references:** `locations_mapped.references` → `parse_work_references` → `work_references` table
- **Pre-resolved IDs:** `locations_mapped.referenced_works` → `CreateWorksBase` → `openalex_works_base.referenced_works`

This creates an inconsistency where `parse_work_references` is the only enrichment job that reads from `locations_mapped` directly.

## Data Statistics

- Total works with references: ~262 million
- Works with references from multiple provenances: exists (crossref + pubmed, crossref + repo, etc.)
- Provenance sources: crossref, pubmed, repo, repo_backfill

## Relevant Files

- `notebooks/end2end/CreateWorksBase.ipynb` - Base work creation
- `notebooks/parsing/parse_work_references.ipynb` - References parsing
- `notebooks/end2end/CreateWorksEnriched.ipynb` - Final enrichment
- `jobs/walden_end2end.yaml` - Pipeline configuration

## See Also

- [PLAN.md](./PLAN.md) - Detailed implementation plan
