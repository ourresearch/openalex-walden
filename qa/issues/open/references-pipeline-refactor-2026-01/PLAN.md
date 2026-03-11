# References Pipeline Refactor Plan

## Current Architecture

### Data Flow

```
locations_mapped
├── references (raw array of structs)
│   └── parse_work_references → work_references → referenced_works table
│
└── referenced_works (pre-resolved IDs from legacy/MAG)
    └── CreateWorksBase → openalex_works_base.referenced_works
                              └── CreateWorksEnriched (merges both sources)
```

### Current Pipeline Order (walden_end2end.yaml)

```
Locations_Mapped
    ↓
parse_referenced_works (reads from locations_mapped)
    ↓
Works_Base (reads from locations_mapped)
    ↓
... enrichment steps ...
    ↓
Works_Enriched (merges references from openalex.works.referenced_works)
```

### Problem

The `parse_work_references` job is an outlier:
- All other enrichment steps read from `openalex_works_base`
- `parse_work_references` reads from `locations_mapped` directly
- This breaks the clean pattern of `locations_mapped → works_base → enrichment`

## Proposed Architecture

### New Data Flow

```
locations_mapped
    └── CreateWorksBase
            ├── openalex_works_base.referenced_works (pre-resolved IDs)
            └── openalex_works_base.references (raw reference data)
                    └── parse_work_references
                            └── work_references → referenced_works table
```

### New Pipeline Order

```
Locations_Mapped
    ↓
Works_Base (now includes raw references)
    ↓
parse_referenced_works (reads from openalex_works_base)
    ↓
... enrichment steps ...
    ↓
Works_Enriched
```

## Implementation Steps

### Step 1: Update CreateWorksBase.ipynb

Add raw `references` collection alongside existing `referenced_works`:

```python
# Current: only collects referenced_works
.select("work_id", "provenance", "referenced_works")

# Proposed: also collect raw references
.select("work_id", "provenance", "references", "referenced_works")
```

#### Deduplication Strategy for Raw References

Raw references need careful deduplication when a work has multiple provenances:

1. **Priority-based selection:** Use provenance priority (crossref > pubmed > datacite > repo)
2. **Union approach:** Combine all references, dedupe by DOI/PMID/title
3. **Recommended:** Priority-based selection (simpler, consistent with other fields)

```python
# Priority-based approach
references_collected = (
    locations_mapped
    .filter(F.col("references").isNotNull())
    .select("work_id", "provenance", "references")
    .withColumn("priority", get_provenance_priority("provenance"))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("work_id").orderBy("priority")
    ))
    .filter(F.col("rn") == 1)
    .select("work_id", "references")
)
```

### Step 2: Update parse_work_references.ipynb

Change the source table from `locations_mapped` to `openalex_works_base`:

```python
# Current
references = spark.table("openalex.works.locations_mapped")

# Proposed
references = spark.table("openalex.works.openalex_works_base")
```

The rest of the parsing logic remains unchanged - it still:
1. Explodes references array with `posexplode()`
2. Resolves `cited_work_id` against `work_id_map`
3. Creates `work_references` and `referenced_works` tables

### Step 3: Update walden_end2end.yaml

Reorder jobs to ensure `Works_Base` runs before `parse_referenced_works`:

```yaml
# Current order
- task_key: parse_referenced_works
  depends_on:
    - task_key: Locations_Mapped

- task_key: Works_Base
  depends_on:
    - task_key: Locations_Mapped

# Proposed order
- task_key: Works_Base
  depends_on:
    - task_key: Locations_Mapped

- task_key: parse_referenced_works
  depends_on:
    - task_key: Works_Base  # Changed dependency
```

## Testing Plan

1. **Schema validation:** Ensure `openalex_works_base` includes `references` column
2. **Data integrity:** Verify reference counts match before/after refactor
3. **Pipeline run:** Test full end2end pipeline with new ordering
4. **Spot check:** Compare `work_references` output for sample works

## Rollback Plan

1. Revert `CreateWorksBase.ipynb` changes
2. Revert `parse_work_references.ipynb` changes
3. Revert `walden_end2end.yaml` dependency changes
4. Re-run pipeline from `Locations_Mapped`

## Notes

- This refactor does not change the final output - `referenced_works` in `openalex_works` will be identical
- The change improves architectural consistency and makes the pipeline easier to understand
- No data loss risk - raw references are preserved in `work_references` table regardless of source
