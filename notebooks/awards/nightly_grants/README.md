# Nightly grants job: what changed vs the deployed release chain (stableid-r1-prod)

Base = the deployed code (build/deployed/, exported 09-29 from /Workspace/Users/rohan@openalex.org/stableid-r1-prod).
build_awards.py and work_awards.py are generated from the deployed files by port_build_awards.py / port_work_awards.py
(anchored edits; a test fails if the lib files drift from the scripts). create_api.py: 5 anchored edits. sync_awards.py and
nightly_runtime.py are new (they replace the release wrapper).

## Removed (the wrapper)
- NightlyD0 pins, quiet window, legacy/registry producer lineage, protected-interval rechecks; CaptureD0 run-scoped copies; PrepareRelease manifest.
- Works receipt + wait on End-2-End (WaitAndReceipt, works barrier, missing-links ledger, rev2 revision). Awards/API can lag works by a day.
- award_runs 12-phase journal, tombstones, adoption, inject hooks, per-release table sets (~110/night), evidence Volumes, code-SHA pinning.
- Dead paths (no gtr_deferred / hrb_* outputs in the live release n20260925): GTR-deferral gates, HRB gates, award_key_conflicts logging.

## Kept unchanged (identity + linking)
Everything in deployed build_awards.py / work_awards.py except the edits listed in the port scripts: component grouping, collision
recovery, NATIVE keys, NATIVE/STAGING disagreement stop, funder canonicalisation, shell retirement, redirect traversal, reverse gate,
metadata election, DOI-pair metadata fill, embedded-link resolution, all nine link legs + election, API content-hash updated_date.
All ~95 deployed checks run, including NATIVE_CONTINUITY, OBSERVATION_CONTINUITY, ONE_BINDING_PER_OBSERVATION, ALLOCATION_FUSE, GONE_FUSE.
The migration-manifest retirement path stays, driven by an optional approved input (config extra_inputs.migration_manifest) for HRB-style cleanups.

## Replaced
| was | now |
|---|---|
| inputs pinned by NightlyD0, read by time travel, copied by CaptureD0 | each input read once at the version current at run start; views copied at start |
| D0 relations (crossref/datacite records, funder map, normalization, normalization_work) built per cohort | rebuilt each run from those inputs, same SQL; 17 identity-relevant D0 gates kept |
| funders fixed pin (v34) | current funders; a funder merge canonicalises through the map (ownership changes still need proof: DIRECT_CORRECTION_UNPROVEN) |
| receipts hard-coded true (CaptureD0.r1.py:219) | per-family completeness: today's observations >= 98% of the last successful run's, else the run stops |
| last_state / previous_bindings from the last ACCEPTED release | last_state = live awards table (public = ACTIVE) + registry; previous bindings in award_bindings_last, carried by each successful run |
| merge_doi_pairs from release outputs | award_merge_doi_pairs (seeded from n20260925) |
| registry lock + tombstones | award_nightly_lock (compare-and-set; taken over only if the holder's run row is finished or its Databricks run is terminated) + max_concurrent_runs 1 |
| award_runs journal | award_nightly_runs: one row per run (versions read, counts, check results, error) |
| ES full re-sync every release | only new/changed documents (updated_date >= last successful search publish, or missing from the index); redirects, deletes and all scan verifications as before |

## Added
- Links on merged-away works move to the surviving work (openalex.works.merged_work_ids; chains followed; a loser with two winners
  takes the latest merge; cycles skipped and counted; only to a served survivor), before the final (work, award) dedupe.
- Never-public ACTIVE ids (minted by a run that failed before its swap) that nothing binds today become GONE (never served, nothing to renumber).
- Write fence: every write target must start with a configured prefix (dev: openalex_dev.rohan_lab.ngr_).
