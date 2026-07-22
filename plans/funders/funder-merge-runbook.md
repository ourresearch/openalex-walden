# Funder entity merge — runbook

How to merge a duplicate funder profile into its canonical twin. First use (2026-07-22):
**F4320307874 "Wellcome" → F4320311904 "Wellcome Trust"** — same ROR (029chgv08), two
Crossref funder DOIs for the same organisation (10.13039/100004440 vs 10.13039/100010269).
Found during Year 1 funder report prep (`funder year 1 report/analyses/stat-fill.md`).

## Why an alias row, not a delete

Aggregator matching resolves funders against `openalex.mid.funder` by **DOI**
(`CreateCrossrefWorkFunders`, `CreateDataCiteWorkFunders`) or **name**
(`CreateEuropePmcWorkFunders`, DataCite name-fallback). Publishers deposit the loser's
funder DOI indefinitely, so deleting the row would silently drop every future work-funder
link carried under it (~26K works for Wellcome). Instead the loser row survives as an
alias: it keeps its `doi`, gets `merge_into_id` = winner, and matchers resolve
`COALESCE(merge_into_id, funder_id)`. The winner absorbs the loser's display_name +
alternate_titles so name matching keeps working. `CreateFundersAPI` filters
`merge_into_id IS NULL` so the loser never surfaces as an API entity (same convention as
the sources registry, oxjob #548).

## Order of operations

1. **Run `notebooks/maintenance/MergeFunders.ipynb`** (one-off job submit, params
   `merge_from_id` / `merge_into_id`, bare numeric ids). It ALTERs `mid.funder`
   (adds `merge_into_id`/`merge_into_date`), absorbs names, tombstones the loser,
   remaps stored edges (`mid.work_funder`, the three `*_work_funders` junctions,
   `fulltext_work_funders`), remaps `openalex_awards_raw` with **recomputed award ids**
   (id = hash(funder_id:award) is the cross-provenance dedup key — rows colliding with
   existing winner awards are deleted as duplicates), writes the `merge-funders` ES
   mapping doc, and deletes the loser doc from `funders-v3`.
2. **Merge the `funder-merge-*` branch to main** — matcher COALESCE edits +
   `CreateFundersAPI` filter + `sync_funders` delete-stale block. Do NOT merge before
   step 1: the edits reference `merge_into_id`, which the notebook creates, and
   scheduled runs from main would fail on the missing column.
3. **Let the scheduled chain cycle** (CreateAwards 3am → CreateFundersAPI → ES 5am;
   end2end works refresh): `openalex_awards`, `work_awards`, `work.funders`, and the
   winner's API counts converge. No full-job run-now needed — the in-place remaps mean
   the next scheduled runs pick everything up.
4. **Deploy the openalex-elastic-api change** (branch `funder-merge-redirect`):
   `funders_id_get` falls back to the `merge-funders` index on a miss and returns the
   documented **301** to the winner.

## Verification

- `GET /funders/F<winner>` — works_count / awards_count ≈ old winner + old loser − overlap
  (works citing both DOIs and awards present under both profiles dedupe).
- `GET /funders/F<loser>` — origin emits the documented **301** → winner, but note:
  api.openalex.org's Cloudflare worker follows origin redirects (it always has — the
  lowercase-id normalize redirect behaves the same), so clients observe a **200 with the
  winner's record** at the loser URL rather than a raw 301. Both are correct outcomes;
  test the mapping by checking the returned `id` is the winner. (404 before step 4.)
- Direct-ingest awards unchanged (Wellcome: 19,611 `wellcome_trust` on the winner).
- The MergeFunders verification cell: zero loser rows across junctions/awards_raw.

## Cautions

- **Only merge aggregator shadows.** The notebook asserts the loser has no direct-ingest
  provenance in `openalex_awards_raw`; a direct provenance means a scraper/registry row
  points at the loser — fix that first.
- **Keep-separate list for Wellcome** (stat-fill.md table): Burroughs Wellcome Fund
  (F4320306133), Wellcome Leap (F6625209195), DBT India Alliance (F4320325580), and the
  Wellcome centres are distinct legal entities — do not merge.
- `openalex.common.funder` is a mirror maintained outside walden; the notebook applies
  the same tombstone/absorb best-effort, but its refresher may rebuild the table —
  confirm with Casey that the mirror preserves (or re-derives) `merge_into_id`.
  `CreateDataCiteAwards` (DLT) matches DataCite *grant records* against `common.funder`
  by ROR/DOI/name; funder-DOI aliasing is not wired there (no Wellcome impact — Wellcome
  registers no DataCite grants). Revisit if a merged funder does.
- Award IDs churn by design: loser-attributed award ids (hash of loser funder_id) are
  replaced by winner-hashed ids. Anything caching `G...` ids for the loser's awards will
  see new ids.
