# OpenAlex Walden

## Guardrails pre-flight: never let the 05:00 UTC run be the first to see a bulk change

`Walden End 2 End` runs `notebooks/end2end/Guardrails` after `CreateWorksEnriched`. Check 1 fails
the run when more than **7.5M** `openalex_works` rows carry this run's `updated_date` stamp, and a
failure skips the entire publish path (ES sync, Full_Snapshot, Wunpaywall, Lakebase, deleted-works
tracking). The Delta write has already happened, so the data is there and nothing publishes; every
job waiting on "after the nightly" loses a day until someone reruns with `guardrails_override=true`.
It has tripped on intended work four times (2026-06-25 corresponding-institution backfill, 07-22
authorships propagation, 08-21 content-hash wave, 09-21 `institution_ancestors` rollup, #1265).

Before shipping anything that feeds `CreateWorkAuthorships` / `CreateWorksEnriched` (institution
ancestors, author ids, affiliations, topics, locations, types): estimate how many works get a new
content hash; a small entity-side change can re-stamp tens of millions of works. If it is anywhere
near 7.5M, hand-run End 2 End yourself with `guardrails_override=true` while awake and say so in
#dev, or ship in the morning; do not leave it for the scheduled run. Over ~10M also crosses the ES
mega-sync threshold (replicas dropped). The override flags are per consequence class
(`guardrails_override`, `deleted_works_guard_override`, `deleted_locations_guard_override`,
`wunpaywall_guard_override`); bypass only the one that fired. Morning after any walden ship, check
the End 2 End result before reading any "after the nightly" acceptance test.
Step-by-step for the overnight override (front-load the upstream job, run-now JSON, cancel the
queued schedule, morning checks): `docs/runbooks/end2end-guardrails-override.md`.

## Landing Page & PDF Integration

Landing page and PDF data get merged into crossref/repo records at two pipeline stages:
- **Super Authorships** — author names, affiliations, is_corresponding
- **Super Locations** — license, abstract, references, URLs

See `docs/landing_page_pdf_integration.md` for field priority tables, schema details, and matching logic.

## Authorship fields have TWO inputs — transform both

`openalex_works_base.authorships[]` is built in `CreateWorksBase` by CONCATing the
`locations_mapped` side with a **frozen legacy snapshot**:

- `raw_affiliation_strings` ← `locations_mapped` authors **+** `works_legacy.raw_affiliation_strings`
- `is_corresponding` ← `locations_mapped` authors **+** `works_legacy.work_authors`

A string-level transform applied only at `locations_mapped` gets **silently half-undone**: the
legacy copy re-adds the untransformed value one step downstream, and `ARRAY_DISTINCT` then
leaves the work carrying *both* variants. The first cut of the oxjob #801 mojibake repair
covered 57% of affected works this way — 102,495 of 237,229 came back via the legacy table.
Repair the legacy side **at read time** — the table is a frozen 850M-row / 44 GB snapshot
(last write 2026-01-07), so a rewrite buys nothing. Applies to any future normalization of
author names, affiliation strings, or is_corresponding (#808/#809 are the near-term ones).

**Raw affiliation strings are exact-match KEYS, not just text** (`affiliation_strings_lookup`
→ `raw_affiliation_strings_institutions_mv`, `ras_curations`), so normalizing a string upstream
of `CreateWorkAuthorships` **re-keys its institution links** — and where the normalized form
already exists as its own key with a different answer, one answer must win. Decide that
explicitly and measure it before shipping. #801's rule (Jason, 2026-08-18): **clean wins** — the
garbled-text matches were sampled and are systematically degraded (`Université Laval` →
"Geological Survey of Canada"; junk like "Anna Needs Neuroblastoma Answers"), so the clean twin's
answer stands and strings without a twin are re-matched on the clean text. Predicted delta was
computed per institution before the run (oxjob #801 EXPLORE). Also carry curations onto the new
key (`SyncRasCurations` re-keys via the same UDF, latest-action-wins on collision) and keep the
RLIKE gate **byte-identical across every call site** (CreateLocationsMapped, CreateWorksBase,
SyncRasCurations, PrepareAffiliationStrings) — a gate that disagrees between sites is what splits
a work across two keys.

`Guardrails.ipynb` check 10 is the tripwire: works still carrying a garbled key of
`openalex.institutions.affiliation_strings_repair` (the M→R map maintained in
`PrepareAffiliationStrings`). Nonzero ⇒ some input path is bypassing the repair.
