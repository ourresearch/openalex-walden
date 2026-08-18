# OpenAlex Walden

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
