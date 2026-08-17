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
leaves the work carrying *both* variants. This is how the oxjob #801 mojibake repair shipped
covering 57% of affected works — 102,495 of 237,229 came back via the legacy table.

Applies to any future normalization of author names, affiliation strings, or is_corresponding
(#809 is the near-term one). Repair the legacy side **at read time** — the table is a frozen
850M-row / 44 GB snapshot (last write 2026-01-07), so a rewrite buys nothing.

**Why it bites harder than a cosmetic diff:** raw affiliation strings are exact-match keys
(`ras_curations`, `affiliation_strings_lookup` → `raw_affiliation_strings_institutions_mv`).
A work carrying both variants gets its curation applied to only one of them, and the sync is
insert/update-only, so the stale-key row freezes instead of erroring. Corollary: any RLIKE gate
guarding such a transform must stay **byte-identical across every call site** — a gate that
disagrees between two sites is what splits a work across two keys. #801 has four
(CreateLocationsMapped, SyncRasCurations, CreateWorksBase, Guardrails).

`Guardrails.ipynb` check 10 is the tripwire: it counts works whose affiliation strings
`repair_mojibake()` would *still* change. Nonzero ⇒ some input path is bypassing the repair.
