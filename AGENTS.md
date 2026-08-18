# OpenAlex Walden

## Landing Page & PDF Integration

Landing page and PDF data get merged into crossref/repo records at two pipeline stages:
- **Super Authorships** — author names, affiliations, is_corresponding
- **Super Locations** — license, abstract, references, URLs

See `docs/landing_page_pdf_integration.md` for field priority tables, schema details, and matching logic.

## Authorship fields have TWO inputs — transform both (or transform at the output)

`openalex_works_base.authorships[]` is built in `CreateWorksBase` by CONCATing the
`locations_mapped` side with a **frozen legacy snapshot**:

- `raw_affiliation_strings` ← `locations_mapped` authors **+** `works_legacy.raw_affiliation_strings`
- `is_corresponding` ← `locations_mapped` authors **+** `works_legacy.work_authors`

A string-level transform applied only at `locations_mapped` gets **silently half-undone**: the
legacy copy re-adds the untransformed value one step downstream, and `ARRAY_DISTINCT` then
leaves the work carrying *both* variants. The first cut of the oxjob #801 mojibake repair
covered 57% of affected works this way — 102,495 of 237,229 came back via the legacy table.
The legacy table is a frozen 850M-row / 44 GB snapshot (last write 2026-01-07), so if you must
transform it, do it at read time.

**Raw affiliation strings are exact-match KEYS, not just text.** `affiliation_strings_lookup` →
`raw_affiliation_strings_institutions_mv` and `ras_curations` all key on the exact bytes, so
changing a string upstream of `CreateWorkAuthorships` *re-keys* its institution links — a
merged key must resolve identically for every work that carries it, which redistributes
attribution (measured for #801: 35K–320K works move under any key-merging design). The
FROZEN rule (Jason, 2026-08-17): an encoding/normalization fix must leave every institution's
`works_count` exactly unchanged. Hence #801 ships as a **display-layer** transform in
`CreateWorkAuthorships`: institutions are joined on the original bytes, and only the emitted
`raw_affiliation_strings` / `affiliations[].raw_affiliation_string` are repaired, via the
`openalex.institutions.affiliation_strings_repair` map (maintained in
`PrepareAffiliationStrings`). Both inputs are already CONCATed by then, so one transform covers
both, and the works content hash (`TO_JSON(authorships)`) propagates it to ES/Lakebase.
Consequences to keep in mind: `SyncRasCurations` fans curations out over the mojibake
equivalence class (works display R but are keyed on M), and users-api `ras_verifier` checks
both the stored key and its repaired form. Any future normalization of author names or
affiliation strings (#808/#809) should reuse this shape unless it is *deliberately* a re-keying.

`Guardrails.ipynb` check 10 is the tripwire: it counts works whose *displayed* affiliation
strings are still a mojibake key of `affiliation_strings_repair`. Nonzero ⇒ some path is
emitting strings without going through the display map.
