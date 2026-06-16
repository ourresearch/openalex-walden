# Wiring PMC/PubMed grant↔output linkages into OpenAlex

Investigation of how funder/grant linkages flow into OpenAlex today, and where to
add PubMed/PMC grant linkages. Motivated by the AHA gold-set finding that OpenAlex
links AHA at the **funder** level but carries almost **no AHA award (grant) IDs** —
because AHA's grantees deposit grant↔publication links in **PubMed Central**, which
we don't currently mine.

## Headline
- **PubMed `<GrantList>` IS already parsed** but then **dropped** — nothing downstream
  consumes it.
- **PMC JATS `<funding-group>` is NOT parsed at all** (there is no PMC metadata parser;
  PMC is download-only). AHA's grantee-created linkages live here.

## How linkages flow today
1. **Ingest is download-only.** `openalex-ingest/pubmed.py`, `pubmed_central.py`,
   `crossref.py` just dump raw XML/JSON to S3.
2. **Parsing lives in walden DLT notebooks** (`notebooks/ingest/`).
   - `Crossref.py:512` parses `funder[]` → a shared funders struct
     `{doi, ror, name, awards[]}`.
   - `PubMed.py:479` parses `MedlineCitation.Article.GrantList.Grant` via
     `consolidate_awards_udf` → the **same** struct, but with `doi/ror = NULL`
     (only the `<Agency>` name string + `<GrantID>`s).
   - **No PMC parser exists** in `notebooks/ingest/`.
3. **Per-source funders** land in `openalex.works.locations_mapped` (one row per
   `(merge_key, native_id, provenance)`, with a `provenance` column).
4. **`notebooks/awards/CreateCrossrefWorkFunders.ipynb`** explodes
   `locations_mapped.funders WHERE provenance='crossref' AND f.doi IS NOT NULL`,
   resolves funder **DOI** → `mid.funder`, and writes `awards.crossref_work_funders`
   + rows into `openalex_awards_raw` (`provenance='crossref_work_funders'`, priority 2).
5. The public **`work.funders`** field is a UNION of 6 legs in
   `notebooks/end2end/CreateWorksEnriched.ipynb` (backfill, fulltext, gtr, crossref,
   work_awards, funder_reported). **There is no PubMed/PMC leg** — so PubMed's parsed
   GrantList dies at `locations_mapped(provenance='pubmed')`.
   Final shape: `ARRAY<STRUCT<id, display_name, ror>>` — **no award IDs, no per-edge source.**

## Resolution mechanisms that already exist
- **By DOI** (crossref): `JOIN mid.funder ON funder_doi = doi`. Useless for PubMed/PMC
  (no funder DOI).
- **By name** (the model to reuse): the fulltext leg resolves a funder *name* via a
  curated allowlist — `JOIN openalex.common.funder_names_keep keep ON keep.name = ft.funder_name`.
  PubMed `<Agency>` / PMC `<funder-name>` strings resolve the same way (with normalization).
- **Award id** = `ABS(XXHASH64(funder_id || ':' || lower(award_id))) % 9e9`; junk filtered
  by `openalex.common.is_usable_award_id`; grant DOIs via `extract_grant_doi`.

## Provenance (AHA's "did it come from PMC?" ask)
Row-level `provenance` already exists on `locations_mapped` and `openalex_awards_raw`.
A `provenance='pmc_work_funders'` tag rides along for free and makes PMC-sourced links
internally queryable. Exposing source on the **public** `work.funders` is a separate,
larger schema change — scope only if AHA needs it API-visible.

## Integration plan
1. **PubMed (quick win — data already parsed):** add `CreatePubmedWorkFunders.ipynb`
   modelled on the crossref one but reading `locations_mapped WHERE provenance='pubmed'`
   and resolving `f.name` by name; write `awards.pubmed_work_funders` +
   `openalex_awards_raw(provenance='pubmed_work_funders')`; add a `from_pubmed` leg to the
   funders UNION in `CreateWorksEnriched.ipynb`.
2. **PMC (the actual AHA payload):** build `notebooks/ingest/PMC.py` (new DLT parser like
   `PubMed.py`) over `s3://openalex-ingest/pubmed-central/`, parsing JATS
   `<funding-group>/<award-group>` (`<funder-name>`→name, `<award-id>`→awards),
   `provenance='pmc'`; then `CreatePmcWorkFunders.ipynb` + a `from_pmc` UNION leg.
3. Register new priorities in `CreateAwards.ipynb`; extend `funder_names_keep` with
   PubMed-Agency / PMC funder-name aliases.

## Gotchas
- **Deploys/sync:** every push to walden `main` fires the Deploy Bundle. Build on a
  feature branch (`jobs submit git_branch:<branch>`), merge once, run the ES works-sync
  once at the end. Adding a UNION leg re-enriches `work.funders` corpus-wide.
- **Agency string quality:** PubMed `<Agency>` is abbreviated/inconsistent
  ("NHLBI NIH HHS", grant number sometimes packed in) — name resolution needs a
  normalization/alias layer; expect lower match than crossref's DOI join. Probe coverage
  before wiring the UNION leg.
- **NIH grant-id format:** confirm `is_usable_award_id` doesn't over-drop activity-code
  serials.
- **Idempotent:** the funders UNION dedups by `(work_id, funder_id)`, so PMC/PubMed only
  *adds* funders the other legs missed — exactly the AHA case.
