# Per-funder award-id translation (oxjob #690) — v2, layered on award-id-audit

**Branch `award-id-translation-v2`. Builds ON the 2026-07-31 award-id-audit machinery
(normalized-key collapse + award_id_aliases + canonical work-joins) — no parallel system.**

## Design

One shared function, `openalex.awards.award_norm_key(funder_id, award_id, side)`
(generated — `scripts/gen_award_norm_key.py`, DO NOT hand-edit): for the 15 audit-validated
funders it applies the per-funder translation rule (side='registry' parses funder-export
spellings, 'deposited' parses citation-side spellings); for every other funder it reproduces
the 07-31 generic key byte-for-byte, so unconfigured funders are untouched. All nk
computations in CreateAwards (aliases + collapse) and WorkAwards (all 8 sites) route through
it — mixed key regimes would silently mismatch, so the function is the single source.

`award_id_verdicts` (same generated file) labels every deposited id:
confirmed / confirmed_ambiguous (renewal-year families) / plausible / garbage / unscored.
Read-only w.r.t. the awards flow; feeds the junk guard (next phase).

**MERGE_FAMILIES=false** (pending @kyle): keys matching >1 registry award (NIH renewal
years; ~375k keys, avg 5.3 rows) are excluded from aliasing + collapse, and WorkAwards
elects the surviving deposited shell for them — pre-#690 behavior preserved exactly until
the lineage decision. Flip = the documented n_in_group / n_reg conditions + election CASE.

## Validation (openalex_dev.rohan_lab, 2026-08-03)

- Function smoke: NIH 'R01 CA80205' == '5r01ca080205-03' → CA080205; Wellcome citable →
  6-digit core; unconfigured funder → generic key identical to shipped behavior.
- Verdicts over all deposited ids for the 15 funders: confirmed 31.1% /
  confirmed_ambiguous 12.7% / plausible 35.9% / garbage 20.3% — reconciles with the #690
  audit (43.6 combined confirmed / 36.0 / 20.4).
- Recall anchors: 17/17 known-good ids verdict=confirmed.

## Deploy order

AwardNormKey.sql (function + verdicts) runs after registry ingests, before CreateAwards →
WorkAwards. Wellcome + 9 funders' rules re-derive after the 07-31 citable-ref backfills land.
