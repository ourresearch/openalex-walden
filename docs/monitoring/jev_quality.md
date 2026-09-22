# Monitoring — Jev quality sample (nightly pipeline QA)

<!-- GENERATED from monitoring/checks/jev_quality.yaml by scripts/monitoring_render_docs.py — edit the YAML -->

Component `jev_quality`. Metrics: `openalex.monitoring.metrics`. Owner: jason. Evaluated nightly by `notebooks/monitoring/MonitoringFindings`; findings in `openalex.monitoring.findings`; the morning report in `openalex.monitoring.reports`.

> Started 2026-09-21 (oxjob #1301) after a 5 × 200 probe: Jev's flags at the per-stratum cuts are 0.75-1.00 precise against Opus 5 but its recall is 35-85%, so precision_jev is a DRIFT signal per rule / band / provenance / endpoint / tier, not an error rate. Relative checks need 7 nights of history before they can fire; the floors fire from night one. Cuts: a_type 0.5, b_affil 0.7, c_merge 0.6, d_repo 0.5, e_auth 0.6.

## How to read this

Each heading is the question we care about. Each row is one check the engine runs. **absolute** rules are fixed lines (critical on day one). **relative** rules compare today to the trailing median in units of MAD: watch at the first number, critical at the second, or at the first number two nights running. Status is deterministic; the LLM only narrates.

## Did the sampler run, cover every stratum, and stay affordable?

| check | expression | rule | known failure modes |
|---|---|---|---|
| **items judged tonight, all strata** `sampled` | `sum(jev_stratum_judged[*])` | absolute: critical if < 1000, watch if < 15000  (**pages**) | design is ~31K (3K/3K/2K/3K/20K); e_auth alone is 20K, so ~11K means pending_author_assignments was missing (the task ran outside the 22:30-05:00 UTC window) and ~0 means the notebook did not reach its metric cells (typesafe secret, Jev outage, cost-guard abort) |
| **each stratum judged at least 100 items** `stratum_present` | `jev_stratum_judged[*]` | absolute: watch if < 100  (dimensions from window) | a stratum going silent: its table had no full night (pick_night found < min rows), or the night's SQL returned nothing (column rename upstream: classified_rule, openalex_created_dt, model_response) |
| **Jev call failures as a share of items** `jev_errors` | `sum(jev_errors[*]) / sum(jev_stratum_judged[*])` | absolute: critical if > 0.2, watch if > 0.02 | 429s (token or request cap at TypeSafe), 5xx, a state over the 32K cap, an answer shape change on jev-latest (we pin jev-1.13.0) |
| **nightly spend, Jev + Opus tail (cents)** `cost` | `jev_cost_cents + opus_cost_cents` | absolute: critical if > 800, watch if > 500 | baseline ~$3 (Jev ~$0.9 + tail ~$2.2 at 150 items on FMAPI without prompt caching); a rerun resumes rather than re-paying; ACCEPTANCE bar is $8/night |
| **Opus tail judged its cap** `tail_ran` | `sum(opus_judged[*])` | absolute: watch if < 50 | FMAPI endpoint renamed / rate-limited (1M input tok/min per model per workspace), or too few flags tonight (a good night) |

## Did any rule, band, provenance, endpoint or tier get worse than its own baseline?

| check | expression | rule | known failure modes |
|---|---|---|---|
| **work-type precision by classified_rule (3-day pool) vs its 21-day baseline** `type_drift_by_rule` | `(jev_judged[a_type\|*] - jev_flagged[a_type\|*]) / jev_judged[a_type\|*]` | relative: 3/6 MAD, down only, 21d window  (3-day rolling, n ≥ 30, **pages**) | a cascade rule edit in CreateLocationsWithTypes (the dimension IS the rule name); a new source landing in a rule with a type that does not fit it; raw_type vocabulary change at a publisher. Pull the rows with the query at the top of this file. |
| **affiliation-pair precision by matcher score band (3-day pool) vs baseline** `affil_drift_by_band` | `(jev_judged[b_affil\|*] - jev_flagged[b_affil\|*]) / jev_judged[b_affil\|*]` | relative: 3/6 MAD, down only, 21d window  (3-day rolling, n ≥ 30, **pages**) | matcher model or index redeploy; institutions table churn (a merged or renamed institution makes a whole band wrong); a country-token regression like #22537 shows in the >=0.9 band |
| **title_author merge precision by the new location's provenance (3-day pool) vs baseline** `merge_drift_by_provenance` | `(jev_judged[c_merge\|*] - jev_flagged[c_merge\|*]) / jev_judged[c_merge\|*]` | relative: 3/6 MAD, down only, 21d window  (3-day rolling, n ≥ 30, **pages**) | a repository feeding generic front-matter titles (Pages de début, Bibliographie, Introduction) that join unrelated books; a merge-key normalisation change; bad_titles list not covering a language |
| **repository admission precision by endpoint_id (3-day pool) vs baseline** `repo_drift_by_endpoint` | `(jev_judged[d_repo\|*] - jev_flagged[d_repo\|*]) / jev_judged[d_repo\|*]` | relative: 3/6 MAD, down only, 21d window  (3-day rolling, n ≥ 30, **pages**) | an endpoint switching to a set_spec that carries issues, images or TOC pages; a harvester filter (raw_native_type) dropped; a new endpoint added without a kind filter |
| **authorship-seat precision by match tier (3-day pool) vs baseline** `auth_drift_by_tier` | `(jev_judged[e_auth\|*] - jev_flagged[e_auth\|*]) / jev_judged[e_auth\|*]` | relative: 3/6 MAD, down only, 21d window  (3-day rolling, n ≥ 30, **pages**) | a tier cut or block-key change in MatchAuthors; a dataset wave (IGSN, PNNL) landing in a name tier; a journal name parsed as an author. Cross-check author_matching/precision_by_tier (Opus 4.8 on 800 seats) before acting; this stratum's Jev recall is the weakest (0.60). |
| **whole-stratum precision (3-day pool) vs baseline** `stratum_drift` | `(jev_stratum_judged[*] - jev_stratum_flagged[*]) / jev_stratum_judged[*]` | relative: 3/6 MAD, down only, 21d window  (3-day rolling, n ≥ 300, **pages**) | the coarse alarm for dimensions too thin to reach n >= 30 on their own; also fires when Jev itself drifts (check self/flag_precision first) |

## Is any rule, band, provenance, endpoint or tier simply bad right now?

| check | expression | rule | known failure modes |
|---|---|---|---|
| **work-type precision by classified_rule under 0.7 (3-day pool, n >= 30)** `type_floor_by_rule` | `(jev_judged[a_type\|*] - jev_flagged[a_type\|*]) / jev_judged[a_type\|*]` | absolute: critical if < 0.7  (3-day rolling, n ≥ 30, **pages**) | probe leads (2-3 items each, so not yet measurements): cr=journal-issue -> paratext, default: raw=other -> other, K: raw ends-in book, struct: conf-abstract (suppl+single), title: standard, K: title discussion-of -> editorial. A rule that is always under 0.7 is a rules question, not a regression; accept it via baseline_overrides once judged. |
| **affiliation-pair precision by score band under 0.7 (3-day pool, n >= 30)** `affil_floor_by_band` | `(jev_judged[b_affil\|*] - jev_flagged[b_affil\|*]) / jev_judged[b_affil\|*]` | absolute: critical if < 0.7  (3-day rolling, n ≥ 30, **pages**) | <0.1 band ~0.13 and 0.1-0.3 ~0.3 are the standing fallback bug (#1309); 0.3-0.5 ~0.6; >=0.9 ~0.95. Anything under 0.7 in the >=0.5 bands is new. |
| **title_author merge precision by provenance under 0.7 (3-day pool, n >= 30)** `merge_floor_by_provenance` | `(jev_judged[c_merge\|*] - jev_flagged[c_merge\|*]) / jev_judged[c_merge\|*]` | absolute: critical if < 0.7  (3-day rolling, n ≥ 30, **pages**) | baseline ~0.97; a provenance under 0.7 means a feed of generic titles is being merged wholesale |
| **repository admission precision by endpoint under 0.7 (3-day pool, n >= 30)** `repo_floor_by_endpoint` | `(jev_judged[d_repo\|*] - jev_flagged[d_repo\|*]) / jev_judged[d_repo\|*]` | absolute: critical if < 0.7  (3-day rolling, n ≥ 30, **pages**) | an endpoint shipping issues, images or front matter as works (Cairn.info front matter, Internet Archive photos); fix-repositories / bad_titles owns the fix |
| **authorship-seat precision by tier under 0.7 (3-day pool, n >= 30)** `auth_floor_by_tier` | `(jev_judged[e_auth\|*] - jev_flagged[e_auth\|*]) / jev_judged[e_auth\|*]` | absolute: critical if < 0.7  (3-day rolling, n ≥ 30, **pages**) | baseline ~0.96 all tiers; author_matching/precision_by_tier (Opus 4.8) is the authoritative line, this one is the cheap early read on 25x more seats |

## Is Jev still a trustworthy flagger?

| check | expression | rule | known failure modes |
|---|---|---|---|
| **share of Jev flags Opus 5 confirms, per stratum (7-day pool)** `flag_precision` | `opus_confirmed[*] / opus_judged[*]` | absolute: critical if < 0.4, watch if < 0.6  (7-day rolling, n ≥ 20) | probe: a_type 0.75, c_merge 0.56 (7 positives), d_repo 0.83, e_auth 0.67 at the cuts. A drop means Jev's flags became false alarms (state shape change, jev model drift, a new record family Jev reads literally); raise the stratum cut or fix the state before trusting drift findings. |
| **flag precision per stratum vs its baseline** `flag_precision_drift` | `opus_confirmed[*] / opus_judged[*]` | relative: 3/6 MAD, down only, 21d window  (7-day rolling, n ≥ 20, max watch) | informational; the absolute line above is the one that acts |

## Known states (true, uninteresting, not findings)

- b_affil band "<0.1" sat near 0.13 precision in the probe: institution_batch_inference.ipynb kept the matcher's top guess when nothing cleared score 0.1 (27% of new strings/day, ~90% wrong). The fix (oxjob #1309, emit -1 below the cut) landed on main 2026-09-21; the sampler skips -1 ids, so the band should thin to near zero. If it is still large and under 0.7 after that, the fix did not take. That is the finding, not noise.
- d_repo: one bulk load (Cairn.info, 2026-09-21) was 98% of a night's admissions; the sample is round-robin per endpoint so the endpoint dimension names the offender rather than hiding it.
- a_type: several low-precision rules are within-taxonomy boundary calls where work_types descriptions themselves disagree (journal-issue paratext vs other; software vs dataset). A rule that has always been low is a rules-cascade question for #1277's "model writes rules" line, not a nightly regression; the drift check is the regression alarm.

## Open decisions

1. Per-dimension floors use 0.7 on a 3-day pool with n >= 30 (the #1301 decision). Rules with fewer than 10 items/night never reach n >= 30 in 3 days and are covered only by the stratum-level drift check; widen the rolling window if those matter.
2. Opus tail confirms Jev's flags (flag precision), not the pipeline's precision; precision_opus_adj in jev_quality_daily scales the flag count by that share. No check reads it yet.
