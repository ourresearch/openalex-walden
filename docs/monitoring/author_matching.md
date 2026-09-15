# Monitoring — Author matching

<!-- GENERATED from monitoring/checks/author_matching.yaml by scripts/monitoring_render_docs.py — edit the YAML -->

Component `author_matching`. Metrics: `openalex.monitoring.metrics`. Owner: casey. Evaluated nightly by `notebooks/monitoring/MonitoringFindings`; findings in `openalex.monitoring.findings`; the morning report in `openalex.monitoring.reports`.

> Every writer (MatchAuthors, AuthorshipDailyMetrics, the judge) emits to openalex.monitoring.metrics; history before 2026-09-15 was copied in from the retired openalex.authors.authorship_daily_metrics (oxjob 640). Metrics born 2026-09-13..16 (stage_*, e2e_runs_on_date, block_skew, seats_by_source, batch_seats, author_gain_top, new_works_seats) have absolute rules only until they carry 7 nights of history.

## How to read this

Each heading is the question we care about. Each row is one check the engine runs. **absolute** rules are fixed lines (critical on day one). **relative** rules compare today to the trailing median in units of MAD: watch at the first number, critical at the second, or at the first number two nights running. Status is deterministic; the LLM only narrates.

## Did the matcher run, finish, and finish in a reasonable time?

| check | expression | rule | known failure modes |
|---|---|---|---|
| **every authorship stage succeeded** `stage_success` | `stage_success[*]` | absolute: critical if < 1  (**pages**) | end2end task failure; bundle-lock deploy race; Guardrails block upstream |
| **exactly one end2end run today** `e2e_runs` | `e2e_runs_on_date` | absolute: critical if < 1, watch if > 1  (**pages**) | manual rerun mid-day (07-27 double-judge); >1 means today's metrics cover a residual window only |
| **MatchAuthors wall time** `match_wall` | `stage_wall_sec[Author_Matching]` | absolute: critical if > 10800; relative: 3/6 MAD, up only, 14d window  (**pages**) | PNNL/EMSL block skew put MatchAuthors at 2h (dedupe shipped 09-13) |
| **MatchAuthors emitted its run metrics tonight** `matcher_emitted` | `batch_seats` | absolute: critical if < 1  (**pages**) | the metric cells at the end of MatchAuthors did not run (task failed before them, or the monitoring schema is missing) |

## Are we matching the seats we should be matching?

| check | expression | rule | known failure modes |
|---|---|---|---|
| **share of seats bound on works created 8..1 days ago (the user-visible outcome)** `bound_share_new_works` | `new_works_seats[bound] / new_works_seats[total]` | relative: 3/6 MAD, down only, 14d window  (n ≥ 1000) | matcher skipping new works; eligibility gate (work_id > 7e9 / created ≥ 2025-12-20) drift; a source flooding with unmatched org names |
| **matched share of the batch** `match_rate` | `match_outcome[MATCHED] / sum(match_outcome[*])` | relative: 3/6 MAD, 14d window | a tier cut (n8, 07-28) shifts the mix legitimately — expect a step, then accept the new baseline |
| **ambiguous share of the batch** `ambiguous_share` | `match_outcome[AMBIGUOUS] / sum(match_outcome[*])` | relative: 3/6 MAD, 14d window | dataset-record waves inflate AMBIGUOUS (baseline ~40%) |
| **no-candidate share of the batch** `no_candidates_share` | `match_outcome[NO_CANDIDATES] / sum(match_outcome[*])` | relative: 3/6 MAD, 14d window | block-key regression → everything looks new |
| **share of matches by tier** `tier_mix` | `match_tier[*] / match_outcome[MATCHED]` | relative: 3/6 MAD, 14d window  (max watch) | a tier that jumps is usually a wave landing in it (s5_n2, 07-29) |
| **ORCID tier still firing** `orcid_tier` | `match_tier[orcid]` | relative: 3/6 MAD, down only, 14d window | ORCID column dropped upstream; publisher ORCID feed change |
| **match-eligible null-author backlog, day-over-day** `eligible_backlog_delta` | `null_reservoir[seats_match_eligible]` | absolute: critical if > 500000, watch if > 100000  (day-over-day delta, **pages**) | 07-22 +185K watch item; reservoir should trend flat-to-down |

## Are the matches we make correct?

| check | expression | rule | known failure modes |
|---|---|---|---|
| **judged precision by tier (7-day rolling)** `precision_by_tier` | `judge_arm_a[*\|same_person] / (judge_arm_a[*\|same_person] + judge_arm_a[*\|different_person])` | absolute: critical if < 0.7, watch if < 0.8  (7-day rolling, n ≥ 30, **pages**) | n8 tiers at 6–26% (cut 07-28); s5_n2 at 28–33% on wave nights = contamination, not the tier |
| **ORCID-tier judged precision (7-day rolling) — 63% of all matches** `precision_orcid` | `judge_arm_a[orcid\|same_person] / (judge_arm_a[orcid\|same_person] + judge_arm_a[orcid\|different_person])` | absolute: critical if < 0.8, watch if < 0.9  (7-day rolling, n ≥ 30, **pages**) | PNNL staff ORCIDs on data-deposit records (36/78 all-time FPs from S7407051155) — exclude that source before trusting a dip |
| **ORCID sanity counters** `orcid_qa` | `orcid_qa[*]` | relative: 3/6 MAD, 14d window  (max watch) | a publisher shipping wrong ORCIDs shows as a name_conflict spike |
| **ORCID mint collisions** `orcid_mint_collisions` | `orcid_mint_collisions` | absolute: critical if > 500, watch if > 100 | two profiles minted for one ORCID in one night |
| **incompatible name changes on bound seats** `name_change_incompatible` | `name_change_quality[works_incompatible] / name_change_quality[works_judged]` | relative: 3/6 MAD, up only, 14d window | 07-21 — 1,250 incompatible vs 3,018 cosmetic; the real rematch workload |
| **foreign-family assignments tonight, all tiers (seat name cannot be the profile it was bound to)** `impossible_name_total` | `sum(assign_name_check[foreign_family\|*])` | absolute: critical if > 1000, watch if > 500  (**pages**) | 09-13 — 454 over 509K judged seats (26 name-tier / 428 ORCID). ORCID matches have NO name gate; this is their only one. A name-tier hit means the profile's display name drifted since the match. |
| **one profile absorbing many impossible names** `absorber` | `assign_name_check_top[*]` | absolute: critical if > 25  (**pages**) | 09-13 — "Naruki Hiranuma" (A5005965575) absorbed every "Moon, Seong-gi" seat from PNNL via a shared ORCID: one collision, dozens of seats. The absorber pattern built #608's 9.9M misbound rows. |
| **most seats bound to one existing author tonight (absorber or hyperauthor, name-agnostic)** `author_gain_max` | `author_gain_top[*]` | absolute: critical if > 25000, watch if > 5000  (**pages**) | an org pseudo-author or a shared ORCID absorbing a roster; a hyperauthorship paper adds 1 seat per author, not thousands to one |

## Are we minting new authors only when we should?

| check | expression | rule | known failure modes |
|---|---|---|---|
| **mints per batch seat** `mint_ratio` | `new_authors_minted / sum(match_outcome[*])` | relative: 3/6 MAD, 14d window | raw mint count spikes with batch size (oxjob 682 reparse, oxjob 649 admissions); ratio stable 21–29% even on 235K-mint days |
| **share of mints carrying an ORCID** `mint_with_orcid` | `new_authors_minted[with_orcid] / new_authors_minted` | relative: 3/6 MAD, 14d window | ORCID loss upstream shows here first |
| **share of mints from AMBIGUOUS (the splinter-risk pool)** `mint_from_ambiguous` | `new_authors_minted[from_ambiguous] / new_authors_minted` | relative: 3/6 MAD, 14d window | baseline ~78% |
| **judged splinter rate (arm B candidate pick, 7-day rolling)** `splinter_rate` | `judge_arm_b[candidate_pick] / (judge_arm_b[candidate_pick] + judge_arm_b[none_correct] + judge_arm_b[cannot_determine])` | absolute: watch if > 0.6  (7-day rolling, n ≥ 30) | an existing candidate would have been the right answer (baseline 45–60%); supports the oxjob 453 coauthor tier |

## Are we being flooded by non-human or organisational "authors"?

| check | expression | rule | known failure modes |
|---|---|---|---|
| **single raw name over the wave line (named)** `hot_name_max` | `name_concentration_top[*]` | absolute: critical if > 100000, watch if > 25000  (**pages**) | Geoscience Australia 43K/night, Scan-the-World, GBIF.org User, Atlas of Living Australia ×5 |
| **largest block, in join rows** `block_skew` | `block_skew[max_block_rows_seats]` | absolute: critical if > 1e+09; relative: 3/6 MAD, up only, 14d window  (**pages**) | `y wang` block 2.5B rows on one task; PNNL/EMSL staff roster |
| **one source's share of the batch (top 5, named)** `source_share` | `seats_by_source[*] / sum(match_outcome[*])` | absolute: watch if > 0.3 | S7407051155 (DOE PNNL) mints 20–24K dataset works/day; S7407053303 IGSN samples |

## Are existing authorships stable?

| check | expression | rule | known failure modes |
|---|---|---|---|
| **author-list churn by class** `churn_by_class` | `author_list_changes[*]` | relative: 3/6 MAD, 14d window  (max watch) | baselines — grows 150–9K/day, shrinks 250–3.6K, name changes 2K–12K; a source reparse (T&F, #682) is a NAMES_CHANGED step; mass-touch days hit 30M+ works on updated_date but the content diff stays small |
| **stale-seat backlog, day-over-day** `stale_backlog_delta` | `stale_seats[rows]` | absolute: critical if > 1e+06, watch if > 100000  (day-over-day delta, **pages**) | no MERGE deletes seats — the backlog only shrinks via explicit work (baseline 5.29M rows / 1.79M works) |
| **fingerprint never shrinks** `fingerprint_coverage` | `fingerprint_works_tracked` | absolute: critical if < 0  (day-over-day delta, **pages**) | fingerprint table truncated / bootstrap rerun |

## Is the monitor's own machinery healthy and affordable?

| check | expression | rule | known failure modes |
|---|---|---|---|
| **judge ran and sampled enough (arm A verdicts)** `judge_sample` | `sum(judge_arm_a[*])` | absolute: watch if < 300 | temperature bug on cluster-side ai_query (07-26); endpoint outage; dedupe by (name, author_id) starving a tier |
| **judge cost (cents)** `judge_cost` | `judge_cost_cents` | absolute: critical if > 5000, watch if > 1000 | double pass (07-26/27); ORCID arms at 100/night ≈ +$1; baseline ~320 |
| **observer collected everything** `collector_errors` | `sum(collector_error[*])` | absolute: watch if > 0 | Jobs API / query-history permission changes |
| **assignment log rows ≈ batch seats** `assignment_log_complete` | `assignment_log_rows / sum(match_outcome[*])` | absolute: watch if < 0.99, watch if > 1.01 | 120-day retention delete misfiring |

## Known states (true, uninteresting, not findings)

- The n3 / n4 / n5 tier families have never fired in ~6.3M tier-attributed matches. Unreachable or a bug — separate job. Not a Monitoring finding.
- s5_n2 precision collapses on dataset-wave nights (IGSN / Geoscience Australia). The tier is healthy on human names; the wave is the problem. See section 5.

## Open decisions

1. Precision thresholds (80/70; 90/80 for ORCID) come from the healthy-tier band in the oxjob 640 history, not from a product target. Replace with the stated author-precision goal if one exists.
2. Precision against a reference: the AER gold set (oxjobs/archived/aer-gold-standard) is author-level JSONL, not a seat-level table, so a "rebound gold seats scored correct" check needs that table built first. Until then precision is judge-only (±3pp on a 7-day window).
3. Which items page — draft: the run section, precision, impossible names, the absorber and author-gain lines, waves, the reservoir/stale/fingerprint absolutes. Observation-only for the first 14 nights.
