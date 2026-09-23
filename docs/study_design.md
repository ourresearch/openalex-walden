# Study design (oxjob #1312)

`study_design` says how the research inside a work was done; `type` says what
kind of document it is. An editorial has no study design; an RCT preprint has
one; a systematic review is `type: review` with `study_design: systematic-review`.

## Values

Eight values, multi-valued with implied parents (RCT ⇒ Clinical Trial;
Meta-Analysis ⇒ Systematic Review). They are the MeSH V03 "Study
Characteristics" half of PubMed's `PublicationType`; the V02 "Publication
Formats" half (Editorial, Letter, Review, Guideline …) already lives in `type`.

| value id | PubMed tags mapped in | tagger class |
|---|---|---|
| `randomized-controlled-trial` | Randomized Controlled Trial (+ Veterinary), Pragmatic Clinical Trial, Equivalence Trial, Adaptive Clinical Trial | `rct` |
| `clinical-trial` | Clinical Trial, Controlled Clinical Trial, Clinical Trial Phase I–IV, Clinical Study, Clinical Trial Veterinary | `nonrandomized_trial` or `rct` |
| `observational-study` | Observational Study (+ Veterinary), Twin Study | `observational` |
| `case-report` | Case Reports | `case_report` |
| `systematic-review` | Systematic Review | `systematic_review` or `meta_analysis` |
| `meta-analysis` | Meta-Analysis, Network Meta-Analysis | `meta_analysis` |
| `study-protocol` | Clinical Trial Protocol | `protocol` |
| `other-primary-research` | none (OpenAlex-only) | `other_primary_research` |

Out: Scoping Review, the modifier tags (Comparative / Multicenter / Evaluation /
Validation Study), funding tags, and "Evidence Synthesis" (54 records, a
grouping term).

## Provenance

Where a **MEDLINE-indexed** PubMed record (`openalex.pubmed.pubmed_exploded`,
latest revision per PMID, `MedlineCitation._Status = 'MEDLINE'`) carries one of
the tags above, PubMed's values are served. Everywhere else the tagger's values
are served. The tagger runs on PubMed works too, so `works_study_design` holds
both columns and disagreement is measurable. There is no provenance field in
the API; the tagger is described as "automated tagging", never by name.

## Tagger

`utils/study_design.py` is the single source: one Jev request per work
(`jev-1.13.0`, pinned) over `rule_design + title + venue + abstract[:6000]`,
a 13-option Choice plus two one-clause Nouls (`is_rct`, `human_subjects`);
eight derived scores (`derive()`), three code-side text gates on RCT
(stated-random regex in ten languages, simulation/manikin title,
secondary-analysis title), per-class thresholds fixed on the dev split,
parents expanded. ~1,500 input tokens per work, $0.00006 at Jev's rate.

Certified 2026-09-22 on 5,069 Opus-judged works under the schema's own rubric
(oxjob #1312 EXPLORE § 5b; `scratch/prod_check.py` reproduces it from this
module): RCT 1.000 precision (0 FP in 555) at 0.83 recall, one-sided 95% lower
bound 0.995; every class clears its bar (RCT ≥ 0.99, MA ≥ 0.98, SR and Protocol
≥ 0.97, the rest ≥ 0.95). `tagger_version = r2_gate4/jev-1.13.0` is the
checkpoint key: change the request, the gates, the thresholds or the Jev
snapshot and you must bump it (which re-queues everything) and re-certify.

Rules learned the hard way: never batch works into one request (7% of answers
flip at N = 8); never add a Noul without re-certifying (four times an extra
gating Noul perturbed every other answer); keep Nouls to one clause (long ones
cost 10–16 points of RCT recall).

## Tables (all `openalex.works`)

| table | role |
|---|---|
| `works_study_design_queue` | tagger inputs, rebuilt every run by `BuildStudyDesignQueue`; partitioned by `chunk_id` (priority × 1e6 + hash bucket) |
| `works_study_design_tagger` | append-only tagger output: `tagger_values`, `scores`, `probabilities`, `is_rct`, `human_subjects`, gates, tokens, `tagger_version`; latest `updated_at` per work wins |
| `works_study_design_errors` | failed Jev calls (the work stays queued) |
| `works_study_design_progress` | finished chunks per queue build; a retried run skips them |
| `works_study_design` | **served**: `work_id`, `study_designs[]`, `source` (pubmed / tagger), `pubmed_values[]`, `tagger_values[]`; full rebuild by `BuildStudyDesignServed` |

`works_study_design` is not yet merged into `openalex_works` or ES (oxjob #1312
step 6). Until that lands nothing downstream reads it, so this job never
re-stamps works and cannot trip Guardrails.

## Job `Study Design` (jobs/study_design.yaml)

Nightly 19:00 UTC (after End 2 End): build_queue → tag → build_served, on
serverless. Job parameters cap each run: `max_works` (400K), `max_usd` ($40),
`max_minutes` (150), `rps` (250 of the 400 req/s account cap), `concurrency`
(64), `dry_run`. Queue priority: works created in the last 30 days, then works
with a PMID, then no-PMID works from 2000 on, then the rest; so the nightly
stream is always tagged first and the backfill drains behind it.

**Backfill** (~155M no-PMID + ~24M PMID works with abstracts, ≈ $11K, approved
2026-09-22): run the same job by hand with big caps, e.g.

```
databricks jobs run-now --json '{"job_id": 552446330684613, "job_parameters": {"max_works": "30000000", "max_usd": "1800", "max_minutes": "1380", "rps": "300"}}'
```

At 300 req/s a 23-hour run tags ~25M works (~$1.5K); the whole corpus is about
a week of such runs. With `max_concurrent_runs: 1` the 19:00 nightly queues
behind a running backfill and starts when it ends. A run that dies resumes:
finished chunks are in `works_study_design_progress`, and the first unfinished
chunk is anti-joined against the tagger table before any Jev call.

Health checks after a run: the tagger task's last line (`run done: … tagged, …
failed`), `works_study_design_errors` for the run's `build_id`, and the
PubMed-vs-tagger table `build_served` displays (`both` / `pubmed_only` /
`tagger_only` per value). Expected: PubMed's RCT tag is only ~0.63 precise
against the judge (secondary analyses, sub-studies), so `pubmed_only` on
`randomized-controlled-trial` is mostly PubMed's over-tagging, not the tagger
missing trials.
