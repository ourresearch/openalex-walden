# AwardTopics (topics, keywords, SDGs on awards): the 10k sample run

Topics, keywords and SDGs on awards, as works get them: the existing unscheduled job **AwardTopics** (`jobs/award_topics.yaml`)
now runs this folder's four notebooks as its tasks (`build_queue` -> `topics` / `keywords` /
`sdgs`). It stays beside Nightly Grants, not a step inside it, and writes side tables: `award_topics_v2`, and beside it the new
`award_keywords` and `award_sdgs` (dev names `openalex_dev.rohan_lab.awt_award_*`). Nightly keeps reading `openalex.awards.award_topics`
(`inputs.topics`) as today; `award_topics_v2` has that table's columns (+ `not_classifiable`, `text_key`), so a later switch is a
config change. **That Nightly change is not in this branch.**

**The old BERT tasks are replaced, not kept behind a switch.** Keeping them "off by default" would need a `condition_task` gate in
front of three tasks that run on DEV-WH (our never-use list) and a g5.12xlarge GPU cluster, for a model q8b_2m supersedes, in a job
that had no run in the 60 days to 11-05. Replacing is one task list; nothing is deleted: the BERT notebooks stay in
`notebooks/topics/award_topics_*.ipynb`, their table `openalex.awards.award_topics` is untouched (no task here writes it), and the
rollback is the previous version of `jobs/award_topics.yaml`. The resource key `AwardTopics` is kept, so a bundle deploy updates
job 647173588911801 in place instead of creating a new job (per jobs-cleanup 11-05, the 05-19 copy 705716963971453 is a leftover the bundle does not manage).

| File | Does |
|---|---|
| `jobs/award_topics.yaml` | job AwardTopics: the four tasks below, dev defaults, `dry_run=true`, no schedule |
| `BuildAwardQueue.py` | usable awards not yet in all three side tables, or whose text changed (`text_key`) -> `<prefix>queue` + the Modal copy |
| `ScoreAwardTopics.py` | Jason's `openalex-topics-nightly` (q8b_2m, H100) on the queue -> ledger -> MergeTopicsV2's builder -> `award_topics_v2` |
| `ScoreAwardKeywords.py` | Jason's `oxjob1322-keywords-nightly` (student_qwen4b_v3fix, B200) -> the shipped closed-vocabulary SQL -> `award_keywords` |
| `ScoreAwardSdgs.py` | `ai_query('databricks-qwen3-embedding-0-6b')` + the works head `sdg_jev_head_v2` (0.4), 250k-award passes, 4 at once, staged -> `award_sdgs` |
| `lib/award_text.py` | the usable rule (+ CJK by characters, placeholders, person feeds), the scorer texts, `text_key` |
| `text_rules.csv` | reviewed data the rule reads: 31 placeholder texts, 56 person / fellowship feeds, each with its count and evidence |
| `lib/modal_call.py` | Jason's POST / poll / call-id-file loop and spend formulas; checks a request against his app sources' rules |
| `lib/sdg_head.py` | the works head file and math (read from `notebooks/sdg/`, not copied) |
| `sample/` | `sample_run.py` (one-off runs on a fixed id list: `sample` = the 10k, `tiny` = 100 awards) + the tiny ids (+ sha256) |
| `tests/test_enrich.py` | offline: rule, requests vs his apps, estimates, head, keyword-template swap, Modal retry, the job file, the approved-warehouse rule, the upload list (`python3 -m pytest -q notebooks/awards/enrich/tests`) |

## Usable text (the queue rule)

An award is queued when its **cleaned** text has a title >= 4 words or a description >= 20 words; CJK text (Han + kana >= 30% of
the non-space characters) counts characters instead, >= 8 in the title or >= 40 in the description. Cleaning, from `text_rules.csv`:
- **placeholders** -> empty: a title or description whose normalised text (lower case, spaces collapsed, trailing dots dropped) is
  listed ("No description available", "n/a", "none", "-", "TBD", NSERC's "No summary - Aucun sommaire" 147k, "No title - Aucun
  titre" 42k, NEH's "No to support statement — no description" 24k, GEPRIS's "Keine Zusammenfassung vorhanden" 17k, ...) or is
  punctuation only. A placeholder never qualifies an award and is never sent as the abstract (175 of the 10k sample were).
- **person feeds** -> title empty: 56 feeds whose titles label a person or a fellowship, not research ("Guggenheim Fellowship -
  <name> (<year>)", "SNII Investigador Nivel 2 — <name>", "Humboldt Research Fellowship (<month year>)"). Their awards are queued
  only when the description qualifies on its own, and the models see the description alone. Rule A (55 feeds, measured):
  >= 90% of a feed's usable titles hold the PI's family name and, without it, collapse to <= 15% distinct labels (so LCRF's
  "<name> - <project title>" is not caught). Rule B (reviewed, 1 feed): Humboldt, 50 fellowship labels over 28k titles.
- Whitespace is Unicode in both the SQL (`(?U)`) and its Python twin, so NBSP-padded placeholders are caught too.
- Effect, `awards_api` 2026-11-09 (feeds >= 30 awards; measured before the `(?U)` change, which only moves NBSP cases): 6,849,724 usable -> 6,807,446 after placeholders (-42,278) -> **6,675,591**
  after person feeds (-131,855): **-174,133**. On the 10k sample: 416 awards drop, holding 324 of the 695 not-classifiable; the
  not-classifiable share of the rest is 3.9% (was 7.0%).
- Not applied (programme-label titles, not people): HRSA "Health Center Program (H80)" 71,891, AEI "Orden de ... convocatoria"
  28,835 (descriptions qualify), Garfield Weston "Main grants award" 3,321, RPB 1,164.

## Runs

```bash
cd <a checkout of this branch>
python3 notebooks/awards/enrich/sample/sample_run.py check tiny        # local dry run: no network, no token, no SQL, no Modal
AWT_OK='<who> <date>' AWT_WAREHOUSE=8a9e2f371f8c265f python3 notebooks/awards/enrich/sample/sample_run.py go tiny
```
`go` re-runs `check` and stops on any blocking gate; then copies this folder, `utils/`, `notebooks/sdg/` and the two app sources to
`/Workspace/Users/<me>/awt-sample/<commit>/`, submits **one** run (`databricks jobs submit`, serverless, `dry_run=false`, the ids
via `force_ids_file`) and polls it. No job is created and nothing is scheduled. The run spec goes to the temp folder.
- `sample`: the 10k (run 831985739633851, 2026-11-09 07:10-07:15 PT, all four tasks green, DEV-WH with Rohan's OK). Its ids stay
  on branch `awards-topics-sample`.
- `tiny`: 100 awards (15 CJK, 88 feeds, all passing the rule above) through the two Modal apps only (no SDGs), own tables
  `openalex_dev.rohan_lab.awt_tiny_*` and queue copy `openalex.transient.awt_tiny_queue`: a test that the apps' identity can
  export through a warehouse (from 11-10, #2062, one-offs can't use DEV-WH; `shared-2xl` 8a9e2f371f8c265f: serverless PRO 2X-Large,
  1 cluster, auto-stop 1 min, ~$0.75 a minute while up). A refusal surfaces as the topics task's "Modal call ... failed:
  statement not accepted ... PERMISSION_DENIED"-style error, which is what to ask Jason about.
  Cost: Modal ~$1 (one H100 + one B200 start; $0.45 + $0.50 by his guards), shared-2xl ~$1.5-3 (two short statements, each
  holding the warehouse up ~1-2 min incl. the 1-min auto-stop; ~$0 if it is already up), serverless tasks ~$0.3: **~$3-4**.
Every notebook defaults to `dry_run=true`: a dry run writes no table (`build_queue` hands its counts to the other tasks as task
values) and the topics / keywords tasks read no secret.

## Gates

1. **Jason's or Rohan's direct OK** (`AWT_OK`): his apps, his bearer tokens (secret scope `modal`), his GPUs.
2. **A warehouse for the apps' queue export** (`AWT_WAREHOUSE`, no default). A warehouse on the grants-hub never-use list (DEV-WH)
   also needs `AWT_WAREHOUSE_OK=<same id>` (passed to the notebooks as `approved_warehouse`, that exact id only).
3. **Where the apps read and write**: `TABLE_RX` admits only `openalex.(works|transient).*`, `OUT_RX` only `/Volumes/openalex/...`.
   So the queue copy is `openalex.transient.awt*_queue` and the shard files go to `awards/<build>/` under his two Volumes; tables
   are dev unless the go-line says otherwise.
4. **Permissions** of the identity that runs it (a one-off: the submitter; the job: its run_as service principal): read scope
   `modal`, CREATE TABLE in `openalex.transient` and the target schema, READ/WRITE VOLUME on the two paths; the apps'
   `oxjob-databricks` identity: SELECT on the queue copy and CAN USE on the warehouse.
5. **GPU slot** clear of his nightlies (Embed 07:00 PT, Keywords 10:00 PT, Topics inside End 2 End from 21:00 PT) and the
   affiliations Backlog (08:30 PT, up to 4 h).
6. **Spend caps**: `topics_max_usd` / `keywords_max_usd` / `sdgs_max_usd` (his guard formulas; refuse before any GPU starts).

## Measured (2026-11-08/09)

- **Sizing** (`awards_api`, 15.2M awards): see "Usable text" above. CJK titles 708,933 (KAKEN 345,201, NSFC 230,784, ...).
- **The 10k sample** (sqrt-stratified by feed, 423 feeds, 1,350 CJK titles): run 831985739633851, 4 min 19 s end to end.
  Topics: 1 shard, 29 shard-s on one H100 (345 awards / s), ~$0.04 + one model load, wall 130 s; 6.95% not classifiable
  (6.9% of the 6,272 with a description). Keywords: 22 shard-s on one B200 (455 / s), ~$0.04 + one start, wall 105 s; 96.8% with >= 1
  keyword, 5.82 per award. SDGs: 37 s; 58.8% with >= 1 goal (head parity with served works SDGs: 24 / 24).

## Backfill (after the push and its bundle deploy)

```bash
# 1. dry run: counts, shards, estimates, request checks; writes nothing, reads no secret
databricks jobs run-now 647173588911801 -p dbc-ce570f73-0362 --json '{"job_parameters": {"dry_run": "true", "warehouse_id": "8a9e2f371f8c265f",
  "state_prefix": "openalex.transient.awt_", "modal_queue_table": "openalex.transient.awt_queue", "max_awards": "7000000",
  "topics_target": "openalex.awards.award_topics_v2", "keywords_target": "openalex.awards.award_keywords", "sdgs_target": "openalex.awards.award_sdgs",
  "topics_max_usd": "45", "keywords_max_usd": "110", "sdgs_max_usd": "30"}}'
# 2. the run: the same with "dry_run": "false"
```
- Rows: **~6.68M** awards queued (268 shards of 25,000), one row each in `award_topics_v2`, `award_keywords` and `award_sdgs`
  (new tables in `openalex.awards`; nothing Nightly reads changes; `openalex.awards.award_topics` is not touched).
- Cost at the sample's measured rates: topics ~$28 GPU + ~$2-4 model loads = **~$30-32**; keywords ~$29 + ~$2-5 starts =
  **~$31-34**; SDGs ~$23 endpoint; shared-2xl ~$25-30 (one export ~5 min + ~30 min of per-shard keyword reads at ~$0.75 / min);
  serverless tasks ~$10-20. **~$120-140 in all.** Guards: topics 45 (his formula: $40.3), keywords 110 (his formula: $105.1;
  measured ~$33), sdgs 30 ($22.7).
- Wall: build_queue ~5 min, then side by side: topics ~55-65 min (export ~10-20 min + 19,350 shard-s / 8 H100), keywords
  ~35-45 min (14,670 shard-s / 10 B200 + normalising), SDGs ~45-50 min (27 passes of 250k, 4 at once, ~2,400 / s):
  **~1 h 10 min end to end.**
- Slot: start **13:30 PT** on a weekday (after Keywords nightly 10:00 PT and the Backlog's 4-h window from 08:30 PT; done by
  ~14:45 PT, long before End 2 End's Topics from 21:00 PT and the 07:00 PT Embed on the same embedding endpoint). Before go:
  `databricks jobs list-runs --active-only -p dbc-ce570f73-0362` shows no Backlog / Keywords / Embed run.
- Cleanup after a good run: `DROP TABLE openalex.transient.awt_queue` and the `openalex.transient.awt_*` scratch.

## Open questions (not decided here)

- **Programme-label titles** (HRSA, AEI, Garfield Weston, RPB above) still pass; they are programmes, not people.
- **Venue**: awards go in with no venue ("(unknown)"); passing the funder name instead is untested.
- **Keyword senses**: the Jev homonym step (oxjob #1476) is not run for awards; a split keyword keeps its main sense.
- **Nightly + ES**: when Nightly reads these tables, every award that gains a value re-dates once in ES (plan in the Jason ask).
