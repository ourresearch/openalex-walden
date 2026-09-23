# Runbook: publish a bulk works change overnight with a Guardrails override

Use this when a walden change will re-stamp more than ~5M `openalex_works` rows in one End 2 End
run (Guardrails Check 1 fails at **7,500,000** `records_changed_by_run`), and you want the wave
published overnight without anyone rerunning the job by hand in the morning. Since 2026-09-23 the
normal way is a pre-cleared row that the scheduled run reads (section 2); the manual run with the
parameter is the fallback (section 3b).

## 0. Rules that do not bend

- The override is applied **only with Jason's explicit yes for this specific run**. A standing
  approval, "go ahead", or last week's similar case does not count. Never ask Casey to apply it.
- Never change the job-level parameter defaults (`guardrails_override` etc. must stay `"false"`).
  The override is a run parameter on one `run-now`, or a row in `openalex.works.e2e_overrides`
  whose window covers exactly one nightly.
- Bypass only the class that will fire: `guardrails_override` (quality gates incl. Check 1),
  `deleted_works_guard_override`, `deleted_locations_guard_override`, `wunpaywall_guard_override`.
- If the blast radius is genuinely unknown, do not override. Let the scheduled run trip, read
  `records_changed_by_run` and the diff in the morning, then decide.

## 1. Size it (before merging)

Anything feeding `CreateWorkAuthorships` / `CreateWorksEnriched` fans out: institution ancestors,
author ids and **author display_name** (embedded in `authorships[].author.display_name`, which is
hashed), affiliations, topics, locations, types. Count works, not entities:

```sql
-- example: works touched by a set of changed authors
SELECT COUNT(DISTINCT work_id) FROM openalex.authors.author_works_staging
WHERE author_id IN (SELECT id FROM <changed_authors>);
```

Under ~5M: ship normally. 5M to 7.5M: ship, expect a pass, check in the morning. Over 7.5M: this
runbook, or the ramp in section 6. Over ~10M also drops ES replicas for the works sync (3h38m for
52M docs on 2026-09-22 vs ~20 min normally); say so in #dev.

## 2. Pre-clear the scheduled run (the normal path)

The nightly runs at its usual time and clears its own gate. No manual run, no pause, nothing to
cancel, no collision with the schedule, and a bundle deploy cannot undo it.

How it works: every override gate in `walden_end2end` (Guardrails, the curation-decline gate in
SyncWorkAuthorCurations, the disappearance gate in CreateWorkAuthors, the row-count gate in
BuildLakebaseWorksDocs, TrackDeleted* / delete_*, the Wunpaywall feed caps) resolves its flag
through `openalex.works.e2e_override_active(flag, param)`: true when the job parameter is
`"true"` OR `openalex.works.e2e_overrides` holds a row for that flag whose
`[valid_from, valid_until)` window covers the moment the gate runs. `scripts/preclear_e2e.py`
writes that row with a window from now to 18 h after the next 05:00 UTC start, so it covers the
nightly and a same-morning repair run, and ends before the following nightly. The Guardrails
task prints the row's reason and requester, and stamps `note = 'override:table'` in
`guardrails_history`.

Evening sequence (all times UTC; nightly = 05:00 UTC = 00:00 CDT):

1. **Land the change upstream** if it lives in a separate job (Authors, 63282467302934, ~25 min;
   scheduled 12:36 UTC, which is *after* the nightly reads it, so hand-run it now):
   ```
   databricks jobs run-now 63282467302934 --no-wait --output json
   ```
2. **Verify the upstream build.** Count what changed and spot-check ten rows. For #1297:
   `COUNT(*) ... WHERE updated_date >= '<today>'` on `openalex_authors`.
3. **Tell Casey in #dev**: what change, how many works, pre-cleared on purpose, replicas dropped
   if over ~10M, no action needed.
4. **Pre-clear, after Jason's yes for this run** (from desk; `databricks` CLI authenticated):
   ```
   scripts/preclear_e2e.py --reason "oxjob #1309 fallback cleanup, 10.9M works" --by jason
   scripts/preclear_e2e.py --flags guardrails_override,deleted_works_guard_override --reason ... --by ...
   scripts/preclear_e2e.py --list      # open windows
   scripts/preclear_e2e.py --cancel    # close every open window (changed your mind)
   ```
   Default target is the next 05:00 UTC; `--night 2026-09-24` (the CT evening date) targets a
   later one. Only the flag class that will fire; `guardrails_override` never authorises a mass
   delete or an oversized feed.
5. Go to bed. Task timeline from a 05:00 start: Works_Base ~07:05, Authorships ~07:10,
   Guardrails ~09:50, ES sync from ~09:55 (3 to 4 h for more than 50M docs), Full_Snapshot
   ~10:00 to ~11:30, Lakebase ~10:00 to ~11:30.

## 3. Morning checks

```sql
SELECT run_ts, value, baseline, accepted, override_enabled, note
FROM openalex.works.guardrails_history
WHERE metric = 'records_changed_by_run' ORDER BY run_ts DESC LIMIT 5;
```
Expect `accepted = true`, `note = 'override:table'` (`'override:param'` for a manual run), value
≈ your estimate plus a normal night (1M to 3M). Then confirm the defaults are still false:
```
databricks jobs get 616701029470182 --output json | python3 -c "import json,sys; print([(p['name'],p['default']) for p in json.load(sys.stdin)['settings']['parameters']])"
```
Spot-check one changed record through a list filter or Elasticsearch, not a single-entity GET
(`/works/{id}` is Lakebase-first and lagged the run's stamp by hours on 2026-09-23), and post
the outcome in the same #dev thread. The next scheduled nightly should show a normal count.

## 3b. Fallback: manual run with the parameter (only when it must publish before 05:00 UTC)

Worked example: 2026-09-22, oxjob #1297 author served names, 52.35M works. Override run
495961030777985 started 04:12 UTC, green at 11:04 UTC. The job id goes inside the JSON
(`--job-id` is not a flag on `run-now`):

```
databricks jobs run-now --json '{"job_id": 616701029470182, "job_parameters": {"guardrails_override": "true"}}' --no-wait --output json
```
Confirm the parameter took: `databricks jobs get-run <run_id>` shows `guardrails_override`
with `"value": "true"`. Then deal with the schedule:

- **Pausing the schedule with `databricks jobs update` does not hold.** The job is
  bundle-managed (`deployment.kind: BUNDLE`, `jobs/walden_end2end.yaml` says
  `pause_status: UNPAUSED`) and `deploy-databricks.yml` redeploys it on every push to `main`.
  Seen 2026-09-22: paused 18:35 CT, silently unpaused by an unrelated push at 22:32 CT, the
  scheduled run fired at 00:00 CT.
- `max_concurrent_runs: 1` does **not** skip the schedule because the job has `queue: enabled`.
  The queued run's five `*_Ingest_DLT` tasks start at 05:00 anyway and collide with the manual
  run's pipelines. Seen 2026-09-23: they timed out once at 06:00 UTC, the retries succeeded when
  the manual run ended, and the scheduled run went green at 13:22 UTC with a normal Guardrails
  count. Cost: a late public snapshot and one failure email to Casey. Seen 2026-09-22 (worse
  case): four hourly retries burned, run failed at ~11:20 UTC with everything UPSTREAM_FAILED.
- To avoid the collision: cancel the queued PERIODIC run right after 05:00 UTC
  (`databricks jobs list-runs --job-id 616701029470182 --active-only`, then
  `databricks jobs cancel-run <run_id>`; Jason's call), or commit `pause_status: PAUSED` to the
  yaml for the night and revert it in the morning. Otherwise pre-announce the red run in #dev.
- A one-shot waiter:
  ```
  until databricks jobs get-run <run_id> --output json | grep -q '"life_cycle_state":"TERMINATED\|INTERNAL_ERROR'; do sleep 120; done
  ```

## 4. If it goes wrong

- Run red at Guardrails despite the override: a *different* check fired. Read the Guardrails task
  output; `guardrails_override` covers all quality checks, the other three flags are separate
  classes.
- Run red elsewhere (ES sync, snapshot): the works Delta table is already written; rerun with
  "Repair run" from the failed task. A pre-cleared window still covers a same-morning repair; a
  manual run keeps its parameter.
- Forgot to pre-clear and the scheduled run tripped Check 1: after Jason's yes, pre-clear now
  (`scripts/preclear_e2e.py`, the window covers today) and repair-run from Guardrails; or
  repair-run with the parameter, the 2026-09-21 pattern (run 930976173247507, repaired 14:05 UTC).

## 5. Why this exists

Guardrails tripped on intended bulk work on 2026-06-25, 07-22, 08-21, 09-12 and 09-21. Each time
the data was written but nothing published, and every job waiting on "after the nightly" lost the
morning. Jason, 2026-09-21: "we should be doing that intentionally, not just making big changes,
messing up the E2E, and then having to ask Casey to cover our asses the next morning." The
pre-clear table (oxjob #1338, 2026-09-23) came from the next lesson: an evening manual run
collides with the schedule, and the schedule cannot be paused reliably, so the override had to
move into the scheduled run itself.

## 6. Alternative: self-advancing ramp, no override

Gate the change with `pmod(hash(<entity_id>), N) < DATEDIFF(CURRENT_DATE(), DATE '<merge date>')`
so 1/N of the entities flip per night; pick N so each night stays under ~5M works on top of the
normal load. No daily commits, no override, a skipped night catches up with two buckets. Remove
the gate once the ramp completes (it is a no-op by then). Sized for #1297: N=25, ~2.1M works per
night, 25 nights.
