# Runbook: publish a bulk works change overnight with a Guardrails override

Use this when a walden change will re-stamp more than ~5M `openalex_works` rows in one End 2 End
run (Guardrails Check 1 fails at **7,500,000** `records_changed_by_run`), and you want the wave
published overnight without anyone rerunning the job by hand in the morning.

Worked example: 2026-09-22, oxjob #1297 author served names, 52.35M works. Override run
495961030777985 started 04:12 UTC, green at 11:04 UTC.

## 0. Rules that do not bend

- The override is applied **only with Jason's explicit yes for this specific run**. A standing
  approval, "go ahead", or last week's similar case does not count. Never ask Casey to apply it.
- Never change the job-level parameter defaults (`guardrails_override` etc. must stay `"false"`).
  The override is a run parameter on one `run-now`.
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

## 2. Evening sequence (all times UTC; nightly schedule is 05:00 UTC = 00:00 CDT / 01:00 EDT)

Start by ~03:30 UTC to leave room. Every step is idempotent; if anything fails, stop and report.

1. **Nothing else running.**
   ```
   databricks jobs list-runs --job-id 616701029470182 --active-only --output json   # End 2 End
   databricks jobs list-runs --job-id 63282467302934  --active-only --output json   # Authors
   ```
2. **Land the change upstream** if it lives in a separate job (Authors, 63282467302934, ~25 min;
   scheduled 12:36 UTC, which is *after* the nightly reads it, so a merge today is first seen by
   tomorrow night's run unless you hand-run it now):
   ```
   databricks jobs run-now 63282467302934 --no-wait --output json
   ```
3. **Verify the upstream build** before throwing the switch. Count what changed and spot-check
   ten rows. For #1297: `COUNT(*) ... WHERE updated_date >= '<today>'` on `openalex_authors`,
   plus the changed-row rate on a sample table matching the dry-run estimate.
4. **Tell Casey in #dev**: what change, how many works, override on purpose, replicas dropped,
   no action needed, "if the override run is red in the morning look at X first".
5. **Start End 2 End with the override, before 05:00 UTC.** The job id goes inside the JSON
   (`--job-id` is not a flag on `run-now`):
   ```
   databricks jobs run-now --json '{"job_id": 616701029470182, "job_parameters": {"guardrails_override": "true"}}' --no-wait --output json
   ```
   Confirm the parameter took: `databricks jobs get-run <run_id>` shows
   `guardrails_override` with `"value": "true"`.
6. **Cancel the queued scheduled run right after 05:00 UTC.** `max_concurrent_runs: 1` does
   **not** skip the schedule because the job has `queue: enabled`. Worse, the queued run's five
   `*_Ingest_DLT` tasks start at 05:00 anyway, collide with the override run's pipelines, time out
   hourly through 4 retries, and the run fails at ~11:20 with everything UPSTREAM_FAILED. No data
   impact, but it emails Casey a failure.
   ```
   databricks jobs list-runs --job-id 616701029470182 --active-only --output json   # find trigger=PERIODIC
   databricks jobs cancel-run <queued_run_id>
   ```
   Cancelling a prod run is Jason's call; if you cannot, pre-announce the red run in #dev.

   **Pausing the schedule with `databricks jobs update` does not hold.** The End 2 End job is
   bundle-managed (`deployment.kind: BUNDLE`, `jobs/walden_end2end.yaml` says
   `pause_status: UNPAUSED`), and `deploy-databricks.yml` redeploys the bundle on every push to
   `main`. Seen 2026-09-22: paused 18:35 CT, silently unpaused by an unrelated push at 22:32 CT,
   scheduled run fired at 00:00 CT as usual. Either commit `pause_status: PAUSED` to the yaml for
   the night and revert it in the morning (two pushes), or rely on cancelling the queued
   PERIODIC run at 00:00 CT (this step). Update 2026-09-23: the collision is survivable. The
   scheduled run's `*_Ingest_DLT` tasks timed out once at 01:00 CT, the retries succeeded when the
   override run ended, and the scheduled run went green at 08:22 CT with a normal Guardrails count.
   Cost is a late public snapshot and one failure email, not a lost night.
7. **Watch.** Task timeline from a 04:12 start: Works_Base ~06:15, Authorships ~06:20,
   Works_Enriched ~06:30, Guardrails ~07:02, ES sync 07:08 to 10:46 (52M docs), Full_Snapshot
   07:13 to 08:22, Lakebase 07:08 to 08:31. A one-shot waiter:
   ```
   until databricks jobs get-run <run_id> --output json | grep -q '"life_cycle_state":"TERMINATED\|INTERNAL_ERROR'; do sleep 120; done
   ```

## 3. Morning checks

```sql
SELECT run_ts, value, baseline, accepted, override_enabled, note
FROM openalex.works.guardrails_history
WHERE metric = 'records_changed_by_run' ORDER BY run_ts DESC LIMIT 5;
```
Expect `accepted = true`, `note = 'override'`, value ≈ your estimate plus a normal night (1M to 3M).
Then confirm the defaults are still false:
```
databricks jobs get 616701029470182 --output json | python3 -c "import json,sys; print([(p['name'],p['default']) for p in json.load(sys.stdin)['settings']['parameters']])"
```
Spot-check one changed record on the public API (author record and one of their works), and
post the outcome in the same #dev thread. The next scheduled nightly should show a normal count.

## 4. If it goes wrong

- Override run red at Guardrails: a *different* check fired. Read the Guardrails task output;
  `guardrails_override` covers all quality checks, the other three flags are separate classes.
- Override run red elsewhere (ES sync, snapshot): the works Delta table is already written; rerun
  with "Repair run" from the failed task, override still set on that run.
- Scheduled run started first (you missed 05:00): it will trip Check 1. Let it, then repair-run
  from Guardrails with the override after Jason's yes; that is the 2026-09-21 pattern (run
  930976173247507, repaired 14:05 UTC).

## 5. Why this exists

Guardrails tripped on intended bulk work on 2026-06-25, 07-22, 08-21, 09-12 and 09-21. Each time
the data was written but nothing published, and every job waiting on "after the nightly" lost the
morning. Jason, 2026-09-21: "we should be doing that intentionally, not just making big changes,
messing up the E2E, and then having to ask Casey to cover our asses the next morning."

## 6. Alternative: self-advancing ramp, no override

Gate the change with `pmod(hash(<entity_id>), N) < DATEDIFF(CURRENT_DATE(), DATE '<merge date>')`
so 1/N of the entities flip per night; pick N so each night stays under ~5M works on top of the
normal load. No daily commits, no override, a skipped night catches up with two buckets. Remove
the gate once the ramp completes (it is a no-op by then). Sized for #1297: N=25, ~2.1M works per
night, 25 nights.
