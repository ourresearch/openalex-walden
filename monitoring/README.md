# Monitoring

Component-level health checks for Walden (oxjob #1116). Each component states, in plain
words, what it is supposed to be doing; concrete checks under each statement are scored
nightly against real metrics; critical deviations alert; an LLM writes the morning report.

Status is deterministic. The LLM never decides whether something is wrong. It explains.

## The contract, in one paragraph

**The part of the system that does the job writes its own metrics.** A job's final task
emits the numbers only that job knows (rows processed, outcomes, drops by reason, wall time)
into the shared tall table `openalex.monitoring.metrics`. A separate observation notebook
may add the numbers a job cannot see about itself (backlog, coverage, judged samples).
A checks file per component turns those metrics into pass/watch/critical. One engine reads
every checks file. One report task narrates every component. Adding a component means a
metrics writer and a checks file, and nothing else changes.

## Adding a component

1. **Emit metrics.** Two ways, same table, same shape:
   - *Self-report* (preferred): the last cells of the notebook that does the job. After
     its final write, a SQL notebook does `DELETE … WHERE snapshot_date = current_date()
     AND component = … AND source = …` then `INSERT INTO openalex.monitoring.metrics`,
     one statement per cell. Template: the "Monitoring: self-reported run metrics" block at
     the end of `notebooks/end2end/MatchAuthors.ipynb`. Python/cluster notebooks call
     `utils.monitoring_metrics.emit(spark, component, rows, source=...)` as their last step.
   - *Observe*: a nightly notebook reading pipeline tables and calling `emit()` with its
     own `source`. Template: `notebooks/metrics/AuthorshipDailyMetrics.py`. A legacy tall
     table can be registered via `metrics_table` in the checks file while it is being
     migrated; the engine unions it with the sink. Author matching is fully on the sink
     (its #640 table was copied in and retired).

   Shape: `snapshot_date / metric / dimension / value`. Keep a metric a plain count or
   level. Ratios and shares are computed by the checks, not the writer.

   **Emit only what a check reads.** A metric with no check is a cell on a pipeline's
   critical path that nobody looks at. Name the offender instead of counting offenders:
   `name_concentration_top[<name>]` (top 15) beats `hot_names` (a count), because the
   finding then says *which*. Top-N emissions stay small (5 for sources, 15 for names).

   **Where the self-report lives.** When the job is a notebook, the metric cells are the
   last cells of that notebook, after its final write. When the job is a pipeline (the DLT
   ingest pipelines have no "last cell"), the writer is a final task in the wrapper job
   that reads the pipeline's event log and tables after it finishes. Same table, same
   shape, same `source` column either way.

   **What stays with an observer.** Only numbers no single notebook owns: state across the
   whole corpus (backlogs, coverage), diffs against yesterday, a job's own wall time and
   SQL cost (a task cannot see its own end time; that comes from the Jobs API afterwards),
   and judged samples (an LLM judge on the critical path of a pipeline would let an
   endpoint outage block the pipeline). For author matching that is
   `notebooks/metrics/AuthorshipDailyMetrics.py` plus the judge tasks in its job.

2. **Write `monitoring/checks/<component>.yaml`.** Sections are the owner's questions in
   plain words ("Are we keeping up with the producer?"). Checks under each name a
   `metric[dimension]` or a ratio, a rule, and the known failure modes from past incidents.
   The grammar and every field are documented at the top of
   `monitoring/checks/author_matching.yaml`. Rules of thumb learned on the prototype:
   - Ratios against batch size, not raw counts, for anything that scales with volume.
   - Pure-volume checks get `max_status: watch`. They are context, not alarms.
   - Absolute lines for the things that page. Relative (median ± MAD) only for shares
     and ratios; raw counts get an absolute line or no check at all.
   - Day-over-day deltas want absolute thresholds; a MAD baseline on a delta is noise.
   - One check per question. A check that duplicates a paging one, or fans out to a row
     per dimension nobody reads, gets cut. Author matching went 46 → 33 checks and
     162 → ~80 findings a night this way (Casey, 2026-09-15: "don't overdo it").

3. **Replay before you deploy.** `.venv/bin/python scripts/monitoring_dryrun.py <component>`
   runs the engine over the component's history read-only and prints what would have fired.
   Tune until the criticals are the incidents you remember and quiet nights are quiet. The
   author-matching prototype went from ~15 criticals a night to ~1 this way, and the replay
   found three engine bugs before anything ran on Databricks.

4. **Render the doc.** `.venv/bin/python scripts/monitoring_render_docs.py` writes
   `docs/monitoring/<component>.md` from the YAML. The YAML is the source of truth; never
   edit the rendered file.

5. Nothing else. The `Monitoring` job picks up every checks file on its next run.

## Pieces

| path | role |
|---|---|
| `monitoring/checks/*.yaml` | the checklists, one per component (data, reviewed in PRs) |
| `utils/monitoring_engine.py` | pure-Python engine: expression grammar, absolute/relative rules, 2-day confirmation, baseline exclusion, overrides |
| `utils/monitoring_metrics.py` | `emit()` for Python jobs |
| `notebooks/monitoring/MonitoringFindings.py` | nightly: every checks file → `openalex.monitoring.findings` |
| `notebooks/monitoring/MonitoringReport.sql` | warehouse `ai_query`: findings + last 7 reports → `openalex.monitoring.reports` |
| `jobs/monitoring.yaml` | the job: findings (serverless) then report (warehouse), 23:30 UTC |
| `scripts/monitoring_dryrun.py` | local replay of a component's history through the engine |
| `scripts/monitoring_render_docs.py` | YAML → `docs/monitoring/<component>.md` |
| `monitoring/alerts/monitoring_daily_email.json` + `scripts/monitoring_create_alert.py` | the daily email: a SQL alert (v2) over `reports` + `findings`, custom subject/body, 00:05 UTC. Hand-created as its owner (not a bundle resource — DABs alerts must be owned by the deploy principal, which cannot read the tables). Re-run the script to update; edit the JSON, not the UI. |

Tables, all in `openalex.monitoring`: `metrics` (shared sink, liquid-clustered on
component + date; one delete-then-append slice per writer `source`), `checks` (mirror of
the YAML, for the report and the dashboard), `findings`, `reports`, `baseline_overrides`
(accept current values as the new normal, Guardrails-style). Per-component detail tables
(judged samples, the assignment log) stay with their component; the sink holds numbers only.

## Status levels

`ok`, `watch`, `critical` are the alert levels. `known` is a suppressed known state.
`insufficient_history`, `insufficient_n`, `no_data` mean the check could not be scored
tonight and say why. Only `critical` on a check marked `page: true` notifies anyone.
