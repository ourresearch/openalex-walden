-- One reader for "how far may tonight's End 2 End run go on metric M?" (oxjob #2005, 3 Nov 2026).
--
-- A pre-clear row in openalex.works.e2e_overrides (written by scripts/preclear_e2e.py) names how much a planned change
-- may move each check: max_changed for the flag's own count (works stamped for guardrails_override, works deleted for
-- deleted_works_guard_override) and max_loss for every other metric (works_total, works_with_abstract_any, ...,
-- work_author_curations). Until 3 Nov, one capped guardrails_override row raised Check 1 only and switched Checks 2-9
-- and the curation decline gates OFF, so any failed loss check was accepted and became the next baseline (6 nights
-- since 21 Sep, each planned only in the row's prose). Now a row raises exactly the checks it names, to the number it
-- names; a check it doesn't name keeps its normal line.
--
-- e2e_cap(flag, metric) -> {cap, preclear}: the cap open pre-clear rows give this metric right now (NULL = none names
--   it; 9223372036854775807 = an uncapped legacy row, max_changed NULL), and the rows that gave it (for the record).
--   Several open rows: the smallest cap wins, as for max_changed since 6 Oct.
-- e2e_gate(flag, metric, amount, base, param) -> TRUE when `amount` (stamps, a loss, a decline, deletions) is within
--   the check's own line `base` or the cap a row names, or the job parameter is "true" (a manual run-now, which still
--   accepts everything, as before).
--
-- Every capped gate in walden_end2end calls these: Guardrails (all checks), SyncWorkAuthorCurations and
-- SyncLocationCurations (curation decline), TrackDeletedWorks and delete_works (deletion cap). On/off switches with no
-- size (keep_replicas, hash_rebaseline, deleted_locations_guard_override, wunpaywall_guard_override) keep
-- e2e_override_active(flag, param).
--
-- Apply (once, after the columns exist; CREATE OR REPLACE is idempotent). First, if missing:
--   ALTER TABLE openalex.works.e2e_overrides ADD COLUMNS (max_loss MAP<STRING, BIGINT>);
--   ALTER TABLE openalex.works.guardrails_history ADD COLUMNS (threshold BIGINT, preclear STRING);
-- Revert: revert the walden commit that calls these (readers go back to e2e_override_active), then
--   DROP FUNCTION openalex.works.e2e_gate; DROP FUNCTION openalex.works.e2e_cap;
-- (the two nullable columns can stay; nothing old reads them).

CREATE OR REPLACE FUNCTION openalex.works.e2e_cap(flag STRING, metric STRING)
RETURNS STRUCT<cap: BIGINT, preclear: STRING>
COMMENT 'oxjob #2005: the cap open pre-clear rows in openalex.works.e2e_overrides give metric for flag right now (NULL = none names it; BIGINT max = uncapped legacy row), and which rows gave it. Source: openalex-walden notebooks/end2end/e2e_gate.sql'
RETURN CASE
  WHEN concat(flag, ':', metric) IN ('guardrails_override:records_changed_by_run', 'deleted_works_guard_override:works_deleted') THEN (
    SELECT named_struct(
      'cap', CASE WHEN count(*) = 0 THEN CAST(NULL AS BIGINT)
                  WHEN count_if(o.max_changed IS NULL) > 0 THEN 9223372036854775807
                  ELSE min(o.max_changed) END,
      'preclear', CASE WHEN count(*) = 0 THEN CAST(NULL AS STRING)
                       ELSE array_join(collect_list(concat(date_format(o.created_at, 'yyyy-MM-dd HH:mm'), ' UTC ',
                                                           o.requested_by, ': ', left(o.reason, 100))), ' + ') END)
    FROM openalex.works.e2e_overrides o
    WHERE o.flag = e2e_cap.flag AND current_timestamp() >= o.valid_from AND current_timestamp() < o.valid_until)
  ELSE (
    SELECT named_struct(
      'cap', min(l.value),
      'preclear', CASE WHEN count(*) = 0 THEN CAST(NULL AS STRING)
                       ELSE array_join(collect_list(concat(date_format(o.created_at, 'yyyy-MM-dd HH:mm'), ' UTC ',
                                                           o.requested_by, ': ', left(o.reason, 100))), ' + ') END)
    FROM openalex.works.e2e_overrides o LATERAL VIEW explode(o.max_loss) l AS key, value
    WHERE o.flag = e2e_cap.flag AND l.key = e2e_cap.metric
      AND current_timestamp() >= o.valid_from AND current_timestamp() < o.valid_until)
END;

CREATE OR REPLACE FUNCTION openalex.works.e2e_gate(flag STRING, metric STRING, amount BIGINT, base BIGINT, param STRING)
RETURNS BOOLEAN
COMMENT 'oxjob #2005: TRUE when amount is within the check''s own line (base), or the cap a pre-clear row names for this metric (e2e_cap), or the job parameter is "true". Source: openalex-walden notebooks/end2end/e2e_gate.sql'
RETURN lower(coalesce(param, 'false')) = 'true'
    OR coalesce(amount, 0) <= greatest(base, openalex.works.e2e_cap(flag, metric).cap);
