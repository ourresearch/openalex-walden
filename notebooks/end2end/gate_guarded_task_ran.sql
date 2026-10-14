-- Gate for an optional End 2 End step (Merge_Declared_Preprints, Affiliation_Matcher_Nightly, Vector_Matcher; run_if ALL_DONE).
-- Passes when the guarded task ran, whatever its result (success, failed, timedout, canceled): the downstream task then runs
-- with or without that step's output. Fails when the guarded task never ran because its own upstream failed or was
-- canceled. The downstream task is skipped in that case anyway (it depends on the same upstream), and a FAILED gate is
-- picked up by "repair: rerun all failed tasks", so the repaired downstream task waits for the guarded step again.
-- A gate that passed here (the old condition_task) stayed SUCCESS through repairs, and the repaired Author_Matching ran
-- in parallel with Vector_Matcher instead of after it: no vector assignments on 12 and 14 Oct (oxjob #1348).
-- :guarded_state = {{tasks.<guarded task>.result_state}}.
SELECT CASE WHEN :guarded_state IN ('upstream_failed', 'upstream_canceled', 'upstream_evicted')
            THEN raise_error(concat('guarded task never ran (', :guarded_state, '); a repair re-runs it, then this gate'))
            ELSE :guarded_state END AS guarded_state
