-- No-merge switch: a deferred decision must never reach serving.
SELECT count(*) active_merges,assert_true(count(*)=0,'deferred merge activated: STOP') gate FROM openalex.funders.nm1013_integrated_merges;
WITH a AS (SELECT funder_id id FROM openalex.funders.funders WHERE merge_into_id IS NULL),
b AS (SELECT id FROM openalex.funders.funders_api)
SELECT count_if(a.id IS NULL) removed,count_if(b.id IS NULL) added,
 assert_true(count_if(a.id IS NULL OR b.id IS NULL)=0,'funder API membership changes in zero-merge switch: STOP') gate
FROM a FULL JOIN b USING(id);
