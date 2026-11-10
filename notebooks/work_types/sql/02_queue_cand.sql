-- Candidates (worktype-student-v2, 2026-11-09): every live work's input_key, the fingerprint of exactly what its pack is
-- assembled from (02_pack_rows.sql's row: the work fields, the records, the tags, the MEDLINE / DataCite types; arrays
-- sorted, so row order never matters) plus the pack and render versions. A work is a candidate when it has no score row
-- for the model yet (new) or its latest-seen row was seen with another key (changed): any change to its inputs, tags and
-- source types included, gives a new key. No state table: the scores table is the state.
CREATE OR REPLACE TEMP VIEW wt_keys AS
SELECT work_id,
       xxhash64(work_j, array_join(array_sort(recs), '\n'), array_join(array_sort(tags), '\n'),
                array_join(array_sort(b3), '\n'), '${pack_version}', '${render_version}') AS input_key
FROM wt_pack_rows;

CREATE OR REPLACE TEMP VIEW wt_cand AS
SELECT k.work_id, k.input_key, s.work_id IS NULL AS new
FROM wt_keys k
LEFT JOIN (
  SELECT work_id, input_key FROM ${S}.work_type_scores
  WHERE model_version = '${model_id}'
  QUALIFY row_number() OVER (PARTITION BY work_id ORDER BY seen_at DESC, scored_at DESC, input_hash) = 1
) s ON s.work_id = k.work_id
WHERE s.input_key IS DISTINCT FROM k.input_key;
