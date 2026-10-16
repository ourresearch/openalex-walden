WITH b AS (SELECT min(baseline_version) lo,max(baseline_version) hi,count(*) links FROM openalex_dev.rohan_lab.fs1012_prefold_links VERSION AS OF 0),
v AS (SELECT max(version) version FROM (DESCRIBE HISTORY openalex.awards.work_awards) WHERE operation='CREATE OR REPLACE TABLE AS SELECT'),
h AS (SELECT max(version) version FROM (DESCRIBE HISTORY openalex_dev.rohan_lab.fs1012_prefold_links))
SELECT b.*,v.version last_publication_version,h.version baseline_table_version,
assert_true(b.lo=b.hi AND b.lo=v.version AND b.links>0 AND h.version=0,'m2 baseline stale or changed: STOP') gate FROM b CROSS JOIN v CROSS JOIN h;
