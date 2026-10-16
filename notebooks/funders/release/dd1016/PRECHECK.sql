-- dd1016-b3 read-only precheck: run on the push morning, before push-walden.sh push. Writes nothing.
-- Push only if live_n=2191, live_ok, alias_n=0 and alias_ok are all true. Otherwise re-cut the hashes (build/render.py) and re-review.
WITH live AS (WITH linked AS (SELECT funder_id FROM openalex.mid.work_funder
  UNION SELECT CAST(regexp_extract(funder_id,'F([0-9]+)$',1) AS BIGINT) funder_id FROM openalex.works.fulltext_work_funders
  UNION SELECT funder_id FROM openalex.awards.crossref_work_funders
  UNION SELECT funder_id FROM openalex.awards.crossref_no_doi_main_cut
  UNION SELECT funder_id FROM openalex.awards.datacite_work_funders
  UNION SELECT funder_id FROM openalex.awards.europepmc_work_funders
  UNION SELECT funder_id FROM openalex.awards.funder_reported_work_funders
  UNION SELECT funder_id FROM openalex.awards.hakai_work_funders
  UNION SELECT funder_id FROM openalex.awards.anr_work_funders
  UNION SELECT funder_id FROM openalex.awards.kaken_work_funders
  UNION SELECT funder_id FROM openalex.awards.nwo_work_funders
  UNION SELECT funder_id FROM openalex.awards.datacite_work_funder_evidence
  UNION SELECT coalesce(funder_id, CAST(regexp_extract(funder.id,'F([0-9]+)$',1) AS BIGINT)) funder_id FROM openalex.awards.gtr_awards WHERE work_id IS NOT NULL
  UNION SELECT CAST(regexp_extract(award.funder_id,'F([0-9]+)$',1) AS BIGINT) funder_id FROM openalex.awards.work_awards
  UNION SELECT CAST(regexp_extract(wf.id,'F([0-9]+)$',1) AS BIGINT) funder_id FROM openalex.works.openalex_works LATERAL VIEW explode(funders) x AS wf
  UNION SELECT CAST(nullif(regexp_extract(id_1,'^F([0-9]+)$',1),'') AS BIGINT) funder_id FROM openalex.mid.entity_link
  UNION SELECT CAST(nullif(regexp_extract(id_2,'^F([0-9]+)$',1),'') AS BIGINT) funder_id FROM openalex.mid.entity_link)
 SELECT f.funder_id FROM openalex.funders.funders f
 WHERE f.merge_into_id IS NULL AND nullif(trim(f.ror_id),'') IS NULL
 AND NOT EXISTS (SELECT 1 FROM openalex.awards.openalex_awards a WHERE a.funder_id = f.funder_id)
 AND NOT EXISTS (SELECT 1 FROM openalex.funders.funders c JOIN linked l ON l.funder_id = c.funder_id WHERE coalesce(c.merge_into_id, c.funder_id) = f.funder_id)
 AND NOT EXISTS (SELECT 1 FROM openalex.funders.funders t WHERE t.merge_into_id = f.funder_id)
 AND NOT EXISTS (SELECT 1 FROM openalex.funders.funder r WHERE r.merged_into_id = f.funder_id)),
alias AS (SELECT funder_id FROM openalex.funders.funders WHERE merge_into_id IN (SELECT funder_id FROM live))
SELECT (SELECT count(*) FROM live) live_n, (SELECT sha2(concat(array_join(transform(sort_array(collect_list(funder_id)),x -> cast(x AS string)), '\n'),'\n'),256) FROM live)='ca97a58804a3f90ceabad9165415e1759ce5073e655b2511eb1d44815e74ce14' live_ok,
 (SELECT count(*) FROM alias) alias_n, (SELECT sha2(concat(array_join(transform(sort_array(collect_list(funder_id)),x -> cast(x AS string)), '\n'),'\n'),256) FROM alias)='01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b' alias_ok;
