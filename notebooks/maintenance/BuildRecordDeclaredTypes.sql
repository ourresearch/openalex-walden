-- Databricks notebook source
-- openalex.works.record_declared_types: one row per record (provenance, native_id, as in locations_mapped) whose own host
-- declares a work type other than article, for records our pipeline types article only because they say nothing (mostly
-- MAG crawls of repository and catalogue pages, which fall to the type cascade's 'default: -> article'). Read by
-- CreateWorksBase's final type CASE (rdt join): when such a record leads a work typed article, the work takes the declared
-- type. Type only: location types, matching and merges are untouched.
-- Shared like openalex.sources.article_page_journal_sources: each job inserts and deletes only its own platform's rows.
--   platform 'diva' (oxjob #2236): DiVA, the shared repository of ~50 Swedish universities (diva-portal.org, diva2:<pid>)
--   platforms 'ads', 'aps', 'eric', 'osti', 'ntrs', 'inis', 'worldbank' (oxjob #2243): MAG crawls of report servers,
--   catalogues and meeting-abstract hosts (NASA ADS, APS, ERIC, OSTI, NASA NTRS, INIS, World Bank documents)
--   platform 'cinii' (oxjob #1160): MAG crawls of CiNii Books catalogue pages whose CiNii note says the item is a thesis
--   platforms 'gallica', 'hispana' (oxjob #2611): digitized old print the holder itself calls a book (Gallica's own type set
--   gallica:typedoc:monographies; Hispana records whose raw type is 'Impresos')
-- Revert one platform: DELETE FROM openalex.works.record_declared_types WHERE platform = '<platform>' (the next night types
-- its works back). Revert the mechanism: drop the rdt join and its WHEN in CreateWorksBase first (its LEFT JOIN fails on a
-- missing table), then this table.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS openalex.works.record_declared_types (
  provenance STRING COMMENT 'locations_mapped.provenance of the record (mag, repo, ...)',
  native_id STRING COMMENT 'locations_mapped.native_id of the record (for mag: the MAG paper id)',
  type STRING COMMENT 'the OpenAlex work type the host''s own declaration maps to (never article)',
  platform STRING COMMENT 'whose rows these are: diva (oxjob 2236). Each job inserts and deletes only its own platform',
  record_key STRING COMMENT 'the host''s own id for the record (diva2:<pid>)',
  declared_code STRING COMMENT 'the type the host declares, in its own words (DiVA publicationTypeCode)',
  oxjob INT COMMENT 'job that wrote the row',
  harvested_at TIMESTAMP COMMENT 'when the host''s declarations were read'
)
COMMENT 'One row per record whose own host declares a work type other than article (theses, conference papers, chapters, reports on repository and catalogue pages our pipeline would type article). Read by the final type CASE of the End 2 End notebook CreateWorksBase. Each job inserts and deletes only its own platform: diva (oxjob 2236). Built by notebooks/maintenance/BuildRecordDeclaredTypes.sql.'

-- COMMAND ----------

-- platform 'diva' (oxjob #2236). DiVA's own publicationTypeCode per record, from DiVA's public search export
-- (https://www.diva-portal.org/smash/export.jsf, format=csv: PIDs only), one request per code and year range, harvested
-- 14 Nov 2026 by oxjobs #2236 work/harvest_diva_pids.py and joined into one file `pid,diva_type_code` at
-- /Volumes/openalex/works/data/diva/pid_types_2026_11.csv. Records whose URL names a diva2 pid get its type:
--   studentThesis, comprehensive/monograph Doctoral/Licentiate Thesis -> dissertation
--   conferencePaper -> conference-paper          chapter -> book-chapter          report -> report
--   book, collection (edited volume), conferenceProceedings -> book                manuscript ('Manuskript (preprint)') -> preprint
--   review (research review article) -> review   bookReview -> book-review        dataset -> dataset
--   other, patent, artisticOutput -> other
-- DiVA's article records are not harvested (they stay article). Manuscripts carry no dateIssued, so they were fetched with no
-- date filter (work/harvest_nodate.py); the rest by code and year. A record whose URLs name pids of two different types, or a
-- pid listed under two codes, gets no row. To refresh: harvest again, copy the new file there, set the path below, re-run.
DELETE FROM openalex.works.record_declared_types WHERE platform = 'diva';

-- COMMAND ----------

INSERT INTO openalex.works.record_declared_types
WITH harvest AS (
  SELECT TRY_CAST(_c0 AS BIGINT) AS pid, _c1 AS code
  FROM read_files('/Volumes/openalex/works/data/diva/pid_types_2026_11.csv',
                  format => 'csv', header => true, schema => '_c0 STRING, _c1 STRING')
),
pid_code AS (
  SELECT pid, min(code) AS code FROM harvest WHERE pid IS NOT NULL GROUP BY pid HAVING count(DISTINCT code) = 1
),
mapped AS (
  SELECT pid, code,
    CASE
      WHEN code IN ('studentThesis', 'comprehensiveDoctoralThesis', 'monographDoctoralThesis',
                    'comprehensiveLicentiateThesis', 'monographLicentiateThesis') THEN 'dissertation'
      WHEN code = 'conferencePaper' THEN 'conference-paper'
      WHEN code = 'chapter' THEN 'book-chapter'
      WHEN code = 'report' THEN 'report'
      WHEN code IN ('book', 'collection', 'conferenceProceedings') THEN 'book'
      WHEN code = 'manuscript' THEN 'preprint'
      WHEN code = 'review' THEN 'review'
      WHEN code = 'bookReview' THEN 'book-review'
      WHEN code = 'dataset' THEN 'dataset'
      WHEN code IN ('other', 'patent', 'artisticOutput') THEN 'other'
    END AS type
  FROM pid_code
),
recs AS (
  -- every record of ours whose URL is a DiVA page naming a pid
  SELECT DISTINCT provenance, native_id,
         TRY_CAST(REGEXP_EXTRACT(landing_page_url, '(?i)diva2(?::|%3A)([0-9]+)', 1) AS BIGINT) AS pid
  FROM openalex.works.locations_mapped
  WHERE landing_page_url ILIKE '%diva-portal.org/%' AND landing_page_url RLIKE '(?i)diva2(:|%3A)[0-9]+'
),
typed AS (
  SELECT r.provenance, r.native_id, min(m.type) AS type, count(DISTINCT m.type) AS n_types,
         min(concat('diva2:', CAST(r.pid AS STRING))) AS record_key, min(m.code) AS declared_code
  FROM recs r JOIN mapped m USING (pid)
  GROUP BY r.provenance, r.native_id
)
SELECT provenance, native_id, type, 'diva' AS platform, record_key, declared_code, 2236 AS oxjob,
       TIMESTAMP '2026-11-14 19:57:00' AS harvested_at  -- UTC: harvest finished 13:57 CT
FROM typed
WHERE n_types = 1 AND type IS NOT NULL

-- COMMAND ----------

-- platforms ads, aps, eric, osti, ntrs, inis, worldbank (oxjob #2243): MAG crawls of report servers, catalogues and
-- meeting-abstract hosts, typed article by the cascade's default, take the type their host's own record declares:
--   ads        NASA ADS doctype (ADS search API by bibcode): abstract, inproceedings, phdthesis, techreport, proposal, ...
--   aps        absimage.aps.org / meetings.aps.org: APS meeting abstracts by construction (no harvest)
--   eric       ERIC publicationtype of ED documents (ERIC API); EJ (journal articles) are not read
--   osti       OSTI product_type (OSTI API); Journal Article gets no row
--   ntrs       NASA NTRS stiType (NTRS search API, every record listed by published-date slices, plus per-id reads)
--   inis       INIS resource_type / resource_subtype || imprint title (INIS records API by RN)
--   worldbank  World Bank documents API majdocty / docty, by the documents.worldbank.org/curated id or guid
-- Harvested 15 Nov 2026 by oxjobs #2243 work/harvest.py and work/ntrs_enum.py into
-- /Volumes/openalex/works/data/record_declared_types/2026_11/<platform>.csv (key, declared_code); work/upload_declared.py.
-- The mapping onto OpenAlex work types follows help.openalex.org/data/work-types; a host type with no clear match gets no
-- row (the work stays article). A record already holding another platform's row keeps it (LEFT ANTI JOIN): one row per
-- record, or the End 2 End's LEFT JOIN would repeat the work. Bars (fresh random 100 per host vs the host's own record
-- read again): oxjobs #2243 README. To refresh: harvest again, upload, set harvested_at, re-run these two cells.
-- Revert: DELETE FROM openalex.works.record_declared_types WHERE platform IN (...) (the next night types them back).
DELETE FROM openalex.works.record_declared_types WHERE platform IN ('ads', 'aps', 'eric', 'osti', 'ntrs', 'inis', 'worldbank');

-- COMMAND ----------

INSERT INTO openalex.works.record_declared_types
WITH recs AS (
  SELECT DISTINCT provenance, native_id, landing_page_url AS u,
         regexp_replace(lower(try_parse_url(landing_page_url, 'HOST')), '^www\\.', '') AS host
  FROM openalex.works.locations_mapped
  WHERE provenance = 'mag'
    AND landing_page_url RLIKE '(?i)^https?://(www\\.)?(ui\\.adsabs\\.harvard\\.edu|adsabs\\.harvard\\.edu|absimage\\.aps\\.org|meetings\\.aps\\.org|eric\\.ed\\.gov|files\\.eric\\.ed\\.gov|osti\\.gov|ntrs\\.nasa\\.gov|inis\\.iaea\\.org|documents\\.worldbank\\.org)([/:?]|$)'
),
keyed AS (
  SELECT provenance, native_id,
    CASE
      WHEN host IN ('ui.adsabs.harvard.edu', 'adsabs.harvard.edu') THEN 'ads'
      WHEN host IN ('absimage.aps.org', 'meetings.aps.org') THEN 'aps'
      WHEN host IN ('eric.ed.gov', 'files.eric.ed.gov') THEN 'eric'
      WHEN host = 'osti.gov' THEN 'osti'
      WHEN host = 'ntrs.nasa.gov' THEN 'ntrs'
      WHEN host = 'inis.iaea.org' THEN 'inis'
      WHEN host = 'documents.worldbank.org' THEN 'worldbank'
    END AS platform,
    CASE
      WHEN host IN ('ui.adsabs.harvard.edu', 'adsabs.harvard.edu')
        THEN replace(replace(replace(replace(regexp_extract(u, '/abs/([^/?#]+)', 1), '%26', '&'), '%2E', '.'), '%3A', ':'), '%25', '%')
      WHEN host IN ('absimage.aps.org', 'meetings.aps.org') THEN regexp_replace(u, '^https?://(www\\.)?', '')
      WHEN host IN ('eric.ed.gov', 'files.eric.ed.gov') THEN upper(regexp_extract(u, '(?i)(ED[0-9]{5,7})', 1))
      WHEN host = 'osti.gov' THEN regexp_extract(u, '(?i)(?:biblio/|osti_id=|servlets/purl/|pages/biblio/)([0-9]+)', 1)
      WHEN host = 'ntrs.nasa.gov' THEN regexp_extract(u, '(?i)(?:R=|citations/|archive/nasa/casi\\.ntrs\\.nasa\\.gov/)([0-9]{8,11})', 1)
      WHEN host = 'inis.iaea.org' THEN regexp_extract(u, '(?i)RN(?::|%3A)([0-9A-Z]+)', 1)
      WHEN host = 'documents.worldbank.org' THEN coalesce(
        nullif(concat('id:', regexp_extract(u, '/curated/[a-z]{2}/[0-9]{4}/[0-9]{2}/([0-9]+)', 1)), 'id:'),
        nullif(concat('guid:', regexp_extract(u, '/curated/[a-z]{2}/([0-9]{12,})', 1)), 'guid:'))
    END AS record_key
  FROM recs
),
decl AS (
  SELECT 'ads' AS platform, key, declared_code FROM read_files('/Volumes/openalex/works/data/record_declared_types/2026_11/ads.csv',
    format => 'csv', header => true, schema => 'key STRING, declared_code STRING')
  UNION ALL
  SELECT 'eric', key, declared_code FROM read_files('/Volumes/openalex/works/data/record_declared_types/2026_11/eric.csv',
    format => 'csv', header => true, schema => 'key STRING, declared_code STRING')
  UNION ALL
  SELECT 'osti', key, declared_code FROM read_files('/Volumes/openalex/works/data/record_declared_types/2026_11/osti.csv',
    format => 'csv', header => true, schema => 'key STRING, declared_code STRING')
  UNION ALL
  SELECT 'ntrs', key, declared_code FROM read_files('/Volumes/openalex/works/data/record_declared_types/2026_11/ntrs.csv',
    format => 'csv', header => true, schema => 'key STRING, declared_code STRING')
  UNION ALL
  SELECT 'inis', key, declared_code FROM read_files('/Volumes/openalex/works/data/record_declared_types/2026_11/inis.csv',
    format => 'csv', header => true, schema => 'key STRING, declared_code STRING')
  UNION ALL
  SELECT 'worldbank', key, declared_code FROM read_files('/Volumes/openalex/works/data/record_declared_types/2026_11/worldbank.csv',
    format => 'csv', header => true, schema => 'key STRING, declared_code STRING')
  UNION ALL
  -- APS: the host is the declaration (every page is a meeting abstract)
  SELECT DISTINCT 'aps', record_key, 'APS meeting abstract (absimage.aps.org / meetings.aps.org)' FROM keyed WHERE platform = 'aps'
),
typed AS (
  SELECT k.provenance, k.native_id, k.platform, k.record_key, d.declared_code AS code,
    CASE k.platform
      WHEN 'aps' THEN 'conference-abstract'
      -- NASA ADS doctype (ADS's own record; https://ui.adsabs.harvard.edu/help/search/search-syntax doctype list)
      WHEN 'ads' THEN CASE d.declared_code
        WHEN 'abstract' THEN 'conference-abstract'          -- meeting abstract
        WHEN 'inproceedings' THEN 'conference-paper'
        WHEN 'phdthesis' THEN 'dissertation'
        WHEN 'mastersthesis' THEN 'dissertation'
        WHEN 'techreport' THEN 'report'
        WHEN 'intechreport' THEN 'report'
        WHEN 'book' THEN 'book'
        WHEN 'inbook' THEN 'book-chapter'
        WHEN 'bookreview' THEN 'book-review'
        WHEN 'catalog' THEN 'dataset'                       -- a data catalog (VizieR)
        WHEN 'dataset' THEN 'dataset'
        WHEN 'software' THEN 'software'
        WHEN 'erratum' THEN 'erratum'
        WHEN 'editorial' THEN 'editorial'
        WHEN 'proposal' THEN 'other'                        -- observing / research proposals
        WHEN 'circular' THEN 'other'
        WHEN 'newsletter' THEN 'other'
        WHEN 'obituary' THEN 'other'
        WHEN 'pressrelease' THEN 'other'
        WHEN 'talk' THEN 'other'
        END                                                 -- article, eprint, misc: no row; proceedings: no row (a sample of 15
                                                            -- was 6 whole volumes and 9 single papers)
      -- ERIC publicationtype (all of a record's types, '; '-joined); first match wins
      WHEN 'eric' THEN CASE
        WHEN d.declared_code LIKE '%Journal Articles%' THEN NULL
        WHEN d.declared_code LIKE '%Dissertations/Theses%' THEN 'dissertation'
        WHEN d.declared_code LIKE '%Book/Product Reviews%' THEN 'book-review'
        WHEN d.declared_code LIKE '%Collected Works - Serial%' THEN 'other'   -- a whole issue of a serial
        WHEN d.declared_code RLIKE 'Books|Collected Works - (Proceedings|General)' THEN 'book'
        WHEN d.declared_code LIKE '%Speeches/Meeting Papers%' THEN 'conference-paper'
        WHEN d.declared_code RLIKE 'Reports - |Information Analyses|ERIC Digests|Numerical/Quantitative Data' THEN 'report'
        WHEN d.declared_code LIKE '%Machine-Readable Data Files%' THEN 'dataset'
        WHEN d.declared_code LIKE '%Computer Programs%' THEN 'software'
        WHEN d.declared_code RLIKE 'Guides - |Tests/Questionnaires|Reference Materials|Legal/Legislative|Non-Print Media|Multilingual/Bilingual|Creative Works|Translations|Opinion Papers|Historical Materials' THEN 'other'
        WHEN d.declared_code LIKE '%ERIC Publications%' THEN 'report'
        END
      -- OSTI product_type
      WHEN 'osti' THEN CASE d.declared_code
        WHEN 'Technical Report' THEN 'report'
        WHEN 'Program Document' THEN 'report'
        WHEN 'Conference' THEN 'conference-paper'
        WHEN 'Thesis/Dissertation' THEN 'dissertation'
        WHEN 'Book' THEN 'book'
        WHEN 'Patent' THEN 'other'
        WHEN 'Dataset' THEN 'dataset'
        WHEN 'Software' THEN 'software'
        WHEN 'Multimedia' THEN 'other'
        END                                                 -- Journal Article, Accepted Manuscript, Miscellaneous: no row
      -- NASA NTRS stiType
      WHEN 'ntrs' THEN CASE
        -- CONFERENCE_PROCEEDINGS: 18 of 20 sampled are single papers (2 whole volumes), so conference-paper
        WHEN d.declared_code IN ('CONFERENCE_PAPER', 'MEETING_PAPER', 'CONFERENCE_PROCEEDINGS', 'CONFERENCE_PUBLICATION') THEN 'conference-paper'
        WHEN d.declared_code IN ('ABSTRACT', 'EXTENDED_ABSTRACT') THEN 'conference-abstract'
        WHEN d.declared_code = 'CONTRIBUTION_TO_LARGER_WORK' THEN 'book-chapter'
        WHEN d.declared_code IN ('CONTRACTOR_REPORT', 'CONTRACTOR_OR_GRANTEE_REPORT', 'TECHNICAL_MEMORANDUM', 'TECHNICAL_PUBLICATION',
                                 'TECHNICAL_NOTE', 'TECHNICAL_REPORT', 'REFERENCE_PUBLICATION', 'SPECIAL_PUBLICATION',
                                 'TECHNICAL_TRANSLATION', 'NASA_TECHNICAL_REPORT') THEN 'report'
        WHEN d.declared_code = 'THESIS_DISSERTATION' THEN 'dissertation'
        WHEN d.declared_code = 'BOOK_CHAPTER' THEN 'book-chapter'
        WHEN d.declared_code = 'BOOK' THEN 'book'
        WHEN d.declared_code IN ('PRESENTATION', 'POSTER', 'VIDEO') THEN 'other'
        END                                                 -- REPRINT, PREPRINT, OTHER, STI_TYPE_NONE: no row
      -- INIS '<resource_type> / <iaea:resource_subtype list> || <iaea:imprint title>' (the imprint is the host volume; a
      -- conference record printed in a 'Programme and abstracts' / 'Book of abstracts' volume is an abstract, not a paper)
      WHEN 'inis' THEN CASE
        WHEN d.declared_code LIKE 'publication-article%' THEN NULL
        WHEN regexp_replace(d.declared_code, ' \\|\\| .*$', '') RLIKE '^publication-(report|book|thesis).*Thesis'
          OR d.declared_code LIKE 'publication-thesis%' THEN 'dissertation'
        WHEN regexp_replace(d.declared_code, ' \\|\\| .*$', '') RLIKE '^publication-(report|book) / .*Conference'
          AND regexp_extract(d.declared_code, ' \\|\\| (.*)$', 1) RLIKE '(?i)abstract|summar|r[eé]sum[eé]|kurzfassung|tezis|thesen|annotat'
          THEN 'conference-abstract'
        WHEN regexp_replace(d.declared_code, ' \\|\\| .*$', '') RLIKE '^publication-(report|book) / .*Conference'
          THEN 'conference-paper'                           -- a paper in proceedings / a conference paper issued as a report
        WHEN d.declared_code LIKE 'publication-report%' THEN 'report'
        WHEN d.declared_code LIKE 'publication-patent%' THEN 'other'
        END                                                 -- book without Conference (chapter or monograph), other: no row
      -- World Bank documents API: '<majdocty> / <docty>'. Project and board paperwork that reports, assesses or reviews -> report,
      -- agreements, letters, plans, data sheets, speeches, newsletters -> other; Publication -> book; Journal Article: no row
      WHEN 'worldbank' THEN CASE
        WHEN d.declared_code LIKE '%Journal Article%' THEN NULL
        WHEN d.declared_code RLIKE '/ Publication$' THEN 'book'
        WHEN d.declared_code RLIKE 'Agreement|Letter|Memorandum|Aide Memoire|Speech|Statement|Newsletter|Viewpoint|Data Sheet|Checklist|Procurement Plan|Engagement Plan|Commitment Plan|Indigenous Peoples Plan|Resettlement Plan|Management Plan' THEN 'other'
        WHEN d.declared_code RLIKE 'Report|Assessment|Document|Paper|Evaluation|Review|Framework|Summary|Brief|Note|Study|Auditing' THEN 'report'
        END
    END AS type
  FROM keyed k JOIN decl d ON d.platform = k.platform AND d.key = k.record_key
  WHERE k.record_key IS NOT NULL AND k.record_key <> ''
),
one AS (
  SELECT provenance, native_id, min(platform) AS platform, min(type) AS type, min(record_key) AS record_key, min(code) AS declared_code
  FROM typed WHERE type IS NOT NULL
  GROUP BY provenance, native_id
  HAVING count(DISTINCT type) = 1 AND count(DISTINCT platform) = 1
)
SELECT o.provenance, o.native_id, o.type, o.platform, o.record_key, o.declared_code, 2243 AS oxjob,
       TIMESTAMP '2026-11-15 13:03:00' AS harvested_at
FROM one o
-- one row per record across every platform: the End 2 End's LEFT JOIN would otherwise repeat the work
LEFT ANTI JOIN openalex.works.record_declared_types t ON t.provenance = o.provenance AND t.native_id = o.native_id

-- COMMAND ----------

-- platform 'cinii' (oxjob #1160, PLAN step 3): MAG crawls of CiNii Books catalogue pages (ci.nii.ac.jp/ncid/<NCID>), typed
-- book or article by the cascade, whose CiNii record notes that the item is a thesis ('Thesis (Ph. D.)--Yale University,
-- 2006', 'Thesis (doctoral)--Universität Leipzig, 1903', 'Facsim. reprint of the author's thesis (Ph. D.)--...', a UMI copy
-- of the thesis itself). CiNii Books' OpenSearch note= field search (https://ci.nii.ac.jp/books/opensearch/search, format
-- json, no appid) listed every NCID whose note reads "thesis (ph. d.)", "thesis (doctoral)", "thesis (ed. d.)", "thesis
-- (m.s.)", "thesis (m.a.)", "thesis (d. phil.)" or "thesis (habilitation)", each query with -originally -revision -revised
-- (books made from a thesis): 295,024 NCIDs, harvested 21 Nov 2026 by oxjobs #1160 (cinii_harvest.py), less 5,668 whose
-- note also says 'based', 'adapted' or 'rev.'. That list is openalex.works.cinii_thesis_ncids (ncid, query_keys: the
-- phrases that matched). Japanese notes ('博士論文', '学位論文') are left out: a sample read mostly as revised books. A
-- record gets a row only when every NCID its URLs name is on the list. Bars (CiNii's own record read again): 21 Nov a
-- random 50, 49 theses (the miss, 'Based on the author's thesis', is now excluded); 22 Nov a fresh random 30 of these rows,
-- 30 theses. CreateWorksBase reads them through its own 'cinii' line (book or article; never over a type curation).
-- Revert: DELETE FROM openalex.works.record_declared_types WHERE platform = 'cinii' (the next night types them back).
DELETE FROM openalex.works.record_declared_types WHERE platform = 'cinii';

-- COMMAND ----------

INSERT INTO openalex.works.record_declared_types
WITH recs AS (
  SELECT DISTINCT provenance, native_id,
         upper(regexp_extract(landing_page_url, '(?i)ci\\.nii\\.ac\\.jp/ncid/([0-9A-Z]+)', 1)) AS ncid
  FROM openalex.works.locations_mapped
  WHERE landing_page_url ILIKE '%ci.nii.ac.jp/ncid/%'
),
one AS (
  SELECT r.provenance, r.native_id, min(r.ncid) AS record_key, min(t.query_keys) AS query_keys,
         count(DISTINCT r.ncid) AS n_ncid, count(DISTINCT t.ncid) AS n_thesis
  FROM recs r LEFT JOIN openalex.works.cinii_thesis_ncids t ON t.ncid = r.ncid
  WHERE r.ncid <> ''
  GROUP BY r.provenance, r.native_id
)
SELECT o.provenance, o.native_id, 'dissertation' AS type, 'cinii' AS platform, o.record_key,
       concat('CiNii Books note: Thesis (', o.query_keys, ')') AS declared_code, 1160 AS oxjob,
       TIMESTAMP '2026-11-21 17:00:00' AS harvested_at  -- UTC: harvest finished 11:00 CT
FROM one o
-- one row per record across every platform: the End 2 End's LEFT JOIN would otherwise repeat the work
LEFT ANTI JOIN openalex.works.record_declared_types t ON t.provenance = o.provenance AND t.native_id = o.native_id
WHERE o.n_thesis >= 1 AND o.n_thesis = o.n_ncid

-- COMMAND ----------

-- platforms 'gallica' and 'hispana' (oxjob #2611, heritage wave 2). Digitized old books stay in OpenAlex (heritage decision
-- of 6 Nov 2026, oxjob #1647) and take type book by the holder's own declaration:
--   gallica: records whose current version sits in Gallica's type set gallica:typedoc:monographies (and not in
--            gallica:typedoc:manuscrits, whose records no longer become works). Gallica's first dc:type is often 'text',
--            which the parser types article. declared_code = the set.
--   hispana: Hispana records whose own raw type is 'Impresos' (printed matter).
-- Bar (25 Nov 2026): a fresh seeded 30 + 30 visible works typed article that these rows make book, judged by #1647's
-- heritage rubric: 30/30 and 30/30 published books. Revert: DELETE ... WHERE platform IN ('gallica', 'hispana').
DELETE FROM openalex.works.record_declared_types WHERE platform IN ('gallica', 'hispana');

-- COMMAND ----------

INSERT INTO openalex.works.record_declared_types
WITH ver AS (
  SELECT native_id, endpoint_id, set_spec, raw_native_type, _sequence FROM openalex.repo.repo_enriched
  WHERE endpoint_id IN ('b6f3a90f96528af2baa', '0ef9aa4cd18142685bb') AND coalesce(_change_type, '') <> 'delete'
  UNION ALL
  SELECT native_id, endpoint_id, set_spec, raw_native_type, _sequence FROM openalex.repo.repo_enriched_reparse
  WHERE endpoint_id IN ('b6f3a90f96528af2baa', '0ef9aa4cd18142685bb') AND coalesce(_change_type, '') <> 'delete'
),
latest AS (  -- each record's current version, as apply_changes keeps it
  SELECT native_id, max_by(named_struct('endpoint_id', endpoint_id, 'set_spec', set_spec, 'rt', raw_native_type), _sequence) AS v
  FROM ver GROUP BY native_id
),
rec AS (
  SELECT native_id,
         CASE WHEN v.endpoint_id = 'b6f3a90f96528af2baa' THEN 'gallica' ELSE 'hispana' END AS platform,
         CASE WHEN v.endpoint_id = 'b6f3a90f96528af2baa' THEN 'gallica:typedoc:monographies' ELSE v.rt END AS declared_code
  FROM latest
  WHERE (v.endpoint_id = 'b6f3a90f96528af2baa' AND array_contains(coalesce(v.set_spec, array()), 'gallica:typedoc:monographies')
         AND NOT array_contains(coalesce(v.set_spec, array()), 'gallica:typedoc:manuscrits'))
     OR (v.endpoint_id = '0ef9aa4cd18142685bb' AND lower(trim(v.rt)) = 'impresos')
),
keyed AS (
  SELECT DISTINCT l.provenance, r.native_id, r.platform, r.declared_code
  FROM rec r JOIN openalex.works.locations_mapped l ON l.native_id = r.native_id AND l.provenance IN ('repo', 'repo_backfill')
)
SELECT k.provenance, k.native_id, 'book' AS type, k.platform, k.native_id AS record_key, k.declared_code, 2611 AS oxjob,
       current_timestamp() AS harvested_at
FROM keyed k
-- one row per record across every platform: the End 2 End's LEFT JOIN would otherwise repeat the work
LEFT ANTI JOIN openalex.works.record_declared_types t ON t.provenance = k.provenance AND t.native_id = k.native_id

-- COMMAND ----------

SELECT platform, provenance, declared_code, type, count(*) AS records
FROM openalex.works.record_declared_types GROUP BY ALL ORDER BY records DESC
