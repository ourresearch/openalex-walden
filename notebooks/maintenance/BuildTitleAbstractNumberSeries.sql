-- Databricks notebook source
-- oxjob #1162, ruling D7 (16 Nov 2026), step 3: the frozen series table for conference abstract numbers.
-- GENERATED from libraries/dlt_utils/openalex/dlt/abstract_number.py (builder_notebook_sql, rules 2026-11-19):
-- edit the module, then regenerate (its docstring says how); a test fails when this file and the module differ.
--
-- A conference abstract's leading program number or code leaves its displayed title ("#303 Risk-reducing ...",
-- "46: THE JULY EFFECT ...", "Mo1428 Hemoclip ...") only when the work's source-year holds a series of that form:
-- openalex.works.title_abstract_number_series holds those series, openalex.works.title_abstract_number_excluded takes one out.
-- Append-only: each run adds the series the rules find in titles AS DEPOSITED (openalex_works.title is never
-- stripped: the strip happens at publish) that the table lacks, dated, and never changes or deletes a row.
-- Take a series out with a row in openalex.works.title_abstract_number_excluded (source_id; year and series_key NULL = all), never a DELETE.
-- CreateWorksEnriched runs the same statements each night once the strip is switched on (#1162 PLAN, D7 steps 4
-- and 6); run this notebook by hand only to create and inspect the tables before that.
-- Measured 19 Nov 2026 on openalex_works: 6,738 series, 1,252,251 works (#1162 work/d7_step3_series_2026-11-19.md;
-- this SQL built openalex.oxlane.oxjob1162_d7_series_frozen). Revert: DROP the two tables (no reader until the switch).

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS openalex.works.title_abstract_number_excluded (
  source_id STRING NOT NULL, year INT, series_key STRING, reason STRING, oxjob STRING, added_on DATE)
COMMENT 'oxjob #1162 (ruling D7): sources (or source-years, or one series) whose leading numbers are never dropped from a title, whatever openalex.works.title_abstract_number_series says. year / series_key NULL = all.';

-- COMMAND ----------

INSERT INTO openalex.works.title_abstract_number_excluded (source_id, year, series_key, reason, oxjob, added_on)
SELECT v.source_id, CAST(NULL AS INT), CAST(NULL AS STRING), v.reason, '1162', DATE'2026-11-19'
FROM VALUES ('https://openalex.org/S4210172488', 'Zorg en Financiering: numbered news items (\'182 Beklonken\')'), ('https://openalex.org/S157670870', 'IEEE Trans Computers: review numbers (\'R68-36 Dataless Programming\')'), ('https://openalex.org/S60436882', 'IEEE Trans Electronic Computers: review numbers (\'R65-42 ...\')'), ('https://openalex.org/S188551233', 'J Stat Comput Simul: literature item numbers (\'C376. ...\')'), ('https://openalex.org/S152305454', 'Food Cosmet Toxicol: abstract-service item numbers (\'950 Triton ...\')'), ('https://openalex.org/S4210201994', 'Black Sacred Music: hymn numbers (\'60 The Crown of Life\')'), ('https://openalex.org/S96872871', 'Chinese Law & Government: document numbers (\'9: General Office of ...\')'), ('https://openalex.org/S195740279', 'Clinics in Dermatology: chapter numbers (\'21 Selected dermatoses of ...\')'), ('https://openalex.org/S90857023', 'Int Migration Review: chapter and document numbers (\'57 Sectarian ...\')'), ('https://openalex.org/S5406931023', 'merz | medien + erziehung: episode numbers (\'#28 - Sprache in den Medien\')'), ('https://openalex.org/S57227388', 'Nursing Research: abstract-service item numbers (\'427: The treatment ...\')'), ('https://openalex.org/S4306545868', 'Guowai yixue (mianyixue): article sequence numbers (\'032 KIR研究进展\')') AS v(source_id, reason)
LEFT ANTI JOIN openalex.works.title_abstract_number_excluded t ON t.source_id = v.source_id AND t.year IS NULL AND t.series_key IS NULL;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS openalex.works.title_abstract_number_series (
  source_id STRING, year INT, form STRING, series_key STRING, n_titles BIGINT, n_distinct BIGINT,
  n_supplement BIGINT, n_conference_abstract BIGINT, n_pass BIGINT, n_pass_latin BIGINT, n_pass_short BIGINT,
  n_pass_year_number BIGINT, n_pass_paged BIGINT, median_pages INT, n_pass_paged_first_pages BIGINT,
  added_on DATE, rule_version STRING)
COMMENT 'oxjob #1162 (ruling D7): source-years whose leading conference abstract numbers the displayed title drops. Append-only (openalex.dlt.abstract_number.series_append_sql); take a series out with a row in openalex.works.title_abstract_number_excluded.';

-- COMMAND ----------

INSERT INTO openalex.works.title_abstract_number_series (source_id, year, form, series_key, n_titles, n_distinct, n_supplement, n_conference_abstract, n_pass, n_pass_latin, n_pass_short, n_pass_year_number, n_pass_paged, median_pages, n_pass_paged_first_pages, added_on, rule_version)
SELECT s.source_id, s.year, s.form, s.series_key, s.n_titles, s.n_distinct, s.n_supplement, s.n_conference_abstract, s.n_pass, s.n_pass_latin, s.n_pass_short, s.n_pass_year_number, s.n_pass_paged, s.median_pages, s.n_pass_paged_first_pages, current_date(), '2026-11-19'
FROM (WITH w AS (
  SELECT w.primary_location.source.id AS source_id, w.publication_year AS year, w.type, w.title AS title,
         w.biblio.first_page AS fp, w.biblio.last_page AS lp, w.biblio.issue AS issue
  FROM openalex.works.openalex_works w
  WHERE w.primary_location.source.type IN ('journal', 'conference')
    AND w.type NOT IN ('book', 'book-chapter', 'dataset', 'reference-entry', 'dissertation', 'book-review', 'editorial', 'paratext', 'erratum', 'retraction')
    AND (w.title RLIKE '^[#0-9]' OR substr(w.title, 1, 12) RLIKE '^[A-Za-z]{1,6}[0-9]{0,2}-?[0-9]')
), f AS (
  SELECT *, CASE WHEN title RLIKE '^[0-9]{4}-[A-Z]{2,4}-[0-9]{1,5}-[A-Z]{2,8}\\s+\\S' THEN 'code' WHEN title RLIKE '^#\\s?[0-9]{1,6}(\\s*[:.–—-]\\s*|\\s+)\\S' THEN 'hash' WHEN title RLIKE '^[0-9]{1,6}:\\s+\\S' THEN 'num_colon' WHEN title RLIKE '^[0-9]{1,5}\\.\\s+\\p{Lu}' THEN 'num_dot' WHEN title RLIKE '^[0-9]{1,5}\\s+\\p{Lu}' THEN 'num_space' WHEN title RLIKE '^[A-Za-z]{1,6}[0-9]{0,2}-?[0-9]{1,5}(?:[.-][0-9]{1,4})?[A-Za-z]?\\s*[:.–—-]?\\s+\\p{Lu}' THEN 'letter_code' END AS form FROM w
), p AS (
  SELECT *, CASE form WHEN 'code' THEN regexp_extract(title, '^([0-9]{4}-[A-Z]{2,4}-[0-9]{1,5}-[A-Z]{2,8}\\s+)', 1) WHEN 'hash' THEN regexp_extract(title, '^(#\\s?[0-9]{1,6}(\\s*[:.–—-]\\s*|\\s+))', 1) WHEN 'num_colon' THEN regexp_extract(title, '^([0-9]{1,6}:\\s+)', 1) WHEN 'num_dot' THEN regexp_extract(title, '^([0-9]{1,5}\\.\\s+)', 1) WHEN 'num_space' THEN regexp_extract(title, '^([0-9]{1,5}\\s+)', 1) WHEN 'letter_code' THEN regexp_extract(title, '^([A-Za-z]{1,6}[0-9]{0,2}-?[0-9]{1,5}(?:[.-][0-9]{1,4})?[A-Za-z]?\\s*[:.–—-]?\\s+)', 1) END AS prefix FROM f WHERE form IS NOT NULL
), k AS (
  SELECT *, CASE WHEN form = 'letter_code' THEN concat('letter_code:', regexp_replace(regexp_replace(prefix, '[\\s:.–—-]+$', ''), '[0-9]+', '9')) ELSE form END AS series_key,
    CASE WHEN form = 'letter_code' THEN regexp_replace(prefix, '[\\s:.–—-]+$', '')
         ELSE CAST(CAST(regexp_extract(title, '([0-9]+)', 1) AS INT) AS STRING) END AS num,
    trim(substr(title, length(prefix) + 1)) AS rest,
    CASE WHEN regexp_extract(fp, '^([A-Za-z]*)', 1) = regexp_extract(lp, '^([A-Za-z]*)', 1)
          AND fp RLIKE '^[A-Za-z]*[0-9]{1,6}$' AND lp RLIKE '^[A-Za-z]*[0-9]{1,6}$'
          AND CAST(regexp_extract(lp, '([0-9]+)$', 1) AS INT) >= CAST(regexp_extract(fp, '([0-9]+)$', 1) AS INT)
         THEN CAST(regexp_extract(lp, '([0-9]+)$', 1) AS INT) - CAST(regexp_extract(fp, '([0-9]+)$', 1) AS INT) + 1
    END AS span
  FROM p
), g AS (
  SELECT *, coalesce((form NOT IN ('code', 'letter_code') AND CAST(regexp_extract(prefix, '([0-9]+)', 1) AS INT) BETWEEN year - 1 AND year + 1) OR (form IN ('num_colon', 'num_dot', 'num_space') AND (regexp_extract(substr(title, length(prefix) + 1), '^(\\S+)', 1) RLIKE '/' OR substr(title, length(prefix) + 1) RLIKE '^(?i)(years?|yrs?|months?|weeks?|days?|hours?|hrs?|h|minutes?|min|seconds?|sec|s|reasons|ways|tips|things|steps|questions|lessons|rules|myths|ideas|keys|facts|million|millions|billion|thousand|hundred|percent|per|x|×|times|fold|of|to|and|or|vs\\.?|versus|january|february|march|april|may|june|july|august|september|october|november|december)\\b' OR substr(title, length(prefix) + 1) RLIKE '^(?:[GMTPkmµn]?(?:b|B|bit|bits|bps|Bps|byte|bytes|Hz|W|V|eV|Pa|J|N|Ω|K|T|g|m|l|L|M|bar|Gy|Sv|Bq|Ci|D|G)(?:/s)?|C|O|F|P|S|CO|CO2|NO|Tc|Ga|Fe|Cu|Zn|Ca|Na|Cl|Br|Co|Xe|Rb|Sr|Lu|Ra|Th|U|Pu|Am)(?=[\\s,.:;)\\-]|$)')) OR (form = 'letter_code' AND (upper(regexp_extract(prefix, '^([A-Za-z]+)', 1)) IN ('ABCB', 'ABCC', 'ABCG', 'ABT', 'ADAM', 'ADAMTS', 'ADP', 'ADRB', 'AG', 'AKT', 'ALK', 'AMD', 'AMG', 'AMP', 'AMPA', 'APO', 'APOE', 'ARQ', 'ATG', 'ATP', 'AZD', 'BAY', 'BCL', 'BIBF', 'BMP', 'BMS', 'BRCA', 'CASP', 'CCL', 'CCR', 'CD', 'CDK', 'CDKN', 'CGP', 'CH', 'CI', 'CK', 'CMV', 'CO', 'COL', 'COVID', 'CTLA', 'CX', 'CXCL', 'CXCR', 'CYP', 'DENV', 'DNA', 'DPP', 'DRD', 'EBV', 'EGF', 'EGFR', 'EPO', 'ERBB', 'ERK', 'FCGR', 'FGF', 'FGFR', 'FLT', 'FOX', 'FOXP', 'GABA', 'GATA', 'GDC', 'GLI', 'GLP', 'GPR', 'GSDM', 'GSK', 'GTP', 'GW', 'HBV', 'HCV', 'HDAC', 'HER', 'HHV', 'HIV', 'HLA', 'HOX', 'HPV', 'HSP', 'HSPA', 'HSV', 'HTLV', 'HTR', 'IFN', 'IGA', 'IGE', 'IGF', 'IGFBP', 'IGG', 'IGM', 'IL', 'INCB', 'IRF', 'JAK', 'JNJ', 'KIF', 'KIR', 'KLF', 'KRAS', 'KRT', 'KU', 'LAG', 'LC', 'LINC', 'LNC', 'LY', 'MAPK', 'MCL', 'MDM', 'MEK', 'MERS', 'MET', 'MHC', 'MIR', 'MIRNA', 'MK', 'MLN', 'MMP', 'MUC', 'MYC', 'NF', 'NKG', 'NLRP', 'NMDA', 'NO', 'NOD', 'NOTCH', 'NOX', 'NPM', 'NRAS', 'NSC', 'NTRK', 'NVP', 'ONO', 'OSI', 'PARP', 'PAX', 'PCI', 'PDGF', 'PDGFR', 'PDL', 'PF', 'PI', 'PKC', 'PLK', 'PM', 'PPAR', 'PTEN', 'RET', 'RG', 'RNA', 'RO', 'ROS', 'RUNX', 'SARS', 'SB', 'SCH', 'SGLT', 'SIRT', 'SIV', 'SLC', 'SO', 'SOX', 'SR', 'STAT', 'TAK', 'TCR', 'TGF', 'TIM', 'TLR', 'TNF', 'TRK', 'TRPA', 'TRPC', 'TRPM', 'TRPV', 'UGT', 'VEGF', 'VEGFR', 'WAY', 'WNT', 'ZD', 'ZIKV') OR regexp_extract(prefix, '^([A-Za-z]+)', 1) IN ('SU'))), false) AS guarded FROM k
), a AS (
  SELECT source_id, year, form, series_key, count(*) AS n_titles, count(DISTINCT num) AS n_distinct,
    count_if(fp RLIKE '^[A-Za-z]' OR lower(coalesce(issue, '')) RLIKE 'sup|s[0-9]|[0-9]s') AS n_supplement,
    count_if(type = 'conference-abstract') AS n_conference_abstract,
    count_if(NOT guarded) AS n_pass,
    count_if(NOT guarded AND rest RLIKE '^[\\p{IsLatin}\\p{P}\\p{S}\\s0-9]+$') AS n_pass_latin,
    count_if(NOT guarded AND rest RLIKE '^[\\p{IsLatin}\\p{P}\\p{S}\\s0-9]+$' AND size(split(rest, '\\s+')) <= 4) AS n_pass_short,
    count_if(NOT guarded AND form IN ('num_colon', 'num_dot', 'num_space', 'hash')
             AND CAST(regexp_extract(prefix, '([0-9]+)', 1) AS INT) BETWEEN 1800 AND year + 1)
      AS n_pass_year_number,
    count_if(NOT guarded AND span IS NOT NULL) AS n_pass_paged,
    percentile_approx(CASE WHEN NOT guarded THEN span END, 0.5) AS median_pages,
    count(DISTINCT CASE WHEN NOT guarded AND span IS NOT NULL THEN fp END) AS n_pass_paged_first_pages
  FROM g
  GROUP BY source_id, year, form, series_key
  HAVING count(*) >= 20
    AND (form = 'code'
         OR (form = 'letter_code' AND count(DISTINCT num) >= 0.9 * count(*))
         OR (form NOT IN ('code', 'letter_code') AND count(DISTINCT num) >= 0.5 * count(*)))
    AND (form <> 'num_dot'
         OR count_if(fp RLIKE '^[A-Za-z]' OR lower(coalesce(issue, '')) RLIKE 'sup|s[0-9]|[0-9]s') >= 0.5 * count(*)
         OR count_if(type = 'conference-abstract') >= 0.5 * count(*))
    AND count_if(NOT guarded) >= 20
)
SELECT a.* FROM a
LEFT ANTI JOIN openalex.works.title_abstract_number_excluded e ON e.source_id = a.source_id AND (e.year IS NULL OR e.year = a.year) AND (e.series_key IS NULL OR e.series_key = a.series_key)
WHERE NOT (a.n_pass_latin >= 0.5 * a.n_pass
       AND a.n_pass_short >= 0.5 * a.n_pass_latin)
  AND a.n_pass_year_number < 0.5 * a.n_pass
  AND NOT (a.n_pass_paged >= 0.5 * a.n_pass AND coalesce(a.median_pages, 0) >= 10
           AND a.n_pass_paged_first_pages >= 0.5 * a.n_pass_paged)) s
LEFT ANTI JOIN openalex.works.title_abstract_number_series t ON t.source_id <=> s.source_id AND t.year <=> s.year AND t.series_key <=> s.series_key;

-- COMMAND ----------

SELECT rule_version, added_on, count(*) AS series, sum(n_pass) AS works
FROM openalex.works.title_abstract_number_series
GROUP BY rule_version, added_on ORDER BY added_on;
