-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Backfill PDF award matches for specific funders
-- MAGIC
-- MAGIC The daily `TagPdfAwardsIncremental` job only scans PDFs parsed inside its
-- MAGIC checkpoint window, so a funder registry ingested AFTER a PDF was parsed never
-- MAGIC gets matched against that PDF's funding sections. This notebook closes that gap
-- MAGIC for a chosen list of funders: it re-extracts funding/acknowledgement sections
-- MAGIC from the stored GROBID XML of works where the funder was already detected
-- MAGIC (`openalex.works.fulltext_work_funders`), regex-matches the funder's CURRENT
-- MAGIC award list, and inserts only new (work, funder, award) rows into
-- MAGIC `openalex.pdf.grobid_award_matches` (which WorkAwards consumes nightly).
-- MAGIC
-- MAGIC Extraction and matching semantics are copied verbatim from
-- MAGIC `TagPdfAwardsIncremental.sql` (steps 2 and 5). Idempotent: safe to re-run.
-- MAGIC
-- MAGIC To backfill more funders, extend the `backfill_funders` VALUES list.
-- MAGIC First run 2026-08-20: FCT (4320334779) after the SciPROJ 7.6k -> 99k upgrade.

-- COMMAND ----------

-- Funders to backfill (numeric funder_id as used in grobid_award_matches)
CREATE OR REPLACE TEMP VIEW backfill_funders AS
SELECT * FROM VALUES
  (4320320994),
  (4320322675),
  (4320326644),
  (4320320943),
  (4320322689),
  (4320320882),
  (4320323031),
  (4320321056),
  (4320327859),
  (4320321873),
  (4320331528),
  (4320306089),
  (4320308324),
  (4320308306),
  (4320319995),
  (4320320853),
  (4320309807),
  (4320321481),
  (4320321945),
  (4320309785),
  (4320310760),
  (4320325902),
  (4320321042),
  (4320309746),
  (4320322885),
  (4320316438),
  (4320306163),
  (4320320084),
  (4320320065),
  (4320320442),
  (4320314607),
  (4320326208),
  (4320334977),
  (4320306122),
  (4320306237),
  (4320306260),
  (4320309392),
  (4320309954),
  (4320310657),
  (4320315323),
  (4320320870),
  (4320320890),
  (4320320904),
  (4320321003),
  (4320321048),
  (4320322282),
  (4320322325),
  (4320322555),
  (4320323269),
  (4320323499),
  (4320323760),
  (4320325580),
  (4320325651),
  (4320331257),
  (4320334901),
  (4320335102),
  (4320335238),
  (4320337430)
AS t(funder_id_numeric);  -- 2026-10-01: step0 batch-1 (33) + batch-2 (25) funder ids, one run after the fold (oxjob #1451); previous runs 2026-09-24 AHA 4320306230, 2026-08-20 FCT 4320334779
