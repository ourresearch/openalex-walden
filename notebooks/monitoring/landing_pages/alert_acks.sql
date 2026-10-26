-- Alarm acks for the landing-page scoreboard (oxjob #1789). This file is the whole table: edit it, then run
-- `python3 scripts/landing_page_scoreboard.py ddl` (rewrites the table) and re-run alerts for the days it covers.
-- One ack per host x id_namespace x day (the latest-ending wins). An ack never hides a drop that its failure class
-- does not explain (alerts.sql header). Add one only for a failure a lane owns, with an end date, and say so in
-- the charter write log (plans/landing-pages.md section 7). Remove a row when its owner ships the fix.
INSERT OVERWRITE openalex.monitoring.landing_page_alert_acks
VALUES
  ('mdpi.com', 'doi', 'soft_block', '#1790',
   'Akamai Bot Manager challenge (meta refresh to ?bm-verify=) on ~48% of first fetches since Aug 2026 and ~100% on most weekends, a soft block since harvest-task:232 (24 Oct 2026). Re-fetches get the article 78-98% of the time (#1789 README, 25 Oct 2026).',
   DATE'2026-10-25', DATE'2026-11-30', TIMESTAMP'2026-10-25 14:00:00'),
  ('spiedigitallibrary.org', 'doi', 'soft_block', '#1790',
   'Bot wall on SPIE PDF links (the links Parseland finds on SPIE pages, fetched by TaxiCab Landing Page PDFs) since ~23 Oct 2026, a soft block since harvest-task:232; landing pages store fine (83% useful on 25 Oct). #1802 phase-2 re-fetches of SPIE pages make bursts of these links (888 on 25 Oct). #1712 breaker holds the host back (#1789 README, 26 Oct 2026).',
   DATE'2026-10-25', DATE'2026-11-30', TIMESTAMP'2026-10-26 13:30:00')
