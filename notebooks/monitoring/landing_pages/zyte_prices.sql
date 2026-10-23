-- Zyte prices per domain for the scoreboard's cost columns (oxjob #1789). Source: Zyte dashboard stats API,
-- cost per billed request 2 Oct-22 Oct 2026, and billed requests per taxicab_results row 3-21 Oct
-- (#1290 EXPLORE 1-2). IOP was $2,582 per 1M Jul-Sep. Refresh monthly: add rows with a new valid_from;
-- never edit old rows (the backfill reads the price that was valid on each fetch day).
INSERT INTO openalex.monitoring.landing_page_zyte_prices REPLACE WHERE valid_from IN (DATE'2026-07-02', DATE'2026-10-02')
VALUES
  ('*',                 DATE'2026-07-02',   40.0, 1.05, 'Zyte dashboard 2-22 Oct 2026: repositories and small journals (#1290)'),
  ('nature.com',        DATE'2026-07-02',   40.0, 1.00, 'Zyte dashboard 2-22 Oct 2026 (#1290)'),
  ('springer.com',      DATE'2026-07-02',   40.0, 1.12, 'Zyte dashboard 2-22 Oct 2026 (#1290)'),
  ('cambridge.org',     DATE'2026-07-02',   40.0, 1.05, 'Zyte dashboard 2-22 Oct 2026 (#1290)'),
  ('ieee.org',          DATE'2026-07-02',   40.0, 1.00, 'Zyte dashboard 2-22 Oct 2026 (#1290)'),
  ('oup.com',           DATE'2026-07-02',  212.0, 1.10, 'Zyte dashboard 2-22 Oct 2026 (#1290)'),
  ('acs.org',           DATE'2026-07-02',  212.0, 1.10, 'Zyte dashboard 2-22 Oct 2026 (#1290)'),
  ('doi.org',           DATE'2026-07-02',  256.0, 1.00, 'Zyte follows doi.org when the free DOI lookup fails (#1290)'),
  ('tandfonline.com',   DATE'2026-07-02',  383.0, 1.10, 'Zyte dashboard 2-22 Oct 2026 (#1290)'),
  ('sagepub.com',       DATE'2026-07-02',  383.0, 1.10, 'Zyte dashboard 2-22 Oct 2026 (#1290)'),
  ('mdpi.com',          DATE'2026-07-02',  383.0, 1.10, 'Zyte dashboard 2-22 Oct 2026 (#1290)'),
  ('handle.net',        DATE'2026-07-02',  383.0, 1.10, 'Zyte dashboard 2-22 Oct 2026 (#1290)'),
  ('sciencedirect.com', DATE'2026-07-02', 1393.0, 1.16, 'Zyte dashboard 2-22 Oct 2026 (#1290)'),
  ('wiley.com',         DATE'2026-07-02', 1126.0, 2.10, 'Zyte dashboard 2-22 Oct 2026; two-step recipe (#1290)'),
  ('iop.org',           DATE'2026-07-02', 2582.0, 1.22, 'Zyte dashboard Jul-Sep 2026 (#1290)'),
  ('iop.org',           DATE'2026-10-02', 8079.0, 1.22, 'Zyte dashboard 2-22 Oct 2026: price tripled (#1290)');
