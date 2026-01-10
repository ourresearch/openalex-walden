-- Timeline Query: Show parser success rate by date
-- This query was used to identify the Dec 27 - Jan 3 regression period

SELECT
    DATE(processed_date) as process_date,
    COUNT(*) as total,
    SUM(CASE WHEN size(parser_response.authors) > 0 THEN 1 ELSE 0 END) as with_authors,
    ROUND(100.0 * SUM(CASE WHEN size(parser_response.authors) > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct
FROM openalex.landing_page.taxicab_enriched_new
WHERE processed_date >= '2025-12-20'
  AND native_id LIKE '10.1016/%'
GROUP BY DATE(processed_date)
ORDER BY process_date DESC;

-- Results from 2026-01-09:
--
-- | process_date | total  | with_authors | pct  |
-- |--------------|--------|--------------|------|
-- | 2026-01-09   | 61,000 | 49,000      | 80.3 |
-- | 2026-01-08   | 58,000 | 43,000      | 74.1 |
-- | 2026-01-07   | 52,000 | 41,000      | 78.8 |
-- | 2026-01-06   | 48,000 | 38,000      | 79.2 |
-- | 2026-01-05   | 47,000 | 35,000      | 74.5 |
-- | 2026-01-04   | 79,000 | 58,000      | 73.4 | <-- Fix deployed
-- | 2026-01-03   | 59,000 | 5,000       |  8.5 |
-- | 2026-01-02   | 83,000 | 10,000      | 12.0 |
-- | 2026-01-01   | 77,000 | 6,000       |  7.8 |
-- | 2025-12-31   | 63,000 | 6,000       |  9.5 |
-- | 2025-12-30   | 62,000 | 6,000       |  9.7 |
-- | 2025-12-29   | 61,000 | 7,000       | 11.5 |
-- | 2025-12-28   | 60,000 | 8,000       | 13.3 |
-- | 2025-12-27   | 59,000 | 8,000       | 13.6 | <-- Regression begins
-- | 2025-12-26   | 61,000 | 35,000      | 57.4 |
-- | 2025-12-25   | 56,000 | 18,000      | 32.1 |
-- | 2025-12-24   | 61,000 | 31,000      | 50.8 |
