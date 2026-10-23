-- Scoreboard alarms for fetch days [{start}, {end}) (oxjob #1789). Rewrites exactly those days, atomically.
-- One statement, no trailing semicolon: the notebook runs the file as is. Every threshold is in `params`.
--
-- First attempts only: the retry lane (live since 18 Oct 2026) and re-fetch campaigns fail by design.
-- Per host x id_namespace (doi = publisher landing pages, pmh = repository records; mixing them made
-- cambridge.org trip on 12 Sep 2026 when repository links to non-article pages arrived).
--
-- useful_rate_drop: that day's useful rate is more than `drop_line` under the MEDIAN of the host's daily rates over
--   the 7 days before (days with >= `baseline_day_min` fetches, >= `baseline_days_min` such days), on >= `day_min`
--   fetches. A median, not a pooled rate: campaign days of 100K+ fetches would otherwise set the baseline alone
--   (springer.com, 16-17 Sep 2026).
-- severity confirmed = it also tripped the day before, or the drop is over `confirm_line`. page = confirmed on one
--   of the `top_n` largest DOI hosts by first-attempt fetches over the 30 days before.
-- many_hosts_drop: `many_hosts` or more of those hosts trip the same day (any severity): a harvester, Zyte or
--   Parseland problem rather than one publisher. Always pages. (A corpus-wide rate was tried first: one big campaign
--   on a failing host, Cairn 17-21 Sep 2026, dragged it under the line on its own.)
INSERT INTO openalex.monitoring.landing_page_alerts
REPLACE WHERE alert_date >= DATE'{start}' AND alert_date < DATE'{end}'
WITH params AS (
  SELECT 0.20 drop_line, 0.40 confirm_line, 200 day_min, 50 baseline_day_min, 3 baseline_days_min, 50 top_n,
         5 many_hosts
),
hosts AS (
  SELECT fetch_date, host, id_namespace, sum(fetches) fetches, sum(stored) stored, sum(useful) / sum(fetches) rate,
    -- largest failure class that day (stored_but_not_useful = stored HTML without authors or abstract)
    array_max(array(
      named_struct('n', sum(fail_soft_block), 'k', 'soft_block'), named_struct('n', sum(fail_403), 'k', '403'),
      named_struct('n', sum(fail_429), 'k', '429'), named_struct('n', sum(fail_5xx), 'k', '5xx'),
      named_struct('n', sum(fail_timeout), 'k', 'timeout'), named_struct('n', sum(fail_400), 'k', '400'),
      named_struct('n', sum(fail_404), 'k', '404'), named_struct('n', sum(fail_other), 'k', 'other'),
      named_struct('n', sum(stored) - sum(useful), 'k', 'stored_but_not_useful'))) top_fail
  FROM openalex.monitoring.landing_page_funnel
  WHERE attempt = 'first' AND fetch_date >= DATE'{start}' - INTERVAL 31 DAYS AND fetch_date < DATE'{end}'
  GROUP BY 1, 2, 3
),
-- the top_n largest DOI hosts, by first-attempt fetches over the 30 days before each alert day
ranked AS (
  SELECT fetch_date, host FROM (
    SELECT d.fetch_date, h.host, rank() OVER (PARTITION BY d.fetch_date ORDER BY sum(h.fetches) DESC) rk
    FROM (SELECT DISTINCT fetch_date FROM hosts WHERE fetch_date >= DATE'{start}' - INTERVAL 1 DAY) d
    JOIN hosts h ON h.id_namespace = 'doi' AND h.fetch_date >= d.fetch_date - INTERVAL 30 DAYS AND h.fetch_date < d.fetch_date
    GROUP BY d.fetch_date, h.host
  ), params
  WHERE rk <= top_n
),
b AS (
  SELECT d.fetch_date, d.host, d.id_namespace, d.fetches, d.stored, d.rate, d.top_fail,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY CASE WHEN p.fetches >= (SELECT baseline_day_min FROM params) THEN p.rate END) baseline_rate,
    count(CASE WHEN p.fetches >= (SELECT baseline_day_min FROM params) THEN 1 END) baseline_days
  FROM hosts d
  JOIN hosts p ON p.host = d.host AND p.id_namespace = d.id_namespace
          AND p.fetch_date >= d.fetch_date - INTERVAL 7 DAYS AND p.fetch_date < d.fetch_date
  WHERE d.fetch_date >= DATE'{start}' - INTERVAL 1 DAY AND d.fetches >= (SELECT day_min FROM params)
  GROUP BY ALL
),
trips AS (
  SELECT b.* FROM b, params
  WHERE baseline_days >= baseline_days_min AND baseline_rate - rate > drop_line
),
x AS (
  SELECT t.*,
    (t.baseline_rate - t.rate > (SELECT confirm_line FROM params) OR y.host IS NOT NULL) confirmed,
    (r.host IS NOT NULL AND t.id_namespace = 'doi') top
  FROM trips t
  LEFT JOIN trips y ON y.host = t.host AND y.id_namespace = t.id_namespace AND y.fetch_date = t.fetch_date - INTERVAL 1 DAY
  LEFT JOIN ranked r ON r.host = t.host AND r.fetch_date = t.fetch_date
  WHERE t.fetch_date >= DATE'{start}'
)
SELECT fetch_date alert_date, host, id_namespace, 'useful_rate_drop' rule,
  CASE WHEN confirmed THEN 'confirmed' ELSE 'watch' END severity,
  confirmed AND top page,
  fetches, rate useful_rate, baseline_rate, CAST(baseline_days AS INT) baseline_days,
  round(100 * (baseline_rate - rate), 1) drop_points, stored / fetches stored_rate,
  concat(top_fail.k, ' ', top_fail.n) top_failure,
  current_timestamp() computed_at
FROM x
UNION ALL
SELECT fetch_date, '(top 50 DOI hosts)', 'doi', 'many_hosts_drop', 'confirmed', true,
  sum(fetches), sum(rate * fetches) / sum(fetches), NULL, CAST(count(*) AS INT), NULL, sum(stored) / sum(fetches),
  concat(count(*), ' hosts: ', array_join(slice(array_sort(collect_list(host)), 1, 12), ', ')),
  current_timestamp()
FROM x
WHERE top
GROUP BY fetch_date
HAVING count(*) >= (SELECT many_hosts FROM params)
