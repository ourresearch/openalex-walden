-- Scoreboard alarms for fetch days [{start}, {end}) (oxjob #1789). Rewrites exactly those days, atomically.
-- One statement, no trailing semicolon: the notebook runs the file as is. Every threshold is in `params`.
--
-- First attempts only: the retry lane (live since 18 Oct 2026) and re-fetch campaigns fail by design.
-- Per host x id_namespace (doi = publisher landing pages, pmh = repository records; mixing them made
-- cambridge.org trip on 12 Sep 2026 when repository links to non-article pages arrived) x kind:
--   page = landing pages (doi: the doi.org URL; pmh: every record URL); rules useful_rate_drop, many_hosts_drop.
--   link = doi fetches of any other URL: PDF links Parseland found on our pages (TaxiCab Landing Page PDFs) and
--          OA-location URLs a campaign re-checks; rules link_useful_drop, many_hosts_link_drop.
--   Judged apart since 27 Oct 2026: a re-parse or campaign makes thousands of links first fetches in a burst, at
--   their own lower rate, and that mix shift alone paged 7 hosts on 26 Oct 2026 (#1802's freshness phase 1 PDF
--   links, #1290's bronze pass on acs.org) while every one of their landing-page rates held.
--
-- useful_rate_drop / link_useful_drop: that day's useful rate is more than `drop_line` under the MEDIAN of the
--   host's daily rates (same kind) over the 7 days before (days with >= `baseline_day_min` fetches, >=
--   `baseline_days_min` such days), on >= `day_min` fetches. A median, not a pooled rate: campaign days of 100K+
--   fetches would otherwise set the baseline alone (springer.com, 16-17 Sep 2026).
-- severity confirmed = it also tripped the day before, or the drop is over `confirm_line`. page = confirmed on one
--   of the `top_n` largest DOI hosts by first-attempt fetches over the 30 days before, or confirmed on a repository
--   (pmh) host with >= `repo_day_min` fetches that day and a drop of >= `repo_line`. The repository bar would have
--   opened ~18 incidents in 1 Sep-23 Oct 2026, all real (Zyte 400 bans, shell pages, 5xx); without it Leiden
--   (95% -> 17%) and luke.fi (95% -> 0%) on 20-22 Oct 2026 reached nobody.
-- many_hosts_drop / many_hosts_link_drop: `many_hosts` or more of those hosts trip the same day on that kind (any
--   severity): a harvester, Zyte or Parseland problem rather than one publisher. Always pages. (A corpus-wide rate
--   was tried first: one big campaign on a failing host, Cairn 17-21 Sep 2026, dragged it under the line on its own.)
-- acks (landing_page_alert_acks, rows in alert_acks.sql): a known failure class on one host x namespace (both
--   kinds), owned by a lane, until a date. An acked row is still written but does not page, and does not count
--   toward many_hosts_*, when the day's useful rate with that class left out of the fetches is within `drop_line`
--   of the same median with that class left out (or fewer than `baseline_day_min` fetches are left to judge). So a
--   Parseland break on an acked host still pages. top_failure says when an ack applied, or that it did not explain
--   the drop. First ack: MDPI's Akamai challenge (~48% of first fetches since Aug 2026, ~100% on most weekends), #1790.
INSERT INTO openalex.monitoring.landing_page_alerts
REPLACE WHERE alert_date >= DATE'{start}' AND alert_date < DATE'{end}'
WITH params AS (
  SELECT 0.20 drop_line, 0.40 confirm_line, 200 day_min, 50 baseline_day_min, 3 baseline_days_min, 50 top_n,
         5 many_hosts, 3000 repo_day_min, 0.40 repo_line
),
f AS (
  SELECT fetch_date, host, id_namespace,
    sum(fetches) fetches, sum(stored) stored, sum(useful) useful,
    sum(fail_soft_block) sb, sum(fail_403) f403, sum(fail_429) f429, sum(fail_5xx) f5xx, sum(fail_timeout) ftmo,
    sum(fail_400) f400, sum(fail_404) f404, sum(fail_other) foth,
    -- coalesce: rows written before the link columns existed count as all landing pages
    coalesce(sum(link_fetches), 0) l_fetches, coalesce(sum(link_stored), 0) l_stored, coalesce(sum(link_useful), 0) l_useful,
    coalesce(sum(link_fail_soft_block), 0) l_sb, coalesce(sum(link_fail_403), 0) l_f403,
    coalesce(sum(link_fail_429), 0) l_f429, coalesce(sum(link_fail_5xx), 0) l_f5xx,
    coalesce(sum(link_fail_timeout), 0) l_ftmo, coalesce(sum(link_fail_400), 0) l_f400,
    coalesce(sum(link_fail_404), 0) l_f404, coalesce(sum(link_fail_other), 0) l_foth
  FROM openalex.monitoring.landing_page_funnel
  WHERE attempt = 'first' AND fetch_date >= DATE'{start}' - INTERVAL 31 DAYS AND fetch_date < DATE'{end}'
  GROUP BY 1, 2, 3
),
k AS (
  SELECT fetch_date, host, id_namespace, 'page' kind, fetches - l_fetches fetches, stored - l_stored stored,
    useful - l_useful useful, sb - l_sb sb, f403 - l_f403 f403, f429 - l_f429 f429, f5xx - l_f5xx f5xx,
    ftmo - l_ftmo ftmo, f400 - l_f400 f400, f404 - l_f404 f404, foth - l_foth foth
  FROM f
  UNION ALL
  SELECT fetch_date, host, id_namespace, 'link', l_fetches, l_stored, l_useful, l_sb, l_f403, l_f429, l_f5xx,
    l_ftmo, l_f400, l_f404, l_foth
  FROM f WHERE id_namespace = 'doi' AND l_fetches > 0
),
hosts0 AS (
  SELECT fetch_date, host, id_namespace, kind, fetches, stored, useful, try_divide(useful, fetches) rate,
    -- every failure class by the names top_failure and the acks use (stored_but_not_useful = stored HTML without
    -- authors or abstract); top_fail below is the largest of them that day
    map('soft_block', sb, '403', f403, '429', f429, '5xx', f5xx, 'timeout', ftmo, '400', f400, '404', f404,
        'other', foth, 'stored_but_not_useful', stored - useful) fails
  FROM k
),
hosts AS (
  SELECT h.*, array_max(transform(map_entries(fails), e -> named_struct('n', e.value, 'k', e.key))) top_fail
  FROM hosts0 h
),
-- the top_n largest DOI hosts, by first-attempt fetches (both kinds) over the 30 days before each alert day
ranked AS (
  SELECT fetch_date, host FROM (
    SELECT d.fetch_date, h.host, rank() OVER (PARTITION BY d.fetch_date ORDER BY sum(h.fetches) DESC) rk
    FROM (SELECT DISTINCT fetch_date FROM f WHERE fetch_date >= DATE'{start}' - INTERVAL 1 DAY) d
    JOIN f h ON h.id_namespace = 'doi' AND h.fetch_date >= d.fetch_date - INTERVAL 30 DAYS AND h.fetch_date < d.fetch_date
    GROUP BY d.fetch_date, h.host
  ), params
  WHERE rk <= top_n
),
-- each candidate day with the ack covering it, if any (one per host x namespace x kind x day: the latest-ending one)
days AS (
  SELECT h.fetch_date, h.host, h.id_namespace, h.kind, h.fetches, h.stored, h.useful, h.rate, h.top_fail,
    k.failure ack_failure, k.owner_job ack_owner, k.until_date ack_until, h.fails[k.failure] ack_n
  FROM hosts h
  LEFT JOIN openalex.monitoring.landing_page_alert_acks k
    ON k.host = h.host AND k.id_namespace = h.id_namespace AND h.fetch_date BETWEEN k.from_date AND k.until_date
  WHERE h.fetch_date >= DATE'{start}' - INTERVAL 1 DAY AND h.fetches >= (SELECT day_min FROM params)
  QUALIFY row_number() OVER (PARTITION BY h.fetch_date, h.host, h.id_namespace, h.kind ORDER BY k.until_date DESC) = 1
),
b AS (
  SELECT d.fetch_date, d.host, d.id_namespace, d.kind, d.fetches, d.stored, d.useful, d.rate, d.top_fail,
    d.ack_failure, d.ack_owner, d.ack_until, d.ack_n,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY CASE WHEN p.fetches >= (SELECT baseline_day_min FROM params) THEN p.rate END) baseline_rate,
    count(CASE WHEN p.fetches >= (SELECT baseline_day_min FROM params) THEN 1 END) baseline_days,
    -- the same median with the acked class left out of each day's fetches (NULL when no ack)
    percentile_cont(0.5) WITHIN GROUP (ORDER BY CASE WHEN p.fetches - p.fails[d.ack_failure] >= (SELECT baseline_day_min FROM params)
      THEN try_divide(p.useful, p.fetches - p.fails[d.ack_failure]) END) ack_baseline_rate
  FROM days d
  JOIN hosts p ON p.host = d.host AND p.id_namespace = d.id_namespace AND p.kind = d.kind
          AND p.fetch_date >= d.fetch_date - INTERVAL 7 DAYS AND p.fetch_date < d.fetch_date
  GROUP BY ALL
),
trips AS (
  SELECT b.* FROM b, params
  WHERE baseline_days >= baseline_days_min AND baseline_rate - rate > drop_line
),
x AS (
  SELECT t.*,
    (t.baseline_rate - t.rate > (SELECT confirm_line FROM params) OR y.host IS NOT NULL) confirmed,
    (r.host IS NOT NULL AND t.id_namespace = 'doi') top,
    (t.id_namespace = 'pmh' AND t.fetches >= (SELECT repo_day_min FROM params)
     AND t.baseline_rate - t.rate >= (SELECT repo_line FROM params)) big_repo,
    -- explained by its ack: too few fetches left outside the acked class, or the rest holds up (coalesce: an
    -- unknown failure name in an ack must never silence a page)
    coalesce(t.ack_failure IS NOT NULL AND (
      t.fetches - t.ack_n < (SELECT baseline_day_min FROM params)
      OR try_divide(t.useful, t.fetches - t.ack_n)
         >= coalesce(t.ack_baseline_rate, t.baseline_rate) - (SELECT drop_line FROM params)), false) acked
  FROM trips t
  LEFT JOIN trips y ON y.host = t.host AND y.id_namespace = t.id_namespace AND y.kind = t.kind
                   AND y.fetch_date = t.fetch_date - INTERVAL 1 DAY
  LEFT JOIN ranked r ON r.host = t.host AND r.fetch_date = t.fetch_date
  WHERE t.fetch_date >= DATE'{start}'
)
SELECT fetch_date alert_date, host, id_namespace,
  CASE WHEN kind = 'link' THEN 'link_useful_drop' ELSE 'useful_rate_drop' END rule,
  CASE WHEN confirmed THEN 'confirmed' ELSE 'watch' END severity,
  confirmed AND (top OR big_repo) AND NOT acked page,
  fetches, rate useful_rate, baseline_rate, CAST(baseline_days AS INT) baseline_days,
  round(100 * (baseline_rate - rate), 1) drop_points, stored / fetches stored_rate,
  concat(top_fail.k, ' ', top_fail.n,
    CASE WHEN acked THEN concat(' (acked: ', ack_failure, ', ', ack_owner, ', until ', ack_until, ')')
         WHEN ack_failure IS NOT NULL THEN concat(' (ack on ', ack_failure, ' by ', ack_owner, ' does not explain it)')
         ELSE '' END) top_failure,
  current_timestamp() computed_at
FROM x
UNION ALL
SELECT fetch_date, '(top 50 DOI hosts)', 'doi',
  CASE WHEN kind = 'link' THEN 'many_hosts_link_drop' ELSE 'many_hosts_drop' END, 'confirmed', true,
  sum(fetches), sum(rate * fetches) / sum(fetches), NULL, CAST(count(*) AS INT), NULL, sum(stored) / sum(fetches),
  concat(count(*), ' hosts: ', array_join(slice(array_sort(collect_list(host)), 1, 12), ', ')),
  current_timestamp()
FROM x
WHERE top AND NOT acked
GROUP BY fetch_date, kind
HAVING count(*) >= (SELECT many_hosts FROM params)
