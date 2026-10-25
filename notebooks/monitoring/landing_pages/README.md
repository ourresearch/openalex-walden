# Landing-page scoreboard (oxjob #1789)

One funnel per publisher host, from fetch to parsed fields, plus the alarm that would have caught the 16 Oct 2026
ScienceDirect drop. Charter: oxjobs `plans/landing-pages.md`. Every landing-pages lane (Parseland gate #1791,
fetch guard #1790, bronze re-check #1290) is judged on these tables and joins on the same host key.

## Host key

`openalex.monitoring.landing_page_host(url, resolved_url, native_id_namespace, native_id)`, on `taxicab_results`
columns: the registered domain of the resolved URL, else of the requested URL (doi.org never counts), with
`pdf.sciencedirectassets.com` and `linkinghub.elsevier.com` folded into `sciencedirect.com`; a DOI fetch that never
reached a publisher URL takes its prefix's dominant host (`landing_page_doi_prefixes`). Helpers:
`landing_page_netloc`, `_registered_domain`, `_netloc_host`, `_fetched_netloc`, `_doi_prefix_host`, `_host_of`.
Need a fold changed? Change `host_key.sql` here, never a private mapping in a lane.

## Tables (`openalex.monitoring`)

| table | grain | written by |
|---|---|---|
| `landing_page_funnel` | fetch_date x host x attempt (first / repeat) x id_namespace (doi / pmh) | `funnel.sql`, nightly |
| `landing_page_alerts` | alert_date x host x id_namespace x rule | `alerts.sql`, nightly |
| `landing_page_host_state` | snapshot_date x host: Crossref works' landing-page age, fresh-and-useful, fields the page adds over Crossref | `host_state.sql`, nightly (as of yesterday) |
| `landing_page_doi_prefixes` | DOI prefix -> dominant host | `doi_prefixes.sql`, by hand |
| `landing_page_zyte_prices` | Zyte domain x valid_from | `zyte_prices.sql`, by hand, monthly |
| `landing_page_alert_acks` | host x id_namespace x failure class, with owner job and end date | `alert_acks.sql`, by hand (`ddl`) |

Useful = stored PDF, or stored HTML where Parseland found authors or an abstract. Judge lanes on `attempt = 'first'`:
retry-lane and re-fetch rows fail by design. Zyte cost is an estimate: fetches x billed requests per fetch x the
domain's price (refresh from the Zyte dashboard stats API; recipe in #1290 EXPLORE 1-2).

## Alarms

`useful_rate_drop`: a host's first-attempt useful rate more than 20 points under the median of its 7 previous daily
rates, on 200+ fetches; `confirmed` on the second day running or past 40 points; pages when confirmed on one of the
50 largest DOI hosts, or on a repository (pmh) host with 3,000+ fetches that day and a drop of 40+ points (added
24 Oct 2026: ~2-3 more incidents a week, all real in the 1 Sep-23 Oct backfill). `many_hosts_drop`: 5+ of those 50 trip the same day (harvester, Zyte or Parseland trouble);
always pages. Every threshold sits in the `params` block at the top of `alerts.sql`. Backfill 1 Sep-22 Oct 2026:
~4 incidents a week, nearly all real (Zyte 400 bans, timeouts).
**Acks** (`alert_acks.sql`, added 25 Oct 2026): a known failure class on one host, owned by a lane, until an end date.
The row is still written; it does not page (or count toward `many_hosts_drop`) while the useful rate with that class
left out holds up, so a Parseland break on an acked host still pages, and `top_failure` says whether the ack applied.
First one: mdpi.com DOI soft blocks (Akamai challenge on ~48% of first fetches, ~100% most weekends), #1790, to 30 Nov.
New paging rows become a Sentry event (project `databricks-jobs`, one issue per host + rule) and a wake-up on oxjob #1789, through a desk task (`~/plexus/tasks.d/landing-page-alarms.sh`, every 30 min).

## Running it

Nightly job `Landing Page Scoreboard` (`jobs/landing_page_scoreboard.yaml`, 13:00 UTC, last 3 days). By hand:

    python3 scripts/landing_page_scoreboard.py ddl         # tables, functions, prices, acks (idempotent)
    python3 scripts/landing_page_scoreboard.py prefixes    # rebuild the DOI-prefix fold (first setup: after ddl)
    python3 scripts/landing_page_scoreboard.py funnel --start 2026-08-01 --end 2026-10-24
    python3 scripts/landing_page_scoreboard.py alerts --start 2026-09-01 --end 2026-10-24

Rebuilding the prefix fold moves rows between hosts: re-run `funnel` (and `alerts`) for 14+ days in the same sitting.
