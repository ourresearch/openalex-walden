#!/usr/bin/env python3
"""
FULL DOE Office of Science (CFDA 81.049) grant pull via USAspending, DATE-SLICED.
HARDENED / RESILIENT variant of /tmp/doe_full.py.

WHY date-slicing: POST /api/v2/search/spending_by_award/ enforces a deep-pagination
ceiling of page*limit <= 10,000. The whole-window (2008-2026) DOE 81.049 set is
~20,931 grants, so a single query truncates to the top-10k-by-dollar. We instead loop
year-by-year (each year is well under 10k), concatenate, and dedup by Award ID (FAIN).

time_period semantics: with NO date_type, USAspending matches awards whose period of
performance OVERLAPS the [start,end] window. A multi-year award therefore appears in
every year it spans -> naive concat double-counts -> dedup-by-FAIN is mandatory (and is
exactly how we reassemble the full unique universe without hitting the 10k cap).

RESILIENCE (vs /tmp/doe_full.py which crashed on a RemoteDisconnected mid-run):
  1. Each PAGE retries up to 10x with EXPONENTIAL backoff (2,4,8,...,60s cap).
  2. Each YEAR is wrapped in try/except: if a page still fails after all retries, the
     whole year is LOGGED as SKIPPED and the run CONTINUES (never raises).
  3. Polite ~0.4s delay between page requests.
  4. Parquet is written ONCE at the very end from accumulated rows (no truncated file
     on a mid-run crash). Per-year row lists are also dumped to /tmp/doe_years/ so a
     re-run could resume — but a clean single run is the goal.

Field mapping reused from the scout. Institution-level: recipient = org, no named PI
(pi_* = null). Assistance only (award types 02-05).

READ-ONLY / HEADLESS: writes only to /tmp.
"""
import json
import os
import pickle
import sys
import time
import http.client
import urllib.request
import urllib.error

import pandas as pd

API = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
AWARD_TYPE_CODES = ["02", "03", "04", "05"]  # grants + cooperative agreements
PROGRAM_NUMBERS = ["81.049"]                 # Office of Science Financial Assistance Program
YEARS = range(2008, 2027)                    # 2008..2026 inclusive
FIELDS = [
    "Award ID", "Recipient Name", "Award Amount", "Start Date", "End Date",
    "Awarding Agency", "Awarding Sub Agency", "Description", "Award Type",
    "prime_award_recipient_id",
]
LIMIT = 100
# Hard ceiling per (year-or-month) slice. 100 pages*100 = 10k = the API cap itself.
MAX_PAGES = 100
OUT = "/tmp/doe_sc_grants.parquet"
YEARS_DIR = "/tmp/doe_years"          # intermediate per-year dumps (resume aid)

# Page-level retry policy.
PAGE_RETRIES = 10                     # retry a failing PAGE up to 10x
BACKOFF_CAP = 60.0                    # exponential backoff cap (seconds)
POLITE_DELAY = 0.4                    # delay between successful page requests (s)

# Network errors worth retrying (urllib + low-level http.client disconnects).
_NET_ERRS = (
    urllib.error.HTTPError, urllib.error.URLError, TimeoutError,
    http.client.RemoteDisconnected, http.client.BadStatusLine,
    http.client.IncompleteRead, ConnectionError, OSError,
)


class PageFailure(Exception):
    """Raised when a single page can't be fetched after PAGE_RETRIES attempts."""


def _post(body, retries=PAGE_RETRIES):
    """POST one page. Retry up to `retries`x with exponential backoff (2,4,8,...,cap).

    Raises PageFailure if every attempt fails (caller decides whether to skip the year).
    """
    data = json.dumps(body).encode()
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                API, data=data, headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read())
        except _NET_ERRS as e:
            last = e
            # exponential backoff: 2,4,8,16,32,60,60,... (capped)
            wait = min(2.0 * (2 ** attempt), BACKOFF_CAP)
            print(
                f"      [retry] page POST failed (attempt {attempt + 1}/{retries}): "
                f"{type(e).__name__}: {e} -> sleeping {wait:.0f}s",
                flush=True,
            )
            time.sleep(wait)
        except Exception as e:  # noqa: BLE001 - unexpected, retry a few then give up
            last = e
            wait = min(2.0 * (2 ** attempt), BACKOFF_CAP)
            print(
                f"      [retry] page POST unexpected error (attempt {attempt + 1}/{retries}): "
                f"{type(e).__name__}: {e} -> sleeping {wait:.0f}s",
                flush=True,
            )
            time.sleep(wait)
    raise PageFailure(f"page POST failed after {retries} retries: {last!r}")


def pull_slice(start_date, end_date, label, max_pages=MAX_PAGES):
    """Paginate one time window fully. Returns (rows, capped, n_pages).

    May raise PageFailure (propagated up to pull_year, which decides skip-vs-continue).
    """
    filters = {
        "award_type_codes": AWARD_TYPE_CODES,
        "time_period": [{"start_date": start_date, "end_date": end_date}],
        "program_numbers": PROGRAM_NUMBERS,
    }
    rows = []
    page = 1
    pm = {}
    while page <= max_pages:
        body = {
            "filters": filters, "fields": FIELDS, "limit": LIMIT,
            "page": page, "sort": "Award Amount", "order": "desc",
        }
        d = _post(body)
        res = d.get("results", [])
        rows.extend(res)
        pm = d.get("page_metadata", {})
        if not pm.get("hasNext") or not res:
            break
        page += 1
        time.sleep(POLITE_DELAY)  # polite delay between page requests
    capped = page >= max_pages and bool(pm.get("hasNext"))
    return rows, capped, page


def pull_year(year):
    """Pull a full calendar year; if it approaches the 10k cap, sub-slice by month.

    May raise PageFailure (propagated to main(), which logs SKIPPED and continues).
    """
    s, e = f"{year}-01-01", f"{year}-12-31"
    rows, capped, pages = pull_slice(s, e, str(year))
    if not capped:
        return rows, False
    # Year hit the cap -> redo month-by-month and union (dedup happens globally later).
    print(f"  [{year}] HIT 10k CAP at {len(rows)} rows -> sub-slicing by month", flush=True)
    mrows = []
    for m in range(1, 13):
        ms = f"{year}-{m:02d}-01"
        # last day of month via next-month-minus-one-day
        if m == 12:
            me = f"{year}-12-31"
        else:
            nxt = pd.Timestamp(year=year, month=m + 1, day=1) - pd.Timedelta(days=1)
            me = nxt.strftime("%Y-%m-%d")
        r, c, _ = pull_slice(ms, me, f"{year}-{m:02d}")
        mrows.extend(r)
        if c:
            print(f"    [{year}-{m:02d}] STILL CAPPED at {len(r)} rows (rare; flag)", flush=True)
    return mrows, True


def _s(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s and s.lower() != "none" else None


def _amount(v):
    """Award Amount -> signed integer string (truncate cents). Negatives kept (de-obligations)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return str(int(f))


COLS = ["funder_award_id", "title", "pi_full", "pi_given", "pi_family",
        "institution", "amount", "currency", "scheme", "start_date_raw",
        "end_date_raw", "description", "landing_page_url"]


def to_frame(rows):
    out = []
    for r in rows:
        out.append({
            "funder_award_id":  _s(r.get("Award ID")),       # FAIN
            "title":            _s(r.get("Description")),
            "pi_full":          None,
            "pi_given":         None,
            "pi_family":        None,
            "institution":      _s(r.get("Recipient Name")),
            "amount":           _amount(r.get("Award Amount")),
            "currency":         "USD",
            "scheme":           _s(r.get("Award Type")),
            "start_date_raw":   _s(r.get("Start Date")),
            "end_date_raw":     _s(r.get("End Date")),
            "description":      _s(r.get("Description")),
            "landing_page_url": None,
        })
    return pd.DataFrame(out, columns=COLS).astype("string")


def main():
    t0 = time.time()
    os.makedirs(YEARS_DIR, exist_ok=True)
    all_rows = []
    per_year = {}
    capped_years = []
    skipped_years = []
    print("==== FULL DOE Office of Science 81.049 — date-sliced pull (HARDENED) ====", flush=True)
    for y in YEARS:
        try:
            rows, was_capped = pull_year(y)
        except PageFailure as ex:
            # A page in this year failed even after all retries -> skip the YEAR, continue.
            print(f"  YEAR {y} SKIPPED — {ex}", flush=True)
            skipped_years.append(y)
            per_year[y] = (0, 0)
            continue
        except Exception as ex:  # noqa: BLE001 - never let one year kill the run
            print(f"  YEAR {y} SKIPPED — unexpected {type(ex).__name__}: {ex}", flush=True)
            skipped_years.append(y)
            per_year[y] = (0, 0)
            continue

        # unique FAINs WITHIN this year (to prove no silent 10k cap)
        yids = {r.get("Award ID") for r in rows}
        per_year[y] = (len(rows), len(yids))
        if was_capped:
            capped_years.append(y)

        # dump this year's raw rows so a re-run could resume from disk
        try:
            with open(os.path.join(YEARS_DIR, f"{y}.pkl"), "wb") as fh:
                pickle.dump(rows, fh)
        except Exception as ex:  # noqa: BLE001 - dump is best-effort, never fatal
            print(f"    [{y}] WARN could not dump intermediate: {ex}", flush=True)

        flag = "  <-- sub-sliced (cap)" if was_capped else ""
        near = "  <<< NEAR 10k!" if len(yids) >= 9500 else ""
        print(f"  {y}: raw_rows={len(rows):5d}  unique_fain={len(yids):5d}{flag}{near}", flush=True)
        all_rows.extend(rows)

    df = to_frame(all_rows)
    pre = len(df)
    # Dedup by FAIN: keep first occurrence. Rows sorted desc by amount within each year;
    # cross-year dupes are the SAME award, so the kept row's fields are equivalent.
    df_dedup = df.drop_duplicates(subset=["funder_award_id"], keep="first").reset_index(drop=True)
    dups_removed = pre - len(df_dedup)

    # Completeness guard (Codex review): a skipped year or a short pull must NOT
    # silently overwrite the good S3 file — hard-fail before write/upload.
    if skipped_years or len(df_dedup) < 18000:
        print(f"[ERROR] DOE pull incomplete — skipped_years={skipped_years}, "
              f"rows={len(df_dedup)} (expected ~20,900): refusing to write/upload", flush=True)
        sys.exit(1)

    # Write parquet ONCE, at the very end, from accumulated rows.
    df_dedup.to_parquet(OUT, index=False)
    import os as _os, subprocess as _sp
    if not _os.environ.get('SKIP_UPLOAD'):
        _sp.run(['aws','s3','cp',OUT,'s3://openalex-ingest/awards/doe_sc/doe_sc_grants.parquet'], check=True)
        print('uploaded s3://openalex-ingest/awards/doe_sc/doe_sc_grants.parquet')

    # ---------- REPORT ----------
    n = len(df_dedup)
    print("\n================= REPORT =================")
    print(f"final unique rows         = {n}")
    print(f"pre-dedup concatenated    = {pre}")
    print(f"dup-FAIN rows removed     = {dups_removed}")
    print(f"years sub-sliced (capped) = {capped_years or 'NONE'}")
    print(f"YEARS SKIPPED (failed)    = {skipped_years or 'NONE'}")
    print(f"elapsed                   = {time.time()-t0:.0f}s")

    print("\n-- PER-YEAR (raw_rows / unique_fain_in_year) --")
    syr = 0
    for y in YEARS:
        rr, uu = per_year.get(y, (0, 0))
        syr += rr
        skip = "  <-- SKIPPED" if y in skipped_years else ""
        print(f"  {y}: raw={rr:6d}  uniq_in_year={uu:6d}{skip}")
    print(f"  SUM raw across years (with cross-year overlap) = {syr}")

    print("\n-- §1.4 PER-COLUMN NON-NULL % (post-dedup) --")
    for c in COLS:
        nn = df_dedup[c].notna().sum()
        pct = 100.0 * nn / n if n else 0.0
        print(f"  {c:<18} {pct:6.1f}%  ({nn}/{n})")

    # negatives (de-obligations)
    amt_num = pd.to_numeric(df_dedup["amount"], errors="coerce")
    neg = int((amt_num < 0).sum())
    zero = int((amt_num == 0).sum())
    print(f"\n-- AMOUNT stats -- negative(de-oblig)={neg}  zero={zero}  "
          f"min={amt_num.min()}  max={amt_num.max()}")
    if neg:
        print("  sample negatives:")
        for _, r in df_dedup[amt_num < 0].head(5).iterrows():
            print(f"    {r['funder_award_id']}  amt={r['amount']}  inst={str(r['institution'])[:40]}")

    print("\n-- 3 SAMPLE ROWS --")
    for _, r in df_dedup.head(3).iterrows():
        print(f"  funder_award_id={r['funder_award_id']}")
        print(f"    institution={r['institution']}")
        print(f"    amount={r['amount']} currency={r['currency']} scheme={r['scheme']}")
        print(f"    start={r['start_date_raw']} end={r['end_date_raw']}")
        print(f"    title={str(r['title'])[:100]}")
        print(f"    pi_full={r['pi_full']} pi_given={r['pi_given']} pi_family={r['pi_family']} "
              f"landing_page_url={r['landing_page_url']}")

    print(f"\n-> wrote {OUT}  ({n} rows x {len(COLS)} cols)")
    print("   dtypes:", {c: str(df_dedup[c].dtype) for c in COLS[:3]}, "...(all string)")
    print("FINAL SUMMARY DONE", flush=True)


if __name__ == "__main__":
    main()
