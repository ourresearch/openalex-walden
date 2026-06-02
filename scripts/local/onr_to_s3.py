#!/usr/bin/env python3
"""
Office of Naval Research (ONR) awards.

Source: USAspending.gov (Method 2/4, public API). ONR is NOT cleanly exposed as
an awarding sub-tier (DoD/Navy office fields are null on assistance awards), so we
key off ONR's FAIN prefix instead: a keyword search on `N00014` (ONR's award-id
prefix) within assistance awards, keeping only award ids that actually start with
`N00014`. ONR's grant numbers (N00014-...) are heavily cited in papers, so the
value is direct work<->award linkage.

ORG-LEVEL GRANT pattern (USAspending exposes the recipient organisation, not a
named PI): lead_investigator given/family = NULL, affiliation.name = recipient,
country = US — same shape as the EPA/AHRQ/CDC USAspending ingests.

USAspending caps paginated search at ~10k records, and ONR has ~16.7k grants, so
we split the pull by award sign-year (each year << 10k) and de-dup by award id.

Awarding body funder: F4320337345 (Office of Naval Research, US).
provenance = 'usaspending_onr' (aggregator-suffixed per runbook).
"""
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass
if sys.platform == "win32":
    import builtins
    from pathlib import Path as _P
    _open = builtins.open
    def _utf8_open(f, mode="r", *a, **k):
        if "b" not in mode and k.get("encoding") is None:
            k["encoding"] = "utf-8"
        return _open(f, mode, *a, **k)
    builtins.open = _utf8_open
    _wt, _rt = _P.write_text, _P.read_text
    _P.write_text = lambda self, data, encoding="utf-8", **k: _wt(self, data, encoding=encoding, **k)
    _P.read_text = lambda self, encoding="utf-8", **k: _rt(self, encoding=encoding, **k)

import argparse
import json
import re
import time
import urllib.request
from html import unescape
from pathlib import Path
from typing import Optional

import pandas as pd

API = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
FUNDER_ID = 4320337345          # Office of Naval Research, US
PROVENANCE = "usaspending_onr"
FAIN_PREFIX = "N00014"          # ONR award-id prefix
COUNTRY = "US"
CURRENCY = "USD"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/onr/onr_projects.parquet"
FIRST_YEAR = 2007          # USAspending assistance coverage starts ~FY2008; pre-2007 queries 500
ASSISTANCE = ["02", "03", "04", "05"]   # block grant, formula grant, project grant, cooperative agreement
USER_AGENT = "Mozilla/5.0 (compatible; openalex-awards-ingest/1.0)"
REQUEST_DELAY_S = 0.6

_last_t = 0.0


def log(m): print(m, flush=True)


def _post(payload: dict, max_attempts: int = 7) -> dict:
    global _last_t
    body = json.dumps(payload).encode("utf-8")
    last_err = None
    for attempt in range(1, max_attempts + 1):
        dt = time.monotonic() - _last_t
        if dt < REQUEST_DELAY_S:
            time.sleep(REQUEST_DELAY_S - dt)
        try:
            req = urllib.request.Request(API, data=body,
                                         headers={"Content-Type": "application/json", "User-Agent": USER_AGENT})
            r = urllib.request.urlopen(req, timeout=120)
            _last_t = time.monotonic()
            return json.load(r)
        except Exception as e:
            _last_t = time.monotonic()
            last_err = e
            # USAspending throttles under load (RemoteDisconnected); back off hard so it can reset.
            if attempt < max_attempts:
                back = min(90, 4 * (2 ** (attempt - 1)))   # 4,8,16,32,64,90
                log(f"    transient API issue ({type(e).__name__}); retry {attempt}/{max_attempts} in {back}s")
                time.sleep(back)
    raise RuntimeError(f"USAspending API failed after {max_attempts} attempts: {last_err}")


FIELDS = ["Award ID", "Recipient Name", "Award Amount", "Start Date", "End Date", "Description"]


def fetch_year(year: int) -> list:
    """All N00014 assistance awards signed in the given year (paginated)."""
    out, page = [], 1
    # USAspending assistance coverage starts 2007-10-01; clamp the floor or the API 500s.
    start = max(f"{year}-01-01", "2007-10-01")
    while True:
        payload = {
            "filters": {
                "award_type_codes": ASSISTANCE,
                "keywords": [FAIN_PREFIX],
                # date_signed = each award appears once (its signing year), not every active year —
                # ~5x fewer pages than action_date, which avoids USAspending's load-throttling.
                "time_period": [{"start_date": start, "end_date": f"{year}-12-31", "date_type": "date_signed"}],
            },
            "fields": FIELDS, "limit": 100, "page": page,
            "sort": "Award Amount", "order": "desc",
        }
        d = _post(payload)
        rows = d.get("results", [])
        out.extend(rows)
        if not d.get("page_metadata", {}).get("hasNext"):
            break
        page += 1
        if page > 100:   # safety; per-year should never approach this
            log(f"    [WARN] year {year} exceeded 100 pages — investigate")
            break
    return out


def scrape(limit: Optional[int], max_year: Optional[int]) -> pd.DataFrame:
    log("=" * 60)
    log("Step 1: harvest ONR (N00014) assistance awards from USAspending")
    log("=" * 60)
    end_year = max_year or (time.gmtime().tm_year)
    by_id: dict[str, dict] = {}
    for year in range(FIRST_YEAR, end_year + 1):
        rows = fetch_year(year)
        kept = 0
        for r in rows:
            aid = str(r.get("Award ID") or "").strip()
            if not aid.upper().startswith(FAIN_PREFIX):   # exclude other Navy offices (N62909..., etc.)
                continue
            if aid not in by_id:
                by_id[aid] = r
                kept += 1
        log(f"  {year}: {len(rows)} rows, +{kept} new ONR (total {len(by_id):,})")
        if limit and len(by_id) >= limit:
            break
    records = list(by_id.values())
    if limit:
        records = records[:limit]
    log(f"  harvested {len(records):,} distinct ONR awards")

    def _amt(v):
        try:
            a = float(v)
        except (TypeError, ValueError):
            return None
        return a if a > 0 else None   # §6.7: 0/negative -> NULL

    def _date(v):
        s = (str(v) if v is not None else "").strip()
        m = re.match(r"\d{4}-\d{2}-\d{2}", s)
        return m.group(0) if m else None

    def _txt(v):
        if v is None:
            return None
        s = unescape(re.sub(r"\s+", " ", str(v))).strip()
        return s or None

    rows = [{
        "funder_award_id":  r["Award ID"],
        "title":            _txt(r.get("Description")),
        "amount":           _amt(r.get("Award Amount")),
        "currency":         CURRENCY,
        "institution_name": _txt(r.get("Recipient Name")),
        "country":          COUNTRY,
        "start_date":       _date(r.get("Start Date")),
        "end_date":         _date(r.get("End Date")),
    } for r in records]
    df = pd.DataFrame.from_records(rows)
    if df.empty:
        raise RuntimeError("No ONR awards harvested — aborting.")
    if df["funder_award_id"].duplicated().any():
        raise RuntimeError("Duplicate Award IDs after de-dup — investigate.")
    n = len(df)
    cov = lambda c: f"{df[c].notna().sum()*100//n}%"
    log(f"  rows={n}")
    for c in ("title", "amount", "institution_name", "start_date"):
        log(f"    {c:18} {cov(c)}")
    a = pd.to_numeric(df["amount"], errors="coerce").dropna()
    log(f"  amount(USD>0): n={len(a)} avg={a.mean():,.0f} max={a.max():,.0f}")
    yrs = pd.to_numeric(df["start_date"].str[:4], errors="coerce").dropna()
    log(f"  start_year {int(yrs.min())}-{int(yrs.max())} | top recipients: "
        f"{df['institution_name'].value_counts().head(3).to_dict()}")
    return df.astype("string")


def check_no_shrink(df, allow_shrink):
    try:
        import boto3
        boto3.client("s3").head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(f"§1.4 shrink-check FAILED: new {len(df):,} < existing {len(prev):,}")
        log(f"  §1.4 shrink-check OK ({len(df):,} >= {len(prev):,})")
    except SystemExit:
        raise
    except Exception as e:
        log(f"  §1.4 shrink-check: no prior/n.a. ({type(e).__name__})")


def main():
    ap = argparse.ArgumentParser(description="ONR (USAspending N00014) awards -> parquet -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/onr"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--max-year", type=int, default=None, help="stop at this sign-year (smoke)")
    args = ap.parse_args()
    df = scrape(args.limit, args.max_year)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "onr_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"\n  wrote {len(df):,} rows -> {out} ({out.stat().st_size//1024} KB)")
    if args.skip_upload:
        log("  --skip-upload: not uploading")
        log(f"  manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
        return
    check_no_shrink(df, args.allow_shrink)
    import boto3
    boto3.client("s3").upload_file(str(out), S3_BUCKET, S3_KEY)
    log(f"  uploaded -> s3://{S3_BUCKET}/{S3_KEY}")


if __name__ == "__main__":
    main()
