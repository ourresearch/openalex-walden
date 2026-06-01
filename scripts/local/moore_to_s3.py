#!/usr/bin/env python3
"""
Gordon and Betty Moore Foundation Grants → S3 Pipeline (ORG-LEVEL GRANT PATTERN, method-5 server-rendered HTML)
==============================================================================================================

Downloads the Gordon and Betty Moore Foundation's published grant record from
its own grants database at moore.org. Moore is a major US research funder
(science, environmental conservation, patient care, San Francisco Bay Area).

Discovery (method-5, server-rendered HTML, plain requests):
  * The grant search at `/grants?searchFunction=StartsWith&searchFields=Title&currentPage=N`
    is fully server-rendered (no Cloudflare, no SPA), paginated via `currentPage`
    (~24 grants/page). Each results-table row exposes the grant id (the
    `grantId=` in its detail link), the grant title, the program, the term, the
    award month/year, and the amount.
  * Each grant's detail page `/grant-detail?grantId=GBMFNNNNN` exposes the
    grantee `Organization` (name + granteeId + website).

Fields per grant:
    grantId (e.g. GBMF14251)   stable unique award id   -> funder_award_id
    title                      grant title              -> display_name
    program                    Science / Environmental Conservation / Patient Care / ...
    term                       e.g. "12 months"
    date                       e.g. "May 2026" (month + year)
    amount                     e.g. "$3,661,871" (USD)
    Organization (detail page) grantee org              -> lead_investigator.affiliation.name

This is an ORG-LEVEL grant funder: each grant is led by the grantee
organization (no named PI). lead_investigator carries given/family NULL and
affiliation.name = the grantee org; affiliation.country is NULL (the source
exposes no grantee country — never guessed). The grant `grantId` is the
stable, unique, source-authoritative award id.

Awarding body in OpenAlex:
  Gordon and Betty Moore Foundation (F4320306202, US, ROR 006wxqw41,
  DOI 10.13039/100000936).

Amount: parsed from the published `$N` figure (USD), populated where present
(> 0); NULL otherwise. §6.7 NOT waived, never imputed; any 0/blank -> NULL.

Dates: the source gives month precision ("May 2026"), so start_date is left
NULL (no false day-level precision); start_year is derived from the year.
end_date/end_year NULL (the source gives a planned `term`, not an end date).

Output
------
  s3://openalex-ingest/awards/moore/moore_grants.parquet

Usage
-----
    python moore_to_s3.py                                  # full run
    python moore_to_s3.py --skip-upload                    # local dev
    python moore_to_s3.py --limit 30                       # smoke (1-2 list pages)
    python moore_to_s3.py --skip-download --skip-upload    # reuse cache
    python moore_to_s3.py --allow-shrink                   # override §1.4

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

import argparse
import json
import re
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet 2026-05-22) -----------------
import sys as _sys_utf8
try:
    _sys_utf8.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    _sys_utf8.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if _sys_utf8.platform == "win32":
    import builtins as _builtins_utf8
    import pathlib as _pathlib_utf8

    _orig_wt = _pathlib_utf8.Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_wt(self, data, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.write_text = _wt

    _orig_rt = _pathlib_utf8.Path.read_text
    def _rt(self, encoding=None, errors=None, newline=None):
        return _orig_rt(self, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.read_text = _rt

    _orig_open = _builtins_utf8.open
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---

# =============================================================================
# Configuration
# =============================================================================

BASE = "https://www.moore.org"
LIST_URL = BASE + "/grants?searchFunction=StartsWith&searchFields=Title&currentPage={page}"
DETAIL_URL = BASE + "/grant-detail?grantId={gid}"

FUNDER_ID = 4320306202
FUNDER_DISPLAY_NAME = "Gordon and Betty Moore Foundation"

PROVENANCE = "moore_foundation"
CURRENCY = "USD"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/moore/moore_grants.parquet"

USER_AGENT = "Mozilla/5.0 (openalex-walden-moore-ingest/1.0; +https://openalex.org)"
REQUEST_DELAY_S = 0.25
MAX_PAGES = 400  # safety; ~4,461 grants / 24 per page ~= 186 pages
DEFAULT_CACHE = Path(".cache/moore_pages.json")

MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
          "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# =============================================================================
# HTTP + cache
# =============================================================================

_session: Optional[requests.Session] = None
_last_t = 0.0


def _get(url: str, timeout: int = 30) -> str:
    global _session, _last_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    elapsed = time.monotonic() - _last_t
    if elapsed < REQUEST_DELAY_S:
        time.sleep(REQUEST_DELAY_S - elapsed)
    r = _session.get(url, timeout=timeout)
    _last_t = time.monotonic()
    r.raise_for_status()
    return r.text


def load_cache(p: Path) -> dict:
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return {}
    return {}


def save_cache(p: Path, c: dict) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(c, ensure_ascii=False))


def cached_get(url: str, cache: dict, use_cache: bool) -> str:
    if use_cache and url in cache:
        return cache[url]
    html = _get(url)
    cache[url] = html
    return html


# =============================================================================
# Parsing
# =============================================================================

def _txt(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", s)).strip()


def parse_list_page(html: str) -> list:
    ti = html.lower().find("<table")
    te = html.lower().find("</table>", ti)
    tbl = html[ti:te + 8] if ti >= 0 else ""
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", tbl, re.S)
    out = []
    for r in rows:
        gid = re.search(r"grantId=([A-Za-z0-9]+)", r)
        if not gid:
            continue
        cells = dict(re.findall(
            r"inline-header[^>]*>(.*?)</div>\s*<div[^>]*inline-value[^>]*>(.*?)</div>", r, re.S))

        def val(key):
            for hk, hv in cells.items():
                if _txt(hk).lower() == key:
                    return _txt(hv) or None
            return None

        title_m = re.search(r'grantId=[A-Za-z0-9]+"[^>]*>(.*?)</a>', r, re.S)
        out.append({
            "grantId": gid.group(1),
            "title": _txt(title_m.group(1)) if title_m else None,
            "date": val("date"),
            "program": val("program"),
            "term": val("term"),
            "amount_raw": val("amount"),
        })
    return out


def parse_detail(html: str) -> dict:
    m = re.search(
        r'Organization:?\s*</span>\s*<h4>\s*<a[^>]*granteeId=(\d+)[^>]*>(.*?)</a>',
        html, re.S | re.I)
    if not m:
        # fallback: any granteeId link near an Organization heading
        m2 = re.search(r'granteeId=(\d+)[^>]*>(.*?)</a>', html, re.S)
        if m2:
            return {"grantee_id": m2.group(1), "grantee_org": _txt(m2.group(2)) or None}
        return {"grantee_id": None, "grantee_org": None}
    return {"grantee_id": m.group(1), "grantee_org": _txt(m.group(2)) or None}


def parse_amount(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    m = re.search(r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)", s)
    if not m:
        return None
    try:
        amt = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    return amt if amt > 0 else None


def parse_month_year(s: Optional[str]):
    if not s:
        return None, None
    m = re.search(r"([A-Za-z]{3,})\.?\s+(\d{4})", s)
    if m:
        mon = MONTHS.get(m.group(1)[:3].lower())
        return mon, int(m.group(2))
    y = re.search(r"\b(19\d{2}|20\d{2})\b", s)
    return None, (int(y.group(1)) if y else None)


def parse_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"\d+", s)
    return int(m.group(0)) if m else None


# =============================================================================
# Discovery + build
# =============================================================================

def discover(use_cache: bool, cache: dict, limit: Optional[int]) -> list:
    """Collect all list rows, then collapse to one row per grantId.

    Moore lists multiple funding actions under the same grantId (an initial
    grant plus later supplements/amendments, sometimes title-less). We keep
    ONE row per grant: the action with the **largest** published amount (a
    real figure — never a synthesized sum, so no double-counting), and we
    backfill the descriptive title and the earliest year from the group.
    """
    raw = []
    seen = set()
    for page in range(1, MAX_PAGES + 1):
        rows = parse_list_page(cached_get(LIST_URL.format(page=page), cache, use_cache))
        page_ids = {r["grantId"] for r in rows}
        new_ids = page_ids - seen
        raw.extend(rows)
        seen |= page_ids
        log(f"  list page {page}: {len(rows)} rows, +{len(new_ids)} new ids (unique {len(seen)})")
        if not rows or not new_ids:
            break
        if limit and len(seen) >= limit:
            break

    by_id = {}
    for r in raw:
        gid = r["grantId"]
        amt = parse_amount(r.get("amount_raw")) or 0.0
        _, yr = parse_month_year(r.get("date"))
        if gid not in by_id:
            by_id[gid] = {"row": dict(r), "amt": amt,
                          "title": r.get("title"), "min_year": yr}
            continue
        cur = by_id[gid]
        if r.get("title") and not cur["title"]:
            cur["title"] = r["title"]
        if yr is not None and (cur["min_year"] is None or yr < cur["min_year"]):
            cur["min_year"] = yr
        if amt > cur["amt"]:
            cur["row"] = dict(r)
            cur["amt"] = amt

    grants = []
    for gid, c in by_id.items():
        row = c["row"]
        row["title"] = c["title"]               # descriptive title from any action
        row["start_year_override"] = c["min_year"]  # earliest year = grant start
        grants.append(row)
    if limit:
        grants = grants[:limit]
    return grants


def build_row(meta: dict, detail_html: Optional[str]) -> dict:
    amount = parse_amount(meta.get("amount_raw"))
    year = meta.get("start_year_override")
    if year is None:
        _, year = parse_month_year(meta.get("date"))
    det = parse_detail(detail_html) if detail_html else {"grantee_id": None, "grantee_org": None}
    return {
        "grant_id": meta["grantId"],
        "funder_award_id": meta["grantId"],
        "title": meta.get("title"),
        "program": meta.get("program"),
        "term_months": parse_int(meta.get("term")),
        "amount": amount,
        "amount_raw": meta.get("amount_raw"),
        "currency": CURRENCY if amount is not None else None,
        "award_date_raw": meta.get("date"),
        "start_year": year,
        "grantee_org": det.get("grantee_org"),
        "grantee_id": det.get("grantee_id"),
        "landing_page_url": DETAIL_URL.format(gid=meta["grantId"]),
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)
    for f in ("title", "funder_award_id", "program", "amount", "start_year", "grantee_org"):
        nn = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<16} coverage {nn}/{n} ({nn*100/n:.1f}%)")
    ids = [r["funder_award_id"] for r in rows if r.get("funder_award_id")]
    if len(ids) != len(set(ids)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(ids).items() if v > 1][:5]
        raise RuntimeError(f"funder_award_id collisions: {dups}")
    log(f"  funder_award_id uniqueness: {len(ids)}/{n} distinct ok")
    years = [r["start_year"] for r in rows if r.get("start_year")]
    if years:
        log(f"  start_year range: {min(years)}–{max(years)}")
    amts = [r["amount"] for r in rows if r.get("amount") is not None]
    if amts:
        srt = sorted(amts)
        log(f"  amount stats (USD): n={len(amts)} ({len(amts)*100/n:.1f}%) min=${min(amts):,.0f} "
            f"median=${srt[len(srt)//2]:,.0f} max=${max(amts):,.0f} total=${sum(amts):,.0f}")
    progs = {}
    for r in rows:
        p = r.get("program") or "(none)"
        progs[p] = progs.get(p, 0) + 1
    log("  programs: " + ", ".join(f"{k}={v}" for k, v in sorted(progs.items(), key=lambda x: -x[1])[:8]))


def build_dataframe(rows: list) -> pd.DataFrame:
    return pd.DataFrame(rows).astype("string")  # §1.2.5


# =============================================================================
# Shrink-check + upload
# =============================================================================

def check_no_shrink(new_count: int, allow_shrink: bool) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3
        import io
        s3 = boto3.client("s3")
        prev = pd.read_parquet(io.BytesIO(s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()))
        log(f"  §1.4 shrink-check: previous S3 parquet had {len(prev):,} rows")
        if new_count < len(prev):
            log(f"  §1.4 FAIL: new ({new_count:,}) < previous ({len(prev):,}). Aborting.")
            return False
        log(f"  §1.4 OK: new {new_count:,} >= previous {len(prev):,}")
        return True
    except Exception as e:
        log(f"  §1.4 shrink-check skipped: {type(e).__name__}: {str(e)[:100]}. (normal on first run)")
        return True


def upload_to_s3(local_file: Path) -> None:
    import boto3
    log(f"Uploading {local_file} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch Gordon and Betty Moore Foundation grants → parquet → S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    ap.add_argument("--skip-download", action="store_true")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--no-detail", action="store_true", help="skip per-grant detail fetch (grantee org)")
    args = ap.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "moore_grants.parquet"
    log("=== Gordon and Betty Moore Foundation grants ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})  provenance={PROVENANCE}")

    cache = load_cache(args.cache)
    grants = discover(args.skip_download, cache, args.limit)
    save_cache(args.cache, cache)
    log(f"  discovered {len(grants):,} grants")

    rows = []
    for i, meta in enumerate(grants, 1):
        detail_html = None
        if not args.no_detail:
            try:
                detail_html = cached_get(DETAIL_URL.format(gid=meta["grantId"]), cache, args.skip_download)
            except Exception as e:
                log(f"  [{i}] detail fetch fail {meta['grantId']}: {type(e).__name__}")
        rows.append(build_row(meta, detail_html))
        if i % 200 == 0:
            log(f"  built {i}/{len(grants)} (detail-enriched)")
            save_cache(args.cache, cache)
    save_cache(args.cache, cache)

    log(f"Built {len(rows):,} grant rows")
    validate_rows(rows)
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {out}")
    df.to_parquet(out, index=False)
    log(f"  wrote {out.stat().st_size/1e3:.0f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Moore ingest done (local-only) ===")
        return
    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")
    upload_to_s3(out)
    log("=== Moore ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
