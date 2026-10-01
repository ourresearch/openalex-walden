#!/usr/bin/env python3
"""
Institute of Museum and Library Services (IMLS) to S3 Data Pipeline
===================================================================

IMLS publishes every grant it has awarded since FY1997 in its "Awarded Grants"
search at https://www.imls.gov/grants/awarded-grants, with a "Download the Data"
button that exports the whole result set as CSV (Drupal views_data_export). This
is ladder step 0: the funder's own bulk export, one file, no page scraping.

The export is a Drupal batch job: GET /grants/awarded-grants-csv?_format=csv
redirects to /batch?id=N&op=start; the no-JS fallback is polled with
/batch?id=N&op=do_nojs until it reports op=finished, after which the landing
page carries a status message with the generated file's URL
(/sites/default/files/views_data_export/.../awarded-grants-YYYY-MM-DD.csv).
The batch takes ~2 minutes. A --csv flag lets you parse a file you already have.

CSV columns: Log Number, Description, Supplement Indicator, Institution,
Fiscal Year, Program, Federal Funds, Funding Office, City, State.
There is no project title and no named project director in the export or on the
grant pages, so awards are org-level (recipient institution).

Scope filter (oxjob #1451, Kyle 2026-10-01): exclude ONLY the formula Grants to
States, i.e. the LSTA population-based allotments to State Library Administrative
Agencies, which are state operating money, not project grants:
  - "Grants to State Library Administrative Agencies" (Funding Office
    "Grants to States Libraries", LS-* log numbers)
  - "Grants to State ARPA State Grants" and "Grants to State CARES ACT State
    Grants" (the pandemic top-ups to the same state allotments, LS-* too)
Every other programme is kept, including the competitive National Leadership
Grants, Laura Bush 21st Century Librarian, Museums for America, and the
non-competitive Native American Library Services Basic Grants (flagged in the
report as kept-by-rule).

Supplements: the export lists each supplement (Supplement Indicator = 1) as an
extra row under the original log number (701 of 702 have a base row). One award
per log number: amount = base + supplements, start year = base fiscal year,
end year = latest supplement fiscal year.

funder_award_id: the IMLS log number (e.g. LG-252287-OLS-22, RE-04-12-0105-12),
which is what grantees cite (465 of 833 existing citation stubs for F4320306122
match a log number exactly, case-insensitive).

Output: s3://openalex-ingest/awards/imls/imls_projects.parquet
"""

import argparse
import html
import io
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O. See runbook §1.2. (grep anchor: sys.stdout.reconfigure)
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

BASE = "https://www.imls.gov"
EXPORT_URL = f"{BASE}/grants/awarded-grants-csv?page&_format=csv"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/imls/imls_projects.parquet"
# The site's export endpoint rejects nothing, but a browser UA matches what the button sends.
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 openalex-walden/1.0"}

# Formula Grants to States (state operating allotments) -- the only programmes excluded.
GRANTS_TO_STATES = {
    "Grants to State Library Administrative Agencies",
    "Grants to State ARPA State Grants",
    "Grants to State CARES ACT State Grants",
}
PRIZE_PROGRAMS = {"National Medals for Museum Service", "National Medals for Library Service"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def export_csv() -> bytes:
    """Run the Drupal views_data_export batch and return the CSV bytes."""
    s = requests.Session()
    s.headers.update(HEADERS)
    r = s.get(EXPORT_URL, allow_redirects=False, timeout=120)
    loc = r.headers.get("Location", "")
    m = re.search(r"batch\?id=(\d+)", loc)
    if not m:
        raise RuntimeError(f"export did not start a batch: HTTP {r.status_code} Location={loc!r}")
    bid = m.group(1)
    s.get(f"{BASE}/batch?id={bid}&op=start", allow_redirects=False, timeout=120)
    t0 = time.time()
    consecutive_err = 0
    while True:
        if time.time() - t0 > 1800:
            raise RuntimeError("export batch did not finish in 30 minutes")
        try:
            r = s.get(f"{BASE}/batch?id={bid}&op=do_nojs", allow_redirects=False, timeout=180)
        except requests.RequestException as e:
            consecutive_err += 1
            log(f"  batch poll error ({consecutive_err}/5): {e}")
            if consecutive_err >= 5:
                raise
            time.sleep(5)
            continue
        if r.status_code != 200:
            consecutive_err += 1
            log(f"  batch poll HTTP {r.status_code} ({consecutive_err}/5)")
            if consecutive_err >= 5:
                raise RuntimeError(f"export batch failing: HTTP {r.status_code}")
            time.sleep(5)
            continue
        consecutive_err = 0
        pct = re.search(r'class="progress__percentage">([^<]*)<', r.text)
        nxt = re.search(r'http-equiv="Refresh" content="0; URL=([^"]+)"', r.text)
        log(f"  export batch {bid}: {pct.group(1) if pct else '?'} ({time.time() - t0:.0f}s)")
        if not nxt:
            raise RuntimeError("export batch page has no refresh target; layout changed?")
        if "op=finished" in nxt.group(1):
            s.get(BASE + html.unescape(nxt.group(1)), allow_redirects=False, timeout=180)
            break
    # the finished batch leaves a status message (BigPipe) on the listing page
    page = s.get(f"{BASE}/grants/awarded-grants", timeout=180).text
    # the link sits inside a JSON string, so slashes may be escaped as \/
    m = re.search(r'(https:(?:\\?/){2}www\.imls\.gov(?:\\?/)sites(?:\\?/)default(?:\\?/)files(?:\\?/)views_data_export[^"<>\s]*?\.csv)', page)
    if not m:
        (Path("imls_export_landing_debug.html")).write_text(page)
        raise RuntimeError("export finished but no download link found on the listing page "
                           "(saved imls_export_landing_debug.html)")
    url = m.group(1).replace("\\/", "/")
    log(f"  export file: {url}")
    r = s.get(url, timeout=600)
    r.raise_for_status()
    return r.content


def parse_amount(s: str | None) -> float | None:
    if s is None or pd.isna(s):
        return None
    t = str(s).strip()
    neg = t.startswith("-") or t.startswith("($") or t.startswith("$-")
    digits = re.sub(r"[^\d.]", "", t)
    if not digits:
        return None
    v = float(digits)
    return -v if neg else v


def clean(s) -> str | None:
    if s is None or pd.isna(s):
        return None
    t = re.sub(r"\s+", " ", str(s)).strip()
    return t or None


def build(raw: pd.DataFrame) -> pd.DataFrame:
    raw = raw.rename(columns=lambda c: c.strip().lstrip("\ufeff"))
    need = ["Log Number", "Description", "Supplement Indicator", "Institution", "Fiscal Year",
            "Program", "Federal Funds", "Funding Office", "City", "State"]
    missing = [c for c in need if c not in raw.columns]
    if missing:
        raise SystemExit(f"export columns changed; missing {missing}: got {list(raw.columns)}")
    raw = raw.copy()
    for c in need:
        raw[c] = raw[c].map(clean)
    raw["amount_row"] = raw["Federal Funds"].map(parse_amount)
    raw["fy"] = pd.to_numeric(raw["Fiscal Year"], errors="coerce")
    raw["is_supplement"] = raw["Supplement Indicator"].eq("1")
    log(f"Export rows: {len(raw)} ({raw['is_supplement'].sum()} supplement rows), "
        f"FY {int(raw['fy'].min())}-{int(raw['fy'].max())}")

    excl = raw["Program"].isin(GRANTS_TO_STATES)
    log(f"Excluding formula Grants to States: {excl.sum()} rows "
        f"({raw.loc[excl, 'Log Number'].nunique()} log numbers)")
    for p, n in raw.loc[excl, "Program"].value_counts().items():
        log(f"    - {p}: {n}")
    kept = raw[~excl & raw["Log Number"].notna()]

    rows = []
    for log_no, g in kept.groupby("Log Number", sort=False):
        base = g[~g["is_supplement"]]
        b = (base if len(base) else g).sort_values("fy").iloc[0]
        sup = g[g["is_supplement"]]
        amounts = g["amount_row"].dropna()
        desc = next((d for d in [b["Description"], *g["Description"].tolist()] if d), None)
        rows.append({
            "log_number": log_no,
            "program": b["Program"],
            "funding_office": b["Funding Office"],
            "institution": b["Institution"],
            "city": b["City"],
            "state": b["State"],
            "fiscal_year": str(int(b["fy"])) if pd.notna(b["fy"]) else None,
            "last_fiscal_year": str(int(g["fy"].max())) if g["fy"].notna().any() else None,
            "description": desc,
            "amount_base": float(base["amount_row"].dropna().sum()) if len(base) and base["amount_row"].notna().any() else None,
            "amount_supplements": float(sup["amount_row"].dropna().sum()) if len(sup) and sup["amount_row"].notna().any() else None,
            "n_supplements": str(len(sup)),
            "amount": float(amounts.sum()) if len(amounts) else None,
            "currency": "USD" if len(amounts) else None,
            "funding_type": "prize" if b["Program"] in PRIZE_PROGRAMS else "grant",
            "landing_page_url": f"{BASE}/grants/awarded/{log_no.lower()}",
        })
    df = pd.DataFrame(rows)
    dupes = df["log_number"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'log_number'].tolist()[:20]}")
    return df


def main() -> None:
    p = argparse.ArgumentParser(description="IMLS awarded-grants CSV export -> parquet -> S3")
    p.add_argument("--csv", type=Path, default=None, help="parse this already-downloaded export instead of fetching")
    p.add_argument("--limit", type=int, default=None, help="keep only the first N log numbers (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    if args.csv:
        data = args.csv.read_bytes()
        log(f"Read {len(data):,} bytes from {args.csv}")
    else:
        log("Starting IMLS awarded-grants export batch")
        data = export_csv()
        log(f"Downloaded {len(data):,} bytes")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        (args.output_dir / "imls_awarded_grants_export.csv").write_bytes(data)
    raw = pd.read_csv(io.BytesIO(data), dtype=str, encoding="utf-8-sig", keep_default_na=False, na_values=[""])
    df = build(raw)
    if args.limit:
        df = df.head(args.limit)

    log(f"Awards: {len(df)} log numbers")
    for c in ["description", "institution", "amount", "fiscal_year", "state"]:
        log(f"  {c:14s} {df[c].notna().mean():6.1%}")
    log(f"  total amount USD {df['amount'].sum():,.0f}")
    for prog, n in df["program"].value_counts().head(15).items():
        log(f"    {n:6d}  {prog}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "imls_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_imls_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
