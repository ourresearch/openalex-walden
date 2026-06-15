#!/usr/bin/env python3
"""
Croatian Science Foundation (HRZZ) to S3 Data Pipeline
======================================================

Downloads the Hrvatska Zaklada za Znanost (HRZZ / Croatian Science Foundation)
full project database and uploads a parquet to S3 for Databricks ingestion.
HRZZ is OpenAlex funder F4320322674 (16,443 works); existing award coverage is
Crossref-derived only, so this dedicated ingest adds canonical project metadata.

Data source: the project database (https://hrzz.hr/en/funding/project-database/)
    exposes full-DB exports at https://epp.hrzz.hr/sifarnici/?export={csv,xls,xml}.
    ⚠️ The `?export=xls` endpoint does NOT return a real .xls — it returns an HTML
    `<table>` (one <tr> per project, ~13 MB). That is actually the CLEANEST source:
    the `?export=csv` file is semicolon-delimited but its abstract/keyword columns
    carry unescaped `;`/newlines that over-split ~10% of rows (a naive CSV read
    silently DROPS them). The HTML table has properly-bounded <td> cells and
    decodes HTML entities, so `pandas.read_html` recovers all ~3,604 rows intact.

    Columns (26, English + Croatian pairs): PID, Naziv projekta HR/EN (title),
    Voditelj (PI), Vrsta natječaja (call/scheme), Šifra (native grant code,
    e.g. IP-2013-11-9780), Trajanje (DD.MM.YYYY - DD.MM.YYYY), Status,
    Vrijednost financiranja (amount "114.136,16 eur"), Scientific area,
    Institution, Summary (EN abstract), …

Output: s3://openalex-ingest/awards/hrzz/hrzz_grants.parquet
"""

import argparse
import html
import io
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
EXPORT_URL = "https://epp.hrzz.hr/sifarnici/?export=xls"  # HTML <table>, not real .xls
LANDING = "https://hrzz.hr/en/funding/project-database/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hrzz/hrzz_grants.parquet"

_NULLISH = {"", "n/a", "na", "-", "—", "/"}
_DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")


def clean(v):
    if v is None:
        return None
    s = html.unescape(re.sub(r"\s+", " ", str(v)).strip())
    return None if s.lower() in _NULLISH or s.lower() == "nan" else s


def norm(name):
    return re.sub(r"\s+", " ", str(name)).strip().lower()


def col(df, *candidates):
    """Return the Series for the first column whose normalized name exactly
    matches (preferred) or contains a candidate. Robust to HR/EN name drift."""
    cols = {norm(c): c for c in df.columns}
    for cand in candidates:                       # exact match first
        if cand in cols:
            return df[cols[cand]]
    for cand in candidates:                       # then substring
        for n, orig in cols.items():
            if cand in n:
                return df[orig]
    return pd.Series([None] * len(df))


def parse_pi(raw):
    name = clean(raw)
    if not name or not re.search(r"[A-Za-zČčĆćĐđŠšŽž]{2}", name):
        return None, None
    parts = name.split()
    if len(parts) < 2:
        return None, name
    return " ".join(parts[:-1]), parts[-1]        # family = last token (Croatian order)


def parse_amount(raw):
    s = clean(raw)
    if not s:
        return None, None
    cur = "EUR"
    tok = re.search(r"([A-Za-z]{2,4})\s*$", s)
    if tok:
        t = tok.group(1).lower()
        cur = "HRK" if t in {"kn", "hrk", "kuna"} else "EUR"
    num = re.sub(r"[^\d.,]", "", s)               # keep digits + separators
    if not num:
        return None, cur
    # European format: '.' = thousands, ',' = decimal -> normalize to float
    num = num.replace(".", "").replace(",", ".")
    try:
        val = float(num)
        return (str(int(round(val))) if val > 0 else None), cur
    except ValueError:
        return None, cur


def parse_dates(raw):
    s = clean(raw)
    if not s:
        return None, None
    ds = _DATE_RE.findall(s)                       # [(dd,mm,yyyy), ...]
    def iso(t):
        d, m, y = t
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    if len(ds) >= 2:
        return iso(ds[0]), iso(ds[1])
    if len(ds) == 1:
        return iso(ds[0]), None
    return None, None


def fetch_table(input_path: Path | None) -> pd.DataFrame:
    if input_path and input_path.exists():
        print(f"Reading local export: {input_path}")
        text = input_path.read_text(encoding="utf-8", errors="replace")
    else:
        print(f"Downloading HRZZ project DB export: {EXPORT_URL}")
        last = None
        for attempt in range(4):
            try:
                r = requests.get(EXPORT_URL, headers={"User-Agent": UA}, timeout=180)
                r.raise_for_status()
                text = r.text
                break
            except Exception as e:
                last = e
                print(f"  [retry {attempt+1}] {e}")
        else:
            raise RuntimeError(f"download failed: {last}")
        print(f"  got {len(text)/1e6:.1f} MB")
    tables = pd.read_html(io.StringIO(text), header=0)
    if not tables:
        raise ValueError("no <table> parsed from export")
    return tables[0].astype(str)


def upload_to_s3(local_path: Path, bucket: str, key: str) -> bool:
    s3_uri = f"s3://{bucket}/{key}"
    print(f"\nUploading to {s3_uri}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print(f"Upload complete: {s3_uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e.stderr}")
        return False


def main():
    ap = argparse.ArgumentParser(description="HRZZ project DB to S3")
    ap.add_argument("--input", type=Path, default=None,
                    help="local copy of the ?export=xls HTML table (optional)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/hrzz_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Croatian Science Foundation (HRZZ) -> S3")
    print("=" * 60)

    df = fetch_table(args.input)
    print(f"Parsed {len(df)} table rows, {len(df.columns)} columns")

    c_pid = col(df, "pid")
    c_title_en, c_title_hr = col(df, "naziv projekta en"), col(df, "naziv projekta hr")
    c_pi = col(df, "voditelj")
    c_scheme = col(df, "vrsta natječaja", "vrsta natjecaja", "vrsta")
    c_code = col(df, "šifra", "sifra")
    c_dur = col(df, "trajanje")
    c_status = col(df, "status")
    c_amount = col(df, "vrijednost financiranja", "vrijednost")
    c_area = col(df, "scientific area")
    c_inst_en, c_inst_hr = col(df, "institution"), col(df, "ustanova")
    c_summary = col(df, "summary")

    recs, seen = [], set()
    for i in range(len(df)):
        pid = clean(c_pid.iloc[i])
        code = clean(c_code.iloc[i]) or (f"hrzz-{pid}" if pid else None)
        if not code or code in seen:
            continue
        seen.add(code)
        given, family = parse_pi(c_pi.iloc[i])
        amount, currency = parse_amount(c_amount.iloc[i])
        start, end = parse_dates(c_dur.iloc[i])
        title = clean(c_title_en.iloc[i]) or clean(c_title_hr.iloc[i])
        summary = clean(c_summary.iloc[i])
        recs.append({
            "funder_award_id": code,
            "title": title,
            "pi_given": given,
            "pi_family": family,
            "institution": clean(c_inst_en.iloc[i]) or clean(c_inst_hr.iloc[i]),
            "amount": amount,
            "currency": currency,
            "scheme": clean(c_scheme.iloc[i]),
            "status": clean(c_status.iloc[i]),
            "research_area": clean(c_area.iloc[i]),
            "start_date_raw": start,
            "end_date_raw": end,
            "description": (summary[:2000] if summary else None),
            "landing_page_url": LANDING,
        })

    out_df = pd.DataFrame(recs).astype("string")
    print(f"\nDataFrame: {len(out_df)} rows, {len(out_df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "start_date_raw",
              "research_area", "scheme", "description"):
        nn = out_df[c].notna().sum()
        print(f"  {c:16}: {nn}/{len(out_df)} ({round(100 * nn / max(len(out_df), 1))}%)")
    if "currency" in out_df:
        print("  currency:", dict(out_df["currency"].value_counts()))

    if len(out_df) < 3000:
        print(f"[ERROR] only {len(out_df)} rows — expected ~3,604; export changed/truncated?")
        sys.exit(1)

    out = args.output_dir / "hrzz_grants.parquet"
    out_df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
