#!/usr/bin/env python3
"""
Ontario Ministry of Colleges and Universities (MCU) -> S3 Data Pipeline
=======================================================================

Downloads the "Ontario Research Funding Summary" open dataset and uploads a
parquet to S3 for Databricks ingestion. The dataset is administered by the
Ontario Ministry of Colleges and Universities (its Legend sheet states every
"Program" is an "Ministry of Colleges and Universities peer-reviewed research
funding program name"), so it maps to OpenAlex funder F4320331473 (MCU), NOT
the over-broad "Government of Ontario" umbrella (F4320314607). Existing award
coverage is Crossref-derived only (a few dozen rows) -- this dedicated ingest
adds ~1,774 canonical project records with full PI/institution/amount metadata.

Data source: data.ontario.ca CKAN package
    package: https://data.ontario.ca/api/3/action/package_show?id=ontario-research-funding-summary-current
    -> the current English XLSX resource ("..._current_..._en.xlsx").
    Sheets: "Legend" (column glossary) + one data sheet ("July 2018-Jan 31 2026",
    name carries a moving date -> we pick the non-Legend sheet). 27 columns, all
    core fields ~100% filled:
      Project Number            -> native grant id (unique per project)
      Project Title             -> title
      Project Description       -> abstract
      First/Middle/Last Name    -> lead investigator (a person on every row)
      Lead Research Institution -> institution      City -> (carried in notes)
      Ontario Commitment        -> amount (CAD; the Ministry's committed funding,
                                   NOT "Total Project Costs" which is all sources)
      Program                   -> funder_scheme (ORF-RI, Early Researcher Awards, ORF-RE)
      Approval Date             -> start_date       FOR Division Title -> research_area

Output: s3://openalex-ingest/awards/ontario_mcu/ontario_mcu_grants.parquet
"""

import argparse
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
CKAN = ("https://data.ontario.ca/api/3/action/package_show"
        "?id=ontario-research-funding-summary-current")
# Stable fallback if CKAN resource discovery changes:
FALLBACK_XLSX = ("https://data.ontario.ca/dataset/c2621390-9362-43ae-b10a-a3258ba800cd/"
                 "resource/ebf11cfd-37f7-4775-b2d7-de4f1343787a/download/"
                 "research_funding_summary_current_2018-2026_updated_en.xlsx")
LANDING = "https://data.ontario.ca/en/dataset/ontario-research-funding-summary-current"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ontario_mcu/ontario_mcu_grants.parquet"

_NULLISH = {"", "n/a", "na", "-", "—", "/", "nan", "none"}


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return None if s.lower() in _NULLISH else s


def norm(name):
    return re.sub(r"\s+", " ", str(name)).strip().lower()


def col(df, *candidates):
    """First column whose normalized name exactly matches (preferred) or contains
    a candidate. Robust to minor header drift across dataset updates."""
    cols = {norm(c): c for c in df.columns}
    for cand in candidates:
        if cand in cols:
            return df[cols[cand]]
    for cand in candidates:
        for n, orig in cols.items():
            if cand in n:
                return df[orig]
    return pd.Series([None] * len(df))


def parse_amount(v):
    s = clean(v)
    if not s:
        return None
    num = re.sub(r"[^\d.]", "", s.replace(",", ""))
    try:
        val = float(num)
        return str(int(round(val))) if val > 0 else None
    except ValueError:
        return None


def parse_date(v):
    s = clean(v)
    if not s:
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        return None if pd.isna(ts) else ts.strftime("%Y-%m-%d")
    except Exception:
        return None


def discover_xlsx(session) -> str:
    """Resolve the current English XLSX resource URL via CKAN; fall back to the
    known direct link if discovery fails or returns nothing usable."""
    try:
        pkg = session.get(CKAN, timeout=60).json()
        resources = pkg.get("result", {}).get("resources", []) or []
        cands = []
        for r in resources:
            url = (r.get("url") or "")
            fmt = (r.get("format") or "").upper()
            low = url.lower()
            if low.endswith(".xlsx") or fmt == "XLSX":
                # prefer English + "current"
                score = (("_en" in low or low.endswith("_en.xlsx")) * 2
                         + ("current" in low) * 1)
                cands.append((score, url))
        cands.sort(reverse=True)
        if cands and cands[0][1]:
            print(f"  CKAN resolved XLSX (score {cands[0][0]}): {cands[0][1]}")
            return cands[0][1]
    except Exception as e:
        print(f"  CKAN discovery failed ({e}); using fallback URL")
    return FALLBACK_XLSX


def fetch_df(input_path: Path | None, session) -> pd.DataFrame:
    if input_path and input_path.exists():
        print(f"Reading local XLSX: {input_path}")
        xl_path = input_path
    else:
        url = discover_xlsx(session)
        print(f"Downloading: {url}")
        last = None
        for attempt in range(4):
            try:
                r = session.get(url, timeout=180)
                r.raise_for_status()
                xl_path = Path("/tmp/ontario_mcu_src.xlsx")
                xl_path.write_bytes(r.content)
                print(f"  got {len(r.content)/1e6:.1f} MB")
                break
            except Exception as e:
                last = e
                print(f"  [retry {attempt+1}] {e}")
        else:
            raise RuntimeError(f"download failed: {last}")
    xl = pd.ExcelFile(xl_path)
    data_sheets = [s for s in xl.sheet_names if norm(s) != "legend"]
    if not data_sheets:
        raise ValueError(f"no data sheet found (sheets={xl.sheet_names})")
    sheet = data_sheets[0]
    print(f"  reading sheet: {sheet!r}  (of {xl.sheet_names})")
    return pd.read_excel(xl_path, sheet_name=sheet)


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
    ap = argparse.ArgumentParser(description="Ontario MCU research funding to S3")
    ap.add_argument("--input", type=Path, default=None, help="local XLSX (optional)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/ontario_mcu_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Ontario Ministry of Colleges and Universities (MCU) -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    df = fetch_df(args.input, s)
    print(f"Parsed {len(df)} rows, {len(df.columns)} columns")

    c_num = col(df, "project number")
    c_title = col(df, "project title")
    c_desc = col(df, "project description")
    c_first = col(df, "first name")
    c_mid = col(df, "middle name")
    c_last = col(df, "last name")
    c_inst = col(df, "lead research institution")
    c_city = col(df, "city")
    c_amount = col(df, "ontario commitment")
    c_prog = col(df, "program")
    c_appr = col(df, "approval date")
    c_area = col(df, "for - level 1 division title", "for division title", "division title")

    recs, seen = [], set()
    for i in range(len(df)):
        num = clean(c_num.iloc[i])
        if not num:
            continue
        # Project Number can be a float-like "14401.0" from Excel -> normalize
        m = re.fullmatch(r"(\d+)\.0", num)
        if m:
            num = m.group(1)
        if num in seen:
            continue
        seen.add(num)
        first = clean(c_first.iloc[i])
        mid = clean(c_mid.iloc[i])
        last = clean(c_last.iloc[i])
        given = " ".join(p for p in (first, mid) if p) or None
        title = clean(c_title.iloc[i])
        desc = clean(c_desc.iloc[i])
        city = clean(c_city.iloc[i])
        recs.append({
            "funder_award_id": num,
            "title": title,
            "pi_given": given,
            "pi_family": last,
            "institution": clean(c_inst.iloc[i]),
            "city": city,
            "amount": parse_amount(c_amount.iloc[i]),
            "currency": "CAD",
            "scheme": clean(c_prog.iloc[i]),
            "research_area": clean(c_area.iloc[i]),
            "start_date_raw": parse_date(c_appr.iloc[i]),
            "end_date_raw": None,
            "description": (desc[:2000] if desc else None),
            "landing_page_url": LANDING,
        })

    out_df = pd.DataFrame(recs).astype("string")
    print(f"\nDataFrame: {len(out_df)} rows, {len(out_df.columns)} columns")
    print(f"  unique funder_award_id: {out_df['funder_award_id'].nunique()}")
    for c in ("title", "pi_family", "institution", "amount", "start_date_raw",
              "research_area", "scheme", "description"):
        nn = out_df[c].notna().sum()
        print(f"  {c:16}: {nn}/{len(out_df)} ({round(100 * nn / max(len(out_df), 1))}%)")
    print("  scheme breakdown:", dict(out_df["scheme"].value_counts()))

    if len(out_df) < 1500:
        print(f"[ERROR] only {len(out_df)} rows — expected ~1,774; source changed/truncated?")
        sys.exit(1)

    out = args.output_dir / "ontario_mcu_grants.parquet"
    out_df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
