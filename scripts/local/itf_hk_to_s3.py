#!/usr/bin/env python3
"""
Innovation and Technology Fund (Hong Kong) — ITF -> S3 Data Pipeline
====================================================================

Downloads the ITF approved-projects annual lists from data.gov.hk and uploads a
parquet to S3 for Databricks ingestion. ITF is OpenAlex funder F4320326427
(works_count ~4,469, awards_count ~889); existing award coverage is Crossref-
derived only, so this dedicated ingest adds canonical project metadata.

Data source: data.gov.hk annual approved-projects CSVs at the predictable URL
    https://www.itf.gov.hk/datagovhk/annuallist_{YYYY}_e.csv
    Confirmed-available years: 2019..2024 (English variant); 2010-2018 + 2025 = 404
    HTML stubs. The CKAN package (hk-itc-team1-annual-itf-approved-projects-list)
    only lists the latest year; the historical English CSVs are reachable solely
    via the URL pattern. 1,763 distinct projects (the annual lists are cumulative-
    by-approval-year, not re-published — zero duplicate references across files).

    Columns: No., Programme, Reference No., Project Title, Lead Applicant,
    Funds Approved ($M), Revised Funds Approved ($M), Remarks.

Build decisions baked in (validated against the live files):
- Lead Applicant is an ORGANISATION (ASTRI, HKUST, PolyU, companies, ...), NOT a
  person -> mapped to `institution`; pi_* left null (org-level, §6.7).
- AMOUNT UNIT LANDMINE: most files express funds in HKD MILLIONS (header "$M",
  bare "2.799"); the 2020 file mislabels "$M" but stores absolute HKD with a
  "HK$" prefix. money_to_hkd() is unit-aware (HK$/$ prefix or value>=1000 =>
  absolute; bare value<1000 => *1e6). Regimes never overlap (grants 0.1M..35M).
- Revised Funds is a "0"/"-" placeholder in every observed row; use only if positive.
- Per-file encoding drift (2024 utf-8-sig, 2019 iso-8859-1) -> robust decode.
- Source has no project dates -> start_date_raw = file/approval YYYY-01-01 (the
  notebook keeps only start_year from this; start_date stays NULL); end null.

Output: s3://openalex-ingest/awards/itf_hk/itf_hk_grants.parquet
"""

import argparse
import io
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}
BASE = "https://www.itf.gov.hk/datagovhk/annuallist_{y}_e.csv"
DATASET_PAGE = "https://data.gov.hk/en-data/dataset/hk-itc-team1-annual-itf-approved-projects-list"
PROBE_YEARS = range(2010, 2026)          # probe broadly; keep whatever returns a real CSV
CURRENCY = "HKD"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/itf_hk/itf_hk_grants.parquet"

OUT_COLS = ["funder_award_id", "title", "pi_full", "pi_given", "pi_family",
            "institution", "amount", "currency", "scheme",
            "start_date_raw", "end_date_raw", "description", "landing_page_url"]

# Programme code -> human-readable scheme name (from the CSV footnotes across years).
SCHEME_NAMES = {
    "ITSP": "Innovation and Technology Support Programme (ITSP)",
    "PRP":  "Partnership Research Programme (PRP)",
    "TCFS": "Guangdong - Hong Kong Technology Cooperation Funding Scheme (TCFS)",
    "MHKJFS": "Mainland-Hong Kong Joint Funding Scheme (MHKJFS)",
    "MRP":  "Midstream Research Programme for Universities (MRP)",
    "UICP": "University-Industry Collaboration Programme (UICP)",
    "NIFS": "New Industrialisation Funding Scheme (NIFS)",
    "RFS":  "Re-industrialisation Funding Scheme (RFS)",
}


def fetch_csv_text(url):
    """GET a CSV and decode robustly across known encodings. Return (text, status)."""
    r = requests.get(url, headers=UA, timeout=45)
    if r.status_code != 200:
        return None, r.status_code
    raw = r.content
    if raw[:15].lstrip().lower().startswith(b"<!doctype") or b"<html" in raw[:200].lower():
        return None, "html-404-stub"
    for enc in ("utf-8-sig", "utf-8", "iso-8859-1", "cp1252"):
        try:
            return raw.decode(enc), 200
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace"), 200


def col(df, prefix):
    for c in df.columns:
        if c.replace(" ", "").lower().startswith(prefix.replace(" ", "").lower()):
            return c
    return None


def money_to_hkd(val):
    """Funds figure -> integer HKD string, or None. Unit-aware (see module docstring)."""
    if val is None:
        return None
    raw = str(val).strip()
    if raw.lower() in ("", "-", "n/a", "nil", "0"):
        return None
    is_dollar = "hk$" in raw.lower() or "$" in raw
    s = raw.lower().replace("hk$", "").replace("$", "").replace(",", "").strip()
    try:
        m = float(s)
    except ValueError:
        return None
    if m <= 0:
        return None
    if is_dollar or m >= 1000:               # already absolute HKD
        return str(int(round(m)))
    return str(int(round(m * 1_000_000)))    # millions -> HKD


def clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s and s.lower() not in ("nan", "-", "n/a") else None


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
    ap = argparse.ArgumentParser(description="ITF Hong Kong approved projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/itf_hk_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Innovation and Technology Fund (Hong Kong) -> S3")
    print("=" * 60)

    frames, found_years = [], []
    for y in PROBE_YEARS:
        url = BASE.format(y=y)
        txt, status = fetch_csv_text(url)
        if txt is None:
            time.sleep(0.2)
            continue
        df = pd.read_csv(io.StringIO(txt), dtype=str, keep_default_na=False)
        df["_year"] = str(y)
        df["_src_url"] = url
        frames.append(df)
        found_years.append(y)
        print(f"  {y}: OK rows={len(df)}")
        time.sleep(0.3)

    if not frames:
        print("FATAL: no CSVs retrieved", file=sys.stderr)
        sys.exit(1)
    print(f"years found: {found_years}")

    rows = []
    for df in frames:
        c_ref = col(df, "Reference")
        c_prog = col(df, "Programme")
        c_title = col(df, "Project Title") or col(df, "Project")
        c_appl = col(df, "Lead Applicant") or col(df, "Applicant")
        c_appr = col(df, "Funds Approved")
        c_rev = col(df, "Revised Funds")
        c_rmk = col(df, "Remarks")
        for _, r in df.iterrows():
            ref = clean(r.get(c_ref))
            if not ref and not clean(r.get(c_title)):
                continue
            approved = money_to_hkd(r.get(c_appr)) if c_appr else None
            revised = money_to_hkd(r.get(c_rev)) if c_rev else None
            amount = revised if revised is not None else approved
            prog = clean(r.get(c_prog))
            year = r.get("_year")
            rows.append({
                "funder_award_id": ref,
                "title": clean(r.get(c_title)),
                "pi_full": None, "pi_given": None, "pi_family": None,
                "institution": clean(r.get(c_appl)),     # org applicant
                "amount": amount,
                "currency": CURRENCY,
                "scheme": SCHEME_NAMES.get(prog, prog),
                "start_date_raw": f"{year}-01-01" if year else None,   # year-only (FLAG)
                "end_date_raw": None,
                "description": clean(r.get(c_rmk)),
                "landing_page_url": r.get("_src_url") or DATASET_PAGE,
            })

    out = pd.DataFrame(rows, columns=OUT_COLS)
    before = len(out)
    out = out[out["funder_award_id"].notna()].copy()
    out = out.sort_values(by=["funder_award_id", "amount"], na_position="last")
    out = out.drop_duplicates(subset=["funder_award_id"], keep="first").reset_index(drop=True)
    for c in OUT_COLS:
        out[c] = out[c].astype(object).where(out[c].notna(), None)
        out[c] = out[c].map(lambda x: None if x is None else str(x))

    print(f"\nraw rows={before} -> after id-filter+dedup={len(out)}")
    n = len(out)
    for c in OUT_COLS:
        nn = out[c].notna().sum()
        print(f"  {c:18s} {nn:5d}/{n} ({round(100*nn/max(n,1))}%)")
    amt = pd.to_numeric(out["amount"], errors="coerce")
    print(f"  amount HKD: min={amt.min():,.0f} median={amt.median():,.0f} max={amt.max():,.0f}")

    if n < 1500:
        print(f"[ERROR] only {n} rows — expected ~1,763; source changed?")
        sys.exit(1)

    schema = pa.schema([(c, pa.string()) for c in OUT_COLS])
    table = pa.Table.from_pandas(out[OUT_COLS], schema=schema, preserve_index=False)
    local = args.output_dir / "itf_hk_grants.parquet"
    pq.write_table(table, local)
    print(f"\nWrote {local} ({local.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(local, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {local} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
