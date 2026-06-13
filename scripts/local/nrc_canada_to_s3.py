#!/usr/bin/env python3
"""
NRC (National Research Council of Canada) to S3 Data Pipeline
================================================

Pulls CSA grants/contributions from Canada's federal Proactive Disclosure
registry via the open.canada.ca CKAN datastore_search API, filtered to
owner_org='nrc-cnrc'.

Data Source:
  https://open.canada.ca/data/api/3/action/datastore_search
  resource_id: 1d15a62f-5656-49ad-8c88-f40ce689d831 (Proactive Disclosure
  - Grants and Contributions, all Canadian federal)
  Filter: {"owner_org": "nrc-cnrc"} → ~1,931 CSA grant records

Output: s3://openalex-ingest/awards/nrc_canada/nrc_canada_projects.parquet

Schema notes:
  - funder_award_id = ref_number (e.g. 003-2018-2019-Q1-03880) — the federal
    Proactive-Disclosure ref, stable per (year, quarter, sequence).
  - Some rows have multiple amendments per grant — we aggregate by ref_number,
    summing agreement_value across amendments and keeping the latest dates.
  - recipient_legal_name is bilingual (English|French separated by `|`); we
    keep the English half as institution_name.
  - description_en is the project abstract.

⚠️ CHECK FIRST: this dataset covers ALL Canadian federal agencies. CIHR/NSERC/
SSHRC are already ingested via direct funder ingests at priorities 3/5/7.
We hard-filter to owner_org='nrc-cnrc' to avoid double-counting.

Usage:
    python nrc_canada_to_s3.py --skip-upload
"""
import argparse, json, re, sys, time, urllib.parse
from datetime import datetime, timezone
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/nrc_canada/nrc_canada_projects.parquet"
CKAN_ACTION = "https://open.canada.ca/data/api/3/action/datastore_search"
RESOURCE_ID = "1d15a62f-5656-49ad-8c88-f40ce689d831"
OWNER_ORG   = "nrc-cnrc"
PAGE_SIZE   = 1000
UA = "OpenAlex-NRC/1.0 (contact@openalex.org)"

def _fetch(url, tries=5):
    """system /usr/bin/curl — anaconda OpenSSL has cert issues on some Canada-gov endpoints."""
    import subprocess
    for a in range(tries):
        try:
            r = subprocess.run(["/usr/bin/curl", "-sL", "--max-time", "60", "-A", UA, url],
                              capture_output=True, timeout=90, check=True)
            return r.stdout.decode("utf-8", "replace")
        except Exception:
            if a == tries-1: raise
            time.sleep(2**a)

def fetch_all():
    params = {"resource_id": RESOURCE_ID, "filters": json.dumps({"owner_org": OWNER_ORG}), "limit": PAGE_SIZE}
    all_rows = []
    offset = 0
    while True:
        params["offset"] = offset
        url = CKAN_ACTION + "?" + urllib.parse.urlencode(params)
        raw = _fetch(url)
        d = json.loads(raw).get("result", {})
        recs = d.get("records", [])
        all_rows.extend(recs)
        total = d.get("total", 0)
        print(f"  offset={offset:6d} got {len(recs):4d}  cumulative {len(all_rows):5d}/{total:,}")
        if not recs or len(all_rows) >= total: break
        offset += PAGE_SIZE
        time.sleep(0.1)
    return all_rows

def _bilingual_left(s):
    """`Saint Mary's University|Université de Saint Mary` → `Saint Mary's University`."""
    if s is None or pd.isna(s): return None
    return str(s).split("|", 1)[0].strip() or None

def _parse_amount(v):
    if v is None or pd.isna(v): return None
    try:
        f = float(v)
        return f if f > 0 else None
    except (ValueError, TypeError):
        return None

def transform(rows):
    df = pd.DataFrame(rows)
    if df.empty: raise SystemExit("no rows fetched")
    # Aggregate by ref_number — same grant can have multiple amendments
    df["agreement_value_num"] = df["agreement_value"].map(_parse_amount)
    # Pick the latest amendment per ref_number using amendment_number as the order key
    df["_amend"] = pd.to_numeric(df["amendment_number"], errors="coerce")
    df = df.sort_values(["ref_number", "_amend"])
    agg = df.groupby("ref_number", dropna=True).agg(
        title_en=("agreement_title_en", "first"),
        description_en=("description_en", "first"),
        prog_name_en=("prog_name_en", "first"),
        prog_purpose_en=("prog_purpose_en", "first"),
        recipient_legal_name=("recipient_legal_name", "last"),
        research_organization_name=("research_organization_name", "last"),
        recipient_country=("recipient_country", "last"),
        recipient_province=("recipient_province", "last"),
        recipient_city=("recipient_city", "last"),
        # Sum agreement_value across amendments (each amendment is an adjustment)
        amount=("agreement_value_num", "max"),  # max = latest non-amended total
        agreement_type=("agreement_type", "first"),
        agreement_start_date=("agreement_start_date", "first"),
        agreement_end_date=("agreement_end_date", "last"),
        n_amendments=("_amend", "max"),
    ).reset_index()
    agg = agg.rename(columns={"ref_number": "funder_award_id"})
    # Resolve bilingual fields to English
    for c in ["recipient_legal_name","research_organization_name","recipient_city"]:
        agg[c] = agg[c].astype("string").map(_bilingual_left)
    # 0/neg → NULL (§6.7)
    agg["amount"] = agg["amount"].where(agg["amount"] > 0, other=pd.NA)
    # institution_name preference: research_organization_name → recipient_legal_name
    agg["institution_name"] = agg["research_organization_name"].combine_first(agg["recipient_legal_name"])
    # date parsing
    agg["start_date"] = pd.to_datetime(agg["agreement_start_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    agg["end_date"]   = pd.to_datetime(agg["agreement_end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    agg["start_year"] = pd.to_datetime(agg["agreement_start_date"], errors="coerce").dt.year.astype("Int64")
    agg["end_year"]   = pd.to_datetime(agg["agreement_end_date"], errors="coerce").dt.year.astype("Int64")
    # future-year cap
    yr_now = datetime.now(timezone.utc).year
    agg.loc[agg["start_year"] > yr_now + 1, "start_year"] = pd.NA
    agg.loc[agg["end_year"]   > yr_now + 1, "end_year"]   = pd.NA
    # Slug-collision guard
    dup = agg["funder_award_id"].duplicated().sum()
    if dup:
        raise RuntimeError(f"§slug-collision: {dup} duplicate ref_numbers")
    agg["provenance"] = "nrc_canada_proactive"
    agg["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for c in agg.columns:
        if agg[c].dtype == object: agg[c] = agg[c].astype("string")
    return agg

def check_no_shrink(df, allow_shrink):
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(f"§1.4 FAILED: new {len(df):,} < {len(prev):,}")
        print(f"  §1.4 OK")
    except SystemExit: raise
    except Exception as e:
        print(f"  §1.4: no prior ({type(e).__name__})")

def main():
    ap = argparse.ArgumentParser(description="CSA Canada (open.canada.ca CKAN) -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/nrc_canada"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    a = ap.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    print("="*60); print("NRC (National Research Council of Canada) -> S3"); print("="*60)
    print(f"[1/2] Fetching CSA grants from Proactive Disclosure (owner_org={OWNER_ORG})")
    raw = fetch_all()
    print(f"      total rows: {len(raw):,}")
    print(f"[2/2] Aggregating by ref_number")
    df = transform(raw)
    out = a.output_dir / "nrc_canada_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"\nSaved {out.name}: {len(df):,} unique grants, {out.stat().st_size/1e6:.1f} MB")
    nn = lambda c: 100 * df[c].notna().sum() / len(df)
    amt_cov = 100 * (df["amount"] > 0).sum() / len(df)
    print(f"\nCoverage:")
    for c in ["funder_award_id","title_en","description_en","amount","institution_name","start_year"]:
        if c in df.columns:
            print(f"  {c:24s} {df[c].notna().sum():,} ({nn(c):.1f}%)")
    if df["amount"].notna().any():
        print(f"\n  CAD amount median: {float(df['amount'].median()):,.0f}, max: {float(df['amount'].max()):,.0f}")
    if df["start_year"].notna().any():
        print(f"  Year range: {int(df['start_year'].min())}-{int(df['start_year'].max())}")
    print(f"\nTop institutions: {df['institution_name'].value_counts().head(5).to_dict()}")
    print(f"Top programmes: {df['prog_name_en'].value_counts().head(3).to_dict()}")
    if not a.skip_upload:
        check_no_shrink(df, a.allow_shrink)
        import subprocess, shutil
        aws = shutil.which("aws")
        if aws: subprocess.run([aws,"s3","cp",str(out),f"s3://{S3_BUCKET}/{S3_KEY}"], check=False)
        else: print(f"  [manual] aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
    print(f"\nNext: notebooks/awards/CreateNRCCanadaAwards.ipynb")

if __name__ == "__main__":
    main()
