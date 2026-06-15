#!/usr/bin/env python3
"""
Medical Research Future Fund (MRFF) to S3 Data Pipeline
=======================================================

Loads the MRFF grant-recipients workbook (the funder's own bulk Excel export)
and uploads a parquet to S3 for Databricks ingestion.

Data source: health.gov.au "MRFF grant recipients" Excel — one "Grants List"
    sheet, ~1,850 grants, native `Grant ID` (MRF…), Organisation, Project Name,
    Project Summary, Chief Investigator A/Project Lead, Total Grant Value (AUD),
    Contract Start/End dates, Grant Opportunity (scheme), MRFF Initiative,
    State, Field of Research.

    ⚠️ The file CANNOT be fetched by script: health.gov.au is Akamai-hardened
    (curl → HTTP 000 / RST_STREAM; headless-browser in-context fetch times out)
    AND the .gov.au domain geo-blocks non-AU IPs. It must be **downloaded once in
    a real browser** (from an AU IP if outside Australia) from
    https://www.health.gov.au/resources/publications/medical-research-future-fund-mrff-grant-recipients
    and placed at --input (default: ~/Downloads/…recipients.xlsx). Per Kyle's
    bulk-export-first rule, a real browser fetch is the sanctioned path here.

    funder_id F4906014721 (Path B, non-F4320) — MISSING from openalex.common.funder
    (registry gap; the notebook hardcodes the funder struct — display_name from the
    live funder API, DOI from Crossref/existing award rows since the funder endpoint
    itself returns doi=null. Confirmed safe downstream: CreateAwards keeps the struct).

Output: s3://openalex-ingest/awards/mrff/mrff_grants.parquet
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import openpyxl

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mrff/mrff_grants.parquet"
LANDING = ("https://www.health.gov.au/resources/publications/"
           "medical-research-future-fund-mrff-grant-recipients")
DEFAULT_INPUT = Path.home() / "Downloads" / \
    "medical-research-future-fund-mrff-grant-recipients.xlsx"

# leading academic/honorific title TOKENS — stripped word-by-word so any order/
# stack works ("Distinguished Professor", "Associate Adjunct Professor", …).
_TITLE_TOKENS = {
    "professor", "prof", "prof.", "a/prof", "a/prof.", "assoc", "assoc.",
    "associate", "adjunct", "assistant", "clinical", "distinguished",
    "emeritus", "honorary", "conjoint", "visiting", "research",
    "dr", "dr.", "doctor", "mr", "mr.", "mrs", "mrs.", "ms", "ms.", "miss",
    "sir", "dame", "the", "honourable", "hon", "hon.",
}
_NULLISH = {"not applicable", "not available", "none", "n/a", "tbc", "tba", ""}


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return None if s.lower() in _NULLISH else s


def parse_pi(raw):
    name = clean(raw)
    if not name:
        return None, None
    # CI-A occasionally lists several leads ("X and Professor Y", "A, Prof B …")
    # — keep only the first person.
    name = re.split(r"\s+and\s+|\s*[,;&]\s*|\s+with\s+", name, 1)[0].strip()
    # strip a leading run of title tokens, in any order/stack
    toks = name.split()
    while toks and toks[0].lower().strip(".,") in _TITLE_TOKENS:
        toks.pop(0)
    name = " ".join(toks)
    if not name or not re.search(r"[A-Za-z]{2}", name):
        return None, None
    parts = name.split()
    if len(parts) < 2:
        return None, name
    return " ".join(parts[:-1]), parts[-1]


def iso(v):
    if v is None:
        return None
    try:
        return pd.to_datetime(v).date().isoformat()
    except Exception:
        s = clean(v)
        return s if s and re.search(r"\d{4}", s) else None


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
    ap = argparse.ArgumentParser(description="MRFF grants to S3")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                    help="path to the MRFF recipients .xlsx (manual browser download)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/mrff_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("MRFF grant recipients -> S3")
    print("=" * 60)
    if not args.input.exists():
        print(f"[ERROR] input not found: {args.input}\n"
              f"        Download it in a real browser (AU IP) from:\n        {LANDING}")
        sys.exit(1)

    ws = openpyxl.load_workbook(args.input, read_only=True, data_only=True)["Grants List"]
    rows = list(ws.iter_rows(values_only=True))
    hdr = [re.sub(r"\s+", " ", str(c)).strip() if c else "" for c in rows[0]]
    idx = {h: i for i, h in enumerate(hdr)}

    def col(name):
        return idx.get(name)
    c_id, c_init, c_opp, c_org, c_state = (col("Grant ID"), col("MRFF Initiative"),
        col("Grant Opportunity"), col("Organisation"), col("State or Territory"))
    c_title, c_sum, c_ci = col("Project Name"), col("Project Summary"), col("Chief Investigator A/Project Lead")
    c_start, c_end, c_val = col("Contract Start Date"), col("Contract End Date"), col("Total Grant Value")
    c_far = col("Broad Research Area")

    recs, seen = [], set()
    for r in rows[1:]:
        gid = clean(r[c_id]) if c_id is not None else None
        if not gid or gid in seen:
            continue
        seen.add(gid)
        given, family = parse_pi(r[c_ci]) if c_ci is not None else (None, None)
        amt = None
        if c_val is not None and r[c_val] not in (None, ""):
            try:
                amt = str(int(round(float(str(r[c_val]).replace(",", "").replace("$", "")))))
            except (ValueError, TypeError):
                amt = None
        recs.append({
            "funder_award_id": gid,
            "title": clean(r[c_title]) if c_title is not None else None,
            "description": (clean(r[c_sum]) or "")[:2000] or None if c_sum is not None else None,
            "pi_given": given, "pi_family": family,
            "institution": clean(r[c_org]) if c_org is not None else None,
            "state": clean(r[c_state]) if c_state is not None else None,
            "amount": amt,
            "start_date_raw": iso(r[c_start]) if c_start is not None else None,
            "end_date_raw": iso(r[c_end]) if c_end is not None else None,
            "scheme": clean(r[c_opp]) if c_opp is not None else None,
            "initiative": clean(r[c_init]) if c_init is not None else None,
            "research_area": clean(r[c_far]) if c_far is not None else None,
            "landing_page_url": LANDING,
        })

    df = pd.DataFrame(recs).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "start_date_raw", "scheme"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    if len(df) < 1500:
        print(f"[ERROR] only {len(df)} rows — expected ~1,850; wrong sheet/file?")
        sys.exit(1)
    out = args.output_dir / "mrff_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
