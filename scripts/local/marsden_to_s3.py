#!/usr/bin/env python3
"""
Marsden Fund (New Zealand) to S3 Data Pipeline
==============================================

Downloads the Marsden Fund's official 2008-2017 combined announcements workbook
and uploads a parquet to S3 for Databricks ingestion. Marsden Fund is OpenAlex
funder F4320335369 (9,491 works); existing award coverage is Crossref-derived,
so this adds canonical project metadata (amount/PI/institution/abstract).

Data source: https://www.royalsociety.org.nz/assets/Uploads/
    Marsden-Announcements-2008-2017.xlsx — the funder's own combined bulk export
    for FY2008-2017 (sheet "Marsden announcements 2008-2017"). 1,029 grants,
    100% fill on: Year, Project ID (e.g. 08-AGR-003), Project (title), Contact
    Investigator (PI, title-prefixed "Dr MJ McCann"), Institution, Panel,
    Category (Fast-Start/Standard = scheme), Funding (ex GST, NZD), Abstract.

    NB: 2018-2025 are published as SEPARATE per-year "announcement supplement"
    XLSX files with INCONSISTENT URLs (2024 base-name, 2025 _updated_v4, 2023
    404s on the obvious name) AND a different per-investigator-role schema
    (media title / role / organisation) that needs harmonization. That extension
    is a documented follow-up; this build is the clean complete 2008-2017 decade.

Output: s3://openalex-ingest/awards/marsden/marsden_grants.parquet
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
XLSX_URL = ("https://www.royalsociety.org.nz/assets/Uploads/"
            "Marsden-Announcements-2008-2017.xlsx")
SHEET = "Marsden announcements 2008-2017"
LANDING = ("https://www.royalsociety.org.nz/what-we-do/funds-and-opportunities/"
           "marsden/awarded-grants/")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/marsden/marsden_grants.parquet"

# leading honorific/title tokens stripped word-by-word off the PI name
_TITLE_TOKENS = {
    "dr", "dr.", "prof", "prof.", "professor", "associate", "assoc", "assoc.",
    "a/prof", "a/prof.", "ass", "ass.", "assistant", "adjunct", "emeritus",
    "distinguished", "honorary", "clinical", "mr", "mr.", "ms", "ms.", "mrs",
    "mrs.", "miss", "sir", "dame", "the", "hon", "hon.", "honourable", "rev",
    "rev.", "reverend",
}
_NULLISH = {"", "nan", "none", "n/a", "na", "-"}


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return None if s.lower() in _NULLISH else s


def parse_pi(raw):
    name = clean(raw)
    if not name:
        return None, None
    toks = name.split()
    while toks and toks[0].lower().strip(".,") in _TITLE_TOKENS:
        toks.pop(0)
    name = " ".join(toks)
    if not name or not re.search(r"[A-Za-z]{2}", name):
        return None, None
    parts = name.split()
    if len(parts) < 2:
        return None, name
    return " ".join(parts[:-1]), parts[-1]        # family = last token


def parse_amount(raw):
    s = clean(raw)
    if not s:
        return None
    num = re.sub(r"[^\d.]", "", s.replace(",", ""))
    try:
        val = float(num)
        return str(int(round(val))) if val > 0 else None
    except ValueError:
        return None


def fetch(input_path: Path | None) -> pd.DataFrame:
    if input_path and input_path.exists():
        print(f"Reading local workbook: {input_path}")
        return pd.read_excel(input_path, sheet_name=SHEET, dtype=str)
    print(f"Downloading Marsden 2008-2017 workbook: {XLSX_URL}")
    import requests
    last = None
    for attempt in range(4):
        try:
            r = requests.get(XLSX_URL, headers={"User-Agent": UA}, timeout=120)
            r.raise_for_status()
            tmp = Path("/tmp/marsden_dl.xlsx")
            tmp.write_bytes(r.content)
            print(f"  got {len(r.content)/1e3:.0f} KB")
            return pd.read_excel(tmp, sheet_name=SHEET, dtype=str)
        except Exception as e:
            last = e
            print(f"  [retry {attempt+1}] {e}")
    raise RuntimeError(f"download failed: {last}")


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
    ap = argparse.ArgumentParser(description="Marsden Fund 2008-2017 grants to S3")
    ap.add_argument("--input", type=Path, default=None,
                    help="local Marsden-Announcements-2008-2017.xlsx (optional)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/marsden_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Marsden Fund (NZ) 2008-2017 -> S3")
    print("=" * 60)

    df = fetch(args.input)
    print(f"Parsed {len(df)} rows, {len(df.columns)} columns")

    def gc(name):
        for c in df.columns:
            if re.sub(r"\s+", " ", str(c)).strip().lower() == name:
                return df[c]
        return pd.Series([None] * len(df))

    c_year, c_id, c_title = gc("year"), gc("project id"), gc("project")
    c_pi, c_inst = gc("contact investigator"), gc("institution")
    c_panel, c_cat = gc("panel"), gc("category")
    c_fund, c_abs = gc("funding (ex gst)"), gc("abstract")

    recs, seen = [], set()
    for i in range(len(df)):
        aid = clean(c_id.iloc[i])
        if not aid or aid in seen:
            continue
        seen.add(aid)
        given, family = parse_pi(c_pi.iloc[i])
        summary = clean(c_abs.iloc[i])
        recs.append({
            "funder_award_id": aid,
            "title": clean(c_title.iloc[i]),
            "pi_given": given,
            "pi_family": family,
            "institution": clean(c_inst.iloc[i]),
            "amount": parse_amount(c_fund.iloc[i]),
            "currency": "NZD",
            "scheme": clean(c_cat.iloc[i]),          # Fast-Start / Standard
            "research_area": clean(c_panel.iloc[i]),  # panel code (CMP, etc.)
            "year_awarded": clean(c_year.iloc[i]),
            "description": (summary[:2000] if summary else None),
            "landing_page_url": LANDING,
        })

    out_df = pd.DataFrame(recs).astype("string")
    print(f"\nDataFrame: {len(out_df)} rows, {len(out_df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "year_awarded",
              "scheme", "description"):
        nn = out_df[c].notna().sum()
        print(f"  {c:14}: {nn}/{len(out_df)} ({round(100 * nn / max(len(out_df), 1))}%)")

    if len(out_df) < 900:
        print(f"[ERROR] only {len(out_df)} rows — expected ~1,029; workbook changed?")
        sys.exit(1)

    out = args.output_dir / "marsden_grants.parquet"
    out_df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
