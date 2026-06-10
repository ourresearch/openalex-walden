#!/usr/bin/env python3
"""
Polish Ministry of Science and Higher Education (MNiSW) to S3 Data Pipeline
==========================================================================

Harvests the Ministry's national-programme funded-project lists from the Polish
open-data portal dane.gov.pl and uploads a parquet to S3 for Databricks
ingestion.

Data source: https://api.dane.gov.pl/1.4/  (CKAN-style JSON API, no auth)
    Institution 431 = "ministerstwo-nauki-i-szkolnictwa-wyzszego". Its grant
    datasets publish per-programme XLSX files (NPRH, Perły Nauki, Nauka dla
    Społeczeństwa, Doskonała nauka, Polska Metrologia, etc.). NCN datasets are
    EXCLUDED (NCN is a separately tracked funder).

Notes:
    - Institution-level award lists (applicant org); individual PI names are NOT
      published here (they live only in the API-key-gated RAD-on register).
    - Amounts are PLN; column headers vary per programme so matching is fuzzy.

Output: s3://openalex-ingest/awards/mnisw/mnisw_projects.parquet

Usage:
    python mnisw_to_s3.py
    python mnisw_to_s3.py --skip-upload
    python mnisw_to_s3.py --max-datasets 5      # smoke test

Author: OpenAlex Team
"""

import argparse
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

API = "https://api.dane.gov.pl/1.4"
INSTITUTION_ID = 431  # Ministerstwo Nauki i Szkolnictwa Wyższego
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mnisw/mnisw_projects.parquet"

# Datasets whose title contains a grant/funding keyword qualify; NCN excluded.
GRANT_KW = ("finansowan", "projekt", "program", "wniosk", "konkurs",
            "beneficjen", "dofinansowan", "wspar", "przedsięwz", "nagrod", "stypend")
EXCLUDE_KW = ("ncn", "narodowe centrum nauki", "narodowego centrum nauki")


def col_match(headers, *needles):
    """Return the first header whose lowercase contains any needle substring."""
    low = {h: (str(h) or "").lower() for h in headers}
    for h in headers:
        if any(n in low[h] for n in needles):
            return h
    return None


def parse_amount(v):
    if v is None:
        return None
    s = str(v).replace("\xa0", "").replace(" ", "").replace("zł", "").strip()
    s = s.replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return m.group(0) if m else None


def year_from(award_id, title_hint):
    for src in (award_id or "", title_hint or ""):
        m = re.search(r"(19|20)\d{2}", str(src))
        if m:
            return m.group(0)
    return None


def parse_sheet(df: pd.DataFrame, programme: str) -> list:
    headers = list(df.columns)
    c_id = col_match(headers, "rejestracyjn")
    c_title = col_match(headers, "tytuł")
    c_inst = col_match(headers, "wnioskodawca", "beneficjent", "nazwa podmiotu", "jednostk")
    c_amt = col_match(headers, "kwota")
    c_year = col_match(headers, "rok")
    c_prog = col_match(headers, "nazwa programu", "moduł", "program")
    if not (c_id or c_title):
        return []
    rows = []
    for _, r in df.iterrows():
        aid = str(r[c_id]).strip() if c_id and pd.notna(r[c_id]) else None
        title = str(r[c_title]).strip() if c_title and pd.notna(r[c_title]) else None
        if not aid and not title:
            continue
        prog = str(r[c_prog]).strip() if c_prog and pd.notna(r[c_prog]) else programme
        yr = (str(r[c_year]).strip() if c_year and pd.notna(r[c_year]) else None) or year_from(aid, programme)
        rows.append({
            "funder_award_id": aid or f"{programme}:{title[:40]}",
            "title": title,
            "institution": str(r[c_inst]).strip() if c_inst and pd.notna(r[c_inst]) else None,
            "amount": parse_amount(r[c_amt]) if c_amt else None,
            "start_year": re.sub(r"\D", "", yr)[:4] if yr else None,
            "programme": prog,
        })
    return rows


def iter_grant_datasets(session):
    page = 1
    while True:
        r = session.get(f"{API}/institutions/{INSTITUTION_ID}/datasets",
                        params={"per_page": 100, "page": page}, timeout=60)
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            break
        for d in data:
            title = (d.get("attributes", {}).get("title") or "").lower()
            if any(x in title for x in EXCLUDE_KW):
                continue
            if any(k in title for k in GRANT_KW):
                yield d
        if len(data) < 100:
            break
        page += 1


def resources_of(session, dataset):
    rid = dataset["id"]
    r = session.get(f"{API}/datasets/{rid}/resources", params={"per_page": 100}, timeout=60)
    r.raise_for_status()
    return r.json().get("data", [])


def download_xlsx(session, res):
    rid = res["id"]
    url = f"{API}/resources/{rid}/file"
    r = session.get(url, allow_redirects=True, timeout=120)
    if r.status_code != 200 or not r.content:
        return None
    return r.content


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
    ap = argparse.ArgumentParser(description="Harvest MNiSW funded projects (dane.gov.pl) to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/mnisw_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--max-datasets", type=int, default=None)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("MNiSW (Polish Ministry of Science) funded projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"Accept": "application/json"})  # default UA fine

    datasets = list(iter_grant_datasets(s))
    if args.max_datasets:
        datasets = datasets[:args.max_datasets]
    print(f"Grant datasets: {len(datasets)}")

    all_rows, seen = [], set()
    for d in datasets:
        programme = d.get("attributes", {}).get("title", "")
        try:
            res_list = resources_of(s, d)
        except Exception as e:
            print(f"  [WARN] resources {d['id']}: {e}")
            continue
        for res in res_list:
            fmt = (res.get("attributes", {}).get("format") or "").lower()
            if fmt not in ("xlsx", "xls"):
                continue
            content = download_xlsx(s, res)
            if not content:
                continue
            try:
                engine = "xlrd" if fmt == "xls" else "openpyxl"
                df = pd.read_excel(io.BytesIO(content), dtype=str, engine=engine)
            except Exception as e:
                print(f"  [WARN] parse {res['id']} ({fmt}): {e}")
                continue
            rows = parse_sheet(df, programme)
            for row in rows:
                key = row["funder_award_id"]
                if key in seen:
                    continue
                seen.add(key)
                all_rows.append(row)
        print(f"  {programme[:55]:55} -> {len(all_rows)} cumulative")

    if not all_rows:
        print("[ERROR] no rows harvested")
        sys.exit(1)

    df = pd.DataFrame(all_rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    out = args.output_dir / "mnisw_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
