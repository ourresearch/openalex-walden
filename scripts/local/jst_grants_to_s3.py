#!/usr/bin/env python3
"""
JST (Japan Science and Technology Agency) GRANTS to S3 Data Pipeline
====================================================================

Harvests JST's official project registry from grants.jst.go.jp (the
JST-operated integrated funder database, built on NII's KAKEN platform)
via its OpenSearch API, using the `c8=jst` record-set filter.

Data Source: https://grants.jst.go.jp/opensearch/  (format=xml, c8=jst)
Funder:      Japan Science and Technology Agency (DOI 10.13039/501100002241)
Output:      s3://openalex-ingest/awards/jst/jst_projects.parquet

Scope (recon 2026-07-07): 29,394 records in the jst record set, roughly
half JST-PROJECT (grants: CREST, PRESTO, ERATO, ACT-X, ...) and half
JST-AREA (umbrella programs); grantAward@projectType distinguishes them
and both are kept (parentAward preserves the hierarchy). Detail pages
carry title, national grant number (JGN, JPMJ*), PI with affiliation,
period, and research area; per-project amounts are not published here.

Same appid as KAKEN (both endpoints are NII-platform, one CiNii Web API
registration covers both: https://support.nii.ac.jp/en/cinii/api/developer).
Reuses the grantAward XML parser from kaken_api_to_s3.py -- the record
schema is shared (https://bitbucket.org/niijp/KAKEN_Definition).

NOTE: needs live validation once a KAKEN_APPID is available -- the c8
parameter is confirmed in the search UI form but not yet against the
OpenSearch endpoint.

Usage:
    python jst_grants_to_s3.py --skip-upload      # validation run
    python jst_grants_to_s3.py                    # full harvest + upload
    python jst_grants_to_s3.py --parse-only       # re-parse cached XML

Author: OpenAlex Team
"""

import argparse
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from lxml import etree

# Shared shim + parser: kaken_api_to_s3 installs the Windows UTF-8 shim on
# import and provides the grantAward XML parser and appid resolution.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from kaken_api_to_s3 import (  # noqa: E402
    get_appid,
    parse_grant_award,
    strip_namespaces,
)

API_URL = "https://grants.jst.go.jp/opensearch/"
ROWS_PER_REQUEST = 500
REQUEST_DELAY = 1.0
MAX_RETRIES = 5
RETRY_BACKOFF = 3.0
USER_AGENT = "OpenAlex-JST-Ingest/1.0 (research data aggregator; contact@openalex.org)"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/jst/jst_projects.parquet"


def api_get(params: dict, appid: str) -> bytes:
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                API_URL,
                params={"appid": appid, "format": "xml", "lang": "en",
                        "c8": "jst", **params},
                headers={"User-Agent": USER_AGENT},
                timeout=120,
            )
            if resp.status_code == 200 and resp.content.lstrip().startswith(b"<?xml"):
                return resp.content
            last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
            if resp.status_code == 403:
                sys.exit(f"[ERROR] API rejected the request: {last_err}")
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
        time.sleep(RETRY_BACKOFF ** (attempt + 1))
    raise RuntimeError(f"API request failed after {MAX_RETRIES} attempts: {last_err}")


def harvest(appid: str, xml_dir: Path, resume: bool) -> None:
    """Single paged sweep -- the whole jst record set is ~29k records."""
    print(f"\n{'='*60}\nPhase 1: Harvesting JST GRANTS via OpenSearch API\n{'='*60}")
    xml_dir.mkdir(parents=True, exist_ok=True)
    st, page, n_seen = 1, 1, 0
    while True:
        out = xml_dir / f"jst_{st:07d}.xml"
        if resume and out.exists():
            content = out.read_bytes()
        else:
            time.sleep(REQUEST_DELAY)
            content = api_get({"rw": ROWS_PER_REQUEST, "st": st}, appid)
            out.write_bytes(content)
        n_page = content.count(b"<grantAward ")
        n_seen += n_page
        print(f"  page {page}: {n_page} records (cumulative {n_seen:,})")
        if n_page < ROWS_PER_REQUEST:
            break
        st += ROWS_PER_REQUEST
        page += 1
    print(f"\n  [DONE] {n_seen:,} records harvested")


def parse_all(xml_dir: Path) -> pd.DataFrame:
    print(f"\n{'='*60}\nPhase 2: Parsing XML\n{'='*60}")
    files = sorted(xml_dir.glob("jst_*.xml"))
    if not files:
        sys.exit(f"[ERROR] No XML files in {xml_dir}; run the harvest first")
    rows = []
    for f in files:
        try:
            root = etree.fromstring(f.read_bytes())
            strip_namespaces(root)
        except etree.XMLSyntaxError as e:
            print(f"  [WARN] {f.name}: XML parse error ({e}); skipping")
            continue
        for ga in root.iter("grantAward"):
            row = parse_grant_award(ga)
            if row:
                # JST landing pages live on projectdb.jst.go.jp, not KAKEN.
                ptype = "AREA" if (row.get("project_type") == "area") else "PROJECT"
                row["landing_page_url"] = (
                    f"https://projectdb.jst.go.jp/grant/JST-{ptype}-{row['project_id']}/"
                )
                rows.append(row)
    df = pd.DataFrame(rows)
    before = len(df)
    df = df.drop_duplicates(subset=["project_id"], keep="first")
    print(f"  Parsed {before:,} rows -> {len(df):,} unique projects")
    return df


def save_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from kaken_api_to_s3 import save_parquet as _kaken_save  # noqa: F401  (schema reference)

    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    # Same column set as kaken_projects.parquet so notebook plumbing is uniform.
    cols = [
        "project_id", "title", "abstract", "category", "start_date", "end_date",
        "amount", "currency", "pi_given_name", "pi_family_name", "pi_affiliation",
        "pi_nrid", "institution", "keywords", "products_json", "landing_page_url",
        "amount_direct", "amount_indirect", "project_type", "project_status",
        "institution_nii_code", "members_json", "ingested_at",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    schema = pa.schema(
        [(c, pa.float64() if c.startswith("amount") else pa.string()) for c in cols]
    )
    out = output_dir / "jst_projects.parquet"
    pq.write_table(pa.Table.from_pandas(df, schema=schema, preserve_index=False), out)
    print(f"\n  [SAVE] {out} ({out.stat().st_size/1e6:.1f} MB)")
    print(f"\n  Summary:")
    print(f"    - Projects: {len(df):,}")
    for col in ("title", "start_date", "pi_family_name", "category", "project_type"):
        print(f"    - With {col}: {df[col].notna().sum():,}")
    return out


def upload_to_s3(local_path: Path) -> bool:
    import shutil
    aws = shutil.which("aws")
    if not aws:
        print("  [ERROR] AWS CLI not found")
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"\n  [UPLOAD] {local_path.name} -> {s3_uri}")
    try:
        subprocess.run([aws, "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print("  [SUCCESS] Upload complete")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Upload failed: {e.stderr}")
        return False


def main():
    parser = argparse.ArgumentParser(description="JST GRANTS harvest to S3")
    parser.add_argument("--output-dir", type=Path, default=Path("./jst_data"))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--parse-only", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    xml_dir = args.output_dir / "xml"

    print("=" * 60)
    print("JST GRANTS (grants.jst.go.jp) to S3 Pipeline")
    print("=" * 60)

    if not args.parse_only:
        harvest(get_appid(), xml_dir, resume=args.resume)

    df = parse_all(xml_dir)
    parquet_path = save_parquet(df, args.output_dir)

    if not args.skip_upload:
        if not upload_to_s3(parquet_path):
            print(f"\n[WARNING] Manual upload: aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    print("\nNext step: write notebooks/awards/CreateJSTAwards.ipynb "
          "(pattern: CreateAMEDAwards) once a validation parquet exists")


if __name__ == "__main__":
    main()
