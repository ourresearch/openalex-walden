#!/usr/bin/env python3
"""
GrantConnect (grants.gov.au) to S3 Data Pipeline
================================================

Downloads ALL published Grant Awards (GAs) from GrantConnect, Australia's
mandatory whole-of-government grants register (Commonwealth Grants Rules and
Guidelines require publication within 21 days of a grant agreement taking
effect; register live since 2017-12-31).

Data source: the "Grant Award Published" report
    https://www.grants.gov.au/reports/gapublishedform
which exposes a parameterless-GET download endpoint:
    https://www.grants.gov.au/Reports/GaPublishedDownload?AgencyStatus=-1
        &DateType=Publish%20Date&DateStart=DD-Mon-YYYY&DateEnd=DD-Mon-YYYY
returning an XLSX (32 columns: agency, GA ID, recipient, program, activity,
purpose, category, selection process, dates, Value (AUD), locations, ...).

The download is capped at 50,000 records per request ("Please download
results for up to 50,000 records"), so this script slices the corpus by
Publish Date into monthly ranges and bisects any slice whose embedded
"Count" statistic exceeds the rows parsed (each XLSX self-reports the
matching record count in its Statistics block, so truncation is detectable).

Outputs (two parquets):
  s3://openalex-ingest/awards/grantconnect/grantconnect_projects.parquet
      - all agencies EXCEPT ARC and NHMRC (and excluding the "A Testing
        Agency" placeholder). Research-relevance filtering happens in the
        notebook (CreateGrantConnectAwards.ipynb), not here, so the raw
        corpus keeps full fidelity.
  s3://openalex-ingest/awards/grantconnect/staging_nhmrc_arc.parquet
      - ARC + NHMRC rows only. These two funders are already Complete
        first-party ingests in OpenAlex; their GrantConnect rows are staged
        separately for overlap assessment and NOT shipped as awards.

Amounts: Value (AUD) is the grant's total value in Australian dollars.
Per GrantConnect guidance, published values are GST-inclusive where GST
applies. Stored as text; the notebook TRY_CASTs to DOUBLE.

Usage:
    python grantconnect_to_s3.py                      # full harvest + upload
    python grantconnect_to_s3.py --limit 3            # smoke: first 3 slices
    python grantconnect_to_s3.py --skip-upload        # no S3 write
    python grantconnect_to_s3.py --allow-shrink       # override 1.4 guard

Checkpointing: each slice's XLSX is cached in {output-dir}/slices/ and
re-used on re-run, so an interrupted harvest resumes where it stopped.

Requirements: pip install pandas pyarrow openpyxl requests boto3
"""

import argparse
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
import sys as _sys_utf8
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
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

BASE_URL = "https://www.grants.gov.au"
DOWNLOAD_PATH = "/Reports/GaPublishedDownload"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

S3_BUCKET = "openalex-ingest"
S3_KEY_MAIN = "awards/grantconnect/grantconnect_projects.parquet"
S3_KEY_STAGING = "awards/grantconnect/staging_nhmrc_arc.parquet"

# First slice opens wide to catch any pre-2018 publish dates.
HARVEST_START_YEAR = 2017
REQUEST_SLEEP_S = 1.2
MAX_CONSECUTIVE_NON200 = 5
DOWNLOAD_CAP = 50_000

# Agencies staged separately (already Complete first-party in OpenAlex)
STAGED_AGENCIES = (
    "Australian Research Council",
    "National Health and Medical Research Council (NHMRC)",
)
# Placeholder/test agency dropped entirely
EXCLUDED_AGENCIES = ("A Testing Agency",)

EXPECTED_COLUMNS = [
    "Agency", "GA ID", "Internal Reference ID", "GO ID", "Recipient Name",
    "Recipient ABN", "PBS Program Name", "Grant Program", "Grant Activity",
    "Purpose", "One-off/Ad hoc", "Aggregate", "Aggregate Reason",
    "Aggregate Number", "Selection Process", "Category",
    "Confidentiality - Contract", "Confidentiality - Outputs",
    "Publish Date", "Approval Date", "Start Date", "End Date", "Value (AUD)",
    "Recipient Suburb", "Recipient Town/City", "Recipient Postcode",
    "Recipient State/Territory", "Recipient Country",
    "Delivery State/Territory", "Delivery Postcode", "Delivery Country",
    "Contact Name",
]

COLUMN_RENAME = {
    "Agency": "agency",
    "GA ID": "ga_id",
    "Internal Reference ID": "internal_reference_id",
    "GO ID": "go_id",
    "Recipient Name": "recipient_name",
    "Recipient ABN": "recipient_abn",
    "PBS Program Name": "pbs_program_name",
    "Grant Program": "grant_program",
    "Grant Activity": "grant_activity",
    "Purpose": "purpose",
    "One-off/Ad hoc": "one_off_ad_hoc",
    "Aggregate": "aggregate",
    "Aggregate Reason": "aggregate_reason",
    "Aggregate Number": "aggregate_number",
    "Selection Process": "selection_process",
    "Category": "category",
    "Confidentiality - Contract": "confidentiality_contract",
    "Confidentiality - Outputs": "confidentiality_outputs",
    "Publish Date": "publish_date",
    "Approval Date": "approval_date",
    "Start Date": "start_date",
    "End Date": "end_date",
    "Value (AUD)": "value_aud",
    "Recipient Suburb": "recipient_suburb",
    "Recipient Town/City": "recipient_town_city",
    "Recipient Postcode": "recipient_postcode",
    "Recipient State/Territory": "recipient_state_territory",
    "Recipient Country": "recipient_country",
    "Delivery State/Territory": "delivery_state_territory",
    "Delivery Postcode": "delivery_postcode",
    "Delivery Country": "delivery_country",
    "Contact Name": "contact_name",
}


# =============================================================================
# Date slicing
# =============================================================================

def fmt(d: date) -> str:
    """GrantConnect report date format: 01-Jan-2024."""
    return d.strftime("%d-%b-%Y")


def month_slices() -> list:
    """(label, start, end) monthly slices from harvest start through today.

    The first slice opens at 2000-01-01 to catch any publish dates that
    predate the register's official go-live.
    """
    slices = []
    today = date.today()
    y, m = HARVEST_START_YEAR, 1
    first = True
    while date(y, m, 1) <= today:
        start = date(2000, 1, 1) if first else date(y, m, 1)
        if m == 12:
            nxt_y, nxt_m = y + 1, 1
        else:
            nxt_y, nxt_m = y, m + 1
        end = min(date(nxt_y, nxt_m, 1) - timedelta(days=1), today)
        slices.append((f"{y:04d}-{m:02d}", start, end))
        first = False
        y, m = nxt_y, nxt_m
    return slices


# =============================================================================
# Download
# =============================================================================

def build_url(start: date, end: date) -> str:
    return (f"{BASE_URL}{DOWNLOAD_PATH}?AgencyStatus=-1"
            f"&DateType=Publish%20Date"
            f"&DateStart={fmt(start)}&DateEnd={fmt(end)}")


def download_slice(session: requests.Session, label: str, start: date, end: date,
                   cache_dir: Path) -> Path:
    """Download one date-range XLSX (cached; resumable)."""
    out = cache_dir / f"ga_{label}_{start.isoformat()}_{end.isoformat()}.xlsx"
    if out.exists() and out.stat().st_size > 1000:
        print(f"  [CACHE] {label} ({out.name}, {out.stat().st_size:,} bytes)")
        return out
    url = build_url(start, end)
    consecutive_non200 = 0
    while True:
        try:
            r = session.get(url, timeout=300)
            status = r.status_code
        except requests.RequestException as e:
            status = f"EXC:{e.__class__.__name__}"
            r = None
        if r is not None and status == 200 and r.content[:2] == b"PK":
            out.write_bytes(r.content)
            print(f"  [DL] {label} {fmt(start)}..{fmt(end)} -> {len(r.content):,} bytes")
            time.sleep(REQUEST_SLEEP_S)
            return out
        consecutive_non200 += 1
        print(f"  [WARN] {label}: HTTP {status} "
              f"({consecutive_non200}/{MAX_CONSECUTIVE_NON200}); retrying")
        if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
            raise RuntimeError(f"slice {label}: {MAX_CONSECUTIVE_NON200} consecutive "
                               f"failed downloads — refusing to silently truncate")
        time.sleep(5 * consecutive_non200)


def parse_slice(xlsx_path: Path) -> tuple:
    """Parse one report XLSX -> (DataFrame, reported_count).

    The report embeds its own criteria + Statistics block above the data
    table; locate the header row by the 'Agency' + 'GA ID' signature and the
    reported record count from the Statistics 'Count' row.
    """
    raw = pd.read_excel(xlsx_path, header=None, dtype=str)
    header_idx = None
    reported_count = None
    for i in range(min(40, len(raw))):
        c0 = str(raw.iat[i, 0])
        if c0 == "Count" and reported_count is None:
            reported_count = int(str(raw.iat[i, 1]).replace(",", ""))
        if c0 == "Agency" and str(raw.iat[i, 1]) == "GA ID":
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError(f"{xlsx_path.name}: no header row found (layout change?)")
    if reported_count is None:
        raise RuntimeError(f"{xlsx_path.name}: no Count statistic found (layout change?)")
    header = [str(x) for x in raw.iloc[header_idx].tolist()]
    df = raw.iloc[header_idx + 1:].copy()
    df.columns = header
    df = df.dropna(how="all")
    # empty result sets carry a single "There are no results..." sentinel row
    df = df[df["Agency"] != "There are no results that match your selection."]
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"{xlsx_path.name}: missing expected columns {missing}")
    return df, reported_count


def harvest(session: requests.Session, slices: list, cache_dir: Path) -> pd.DataFrame:
    """Download + parse every slice, bisecting any truncated one."""
    frames = []
    total = 0
    t0 = time.time()
    n = len(slices)
    queue = list(slices)
    done = 0
    while queue:
        label, start, end = queue.pop(0)
        xlsx = download_slice(session, label, start, end, cache_dir)
        df, reported = parse_slice(xlsx)
        if reported > len(df) or reported >= DOWNLOAD_CAP:
            if start == end:
                raise RuntimeError(f"slice {label} single day still truncated "
                                   f"({reported} reported, {len(df)} rows)")
            mid = start + (end - start) / 2
            print(f"  [SPLIT] {label}: reported {reported:,} > parsed {len(df):,} "
                  f"— bisecting {fmt(start)}..{fmt(end)}")
            xlsx.unlink()  # discard truncated cache
            queue.insert(0, (f"{label}b", mid + timedelta(days=1), end))
            queue.insert(0, (f"{label}a", start, mid))
            continue
        if reported != len(df):
            raise RuntimeError(f"slice {label}: reported {reported} != parsed {len(df)}")
        frames.append(df)
        total += len(df)
        done += 1
        elapsed = time.time() - t0
        print(f"  [{elapsed:6.0f}s] {label}: {len(df):>6,} rows "
              f"(total {total:>8,}; {done}/{n}+ slices)")
    combined = pd.concat(frames, ignore_index=True)
    return combined


# =============================================================================
# Processing
# =============================================================================

def process(df: pd.DataFrame) -> tuple:
    """Clean, dedup, split into (main_df, staging_df)."""
    print(f"\n{'='*60}\nStep 2: Processing {len(df):,} rows\n{'='*60}")

    df = df.rename(columns=COLUMN_RENAME)
    df = df[[c for c in COLUMN_RENAME.values()]]

    # normalize whitespace in text fields
    for c in ("grant_activity", "purpose", "grant_program", "recipient_name"):
        df[c] = df[c].str.replace(r"\s+", " ", regex=True).str.strip()

    # GA ID sanity
    bad_ga = df[~df["ga_id"].astype(str).str.match(r"^GA\d+", na=False)]
    if len(bad_ga):
        print(f"  [WARN] {len(bad_ga)} rows with non-GA-shaped ga_id; dropping")
        print(bad_ga["ga_id"].head().to_string())
        df = df[df["ga_id"].astype(str).str.match(r"^GA\d+", na=False)]

    # dates -> ISO strings (Spark-safe)
    for c in ("publish_date", "approval_date", "start_date", "end_date"):
        parsed = pd.to_datetime(df[c], errors="coerce")
        df[c] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S")
        df[c] = df[c].where(parsed.notna(), None)

    # dedup by GA ID, keep the latest-published row
    before = len(df)
    df = df.sort_values("publish_date").drop_duplicates(subset=["ga_id"], keep="last")
    print(f"  Dedup by ga_id: {before:,} -> {len(df):,} "
          f"({before - len(df):,} duplicates removed)")

    # drop test agency
    n_test = int(df["agency"].isin(EXCLUDED_AGENCIES).sum())
    if n_test:
        print(f"  Dropping {n_test} '{EXCLUDED_AGENCIES[0]}' placeholder rows")
    df = df[~df["agency"].isin(EXCLUDED_AGENCIES)]

    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    staged_mask = df["agency"].isin(STAGED_AGENCIES)
    staging = df[staged_mask].copy()
    main = df[~staged_mask].copy()
    print(f"  Main corpus (non-ARC/NHMRC): {len(main):,} rows")
    print(f"  Staged ARC+NHMRC:            {len(staging):,} rows")
    print("\n  Agency counts (main, top 15):")
    print(main["agency"].value_counts().head(15).to_string())
    print("\n  Category counts (main, top 15):")
    print(main["category"].value_counts().head(15).to_string())
    return main, staging


def write_parquet(df: pd.DataFrame, path: Path) -> Path:
    # force string dtype on every column (runbook §1.2 item 5)
    df = df.astype("string")
    schema = pa.schema([(c, pa.string()) for c in df.columns])
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, path)
    print(f"  [SAVE] {path} ({path.stat().st_size/1e6:.1f} MB, {len(df):,} rows)")
    return path


# =============================================================================
# S3 upload with §1.4 shrink check
# =============================================================================

def check_no_shrink(new_count: int, s3_key: str, allow_shrink: bool,
                    output_dir: Path) -> bool:
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError("boto3 required for §1.4 shrink-check; "
                           "rerun with --skip-upload to bypass") from exc
    client = boto3.client("s3")
    print(f"  §1.4 re-ingest safety check vs s3://{S3_BUCKET}/{s3_key}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=s3_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet — first ingest, no shrink check.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / ("_prev_" + Path(s3_key).name)
    try:
        client.download_file(S3_BUCKET, s3_key, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        print(f"    [ERROR] couldn't read existing parquet ({e}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    print(f"    previous: {prev_count:,}   new: {new_count:,}")
    if new_count < prev_count:
        if allow_shrink:
            print("    [OVERRIDE] --allow-shrink set; proceeding.")
            return True
        print(f"\n[ERROR] §1.4 violation: refusing to shrink "
              f"({prev_count:,} -> {new_count:,}).")
        return False
    print("    [OK] not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, row_count: int, s3_key: str,
                 output_dir: Path, allow_shrink: bool) -> bool:
    s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
    if not check_no_shrink(row_count, s3_key, allow_shrink, output_dir):
        return False
    print(f"  Uploading {parquet_path.name} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        print(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        print("[ERROR] aws CLI not found.")
        return False
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] aws s3 cp failed (exit {e.returncode}).")
        return False


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download GrantConnect grant awards and upload to S3")
    parser.add_argument("--output-dir", type=Path, default=Path("./grantconnect_data"),
                        help="Directory for cached slices and parquet output")
    parser.add_argument("--limit", type=int, default=None,
                        help="Smoke test: only harvest the first N date slices")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquets to S3 (local smoke test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override the §1.4 never-shrink guard")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = args.output_dir / "slices"
    cache_dir.mkdir(exist_ok=True)

    print("=" * 60)
    print("GrantConnect (grants.gov.au) to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 main:    s3://{S3_BUCKET}/{S3_KEY_MAIN}")
    print(f"S3 staging: s3://{S3_BUCKET}/{S3_KEY_STAGING}")

    slices = month_slices()
    if args.limit:
        slices = slices[:args.limit]
        print(f"[LIMIT] harvesting only first {args.limit} slices")
    print(f"\n{'='*60}\nStep 1: Downloading {len(slices)} date slices\n{'='*60}")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT,
                            "Accept-Language": "en-US,en;q=0.9"})

    df = harvest(session, slices, cache_dir)
    main_df, staging_df = process(df)

    print(f"\n{'='*60}\nStep 3: Writing parquet\n{'='*60}")
    main_pq = write_parquet(main_df, args.output_dir / "grantconnect_projects.parquet")
    staging_pq = write_parquet(staging_df, args.output_dir / "staging_nhmrc_arc.parquet")

    ok = True
    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not writing to S3.")
    else:
        print(f"\n{'='*60}\nStep 4: Upload to S3 (with §1.4 shrink check)\n{'='*60}")
        ok = upload_to_s3(main_pq, len(main_df), S3_KEY_MAIN,
                          args.output_dir, args.allow_shrink)
        ok = upload_to_s3(staging_pq, len(staging_df), S3_KEY_STAGING,
                          args.output_dir, args.allow_shrink) and ok

    print(f"\n{'='*60}")
    print("Pipeline complete!" if ok else "Pipeline finished WITH ERRORS")
    print(f"{'='*60}")
    print(f"  main rows:    {len(main_df):,}")
    print(f"  staged rows:  {len(staging_df):,}")
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
