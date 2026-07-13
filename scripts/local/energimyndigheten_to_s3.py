#!/usr/bin/env python3
"""
Energimyndigheten (Swedish Energy Agency) to S3 Data Pipeline
=============================================================

Downloads Energimyndigheten (Statens energimyndighet, org nr 202100-5000)
grant data from SweCRIS (Sweden's national research-grants registry, run by
Vetenskapsradet, CC0), processes it into a parquet file, and uploads to S3.

Why SweCRIS and not energimyndigheten.se's own projektdatabas:
- The projektdatabas (energimyndigheten.se/forskning-och-innovation/
  data-om-finansiering-av-forskning-och-innovation/projektdatabas/) is a
  server-rendered Episerver search with a hard 100-results-per-query cap,
  no pagination, and no JSON endpoint (probed 2026-07-12).
- The agency's machine API ("GDP" joint-data API shared by 5 Swedish
  funders) requires an API key application.
- SweCRIS carries the same native project numbers (P-numbers, e.g.
  "P2023-00317"), SEK amounts, dates, Swedish+English titles and abstracts
  for ~6,000 Energimyndigheten projects 2008-2026.

Data source: SweCRIS API (https://swecris-api.vr.se), public token.
Output: s3://openalex-ingest/awards/energimyndigheten/energimyndigheten_projects.parquet

Notes:
- SweCRIS peopleList is EMPTY for Energimyndigheten projects (the agency
  does not report PIs to SweCRIS), so pi_* columns are NULL by design.
- SweCRIS projectId carries an "_Energi" suffix ("P2023-00317_Energi");
  the raw value is preserved here, the notebook strips the suffix so
  funder_award_id matches the citable grant number.

Usage:
    python energimyndigheten_to_s3.py [--output-dir DIR] [--limit N]
                                      [--skip-download] [--skip-upload]
                                      [--allow-shrink]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22, runbook §1.2 #7) ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if sys.platform == "win32":
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
    def _open(file, mode="r", buffering=-1, encoding=None, errors=None,
              newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open
# --- end shim ---

SWECRIS_API_BASE = "https://swecris-api.vr.se/v1"
SWECRIS_FUNDER_ORG_NR = "202100-5000"   # Statens energimyndighet
# Public API token. NOTE: rotates yearly — the 2025 token ("VRSwecrisAPI2025-1")
# stopped working; confirmed "VRSwecrisAPI2026-1" live on 2026-07-12.
SWECRIS_API_TOKEN = "VRSwecrisAPI2026-1"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/energimyndigheten/energimyndigheten_projects.parquet"

USER_AGENT = "OpenAlex awards ingest (mailto:kyle@ourresearch.org)"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_repo_dotenv() -> None:
    """AWS creds live in the repo .env (never ~/.aws). Load if unset."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def download_grants(output_dir: Path) -> Path:
    url = f"{SWECRIS_API_BASE}/projects/funders/{SWECRIS_FUNDER_ORG_NR}"
    log(f"Downloading Energimyndigheten projects from {url}")
    headers = {"Authorization": f"Bearer {SWECRIS_API_TOKEN}", "User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=600)
    r.raise_for_status()
    out = output_dir / "swecris_energimyndigheten_raw.json"
    out.write_text(r.text)
    log(f"  saved {out.stat().st_size/1e6:.1f} MB -> {out}")
    return out


def parse_projects(json_path: Path, limit=None) -> list[dict]:
    data = json.loads(json_path.read_text())
    log(f"  {len(data):,} projects in raw JSON")
    if limit:
        data = data[:limit]
        log(f"  --limit {limit}: truncated to {len(data):,}")
    rows = []
    for row in data:
        scbs = row.get("scbs") or []
        rows.append({
            "project_id": row.get("projectId"),
            "title": row.get("projectTitleSv"),
            "title_english": row.get("projectTitleEn"),
            "abstract": row.get("projectAbstractSv"),
            "abstract_english": row.get("projectAbstractEn"),
            "start_date": row.get("projectStartDate"),
            "end_date": row.get("projectEndDate"),
            "coordinating_organisation_id": row.get("coordinatingOrganisationId"),
            "coordinating_organisation": row.get("coordinatingOrganisationNameEn") or row.get("coordinatingOrganisationNameSv"),
            "coordinating_organisation_type": row.get("coordinatingOrganisationTypeOfOrganisationEn"),
            "funding_organisation_id": row.get("fundingOrganisationId"),
            "funding_organisation": row.get("fundingOrganisationNameEn") or row.get("fundingOrganisationNameSv"),
            "amount": row.get("fundingsSek"),
            "funding_year": row.get("fundingYear"),
            "funding_start_date": row.get("fundingStartDate"),
            "funding_end_date": row.get("fundingEndDate"),
            "type_of_award_id": row.get("typeOfAwardId"),
            "type_of_award": row.get("typeOfAwardDescrEn") or row.get("typeOfAwardDescrSv"),
            # peopleList is empty for Energimyndigheten in SweCRIS; keep the
            # raw JSON anyway so a future SweCRIS change is captured.
            "people_json": json.dumps(row.get("peopleList") or [], ensure_ascii=False),
            "main_discipline": scbs[0].get("scb5NameEn") if scbs else None,
            "main_discipline_level1": scbs[0].get("scb1NameEn") if scbs else None,
            "updated_date": row.get("updatedDate"),
            "loaded_date": row.get("loadedDate"),
        })
    return rows


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for col in ("start_date", "end_date"):
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
    before = len(df)
    df = df.sort_values("amount", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["project_id"], keep="first").reset_index(drop=True)
    if len(df) < before:
        log(f"  deduped {before - len(df)} duplicate project_ids (kept max-amount row)")
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df = df.astype("string")   # §1.2 #5: force string dtype before to_parquet
    return df


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """§1.4 re-ingest safety: never overwrite S3 with a smaller corpus."""
    if allow_shrink:
        log("  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3
        client = boto3.client("s3")
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev_path = output_dir / "_previous_s3.parquet"
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
        log(f"  §1.4 shrink-check: previous S3 parquet has {prev_count:,} rows")
        if new_count < prev_count:
            log(f"  §1.4 FAIL: new ({new_count:,}) < previous ({prev_count:,}). Aborting upload.")
            return False
        log(f"  §1.4 OK: new {new_count:,} >= previous {prev_count:,}")
        return True
    except Exception as e:
        log(f"  §1.4 shrink-check skipped ({type(e).__name__}: {str(e)[:80]}) — normal on first run")
        return True


def upload_to_s3(local_file: Path) -> None:
    import boto3
    log(f"Uploading {local_file.name} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


def main() -> None:
    parser = argparse.ArgumentParser(description="Energimyndigheten (via SweCRIS) -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).parent / "energimyndigheten_data",
                        help="Directory for downloaded/processed files")
    parser.add_argument("--limit", type=int, default=None,
                        help="Smoke test: only process the first N projects")
    parser.add_argument("--skip-download", action="store_true", help="Reuse existing raw JSON")
    parser.add_argument("--skip-upload", action="store_true", help="Build parquet only")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override the §1.4 never-shrink safety check")
    args = parser.parse_args()

    load_repo_dotenv()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    raw = args.output_dir / "swecris_energimyndigheten_raw.json"
    if not args.skip_download or not raw.exists():
        raw = download_grants(args.output_dir)
    else:
        log(f"--skip-download: using {raw}")

    rows = parse_projects(raw, limit=args.limit)
    if not rows:
        log("ERROR: no projects parsed"); sys.exit(1)

    df = build_dataframe(rows)
    out = args.output_dir / "energimyndigheten_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {len(df):,} rows -> {out} ({out.stat().st_size/1e6:.1f} MB)")
    log(f"  with amount: {df['amount'].notna().sum():,} | with title_en: {df['title_english'].notna().sum():,} | with abstract: {(df['abstract'].notna() | df['abstract_english'].notna()).sum():,}")

    if args.limit:
        log("--limit run: refusing to upload a truncated corpus (use full run for S3)")
        return
    if args.skip_upload:
        log("--skip-upload: done (no S3 write)")
        return
    if not check_no_shrink(len(df), args.allow_shrink, args.output_dir):
        sys.exit(2)
    upload_to_s3(out)


if __name__ == "__main__":
    main()
