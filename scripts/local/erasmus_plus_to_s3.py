#!/usr/bin/env python3
"""
Erasmus+ Project Results platform -> S3 Data Pipeline
=====================================================

Downloads ALL funded projects from the Erasmus+ Project Results platform
(https://erasmus-plus.ec.europa.eu/projects) via its VALOR search service —
the JSON endpoint the SPA itself calls:

    POST https://ec.europa.eu/programmes/service/search/project/search
    body: {"index": "eplus2021", "from": N, "size": 500,
           "project": {"projectCallYear": "YYYY"}, ...}

No auth/API key required. ~327K projects covering both programme periods
(2014-2020 and 2021-2027). A bulk file was looked for FIRST per the ingest
method ladder: data.europa.eu has no central Erasmus+ *projects* dataset
(only mobility statistics and per-university lists), so the search API it is.

Pagination strategy: the corpus is partitioned by projectCallYear (13 call
years, each <= ~40K rows; bucket totals sum exactly to the grand total) and
paged with from/size inside each partition. Deep offsets ARE allowed by this
endpoint (tested past 326,700), but per-year partitions keep windows small,
make the harvest resumable, and let us verify each partition's count.
Rows are deduped by projectId (GUID) at build time.

Fields: project native reference (projectName, e.g. 2021-1-DE01-KA220-...),
title, descriptions, dates, key action (level1/2/3 labels), call year, EU
grant amount (projectGrantedEuAmount, EUR), status, website, coordinator
organisation (name/PIC/country/city/type) and full partner list (JSON).
There is no person PI in the source — the platform publishes organisations
only (source-authority NULL for lead_investigator person fields; the
coordinator org fills lead_investigator.affiliation).

Output: s3://openalex-ingest/awards/erasmus_plus/erasmus_plus_projects.parquet

Funder: Erasmus+, OpenAlex funder_id 4320335551 (F4320* — in the Databricks
common.funder dim). Currency: EUR (implicit; EU programme).

Usage
-----
    python erasmus_plus_to_s3.py                   # full harvest (~1h) + upload
    python erasmus_plus_to_s3.py --limit 600       # smoke test (stops early)
    python erasmus_plus_to_s3.py --skip-upload     # no S3 upload
    python erasmus_plus_to_s3.py --resume          # resume from checkpoint
    python erasmus_plus_to_s3.py --allow-shrink    # override §1.4 guard

Requirements
------------
    pip install pandas pyarrow requests boto3
    AWS creds (repo .env is auto-loaded if AWS_ACCESS_KEY_ID unset)
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22, runbook §1.2 #7) ---
import sys as _sys_utf8
try:
    _sys_utf8.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    _sys_utf8.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
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
    def _open(file, mode="r", buffering=-1, encoding=None, errors=None,
              newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open
# --- end shim ---

SEARCH_URL = "https://ec.europa.eu/programmes/service/search/project/search"
DETAILS_BASE = "https://erasmus-plus.ec.europa.eu/projects/search/details/"
PAGE_SIZE = 500
MAX_CONSECUTIVE_EMPTY = 3      # §1: empty page != end of corpus
MAX_CONSECUTIVE_NON200 = 5

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/erasmus_plus/erasmus_plus_projects.parquet"

USER_AGENT = "OpenAlex awards ingest (mailto:kyle@ourresearch.org)"

KEEP_ORG_FIELDS = ["organisationId", "organisationName", "organisationPIC",
                   "organisationRole", "organisationCountry", "organisationCity",
                   "organisationType", "organisationWebsite"]


def load_repo_dotenv() -> None:
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def build_body(call_year=None, from_=0, size=PAGE_SIZE, with_aggs=False) -> dict:
    project = {}
    if call_year:
        project["projectCallYear"] = str(call_year)
    return {
        "from": from_,
        "index": "eplus2021",
        "keyword": "",
        "matchAllCountries": False,
        "project": project,
        "coordinator": {},
        "partner": {},
        "organisation": {"organisationRole": "COORDINATOR_OR_PARTNER"},
        "size": size,
        "sort": "",
        "withAggregations": with_aggs,
    }


def post_search(session: requests.Session, body: dict) -> dict:
    resp = session.post(SEARCH_URL, json=body, timeout=120)
    resp.raise_for_status()
    return resp.json()


def get_call_year_partitions(session: requests.Session) -> tuple:
    """Return ([(year, count), ...] descending by count, grand_total)."""
    d = post_search(session, build_body(size=0, with_aggs=True))
    total = d["total"]
    buckets = [(b["label"], b["total"]) for b in d["aggregations"]["callYear"]]
    if sum(n for _, n in buckets) != total:
        raise RuntimeError(
            f"callYear buckets sum {sum(n for _, n in buckets)} != total {total}; "
            "partitioning would silently drop rows")
    return buckets, total


def doc_to_record(doc: dict) -> dict:
    orgs = doc.get("organisations") or []
    coord = next((o for o in orgs if (o.get("organisationRole") or "").lower() == "coordinator"), None)
    partners = [{k: o.get(k) for k in KEEP_ORG_FIELDS} for o in orgs
                if (o.get("organisationRole") or "").lower() != "coordinator"]
    name = doc.get("projectName")
    return {
        "project_uuid": doc.get("projectId"),
        "project_reference": name,                     # native id, e.g. 2021-1-DE01-KA220-SCH-000024914
        "project_title": doc.get("projectTitle"),
        "project_description": doc.get("projectDescription"),
        "description_background": doc.get("projectDescriptionBackground"),
        "description_objectives": doc.get("projectDescriptionObjectives"),
        "description_implementation": doc.get("projectDescriptionImplementation"),
        "description_results": doc.get("projectDescriptionResults"),
        "programme_years": doc.get("projectProgrammeYears"),
        "call_year": doc.get("projectCallYear"),
        "level1_label": doc.get("projectLevel1Label"),
        "level2_label": doc.get("projectLevel2Label"),   # key action
        "level3_label": doc.get("projectLevel3Label"),   # action type / scheme
        "granted_eu_amount": (str(doc["projectGrantedEuAmount"])
                              if doc.get("projectGrantedEuAmount") is not None else None),
        "start_date": (doc.get("projectStartDate") or "")[:10] or None,
        "end_date": (doc.get("projectEndDate") or "")[:10] or None,
        "status": doc.get("projectStatus"),
        "good_practice": str(doc.get("projectGoodPractice")),
        "project_website": doc.get("projectWebsite"),
        "landing_page_url": (DETAILS_BASE + name) if name else None,
        "coordinator_name": (coord or {}).get("organisationName"),
        "coordinator_pic": (coord or {}).get("organisationPIC"),
        "coordinator_country": (coord or {}).get("organisationCountry"),
        "coordinator_city": (coord or {}).get("organisationCity"),
        "coordinator_website": (coord or {}).get("organisationWebsite"),
        "partners_json": json.dumps(partners, ensure_ascii=False) if partners else None,
        "n_partners": str(len(partners)),
    }


def harvest(session: requests.Session, output_dir: Path, limit=None, resume=False) -> Path:
    """Harvest all partitions to a JSONL file; returns its path."""
    jsonl_path = output_dir / "erasmus_plus_records.jsonl"
    ckpt_path = output_dir / "erasmus_plus_checkpoint.json"
    ckpt = {"done_years": [], "current_year": None, "current_from": 0}
    if resume and ckpt_path.exists():
        ckpt = json.loads(ckpt_path.read_text())
        print(f"  [RESUME] done years: {ckpt['done_years']}; "
              f"current {ckpt['current_year']} from {ckpt['current_from']}")
    elif jsonl_path.exists() and not resume:
        jsonl_path.unlink()

    partitions, grand_total = get_call_year_partitions(session)
    print(f"  corpus total: {grand_total:,} projects across {len(partitions)} call years")
    fetched_total = 0
    t0 = time.time()

    for year, count in sorted(partitions):
        if year in ckpt["done_years"]:
            continue
        from_ = ckpt["current_from"] if ckpt.get("current_year") == year else 0
        consecutive_empty = consecutive_non200 = 0
        print(f"  [{year}] expecting {count:,} projects")
        while from_ < count:
            body = build_body(call_year=year, from_=from_, size=PAGE_SIZE)
            try:
                resp = session.post(SEARCH_URL, json=body, timeout=120)
            except requests.RequestException as e:
                consecutive_non200 += 1
                print(f"    from={from_}: {type(e).__name__} "
                      f"({consecutive_non200}/{MAX_CONSECUTIVE_NON200}); retrying")
                if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                    raise RuntimeError(f"[{year}] repeated network failures at from={from_}")
                time.sleep(5 * consecutive_non200)
                continue
            if resp.status_code != 200:
                consecutive_non200 += 1
                print(f"    from={from_}: HTTP {resp.status_code} "
                      f"({consecutive_non200}/{MAX_CONSECUTIVE_NON200}); continuing")
                if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                    raise RuntimeError(f"[{year}] repeated HTTP errors at from={from_}")
                time.sleep(5 * consecutive_non200)
                continue
            consecutive_non200 = 0
            docs = resp.json().get("projectDocuments") or []
            if not docs:
                consecutive_empty += 1
                print(f"    from={from_}: empty page "
                      f"({consecutive_empty}/{MAX_CONSECUTIVE_EMPTY}); continuing")
                if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                    print(f"    [{year}] {MAX_CONSECUTIVE_EMPTY} consecutive empties — "
                          f"treating as partition EOF at {from_}/{count}")
                    break
                from_ += PAGE_SIZE
                continue
            consecutive_empty = 0
            with open(jsonl_path, "a") as fh:
                for doc in docs:
                    fh.write(json.dumps(doc_to_record(doc), ensure_ascii=False) + "\n")
            fetched_total += len(docs)
            from_ += len(docs)
            elapsed = time.time() - t0
            rate = fetched_total / elapsed if elapsed else 1
            eta_min = (grand_total - fetched_total) / rate / 60 if rate else 0
            print(f"    [{elapsed:5.0f}s] {year}: {from_:,}/{count:,} "
                  f"(session total {fetched_total:,}; ETA {eta_min:.0f}m)")
            ckpt.update(current_year=year, current_from=from_)
            ckpt_path.write_text(json.dumps(ckpt))
            if limit and fetched_total >= limit:
                print(f"  [LIMIT] stopping after {fetched_total:,} records")
                return jsonl_path
            time.sleep(0.4)
        ckpt["done_years"].append(year)
        ckpt.update(current_year=None, current_from=0)
        ckpt_path.write_text(json.dumps(ckpt))
    return jsonl_path


def build_dataframe(jsonl_path: Path) -> pd.DataFrame:
    records = {}
    dup_refs = 0
    with open(jsonl_path) as fh:
        for line in fh:
            r = json.loads(line)
            key = r.get("project_uuid") or r.get("project_reference")
            if key in records:
                dup_refs += 1
            records[key] = r
    df = pd.DataFrame(list(records.values()))
    print(f"  {len(df):,} unique projects ({dup_refs} duplicate fetches collapsed)")
    # funder_award_id collision check — MUST raise, not warn (runbook)
    refs = df["project_reference"].dropna()
    dup = refs[refs.duplicated()].unique()
    if len(dup):
        raise RuntimeError(f"project_reference collisions (funder_award_id): {dup[:10]}")
    return df.astype("string")


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 required for §1.4 shrink-check; rerun with --skip-upload to bypass"
        ) from exc
    client = boto3.client("s3")
    print(f"  §1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet — first ingest, no shrink check.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_erasmus_plus_projects.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        print(f"    [ERROR] couldn't read existing parquet ({e}); aborting upload.")
        return False
    print(f"    previous: {prev_count}   new: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            print("    [OVERRIDE] --allow-shrink set; proceeding.")
            return True
        print(f"\n[ERROR] §1.4 violation: refusing to shrink ({prev_count} -> {new_count}).")
        return False
    print("    [OK] not smaller; safe to overwrite.")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Erasmus+ Project Results -> S3")
    parser.add_argument("--output-dir", default=str(Path(__file__).parent / "erasmus_plus_data"),
                        help="Directory for checkpoints + parquet output")
    parser.add_argument("--limit", type=int, default=None,
                        help="Smoke test: stop after ~N records")
    parser.add_argument("--skip-upload", action="store_true", help="Build parquet only")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override the §1.4 never-shrink guard")
    args = parser.parse_args()

    load_repo_dotenv()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Content-Type": "application/json"})

    print("=" * 60)
    print("Step 1: Harvest Erasmus+ Project Results (VALOR search API)")
    print("=" * 60)
    jsonl_path = harvest(session, output_dir, limit=args.limit, resume=args.resume)

    print("\n" + "=" * 60)
    print("Step 2: Build dataframe (dedupe + collision check + string dtype)")
    print("=" * 60)
    df = build_dataframe(jsonl_path)
    amt_cov = df["granted_eu_amount"].notna().mean() * 100
    print(f"  amount coverage: {amt_cov:.1f}%")

    parquet_path = output_dir / "erasmus_plus_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"  [OK] wrote {len(df):,} rows ({parquet_path.stat().st_size/1024/1024:.1f} MB) "
          f"to {parquet_path}")

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading.")
        return
    if args.limit:
        print("\n[SKIP] --limit set; refusing to upload a truncated corpus.")
        return

    print("\n" + "=" * 60)
    print("Step 3: Upload to S3 (with §1.4 shrink check)")
    print("=" * 60)
    if not check_no_shrink(len(df), args.allow_shrink, output_dir):
        sys.exit(1)
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  Uploading {parquet_path} -> {s3_uri}")
    subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
    print(f"  [OK] uploaded to {s3_uri}")
    print(f"\nDONE {datetime.now().isoformat(timespec='seconds')}")


if __name__ == "__main__":
    main()
