#!/usr/bin/env python3
"""
NCBR (Narodowe Centrum Badan i Rozwoju, Poland) to S3 Data Pipeline
===================================================================

Harvests NCBR-financed research projects from RAD-on (radon.nauka.gov.pl),
Poland's national research-information system (POLON registry, operated by
OPI-PIB for the science ministry), filters to projects whose
financingInstitutions include NCBR, and uploads a parquet to S3.

Why RAD-on and not dane.gov.pl dataset 2785:
- The tracker's source (https://dane.gov.pl/en/dataset/2785) holds a single
  XLSX with only 215 domestic-programme contracts signed in 2021 (5 columns,
  no dates/PIs) — probed 2026-07-12. NCBR publishes no other bulk file.
- RAD-on's open API (https://radon.nauka.gov.pl/opendata/polon/projects) is
  the systemic national source: ~48K projects with titles (PL/EN),
  abstracts, dates, PLN amounts per financing institution, project managers
  (firstName/lastName — no name splitting needed) and implementing
  institutions. ~10% are NCBR-financed (domestic programmes: LIDER,
  STRATEGMED, TECHMATSTRATEG, GOSPOSTRATEG, CyberSecIdent, TANGO, DOB
  (defence), plus EU operational programmes POIR/POWER where NCBR is the
  granting institution).
- Caveat: POLON registers projects reported by research institutions, so
  NCBR grants to companies with no research-institution consortium partner
  are not covered.

API: no auth, token-based pagination, max 100 records/request.
Filter: financingInstitutionUuid == ff801008-38fa-4388-b3df-749f510215d0
        (or name match on "Narodowe Centrum Badan i Rozwoju").

Output: s3://openalex-ingest/awards/ncbr/ncbr_projects.parquet

Usage:
    python ncbr_to_s3.py [--output-dir DIR] [--limit N] [--resume]
                         [--skip-upload] [--allow-shrink] [--max-pages N]
"""

import argparse
import json
import os
import sys
import time
import unicodedata
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

API_URL = "https://radon.nauka.gov.pl/opendata/polon/projects"
PAGE_SIZE = 100
NCBR_UUID = "ff801008-38fa-4388-b3df-749f510215d0"
MAX_CONSECUTIVE_NON200 = 5     # §1: non-200 != end of corpus
REQUEST_SLEEP = 0.25

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ncbr/ncbr_projects.parquet"

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


def is_ncbr(fi: dict) -> bool:
    if (fi.get("financingInstitutionUuid") or "") == NCBR_UUID:
        return True
    name = unicodedata.normalize("NFKD", fi.get("financingInstitutionName") or "")
    name = "".join(c for c in name if not unicodedata.combining(c)).lower()
    return "narodowe centrum badan i rozwoju" in name


def record_to_row(rec: dict, fi: dict) -> dict:
    # Project manager: prefer kind KP (kierownik projektu = project leader),
    # else the first listed manager. RAD-on gives firstName/lastName natively.
    managers = rec.get("projectManagers") or []
    mgr = None
    for m in managers:
        if (m.get("kindManagerCode") or "").upper() == "KP":
            mgr = m
            break
    if mgr is None and managers:
        mgr = managers[0]

    # Leading implementing institution
    impls = rec.get("implementingInstitutions") or []
    leader_inst = None
    for inst in impls:
        if (inst.get("leader") or "") == "Tak":
            leader_inst = inst.get("institutionName")
            break
    if leader_inst is None and impls:
        leader_inst = impls[0].get("institutionName")

    return {
        "project_uuid": rec.get("projectUuid"),
        "project_number": (rec.get("projectNumber") or "").strip() or None,
        "title_pl": rec.get("projectTitlePl"),
        "title_en": rec.get("projectTitleEn"),
        "abstract_pl": rec.get("projectAbstractPl"),
        "abstract_en": rec.get("projectAbstractEn"),
        "start_date": rec.get("projectStartDate"),
        "end_date": rec.get("projectEndDate"),
        "grant_date": fi.get("grantDate") or rec.get("projectGrantDate"),
        "amount_pln": fi.get("receivedFunds"),      # NCBR's share for this project
        "national_funds_pln": fi.get("nationalFunds"),
        "foreign_funds_pln": fi.get("foreignFunds"),
        "total_funds_pln": rec.get("totalFunds"),
        "scheme": rec.get("projectClassification"),
        "keywords": rec.get("keywords"),
        "manager_first_name": (mgr or {}).get("firstName"),
        "manager_last_name": (mgr or {}).get("lastName"),
        "manager_kind": (mgr or {}).get("kindManager"),
        "manager_institution": (mgr or {}).get("institutionName"),
        "leader_institution": leader_inst,
        "entity_showing_achievements": rec.get("entityShowingAchievementsName"),
        "implementing_institutions_json": json.dumps(impls, ensure_ascii=False),
        "project_managers_json": json.dumps(managers, ensure_ascii=False),
        "financing_institution_json": json.dumps(fi, ensure_ascii=False),
        "data_source": rec.get("dataSource"),
    }


def harvest(output_dir: Path, limit=None, resume=False, max_pages=2000) -> Path:
    """Page through the whole RAD-on projects corpus, keep NCBR rows as JSONL."""
    jsonl = output_dir / "ncbr_radon_rows.jsonl"
    state_path = output_dir / "ncbr_harvest_state.json"

    token = None
    pages_done = 0
    scanned = 0
    kept = 0
    if resume and state_path.exists():
        st = json.loads(state_path.read_text())
        token, pages_done, scanned, kept = st["token"], st["pages"], st["scanned"], st["kept"]
        log(f"Resuming from page {pages_done} (scanned {scanned:,}, kept {kept:,})")
    elif jsonl.exists():
        jsonl.unlink()

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    consecutive_non200 = 0
    max_count = None
    t0 = time.time()

    with open(jsonl, "a", encoding="utf-8") as fh:
        while pages_done < max_pages:
            url = f"{API_URL}?resultNumbers={PAGE_SIZE}"
            if token:
                url += f"&token={token}"
            try:
                r = session.get(url, timeout=120)
            except requests.RequestException as e:
                consecutive_non200 += 1
                log(f"page {pages_done}: {type(e).__name__} ({consecutive_non200}/{MAX_CONSECUTIVE_NON200}); retrying")
                if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                    raise
                time.sleep(5)
                continue
            if r.status_code != 200:
                consecutive_non200 += 1
                log(f"page {pages_done}: HTTP {r.status_code} ({consecutive_non200}/{MAX_CONSECUTIVE_NON200}); retrying")
                if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                    raise RuntimeError(f"{MAX_CONSECUTIVE_NON200} consecutive non-200s — aborting rather than truncating")
                time.sleep(5)
                continue
            consecutive_non200 = 0

            d = r.json()
            results = d.get("results") or []
            pagination = d.get("pagination") or {}
            if max_count is None:
                max_count = pagination.get("maxCount")
                log(f"Corpus reports maxCount={max_count:,}")

            for rec in results:
                scanned += 1
                for fi in rec.get("financingInstitutions") or []:
                    if is_ncbr(fi):
                        fh.write(json.dumps(record_to_row(rec, fi), ensure_ascii=False) + "\n")
                        kept += 1
                        break

            pages_done += 1
            token = pagination.get("token")

            if pages_done % 20 == 0 or scanned >= (max_count or 0):
                elapsed = time.time() - t0
                rate = scanned / elapsed if elapsed else 0
                eta = ((max_count or scanned) - scanned) / rate if rate else 0
                log(f"[page {pages_done}] scanned {scanned:,}/{max_count:,} | NCBR kept {kept:,} | ETA {eta/60:.1f} min")
                fh.flush()
                state_path.write_text(json.dumps(
                    {"token": token, "pages": pages_done, "scanned": scanned, "kept": kept}))

            if limit and kept >= limit:
                log(f"--limit {limit}: stopping harvest early with {kept:,} NCBR rows")
                break
            # Authoritative terminator: the source's reported total (§1).
            if max_count is not None and scanned >= max_count:
                log("Reached reported maxCount — harvest complete")
                break
            if not token:
                # No continuation token: end of corpus. maxCount is slightly
                # approximate (observed 47,741 scanned vs 47,766 reported =
                # 99.95%), so allow a 0.5% shortfall; anything larger is a
                # silent truncation and must raise.
                if max_count is not None and scanned < max_count * 0.995:
                    raise RuntimeError(f"pagination token exhausted at {scanned:,}/{max_count:,} — refusing to truncate silently")
                if max_count is not None and scanned < max_count:
                    log(f"  token exhausted at {scanned:,}/{max_count:,} (>=99.5% — maxCount is approximate); accepting as end of corpus")
                break
            time.sleep(REQUEST_SLEEP)

    log(f"Harvest done: scanned {scanned:,}, kept {kept:,} NCBR rows -> {jsonl}")
    return jsonl


def build_dataframe(jsonl: Path) -> pd.DataFrame:
    # NB: split on '\n' only — abstracts can contain U+2028 line separators
    # that str.splitlines() would split on, corrupting JSONL lines.
    rows = [json.loads(line) for line in jsonl.read_text().split("\n") if line.strip()]
    df = pd.DataFrame(rows)
    log(f"  {len(df):,} raw NCBR rows")
    df = df[df["project_number"].notna()].copy()
    log(f"  {len(df):,} rows with a project number (funder_award_id)")
    # §1.2 #6: dedup by the column shipped as amount — consortium members
    # register the same projectNumber separately in POLON; keep max NCBR share.
    df["_amt"] = pd.to_numeric(df["amount_pln"], errors="coerce")
    df = df.sort_values("_amt", ascending=False, na_position="last")
    before = len(df)
    df = df.drop_duplicates(subset=["project_number"], keep="first").drop(columns=["_amt"]).reset_index(drop=True)
    log(f"  deduped {before - len(df):,} duplicate project_numbers -> {len(df):,} projects")
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
    parser = argparse.ArgumentParser(description="NCBR (via RAD-on) -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).parent / "ncbr_data",
                        help="Directory for downloaded/processed files")
    parser.add_argument("--limit", type=int, default=None,
                        help="Smoke test: stop after keeping N NCBR rows")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--skip-harvest", action="store_true", help="Reuse existing JSONL")
    parser.add_argument("--skip-upload", action="store_true", help="Build parquet only")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override the §1.4 never-shrink safety check")
    parser.add_argument("--max-pages", type=int, default=2000,
                        help="Hard cap on pages (corpus is ~478 pages of 100)")
    args = parser.parse_args()

    load_repo_dotenv()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    jsonl = args.output_dir / "ncbr_radon_rows.jsonl"
    if not args.skip_harvest:
        jsonl = harvest(args.output_dir, limit=args.limit, resume=args.resume,
                        max_pages=args.max_pages)
    else:
        log(f"--skip-harvest: using {jsonl}")

    df = build_dataframe(jsonl)
    out = args.output_dir / "ncbr_projects.parquet"
    df.to_parquet(out, index=False)
    n_amt = pd.to_numeric(df["amount_pln"], errors="coerce").notna().sum()
    n_pi = df["manager_last_name"].notna().sum()
    n_title = (df["title_pl"].notna() | df["title_en"].notna()).sum()
    log(f"Wrote {len(df):,} rows -> {out} ({out.stat().st_size/1e6:.1f} MB)")
    log(f"  with amount: {n_amt:,} | with manager (PI): {n_pi:,} | with title: {n_title:,}")

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
