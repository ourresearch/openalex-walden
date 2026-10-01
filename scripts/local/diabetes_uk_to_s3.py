#!/usr/bin/env python3
"""
Diabetes UK to S3 Data Pipeline (Europe PMC funder-deposited grant list)
========================================================================

Diabetes UK (OpenAlex F4320320065, GB) is a Europe PMC funder: it deposits
its own research-grant list with Europe PMC, which serves it through the
GRIST grants API:

    https://www.ebi.ac.uk/europepmc/GristAPI/rest/get/query=ga:"Diabetes UK"&resultType=core&format=json&page=N

This is the funder's own bulk export (ladder item 0): one record per grant
holder with the Diabetes UK grant reference (``Id``, e.g. ``25/0006928``),
title, abstract, award type, start/end dates, amount + currency, the holder's
structured given/family name (+ ORCID where known) and the administering
institution with its ROR id. ~587 records / ~577 grants, 2003-2026.

Diabetes UK does NOT publish a 360Giving file (checked the 360Giving registry
2026-09-30), and its website only lists ~120 currently active projects with
no grant numbers, so the Europe PMC deposit is the richest direct source.

Records sharing a grant ``Id`` are the same grant with several holders
(co-applicants); they are collapsed to one row, holders kept in order.

``funder_award_id`` (runbook §2.1.1): the Diabetes UK grant reference
``YY/000NNNN`` is exactly what citing works carry (Crossref funder metadata
on F4320320065: ``20/0006307``, ``12/0004538`` ...), so it is shipped as-is.

Output: s3://openalex-ingest/awards/diabetes_uk/diabetes_uk_projects.parquet
"""

import argparse
import json
import re
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (renamed-sys variant; equivalent to sys.stdout.reconfigure(...) — runbook §1.2)
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). No-op on
# Linux/Databricks. See runbook §1.2.
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
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---

AGENCY = "Diabetes UK"
FUNDREF = "10.13039/501100000361"  # Diabetes UK, OpenAlex F4320320065
SLUG = "diabetes_uk"
S3_BUCKET = "openalex-ingest"
S3_KEY = f"awards/{SLUG}/{SLUG}_projects.parquet"
GRIST = "https://www.ebi.ac.uk/europepmc/GristAPI/rest/get/query={q}&resultType=core&format=json&page={p}"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org; team@ourresearch.org)"}
REQUEST_DELAY = 0.5
RETRIES = 4
MAX_CONSECUTIVE_EMPTY = 3
QUERY_VARIANTS = ['ga:"Diabetes UK"', 'ga:"diabetes uk"', 'ga:"DIABETES UK"', 'ga:"Diabetes uk"']


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get_json(url: str) -> dict:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            log(f"  GET {url[-60:]} -> {r.status_code} ({len(r.content)} bytes)")
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last_err}")


def fetch_grist(query: str, limit: int | None) -> tuple[int, list[dict]]:
    """Page through GRIST until the reported HitCount is reached. Empty pages
    before that are logged and skipped (runbook Step 1: empty page != EOF)."""
    q = urllib.parse.quote(query)
    records, page, hit_count, empty = [], 1, None, 0
    while True:
        d = get_json(GRIST.format(q=q, p=page))
        hit_count = int(d.get("HitCount") or 0)
        recs = (d.get("RecordList") or {}).get("Record") or []
        if isinstance(recs, dict):
            recs = [recs]
        if recs:
            empty = 0
            records += recs
        else:
            empty += 1
            log(f"  page {page}: empty ({empty}/{MAX_CONSECUTIVE_EMPTY})")
        log(f"  page {page}: {len(records)}/{hit_count} records")
        if len(records) >= hit_count or empty >= MAX_CONSECUTIVE_EMPTY:
            break
        if limit and len(records) >= limit:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
    if not limit and len(records) < hit_count:
        raise RuntimeError(f"GRIST returned {len(records)} of {hit_count} records; refusing a partial corpus")
    return hit_count, records


def fetch_union(limit: int | None, max_rounds: int = 6) -> list[dict]:
    """GRIST paging is NOT stable: a full pass returns HitCount records but
    repeats some and silently skips others, and repeating the SAME query
    tends to skip the same ones (2026-09-30: three identical passes all
    missed the same 5 grants). Different spellings of the query (case,
    alias) page in a different order, so union full passes over several
    query variants until the distinct-record count reaches HitCount.
    Tolerates a <=2% shortfall (GRIST ordering varies over time: on
    2026-09-30 one Diabetes UK run reached 587/587 in 3 passes, a later one
    stalled at 578/587 after 24; the §1.4 shrink guard then refuses to
    overwrite a fuller earlier upload). Genuinely identical records can
    never be counted twice); anything bigger raises."""
    if limit:
        return fetch_grist(QUERY_VARIANTS[0], limit)[1]
    seen: dict[str, dict] = {}
    hit_count = 0
    for rnd in range(1, max_rounds + 1):
        for q in QUERY_VARIANTS:
            hit_count, recs = fetch_grist(q, None)
            before = len(seen)
            for r in recs:
                seen.setdefault(json.dumps(r, sort_keys=True, ensure_ascii=False), r)
            log(f"GRIST round {rnd} [{q}]: +{len(seen) - before} -> {len(seen)}/{hit_count} distinct records")
            if len(seen) >= hit_count:
                return list(seen.values())
    if len(seen) >= 0.98 * hit_count:
        log(f"WARNING: {len(seen)}/{hit_count} distinct records after {max_rounds} rounds; proceeding (<=2% short; the §1.4 shrink guard still refuses a smaller corpus than the last upload)")
        return list(seen.values())
    raise RuntimeError(f"GRIST: only {len(seen)}/{hit_count} distinct records after {max_rounds} rounds")


def clean(s) -> str | None:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s or None


def person(rec: dict) -> dict:
    p = rec.get("Person") or {}
    inst = rec.get("Institution") or {}
    aliases = p.get("Alias") or []
    if isinstance(aliases, dict):
        aliases = [aliases]
    orcid = next((a.get("value") for a in aliases
                  if (a.get("Source") or "").upper() == "ORCID"
                  and re.fullmatch(r"\d{4}-\d{4}-\d{4}-\d{3}[\dX]", a.get("value") or "")), None)
    ror = clean(inst.get("RORID"))
    return {
        "given_name": clean(p.get("GivenName")),
        "family_name": clean(p.get("FamilyName")),
        "title": clean(p.get("Title")),
        "orcid": orcid,
        "institution": clean(inst.get("Name")),
        "institution_ror": f"https://ror.org/{ror}" if ror and not ror.startswith("http") else ror,
    }


def funding_type(award_type: str | None) -> str:
    t = (award_type or "").lower()
    if "fellow" in t:
        return "fellowship"
    if "studentship" in t or "phd" in t or "postgraduate" in t:
        return "training"
    return "research"


def build_rows(records: list[dict]) -> list[dict]:
    by_id: dict[str, list[dict]] = {}
    dropped = 0
    for r in records:
        g = r.get("Grant") or {}
        fr = ((g.get("Funder") or {}).get("FundRefID") or "").lower()
        if not fr.endswith(FUNDREF):
            dropped += 1
            continue
        gid = clean(g.get("Id"))
        if gid:
            by_id.setdefault(gid, []).append(r)
    if dropped:
        log(f"  dropped {dropped} records with another FundRef id")
    rows = []
    for gid, recs in by_id.items():
        g = recs[0]["Grant"]
        people, seen = [], set()
        for r in recs:
            p = person(r)
            key = ((p["given_name"] or "").lower(), (p["family_name"] or "").lower())
            if key in seen or not p["family_name"]:
                continue
            seen.add(key)
            people.append(p)
        lead = people[0] if people else {}
        amt = g.get("Amount") or {}
        abstract = g.get("Abstract") or {}
        if isinstance(abstract, list):
            abstract = abstract[0] if abstract else {}
        q = urllib.parse.quote(f'gid:"{gid}" ga:"{AGENCY}"')
        rows.append({
            "grant_id": gid,
            "funder_award_id": gid,
            "title": clean(g.get("Title")),
            "abstract": clean(abstract.get("value")) if isinstance(abstract, dict) else clean(abstract),
            "award_type": clean(g.get("Type")),
            "funding_type": funding_type(g.get("Type")),
            "stream": clean(g.get("Stream")),
            "start_date": clean(g.get("StartDate")),
            "end_date": clean(g.get("EndDate")),
            "amount": amt.get("value"),
            "currency": clean(amt.get("Currency")),
            "lead_given_name": lead.get("given_name"),
            "lead_family_name": lead.get("family_name"),
            "lead_orcid": lead.get("orcid"),
            "lead_institution": lead.get("institution"),
            "lead_institution_ror": lead.get("institution_ror"),
            "people_json": json.dumps(people, ensure_ascii=False),
            "n_people": len(people),
            "n_source_records": len(recs),
            "landing_page_url": f"https://europepmc.org/grantfinder/grantdetails?query={q}",
            "source": "europepmc_grist",
        })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=f"{AGENCY} Europe PMC GRIST grant list -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="smoke test: stop after ~N source records")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = ap.parse_args()

    records = fetch_union(args.limit)
    log(f"GRIST: {len(records)} source records")
    rows = build_rows(records)
    df = pd.DataFrame(rows)
    log(f"Collapsed to {len(df)} grants")
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")
    for c in ["title", "abstract", "award_type", "start_date", "end_date", "amount",
              "lead_family_name", "lead_orcid", "lead_institution", "lead_institution_ror"]:
        log(f"  {c:22s} {df[c].notna().mean():6.1%}")
    log(f"  currencies {df['currency'].value_counts(dropna=False).to_dict()}")
    log(f"  years {df['start_date'].str[:4].min()}-{df['start_date'].str[:4].max()}")

    df["amount"] = df["amount"].map(lambda v: None if v is None or pd.isna(v) else repr(float(v)))
    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / f"{SLUG}_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload or args.limit:
        log("Upload skipped")
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / f"_previous_{SLUG}_projects.parquet"
    try:  # runbook §1.4: never shrink the corpus on re-ingest
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
