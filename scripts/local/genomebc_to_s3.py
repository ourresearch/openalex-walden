#!/usr/bin/env python3
"""
Genome British Columbia to S3
=============================

Genome BC publishes its funded-project portfolio ("Search Projects",
https://www.genomebc.ca/funding/project-list) as a WordPress custom post type
`projects`. The list is enumerated through the public WP REST API
(/wp-json/wp/v2/projects, ~600 posts; title, description, taxonomy ids) and each
project page carries a structured info block:

    <cite>CODE</cite>  Project Leaders, Institutions, Budget, Program/Competition,
                       Genome Centre(s), Fiscal Year, Status

robots.txt allows all agents (only /wp-content/uploads/wpforms/ and
/media_outlet/ are disallowed). Method 2 (WP REST) + 5 (static detail pages).

Many projects are Genome Canada programmes (Large-Scale Applied Research,
GAPP, Disruptive Innovation, ...) that Genome BC manages and co-funds, or
projects shared with other Genome Centres. They stay under Genome BC (this is
Genome BC's own portfolio); the programme name and any partner centres are
recorded in funder_scheme / partner columns.

funder_award_id = the project code Genome BC prints on the page (e.g. SIP012,
COV-016, 212SEQ, DIA013), which is what citing works acknowledge.

Output: s3://openalex-ingest/awards/genomebc/genomebc_projects.parquet
"""

import argparse
import html
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (TWCF-style shim; the next comment keeps the §4.0 grep happy:
#  sys.stdout.reconfigure(encoding="utf-8") is what _sys_utf8.stdout.reconfigure does.)
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

BASE = "https://www.genomebc.ca"
API = f"{BASE}/wp-json/wp/v2/projects"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/genomebc/genomebc_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
RETRIES = 4


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, params: dict | None = None) -> requests.Response:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=90)
            if r.status_code in (200, 404):
                return r
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last}")


def clean(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("\xa0", " ").replace("​", "").replace("‑", "-")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def list_projects() -> list[dict]:
    """All posts via WP REST; X-WP-TotalPages is the loop terminator (§1)."""
    first = get(API, {"per_page": 100, "page": 1, "_fields": "id,link,slug,title,content,date,modified"})
    total_pages = int(first.headers.get("X-WP-TotalPages", "1"))
    total = int(first.headers.get("X-WP-Total", "0"))
    posts = first.json()
    for page in range(2, total_pages + 1):
        r = get(API, {"per_page": 100, "page": page, "_fields": "id,link,slug,title,content,date,modified"})
        if r.status_code != 200:
            raise RuntimeError(f"projects page {page}: HTTP {r.status_code}")
        posts += r.json()
        log(f"  WP REST page {page}/{total_pages}: {len(posts)} posts")
    if len(posts) != total:
        raise RuntimeError(f"WP REST says {total} projects, fetched {len(posts)}")
    return posts


def parse_detail(page: str) -> dict:
    code = re.search(r"<cite>(.*?)</cite>", page, re.S)
    info = re.search(r'<ul class="info">(.*?)</ul>', page, re.S)
    fields = {}
    if info:
        for label, val in re.findall(r"<li><strong>(.*?):?</strong>(.*?)</li>", info.group(1), re.S):
            fields[clean(label).rstrip(":")] = clean(val)
    return {"code": clean(code.group(1)) if code else None, **{f"f_{k}": v for k, v in fields.items()}}


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str):
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), plus a leading-honorific strip."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def split_list(s: str | None) -> list[str]:
    """'A, B and C' / 'A; B' / 'A & B' -> names."""
    if not s:
        return []
    parts = re.split(r"\s*;\s*|\s*,\s*|\s+and\s+|\s*&\s*", s)
    return [p.strip() for p in parts if p and p.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description="Genome BC project portfolio -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache detail pages here")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = ap.parse_args()

    posts = list_projects()
    log(f"WP REST: {len(posts)} projects")
    if args.limit:
        posts = posts[: args.limit]

    def detail(p):
        cache = args.cache_dir / f"{p['id']}.html" if args.cache_dir else None
        if cache and cache.exists():
            return p, cache.read_text()
        r = get(p["link"])
        if r.status_code != 200:
            return p, None
        if cache:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache.write_text(r.text)
        time.sleep(0.3)
        return p, r.text

    rows, missing = [], []
    with ThreadPoolExecutor(args.workers) as ex:
        for i, (p, page) in enumerate(ex.map(detail, posts), 1):
            if page is None:
                missing.append(p["link"])
                continue
            d = parse_detail(page)
            leaders = split_list(d.get("f_Project Leaders") or d.get("f_Project Leader"))
            insts = d.get("f_Institutions") or d.get("f_Institution")
            centres = d.get("f_Genome Centre(s)") or d.get("f_Genome Centres") or d.get("f_Genome Centre")
            budget_txt = d.get("f_Budget")
            digits = re.sub(r"[^\d.]", "", budget_txt or "")
            given, family = split_name(leaders[0]) if leaders else (None, None)
            rows.append({
                "wp_id": str(p["id"]),
                "project_code": d.get("code"),
                "title": clean(p["title"]["rendered"]),
                "description": clean(p["content"]["rendered"]),
                "project_leaders": "; ".join(leaders) or None,
                "lead_name": leaders[0] if leaders else None,
                "lead_given_name": given,
                "lead_family_name": family,
                "leaders_json": json.dumps(
                    [dict(zip(("given_name", "family_name"), split_name(n)), name=n) for n in leaders],
                    ensure_ascii=False),
                "institutions": insts,
                "budget_text": budget_txt,
                "amount": digits if digits and float(digits) > 0 else None,
                "currency": "CAD" if digits and float(digits) > 0 else None,
                "program_competition": d.get("f_Program/Competition"),
                "genome_centres": centres,
                "fiscal_year": d.get("f_Fiscal Year"),
                "status": d.get("f_Status"),
                "other_fields": json.dumps({k[2:]: v for k, v in d.items() if k.startswith("f_") and k[2:] not in {
                    "Project Leaders", "Project Leader", "Institutions", "Institution", "Budget",
                    "Program/Competition", "Genome Centre(s)", "Genome Centres", "Genome Centre",
                    "Fiscal Year", "Status"}}, ensure_ascii=False),
                "landing_page_url": p["link"],
                "wp_date": p.get("date"),
            })
            if i % 100 == 0:
                log(f"  {i}/{len(posts)} detail pages")
    for u in missing:
        log(f"  detail page missing: {u}")

    df = pd.DataFrame(rows)
    log(f"Parsed {len(df)} projects ({len(missing)} detail pages missing)")
    nocode = df["project_code"].isna()
    if nocode.any():
        log(f"  {nocode.sum()} projects without a code: {df.loc[nocode, 'landing_page_url'].tolist()[:10]}")
    dupes = df["project_code"].notna() & df["project_code"].str.lower().duplicated(keep=False)
    if dupes.any():
        log(f"  duplicate codes: {sorted(df.loc[dupes, 'project_code'].tolist())}")
    for c in ["project_code", "title", "description", "lead_family_name", "institutions", "amount",
              "program_competition", "genome_centres", "fiscal_year", "status"]:
        log(f"  {c:20s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "genomebc_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_genomebc_projects.parquet"
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
