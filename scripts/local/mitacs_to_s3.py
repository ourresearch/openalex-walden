#!/usr/bin/env python3
"""
Mitacs to S3 Data Pipeline
==========================

Mitacs (Canadian not-for-profit funding research internships and
fellowships: Accelerate, Elevate, Globalink Research Award, Business
Strategy Internship) publishes every completed project on
https://www.mitacs.ca/projects/ (WordPress, custom post type `project`,
~31,100 projects; X-WP-Total on /wp-json/wp/v2/project).

The paginated listing (https://www.mitacs.ca/projects/page/{n}/, 12 per
page, ~2,595 pages) renders each project's full record inline in its
slide-out panel, so the listing alone gives everything the per-project
pages have, in 1/12th of the requests:

    title, description, landing URL (slug), Faculty Supervisor, Student,
    Partner, Discipline, Sector, University, Program

Method 5 (static HTML) on the runbook ladder. Ladder item 0 checked
2026-09-30: no CSV/Excel export on mitacs.ca; the only open.canada.ca /
donneesquebec.ca Mitacs dataset ("Business innovation internships") is
aggregate counts, not project-level. The WP REST `project` endpoint
exposes title/content/taxonomy term ids but NOT the supervisor/student,
and its post dates are the 2026-05-27 site re-import, not award dates.

What Mitacs does NOT publish: award/reference numbers (the IT##### /
FR##### refs researchers cite), start/end dates, or amounts. So
funder_award_id is the project's public URL slug (the only public-facing
per-project reference; runbook §2.1.1 rule 2), and dates/amounts are NULL.

Output: s3://openalex-ingest/awards/mitacs/mitacs_projects.parquet
"""

import argparse
import html
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). No-op on
# Linux/Databricks. See runbook §1.2.
import sys as _sys_utf8  # _sys_utf8 is sys: this block is sys.stdout.reconfigure(...) + file-I/O utf-8 defaults
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

BASE = "https://www.mitacs.ca"
LIST_URL = BASE + "/projects/page/{n}/"
REST_URL = BASE + "/wp-json/wp/v2/project"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mitacs/mitacs_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.3          # per worker
WORKERS = 3
RETRIES = 4
MAX_CONSECUTIVE_EMPTY = 3    # runbook §1: empty page != end of corpus

FIELDS = {
    "Faculty Supervisor": "supervisor",
    "Student": "student",
    "Partner": "partner",
    "Discipline": "discipline",
    "Sector": "sector",
    "University": "university",
    "Program": "program",
}

_log_lock = threading.Lock()


def log(msg: str) -> None:
    with _log_lock:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, **kw) -> requests.Response:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60, **kw)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r
            last_err = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>|</p>", "\n", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"[ \t\r\f\v]+", " ", t)
    t = re.sub(r"\s*\n\s*", "\n", t).strip()
    return t or None


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms|mme|m)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), plus a leading
    honorific strip ("Dr. Jane Doe")."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "p.eng", "peng"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def total_pages() -> int:
    """Authoritative loop terminator: X-WP-Total from REST (12 per listing page),
    cross-checked against the last page number linked from the listing."""
    r = get(REST_URL, params={"per_page": 1, "_fields": "id"})
    total = int(r.headers["X-WP-Total"])
    first = get(LIST_URL.format(n=1).replace("page/1/", "")).text
    linked = max((int(n) for n in re.findall(r"/projects/page/(\d+)/", first)), default=0)
    pages = -(-total // 12)
    log(f"REST X-WP-Total={total} -> {pages} listing pages; listing links up to page {linked}")
    return max(pages, linked)


def parse_listing(page: str) -> list[dict]:
    out = []
    for blk in re.split(r'<article class="slideout-item">', page)[1:]:
        hidden = re.search(r'class="team-slideout-hidden project-slider-hidden">(.*)', blk, re.S)
        if not hidden:
            continue
        h = hidden.group(1)
        title = re.search(r'<h3 class="title">(.*?)</h3>', h, re.S)
        link = re.search(r'<a href="(https://www\.mitacs\.ca/our-projects/[^"]+)"', h)
        content = re.search(r'project-content">\s*<h3 class="title">.*?</h3>(.*?)<a href=', h, re.S)
        rec = {
            "title": text(title.group(1)) if title else None,
            "description": text(content.group(1)) if content else None,
            "landing_page_url": link.group(1) if link else None,
        }
        for label, val in re.findall(r'<div class="h6">\s*([^<:]+):?\s*</div>\s*<p>(.*?)</p>', h, re.S):
            key = FIELDS.get(label.strip().rstrip(":"))
            if key:
                rec[key] = text(val)
        out.append(rec)
    return out


def fetch_page(n: int, cache_dir: Path | None) -> tuple[int, list[dict]]:
    cache = cache_dir / f"p{n:05d}.html" if cache_dir else None
    if cache and cache.exists():
        page = cache.read_text()
    else:
        page = get(LIST_URL.format(n=n)).text
        if cache:
            cache.write_text(page)
        time.sleep(REQUEST_DELAY)
    return n, parse_listing(page)


def rest_ids() -> dict[str, int]:
    """slug -> current WordPress post id (kept only as an audit column)."""
    out, page, pages = {}, 1, None
    while pages is None or page <= pages:
        r = get(REST_URL, params={"per_page": 100, "page": page, "_fields": "id,link"})
        pages = int(r.headers["X-WP-TotalPages"])
        for it in r.json():
            out[it["link"].rstrip("/").rsplit("/", 1)[-1]] = it["id"]
        page += 1
        time.sleep(REQUEST_DELAY)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Mitacs completed projects -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only fetch the first N listing pages")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw listing HTML (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    n_pages = total_pages()
    pages = list(range(1, n_pages + 1))
    if args.limit:
        pages = pages[: args.limit]
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    results: dict[int, list[dict]] = {}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for i, (n, recs) in enumerate(ex.map(lambda n: fetch_page(n, args.cache_dir), pages), 1):
            results[n] = recs
            if i % 100 == 0 or i == len(pages):
                el = time.time() - t0
                eta = el / i * (len(pages) - i)
                log(f"  {i}/{len(pages)} pages, {sum(map(len, results.values()))} projects, ETA {eta/60:.1f} min")

    # runbook §1: empty pages are logged, not treated as EOF; a run of empties
    # before the known last page means the source broke -> fail closed.
    empties = sorted(n for n, r in results.items() if not r)
    if empties:
        log(f"  empty listing pages: {empties[:30]}{' ...' if len(empties) > 30 else ''}")
    run = 0
    for n in pages:
        run = run + 1 if not results[n] else 0
        if run >= MAX_CONSECUTIVE_EMPTY and n < pages[-1]:
            raise SystemExit(f"{MAX_CONSECUTIVE_EMPTY}+ consecutive empty pages ending at {n}; source broken?")

    rows = [r for n in pages for r in results[n]]
    df = pd.DataFrame(rows)
    for c in FIELDS.values():
        if c not in df:
            df[c] = None
    df["slug"] = df["landing_page_url"].str.rstrip("/").str.rsplit("/", n=1).str[-1]
    before = len(df)
    df = df[df["slug"].notna() & df["title"].notna()]
    df = df.drop_duplicates(subset=["slug"], keep="first")
    log(f"Parsed {before} listing items -> {len(df)} unique projects")

    if not args.limit:
        ids = rest_ids()
        df["wp_post_id"] = df["slug"].map(ids)
        log(f"  REST ids: {len(ids)}; matched {df['wp_post_id'].notna().sum()}; "
            f"REST-only slugs {len(set(ids) - set(df['slug']))}")
    else:
        df["wp_post_id"] = None

    # supervisor -> lead investigator (Mitacs' award holder of record is the
    # academic supervisor). A few rows list several supervisors ("A, B" / "A and B"):
    # the first is lead, all are kept in supervisors_json.
    placeholders = {"tbd", "tba", "n/a", "na", "none", "-", "--", "unknown", "to be determined"}

    def people(s):
        if not s or s.strip().lower() in placeholders:
            return []
        parts = [x.strip() for x in re.split(r"\s*(?:;|,|\n| and | et | & )\s*", s)
                 if x.strip() and x.strip().lower() not in placeholders]
        return list(dict.fromkeys(parts))  # source repeats names ("A;B;B;A")
    sup = df["supervisor"].map(people)
    df["supervisor_count"] = sup.map(len)
    lead = sup.map(lambda xs: xs[0] if xs else None)
    split = lead.map(split_name)
    df["lead_name"] = lead
    df["lead_given_name"] = split.map(lambda t: t[0])
    df["lead_family_name"] = split.map(lambda t: t[1])
    df["supervisors_json"] = sup.map(
        lambda xs: json.dumps([{"name": x, "given_name": split_name(x)[0], "family_name": split_name(x)[1]} for x in xs],
                              ensure_ascii=False) if xs else None)
    stu = df["student"].map(people)
    df["students_json"] = stu.map(
        lambda xs: json.dumps([{"name": x, "given_name": split_name(x)[0], "family_name": split_name(x)[1]} for x in xs],
                              ensure_ascii=False) if xs else None)
    df["funder_award_id"] = df["slug"]
    df["currency"] = None  # Mitacs publishes no amounts

    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")
    for c in ["title", "description", "supervisor", "student", "partner", "discipline",
              "sector", "university", "program", "lead_family_name"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  multi-supervisor rows: {(df['supervisor_count'] > 1).sum()}")
    log(f"  programs: {df['program'].value_counts().head(10).to_dict()}")

    df = df.astype("string")  # runbook §1.2 item 5
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "mitacs_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_mitacs_projects.parquet"
    try:
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
