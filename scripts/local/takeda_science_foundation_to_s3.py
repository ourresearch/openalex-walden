#!/usr/bin/env python3
"""
Takeda Science Foundation (武田科学振興財団) grants to S3
=========================================================

The foundation publishes every research-grant recipient in its
"研究助成対象者データベース" (grantee database) at
https://file.takeda-sci.or.jp/bopdb/ (~8,450 grants). An empty search returns
all of them, 30 per page; each result carries the grant number (the DB's
detail.php?no=<n> key, e.g. 2024013172), research title, programme
(武田報彰医学研究助成, 生命科学研究助成, 医学系研究助成, 薬学系研究助成, ...),
institution, department, applicant name (the DB's applicant link carries the
name pre-split: l = family, f = given), fiscal year, and for older grants
keywords and a results summary. Method 5 (static HTML) on the runbook ladder;
the per-year PDF lists on takeda-sci.or.jp/research/list.php are the same data.

The database does not publish per-grant amounts (programme amounts are fixed
by scheme and only described in the call pages), so no amount is shipped.

Output: s3://openalex-ingest/awards/takeda_science_foundation/takeda_science_foundation_projects.parquet
"""

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (sys.stdout.reconfigure + utf-8 file I/O defaults, under the _sys_utf8 alias)
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
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

import argparse
import html
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests

DB = "https://file.takeda-sci.or.jp/bopdb"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/takeda_science_foundation/takeda_science_foundation_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 3
MAX_CONSECUTIVE_EMPTY = 3
MAX_CONSECUTIVE_NON200 = 5


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def tx(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>", " ", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("　", " ").replace("​", "").replace("﻿", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def parse_list(page: str) -> list[dict]:
    rows = []
    for art in re.findall(r"<article>(.*?)</article>", page, re.S):
        a = re.search(r'<h2><a href="/bopdb/detail\.php\?no=(\d+)">(.*?)</a></h2>', art, re.S)
        if not a:
            continue
        fields = {tx(k): v for k, v in re.findall(r"<dt>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>", art, re.S)}
        cls = re.search(r"submitSubSearch\('class','(\d+)'\)", fields.get("プログラム") or "")
        app = re.search(r'applicant\.php\?([^"]+)"', fields.get("申請者名") or "")
        q = parse_qs(html.unescape(app.group(1)), keep_blank_values=True) if app else {}
        year = re.search(r"(\d{4})", tx(fields.get("申請年度")) or "")
        rows.append({
            "grant_no": a.group(1),
            "title": tx(a.group(2)),
            "program": tx(fields.get("プログラム")),
            "program_code": cls.group(1) if cls else None,
            "institution": tx(fields.get("所属機関")),
            "department": tx(fields.get("所属部署")),
            "applicant_name": tx(fields.get("申請者名")),
            # the DB's own applicant link carries the name already split:
            # l = family name, f = given name, n = applicant id
            "lead_family_name": (q.get("l") or [""])[0].strip() or None,
            "lead_given_name": (q.get("f") or [""])[0].strip() or None,
            "applicant_id": (q.get("n") or [""])[0].strip() or None,
            "fiscal_year": year.group(1) if year else None,
            "keywords": tx(fields.get("キーワード")),
            "result_summary": tx(fields.get("研究結果の概要")),
            "landing_page_url": f"{DB}/detail.php?no={a.group(1)}",
        })
    return rows


def fetch(sess: requests.Session, method: str, url: str, **kw) -> requests.Response:
    last = None
    for attempt in range(RETRIES):
        try:
            r = sess.request(method, url, headers=HEADERS, timeout=60, **kw)
            r.encoding = "utf-8"
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"{method} {url} failed: {last}")


def main() -> None:
    p = argparse.ArgumentParser(description="Takeda Science Foundation grantee DB -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only crawl the first N result pages (30 grants each)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    sess = requests.Session()
    # empty search = every grantee; pagination is held in the PHP session cookie
    r = fetch(sess, "POST", f"{DB}/list.php", files={"bd_mode": (None, "search")})
    r.raise_for_status()
    m = re.search(r"検索結果:\s*[\d,]+～[\d,]+\s*/\s*([\d,]+)\s*件", r.text)
    last = re.search(r'list\.php\?page=(\d+)" aria-label="最後のページ"', r.text)
    if not m or not last:
        raise SystemExit("could not read result total / last page from the search result")
    total, total_pages = int(m.group(1).replace(",", "")), int(last.group(1))
    log(f"Search: {total} grantees over {total_pages} pages")
    if args.limit:
        total_pages = min(total_pages, args.limit)

    rows = parse_list(r.text)
    empty = non200 = 0
    page = 2
    while page <= total_pages:
        time.sleep(REQUEST_DELAY)
        r = fetch(sess, "GET", f"{DB}/list.php", params={"page": page})
        if r.status_code != 200:
            non200 += 1
            log(f"  page {page}: HTTP {r.status_code} ({non200}/{MAX_CONSECUTIVE_NON200}); continuing")
            if non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(f"{non200} consecutive non-200 responses at page {page}")
            page += 1
            continue
        non200 = 0
        got = parse_list(r.text)
        if not got:
            empty += 1
            log(f"  page {page}: 0 grants ({empty}/{MAX_CONSECUTIVE_EMPTY}); continuing")
            if empty >= MAX_CONSECUTIVE_EMPTY:
                raise RuntimeError(f"{empty} consecutive empty pages at page {page}; not truncating silently")
            page += 1
            continue
        empty = 0
        rows += got
        if page % 25 == 0:
            log(f"  page {page}/{total_pages}: {len(rows)} grants")
        page += 1

    df = pd.DataFrame(rows)
    before = len(df)
    df = df.drop_duplicates(subset=["grant_no"], keep="first")
    log(f"Parsed {before} rows, {len(df)} unique grant numbers (site reports {total})")
    if not args.limit and len(df) < total * 0.99:
        raise SystemExit(f"crawl short: {len(df)} of {total}")
    for c in ["title", "program", "institution", "lead_family_name", "lead_given_name", "fiscal_year",
              "keywords", "result_summary"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  fiscal years {df['fiscal_year'].min()}-{df['fiscal_year'].max()}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "takeda_science_foundation_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_takeda_science_foundation_projects.parquet"
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
