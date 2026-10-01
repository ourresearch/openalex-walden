#!/usr/bin/env python3
"""
Irish Research Council (IRC) awardees to S3 Data Pipeline
=========================================================

The IRC's public "Search Awardees Database" (https://research.ie/awardees/)
loads its results from a plain server-rendered endpoint:

    https://research.ie/awardees_search/search_new.php?search_year=YYYY&...

which returns one HTML table (<table id="awardees_results">) with one row per
awardee:

    Year | Name | Award Category | Award | Higher Education Institution |
    Partner (where applicable) | Discipline | Project Title

We query it one year at a time (1999..current; the unfiltered query is too
slow to finish) and keep every row. No bulk file exists for the IRC-era
database: data.gov.ie only has Research Ireland's post-merger "Grant
Commitments" CSV (Aug 2024+, funder Research Ireland) and the old SFI file
(ladder item 0 checked 2026-09-30). The database publishes no grant numbers
and no amounts, so funder_award_id is synthetic (see below).

The raw parquet keeps ALL years. Which rows ship as Irish Research Council
awards is decided in the notebook (IRC era = award years 2012-2024; 1999-2011
are its predecessors IRCSET / IRCHSS, 2025+ are Research Ireland calls).

funder_award_id: "IRC-{year}-{award-slug}-{name-slug}" (e.g.
IRC-2019-postgraduate-scholarship-programme-aaron-barron). The database has
no award number; the GOIPG/2019/2511-style references grantees cite are not
published anywhere for IRC-era awards. Exact duplicate rows are dropped;
distinct rows that would share a key get a "-2", "-3" suffix in a
deterministic (title-sorted) order, and the script raises if any key is
still duplicated.

Method 5 (static HTML) on the runbook ladder.

Output: s3://openalex-ingest/awards/irish_research_council/irish_research_council_projects.parquet
"""

import argparse
import html
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). See runbook §1.2.
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

ENDPOINT = "https://research.ie/awardees_search/search_new.php"
SLUG = "irish_research_council"
S3_BUCKET = "openalex-ingest"
S3_KEY = f"awards/{SLUG}/{SLUG}_projects.parquet"
FIRST_YEAR = 1999

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org; awards ingest)",
    "Referer": "https://research.ie/awardees/",
}
REQUEST_DELAY = 1.0
RETRIES = 4
COLUMNS = ["year", "name", "award_category", "award", "institution", "partner", "discipline", "title"]


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get_year(year: int, cache: Path | None) -> str:
    if cache is not None and cache.exists():
        return cache.read_text()
    params = {"searchterm": "", "search_year": str(year), "search_award_cat": "", "search_award_type": "",
              "search_discipline": "", "search_institution": ""}
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(ENDPOINT, params=params, headers=HEADERS, timeout=180)
            log(f"  GET year={year} -> HTTP {r.status_code}, {len(r.content):,} bytes")
            r.raise_for_status()
            r.encoding = "utf-8"
            if 'id="searchtoload"' not in r.text:
                raise RuntimeError("response is not the awardees search page")
            if cache is not None:
                cache.write_text(r.text)
            time.sleep(REQUEST_DELAY)
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            log(f"  retry {attempt + 1}/{RETRIES} year={year}: {e}")
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"year {year} failed: {last_err}")


def cell(fragment: str) -> str | None:
    t = html.unescape(re.sub(r"<[^>]+>", " ", fragment)).replace("﻿", "").replace("​", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def parse(page: str) -> list[dict]:
    table = re.search(r'<table id="awardees_results">(.*?)</table>', page, re.S)
    if not table:
        return []
    rows = []
    for tr in re.findall(r"<tr>(.*?)</tr>", table.group(1), re.S):
        tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
        if len(tds) != len(COLUMNS):
            continue
        rows.append(dict(zip(COLUMNS, (cell(td) for td in tds))))
    return rows


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|sir|dame|mr|mrs|ms|miss)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) with a leading-honorific strip."""
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


def slug(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-") or "na"


def main() -> None:
    p = argparse.ArgumentParser(description="Irish Research Council awardees database -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="keep only the first N rows (smoke test)")
    p.add_argument("--years", type=str, default=None, help="e.g. 2019 or 2012-2024 (default: all)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    last = datetime.now().year
    if args.years:
        a, _, b = args.years.partition("-")
        years = list(range(int(a), int(b or a) + 1))
    else:
        years = list(range(FIRST_YEAR, last + 1))
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for y in years:
        page = get_year(y, args.cache_dir / f"year_{y}.html" if args.cache_dir else None)
        got = parse(page)
        bad = [r for r in got if r["year"] != str(y)]
        if bad:
            raise SystemExit(f"year {y}: {len(bad)} rows carry another year, e.g. {bad[0]}")
        log(f"year {y}: {len(got)} awardees")
        rows += got
        if args.limit and len(rows) >= args.limit:
            rows = rows[: args.limit]
            break

    df = pd.DataFrame(rows, columns=COLUMNS)
    before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    log(f"{before} rows, {before - len(df)} exact duplicates dropped")

    names = df["name"].map(split_name)
    df["given_name"] = [n[0] for n in names]
    df["family_name"] = [n[1] for n in names]
    base = "IRC-" + df["year"].fillna("na") + "-" + df["award"].map(slug) + "-" + df["name"].map(slug)
    df["_base"] = base
    df = df.sort_values(["_base", "title", "institution", "discipline"], na_position="first").reset_index(drop=True)
    n = df.groupby("_base").cumcount()
    df["funder_award_id"] = [b if k == 0 else f"{b}-{k + 1}" for b, k in zip(df["_base"], n)]
    df = df.drop(columns="_base")
    log(f"  {int((n > 0).sum())} same-person/award/year rows got a -N suffix")
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")

    df["source_url"] = ENDPOINT + "?search_year=" + df["year"].fillna("")
    for c in COLUMNS + ["given_name", "family_name"]:
        log(f"  {c:16s} {df[c].notna().mean():6.1%}")
    log("  by year: " + ", ".join(f"{k}:{v}" for k, v in df["year"].value_counts().sort_index().items()))

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / f"{SLUG}_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / f"_previous_{SLUG}_projects.parquet"
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
