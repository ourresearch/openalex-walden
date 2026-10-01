#!/usr/bin/env python3
"""
Alzheimer's Drug Discovery Foundation (ADDF) to S3 Data Pipeline
================================================================

ADDF publishes its whole funding portfolio (1999-present) in the "Our Portfolio" database
(https://www.alzdiscovery.org/research-and-grants/portfolio). Ladder step 0: the page's own
"Export Results" button posts the filter form to /research-and-grants/database-export,
which returns a plain-text file of every matching project (year awarded, organisation,
detail-page URL, investigator, title, funding amount, organisation type, programme,
target, status). The database has two states, Active ("granted") and Closed, so the
export is fetched twice and the two lists are concatenated.

Each project then has a server-rendered detail page (portfolio-details/<id>) with the
project duration (start - end date), therapeutic stage / type, biomarker type, the
investigator's state + country, and a lay summary; those are parsed from the
`research-overview-list` (span.data / span.data-label pairs) and `project-summary` blocks.

ADDF publishes no grant numbers on the site (citing works quote internal numbers such as
"201809-2016862" or "20150701.01" that never appear on the site), so funder_award_id is
the synthetic "ADDF-{portfolio id}" (the detail-page id, stable across Active -> Closed).

Scope note: the portfolio covers grants AND "mission-related investments" (programme-
related investments in biotech companies); the site does not distinguish them, so all
projects are kept and the organisation type (Academic/Nonprofit vs Biotechnology/For
Profit) is carried through. Conferences/Other programme entries (workshops, meetings) are
kept as well.

alzdiscovery.org serves no robots.txt (404).

Output: s3://openalex-ingest/awards/addf/addf_projects.parquet
"""

import argparse
import html
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22; see runbook 1.2) ---
# (renamed-sys variant; the canonical call is sys.stdout.reconfigure(encoding="utf-8"))
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

BASE = "https://www.alzdiscovery.org"
EXPORT_URL = BASE + "/research-and-grants/database-export"
STATES = ["granted", "closed"]  # Active, Closed
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/addf/addf_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 0.7
RETRIES = 4
SEP = "-" * 93
MIN_EXPECTED = 750  # 118 active + 706 closed on 2026-10-01

HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, params=None) -> str:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=120)
            if r.status_code == 404:
                return ""
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            log(f"  GET {url} attempt {attempt + 1} failed: {e}")
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook 2.4.1 helper (wolf_to_s3.py): strip degree / suffix tokens,
    last remaining token = family name."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "mph", "mpp", "mba", "ms", "msc", "pharmd", "dvm", "rn", "bs", "ma", "facp"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_investigator(s: str) -> tuple[str | None, str | None, str | None]:
    """'Chien-liang Lin, PhD' / 'Ben Tiede, PhD MPP' / 'Farhad Imam' -> (given, family, degrees)."""
    s = re.sub(r"\s+", " ", (s or "")).strip()
    if not s:
        return None, None, None
    name, _, degrees = s.partition(",")
    given, family = split_name(name)
    return given, family, degrees.strip() or None


def parse_export(text: str, state: str) -> list[dict]:
    out = []
    for block in text.split(SEP)[1:]:
        lines = [l.strip() for l in block.strip("\n").split("\n")]
        u = next((i for i, l in enumerate(lines) if re.match(r"^\(https?://", l)), None)
        if u is None:
            continue
        url = lines[u].strip("()")
        pid = re.search(r"portfolio-details/(\d+)", url)
        org = lines[u - 1] if u >= 1 else None
        year = lines[u - 2] if u >= 2 and re.fullmatch(r"(19|20)\d{2}", lines[u - 2]) else None
        investigator = lines[u + 1] if u + 1 < len(lines) else ""
        k = next((i for i, l in enumerate(lines) if l.startswith("Funding Amount:")), len(lines))
        title = " ".join(l for l in lines[u + 2:k] if l) or None
        kv = {}
        for l in lines[k:]:
            key, sep, val = l.partition(":")
            if sep:
                kv[key.strip()] = val.strip() or None
        out.append({
            "portfolio_id": pid.group(1) if pid else None,
            "landing_page_url": url,
            "year_awarded": year,
            "organization": org or None,
            "investigator_raw": investigator or None,
            "title": title,
            "funding_amount_raw": kv.get("Funding Amount"),
            "organization_type": kv.get("Organization Type"),
            "program": kv.get("Program"),
            "target": kv.get("Target"),
            "status": kv.get("Status"),
            "export_state": state,
        })
    return out


def parse_detail(page: str) -> dict:
    s = BeautifulSoup(page, "html.parser")
    d = {}
    for li in s.select("ul.research-overview-list li"):
        v, lab = li.select_one("span.data"), li.select_one("span.data-label")
        if v and lab:
            d[lab.get_text(" ", strip=True)] = v.get_text(" ", strip=True) or None
    meta = s.select_one("div.research-details header p.meta")
    loc = None
    if meta:
        parts = [p.strip() for p in meta.get_text(" ", strip=True).split("|")]
        loc = parts[1] if len(parts) > 1 and parts[1] else None
    summ = s.select_one("div.project-summary")
    title = s.select_one("h2.project-title")
    dur = d.get("Project Duration") or ""
    m = re.match(r"\s*(\d{1,2}/\d{1,2}/\d{2,4})\s*\W+\s*(\d{1,2}/\d{1,2}/\d{2,4})?", dur)
    return {
        "detail_title": title.get_text(" ", strip=True) if title else None,
        "summary": re.sub(r"\s+", " ", summ.get_text(" ", strip=True)).strip() if summ else None,
        "project_duration_raw": dur or None,
        "start_date": to_iso(m.group(1)) if m else None,
        "end_date": to_iso(m.group(2)) if m and m.group(2) else None,
        "therapeutic_stage": d.get("Therapeutic Stage"),
        "therapeutic_type": d.get("Therapeutic Type"),
        "biomarker_type": d.get("Biomarker Type"),
        "location": loc,
    }


def to_iso(s: str) -> str | None:
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def parse_amount(s):
    v = re.sub(r"[^0-9.]", "", s or "")
    return float(v) if v else None


def main() -> None:
    p = argparse.ArgumentParser(description="ADDF portfolio -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="keep only the first N projects (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache detail-page HTML here")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()

    recs = []
    for st in STATES:
        txt = get(EXPORT_URL, params={"state[]": st, "per_page": "250", "keyword": ""})
        header = re.search(r"([\d,]+) Projects \|", txt)
        got = parse_export(txt, st)
        log(f"export state={st}: header says {header.group(1) if header else '?'} projects, parsed {len(got)}")
        if header and len(got) < 0.98 * int(header.group(1).replace(",", "")):
            raise SystemExit(f"export for {st} parsed {len(got)} < header {header.group(1)}")
        recs += got
    if not args.limit and len(recs) < MIN_EXPECTED:
        raise SystemExit(f"only {len(recs)} projects (expected >= {MIN_EXPECTED})")
    if args.limit:
        recs = recs[: args.limit]

    t0 = time.time()
    for i, r in enumerate(recs, 1):
        cache = args.cache_dir / f"{r['portfolio_id']}.html" if args.cache_dir else None
        if cache and cache.exists():
            page = cache.read_text()
        else:
            page = get(r["landing_page_url"])
            if cache and page:
                args.cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(page)
            time.sleep(REQUEST_DELAY)
        r.update(parse_detail(page) if page else {"detail_title": None})
        r["detail_page_ok"] = bool(page)
        if i % 50 == 0:
            el = time.time() - t0
            log(f"  {i}/{len(recs)} detail pages ({el:.0f}s, ETA {el / i * (len(recs) - i):.0f}s)")

    df = pd.DataFrame(recs)
    df["funder_award_id"] = "ADDF-" + df["portfolio_id"]
    df["title"] = df["title"].where(df["title"].notna(), df.get("detail_title"))
    inv = df["investigator_raw"].map(parse_investigator)
    df["lead_given_name"] = [x[0] for x in inv]
    df["lead_family_name"] = [x[1] for x in inv]
    df["lead_degrees"] = [x[2] for x in inv]
    df["amount"] = df["funding_amount_raw"].map(parse_amount)
    df["currency"] = df["amount"].map(lambda a: "USD" if a is not None and a == a else None)
    # detail-page location is "State, Country" or a bare US state code ("NY")
    df["country"] = df["location"].map(
        lambda s: None if not isinstance(s, str) or not s.strip() else
        ("United States" if re.fullmatch(r"[A-Z]{2}", s.split(",")[-1].strip()) else s.split(",")[-1].strip()))
    df["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if df["portfolio_id"].isna().any():
        raise SystemExit("record without a portfolio id")
    dupes = df["funder_award_id"].duplicated(keep=False)
    if dupes.any():
        log(f"  {int(dupes.sum())} rows share a portfolio id across Active/Closed; keeping the Active copy")
        df = df.drop_duplicates(subset=["funder_award_id"], keep="first")

    log(f"{len(df)} projects; detail pages ok {int(df['detail_page_ok'].sum())}")
    for c in ["title", "lead_family_name", "organization", "amount", "start_date", "end_date",
              "summary", "country", "year_awarded"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    yrs = df["year_awarded"].dropna()
    log(f"  years {yrs.min()}-{yrs.max()} ({df['year_awarded'].isna().sum()} without year), total USD {df['amount'].sum():,.0f}")
    for k, n in df["program"].value_counts().items():
        log(f"    {n:5d}  {k}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "addf_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload or args.limit:
        log("Upload skipped")
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_addf_projects.parquet"
    try:  # runbook 1.4: never shrink the corpus on re-ingest
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
