#!/usr/bin/env python3
"""
American Diabetes Association (ADA) to S3 Data Pipeline
=======================================================

ADA publishes its funded research grants as "Research Database" detail pages
on professional.diabetes.org (/rdb/<slug>, Drupal node type research_db). The
old searchable listing (/research-grants/research-database) now redirects to
the Research & Grants landing page, but the detail pages are live and every
one is enumerated by the site's XML sitemap. Each page has: project title, PI
name + degrees, institution, Grant Number (the form grantees cite, e.g.
11-23-PDF-04), type of grant, diabetes type, therapeutic goal, focus areas,
project start/end dates, status (active/completed), a research description and
a research-profile Q&A. Method 5 (static HTML, sitemap -> detail pages).

A second first-party list, diabetes.org/research/recipients (Pathway to Stop
Diabetes awardees 2014-2020: name, institution, title, Grant #), adds the
Pathway grants not in the research database.

ADA does not publish per-grant amounts; amount/currency are NULL.

Output: s3://openalex-ingest/awards/ada/ada_projects.parquet
"""

import argparse
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
import sys
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
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---

BASE = "https://professional.diabetes.org"
SITEMAP = f"{BASE}/sitemap.xml"
PATHWAY_URL = "https://diabetes.org/research/recipients"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ada/ada_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 3
MAX_CONSECUTIVE_NON200 = 5

# ADA grant number: "{n}-{YY}-{CODE}-{seq}", e.g. 11-23-PDF-04, 1-18-INI-14
GRANT_NO_RE = re.compile(r"^(\d{1,2})-(\d{2})-([A-Z]+)-(\d+)$")
PATHWAY_CODES = {"INI": "Pathway Initiator Award", "ACE": "Pathway Accelerator Award",
                 "ACN": "Pathway Accelerator Award", "VSN": "Pathway Visionary Award"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> tuple[int, str, str]:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            log(f"GET {url} -> {r.status_code} ({len(r.content)} bytes)")
            r.encoding = "utf-8"
            return r.status_code, r.text, r.url
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (attempt + 1))
    log(f"GET {url} failed: {last}")
    return 0, "", url


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>", " ", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


SUFFIXES = {"phd", "md", "dphil", "dsc", "scd", "jr", "sr", "ii", "iii", "iv",
            "mph", "msc", "mbbs", "mbchb", "dvm", "pharmd", "facp", "frcp", "frcpc",
            "dpt", "mba", "bsc", "mres", "mphil", "rd", "rdn", "cdces", "cde", "faan", "mhs",
            "mscr", "mpp", "dnp", "drph", "ma", "ms", "rn", "psyd", "edd"}


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Canonical runbook section 2.4.1 helper (wolf_to_s3.py): strip trailing
    degree/suffix tokens, last token = family, rest = given. ADA writes
    degrees after a comma ("Masa Josipovic, MD, PhD"), so everything after
    the first comma is dropped first; that makes short degree tokens like
    "MS"/"MA" safe to strip too."""
    if not name:
        return None, None
    name = re.sub(r"^(?:(?:dr|prof|professor)\.?\s+)+", "", name.strip(), flags=re.I)
    name = name.split(",")[0]
    tokens = name.split()
    while len(tokens) > 1 and tokens[-1].lower().replace(".", "") in SUFFIXES:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def rdb_urls() -> list[str]:
    status, xml, _ = get(SITEMAP)
    if status != 200:
        raise RuntimeError(f"sitemap HTTP {status}")
    return sorted({u for u in re.findall(r"<loc>([^<]+/rdb/[^<]+)</loc>", xml)})


def rdb_field(page: str, cls: str) -> str | None:
    m = re.search(rf'<div class="{cls}">\s*<div class="profile-label">.*?</div>\s*<div class="profile-[a-z]+">(.*?)</div>\s*</div>',
                  page, re.S)
    return text(m.group(1)) if m else None


def parse_rdb(url: str, page: str) -> dict | None:
    if "node--type-research-db" not in page:
        return None
    title = re.search(r'research-hero__preheadline">Research Database</div>(.*?)</div>', page, re.S)
    name = re.search(r'<div class="investor-name">(.*?)</div>', page, re.S)
    dates = re.findall(r'<time datetime="(\d{4}-\d{2}-\d{2})T', page)
    status = re.search(r'<div class="profile-status">.*?<div class="([a-z]+)">', page, re.S)
    desc = re.search(r'<div class="research-des">\s*<h2>Research Description</h2>(.*?)</div>', page, re.S)
    profile = re.search(r'<div class="db-des">\s*<h2>Research Profile</h2>(.*?)</div>', page, re.S)
    # The visible name block drops the surname for some PIs ("Angela  G, PhD");
    # the page's Drupal datalayer carries the full taxonomy term
    # ("investigator":{"556":"Angela G Brega"}). Prefer it; keep both.
    display_person = text(name.group(1)) if name else None
    tax = re.search(r'"entityTaxonomy":\{.*?"investigator":\{"\d+":"((?:[^"\\]|\\.)*)"', page, re.S)
    person = json.loads(f'"{tax.group(1)}"').strip() if tax else display_person
    given, family = split_name(person)
    node_id = re.search(r'data-history-node-id="(\d+)"', page)
    return {
        "source": "research_database",
        "grant_number": rdb_field(page, "grant-num"),
        "title": text(title.group(1)) if title else None,
        "lead_name": person,
        "lead_name_display": display_person,
        "lead_given_name": given,
        "lead_family_name": family,
        "node_id": node_id.group(1) if node_id else None,
        "institution": rdb_field(page, "instistue-category"),
        "grant_type": rdb_field(page, "grant-type"),
        "diabetes_type": rdb_field(page, "diabetes-type"),
        "therapeutic_goal": rdb_field(page, "therapeutic-goal"),
        "focus": rdb_field(page, "focus-category"),
        "start_date": dates[0] if dates else None,
        "end_date": dates[1] if len(dates) > 1 else None,
        "project_status": status.group(1) if status else None,
        "description": text(desc.group(1)) if desc else None,
        "research_profile": text(profile.group(1)) if profile else None,
        "landing_page_url": url,
    }


def parse_pathway(page: str) -> list[dict]:
    """diabetes.org/research/recipients: accordion per cohort; each awardee is
    <h3>Name, PhD [links]</h3><p>Institution<br><em>Title</em><br>Grant #1-18-INI-14</p><p>abstract</p>"""
    out = []
    for block in re.split(r"<h3>", page)[1:]:
        g = re.search(r"Grant\s*#\s*([0-9A-Z-]+)", block)
        if not g:
            continue
        head = block.split("</h3>", 1)[0]
        name = text(re.split(r"<br|<a ", head)[0])
        after = block.split("</h3>", 1)[1] if "</h3>" in block else ""
        p1 = re.search(r"<p>(.*?)</p>", after, re.S)
        inst = title = None
        if p1:
            parts = re.split(r"<br\s*/?>", p1.group(1))
            inst = text(parts[0])
            em = re.search(r"<em>(.*?)</em>", p1.group(1), re.S)
            title = text(em.group(1)) if em else None
        p2 = re.findall(r"<p>(.*?)</p>", after, re.S)
        desc = text(p2[1]) if len(p2) > 1 and "Grant #" not in p2[1] else None
        gn = g.group(1).strip("-")
        m = GRANT_NO_RE.match(gn)
        given, family = split_name(name)
        out.append({
            "source": "pathway_recipients",
            "grant_number": gn,
            "title": title,
            "lead_name": name,
            "lead_given_name": given,
            "lead_family_name": family,
            "institution": inst,
            "grant_type": PATHWAY_CODES.get(m.group(3)) if m else None,
            "start_date": None,
            "end_date": None,
            "description": desc,
            "landing_page_url": PATHWAY_URL,
        })
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="ADA research database + Pathway recipients -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N research-database pages (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None)
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the section 1.4 shrink guard")
    args = p.parse_args()

    urls = rdb_urls()
    log(f"Sitemap: {len(urls)} research-database (/rdb/) pages")
    if args.limit:
        urls = urls[: args.limit]
    rows, skipped, non200 = [], [], 0
    t0 = time.time()
    for i, url in enumerate(urls, 1):
        cache = args.cache_dir / (url.rstrip("/").rsplit("/", 1)[-1][:80] + ".html") if args.cache_dir else None
        if cache and cache.exists():
            status, page = 200, cache.read_text()
        else:
            status, page, final = get(url)
            time.sleep(REQUEST_DELAY)
            if status == 200 and "/rdb/" not in final:
                log(f"  redirected away: {url} -> {final}")
                skipped.append(url)
                continue
        if status != 200:
            non200 += 1
            skipped.append(url)
            if non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError("too many consecutive non-200 detail pages; refusing to truncate")
            continue
        non200 = 0
        if cache:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache.write_text(page)
        rec = parse_rdb(url, page)
        if rec is None or not rec["grant_number"]:
            skipped.append(url)
        else:
            rows.append(rec)
        if i % 25 == 0:
            log(f"[{time.time() - t0:5.0f}s] {i}/{len(urls)} pages, {len(rows)} grants")
    log(f"Research database: {len(rows)} grants, {len(skipped)} pages skipped")
    for u in skipped[:20]:
        log(f"  skipped: {u}")

    status, page, _ = get(PATHWAY_URL)
    if status != 200:
        raise RuntimeError(f"Pathway recipients page HTTP {status}")
    pathway = parse_pathway(page)
    log(f"Pathway recipients page: {len(pathway)} grants")
    have = {r["grant_number"].upper() for r in rows}
    extra = [r for r in pathway if r["grant_number"].upper() not in have]
    log(f"  {len(pathway) - len(extra)} already in the research database, {len(extra)} added")
    rows += [] if args.limit else extra

    df = pd.DataFrame(rows)
    # funder_award_id = ADA's printed grant number (the form grantees cite)
    df["funder_award_id"] = df["grant_number"].str.strip().str.upper()
    parsed = df["funder_award_id"].str.extract(GRANT_NO_RE)
    df["grant_year"] = ("20" + parsed[1]).where(parsed[1].notna())
    df["grant_code"] = parsed[2]
    bad = df["funder_award_id"][~df["funder_award_id"].str.match(GRANT_NO_RE)]
    for b in bad:
        log(f"  WARNING: non-standard grant number {b!r}")
    before = len(df)
    df = df.drop_duplicates(subset=["funder_award_id", "title", "lead_name"], keep="first")
    if before != len(df):
        log(f"  {before - len(df)} exact duplicate rows dropped")
    dupes = df["funder_award_id"].duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    for c in ["title", "start_date", "end_date", "lead_family_name", "institution", "grant_type", "description"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "ada_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_ada_projects.parquet"
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
