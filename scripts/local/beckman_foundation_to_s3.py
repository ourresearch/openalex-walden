#!/usr/bin/env python3
"""
Arnold and Mabel Beckman Foundation to S3 Data Pipeline
=======================================================

The Beckman Foundation's "Awarded Scientists" directory
(https://www.beckman-foundation.org/awarded-scientists/, Wagtail CMS) has one
server-rendered profile per awardee at /people/<slug>/, all enumerated in
https://www.beckman-foundation.org/sitemap.xml. Each profile lists one
`award-item` block per Beckman award the person received:

    Research Title, Beckman Program, Award Year, Institution at Time of Award,
    Faculty Mentor (Scholars / Postdoctoral Fellows)

plus the person's name + degrees, ORCID (when given), discipline and current
institution. One row = one award (a person can hold several, e.g. Beckman
Scholar 2009 then Beckman Postdoctoral Fellow 2016). Profiles without award
blocks (staff, board) are skipped.

Programs (all research funding, so all kept): Beckman Young Investigators,
Beckman Scholars (undergraduate research training), Arnold O. Beckman
Postdoctoral Fellows, Beckman-Argyros Award in Vision Research, Beckman Research
Technology Initiative, Beckman Speaker and Conference Support Fund, and the
institutional instrumentation/center grants (CryoEM, Light-Sheet Microscopy,
FIB-Milling for Cellular CryoET, Mass Spec for Atmospheric Monitoring).

No award amounts or grant numbers are published per award. Citing works write
free text ("BYI 2017", "2021BYI"), so funder_award_id is a stable synthetic
key BECKMAN-{program-slug}-{year}-{person-slug}.

Method 5 (static HTML, sitemap -> profile pages).

Output: s3://openalex-ingest/awards/beckman_foundation/beckman_foundation_projects.parquet
"""

import argparse
import html
import json
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O. See runbook §1.2. (grep anchor: sys.stdout.reconfigure)
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

BASE = "https://www.beckman-foundation.org"
SITEMAP = f"{BASE}/sitemap.xml"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/beckman_foundation/beckman_foundation_projects.parquet"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 0.3
RETRIES = 4


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> requests.Response:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}")
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last}")


def people_urls() -> list[str]:
    r = get(SITEMAP)
    r.raise_for_status()
    locs = re.findall(r"<loc>([^<]+)</loc>", r.text)
    urls = sorted({l.strip().replace("http://", "https://") for l in locs
                   if re.match(r"https?://www\.beckman-foundation\.org/people/[^/]+/$", l.strip())})
    if len(urls) < 1000:
        raise RuntimeError(f"sitemap lists only {len(urls)} people pages; expected ~2,500")
    return urls


def clean(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


DEGREE_TOKENS = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                 # profiles print degree strings after the name ("Dr. Abigail Knight, PhD", "Mr. Nile Abularrage, BS")
                 "bs", "ba", "bsc", "ms", "msc", "ma", "mph", "mba", "mfa", "od", "dvm", "pharmd", "mbbs",
                 "frs", "facs", "mpp", "meng", "beng", "bse", "edd", "jd", "mph.", "rn", "mpa", "bfa"}
HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms|miss|mx)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), adapted to this site's
    "First Last, DEGREES" style: everything after the first comma is degrees /
    suffixes ("Jason Miller, MD, PhD", "Ganesh Babulal, MSCI, OTD, PhD",
    "Emanuel F. Petricoin, III, PhD") and is dropped; a leading honorific is
    stripped ("Dr.", "Prof."); then trailing suffix tokens are stripped as in
    wolf_to_s3.py, never the last remaining token, and never surname-like
    abbreviations ("Eric Ma", "Tao Do") - only the canonical set plus degree
    tokens that cannot be surnames ("Devraj Basu MD", "A James Hudspeth MD/PhD")."""
    if not name:
        return None, None
    base = name.partition(",")[0]
    tokens = HONORIFIC_RE.sub("", base.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    suffixes |= {"mph", "msc", "pharmd", "dds", "dvm", "mbbs", "mbchb", "facs", "frcpc", "frcp", "mmed",
                 "dr", "med", "dipl", "frs", "fmedsci"}

    def is_suffix(tok: str) -> bool:
        parts = [x.strip(".") for x in tok.lower().strip(",.()").split("/")]
        return all(x in suffixes for x in parts if x) and any(parts)

    while len(tokens) > 1 and is_suffix(tokens[-1]):
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


AWARD_RE = re.compile(r'<div class="award-item">(.*?)(?=<div class="award-item">|<div class="honors"|</section>)', re.S)


def meta(block: str, cls: str) -> str | None:
    m = re.search(rf'<div class="award-item__meta award-item__{cls}">(.*?)</div>', block, re.S)
    if not m:
        return None
    return clean(re.sub(r'<span class="award-item__label">.*?</span>', "", m.group(1), flags=re.S))


def parse_person(url: str, page: str) -> list[dict]:
    name = re.search(r'<h1 class="person-overview__heading">(.*?)</h1>', page, re.S)
    if not name:
        return []
    name = clean(name.group(1))
    orcid = re.search(r'orcid\.org/(\d{4}-\d{4}-\d{4}-\d{3}[\dX])', page)
    disc = re.search(r'#svg__discipline".*?<div class="icon-link-text__text">(.*?)</div>', page, re.S)
    summary = re.search(r'<h2 class="sf-heading">Research Summary</h2>\s*<div class="rich-text">(.*?)</div>', page, re.S)
    given, family = split_name(name)
    out = []
    for block in AWARD_RE.findall(page):
        title = re.search(r'<h2 class="award-item__heading">(.*?)</h2>', block, re.S)
        prog = re.search(r'<a href="(/programs/[^"]*)"[^>]*class="award-item__program-link">(.*?)</a>', block, re.S)
        prog_txt = clean(prog.group(2)) if prog else meta(block, "program")
        year = meta(block, "date")
        title = clean(title.group(1)) if title else None
        if title and re.match(r"(?i)to be filled|tbd$|n/?a$", title):
            title = None  # CMS placeholder ("To be filled in by Scholar Summary")
        out.append({
            "person_slug": slug(unquote(url.rstrip("/").rsplit("/", 1)[-1])),  # nicolas-p%C3%A9gard -> nicolas-pegard
            "name": name,
            "given_name": given,
            "family_name": family,
            "orcid": orcid.group(1) if orcid else None,
            "discipline": clean(disc.group(1)) if disc else None,
            "research_summary": clean(summary.group(1)) if summary else None,
            "research_title": title,
            "program": prog_txt,
            "program_url": (BASE + prog.group(1)) if prog else None,
            "award_year": year if year and re.fullmatch(r"(19|20)\d{2}", year) else None,
            "award_year_text": year,
            "institution_at_award": meta(block, "location"),
            "faculty_mentor": meta(block, "mentor"),
            "landing_page_url": url,
        })
    return out


def program_key(prog_url: str | None, prog_txt: str | None) -> str:
    if prog_url:
        return prog_url.rstrip("/").rsplit("/", 1)[-1]
    return slug(prog_txt or "unknown-program")


def main() -> None:
    p = argparse.ArgumentParser(description="Beckman Foundation awarded scientists -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    urls = people_urls()
    log(f"Sitemap: {len(urls)} /people/ profiles")
    if args.limit:
        urls = urls[: args.limit]

    rows, no_awards, failed, t0 = [], [], [], time.time()
    for i, url in enumerate(urls, 1):
        cache = None
        if args.cache_dir:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache = args.cache_dir / (url.rstrip("/").rsplit("/", 1)[-1][:100] + ".html")
        if cache and cache.exists():
            page = cache.read_text()
        else:
            r = get(url)
            if r.status_code != 200:
                failed.append(f"{r.status_code} {url}")
                page = ""
            else:
                page = r.text
                if cache:
                    cache.write_text(page)
            time.sleep(REQUEST_DELAY)
        got = parse_person(url, page) if page else []
        if page and not got:
            no_awards.append(url)
        rows += got
        if i % 200 == 0:
            el = time.time() - t0
            log(f"  {i}/{len(urls)} profiles, {len(rows)} awards, ETA {el / i * (len(urls) - i) / 60:.1f} min")
    log(f"Parsed {len(rows)} awards from {len(urls)} profiles; {len(no_awards)} profiles without awards; "
        f"{len(failed)} fetch failures")
    for f in failed[:20]:
        log(f"  failed: {f}")
    if len(failed) > 0.02 * len(urls):
        raise SystemExit("more than 2% of profile fetches failed; not writing a partial corpus")

    df = pd.DataFrame(rows)
    df["program_key"] = [program_key(u, t) for u, t in zip(df["program_url"], df["program"])]
    df["funder_award_id"] = ("BECKMAN-" + df["program_key"] + "-" + df["award_year"].fillna("NA")
                             + "-" + df["person_slug"])
    dup = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dup.any():
        # the same person listed twice for one program+year (two projects): number them
        df.loc[dup, "funder_award_id"] = df.loc[dup, "funder_award_id"] + "-" + \
            (df[dup].groupby("funder_award_id").cumcount() + 1).astype(str)
        log(f"  {dup.sum()} rows shared a program+year+person key; numbered")
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate funder_award_id")

    for c in ["research_title", "program", "award_year", "institution_at_award", "faculty_mentor",
              "orcid", "discipline", "research_summary"]:
        log(f"  {c:22s} {df[c].notna().mean():6.1%}")
    log(f"  programs: {df['program'].value_counts(dropna=False).to_dict()}")
    log(f"  years: {df['award_year'].min()}-{df['award_year'].max()}; "
        f"non-year award_year_text: {df.loc[df['award_year'].isna(), 'award_year_text'].value_counts(dropna=False).head(10).to_dict()}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "beckman_foundation_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_beckman_foundation_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound", "403"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
