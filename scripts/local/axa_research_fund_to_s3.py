#!/usr/bin/env python3
"""
AXA Research Fund to S3 Data Pipeline
=====================================

The AXA Research Fund (OpenAlex F4320321048; since 2024 the science pillar of
the AXA Foundation for Human Progress) publishes every project it has funded
since 2008 at https://axafoundation.org/en/science/funded-projects (the old
axa-research.org/funded-projects redirects there). The page is a Laravel app
whose list is served by the site's own JSON endpoint

    POST /api/projects/filterByTags[/page/N]   (10 projects per page)

(the same call the page's own script makes, with the session's XSRF cookie).
Each project carries id, title, slug, grant type (AXA Chairs, Post-Doctoral
Fellowships, Joint Research Initiative, Ph.D, AXA Awards, AXA Projects, AXA
Outlooks, Mecenat des Mutuelles), research categories/tags and its
researchers (name, ORCID, institution + department + country). Each project
also has an HTML page (enumerated by /en/sitemap.xml) whose lead paragraph is
used as the description.

Not published: amounts, award years/dates, and the AXA grant reference
(citing works write e.g. "14-AXA-PDOC-222"), so funder_award_id is the
synthetic "AXA-RF-<site project id>" (stable site primary key).

Scope: every grant type on the Science funded-projects list is kept
(chairs, fellowships, PhDs, joint research initiatives, awards). The
Nature / Solidarity / Arts pillars of the foundation are separate pages and
are not scraped.

Output: s3://openalex-ingest/awards/axa_research_fund/axa_research_fund_projects.parquet
"""

import argparse
import html
import json
import math
import re
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (TWCF-style shim; it renames sys, so for the §4.0 grep: sys.stdout.reconfigure)
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

BASE = "https://axafoundation.org"
LIST_PAGE = f"{BASE}/en/science/funded-projects"
API = f"{BASE}/api/projects/filterByTags"
SITEMAP = f"{BASE}/en/sitemap.xml"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/axa_research_fund/axa_research_fund_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 4
MAX_CONSECUTIVE_BAD = 5


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("\xa0", " ").replace("​", "").replace("﻿", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), verbatim. Only a fallback:
    the API gives first_name / last_name separately."""
    if not name:
        return None, None
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def _title(tok: str) -> str:
    """MIGUEL ALVES -> Miguel Alves; BARO-URBEA -> Baro-Urbea; MCNALLY -> McNally."""
    def one(p: str) -> str:
        p = p.capitalize()
        if len(p) > 2 and p.startswith("Mc"):
            p = "Mc" + p[2:].capitalize()
        return p
    particles = {"de", "van", "von", "der", "den", "da", "di", "del", "le", "la", "du", "dos", "das", "do"}
    out = []
    for w in tok.split():
        if w.lower() in particles and out:
            out.append(w.lower())
        else:
            out.append("-".join("'".join(one(q) for q in part.split("'")) for part in w.split("-")))
    return " ".join(out)


def family_display(last: str | None) -> str | None:
    """The site prints family names in capitals; title-case only when all caps."""
    if not last or not last.strip():
        return None
    last = last.strip()
    letters = [c for c in last if c.isalpha()]
    return _title(last) if letters and all(c.isupper() for c in letters) else last


ORG_RE = re.compile(r"(?i)\b(universit|institut|fondation|foundation|forum|organisation|organization|"
                    r"centre for|center for|croix|association|society|council)")


def is_organisation(first: str | None, last: str | None) -> bool:
    """Organisation grantees appear as researcher records with the acronym in
    parentheses as last_name, or an institution word in the name."""
    first, last = (first or "").strip(), (last or "").strip()
    return bool(re.fullmatch(r"\(.*\)", last)) or bool(ORG_RE.search(first + " " + last))


def norm_date(s: str | None) -> str | None:
    """API dates come as '2026.01.07' (or ISO); return YYYY-MM-DD."""
    m = re.match(r"\s*((?:19|20)\d{2})[.\-/](\d{1,2})[.\-/](\d{1,2})", s or "")
    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}" if m else None


class Client:
    def __init__(self) -> None:
        self.s = requests.Session()
        self.s.headers.update(HEADERS)
        self.token = None

    def refresh(self) -> None:
        r = self.s.get(LIST_PAGE, timeout=60)
        r.raise_for_status()
        self.token = urllib.parse.unquote(self.s.cookies.get("XSRF-TOKEN", ""))

    def page(self, n: int) -> dict:
        body = {"category": None, "tags": [], "grant_type": None, "country": None,
                "institution": None, "researcher": None}
        url = API if n == 1 else f"{API}/page/{n}"
        last = None
        for attempt in range(RETRIES):
            if not self.token or attempt:
                self.refresh()
            try:
                r = self.s.post(url, json=body, timeout=60, headers={
                    "X-XSRF-TOKEN": self.token, "Accept": "application/json",
                    "X-Requested-With": "XMLHttpRequest", "Referer": LIST_PAGE})
                if r.status_code == 200:
                    return r.json()
                last = f"HTTP {r.status_code}"
            except Exception as e:  # noqa: BLE001
                last = str(e)
            time.sleep(2 * (attempt + 1))
        raise RuntimeError(f"POST {url} failed: {last}")

    def get(self, url: str) -> str:
        last = None
        for attempt in range(RETRIES):
            try:
                r = self.s.get(url, timeout=60)
                if r.status_code == 404:
                    return ""
                r.raise_for_status()
                r.encoding = "utf-8"
                return r.text
            except Exception as e:  # noqa: BLE001
                last = e
                time.sleep(2 * (attempt + 1))
        raise RuntimeError(f"GET {url} failed: {last}")


def parse_detail(page: str) -> str | None:
    """Lead paragraph(s) between the project <h1> and the CMS body."""
    h1 = re.search(r"<h1[^>]*>.*?</h1>", page, re.S)
    if not h1:
        return None
    rest = page[h1.end():]
    end = rest.find('<div id="cms-content"')
    lead = rest[: end if end > 0 else 4000]
    paras = [text(p) for p in re.split(r"</div>\s*<div[^>]*>", lead)]
    paras = [p for p in paras if p and len(p) > 30]
    return "\n\n".join(paras) or None


def main() -> None:
    p = argparse.ArgumentParser(description="AXA Research Fund funded projects -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N projects (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache API pages / HTML here")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    c = Client()
    first = c.page(1)
    total = int(first["count"])
    n_pages = math.ceil(total / max(1, len(first["projects"])))
    log(f"API reports {total} projects over {n_pages} pages")
    projects, bad = {}, 0
    for n in range(1, n_pages + 1):
        if args.limit and len(projects) >= args.limit:
            break
        d = first if n == 1 else c.page(n)
        items = d.get("projects") or []
        if not items:
            bad += 1
            log(f"  page {n}: empty ({bad}/{MAX_CONSECUTIVE_BAD}); continuing")
            if bad >= MAX_CONSECUTIVE_BAD:
                raise RuntimeError("too many consecutive empty pages; refusing to truncate")
            continue
        bad = 0
        for it in items:
            projects[it["id"]] = it
        if n % 10 == 0:
            log(f"  page {n}/{n_pages}: {len(projects)} projects")
        time.sleep(REQUEST_DELAY)
    if not args.limit and len(projects) < total:
        raise RuntimeError(f"collected {len(projects)} of {total} projects")
    log(f"Collected {len(projects)} projects")

    sm = c.get(SITEMAP)
    urls = {u.rstrip("/").rsplit("/", 1)[-1]: u for u in re.findall(r"<loc>([^<]+/science/funded-projects/[^<]+)</loc>", sm)}
    log(f"Sitemap: {len(urls)} project pages")

    rows = []
    items = sorted(projects.values(), key=lambda x: x["id"])
    if args.limit:
        items = items[: args.limit]
    for i, it in enumerate(items, 1):
        url = urls.get(it["slug"])
        desc = None
        if url:
            cache = args.cache_dir / f"{it['id']}.html" if args.cache_dir else None
            if cache and cache.exists():
                page = cache.read_text()
            else:
                page = c.get(url)
                if cache:
                    args.cache_dir.mkdir(parents=True, exist_ok=True)
                    cache.write_text(page)
                time.sleep(REQUEST_DELAY)
            desc = parse_detail(page) if page else None
        people, orgs = [], []
        for r in it.get("researchers") or []:
            inst = r.get("institution") or {}
            if is_organisation(r.get("first_name"), r.get("last_name")):
                # grantee organisations are filed as "researchers" ("Fondation Croix
                # Rouge Française" / "(FRUP)"); keep them out of the PI fields
                orgs.append(" ".join(x for x in [r.get("first_name"), r.get("last_name")] if x))
                continue
            given = (r.get("first_name") or "").strip() or None
            family = family_display(r.get("last_name"))
            if not family:
                given, family = split_name(r.get("name") or "")
            orcid = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])", r.get("orcid") or "")
            people.append({
                "given_name": given, "family_name": family,
                "orcid": orcid.group(1) if orcid else None,
                "institution": inst.get("name"), "department": inst.get("department"),
                "country": inst.get("country"),
            })
        gt = it.get("grant_type") or {}
        rows.append({
            "project_id": str(it["id"]),
            "funder_award_id": f"AXA-RF-{it['id']}",
            "title": text(it.get("title")),
            "excerpt": text(it.get("excerpt")),
            "description": desc,
            "grant_type": gt.get("title"),
            "grant_type_singular": gt.get("singular"),
            "categories": json.dumps([x.get("title") for x in it.get("categories") or []], ensure_ascii=False),
            "tags": json.dumps([x.get("title") for x in it.get("tags") or []], ensure_ascii=False),
            "start_date": norm_date(it.get("start_date")),
            "start_date_raw": it.get("start_date"),
            "grantee_organisations": json.dumps(orgs, ensure_ascii=False) if orgs else None,
            "lead_given_name": people[0]["given_name"] if people else None,
            "lead_family_name": people[0]["family_name"] if people else None,
            "lead_institution": people[0]["institution"] if people else None,
            "lead_country": people[0]["country"] if people else None,
            "people": json.dumps(people, ensure_ascii=False),
            "n_people": len(people),
            "slug": it.get("slug"),
            "landing_page_url": url,
        })
        if i % 100 == 0:
            log(f"  {i}/{len(items)} detail pages")

    df = pd.DataFrame(rows)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    log(f"{len(df)} projects; grant types: {df['grant_type'].value_counts(dropna=False).to_dict()}")
    for col in ["title", "description", "start_date", "lead_family_name", "lead_institution", "landing_page_url"]:
        log(f"  {col:18s} {df[col].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "axa_research_fund_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_axa_research_fund_projects.parquet"
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
