#!/usr/bin/env python3
"""
BrightFocus Foundation to S3 Data Pipeline
==========================================

BrightFocus Foundation (formerly the American Health Assistance Foundation,
renamed 2013) funds three research programs: Alzheimer's Disease Research,
Macular Degeneration Research and National Glaucoma Research. Its public
"Grant Search" at https://www.brightfocus.org/explore-research/ is backed by a
WordPress custom post type `grant`, which the site exposes through the public
WP REST API (/wp-json/wp/v2/grant, X-WP-Total ~1,535, 1994-2026). The REST
records carry only id/title/link (ACF fields are not exposed), so each grant's
server-rendered page is fetched for the details block:

    Program, Award Type, Award Amount (USD), Active Dates, Grant ID,
    Acknowledgement (named/co-funded award), Mentor(s), Goals, Summary,
    plus the Principal Investigator card (name + degrees, institution,
    city/state/country) and the topic chips.

Method 2 (WP REST) for enumeration + method 5 (static HTML) for details.

`Grant ID` (e.g. A2021036S, G2016023, M2026014N) is the citable reference:
it is the form BrightFocus grantees put in acknowledgements (crossref_work.grants
/ crossref_work_funders / europepmc_work_funders shells for F4320308990 and for
the AHAF twin F4320306229), so it is shipped as funder_award_id. BrightFocus does
not register grant DOIs with Crossref (0 type:grant deposits for 10.13039/100006312
or 10.13039/100000967 on 2026-10-01).

Output: s3://openalex-ingest/awards/brightfocus/brightfocus_projects.parquet
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

BASE = "https://www.brightfocus.org"
REST = f"{BASE}/wp-json/wp/v2/grant"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/brightfocus/brightfocus_projects.parquet"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 0.4
RETRIES = 4
MAX_CONSECUTIVE_NON200 = 5

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], 1)}
MONTHS.update({m[:3]: i for m, i in list(MONTHS.items())})

COUNTRY_ALIASES = {"usa": "United States", "us": "United States", "u.s.a.": "United States",
                   "u.s.": "United States", "united states of america": "United States",
                   "uk": "United Kingdom", "england": "United Kingdom", "scotland": "United Kingdom"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, params: dict | None = None) -> requests.Response | None:
    """None = permanent redirect loop (the site has a few grant posts whose slug
    and slug-2 301 to each other; those pages cannot be rendered by anyone)."""
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=90)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}")
            return r
        except requests.TooManyRedirects:
            return None
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last_err}")


def list_grants() -> list[dict]:
    """All `grant` posts from WP REST. X-WP-TotalPages is the terminator
    (runbook §1: never stop on the first empty page)."""
    out, page, total_pages, non200 = [], 1, None, 0
    while total_pages is None or page <= total_pages:
        r = get(REST, {"per_page": 100, "page": page,
                       "_fields": "id,link,slug,title,date,modified,categories"})
        if r is None or r.status_code != 200:
            non200 += 1
            log(f"  REST page {page}: HTTP {getattr(r, 'status_code', 'redirect loop')} ({non200}/{MAX_CONSECUTIVE_NON200}); retrying")
            if non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(f"REST page {page} kept failing")
            time.sleep(5)
            continue
        non200 = 0
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        total = int(r.headers.get("X-WP-Total", 0))
        out += r.json()
        log(f"  REST page {page}/{total_pages}: {len(out)}/{total}")
        page += 1
    if len(out) < total:
        raise RuntimeError(f"REST returned {len(out)} of {total} grants")
    return out


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>|</p>|</li>", "\n", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = "\n".join(re.sub(r"[ \t]+", " ", ln).strip() for ln in t.split("\n"))
    t = re.sub(r"\n{2,}", "\n", t).strip()
    return t or None


def one_line(fragment: str | None) -> str | None:
    t = text(fragment)
    return re.sub(r"\s+", " ", t).strip() if t else None


def parse_date(s: str | None) -> str | None:
    # "July 01, 2026"
    if not s:
        return None
    m = re.search(r"([A-Za-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})", s)
    if not m or m.group(1).lower() not in MONTHS:
        return None
    return f"{m.group(3)}-{MONTHS[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)
DEGREE_TOKENS = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                 # BrightFocus prints full degree strings after the name ("Masayuki Hata, MD, PhD")
                 "od", "dvm", "vmd", "mph", "ms", "msc", "ma", "mba", "bs", "bsc", "ba", "pharmd",
                 "rn", "facs", "frcs", "frcsc", "frcophth", "faao", "mbbs", "mbchb", "frcp", "mrcp",
                 "dds", "dmd", "mpharm", "psyd", "edd", "drph", "msph", "mhs", "mas", "facp", "faan"}


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


# a few "C..." contract grants name an organisation, not a person, as PI
ORG_RE = re.compile(r"\b(Inc|LLC|Ltd|Corporation|Corp|Society|Bionetworks|Company|GmbH)\b\.?", re.I)
# scope rule (oxjob #1451): exclude only money that is clearly not research
# funding. A patient support-group grant is the one such case on the site.
NOT_RESEARCH_RE = re.compile(r"\bsupport group\b", re.I)


def strip_degrees(name: str | None) -> str | None:
    if not name:
        return None
    g, f = split_name(name)
    return " ".join(x for x in (g, f) if x) or None


def norm_country(loc: str | None) -> str | None:
    if not loc:
        return None
    c = loc.split(",")[-1].strip().rstrip(".")
    if not c:
        return None
    return COUNTRY_ALIASES.get(c.lower(), c)


FIELD_RE = re.compile(
    r'<h4 class="font-bold[^"]*">\s*(.*?)\s*</h4>(.*?)(?=<div class="mb-6">|<div class="definitions-content|$)', re.S)
SECTION_RE = re.compile(
    r'<h3 class="font-bold text-2xl mb-2[^"]*">\s*(.*?)\s*</h3>\s*<div class="mb-12[^"]*">(.*?)</div>', re.S)


def parse_people(card: str) -> list[dict]:
    """PI card: one or more people. Each person = linked or plain name, then
    institution and location paragraphs."""
    people = []
    # split on each name anchor / name paragraph that follows the h2
    chunks = re.split(r'(?=<a class="group" href="https://www\.brightfocus\.org/grantee/)', card)
    for ch in chunks:
        if "/grantee/" not in ch:
            continue
        url = re.search(r'href="(https://www\.brightfocus\.org/grantee/[^"]+)"', ch)
        ps = [one_line(p) for p in re.findall(r"<p[^>]*>(.*?)</p>", ch, re.S)]
        ps = [p for p in ps if p]
        if not ps:
            continue
        people.append({"name": ps[0], "institution": ps[1] if len(ps) > 1 else None,
                       "location": ps[2] if len(ps) > 2 else None,
                       "grantee_url": url.group(1) if url else None})
    if not people:
        # unlinked PI (no grantee profile)
        ps = [one_line(p) for p in re.findall(r"<p[^>]*>(.*?)</p>", card, re.S)]
        ps = [p for p in ps if p]
        if ps:
            people.append({"name": ps[0], "institution": ps[1] if len(ps) > 1 else None,
                           "location": ps[2] if len(ps) > 2 else None, "grantee_url": None})
    for p in people:
        if ORG_RE.search(p["name"] or ""):
            p["given_name"], p["family_name"], p["is_org"] = None, None, True
        else:
            p["given_name"], p["family_name"] = split_name(p["name"])
            p["is_org"] = False
        p["country"] = norm_country(p["location"])
    return people


def parse_grant(item: dict, page: str) -> dict | None:
    main = page[page.find("<main"):]
    end = main.find("Related Grants")
    if end > 0:
        main = main[:end]
    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", main, re.S)
    header = re.search(r'<span class="font-bold text-base lg:text-2xl">(.*?)</span>', main, re.S)
    chips = [one_line(c) for c in re.findall(r'<div class="bg-gray text-indigo-dark[^"]*">(.*?)</div>', main, re.S)]
    updated = re.search(r"Updated On:\s*([^<]+)</span>", main)
    card = re.search(r'<h2 class="font-bold text-2xl">\s*(Principal Investigator[^<]*)</h2>(.*?)'
                     r'(?=<div class="grid grid-cols-12)', main, re.S)
    people = parse_people(card.group(2)) if card else []
    fields: dict[str, str] = {}
    for label, body in FIELD_RE.findall(main):
        fields[one_line(label) or ""] = body
    sections = {one_line(k): text(v) for k, v in SECTION_RE.findall(main)}
    grant_id = one_line(fields.get("Grant ID"))
    amount_txt = one_line(fields.get("Award Amount"))
    digits = re.sub(r"[^\d.]", "", amount_txt or "")
    amount = float(digits) if digits and re.search(r"\d", digits) else None
    dates = one_line(fields.get("Active Dates")) or ""
    dparts = [d.strip() for d in re.split(r"\s+-\s+|\s+–\s+", dates) if d.strip()]
    mentors = [one_line(m) for m in re.findall(r"<p[^>]*>(.*?)</p>", fields.get("Mentor(s)") or "", re.S)]
    mentors = [m for m in mentors if m]
    lead = people[0] if people else {}
    return {
        "wp_id": str(item["id"]),
        "slug": item.get("slug"),
        "grant_id": grant_id,
        "title": one_line(item["title"]["rendered"]),
        "lay_title": one_line(h1.group(1)) if h1 else None,
        "program_header": one_line(header.group(1)) if header else None,
        "program": one_line(fields.get("Program")),
        "award_type": one_line(fields.get("Award Type")),
        "amount_text": amount_txt,
        "amount": amount,
        "currency": "USD" if amount is not None else None,
        "active_dates": dates or None,
        "start_date": parse_date(dparts[0]) if dparts else None,
        "end_date": parse_date(dparts[1]) if len(dparts) > 1 else None,
        "acknowledgement": one_line(fields.get("Acknowledgement")),
        "mentors": json.dumps(mentors, ensure_ascii=False) if mentors else None,
        "topics": json.dumps([c for c in chips if c], ensure_ascii=False) if chips else None,
        "pi_role_label": one_line(card.group(1)) if card else None,
        "lead_name": lead.get("name"),
        "lead_given_name": lead.get("given_name"),
        "lead_family_name": lead.get("family_name"),
        "lead_institution": lead.get("institution"),
        "lead_location": lead.get("location"),
        "lead_country": lead.get("country"),
        "lead_grantee_url": lead.get("grantee_url"),
        "people": json.dumps(people, ensure_ascii=False) if people else None,
        "n_people": str(len(people)),
        "goals": sections.get("Goals"),
        "summary": sections.get("Summary"),
        "description": sections.get("Summary") or sections.get("Goals"),
        "updated_on": one_line(updated.group(1)) if updated else None,
        "wp_date": item.get("date"),
        "wp_modified": item.get("modified"),
        "landing_page_url": item["link"],
    }


def main() -> None:
    p = argparse.ArgumentParser(description="BrightFocus grants (WP REST + grant pages) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    items = list_grants()
    log(f"WP REST: {len(items)} grant posts")
    if args.limit:
        items = items[: args.limit]

    rows, skipped, t0 = [], [], time.time()
    for i, item in enumerate(items, 1):
        cache = None
        if args.cache_dir:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache = args.cache_dir / f"{item['id']}.html"
        if cache and cache.exists():
            page = cache.read_text()
        else:
            r = get(item["link"])
            page = r.text if r is not None and r.status_code == 200 else ""
            if r is None:
                log(f"  redirect loop (site bug), skipped: {item['link']}")
            elif r.status_code != 200:
                log(f"  HTTP {r.status_code} for {item['link']}")
            elif cache:
                cache.write_text(page)
            time.sleep(REQUEST_DELAY)
        rec = parse_grant(item, page) if page else None
        if rec is None or not rec["title"]:
            skipped.append(item["link"])
        else:
            rows.append(rec)
        if i % 100 == 0:
            el = time.time() - t0
            log(f"  {i}/{len(items)} pages, {len(rows)} parsed, ETA {el / i * (len(items) - i) / 60:.1f} min")

    df = pd.DataFrame(rows)
    log(f"Parsed {len(df)} grants, {len(skipped)} pages skipped")
    for u in skipped[:20]:
        log(f"  skipped: {u}")
    if len(skipped) > 0.02 * len(items):
        raise SystemExit(f"{len(skipped)} of {len(items)} grant pages unavailable; not writing a partial corpus")

    df["excluded"] = None
    df["exclusion_reason"] = None
    nr = df["title"].str.contains(NOT_RESEARCH_RE, na=False)
    df.loc[nr, "excluded"] = "1"
    df.loc[nr, "exclusion_reason"] = "not_research_funding"
    for gid, t in zip(df.loc[nr, "grant_id"], df.loc[nr, "title"]):
        log(f"  excluded (not research funding): {gid} {t}")

    # funder_award_id = BrightFocus Grant ID; fall back to a synthetic key on the
    # WP post id only when a page has no Grant ID.
    df["funder_award_id"] = [g if g else f"BRIGHTFOCUS-WP-{w}" for g, w in zip(df["grant_id"], df["wp_id"])]
    dup = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dup.any():
        log(f"  {dup.sum()} rows share a Grant ID:")
        for gid, grp in df[dup].groupby(df.loc[dup, "funder_award_id"].str.lower()):
            log(f"    {gid}: " + " || ".join(f"{t} ({u})" for t, u in zip(grp['title'], grp['landing_page_url'])))
        raise SystemExit("duplicate funder_award_id -- resolve before shipping")

    for c in ["grant_id", "title", "program", "award_type", "amount", "start_date", "end_date",
              "lead_name", "lead_institution", "lead_country", "description", "acknowledgement", "mentors"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  people per grant: {df['n_people'].value_counts().to_dict()}")
    log(f"  programs: {df['program'].value_counts(dropna=False).to_dict()}")
    log(f"  award types: {df['award_type'].value_counts(dropna=False).head(15).to_dict()}")
    log(f"  total amount USD {df['amount'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "brightfocus_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_brightfocus_projects.parquet"
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
