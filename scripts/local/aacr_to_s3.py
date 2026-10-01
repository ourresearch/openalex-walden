#!/usr/bin/env python3
"""
American Association for Cancer Research (AACR) grants to S3 Data Pipeline
==========================================================================

AACR publishes its grantees in the "Funded Research" section of aacr.org
(WordPress pages, server-rendered):

    https://www.aacr.org/professionals/research-funding/funded-research/
        career-development-awards/<program>/
        independent-research-grants/<program>/
        research-training-and-fellowships/<program>/

Each program page lists grantees under year headings ("2025 Grantees") as
`block bio` cards: name + degrees, position, institution, city/country and the
project title, followed by a "Research" summary paragraph. Many programs are
co-funded with a partner (AACR-AstraZeneca, BCRF-AACR, Lustgarten Foundation-
AACR, ...); those are AACR grants and stay under AACR, with the partner
recorded in the program name shipped as funder_scheme.

AACR prunes older cohorts from the live pages and retires whole programs, so
the script also reads the Internet Archive's copies of the same program URLs
(one snapshot per URL per year, 2021-2026, CDX-enumerated). Those are AACR's
own pages as AACR published them. Live pages win over archived copies for the
same grantee.

1993-2018 come from AACR's consolidated "Recipients of AACR Research Funding &
Fellowships 1993-2018" PDF (linked from the pre-2020 aacr.org Funding pages as
"the list of all previous and current AACR grant recipients"; read from the
Internet Archive's copy). Its five grants footnoted "*Awarded by Pancreatic
Cancer Action Network, administered by AACR" are kept in the parquet with
excluded=1 (not AACR money); its closing honors sections (AACR-NFCR
Professorship, Landon Prizes) are not parsed. A grantee+year present on both
the web pages and the PDF keeps the web record.

Out of scope (excluded at URL level): Stand Up To Cancer grants (Dream Teams,
Catalyst, Innovative Research Grants, Phillip A. Sharp Awards) and the
Pancreatic Cancer Collective. AACR is SU2C's scientific partner and
administers them, but the money is SU2C's (OpenAlex funder F4320332898) /
Lustgarten's, so they must not be attributed to AACR (runbook §2.3.2).

No grant number or amount is published per grantee. Citing works use AACR's
internal grant number ("17-40-48-NORT"), which cannot be derived from the
pages, so funder_award_id is a stable synthetic key AACR-{year}-{person-slug}
(program slug appended only if one person has two grants in one year).

Method 5 (static HTML) + Internet Archive snapshots of the same pages.

Output: s3://openalex-ingest/awards/aacr/aacr_projects.parquet
"""

import argparse
import hashlib
import html
import json
import re
import time
import unicodedata
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

BASE = "https://www.aacr.org"
SECTION = f"{BASE}/professionals/research-funding/funded-research/"
CATEGORIES = ["career-development-awards", "independent-research-grants", "research-training-and-fellowships"]
CDX = "https://web.archive.org/cdx/search/cdx"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/aacr/aacr_projects.parquet"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
LIVE_DELAY = 0.5
WAYBACK_DELAY = 1.5
RETRIES = 4

# SU2C / Pancreatic Cancer Collective: administered by AACR, funded by others
EXCLUDE_RE = re.compile(r"su2c|stand-up-to-cancer|pancreatic-cancer-collective", re.I)
# Programs that are honors/prizes rather than research grants live under
# /awards-and-lectureships/, not here; everything in funded-research is a grant.


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def http_get(url: str, params: dict | None = None, delay: float = 0.0) -> requests.Response | None:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=120)
            if r.status_code in (429, 500, 502, 503, 504, 520, 522, 523, 524):
                raise RuntimeError(f"HTTP {r.status_code}")
            time.sleep(delay)
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(5 * (attempt + 1) ** 2)
    log(f"  giving up on {url}: {last}")
    return None


def canon(u: str) -> str:
    u = re.sub(r"[?#].*$", "", u).replace("http://", "https://").replace("://aacr.org", "://www.aacr.org")
    u = u.replace("research-training-and-%20fellowships", "research-training-and-fellowships")
    return u.rstrip("/") + "/"


def is_program(u: str) -> bool:
    rest = u[len(SECTION):].strip("/").split("/") if u.startswith(SECTION) else []
    return (len(rest) == 2 and rest[0] in CATEGORIES and rest[1] != "embed"
            and not EXCLUDE_RE.search(u))


def live_program_urls() -> list[str]:
    urls = []
    for cat in CATEGORIES:
        r = http_get(SECTION + cat + "/", delay=LIVE_DELAY)
        if r is None or r.status_code != 200:
            raise RuntimeError(f"category page {cat} unavailable")
        body = r.text[r.text.find("first-section"):]
        body = body[: body.find("<footer")]
        for h in re.findall(r'<a[^>]*href="([^"]+)"', body):
            if not h.startswith(BASE):
                continue
            # a few links point outside funded-research (e.g. /research-funding/aacr-bms-...)
            urls.append((cat, canon(h)))
    return urls


def wayback_snapshots() -> dict[str, list[str]]:
    """url -> sorted snapshot timestamps (one per year: the last capture of each
    year, plus the earliest capture overall), deduped by content digest."""
    r = http_get(CDX, {"url": SECTION.replace("https://www.", ""), "matchType": "prefix", "output": "json",
                       "fl": "original,timestamp,digest", "filter": "statuscode:200",
                       "collapse": "digest", "limit": "50000"})
    if r is None or r.status_code != 200:
        raise RuntimeError("Wayback CDX unavailable")
    by_url: dict[str, list[str]] = {}
    for orig, ts, _ in r.json()[1:]:
        u = canon(orig)
        if is_program(u):
            by_url.setdefault(u, []).append(ts)
    out = {}
    for u, tss in by_url.items():
        tss = sorted(set(tss))
        per_year = {}
        for ts in tss:
            per_year[ts[:4]] = ts
        out[u] = sorted(set(per_year.values()) | {tss[0]})
    return out


def fetch(url: str, ts: str | None, cache_dir: Path | None) -> str | None:
    key = hashlib.md5(f"{ts}|{url}".encode()).hexdigest()
    cache = cache_dir / f"{key}.html" if cache_dir else None
    if cache and cache.exists():
        return cache.read_text()
    if ts is None:
        r = http_get(url, delay=LIVE_DELAY)
    else:
        r = http_get(f"https://web.archive.org/web/{ts}id_/{url}", delay=WAYBACK_DELAY)
    if r is None or r.status_code != 200:
        if r is not None:
            log(f"  HTTP {r.status_code}: {ts or 'live'} {url}")
        return None
    r.encoding = "utf-8"
    page = r.text
    if cache:
        cache.write_text(page)
    return page


def clean(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def lines_of(fragment: str) -> list[str]:
    parts = re.split(r"<br\s*/?>|</p>\s*<p[^>]*>|</p>|<p[^>]*>", fragment)
    return [c for c in (clean(p) for p in parts) if c]


DEGREE_TOKENS = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                 # AACR prints full degree strings after the name ("Jennifer Kwan, MD, PhD, FRCPC")
                 "pharmd", "mmed", "msc", "ms", "mph", "mbbs", "mbchb", "frcpc", "frcp", "frcpath", "facp",
                 "dvm", "mba", "ma", "bs", "bsc", "rn", "np", "od", "do", "mpharm", "mres", "mrcp", "frcr",
                 "frcs", "facs", "fasco", "mhs", "mas", "mscr", "msci", "drph", "edd", "psyd", "md-phd"}
HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


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


LOC_HINT = re.compile(r",\s*[A-Z]|\b(USA|U\.S\.A\.|United States|Canada|United Kingdom|UK|Spain|France|Italy|"
                      r"Germany|Netherlands|Australia|Israel|Japan|China|Switzerland|Sweden|Singapore|"
                      r"Argentina|Brazil|Mexico|Uganda|Nigeria|India|Belgium|Denmark|Portugal|Austria|Korea)\b")


def parse_bio(name: str, bio_html: str) -> dict:
    em = re.search(r"<em>(.*?)</em>", bio_html, re.S)
    title = clean(em.group(1)) if em else None
    body = bio_html[: em.start()] + bio_html[em.end():] if em else bio_html
    ls = [l for l in lines_of(body) if l.lower() != name.lower()]
    if title is None and len(ls) >= 4:
        title = ls.pop()  # untagged project title on the last line (Derosa, Morsby)
    loc = inst = pos = None
    if ls and LOC_HINT.search(ls[-1]) and len(ls) >= 2:
        loc = ls.pop()
    if ls:
        inst = ls.pop()
    if ls:
        pos = "; ".join(ls)
    return {"project_title": title, "position": pos, "institution": inst, "location": loc}


TOKEN_RE = re.compile(
    r'(?P<h2><h2[^>]*>(?P<h2t>.*?)</h2>)'
    r'|(?P<sub>class="page-subtitle">(?P<subt>.*?)</span>)'
    # some cards carry data-wpr-lazyrender="1" before the class attribute
    r'|(?P<bio><div[^>]*class="block bio"[^>]*>.*?<h1 class="post-title">(?P<name>.*?)</h1>\s*<div class="bio">(?P<biot>.*?)</div>)'
    # "Research" / "Research&nbsp;" / "Scientific Statement of Research", optionally followed by an empty <strong></strong>
    r'|(?P<research><p[^>]*>\s*<strong>\s*(?:Scientific(?:&nbsp;|\s)+Statement(?:&nbsp;|\s)+of(?:&nbsp;|\s)+)?Research(?:&nbsp;|\s)*</strong>'
    r'(?:<strong>\s*</strong>)?(?:&nbsp;|\s)*</p>\s*<p[^>]*>(?P<rest>.*?)</p>)',
    re.S | re.I)


def page_program(page: str) -> str | None:
    t = re.search(r"<title>(.*?)</title>", page, re.S)
    if not t:
        return None
    s = clean(t.group(1)) or ""
    s = re.split(r"\s+[|]\s+|\s+-\s+American Associat", s)[0].strip()
    return s or None


def parse_page(page: str, url: str, ts: str | None, category: str) -> list[dict]:
    body = page[page.find("first-section"):]
    end = body.find("<footer")
    body = body[:end] if end > 0 else body
    program = page_program(page)
    year, sub, out = None, None, []
    for m in TOKEN_RE.finditer(body):
        if m.group("h2"):
            h = clean(m.group("h2t")) or ""
            y = re.search(r"\b(19|20)\d{2}\b", h)
            if y:
                year, sub = int(y.group(0)), None
        elif m.group("sub"):
            sub = clean(m.group("subt"))
        elif m.group("bio"):
            name = clean(m.group("name"))
            if not name:
                continue
            given, family = split_name(name)
            rec = {"name": name, "given_name": given, "family_name": family,
                   "year": year, "program": program, "program_variant": sub,
                   "category": category, "page_url": url, "snapshot": ts or "live"}
            rec.update(parse_bio(name, m.group("biot")))
            rec["research_summary"] = None
            out.append(rec)
        elif m.group("research") and out and out[-1]["research_summary"] is None:
            out[-1]["research_summary"] = clean(m.group("rest"))
    return out


# --- AACR "Recipients of AACR Research Funding & Fellowships 1993-2018" PDF ---
# Linked from the pre-2020 aacr.org Funding pages ("View the list of all previous
# and current AACR grant recipients"); AACR's own consolidated list, read from
# the Internet Archive's copy. Two-column layout; fonts carry the structure:
# 8pt bold = year / program heading / grantee name, 8pt regular = institution,
# 8pt italic = "Project: ..." (wrapped), 7pt = "*Awarded by ..." footnotes,
# 12pt bold = section titles. The Avon Foundation-AACR International Scholar
# section uses Scholar / Term in United States / Host / Research blocks.
RECIPIENTS_PDF = ("https://web.archive.org/web/20190921213331id_/"
                  "https://www.aacr.org/Funding/Shared%20Documents/Recipients%201993-2018.pdf")
PDF_STOP_SECTION = "AACR-National Foundation for Cancer Research"  # professorship + Landon prizes (honors) follow
DEGREE_RE = re.compile(r",\s*(?:[A-Z][A-Za-z.]{0,7}(?:\s*\([A-Za-z]+\))?)(?:,\s*[A-Z][A-Za-z.]{0,7})*\s*$")
YEAR_RE = re.compile(r"(19|20)\d{2}")
NOT_NAME_RE = re.compile(r"\b(AACR|Award|Grant|Grants|Fellowship|Research|Foundation|Network|Inc|Company|"
                         r"Corporation|in memory|in honor|for|of the)\b")
DESCRIPTION_LEAK_RE = re.compile(r"\$|research funding|the recipient|^Page \d+$", re.I)


def _pdf_kind(line: dict) -> str:
    import collections
    name, size = collections.Counter(
        (c["fontname"].split("+")[-1], round(c["size"])) for c in line["chars"]).most_common(1)[0][0]
    if size >= 10:
        return "title" if "Bold" in name else "pageno"
    if size < 8:
        return "small"
    if "Bold" in name:
        return "bold"
    if "Italic" in name:
        return "italic"
    return "regular"


def _pdf_lines(pdf) -> list[tuple]:
    """(page, top, col, kind, text) in reading order. A full-width section title
    splits its page into bands: the band above it is read left column then right
    column, then the title, then the band below it (left, then right). Section-
    description paragraphs that span both columns are dropped."""
    out = []
    for pi, p in enumerate(pdf.pages):
        mid = p.width / 2
        titles = sorted((l["top"], l["text"].strip()) for l in p.extract_text_lines()
                        if _pdf_kind(l) == "title" and l["text"].strip())
        tops = [tp for tp, _ in titles]

        def band(top: float) -> int:
            return sum(1 for tp in tops if tp <= top + 0.5)

        rows = []
        for ci, box in enumerate([(0, 0, mid, p.height), (mid, 0, p.width, p.height)]):
            for l in p.crop(box).extract_text_lines():
                k, t = _pdf_kind(l), l["text"].strip()
                if not t or k in ("title", "pageno"):
                    continue
                full = (ci == 0 and l["x1"] >= mid - 1) or (ci == 1 and l["x0"] <= mid + 1)
                if full and k in ("regular", "small"):
                    continue
                rows.append((band(l["top"]), ci, l["top"], k, t))
        for n, (tp, t) in enumerate(titles, 1):
            rows.append((n, -1, tp, "title", t))
        rows.sort(key=lambda r: (r[0], r[1], r[2]))
        out += [(pi, top, ci, k, t) for _, ci, top, k, t in rows]
    return out


def _is_name(s: str) -> bool:
    if NOT_NAME_RE.search(s) or len(s) > 80:
        return False
    head = s.split(",")[0].split()
    return len(head) >= 2  # "Allen, MD" is the tail of "in honor of Robert C. Allen, MD"


def parse_recipients_pdf(pdf_path: Path) -> list[dict]:
    import pdfplumber
    pdf = pdfplumber.open(str(pdf_path))
    lines = _pdf_lines(pdf)
    entries, section, year, heading, cur = [], None, None, None, None
    title_buf, avon_state = [], None
    i = 0
    while i < len(lines):
        pi, top, ci, k, t = lines[i]
        if k == "title":
            if title_buf and title_buf[-1][0] == pi and title_buf[-1][2] == ci and abs(title_buf[-1][1] - top) < 30:
                title_buf.append((pi, top, ci, t))
            else:
                title_buf = [(pi, top, ci, t)]
            full = " ".join(x[3] for x in title_buf)
            if PDF_STOP_SECTION in full:
                break
            if not full.startswith("Recipients of AACR") and full != section:
                section, heading, cur = full, None, None
            i += 1
            continue
        avon = bool(section and "International Scholar" in section)
        if avon:
            if k == "bold" and t.startswith("Scholar:"):
                name = t.split(":", 1)[1].strip()
                cur = {"section": section, "year": None, "program": section, "people": [
                    {"name": name, "institution": None, "position": None}], "project": None, "note": None,
                    "pdf_page": pi + 1, "_lines": []}
                entries.append(cur)
                avon_state = "scholar"
            elif cur is not None and k == "regular":
                if t.startswith("Term in U"):  # one block misspells "Untied States"
                    m = YEAR_RE.search(t)
                    cur["year"] = int(m.group(0)) if m else None
                    cur["note"] = t
                    avon_state = "term"
                elif t.startswith("Host:"):
                    avon_state = "host"
                elif avon_state == "scholar":
                    cur["_lines"].append(t)
            elif cur is not None and k == "italic":
                if t.startswith("Research:"):
                    cur["project"] = t.split(":", 1)[1].strip()
                elif cur["project"]:
                    cur["project"] += " " + t
            i += 1
            continue
        if k == "bold" and YEAR_RE.fullmatch(t):
            year, heading = int(t), None
            i += 1
            continue
        if k == "bold":
            j, run = i, []
            while (j < len(lines) and lines[j][3] == "bold" and not YEAR_RE.fullmatch(lines[j][4])
                   and lines[j][0] == pi and lines[j][2] == ci):
                run.append(lines[j][4])
                j += 1
            names = []
            while run and DEGREE_RE.search(run[-1]) and _is_name(run[-1]):
                names.insert(0, run.pop())
            nxt = lines[j][3] if j < len(lines) else None
            if not names and run and nxt in ("regular", "italic", None) and _is_name(run[-1]):
                names = [run.pop()]
            if run:
                heading = " ".join(run)
            if names:
                if cur is not None and not run and cur["project"] is None and cur["year"] == year \
                        and cur["section"] == section:
                    cur["people"] += [{"name": n, "institution": None, "position": None} for n in names]
                else:
                    cur = {"section": section, "year": year, "program": heading,
                           "people": [{"name": n, "institution": None, "position": None} for n in names],
                           "project": None, "note": None, "pdf_page": pi + 1}
                    entries.append(cur)
            i = j
            continue
        if cur is None:
            i += 1
            continue
        if k == "small" or t.startswith("*"):
            cur["note"] = (cur["note"] + " " + t) if cur["note"] else t
        elif k == "regular":
            if not DESCRIPTION_LEAK_RE.search(t):
                t = re.sub(r"^Project title:\s*", "", t)
                proj = None
                if "Project:" in t:
                    t, proj = [x.strip() for x in t.split("Project:", 1)]
                open_people = [p for p in cur["people"] if p["institution"] is None]
                if t and open_people and cur["project"] is None:
                    for p in open_people:  # two PIs listed above one shared institution
                        p["institution"] = t
                if proj:
                    cur["project"] = proj
        elif k == "italic":
            if t.lower().startswith("project:"):
                cur["project"] = t.split(":", 1)[1].strip()
            elif cur["project"]:
                cur["project"] += ("" if cur["project"].endswith("-") else " ") + t
            else:
                cur["project"] = t
        i += 1
    for e in entries:
        ls = e.pop("_lines", None)
        if ls:  # Avon scholar: position line(s), then the home institution
            e["people"][0]["institution"] = ls[-1]
            e["people"][0]["position"] = "; ".join(ls[:-1]) or None
    return entries


US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware",
    "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky",
    "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
    "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming", "district of columbia", "d.c.", "dc", "puerto rico",
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky",
    "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd",
    "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy"}
COUNTRY_ALIASES = {"usa": "United States", "u.s.a.": "United States", "us": "United States",
                   "u.s.": "United States", "united states of america": "United States",
                   "uk": "United Kingdom", "england": "United Kingdom", "scotland": "United Kingdom"}


def country_of(location: str | None) -> str | None:
    """'Houston, Texas, USA' / 'New York, New York' / 'Paris, France' -> country name."""
    if not location:
        return None
    last = location.split(",")[-1].strip().rstrip(".").strip()
    if not last:
        return None
    low = last.lower()
    if low in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[low]
    if low in US_STATES or low.replace(".", "") in US_STATES:
        return "United States"
    if "," not in location:
        return None  # a bare city: country unknown
    return last


def norm_person(name: str) -> str:
    g, f = split_name(name)
    return slug(" ".join(x for x in (g, f) if x))


RECIPIENTS_PDF_LANDING = RECIPIENTS_PDF.replace("id_/", "/")


def recipients_pdf_frame(cache_dir: Path) -> pd.DataFrame:
    """1993-2018 grants from AACR's consolidated recipients PDF, in the same
    columns as the web cards (plus people/note/excluded)."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / "aacr_recipients_1993_2018.pdf"
    if not path.exists():
        r = http_get(RECIPIENTS_PDF, delay=WAYBACK_DELAY)
        if r is None or r.status_code != 200 or not r.content.startswith(b"%PDF"):
            raise RuntimeError("recipients PDF unavailable from the Internet Archive")
        path.write_bytes(r.content)
    entries = parse_recipients_pdf(path)
    rows = []
    for e in entries:
        if e["year"] is None or not e["people"]:
            log(f"    PDF entry without year/person skipped: {e}")
            continue
        people = []
        for pp in e["people"]:
            g, f = split_name(pp["name"])
            people.append({"name": pp["name"], "given_name": g, "family_name": f,
                           "institution": pp["institution"], "country": None})
        lead = people[0]
        note = e["note"]
        # "*Awarded by Pancreatic Cancer Action Network, administered by AACR": not AACR money
        excluded = bool(note and re.search(r"Awarded by (?!AACR)", note))
        rows.append({
            "name": lead["name"], "given_name": lead["given_name"], "family_name": lead["family_name"],
            "year": int(e["year"]), "program": e["program"] or e["section"], "program_variant": None,
            "category": e["section"], "page_url": RECIPIENTS_PDF_LANDING, "snapshot": "pdf-recipients-1993-2018",
            "project_title": e["project"], "position": e["people"][0].get("position"),
            "institution": lead["institution"], "location": None, "research_summary": None,
            "person_key": norm_person(lead["name"]), "program_key": slug(e["program"] or e["section"] or ""),
            "n_renderings": 1, "source": "pdf", "people": json.dumps(people, ensure_ascii=False),
            "note": note, "pdf_page": str(e["pdf_page"]),
            "excluded": "1" if excluded else None,
            "exclusion_reason": "awarded_by_other_funder" if excluded else None,
        })
    return pd.DataFrame(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="AACR funded-research grantees (+ Internet Archive copies) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N program URLs")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--no-wayback", action="store_true", help="live pages only")
    p.add_argument("--no-pdf", action="store_true", help="skip the 1993-2018 recipients PDF")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    live = live_program_urls()
    cat_of = {u: c for c, u in live}
    log(f"Live: {len(live)} program links")
    snaps = {} if args.no_wayback else wayback_snapshots()
    for u in snaps:
        cat_of.setdefault(u, u[len(SECTION):].split("/")[0])
    urls = [u for u in dict.fromkeys([u for _, u in live] + sorted(snaps)) if not EXCLUDE_RE.search(u)]
    log(f"Program URLs: {len(urls)} ({len(snaps)} with archived copies, "
        f"{sum(len(v) for v in snaps.values())} snapshots)")
    if args.limit:
        urls = urls[: args.limit]

    recs, zero_pages, t0 = [], [], time.time()
    live_set = {u for _, u in live}
    jobs = [(u, None) for u in urls if u in live_set] + [(u, ts) for u in urls for ts in snaps.get(u, [])]
    for i, (u, ts) in enumerate(jobs, 1):
        page = fetch(u, ts, args.cache_dir)
        if page:
            got = parse_page(page, u, ts, cat_of.get(u, ""))
            if not got:
                zero_pages.append(f"{ts or 'live'} {u}")
            recs += got
        if i % 25 == 0:
            el = time.time() - t0
            log(f"  {i}/{len(jobs)} pages, {len(recs)} grantee cards, ETA {el / i * (len(jobs) - i) / 60:.1f} min")
    log(f"Fetched {len(jobs)} pages -> {len(recs)} grantee cards; {len(zero_pages)} pages with no cards")
    for z in zero_pages[:40]:
        log(f"  no cards: {z}")

    df = pd.DataFrame(recs)
    df["person_key"] = df["name"].map(norm_person)
    df["program_key"] = df["program"].fillna("").map(slug)
    # live first, then newest snapshot first: the most recent rendering wins
    df["_order"] = df["snapshot"].map(lambda s: "99999999999999" if s == "live" else s)
    df = df.sort_values("_order", ascending=False)
    df["year"] = df["year"].astype("object")
    no_year = df["year"].isna()
    log(f"  cards without a year heading: {no_year.sum()} (dropped unless the same grantee has a dated card)")
    # fill year for undated cards from a dated card of the same person+program
    dated = df[~no_year].drop_duplicates(["person_key", "program_key"]).set_index(["person_key", "program_key"])["year"]
    df.loc[no_year, "year"] = [dated.get((pk, gk)) for pk, gk in zip(df.loc[no_year, "person_key"], df.loc[no_year, "program_key"])]
    still = df["year"].isna()
    for _, r in df[still].drop_duplicates(["person_key", "program_key"]).iterrows():
        log(f"    undated, dropped: {r['name']} | {r['program']} | {r['page_url']}")
    df = df[~still].copy()
    df["year"] = df["year"].astype(int)

    # one grant = person x year x program; fill blanks from older renderings
    agg = {c: "first" for c in df.columns if c not in ("person_key", "year", "program_key")}
    df = df.groupby(["person_key", "year", "program_key"], as_index=False, sort=False).agg(
        {**agg, "snapshot": lambda s: ",".join(sorted(set(s)))})
    df["n_renderings"] = df["snapshot"].str.count(",") + 1
    # the same person+year listed under two slugs of one renamed program
    # ("aacr-fellowship-to-..." vs "aacr-fellowships-to-..."): collapse on project title
    df["title_key"] = df["project_title"].fillna("").map(lambda s: slug(s)[:60])
    dup = df.duplicated(["person_key", "year", "title_key"], keep="first") & (df["title_key"] != "")
    if dup.any():
        log(f"  {dup.sum()} cross-program duplicates (same person, year, project title) collapsed")
    df = df[~dup].copy()
    # one grant re-listed under a later cohort heading on a later rendering
    # (Victoria's Secret Rising Innovator 2022 grantees shown again as "2023
    # Grantees"; a renamed grantee "Marlene"/"Marleen Kok"): same program + same
    # project title -> one grant, at the earliest cohort year it was listed under
    df = df.sort_values(["year", "_order"], ascending=[True, False])
    relisted = df.duplicated(["program_key", "title_key"], keep="first") & (df["title_key"] != "")
    for _, r in df[relisted].iterrows():
        log(f"    re-listed under a later year, dropped: {r['year']} {r['name']} | {r['program']}")
    df = df[~relisted].copy()
    df["source"] = "web"
    df["location_country"] = df["location"].map(country_of)
    df["people"] = [json.dumps([{"name": n, "given_name": g, "family_name": f, "institution": i, "country": c}],
                               ensure_ascii=False)
                    for n, g, f, i, c in zip(df["name"], df["given_name"], df["family_name"],
                                             df["institution"], df["location_country"])]
    df = df.drop(columns=["location_country"])

    if not args.no_pdf:
        pdf_df = recipients_pdf_frame(args.cache_dir or args.output_dir)
        # the PDF itself prints one project title under two different grantees
        # twice (Wurdak/Yong 2008, Wrana 1997/Lyssiotis 2013): unknowable which is
        # right, so neither keeps the title (display_name falls back to program+year)
        shared = pdf_df["project_title"].notna() & (
            pdf_df.groupby("project_title")["person_key"].transform("nunique") > 1)
        for n, t in zip(pdf_df.loc[shared, "name"], pdf_df.loc[shared, "project_title"]):
            log(f"    PDF prints this title under 2+ grantees, title dropped: {n} | {t}")
        pdf_df.loc[shared, "project_title"] = None
        pdf_df["title_key"] = pdf_df["project_title"].fillna("").map(lambda s: slug(s)[:60])
        # same grantee on the web pages and in the PDF (same year, or same project
        # title under a year that differs by the cohort/announcement offset)
        seen = set(zip(df["person_key"], df["year"]))
        seen_t = set(zip(df["person_key"], df["title_key"]))
        dup_pdf = [(pk, y) in seen or (tk != "" and (pk, tk) in seen_t)
                   for pk, y, tk in zip(pdf_df["person_key"], pdf_df["year"], pdf_df["title_key"])]
        log(f"  recipients PDF: {len(pdf_df)} grants, {sum(dup_pdf)} already on the web pages (web kept)")
        df = pd.concat([df, pdf_df[[not d for d in dup_pdf]]], ignore_index=True)

    df["funder_award_id"] = "AACR-" + df["year"].astype(str) + "-" + df["person_key"]
    clash = df["funder_award_id"].duplicated(keep=False)
    df.loc[clash, "funder_award_id"] = df.loc[clash, "funder_award_id"] + "-" + df.loc[clash, "program_key"].str[:40]
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate funder_award_id after disambiguation")
    df["landing_page_url"] = df["page_url"]
    df["country"] = df["location"].map(country_of)
    df = df.drop(columns=["_order", "title_key"], errors="ignore").sort_values(["year", "program", "family_name"])

    log(f"Grants: {len(df)}; years {df['year'].min()}-{df['year'].max()}")
    log(f"  by source: {df['source'].value_counts().to_dict()}; excluded: "
        f"{df['exclusion_reason'].value_counts().to_dict() if 'exclusion_reason' in df else {}}")
    log(f"  by year: {df['year'].value_counts().sort_index().to_dict()}")
    log(f"  by category: {df['category'].value_counts().to_dict()}")
    for c in ["project_title", "position", "institution", "location", "research_summary", "program_variant"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  web grants only in archived copies: "
        f"{((df['source'] == 'web') & ~df['snapshot'].str.contains('live')).sum()}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "aacr_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_aacr_projects.parquet"
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
