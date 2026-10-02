#!/usr/bin/env python3
"""
Danmarks Grundforskningsfond (DNRF, Danish National Research Foundation) to S3
===============================================================================

DNRF funds a small portfolio of large, long grants: Centers of Excellence
(application rounds since 1993), DNRF Chairs, Pioneer Centres, and the retired
Niels Bohr Professorship / Niels Bohr Visiting Professorship / DG Professorship
programmes. It publishes no bulk export or API of grants; dg.dk is WordPress,
so this uses the WP REST API plus static HTML (Methods 2 + 5 on the ladder):

  1. WP REST /wp-json/wp/v2/centers -> every ACTIVE grant with a detail page
     (Centers of Excellence, DNRF Chairs, Pioneer Centres, Niels Bohr profs).
     Each English detail page (dg.dk/en/centers/<slug>/) has a structured
     <div class="center-info"> block: leader, Period, Application round,
     Host institution(s), Grant ("54,6 M DKK").
  2. /en/former-centers-of-excellence/ -> every FORMER Center of Excellence,
     grouped by application round ("Centers established in 2012 - 7th
     Application round"): name, head of center, location. No amounts/dates.
  3. The retired-programme pages (Danish only): Niels Bohr Professorater,
     Niels Bohr Gaesteprofessorer, DG professorer -> name, institutions,
     grant period.

funder_award_id (runbook 2.1.1): DNRF numbers its grants "DNRF<n>" and that
is what grantees cite (e.g. "CENPERM DNRF100", "HADAL, Grant No. DNRF145"),
but dg.dk never prints the number (checked the site search, every detail page
and the 2016 + 2025 annual reports). dnrf_grant_numbers.csv (next to this
script) is a static crosswalk from grant to DNRF number, built from
acknowledgement strings already in openalex_awards_raw that name BOTH the
center acronym and the number (see that file's header). Grants with a
crosswalk hit ship as "DNRF<n>" so they collapse onto the existing citation
shells; the rest ship as a stable synthetic key "DG-<programme>-<key>".
The dnrf_number column records which rows came from the crosswalk.

Output: s3://openalex-ingest/awards/dnrf/dnrf_projects.parquet
"""

import argparse
import calendar
import csv
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

BASE = "https://dg.dk"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/dnrf/dnrf_projects.parquet"
CROSSWALK = Path(__file__).with_name("dnrf_grant_numbers.csv")

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 3

CENTERTYPES = {14: "Center of Excellence", 215: "DNRF Chair",
               214: "Pioneer Centre", 15: "Niels Bohr Professorship"}
PROGRAMME_CODES = {"Center of Excellence": "COE", "DNRF Chair": "CHAIR", "Pioneer Centre": "PIONEER",
                   "Niels Bohr Professorship": "NBP", "Niels Bohr Visiting Professorship": "NBVP",
                   "DG Professorship": "DGP"}
MONTHS_EN = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], 1)}
MONTHS_DA = {m: i for i, m in enumerate(
    ["januar", "februar", "marts", "april", "maj", "juni", "juli",
     "august", "september", "oktober", "november", "december"], 1)}

RETIRED_PAGES = [  # (programme, url) -- Danish-only pages
    ("Niels Bohr Professorship", f"{BASE}/forskningsaktiviteter/tidligere-virkemidler/niels-bohr-professorat/"),
    ("Niels Bohr Visiting Professorship", f"{BASE}/forskningsaktiviteter/tidligere-virkemidler/niels-bohr-gaesteprofessorer/"),
    ("DG Professorship", f"{BASE}/forskningsaktiviteter/tidligere-virkemidler/dg-professorer/"),
]


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            log(f"  GET {url} -> {r.status_code} ({len(r.content)} bytes)")
            if r.status_code == 404:
                return ""
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>", " ", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def slugify(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


HONORIFIC_RE = re.compile(r"^(?:(?:professor|prof|dr|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook 2.4.1 helper (wolf_to_s3.py) plus a leading
    honorific strip ("Professor Ronnie Glud")."""
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


def clean_person(name: str | None) -> str | None:
    if not name:
        return None
    return re.sub(r"\s+", " ", HONORIFIC_RE.sub("", name.strip())).strip() or None


def acronym(title: str | None) -> str | None:
    """"Center for Hadal Research (HADAL)" -> "HADAL";
    "Center for Hyperpolarization in Magnetic Resonance - HYPERMAG" -> "HYPERMAG"."""
    m = re.search(r"\(([A-Za-z][A-Za-z0-9\-]{1,15})\)\.?\s*$", title or "")
    if m:
        return m.group(1)
    m = re.search(r"\s[–—-]\s*([A-Z][A-Za-z0-9]{2,15})\s*$", title or "")
    return m.group(1) if m else None


def parse_en_date(s: str | None, end: bool = False) -> str | None:
    """'September 01, 2020' -> 2020-09-01; 'September 2025' -> 2025-09-01 (month end if end=True)."""
    m = re.search(r"([A-Za-z]+),?\s+(?:(\d{1,2}),?\s+)?(\d{4})", s or "")
    month = m.group(1).lower() if m else ""
    mo = MONTHS_EN.get(month) or MONTHS_DA.get(month)  # one English page says "Marts 2025"
    if not mo:
        return None
    y = int(m.group(3))
    if y > 2100:  # source typo ("August 3031"): drop rather than guess
        return None
    d = int(m.group(2)) if m.group(2) else (calendar.monthrange(y, mo)[1] if end else 1)
    return f"{y}-{mo:02d}-{d:02d}"


def parse_da_date(s: str | None, end: bool = False) -> str | None:
    """'1. oktober 2016' -> 2016-10-01; 'oktober 2006' -> 2006-10-01 (month end if end=True)."""
    m = re.search(r"(?:(\d{1,2})\.\s*)?([a-zæøå]+)\s+(\d{4})", (s or "").strip().lower())
    if not m or m.group(2) not in MONTHS_DA:
        return None
    y, mo = int(m.group(3)), MONTHS_DA[m.group(2)]
    d = int(m.group(1)) if m.group(1) else (calendar.monthrange(y, mo)[1] if end else 1)
    return f"{y}-{mo:02d}-{d:02d}"


def parse_amount_dkk(s: str | None) -> float | None:
    """'54,6 M DKK' -> 54600000.0 ; '10 mio. kr.' -> 10000000.0 ; '60.000.000 DKK' -> 60000000.0"""
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:M\b|mio|million)", s or "", re.I)
    if m:
        return round(float(m.group(1).replace(",", ".")) * 1_000_000, 2)
    m = re.search(r"\d{1,3}(?:[.,]\d{3})+|\d{4,}", s or "")
    return float(re.sub(r"[.,]", "", m.group(0))) if m else None


def base_row() -> dict:
    return {k: None for k in [
        "programme", "status", "title", "acronym", "lead_name", "lead_given_name", "lead_family_name",
        "host_institution", "start_date", "end_date", "start_year", "end_year", "established_year",
        "application_round", "amount_text", "amount", "description", "landing_page_url", "source_key",
        "raw_fields", "co_lead_name", "co_lead_given_name", "co_lead_family_name"]}


# ---------------------------------------------------------------- active (REST)
def rest_centers() -> list[dict]:
    out, page, total_pages = [], 1, None
    while total_pages is None or page <= total_pages:
        r = requests.get(f"{BASE}/wp-json/wp/v2/centers", params={"per_page": 100, "page": page},
                         headers=HEADERS, timeout=60)
        log(f"  REST centers page {page} -> {r.status_code}")
        r.raise_for_status()
        total_pages = int(r.headers.get("X-WP-TotalPages", "1"))  # authoritative terminator
        out += r.json()
        page += 1
    return out


def parse_detail(page: str) -> dict:
    start = page.find('<div class="center-info">')
    end = page.find('<div class="bg">', start)
    block = page[start:end] if start >= 0 else ""
    fields = {}
    for head, val in re.findall(r'<p class="data-head">(.*?)</p>\s*<p>(.*?)</p>', block, re.S):
        fields[text(head).rstrip(":").strip().lower()] = text(val)
    h1 = re.search(r"<h1>(.*?)</h1>", page, re.S)
    body = re.search(r'<div id="section-1" class="item item-text">(.*?)</div>', page, re.S)
    return {"h1": text(h1.group(1)) if h1 else None, "fields": fields,
            "description": text(body.group(1)) if body else None}


LEADER_KEYS = ("dnrf chair", "center leader", "centre leader", "head of cent", "chair", "professor",
               "center director", "centre director", "leader", "centerleder")


def active_rows(cache_dir: Path | None, limit: int | None) -> list[dict]:
    rows = []
    items = rest_centers()
    log(f"REST: {len(items)} active grants")
    if limit:
        items = items[:limit]
    for n, it in enumerate(items, 1):
        slug = it["slug"]
        url_en = f"{BASE}/en/centers/{slug}/"
        cache = cache_dir / f"center-{slug[:80]}.html" if cache_dir else None
        if cache and cache.exists():
            page = cache.read_text()
        else:
            page = get(url_en)
            if not page:
                page = get(it["link"])
            if cache:
                cache.write_text(page)
            time.sleep(REQUEST_DELAY)
        d = parse_detail(page)
        f = d["fields"]
        if not f:
            log(f"  WARNING: no center-info block on {slug}")
        leader = next((v for k, v in f.items() if any(k.startswith(x) for x in LEADER_KEYS)), None)
        period = f.get("period") or f.get("periode") or ""
        parts = re.split(r"\s*[-–]\s*", period)
        years = re.fullmatch(r"\s*(\d{4})\s*[-–]\s*(\d{4})\s*", period)  # "2024-2036"
        title = d["h1"] or text(it["title"]["rendered"])
        ctype = next((CENTERTYPES[t] for t in it.get("centertype") or [] if t in CENTERTYPES), None)
        if ctype == "Niels Bohr Professorship" and not title.startswith("Niels Bohr"):
            title = f"Niels Bohr Professorship: {clean_person(title)}"
        # "Tejs Vegge/ co.lead Frede Blaabjerg" -> lead + co-lead
        people = [clean_person(re.sub(r"^co\.?\s*-?lead\s*", "", x.strip(), flags=re.I))
                  for x in (leader or "").split("/") if x.strip()]
        lead = people[0] if people else None
        co_lead = people[1] if len(people) > 1 else None
        given, family = split_name(lead or "")
        co_given, co_family = split_name(co_lead or "")
        grant_txt = f.get("grant") or f.get("bevilling")
        r = base_row()
        r.update({
            "programme": ctype,
            "start_year": years.group(1) if years else None,
            "end_year": years.group(2) if years else None,
            "co_lead_name": co_lead,
            "co_lead_given_name": co_given,
            "co_lead_family_name": co_family,
            "status": "active",
            "title": title,
            "acronym": acronym(title),
            "lead_name": lead,
            "lead_given_name": given,
            "lead_family_name": family,
            "host_institution": next((v for k, v in f.items() if k.startswith("host institution") or k.startswith("værtsinstitution")), None),
            "start_date": parse_en_date(parts[0]) if parts[0] else None,
            "end_date": parse_en_date(parts[1], end=True) if len(parts) > 1 else None,
            "application_round": f.get("application round"),
            "amount_text": grant_txt,
            "amount": parse_amount_dkk(grant_txt),
            "description": d["description"] or text(it["content"]["rendered"]),
            "landing_page_url": url_en if f'href="{url_en}"' in page or 'lang="en' in page[:3000] else it["link"],
            "source_key": f"rest-{it['id']}",
            "raw_fields": json.dumps(f, ensure_ascii=False),
        })
        rows.append(r)
        if n % 10 == 0:
            log(f"  {n}/{len(items)} active detail pages parsed")
    return rows


# ------------------------------------------------------------- former centers
LEAD_LABEL = re.compile(r"^(head of cent(?:er|re)|cent(?:er|re) ?leader|centerleder)\s*:?\s*", re.I)
LOC_LABEL = re.compile(r"^location\s*:?\s*", re.I)


def former_rows() -> list[dict]:
    url = f"{BASE}/en/former-centers-of-excellence/"
    page = get(url)
    rows = []
    sections = re.findall(r'<button class="trigger"[^>]*>(.*?)</button>\s*<div class="accordion-content"[^>]*>(.*?)</div>',
                          page, re.S)
    for head_html, body in sections:
        head = text(head_html)  # "Centers established in 2012 - 7th Application round"
        ym = re.search(r"(\d{4})", head or "")
        rm = re.search(r"(\d+(?:st|nd|rd|th))\s+application round", head or "", re.I)
        for p in re.findall(r"<p>(.*?)</p>", body.replace("<p><p>", "<p>"), re.S):
            lines = [x for x in (text(y) for y in re.split(r"<br\s*/?>", p)) if x]
            if not lines:
                continue
            lead_i = next((i for i, x in enumerate(lines) if LEAD_LABEL.match(x)), None)
            loc_i = next((i for i, x in enumerate(lines) if LOC_LABEL.match(x)), None)
            if lead_i is None:
                # unlabelled leader line ("Professor Henrik Clausen") sits right before Location
                lead_i = loc_i - 1 if loc_i is not None and loc_i >= 2 else None
            # "Head of center: Joern Olsen, Location" followed by "Statens Serum Institut"
            if lead_i is not None and loc_i is None and re.search(r",\s*location\s*:?$", lines[lead_i], re.I):
                lines[lead_i] = re.sub(r",\s*location\s*:?$", "", lines[lead_i], flags=re.I)
                if lead_i + 1 < len(lines):
                    lines[lead_i + 1] = "Location: " + lines[lead_i + 1]
                    loc_i = lead_i + 1
            if lead_i is None or lead_i == 0:
                log(f"  WARNING: former-center block without a leader: {lines}")
                continue
            title = " ".join(lines[:lead_i])
            lead = clean_person(LEAD_LABEL.sub("", lines[lead_i]))
            given, family = split_name(lead or "")
            r = base_row()
            r.update({
                "programme": "Center of Excellence",
                "status": "former",
                "title": title,
                "acronym": acronym(title),
                "lead_name": lead,
                "lead_given_name": given,
                "lead_family_name": family,
                "host_institution": LOC_LABEL.sub("", lines[loc_i]) or None if loc_i is not None else None,
                "established_year": ym.group(1) if ym else None,
                "application_round": f"{rm.group(1)} Round" if rm else None,
                "landing_page_url": url,
                "source_key": f"former-{ym.group(1) if ym else 'x'}-{slugify(acronym(title) or title)[:60]}",
                "raw_fields": json.dumps({"round_heading": head, "lines": lines}, ensure_ascii=False),
            })
            rows.append(r)
    log(f"Former centers: {len(sections)} round sections, {len(rows)} centers")
    return rows


# ------------------------------------------------------- retired programmes
PROF_RE = re.compile(r"^(?:[A-Za-zæøå ]+,\s*)?Professor\s+(.+)$")


def retired_rows() -> list[dict]:
    rows = []
    for programme, url in RETIRED_PAGES:
        page = get(url)
        main = page[page.find("<main"): page.find("</main>")]
        main = re.sub(r"<script.*?</script>|<style.*?</style>", "", main, flags=re.S)
        lines = [x for x in (text(y) for y in re.split(r"<br\s*/?>|<p[^>]*>|</p>|</h\d>|</li>|</div>|</button>", main)) if x]
        i, found = 0, 0
        while i < len(lines):
            m = PROF_RE.match(lines[i])
            if not m or len(m.group(1).split()) > 6:
                i += 1
                continue
            name = m.group(1).strip()
            j, blk = i + 1, []
            while j < len(lines) and not PROF_RE.match(lines[j]) and len(blk) < 8:
                blk.append(lines[j])
                j += 1
            joined = " | ".join(blk)
            inst = re.search(r"Institution:\s*\|?\s*([^|]+)", joined)
            inst_dk = re.search(r"Institution i DK:\s*\|?\s*([^|]+)", joined)
            per = re.search(r"Bevillingsperiode:\s*\|?\s*([^|]+)", joined)
            if not per and not inst:
                i += 1
                continue
            s = e = None
            if per:
                pp = re.split(r"\s+[-–]\s+", re.sub(r"\(.*?\)", "", per.group(1)).strip())
                s = parse_da_date(pp[0])
                e = parse_da_date(pp[1], end=True) if len(pp) > 1 else None
            given, family = split_name(name)
            host = inst_dk.group(1) if inst_dk else (inst.group(1) if inst else None)
            r = base_row()
            r.update({
                "programme": programme,
                "status": "former",
                "title": f"{programme}: {name}",
                "lead_name": name,
                "lead_given_name": given,
                "lead_family_name": family,
                "host_institution": host.strip() if host else None,
                "start_date": s,
                "end_date": e,
                "landing_page_url": url,
                "source_key": f"{PROGRAMME_CODES[programme].lower()}-{slugify(name)}",
                "raw_fields": json.dumps({"lines": blk, "home_institution": inst.group(1).strip() if inst else None},
                                         ensure_ascii=False),
            })
            rows.append(r)
            found += 1
            i = j
        log(f"  {programme}: {found} grants")
    return rows


# ------------------------------------------------------------ award number
def load_crosswalk() -> dict[str, str]:
    """source_key -> DNRF<n>. Lines starting with '#' are the provenance header."""
    if not CROSSWALK.exists():
        return {}
    with open(CROSSWALK, newline="", encoding="utf-8") as fh:
        return {r["source_key"]: r["dnrf_number"]
                for r in csv.DictReader(l for l in fh if not l.startswith("#")) if r.get("dnrf_number")}


def main() -> None:
    ap = argparse.ArgumentParser(description="DNRF (dg.dk) grants -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="limit active detail-page fetches (smoke test)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = ap.parse_args()
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    active = active_rows(args.cache_dir, args.limit)
    former = former_rows()
    retired = retired_rows()
    log(f"Parsed: {len(active)} active, {len(former)} former centers, {len(retired)} retired-programme grants")
    if len(former) < 50:
        raise SystemExit(f"former-centers parse returned only {len(former)} rows; page layout changed?")

    # Active Niels Bohr professors also appear on the retired-programme page: keep the detail-page row.
    active_people = {(r["programme"], (r["lead_name"] or "").lower()) for r in active}
    retired = [r for r in retired if (r["programme"], r["lead_name"].lower()) not in active_people]
    # A center on the former list that still has a detail page: keep the detail-page row.
    active_keys = {(r["acronym"] or "").lower() for r in active if r["acronym"]} | {slugify(r["title"]) for r in active}
    former = [r for r in former
              if not ((r["acronym"] or "").lower() in active_keys or slugify(r["title"]) in active_keys)]

    df = pd.DataFrame(active + former + retired)
    xw = load_crosswalk()
    df["dnrf_number"] = df["source_key"].map(xw)
    df["funder_award_id"] = [
        n if isinstance(n, str) and n else f"DG-{PROGRAMME_CODES.get(p, 'X')}-{k.removeprefix('rest-')}"
        for n, p, k in zip(df["dnrf_number"], df["programme"], df["source_key"])
    ]
    df["currency"] = df["amount"].map(lambda a: "DKK" if pd.notna(a) else None)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, ['funder_award_id', 'title']].values.tolist()}")

    for c in ["title", "acronym", "lead_name", "host_institution", "start_date", "established_year",
              "amount", "description", "dnrf_number"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  programmes: {df['programme'].value_counts().to_dict()}")
    log(f"  total amount DKK {pd.to_numeric(df['amount']).sum():,.0f}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "dnrf_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook 1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_dnrf_projects.parquet"
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
