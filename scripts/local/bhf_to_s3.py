#!/usr/bin/env python3
"""
British Heart Foundation (BHF) to S3 Data Pipeline (multi-era PDF archive)
=========================================================================

BHF does NOT publish a grants API, CSV export, or per-grant landing pages.
It publishes one **annual PDF report** of "Research Grant Awards" per
financial year (1 April -> 31 March), back to 2004/2005, at:

    https://www.bhf.org.uk/for-professionals/information-for-researchers/previous-awards

Each PDF lists every new award made that year as an implicit table:

    Reference number | Name | Institution | Grant title | Total

grouped under **scheme/committee headings** (Fellowships, Project Grants,
Programme Grants, Clinical Study Grants, Translational Awards, ...). There
are no ruled table lines, so we recover the table by **word x-coordinates**
(PyMuPDF `get_text("words")`), clustering words into visual rows and binning
each row's words into columns by x against cut points taken from the
repeated "Reference number ... Total" header row.

This is a NEW pattern in the awards pipeline: a **multi-era PDF archive**.
The layout drifts across the 20-year span, so the parser auto-adapts per
PDF rather than hardcoding one geometry:

  * MODERN era (~2015/16 -> present): single-column table; reference like
    `FS/SBSRF/24/31040` (scheme code embedded); duration in MONTHS; scheme
    also in the heading.
  * MID era (~2008 -> 2014): single-column table; reference like
    `FS/13/16/30199` (NO scheme code -> heading is the only scheme source);
    PI names carry degree suffixes (`BSc PhD MA`); duration in YEARS.
  * OLD era (~2004 -> 2007): TWO-column page layout (two independent record
    streams side by side); reference like `FS/04/082/17531`.

Because the columns and eras differ, the parser:
  1. clusters words into visual rows by y (tolerance merges the common
     half-row offset where the Name sits ~1pt above its Reference row);
  2. detects 1- vs 2-column pages by clustering the x of reference tokens,
     and splits two-column pages into independent left/right streams;
  3. derives column cut points from the most recent header row per stream;
  4. tracks the running scheme/committee heading as parser state and tags
     every record beneath it (the only scheme source for the MID/OLD eras);
  5. reassembles reference numbers that wrap across two physical lines
     (`FS/CRERF/` + `DJT/25/22504`; `FS/13/16/` + `30199`);
  6. strips honorifics + degree suffixes from PI names (initials-only given
     names throughout; no ORCIDs are published).

Source authority
----------------
bhf.org.uk is the awarding body's own site (public PDFs, no auth). No
Wikipedia/Wikidata.

Schema notes / known limitations
--------------------------------
  * `currency` = GBP, hardcoded (single-country funder).
  * PI names are **initials-only** (`Dr H F Jorgensen`) and there are **no
    ORCIDs** -> `lead_investigator.given_name` is initials, `orcid` NULL.
  * No abstracts are published -> `description` NULL.
  * No exact per-grant dates -> we publish `start_year` = the report's
    financial-year start (the award was *made* during that FY); `start_date`
    is left NULL to avoid false day/month precision. `end_year` is derived
    from the published duration when present.
  * `funder_scheme` = the captured heading; `funding_type` is derived from
    it (Fellowship/Studentship -> fellowship/training, else research).

Output
------
s3://openalex-ingest/awards/bhf/bhf_projects.parquet

Usage
-----
    python bhf_to_s3.py                    # full run (default)
    python bhf_to_s3.py --skip-upload      # local dev / smoke test
    python bhf_to_s3.py --skip-download    # reuse cached PDFs
    python bhf_to_s3.py --limit 3          # smoke-test on 3 PDFs
    python bhf_to_s3.py --allow-shrink     # override §1.4

Requirements
------------
    pip install pandas pyarrow requests boto3 pymupdf
    AWS CLI configured for s3://openalex-ingest/awards/bhf/
"""

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22, runbook §1.2 #7) ---
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


# =============================================================================
# Configuration
# =============================================================================

# Awarding body — British Heart Foundation. Verified F4320319992, country GB.
FUNDER_ID = 4320319992
FUNDER_DISPLAY_NAME = "British Heart Foundation"

PROVENANCE = "bhf_annual_reports"
CURRENCY = "GBP"  # Hardcoded — BHF is UK-only

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/bhf/bhf_projects.parquet"

AWARDS_PAGE = ("https://www.bhf.org.uk/for-professionals/"
               "information-for-researchers/previous-awards")

USER_AGENT = "openalex-walden-bhf-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.5

# The 20 annual PDFs, newest -> oldest. Each tuple is (report_period, url).
# report_period's first year is the FY start (award "made during" 1 Apr that year).
# Scraped from the previous-awards page 2026-06-22; the page is updated yearly,
# so re-runs should re-scrape PDF_INDEX rather than trust this static list (see
# discover_pdf_urls() — this list is the fallback / cross-check).
PDF_LIST = [
    ("2024/2025", "https://www.bhf.org.uk/-/media/files/for-professionals/research-grant-awards-2024-2025.pdf"),
    ("2023/2024", "https://www.bhf.org.uk/-/media/files/for-professionals/r13137_research_report_2024_final_rgbpdf.pdf"),
    ("2022/2023", "https://www.bhf.org.uk/-/media/files/for-professionals/research-grant-awards-2022_2023.pdf"),
    ("2021/2022", "https://www.bhf.org.uk/-/media/files/for-professionals/research-grant-awards-2021_2022.pdf"),
    ("2020/2021", "https://www.bhf.org.uk/-/media/files/for-professionals/research/com121-aromd-report-21-final.pdf"),
    ("2019/2020", "https://www.bhf.org.uk/-/media/files/for-professionals/research/com102-research-grant-report-7.pdf"),
    ("2018/2019", "https://www.bhf.org.uk/-/media/files/for-professionals/research/bhf076_research-grants-19_aw.pdf"),
    ("2017/2018", "https://www.bhf.org.uk/-/media/files/for-professionals/research/bhf070_research-grants-18_aw.pdf"),
    ("2016/2017", "https://www.bhf.org.uk/-/media/files/for-professionals/research/bhf052_research-grants-2017_single-pages.pdf"),
    ("2015/2016", "https://www.bhf.org.uk/-/media/files/for-professionals/research/bhf_research-grants-2016.pdf"),
    ("2014/2015", "https://www.bhf.org.uk/-/media/files/for-professionals/research/bhf-grant-awards-2014-15_v1.pdf"),
    ("2013/2014", "https://www.bhf.org.uk/-/media/files/information-and-support/publications/research/research-grant-awards-2013-14.pdf"),
    ("2012/2013", "https://www.bhf.org.uk/-/media/files/information-and-support/publications/research/research-grant-awards-2012_13.pdf"),
    ("2011/2012", "https://www.bhf.org.uk/-/media/files/information-and-support/publications/research/research-grant-awards-2011_2012.pdf"),
    ("2010/2011", "https://www.bhf.org.uk/-/media/files/information-and-support/publications/research/research-grant-awards-2010_11.pdf"),
    ("2009/2010", "https://www.bhf.org.uk/-/media/files/information-and-support/publications/research/research-grant-awards-2009_10.pdf"),
    ("2008/2009", "https://www.bhf.org.uk/-/media/files/information-and-support/publications/research/research-grant-awards-2008_2009.pdf"),
    ("2007/2008", "https://www.bhf.org.uk/-/media/files/information-and-support/publications/research/research-grant-awards-2007_2008.pdf"),
    ("2006/2007", "https://www.bhf.org.uk/-/media/files/information-and-support/publications/research/research-grant-awards-2006_2007.pdf"),
    ("2004/2005", "https://www.bhf.org.uk/-/media/files/information-and-support/publications/grants/grant-awards-2004_5.pdf"),
]


# =============================================================================
# Regexes / constants
# =============================================================================

# A reference number is 2+ uppercase letters then "/" then alnum, e.g.
# FS/SBSRF/24/31040, PG/24/123, RG/F/22/1, FS/13/16/30199, BS/04/002/16787.
REF_RE = re.compile(r'^[A-Z]{2,}/[A-Za-z0-9]')
# A pure reference-continuation token (second physical line of a wrapped ref):
# trailing id like "30199", or "DJT/25/22504" (path fragment, no leading word).
REF_CONT_RE = re.compile(r'^(\d{3,}|[A-Z]{2,}/\d)')
HEADER_RE = re.compile(r'reference\s*number', re.I)
AMOUNT_RE = re.compile(r'£\s?[\d,]+(?:\.\d+)?')
# Lines that are never headings (running header/footer, intro boilerplate).
NOISE_RE = re.compile(
    r'(british heart foundation research grant awards'
    r'|^bhf\b|^awards made|^listed|^contents$|^introduction$'
    r'|^non-clinical|^clinical fellowships$|^\d+\s*$'
    r'|^\d+\s+april|^\d+\s+\w+\s+\d{4})', re.I)
# Marks the start of the ingestable section (skip cover/contents/intro/chairs).
START_RE = re.compile(r'awards made during the year', re.I)
# Duration: "60 months", "5 years", "3 yrs".
DUR_MONTHS_RE = re.compile(r'(\d+)\s*months?\b', re.I)
DUR_YEARS_RE = re.compile(r'(\d+)\s*(?:years?|yrs?)\b', re.I)

# Name parsing — honorifics + degree/qualification suffixes to strip.
_PREFIX_TITLES = {"dr", "prof", "professor", "mr", "mrs", "ms", "miss",
                  "sir", "dame", "lord", "lady", "rev", "revd"}
_DEGREE_SUFFIXES = {
    "phd", "md", "dphil", "dsc", "scd", "msc", "ma", "ba", "bs", "bsc", "bm", "bch",
    "bchir", "mb", "mbbs", "mbchb", "chb", "bmedsci", "bmbs", "mbbch",
    "frcp", "frcs", "frcpath", "frcpe", "frceng", "frs", "frcr", "frca",
    "fmedsci", "facc", "faha", "fesc", "frcpsych", "mrcp", "mrcs", "mrcpch",
    "dm", "do", "edd", "llb", "llm", "mphil", "pgce", "rgn", "rn", "msci",
    "jr", "sr", "jr.", "sr.", "ii", "iii", "iv", "frcgp", "fhea", "mrcgp",
    "dnbe", "ches", "bvsc", "bvms", "mrcvs", "vmd", "frse", "mres", "frsb",
    "frsc", "fba", "frcophth", "facss", "msci", "meng", "beng", "bm",
    # UK honours (post-nominal, not part of the name)
    "cbe", "obe", "mbe", "kbe", "dbe", "gbe",
}

# Placeholder "names" for unfilled studentship/fellowship slots -> NULL the PI.
_PLACEHOLDER_NAME_RE = re.compile(
    r'\b(to be (appointed|confirmed|announced|recruited|named)'
    r'|student to be|not yet|tbc|tba|n/?a)\b', re.I)


def split_name(full: str) -> tuple[str, str]:
    """(given, family) with honorifics + degree suffixes stripped.

    BHF gives initials-only given names (`Dr H F Jorgensen` -> given='H F',
    family='Jorgensen'). Mid-era names carry degrees (`Dr K A Dora BSc PhD MA`)
    and parentheticals (`Prof A Baker BSc (Hons) PhD FAHA`).
    Canonical wolf_to_s3.py shape; the suffix set is the load-bearing part.
    """
    # Drop parentheticals ("(Hons)") and trailing degree clutter.
    s = re.sub(r'\([^)]*\)', " ", full).replace(",", " ")
    tokens = [t for t in s.split() if t]
    # strip leading honorifics
    while tokens and tokens[0].rstrip(".").lower() in _PREFIX_TITLES:
        tokens.pop(0)
    # strip trailing degree suffixes (repeatedly: "BSc PhD MA")
    while tokens and tokens[-1].rstrip(".").lower() in _DEGREE_SUFFIXES:
        tokens.pop()
    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return ("", tokens[0])
    return (" ".join(tokens[:-1]), tokens[-1])


def funding_type_for(scheme: Optional[str]) -> str:
    """Map a BHF scheme heading to an OpenAlex funding_type."""
    s = (scheme or "").lower()
    if "studentship" in s or "phd" in s:
        return "training"
    if "fellowship" in s:
        return "fellowship"
    return "research"


# =============================================================================
# HTTP / download
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 90) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    return resp


def discover_pdf_urls() -> list[tuple[str, str]]:
    """Re-scrape the previous-awards page for (period, url) pairs.

    The page is updated yearly with a new PDF, so prefer the live list over
    the static PDF_LIST fallback. Returns PDF_LIST if discovery finds fewer
    rows than the fallback (anti-bot blip / markup change — never shrink).
    """
    print("\n" + "=" * 60)
    print("Step 0: Discover annual PDF links from previous-awards page")
    print("=" * 60)
    try:
        resp = _http_get(AWARDS_PAGE)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        print(f"  [WARN] discovery failed ({e}); using static PDF_LIST")
        return PDF_LIST
    # Links look like: href="/-/media/files/.../...pdf?rev=...&hash=..."
    # with nearby text "Grant Awards 2024/2025".
    pairs: list[tuple[str, str]] = []
    seen = set()
    for m in re.finditer(
        r'href="(?P<href>[^"]+\.pdf[^"]*)"[^>]*>(?P<text>[^<]*?(?P<yr>\d{4})\s*[/\-]\s*\d{2,4}[^<]*)</a>',
        html, re.I,
    ):
        href = m.group("href")
        if href.startswith("/"):
            href = "https://www.bhf.org.uk" + href
        ym = re.search(r'(\d{4})\s*[/\-]\s*(\d{2,4})', m.group("text"))
        if not ym:
            continue
        y1 = ym.group(1)
        y2 = ym.group(2)
        if len(y2) == 2:
            y2 = y1[:2] + y2
        period = f"{y1}/{y2}"
        if period in seen:
            continue
        seen.add(period)
        pairs.append((period, href))
    print(f"  discovered {len(pairs)} PDF links on page")
    if len(pairs) < len(PDF_LIST):
        print(f"  [WARN] discovery found {len(pairs)} < {len(PDF_LIST)} fallback "
              f"links; using static PDF_LIST (never shrink the archive)")
        return PDF_LIST
    return sorted(pairs, key=lambda p: p[0], reverse=True)


def download_pdfs(pdfs: list[tuple[str, str]], pdf_dir: Path,
                  skip_download: bool) -> list[tuple[str, Path]]:
    """Download each PDF to pdf_dir/bhf-<period>.pdf. Returns (period, path)."""
    print("\n" + "=" * 60)
    print(f"Step 1: Download {len(pdfs)} annual PDFs")
    print("=" * 60)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    out: list[tuple[str, Path]] = []
    for i, (period, url) in enumerate(pdfs, 1):
        slug = period.replace("/", "-")
        path = pdf_dir / f"bhf-{slug}.pdf"
        if skip_download and path.exists():
            print(f"  [{i}/{len(pdfs)}] [cached] {path.name} ({path.stat().st_size//1024} KB)")
            out.append((period, path))
            continue
        try:
            resp = _http_get(url)
            if resp.status_code != 200:
                print(f"  [{i}/{len(pdfs)}] {period}: HTTP {resp.status_code} — SKIP")
                continue
            path.write_bytes(resp.content)
            print(f"  [{i}/{len(pdfs)}] {period}: {len(resp.content)//1024} KB -> {path.name}")
            out.append((period, path))
        except Exception as e:
            print(f"  [{i}/{len(pdfs)}] {period}: download error {e} — SKIP")
    print(f"\n  Have {len(out)}/{len(pdfs)} PDFs")
    return out


# =============================================================================
# PDF parsing (coordinate-based, multi-era)
# =============================================================================
#
# Three layout families are handled (see module docstring):
#   * header-columnar   (2015/16-2024/25): "Reference number ... Total" header
#                        rows give exact column cuts; 1 or 2 columns/page.
#   * headerless-columnar (2006/07-2014/15): no header row, but stable columns;
#                        cuts detected from the word-x histogram (gutters).
#   * stacked two-column (2004/2005): each record is stacked VERTICALLY (ref /
#                        name / institution / title / amount on separate lines),
#                        two records side by side -> dedicated parser.

Y_TOL = 4.0  # words within this many points of y are the same visual row
AMT_TOK_RE = re.compile(r'£|^[\d,]+\.\d{2}$')  # money-shaped token (route by content)


def _cluster_rows(words: list) -> list[list]:
    """Group PyMuPDF words into visual rows by y (tolerance merges the
    Name-above-Reference half-row offset). Each row sorted left->right."""
    rows: list[list] = []
    for w in sorted(words, key=lambda w: (round(w[1], 1), w[0])):
        y = w[1]
        if rows and abs(y - rows[-1][0]) <= Y_TOL:
            rows[-1][1].append(w)
        else:
            rows.append([y, [w]])
    return [sorted(r[1], key=lambda w: w[0]) for r in rows]


def _split_gutter(words: list, page_width: float) -> Optional[float]:
    """Detect a two-column page and return the inter-column gutter x, else None.

    A page is two-column ONLY when reference numbers appear in two
    well-separated x-clusters (>150pt apart). Keying on the reference column —
    not on the widest whitespace — avoids the classic false positive where the
    intra-row name->institution gap (~75pt) is mistaken for a column gutter and
    shreds every single-column page. Once two reference columns are confirmed,
    the split is the widest word-gap in the empty band between them."""
    ref_xs = sorted(w[0] for w in words if REF_RE.match(w[4]))
    if len(ref_xs) < 4:
        return None
    # cluster reference x-starts; a gap >150pt marks a second column
    clusters: list[list[float]] = [[ref_xs[0]]]
    for x in ref_xs[1:]:
        if x - clusters[-1][-1] > 150:
            clusters.append([x])
        else:
            clusters[-1].append(x)
    clusters = [c for c in clusters if len(c) >= 2]
    if len(clusters) < 2:
        return None
    cL = sum(clusters[0]) / len(clusters[0])
    cR = sum(clusters[1]) / len(clusters[1])
    # split in the gutter between the left column's rightmost content and the
    # right column's reference start.
    left_max = max((w[0] for w in words if w[0] < cR - 60), default=cL)
    return (left_max + cR) / 2


def _header_cuts(row: list) -> Optional[list[float]]:
    """From a 'Reference number Name Institution Grant title Total' row,
    return [x_name, x_inst, x_title, x_total] cut points (absolute x)."""
    def find(*names):
        for w in row:
            if w[4].lower() in names:
                return w[0]
        return None
    x_name = find("name", "applicant", "recipient")
    x_inst = find("institution", "university", "department")
    x_title = find("grant", "title", "project")
    x_total = find("total", "amount", "value")
    if None in (x_name, x_inst, x_title, x_total):
        return None
    return [x_name, x_inst, x_title, x_total]


def _detect_cuts(rows: list[list], ref_x: float) -> Optional[list[float]]:
    """Derive [x_name, x_inst, x_title, x_total] for a headerless columnar
    stream from the word-x-start histogram: find the gutters (empty x bands)
    that separate ref|name|institution|title|amount columns."""
    # word x-starts to the right of the reference column
    xs = sorted(w[0] for r in rows for w in r if w[0] > ref_x + 8)
    amt_xs = sorted(w[0] for r in rows for w in r if AMT_TOK_RE.search(w[4]))
    if len(xs) < 30 or not amt_xs:
        return None
    amt_x = amt_xs[len(amt_xs) // 2]  # median amount x = the Total column
    # gutters: gaps in the x-start distribution between ref and amount columns
    body = [x for x in xs if x < amt_x - 5]
    gaps = []  # (gap_width, midpoint)
    for i in range(1, len(body)):
        g = body[i] - body[i - 1]
        if g >= 12:
            gaps.append((g, (body[i] + body[i - 1]) / 2))
    gaps.sort(reverse=True)
    # need at least 2 internal gutters (name|inst, inst|title); name starts
    # right after ref so name boundary ~ first column start.
    cut_mids = sorted(m for _, m in gaps[:3])
    if len(cut_mids) < 2:
        return None
    name_x = min(x for x in xs)            # name column start (first cluster)
    # choose inst and title cuts from the detected gutters
    inst_x = cut_mids[0]
    title_x = cut_mids[1] if len(cut_mids) >= 2 else (inst_x + amt_x) / 2
    return [name_x - 4, inst_x, title_x, amt_x - 8]


def _bin_row(row: list, cuts: list[float]) -> dict:
    """Assign a row's words to columns by x against cut points. Money-shaped
    tokens go to amt by CONTENT regardless of x (right-aligned amounts render
    left of the 'Total' header, so position alone misfiles them into title)."""
    x_name, x_inst, x_title, x_total = cuts
    cols = {"ref": [], "name": [], "inst": [], "title": [], "amt": []}
    for w in row:
        x, t = w[0], w[4]
        if AMT_TOK_RE.search(t):
            cols["amt"].append(t)
        elif x < x_name - 5:
            cols["ref"].append(t)
        elif x < x_inst - 5:
            cols["name"].append(t)
        elif x < x_title - 5:
            cols["inst"].append(t)
        else:
            cols["title"].append(t)
    return cols


# Honorific that can bleed onto the end of a reference token when the name
# column starts tight against the reference column (e.g. "FS/06/061Prof").
_REF_TAIL_HONORIFIC = re.compile(r'(Dr|Prof|Professor|Mr|Mrs|Ms|Miss|Sir|Dame|Rev)\.?$')


def _new_record(cols: dict, text: str, scheme, period, url) -> dict:
    ref = "".join(cols["ref"])
    m = _REF_TAIL_HONORIFIC.search(ref)
    honorific = ""
    if m:
        honorific = m.group(0)
        ref = ref[:m.start()]
    name = " ".join(cols["name"]).strip()
    if honorific and not name.lower().startswith(honorific.lower()):
        name = (honorific + " " + name).strip()
    amt_m = AMOUNT_RE.search(text)
    return {
        "ref": ref,
        "name": name,
        "inst": " ".join(cols["inst"]).strip(),
        "title": " ".join(cols["title"]).strip(),
        "amt": amt_m.group(0).replace(" ", "") if amt_m else "",
        "scheme": scheme,
        "period": period,
        "url": url,
    }


def _parse_stream(rows: list[list], period: str, url: str,
                  scheme: Optional[str], cuts: Optional[list[float]]
                  ) -> tuple[list[dict], Optional[str], Optional[list[float]]]:
    """Parse one columnar stream of visual rows into records. `scheme` and
    `cuts` are threaded in from the previous page (a scheme/table often spans
    pages) and returned so the caller can carry them forward. Header rows
    refine cuts and the scheme when present."""
    recs: list[dict] = []
    pending: list[str] = []
    ref_x = min((r[0][0] for r in rows if REF_RE.match(r[0][4])), default=36.0)

    for row in rows:
        first = row[0][4]
        x0 = row[0][0]
        text = " ".join(w[4] for w in row)

        if HEADER_RE.search(text):
            nc = _header_cuts(row)
            if nc:
                cuts = nc
            if pending:
                scheme = pending[-1].strip()
                pending = []
            continue

        if cuts is None:
            cuts = [ref_x + 75, ref_x + 160, ref_x + 270, ref_x + 430]
        name_cut = cuts[0]

        # record line: first token is a full reference number, at ref column
        if REF_RE.match(first) and x0 < name_cut - 5:
            if pending:
                scheme = pending[-1].strip()
                pending = []
            recs.append(_new_record(_bin_row(row, cuts), text, scheme, period, url))
            continue

        # reference-continuation line: completes a wrapped ref id
        if recs and x0 < name_cut - 5 and REF_CONT_RE.match(first) \
                and (recs[-1]["ref"].endswith("/") or not AMOUNT_RE.search(recs[-1]["amt"])):
            cols = _bin_row(row, cuts)
            recs[-1]["ref"] += "".join(cols["ref"])
            for k in ("name", "inst", "title"):
                if cols[k]:
                    recs[-1][k] = (recs[-1][k] + " " + " ".join(cols[k])).strip()
            if cols["amt"] and not recs[-1]["amt"]:
                recs[-1]["amt"] = "".join(cols["amt"])
            continue

        # continuation line: append wrapped name / title / amount to last record
        if recs and x0 >= name_cut - 5:
            cols = _bin_row(row, cuts)
            if cols["name"]:
                if not recs[-1]["name"]:
                    recs[-1]["name"] = " ".join(cols["name"]).strip()
                elif recs[-1]["name"].endswith("-"):
                    recs[-1]["name"] = (recs[-1]["name"][:-1] + " ".join(cols["name"])).strip()
                else:
                    recs[-1]["name"] = (recs[-1]["name"] + " " + " ".join(cols["name"])).strip()
            if cols["inst"]:
                # institution wrapped to a second line ("University of\nLeeds").
                # These words sit in the institution column by x, so they are
                # genuine overflow (title overflow stays in the title column).
                recs[-1]["inst"] = (recs[-1]["inst"] + " " + " ".join(cols["inst"])).strip()
            if cols["title"]:
                recs[-1]["title"] = (recs[-1]["title"] + " " + " ".join(cols["title"])).strip()
            if cols["amt"] and not recs[-1]["amt"]:
                recs[-1]["amt"] = "".join(cols["amt"])
            continue

        # heading candidate
        if x0 < name_cut + 20 and not NOISE_RE.search(text) and len(text) > 3:
            pending.append(text.strip())

    return recs, scheme, cuts


def _parse_stacked(rows: list[list], period: str, url: str,
                   scheme: Optional[str]) -> tuple[list[dict], Optional[str]]:
    """Parse a STACKED record stream (2004/2005): ref alone on a line, then
    name / institution / title lines / 'N years £amount' line, until the next
    ref or a heading. One column at a time (caller splits two columns)."""
    recs: list[dict] = []
    cur: Optional[dict] = None

    def close():
        nonlocal cur
        if cur and cur["ref"]:
            cur["title"] = " ".join(cur["_title"]).strip()
            del cur["_title"]
            recs.append(cur)
        cur = None

    for row in rows:
        text = " ".join(w[4] for w in row).strip()
        first = row[0][4]
        # new record: a line whose first token is a reference number
        if REF_RE.match(first):
            close()
            ref = first
            cur = {"ref": ref, "name": "", "inst": "", "_title": [],
                   "amt": "", "scheme": scheme, "period": period, "url": url}
            continue
        if cur is None:
            # between records: heading text sets the scheme
            if not NOISE_RE.search(text) and len(text) > 3 and not AMOUNT_RE.search(text):
                scheme = text
            continue
        amt_m = AMOUNT_RE.search(text)
        # name line: starts with honorific / initials and no amount
        if not cur["name"] and re.match(r'^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss|Sir|Dame)\b', text):
            cur["name"] = text
            continue
        # institution line (no amount, has an org keyword), only if title empty
        if not cur["_title"] and not amt_m and re.search(
                r'university|college|institute|hospital|trust|imperial|school', text, re.I):
            cur["inst"] = (cur["inst"] + " " + text).strip()
            continue
        # amount / duration line closes the record's data
        if amt_m:
            cur["amt"] = amt_m.group(0).replace(" ", "")
            # strip the "N years £x" tail; keep any leading title words
            lead = AMOUNT_RE.split(text)[0]
            lead = DUR_YEARS_RE.sub("", DUR_MONTHS_RE.sub("", lead)).strip()
            if lead:
                cur["_title"].append(lead)
            continue
        # otherwise: title text
        cur["_title"].append(text)
    close()
    return recs, scheme


def parse_pdf(period: str, path: Path, url: str) -> list[dict]:
    """Parse one annual PDF into raw records (auto-detects layout family)."""
    import fitz  # PyMuPDF
    doc = fitz.open(str(path))
    stacked = (period == "2004/2005")  # the sole stacked-layout report

    # First locate the award pages (everything from "Awards made..." onward),
    # dropping the running header/footer band.
    award_pages: list[list] = []
    started = False
    for pno in range(doc.page_count):
        page = doc[pno]
        words = page.get_text("words")
        if not words:
            continue
        if not started:
            if START_RE.search(page.get_text()):
                started = True
            else:
                continue
        ph = page.rect.height
        words = [w for w in words if 50 < w[1] < ph - 42]
        if words:
            award_pages.append((words, page.rect.width))
    doc.close()

    # Calibrate column cuts ONCE per PDF for the headerless single-column eras
    # (per-page detection is unstable on sparse pages). Header-era PDFs get
    # their cuts from the in-stream header rows, so leave init cuts None there.
    has_header = any(HEADER_RE.search(" ".join(w[4] for w in pg)) for pg, _ in award_pages)
    init_cuts: Optional[list[float]] = None
    if not has_header and not stacked and award_pages:
        all_words = [w for pg, _ in award_pages for w in pg]
        ref_x = min((w[0] for w in all_words if REF_RE.match(w[4])), default=36.0)
        init_cuts = _detect_cuts(_cluster_rows(all_words), ref_x)

    all_recs: list[dict] = []
    scheme: Optional[str] = None       # threaded across pages (schemes span pages)
    # cuts are threaded PER STREAM INDEX: in a two-column page the left and
    # right columns have different absolute x, so a continuation page's right
    # column must keep the right column's cuts, not inherit the left's.
    cuts_by_stream: dict[int, Optional[list[float]]] = {0: init_cuts, 1: None}
    for words, pw in award_pages:
        gutter = _split_gutter(words, pw)
        if gutter:
            streams = [_cluster_rows([w for w in words if w[0] < gutter]),
                       _cluster_rows([w for w in words if w[0] >= gutter])]
        else:
            streams = [_cluster_rows(words)]
        for i, st in enumerate(streams):
            if stacked:
                recs, scheme = _parse_stacked(st, period, url, scheme)
                all_recs += recs
            else:
                recs, scheme, cuts_by_stream[i] = _parse_stream(
                    st, period, url, scheme, cuts_by_stream.get(i))
                all_recs += recs
    return all_recs


# =============================================================================
# Build DataFrame
# =============================================================================

def _clean_amount(raw: str) -> Optional[str]:
    if not raw:
        return None
    m = re.search(r'[\d,]+(?:\.\d+)?', raw)
    if not m:
        return None
    val = m.group(0).replace(",", "")
    try:
        f = float(val)
    except ValueError:
        return None
    if f <= 0:
        return None
    return f"{f:.2f}"


def _duration_months(title: str) -> Optional[int]:
    m = DUR_MONTHS_RE.search(title)
    if m:
        return int(m.group(1))
    y = DUR_YEARS_RE.search(title)
    if y:
        return int(y.group(1)) * 12
    return None


def _clean_title(title: str) -> str:
    # strip the trailing duration token that the source appends to the title
    t = DUR_MONTHS_RE.sub("", title)
    t = DUR_YEARS_RE.sub("", t)
    return re.sub(r'\s+', " ", t).strip(" .")


# Institution names sometimes spill across the inst/title column cut, leaving a
# truncated stem ("University", "University of") in inst and the completion at
# the front of the title ("of Cambridge", "College London"). Pull it back.
# A "place" is one capitalized token, the explicit two-word UK places, or a
# "the X" form — deliberately NOT greedy past that, so a following capitalized
# title word ("...Cambridge Structural mechanisms") is not swallowed.
_PLACE = r"(?:East Anglia|the [A-Z][\w’\-]+|[A-Z][\w’\-]+)"
_INST_STEM_RE = re.compile(r'(University|College|Imperial|King’?s|Queen’?s?)$', re.I)
_COMPLETION_OF_RE = re.compile(rf'^({_PLACE})')                 # after "University of"
_COMPLETION_STEM_RE = re.compile(rf'^(of {_PLACE}|College London|College|London)')


def _repair_institution(inst: Optional[str], title: str) -> tuple[Optional[str], str]:
    if not inst or not title:
        return inst, title
    stem = inst.rstrip()
    t = title.strip()
    if stem.endswith(" of"):
        m = _COMPLETION_OF_RE.match(t)
    elif _INST_STEM_RE.search(stem):
        m = _COMPLETION_STEM_RE.match(t)
    else:
        return inst, title
    if not m:
        return inst, title
    return (stem + " " + m.group(0)).strip(), t[m.end():].strip()


def _clean_scheme(scheme: Optional[str]) -> Optional[str]:
    if not scheme:
        return None
    # drop the "(continued)" page-break artifact so a multi-page scheme is one
    s = re.sub(r'\s*\(continued\)\s*$', "", scheme, flags=re.I).strip()
    return s or None


def build_dataframe(records: list[dict]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)
    rows: list[dict] = []
    seen: set[str] = set()
    dropped_noref = 0
    for r in records:
        ref = re.sub(r'\s+', "", r["ref"]).strip()
        if not ref or not REF_RE.match(ref):
            dropped_noref += 1
            continue
        period = r["period"]
        fy_start = period.split("/")[0]
        # funder_award_id namespaced by period to guarantee global uniqueness
        # (a ref can in principle recur across reports; supplements relist).
        funder_award_id = f"{ref}"
        if funder_award_id in seen:
            # same grant listed in two reports (e.g. supplement) — keep first
            continue
        seen.add(funder_award_id)
        raw_name = r["name"] or ""
        if _PLACEHOLDER_NAME_RE.search(raw_name):
            raw_name = ""  # unfilled studentship/fellowship slot
        given, family = split_name(raw_name) if raw_name else ("", "")
        # guard: an institution word in the family slot means a mis-binned row
        if (family or "").lower() in {"university", "college", "institute",
                                       "hospital", "trust", "imperial"}:
            given, family, raw_name = "", "", ""
        inst, title_fixed = _repair_institution(r["inst"], r["title"])
        dur = _duration_months(title_fixed)
        rows.append({
            "funder_award_id":  funder_award_id,
            "reference_number": ref,
            "report_period":    period,
            "report_year_start": fy_start,
            "funder_scheme":    _clean_scheme(r["scheme"]),
            "title":            _clean_title(title_fixed),
            "amount":           _clean_amount(r["amt"]),
            "currency":         CURRENCY if _clean_amount(r["amt"]) else None,
            "duration_months":  str(dur) if dur is not None else None,
            "lead_full_name":   raw_name or None,
            "lead_given_name":  given or None,
            "lead_family_name": family or None,
            "institution":      inst or None,
            "funding_type":     funding_type_for(r["scheme"]),
            "landing_page_url": AWARDS_PAGE,
            "source_pdf_url":   r["url"],
            "declined":         False,
        })
    df = pd.DataFrame.from_records(rows)
    if dropped_noref:
        print(f"  dropped {dropped_noref} rows with no valid reference number")
    n = len(df)
    if n:
        n_amt = df["amount"].notna().sum()
        n_sch = df["funder_scheme"].notna().sum()
        n_inst = df["institution"].notna().sum()
        n_pi = df["lead_family_name"].notna().sum()
        n_dur = df["duration_months"].notna().sum()
        print(f"  rows: {n}")
        print(f"    amount:      {n_amt} ({n_amt*100//n}%)")
        print(f"    scheme:      {n_sch} ({n_sch*100//n}%)")
        print(f"    institution: {n_inst} ({n_inst*100//n}%)")
        print(f"    PI family:   {n_pi} ({n_pi*100//n}%)")
        print(f"    duration:    {n_dur} ({n_dur*100//n}%)")
        print(f"\n  By report period:")
        print(df.groupby("report_period").size().sort_index(ascending=False).to_string())
    # Runbook §1.2.5 — cast all columns to string before parquet.
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "bhf_projects.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df)} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 required for §1.4 shrink-check; rerun with --skip-upload to bypass"
        ) from exc
    client = boto3.client("s3")
    print(f"  §1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet — first ingest, no shrink check.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_bhf_projects.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        print(f"    [ERROR] couldn't read existing parquet ({e}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    print(f"    previous: {prev_count}   new: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            print("    [OVERRIDE] --allow-shrink set; proceeding.")
            return True
        print(f"\n[ERROR] §1.4 violation: refusing to shrink ({prev_count} -> {new_count}).")
        return False
    print("    [OK] not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3 (with §1.4 shrink check)")
    print("=" * 60)
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  Uploading {parquet_path} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        print(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        print("[ERROR] aws CLI not found.")
        return False
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] aws s3 cp failed (exit {e.returncode}).")
        return False


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/bhf"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse PDFs already in output-dir/pdfs")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N PDFs (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir = args.output_dir / "pdfs"
    print("=" * 60)
    print("British Heart Foundation → S3 Pipeline (multi-era PDF archive)")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    pdfs = discover_pdf_urls()
    if args.limit:
        pdfs = pdfs[:args.limit]
        print(f"  [LIMIT] only processing first {args.limit} PDFs")

    have = download_pdfs(pdfs, pdf_dir, args.skip_download)
    if not have:
        print("[ERROR] no PDFs downloaded — aborting")
        sys.exit(6)

    print("\n" + "=" * 60)
    print(f"Parsing {len(have)} PDFs")
    print("=" * 60)
    all_records: list[dict] = []
    url_by_period = {p: u for p, u in pdfs}
    for period, path in have:
        recs = parse_pdf(period, path, url_by_period.get(period, ""))
        print(f"  {period}: {len(recs)} records")
        all_records.extend(recs)

    # cache raw records for debugging / re-runs
    (args.output_dir / "bhf_records_raw.json").write_text(
        json.dumps(all_records, ensure_ascii=False, indent=2))

    df = build_dataframe(all_records)
    if df.empty:
        print("[ERROR] no records parsed — aborting before write")
        sys.exit(6)
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload; manual upload command:")
        print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
    else:
        ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
        if not ok:
            sys.exit(7)

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print("Next: notebooks/awards/CreateBHFAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
