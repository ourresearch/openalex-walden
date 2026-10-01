#!/usr/bin/env python3
"""
Alberta Innovates to S3 Data Pipeline
=====================================

Alberta Innovates (the Alberta provincial research and innovation agency)
publishes its funded projects at https://albertainnovates.ca/impact/funded-projects/
(WordPress custom post type `project`, ~635 projects; X-WP-Total on
/wp-json/wp/v2/project). Each project page (/projects/{slug}/) carries a
header description and a "Project at a Glance" block:

    Funding Recipient, Website, Funding Awarded ($), Project Duration
    ("Jan 1, 2025 - Dec 31, 2029"), Sector, Program, Related Files (PDFs)

Method: WP REST for the complete slug list + static HTML for each project
page (method 2 + 5 on the ladder). Ladder item 0 checked 2026-09-30: no
export on the site; the open.alberta.ca "grant disclosure" dataset covers
Government of Alberta ministries (a different funder, F4320314105), not
Alberta Innovates. The REST `project` objects expose no content/ACF fields.

funder_award_id (runbook §2.1.1): works citing Alberta Innovates write its
9-digit application number ("Advance 232403381", "CASBE 222301211",
"202102818"; bare numbers are the largest shape among the funder's ~1,000
citation shells). The project pages do not print it as a field, but the
funder's own "Related Files" PDFs are named after it
(".../232404951-Bayat-Project-Summary-WIP.pdf"). When exactly one 9-digit
number leads the related-file names, that is shipped; otherwise the project
URL slug is the public-facing reference. Both are kept as columns.

"Funding Recipient" is usually an organization; when it names a person
("University of Alberta, Dr. Alireza Bayat") the person is split out as lead.

Which posts are awards (reviewed 2026-10-01, oxjob #1451):
  * ~22 posts are COHORT pages ("AICE Concepts – 2025 Recipients", "LevMax 4.0
    (2025)", "PRIHS 6 (2020)", "Postdoctoral Fellowships Recipients", "DICE Open
    Call 1.0", "Ecosystem Development Partnerships Program Round 2", ...) whose
    table lists one row per funded project. Each table row is emitted as its own
    award (record_type = 'cohort_table_row', funder_award_id =
    '{cohort-slug}/{title-slug}'); the cohort page itself is not an award. Rows
    that also have their own project page are dropped in favour of that page.
  * Programme-level reports/summaries with no single recipient and no table
    ("Carbon Storage – A Summary of Experience...", "Hydrogen Centre of Excellence
    Public Update Report") and the NON_AWARD_SLUGS list are dropped.
  * The same project posted twice (WordPress "-2" slugs with identical title,
    amount and dates) is kept once.
  * Every programme is kept, including training (Postdoctoral Fellowships) and the
    Ecosystem Development Partnerships Program (innovation-ecosystem support; flagged
    in the ingest report, Kyle scope rule 2026-10-01: keep when unsure).

Output: s3://openalex-ingest/awards/alberta_innovates/alberta_innovates_projects.parquet
"""

import argparse
import calendar
import hashlib
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
# file I/O (Path.write_text / open() without explicit encoding=). No-op on
# Linux/Databricks. See runbook §1.2.
import sys as _sys_utf8  # _sys_utf8 is sys: this block is sys.stdout.reconfigure(...) + file-I/O utf-8 defaults
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

BASE = "https://albertainnovates.ca"
REST_URL = BASE + "/wp-json/wp/v2/project"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/alberta_innovates/alberta_innovates_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 4

MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, **kw) -> requests.Response | None:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60, **kw)
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r
            last_err = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>|</p>|</li>", "\n", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"[ \t\r\f\v]+", " ", t)
    t = re.sub(r"\s*\n\s*", "\n", t).strip()
    return t or None


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), plus a leading honorific strip."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "p.eng", "peng"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_date(s: str | None, is_end: bool = False) -> str | None:
    """'Jan 1, 2025' / 'June 1, 2026' / '2020-01-13' -> ISO date. Month-only 'March 2026'
    -> first of the month (or last of the month when is_end)."""
    s = (s or "").strip()
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})$", s)
    if m:
        return s
    m = re.match(r"([A-Za-z]{3})[A-Za-z]*\.?\s+(\d{1,2}),?\s+(\d{4})", s)
    if m and m.group(1).lower() in MONTHS:
        return f"{m.group(3)}-{MONTHS[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"
    m = re.match(r"([A-Za-z]{3})[A-Za-z]*\.?,?\s+(\d{4})\b", s)
    if m and m.group(1).lower() in MONTHS:
        y, mo = int(m.group(2)), MONTHS[m.group(1).lower()]
        day = calendar.monthrange(y, mo)[1] if is_end else 1
        return f"{y}-{mo:02d}-{day:02d}"
    return None


def split_duration(duration: str | None) -> tuple[str | None, str | None]:
    """'Jan 1, 2025 - Dec 31, 2029' / 'March 2021 to June 2024' -> (start, end) ISO dates."""
    dparts = re.split(r"\s*(?:[–—‐-]|\bto\b)\s*(?=[A-Za-z]{3})", duration or "", maxsplit=1)
    start = parse_date(dparts[0]) if dparts and dparts[0] else None
    end = parse_date(dparts[1], is_end=True) if len(dparts) > 1 else None
    return start, end


def parse_amount(s: str | None) -> float | None:
    """'$600,000' -> 600000. Free-text variants ('Total value ... is $8.8 million,
    with $7.2 million contributed from Alberta Innovates') -> the Alberta Innovates
    share when named, else the single $ figure, else None."""
    if not s:
        return None
    s = re.sub(r"(\d),\s+(\d{3})\b", r"\1,\2", s)   # "$160, 000" (stray space) -> "$160,000"
    s = re.sub(r"(\d)\.(\d{3})\b(?![.\d]|\s*(?:million|M\b))", r"\1,\2", s)  # "$594.200" -> "$594,200"
    figs = [(m.start(), float(m.group(1).replace(",", "")) * (1e6 if m.group(2) else 1))
            for m in re.finditer(r"\$\s*([\d,]+(?:\.\d+)?)\s*(million|M(?![A-Za-z]))?", s, re.I)]
    if not figs:
        bare = re.fullmatch(r"\s*([\d,]+(?:\.\d+)?)\s*", s)
        figs = [(0, float(bare.group(1).replace(",", "")))] if bare else []
    ai = re.search(r"(?:from|by) Alberta Innovates", s, re.I)
    if ai and figs:
        before = [v for pos, v in figs if pos < ai.start()]
        val = before[-1] if before else None
    else:
        val = figs[0][1] if len(figs) == 1 else None
    return val if val and val > 0 else None


TITLED_RE = re.compile(r"^(?:Dr|Prof|Professor)\.?\s+(.+?)(?:\s*\(([^)]*)\))?\s*$")
# Untitled person names are only accepted next to an academic/public-research organization
# ("Kevin Hodder, University of Alberta"); next to a company the second name is too often
# another company ("TerraVerdae Bioworks Inc., Barraca Falero"), so those stay organization-only.
ACADEMIC_RE = re.compile(r"Universit|College|Polytechni|Institute of Technology|\bNAIT\b|\bSAIT\b|"
                         r"National Research Council|Georgia Tech|Agri-Food Canada|InnoTech Alberta|CanmetENERGY", re.I)
ORGWORD_RE = re.compile(
    r"\b(?:Inc|Ltd|Corp|Corporation|LLC|Limited|Company|Group|Energy|Power|Gentec|Advisory|Solutions|"
    r"Technolog\w*|Systems|Services|Canada|Canadian|Alberta|Society|Association|Foundation|Partnerships?|"
    r"Agriculture|District|County|Town|City|Resources|Engineering|Labs?|Bioworks|Innovations?|Institute|"
    r"Centre|Center|Council|Research|Network|Alliance|Coalition|Assets|Wastewater|Flooring|Chemicals|"
    r"Members|Programs?|University|College|Smarter|Department|Dept|Health|Devices|Medical|AI|Robotics|Plastics|"
    r"Hydrocarbons|Biotech|Imaging|Diagnostics|Pharmaceuticals|Therapeutics|Sciences|Materials|Industries|"
    r"Manufacturing|Construction|Textiles?|Oil|Gas|Pipelines|Environmental|Consulting|Partners|Ventures|Capital|"
    r"Holdings|Farms|Analytics|Software|Data)\b", re.I)
# "McBrien", "Schick-Makaroff", "Mi-Young", "O'Brien" pass; "PhD", "MBA", "AI" do not
NAME_TOKEN_RE = re.compile(r"^(?:[A-ZÀ-Þ][a-zà-ÿ'’]*(?:[-'’]?[A-ZÀ-Þ]?[a-zà-ÿ'’]+)*|[A-Z]\.(?:[A-Z]\.)*|[A-Z]|"
                           r"van|de|der|von|la|le|da|del|\([A-Z][a-z]+\))$")
LEGAL_SUFFIX_RE = re.compile(r"^(?:Inc|Ltd|Corp|LLC|L\.L\.C|Limited)\.?$", re.I)
DEGREE_RE = re.compile(r"^(?:(?:PhD|MD|MSc|BSc|BScN|MBA|MBBS|MD\(Res\)|RN|MTA|P\.?Eng\.?|PENG|FRCPC|FRCSC|FRSC|"
                       r"FSfC|FAIMBE|FCAE|FCIC|FEC)[.,]?\s*)+$", re.I)
INSTITUTION_ABBR = {"UCalgary": "University of Calgary", "UAlberta": "University of Alberta",
                    "ULethbridge": "University of Lethbridge"}


def is_person(s: str) -> bool:
    toks = s.split()
    return (2 <= len(toks) <= 4 and all(NAME_TOKEN_RE.match(t) for t in toks)
            and not ORGWORD_RE.search(s) and not re.search(r"\d", s))


def _parts(seg: str) -> list[str]:
    """Comma / spaced-dash split that re-attaches bare legal suffixes ('Baymag, Inc.')."""
    out = []
    for p in re.split(r",\s*|\s+[-–]\s+", seg):
        p = p.strip()
        if not p:
            continue
        if out and LEGAL_SUFFIX_RE.match(p):
            out[-1] = f"{out[-1]}, {p}"
        else:
            out.append(p)
    return out


def split_recipient(rec: str | None) -> tuple[str | None, str | None]:
    """'Funding Recipient' text -> (organization, lead person name or None).

    'University of Alberta, Dr. Alireza Bayat'        -> ('University of Alberta', 'Alireza Bayat')
    'Dr. Reza Vakili (University of Alberta), PureAox' -> ('University of Alberta', 'Reza Vakili')
    'Kevin Hodder, University of Alberta'              -> ('University of Alberta', 'Kevin Hodder')
    'ALUS Canada, Koral Wysocki'                       -> ('ALUS Canada', None)   (company: org only)
    The FIRST titled person is the lead; honorifics are dropped here, degrees in split_name."""
    if not rec:
        return None, None
    # free-text variants: "Recipient is X. Partners are: ...",
    # "X is the funding recipient. Project partners include: ...", "Recipient: X / Partners: ..."
    rec = re.split(r"\.?\s*/?\s*(?:Project )?[Pp]artners?(?: are| include| includes)?\s*:|"
                   r"\.?\s*(?:Project )?[Pp]artners? (?:are|include|includes)\b", rec)[0]
    rec = re.sub(r"^(?:The )?(?:funding )?recipient(?: is|:)\s*", "", rec.strip(), flags=re.I)
    rec = re.sub(r"\s+is the (?:funding )?recipient\.?$", "", rec.strip(), flags=re.I)
    rec = rec.strip().rstrip(".").strip()
    if not rec:
        return None, None
    seg = re.split(r"\s*[;|]\s*|\s+/\s+", rec)[0]          # lead segment; partners follow ; or |
    parts = [p for p in _parts(seg) if not DEGREE_RE.match(p)]
    if not parts:
        return None, None
    persons = {}
    for i, p in enumerate(parts):
        m = TITLED_RE.match(p)
        if m:
            persons[i] = (m.group(1).strip(), (m.group(2) or "").strip() or None)
    if not persons and any(ACADEMIC_RE.search(p) for p in parts):
        for i, p in enumerate(parts):
            if is_person(re.sub(r"\s*\([^)]*\)", "", p)) and not ACADEMIC_RE.search(p):
                persons[i] = (p, None)
    if not persons:
        return parts[0], None
    i = min(persons)
    name, paren_org = persons[i]
    name = re.sub(r"\s*\([^)]*\)", "", name).strip().rstrip(".")  # "Edward (Ted) Roberts" -> "Edward Roberts"
    org = paren_org
    if not org:
        # nearest non-person part, academic first; on a tie the preceding part wins
        others = sorted((j for j in range(len(parts)) if j not in persons), key=lambda j: (abs(j - i), j > i))
        academic = [j for j in others if ACADEMIC_RE.search(parts[j])]
        pick = academic or others
        org = parts[pick[0]] if pick else None
    return org, name


def parse_lead_applicant(v: str, bare_is_person: bool = True) -> tuple[str | None, str | None]:
    """Cohort-table 'Lead Applicant' / 'Main Project Partners' cell -> (organization, person).

    'Hadis Karimipour, PhD, UCalgary'               -> ('University of Calgary', 'Hadis Karimipour')
    'OKAKI Health Intelligence Inc. Salim Samanani, MD' -> ('OKAKI Health Intelligence Inc.', 'Salim Samanani')
    'Beam Medical Inc.\\nYannick Boutin'             -> ('Beam Medical Inc.', 'Yannick Boutin')
    'University of Alberta,\\nKyle Nishiyama'        -> ('University of Alberta', 'Kyle Nishiyama')
    'Dr. Tuan Trang of University of Calgary and AphioTx' -> ('University of Calgary', 'Tuan Trang')
    'Fedora Pharmaceuticals Inc.'                   -> ('Fedora Pharmaceuticals Inc.', None)"""
    v = (v or "").strip()
    if not v:
        return None, None
    m = re.match(r"^(?:Dr|Prof)\.?\s+(.+?)\s+of\s+(.+?)(?:,|\s+and\s+|$)", v)
    if m:
        return m.group(2).strip(), m.group(1).strip()
    lines = [ln.strip().rstrip(",").strip() for ln in v.split("\n") if ln.strip()]
    org = None
    if len(lines) >= 2:
        rest_first = lines[1].split(",")[0].strip()
        if is_person(rest_first) and not DEGREE_RE.match(lines[0].split(",")[-1].strip()):
            org, v = lines[0], ", ".join(lines[1:])
        else:
            v = " ".join(lines)
    parts = [p.strip() for p in v.split(",") if p.strip()]
    degrees = [p for p in parts[1:] if DEGREE_RE.match(p)]
    others = [p for p in parts[1:] if not DEGREE_RE.match(p)]
    name = parts[0]
    toks = re.findall(r"\S+", name)  # token list (degree stripping), not a given/family split
    while len(toks) > 1 and DEGREE_RE.match(toks[-1]):   # "Shabir Barzanjeh PhD" (missing comma)
        degrees.append(toks.pop())
    name = " ".join(toks)
    if org is None and not degrees and not others and (not bare_is_person or not is_person(name)):
        return name, None                                  # organization only
    mc = re.match(r"^(.+?\b(?:Inc|Ltd|Limited|Corp|Corporation)\.?)\s+(.+)$", name)
    if org is None and mc and is_person(mc.group(2)):
        org, name = mc.group(1), mc.group(2)
    elif org is None and len(re.findall(r"\S+", name)) > 4:  # "Deep Life Sensors Bob Sheldon, MD, PhD"
        toks = re.findall(r"\S+", name)
        org, name = " ".join(toks[:-2]), " ".join(toks[-2:])
    if org is None and others:
        org = INSTITUTION_ABBR.get(others[-1], others[-1])
    if not is_person(name):
        return org or name, None
    return org, name


def slugs() -> list[str]:
    out, page, pages = [], 1, None
    while pages is None or page <= pages:
        r = get(REST_URL, params={"per_page": 100, "page": page, "_fields": "id,link"})
        pages = int(r.headers["X-WP-TotalPages"])
        total = int(r.headers["X-WP-Total"])
        out += [it["link"] for it in r.json()]
        page += 1
        time.sleep(REQUEST_DELAY)
    log(f"REST: {len(out)} project URLs (X-WP-Total {total})")
    if len(out) < total:
        raise SystemExit(f"REST returned {len(out)} of {total} projects")
    return out


def parse_project(url: str, page: str) -> dict:
    title = re.search(r'<h1 class="page-header-title">(.*?)</h1>', page, re.S)
    desc = re.search(r'<p class="header-description">(.*?)</p>', page, re.S)
    glance = {}
    for key, val in re.findall(
            r'<span class="label" id="([a-z_]+)">.*?</span>\s*<span class="value">(.*?)</span>\s*</div>', page, re.S):
        glance[key] = val
    files = re.findall(r'<div class="related-file">\s*<a href="([^"]+)"[^>]*>(.*?)</a>', page, re.S)
    file_urls = [u for u, _ in files]
    appnos = sorted({m.group(1) for u in file_urls
                     if (m := re.match(r"(\d{9})\b", u.rsplit("/", 1)[-1]))})
    duration = text(glance.get("project_duration"))
    start, end = split_duration(duration)
    amount_txt = text(glance.get("funding_awarded"))
    amount = parse_amount(amount_txt)
    recipient = text(glance.get("funding_recipient"))
    org, person = split_recipient(recipient)
    if org:  # "University of Alberta, Dr. A, Dr. B" -> keep the organization only
        org = re.split(r",\s*(?:Dr|Prof)\.?\s", org)[0].strip() or None
    if org and org.lower() in {"various", "tbd", "n/a"}:
        org = None
    given, family = split_name(person) if person else (None, None)
    website = re.search(r'href="([^"]+)"', glance.get("website") or "")
    return {
        "slug": url.rstrip("/").rsplit("/", 1)[-1],
        "landing_page_url": url,
        "title": text(title.group(1)) if title else None,
        "description": text(desc.group(1)) if desc else None,
        "funding_recipient": recipient,
        "recipient_organization": org,
        "recipient_person": person,
        "lead_given_name": given,
        "lead_family_name": family,
        "recipient_website": website.group(1) if website else None,
        "amount_text": amount_txt,
        "amount": f"{amount:.2f}" if amount else None,
        "currency": "CAD" if amount else None,
        "project_duration": duration,
        "start_date": start,
        "end_date": end,
        "sector": text(glance.get("sector")),
        "program": text(glance.get("program")),
        "related_files_json": json.dumps(file_urls) if file_urls else None,
        "application_numbers": ";".join(appnos) if appnos else None,
        "record_type": "project_page",
        "parent_slug": None,
        "n_tables": len(re.findall(r"<table", page)),
    }


# ---------------------------------------------------------------------------
# Cohort pages. ~20 of the `project` posts are not one project but a cohort
# ("AICE Concepts – 2025 Recipients", "LevMax 4.0 (2025)", "PRIHS 6 (2020)",
# "Postdoctoral Fellowships Recipients", "DICE Open Call 1.0", ...) whose body
# holds a table with one row per funded project (lead applicant, title, and
# sometimes value / award date). Those rows are the grant-level records: the
# cohort page itself is not an award. Each table row becomes its own record
# (record_type = 'cohort_table_row'); the cohort page is dropped.
# ---------------------------------------------------------------------------
# cohort/summary pages with neither a table nor a recipient (checked by hand 2026-10-01)
NON_AWARD_SLUGS = {
    "care-in-the-community-2018-2019": "programme summary (6 projects, no per-project data)",
    "health-innovation-platform-partnership-past-selections": "programme summary, no per-project data",
}


def cell_text(fragment: str) -> str:
    t = re.sub(r"<br\s*/?>|</p>", "\n", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"[ \t\r\f\v]+", " ", t)
    return re.sub(r"\s*\n\s*", "\n", t).strip()


def slugify(s: str, n: int = 70) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")
    return s[:n].rstrip("-")


def cohort_rows(parent: dict, page: str) -> list[dict]:
    body = page[page.find("page-header-title"):]
    if "<footer" in body:
        body = body[: body.find("<footer")]
    title = parent["title"] or ""
    # cohort-level dates: the page's own "Project Duration" ("September 2024 - February 2028")
    # describes the whole cohort; else the cohort year in the title/slug ("LevMax 1.0 (2022)").
    yr = re.search(r"\b(20[12]\d)\b", title) or re.search(r"\b(20[12]\d)\b", parent["slug"])
    out = []
    for t in re.findall(r"<table.*?</table>", body, re.S):
        rows = [[cell_text(c) for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.S)]
                for tr in re.findall(r"<tr.*?</tr>", t, re.S)]
        if len(rows) < 2:
            continue
        hdr = [h.lower().rstrip(":").strip() for h in rows[0]]

        def col(*names):
            return next((hdr.index(n) for n in names if n in hdr), None)

        ti = col("project title", "title", "project")
        if ti is None or "document type" in hdr or "file" in hdr:
            continue  # summary tables ("Primary Focus Area") / document indexes of other project pages
        li = col("lead applicant", "main project partners")
        ri = col("researcher")
        ai = col("applicant")
        fi, ni = col("first name"), col("last name")
        oi = col("institution", "organization")
        vi = col("value", "award amount", "funding amount", "ai investment", "requested ai funding",
                 "ai funding amount ($)")
        di = col("award date")
        si = col("levmax-health stream", "award", "lead scn", "sector focus")
        bi = col("total project budget", "total project cost")
        pi = col("project term")
        for r in rows[1:]:
            cell = lambda i: (r[i].strip() if i is not None and i < len(r) else "")  # noqa: E731
            ptitle = re.sub(r"\s*\n\s*", " ", cell(ti)).strip()
            if not ptitle or ptitle.lower() in {"total", "totals"} or (r and r[0].strip().lower() == "total"):
                continue
            org = person = given = family = None
            if li is not None:
                # a bare one-line "Main Project Partners" cell is a company ("My Viva"), a bare
                # "Lead Applicant" cell is a person (PRIHS 7: "Kamala Adhikari")
                org, person = parse_lead_applicant(cell(li), bare_is_person=hdr[li] == "lead applicant")
            elif ri is not None:
                person = re.split(r"\s*/\s*", cell(ri))[0].strip() or None
            elif ai is not None:
                org = re.sub(r"\s*\n\s*", " ", cell(ai)).strip() or None
            if fi is not None and ni is not None and cell(ni):
                given, family = cell(fi) or None, cell(ni)
                person = f"{given or ''} {family}".strip()
            elif person:
                given, family = split_name(person)
            if oi is not None and cell(oi):
                org = INSTITUTION_ABBR.get(cell(oi), re.sub(r"\s*\n\s*", " ", cell(oi)))
            amount_txt = cell(vi) if vi is not None else None
            amount = parse_amount(amount_txt) if amount_txt else None
            start = parse_date(cell(di)) if di is not None else None
            end = None
            if di is None:  # no per-row date: cohort-level duration from the page, if it has one
                start, end = parent["start_date"], parent["end_date"]
            out.append({
                "slug": None,
                "landing_page_url": parent["landing_page_url"],
                "title": ptitle,
                "description": None,
                "funding_recipient": re.sub(r"\s*\n\s*", " ", cell(li if li is not None else ri if ri is not None else ai)) or None,
                "recipient_organization": org,
                "recipient_person": person,
                "lead_given_name": given,
                "lead_family_name": family,
                "recipient_website": None,
                "amount_text": amount_txt or None,
                "amount": f"{amount:.2f}" if amount else None,
                "currency": "CAD" if amount else None,
                "project_duration": cell(pi) or None if pi is not None else parent["project_duration"],
                "start_date": start,
                "end_date": end,
                "sector": parent["sector"],
                "program": parent["program"],
                "related_files_json": None,
                "application_numbers": None,
                "record_type": "cohort_table_row",
                "parent_slug": parent["slug"],
                "parent_title": title,
                "cohort_year": yr.group(1) if yr else None,
                "scheme_detail": re.sub(r"\s*\n\s*", " ", cell(si)) or None,
                "total_project_budget": cell(bi) or None,
            })
    # identical rows repeated on a page (AICE Concepts 2023 lists one SME twice)
    seen, uniq = set(), []
    for o in out:
        k = (o["title"].lower(), (o["recipient_person"] or o["recipient_organization"] or "").lower())
        if k not in seen:
            seen.add(k)
            uniq.append(o)
    return uniq


def main() -> None:
    p = argparse.ArgumentParser(description="Alberta Innovates funded projects -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    urls = slugs()
    if args.limit:
        urls = urls[: args.limit]
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    rows, cohort, dropped, missing = [], [], [], []
    t0 = time.time()
    for i, url in enumerate(urls, 1):
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        # long slugs differ only in their tail ("...-in-wastewater" vs "...-in-wastewater-2"):
        # never truncate a cache key without a hash of the full slug
        key = slug if len(slug) <= 100 else slug[:80] + "-" + hashlib.md5(slug.encode()).hexdigest()[:12]
        cache = args.cache_dir / (key + ".html") if args.cache_dir else None
        if cache and cache.exists():
            page = cache.read_text()
        else:
            r = get(url)
            page = r.text if r is not None else None
            if cache and page:
                cache.write_text(page)
            time.sleep(REQUEST_DELAY)
        if not page:
            missing.append(url)
            continue
        proj = parse_project(url, page)
        crow = cohort_rows(proj, page) if proj["n_tables"] else []
        if proj["n_tables"]:
            # cohort page (or a table-only document index): its table rows are the awards,
            # the page itself is not one
            cohort += crow
            dropped.append((proj["slug"], f"cohort page, {len(crow)} table rows"))
        elif proj["slug"] in NON_AWARD_SLUGS:
            dropped.append((proj["slug"], NON_AWARD_SLUGS[proj["slug"]]))
        elif (proj["funding_recipient"] or "").strip().lower() in {"various", "n/a", "tbd"}:
            # programme-level reports/summaries ("Carbon Storage – A Summary of Experience and
            # Lessons Learned...", "Hydrogen Centre of Excellence Public Update Report") with no
            # single recipient and no per-project table: not an award
            dropped.append((proj["slug"], f"no single recipient ({proj['funding_recipient']})"))
        else:
            rows.append(proj)
        if i % 50 == 0 or i == len(urls):
            el = time.time() - t0
            log(f"  {i}/{len(urls)} pages, ETA {el / i * (len(urls) - i) / 60:.1f} min")
    if missing:
        log(f"  {len(missing)} project URLs 404'd: {missing[:10]}")
        if len(missing) > max(5, len(urls) // 50):
            raise SystemExit("too many missing project pages; source broken?")

    log(f"  dropped {len(dropped)} non-award pages: {dropped}")
    norm = lambda t: re.sub(r"[^a-z0-9]+", "", (t or "").lower())  # noqa: E731

    # The same project posted twice (WP "-2" slugs: same title, amount and dates) -> keep one,
    # preferring the copy that carries an application number / a description.
    df = pd.DataFrame(rows)
    df["_k"] = df["title"].map(norm) + "|" + df["amount"].fillna("") + "|" + df["start_date"].fillna("")
    df["_rank"] = (df["application_numbers"].isna().astype(int) * 4 + df["description"].isna().astype(int) * 2
                   + df["slug"].str.contains(r"-\d+$").astype(int))
    before = len(df)
    df = df.sort_values(["_k", "_rank", "slug"]).drop_duplicates("_k", keep="first").sort_index()
    log(f"  removed {before - len(df)} duplicate project pages (same title, amount and start date)")

    # cohort rows already published as their own project page -> keep the project page
    page_titles = set(df["title"].map(norm))
    cdf = pd.DataFrame(cohort)
    if len(cdf):
        dup = cdf["title"].map(norm).isin(page_titles)
        log(f"  {len(cdf)} cohort table rows; {dup.sum()} also have their own project page (dropped)")
        cdf = cdf[~dup].copy()
        cdf["funder_award_id"] = cdf["parent_slug"] + "/" + cdf["title"].map(slugify)
        cdf["award_id_source"] = "cohort_slug"

    single = df["application_numbers"].notna() & ~df["application_numbers"].fillna("").str.contains(";")
    df["funder_award_id"] = df["slug"].where(~single, df["application_numbers"])
    df["award_id_source"] = single.map({True: "application_number", False: "slug"})
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        # two projects sharing one application number (e.g. phases) -> fall back to slug for those
        log(f"  {dupes.sum()} rows share an application number; using slug for them: "
            f"{df.loc[dupes, 'funder_award_id'].unique().tolist()[:10]}")
        df.loc[dupes, "funder_award_id"] = df.loc[dupes, "slug"]
        df.loc[dupes, "award_id_source"] = "slug"
    df = pd.concat([df.drop(columns=["_k", "_rank"]), cdf], ignore_index=True)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")

    log(f"Parsed {len(df)} awards ({df['record_type'].value_counts().to_dict()}); "
        f"award id from application number: {(df['award_id_source'] == 'application_number').sum()}")
    for c in ["title", "description", "funding_recipient", "recipient_organization", "recipient_person",
              "amount", "start_date", "end_date", "program", "application_numbers"]:
        log(f"  {c:20s} {df[c].notna().mean():6.1%}")
    amt = pd.to_numeric(df["amount"], errors="coerce")
    log(f"  total amount CAD {amt.sum():,.0f} (min {amt.min():,.0f}, max {amt.max():,.0f})")
    log(f"  programs: {df['program'].value_counts().head(8).to_dict()}")

    df = df.astype("string")  # runbook §1.2 item 5
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "alberta_innovates_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_alberta_innovates_projects.parquet"
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
