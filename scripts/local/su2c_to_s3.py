#!/usr/bin/env python3
"""
Stand Up To Cancer (SU2C) research portfolio to S3
===================================================

SU2C funds collaborative cancer research teams (Dream Teams, Research Teams,
Convergence teams, Catalyst teams, Health Equity Breakthrough Team) and
individual grants/awards (Innovative Research Grants, Phillip A. Sharp
Innovation in Collaboration Awards, Ziskin Prize and other named awards, SU2C
Kimmel Scholars). AACR administers most of them as SU2C's scientific partner,
but the money is SU2C's (the AACR ingest, priority 560, excludes them).

Source: SU2C's own Scientific Summit Program Guides, linked from
https://standuptocancer.org/science/portfolio-overview/ ("includes the entire
SU2C science portfolio since the start of the organization"):

  2024  su2c-2024-scientific-summit-program-guide-updated-v1.pdf  (216 pp: every team
        since 2009 incl. closed ones, IRG classes, Sharp awards, other awards, Kimmel Scholars)
  2025  su2c-2025-scientific-summit-program-guide.pdf              (active teams)
  2026  SU2C-scientific-summit-program-guide-2026-final-1.pdf      (active + new teams, 2024-25 awards)

No CSV/API exists (ladder item 4: funder-published bulk PDF). Team pages carry
SU2C's grant number (DT/RT/CT/CV/BT + 4 digits), team name, project title,
"Grant Term: <Month YYYY> - <Month YYYY>" and KEY PERSONNEL (Team Leader,
Co-Leaders, Principals ...). Teams are merged across the three guides by grant
number (the newest guide's term/people win; a number printed with two
prefixes for the same team, e.g. RT6188/DT6188, keeps the first-printed
code). The award sections are two-column lists (CAPS title, then "Name,
degrees, institution"); columns are split at the page's right-column
line-start x position.

Routing (§2.3.2): teams named "SU2C Canada ..." are funded by Stand Up To
Cancer Canada, which has its own OpenAlex funder row (F4320315605); they get
route_funder_id 4320315605. Every other row is SU2C (F4320332898), including
partner co-funded teams (SU2C-Lustgarten, SU2C-CRUK, SU2C-Dutch Cancer
Society, Van Andel Institute-SU2C, Pancreatic Cancer Collective ...), whose
partner stays visible in funder_scheme (= the team name).

funder_award_id: the team's SU2C grant number as printed (e.g. "DT5918").
Citing works use AACR-era numbers for pre-2019 teams (SU2C-AACR-DT0209), which
the guides do not print, so those cannot be matched. Grants without a printed
number get a synthetic key: SU2C-{IRG|SHARP|ZISKIN|...}-{year}-{lead-slug}.

Output: s3://openalex-ingest/awards/su2c/su2c_projects.parquet
"""

import argparse
import json
import calendar
import re
import time
import unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
import pdfplumber
import requests


# (self-check marker: this block is the runbook sys.stdout.reconfigure shim, with sys renamed)
# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing names with non-ASCII chars. Production runs on
# Linux/Databricks where UTF-8 is the default; this fixes local validation on
# Windows. See runbook §1.2.
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


UPLOADS = "https://standuptocancer.org/wp-content/uploads"
PORTFOLIO_PAGE = "https://standuptocancer.org/science/portfolio-overview/"
GUIDES = [  # oldest first: later guides update the same grant number
    (2024, f"{UPLOADS}/su2c-2024-scientific-summit-program-guide-updated-v1.pdf"),
    (2025, f"{UPLOADS}/su2c-2025-scientific-summit-program-guide.pdf"),
    (2026, f"{UPLOADS}/SU2C-scientific-summit-program-guide-2026-final-1.pdf"),
]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/su2c/su2c_projects.parquet"
SU2C_ID, SU2C_CANADA_ID = 4320332898, 4320315605

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 2.0
RETRIES = 3

CODE = r"(?:DT|RT|CT|BT|CV)\d{4}"
HEADER_RE = re.compile(rf"^(?P<name>.*?)\s*(?P<codes>{CODE}(?:\s*,\s*{CODE})*)\s*,?\s*$")
CODES_ONLY_RE = re.compile(rf"^\s*{CODE}(?:\s*,\s*{CODE})*\s*,?\s*$")
MONTHS = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
TERM_RE = re.compile(r"Grant Term:\s*([A-Za-z]+)\s+(?:\d{1,2},\s*)?(\d{4})\s*[–—-]\s*([A-Za-z]+)?\s*(?:\d{1,2},\s*)?(\d{4})")
ROLE_RE = re.compile(
    r"^(?:KEY PERSONNEL\s+)?(?P<role>Team Leader and Clinical Lead|Team Leaders?|Team Co-[Ll]eaders?|Co-Leaders?|"
    r"Leaders?|Principal Investigator|Principal|Team Member|Senior Co-Investigator|Co-Investigator|Clinical Lead|"
    r"Center Principal|Investigator|Young Investigator|Early Career Investigator|Collaborator|Project Manager|"
    r"Advocate|Clinical Research Manager)s?:\s*(?P<rest>.*)$")
LEAD_ROLES = ("Team Leader and Clinical Lead", "Team Leader", "Team Leaders", "Leader", "Leaders", "Principal Investigator")
CO_ROLES = ("Team Co-Leader", "Team Co-leader", "Co-Leader", "Co-Leaders", "Team Co-Leaders", "Team Co-leaders")
INVESTIGATOR_ROLES = LEAD_ROLES + CO_ROLES + ("Principal", "Center Principal", "Team Member", "Co-Investigator",
                                              "Senior Co-Investigator", "Clinical Lead", "Investigator",
                                              "Young Investigator", "Early Career Investigator", "Collaborator")
DEGREES = {"md", "phd", "mph", "ms", "msc", "mba", "dphil", "dsc", "scd", "mbbs", "frcpc", "frcp", "facp", "rn",
           "bsn", "dvm", "pharmd", "mpp", "ma", "bs", "ba", "drph", "mbchb", "mb", "bchir", "mrcp", "frcpath",
           "jd", "mse", "msce", "mhs", "dds", "od", "aprn", "np", "pa", "bsc", "frcsc", "frcs", "facs", "dmsc",
           "hon", "(hon)", "(hon.)", "moh", "mas", "ccrp", "lmsw", "msw", "cphq", "mscr", "edd", "faan"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, binary: bool = False):
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=300)
            r.raise_for_status()
            return r.content if binary else r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def cached(cache_dir: Path | None, name: str, url: str, binary: bool = False):
    path = cache_dir / name if cache_dir else None
    if path and path.exists():
        return path.read_bytes() if binary else path.read_text()
    body = get(url, binary)
    if path:
        cache_dir.mkdir(parents=True, exist_ok=True)
        if binary:
            path.write_bytes(body)
        else:
            path.write_text(body)
    time.sleep(REQUEST_DELAY)
    return body


def tidy(s: str | None) -> str | None:
    if not s:
        return None
    s = s.replace("\xa0", " ").replace("‐", "-").replace("‑", "-")
    s = re.sub(r"\s+", " ", s).strip(" ,;:|")
    return s or None


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical wolf_to_s3.py helper (runbook §2.4.1): strip trailing
    degree/suffix tokens, last remaining token = family name."""
    if not name:
        return None, None
    tokens = [t for t in re.split(r"\s+", name.strip()) if t]
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


PARTICLES = {"van", "von", "vander", "der", "den", "de", "del", "della", "di", "da", "le", "la", "st.", "dos", "das"}


def split_name_particles(name: str) -> tuple[str | None, str | None]:
    """split_name, then keep surname particles with the family name
    ('Matthew Vander Heiden' -> 'Vander Heiden', 'Daniel D. Von Hoff' -> 'Von Hoff')."""
    given, family = split_name(name)
    if given and family:
        g = given.split()
        while len(g) > 1 and g[-1].lower() in PARTICLES:
            family = g.pop() + " " + family
        given = " ".join(g)
    return given, family


def is_degrees(part: str) -> bool:
    toks = [t.lower().strip(".()") for t in re.split(r"[\s./]+", part) if t.strip(".()")]
    return bool(toks) and all(t in DEGREES for t in toks)


def person(text: str) -> tuple[str | None, str | None, str | None]:
    """'Daniel A. Haber, MD, PhD' -> (given, family, trailing non-degree text, e.g. an institution
    printed on the same line). Parenthesised notes ('(deceased)', '(2009-2012)') are dropped."""
    text = re.sub(r"\([^)]*\)", " ", text or "")
    parts = [p.strip() for p in text.split(",")]
    name = parts[0] if parts else ""
    rest = [p for p in parts[1:] if p and not is_degrees(p)]
    # degree printed before an institution on the same line: 'MD Virginia'
    rest = [re.sub(r"^(?:(?:MD|PhD|MPH|MBA|DSc|MBBS|MB/BChir|FRCP|FRCPC)\s+)+", "", p) for p in rest]
    given, family = split_name_particles(tidy(name) or "")
    return given, family, tidy(", ".join(r for r in rest if r))


def people_list(raw: str, with_inst: bool) -> list[dict]:
    """'A B, MD, C D, PhD, and E F, MD, PhD' -> three people. With with_inst (IRG
    lists: 'Name, degrees, Institution, City'), the text after the first person's
    degrees is that person's institution."""
    raw = re.sub(r"\([^)]*\)", " ", raw or "")
    parts = [p.strip() for p in re.split(r",|\s+and\s+(?=[A-Z])", raw) if p.strip()]
    parts = [re.sub(r"^and\s+", "", p) for p in parts]
    people, inst = [], []
    for p in parts:
        if is_degrees(p):
            continue
        if with_inst and people:
            inst.append(p)
            continue
        given, family = split_name_particles(p)
        if family:
            people.append({"given": given, "family": family, "inst": None})
    if with_inst and people:
        people[0]["inst"] = tidy(", ".join(inst))
    return people


def page_columns(page) -> tuple[list[tuple[float, str]], list[tuple[float, str]], float]:
    """Lines of the left and right column as (top, text). The split is the right
    column's line-start x (most common line-start positions on the page)."""
    words = page.extract_words()
    starts = Counter()
    for w in words:
        if not any(abs(v["top"] - w["top"]) < 2 and v["x1"] <= w["x0"] + 0.5 and w["x0"] - v["x1"] < 12 for v in words):
            starts[round(w["x0"])] += 1
    cands = sorted(x for x, c in starts.most_common(2) if c >= 5)
    split = cands[-1] - 4 if len(cands) == 2 and cands[-1] - cands[0] > 150 else page.width
    cols = ([], [])
    for side in (0, 1):
        ws = [w for w in words if (w["x0"] < split) == (side == 0)]
        ws.sort(key=lambda w: (w["top"], w["x0"]))
        lines: list[list] = []
        for w in ws:
            if lines and abs(lines[-1][0] - w["top"]) < 2.5:
                lines[-1][1].append(w)
            else:
                lines.append([w["top"], [w]])
        cols[side].extend((t, " ".join(x["text"] for x in sorted(g, key=lambda x: x["x0"]))) for t, g in lines)
    return cols[0], cols[1], split


def is_caps(s: str) -> bool:
    letters = [c for c in s if c.isalpha()]
    return bool(letters) and sum(c.isupper() for c in letters) / len(letters) > 0.9


def month_date(mon: str | None, year: str, end: bool) -> str | None:
    m = MONTHS.get((mon or "").lower())
    if not m:
        return None
    day = calendar.monthrange(int(year), m)[1] if end else 1
    return f"{int(year):04d}-{m:02d}-{day:02d}"


# ---------------------------------------------------------------- teams
def clean_inst(s: str | None) -> str | None:
    s = tidy(s)
    if not s:
        return None
    s = re.sub(r"^(?:\([^)]*\)\s*|\d{4}\)\s*|Director,?\s+)", "", s)
    s = re.sub(r"\s*\((?:formerly|deceased)\)\s*", " ", s)
    return tidy(s)


def parse_people(lines: list[str]) -> list[dict]:
    """Role lines ('Team Leader: Name, MD') followed by an institution line."""
    # a parenthetical that wraps ('Team Leader and Clinical Lead (May 2017 -' / 'April 2023): Name')
    joined, open_label = [], False
    for l in lines:
        if open_label:  # only a role label's own parenthetical is joined, and only once
            joined[-1] += " " + l
            open_label = False
            continue
        joined.append(l)
        open_label = bool(re.match(r"^[A-Za-z -]+\(", l)) and l.count("(") > l.count(")")
    lines = [re.sub(r"^(.*?)\s*\([^)]*\)\s*:", r"\1:", l) if re.match(r"^[A-Za-z -]+\(", l) else l for l in joined]
    out = []
    i = 0
    while i < len(lines):
        m = ROLE_RE.match(lines[i])
        if not m:
            i += 1
            continue
        role, rest = m.group("role"), m.group("rest")
        # name wrapped onto the next line ('Team Leader: Daniel D.' / 'Von Hoff, MD')
        if "," not in rest and i + 1 < len(lines) and not ROLE_RE.match(lines[i + 1]) and \
                re.match(r"^[A-Z][\w'’.\- ]{0,30},\s*(MD|PhD|MBBS|DPhil|MPH|DSc)\b", lines[i + 1]):
            rest = rest + " " + lines[i + 1]
            i += 1
        inst_lines = []
        j = i + 1
        while j < len(lines) and len(inst_lines) < 2:
            nxt = lines[j]
            if ROLE_RE.match(nxt) or "@" in nxt or is_caps(nxt) or re.match(r"^(PURPOSE|SPECIFIC AIMS|KEY )", nxt):
                break
            if inst_lines and len(nxt.split()) > 4:
                break
            inst_lines.append(nxt)
            j += 1
        names = [rest]
        if role.endswith("s") or re.search(r",? and ", rest):
            names = [n for n in re.split(r",?\s+and\s+|;\s*", rest) if n.strip()]
        for n in names:
            given, family, same_line_inst = person(n)
            if family:
                inst = " ".join(x for x in [same_line_inst] + inst_lines if x)
                out.append({"role": role, "given": given, "family": family, "inst": clean_inst(inst)})
        i = j
    return out


def parse_teams(pdf_path: Path, guide_year: int) -> list[dict]:
    teams = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for pno, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            lines = [l.strip() for l in text.split("\n")]
            if any("TABLE OF CONTENTS" in l for l in lines[:2]):
                continue
            if not any(l.startswith(("Grant Term", "KEY PERSONNEL", "Project:", "PROJECT:")) for l in lines):
                continue
            heads = []
            for j, line in enumerate(lines):
                m = HEADER_RE.match(line)
                if not m or not is_caps(line) or j == 0:
                    continue
                window = lines[j + 1:j + 7]
                if not any(w.startswith(("Grant Term", "KEY PERSONNEL", "Project:", "PROJECT:")) or "Team Leader" in w
                           for w in window):
                    continue
                heads.append((j, m))
            if not heads:
                continue
            words = page.extract_words()
            left, right, _ = page_columns(page)
            for hi, (j, m) in enumerate(heads):
                codes = re.findall(CODE, m.group("codes"))
                k = j + 1
                if k < len(lines) and CODES_ONLY_RE.match(lines[k]):
                    codes += re.findall(CODE, lines[k])
                    k += 1
                name_parts = [m.group("name")]
                p = j - 1
                # a team that starts the page: every caps line between the section label (line 0)
                # and the code line is the wrapped team name; mid-page, only name-like caps lines
                while p >= 1 and is_caps(lines[p]) and not HEADER_RE.match(lines[p]) and (
                        j <= 4 or re.search(r"SU2C|STAND UP|FOUNDATION|TEAM|PROGRAM|SOCIETY|ASSOCIATION|ALLIANCE|"
                                            r"INSTITUTE|RESEARCH|SUPPORT|WITH|CANCER|COLLECTIVE|CONVERGENCE|CATALYST",
                                            lines[p])):
                    name_parts.insert(0, lines[p])
                    p -= 1
                team_name = tidy(" ".join(name_parts))
                title_lines, term = [], None
                while k < len(lines) and len(title_lines) < 4:
                    l = lines[k]
                    if l.startswith("Grant Term"):
                        term = TERM_RE.search(l + " " + (lines[k + 1] if k + 1 < len(lines) else ""))
                        break
                    if l.startswith("KEY PERSONNEL") or ROLE_RE.match(l):
                        break
                    title_lines.append(re.sub(r"^(Project|PROJECT):\s*", "", l))
                    k += 1
                # personnel: column lines between this header and the next one (by y position)
                code_words = [w for w in words if w["text"].rstrip(",:") in codes]
                y0 = min((w["top"] for w in code_words), default=0)
                y1 = page.height
                if hi + 1 < len(heads):
                    nxt_codes = re.findall(CODE, heads[hi + 1][1].group("codes"))
                    y1 = min((w["top"] for w in words if w["text"].rstrip(",:") in nxt_codes), default=page.height)
                col_lines = [t for y, t in left if y0 < y < y1] + [t for y, t in right if y0 < y < y1]
                people = parse_people(col_lines)
                teams.append({
                    "codes": codes, "team_name": team_name,
                    "title": tidy(" ".join(title_lines)),
                    "start_date": month_date(term.group(1), term.group(2), False) if term else None,
                    "end_date": month_date(term.group(3), term.group(4), True) if term and term.group(3) else None,
                    "people": people, "guide_year": guide_year, "page": pno,
                })
    return teams


def merge_teams(all_teams: list[dict]) -> list[dict]:
    """One row per grant number. Multi-code program overviews whose codes all have
    their own pages are dropped; a code printed with two prefixes for the same team
    (same number, same title) keeps the first-printed code."""
    single = {c for t in all_teams if len(t["codes"]) == 1 for c in t["codes"]}
    rows: dict[str, dict] = {}
    by_number: dict[str, str] = {}
    program_term: dict[str, tuple] = {}
    norm_t = lambda s: re.sub(r"[^a-z0-9]+", "", (s or "").lower())  # noqa: E731
    for t in all_teams:  # oldest guide first
        codes = t["codes"]
        if len(codes) > 1 and all(c in single for c in codes):
            for c in codes:  # program overview page (RT6342-RT6345): its term applies to each project
                if t["start_date"]:
                    program_term[c] = (t["start_date"], t["end_date"])
            continue
        for c in codes:
            key, alt = c, False
            prev = by_number.get(c[2:])
            if prev and prev != c and rows.get(prev) and \
                    (norm_t(rows[prev]["title"]) in norm_t(t["title"]) or norm_t(t["title"]) in norm_t(rows[prev]["title"])):
                key, alt = prev, True  # same team, other prefix (RT6188 / DT6188, DT5978 / CT5978)
                rows[key].setdefault("alt_codes", set()).add(c)
            old = rows.get(key)
            new = dict(t, code=key, sibling_codes=[x for x in codes if x != c])
            if old:
                # newer guide wins for the term (extensions); the earliest guide's people are kept
                # (closest to the award); an alt-prefix page never renames the team
                for f in ("title", "start_date", "end_date", "team_name"):
                    if not new.get(f) or (alt and f in ("title", "team_name")):
                        new[f] = old.get(f)
                if old["people"]:
                    new["people"] = old["people"]
                new["sibling_codes"] = old.get("sibling_codes") or new["sibling_codes"]
                new["alt_codes"] = old.get("alt_codes", set()) | new.get("alt_codes", set())
                new["guides"] = sorted(set(old["guides"] + [t["guide_year"]]))
                new["guide_year"], new["page"] = old["guide_year"], old["page"]
            else:
                new["guides"] = [t["guide_year"]]
                by_number.setdefault(c[2:], key)
            rows[key] = new
    for r in rows.values():
        if not r["start_date"] and r["code"] in program_term:
            r["start_date"], r["end_date"] = program_term[r["code"]]
        if not r["people"]:  # one team, two grant numbers (CV6179, CV6185)
            for s in r.get("sibling_codes") or []:
                if rows.get(s, {}).get("people"):
                    r["people"] = rows[s]["people"]
                    break
    return list(rows.values())


# ---------------------------------------------------------------- award lists
BODY_TOP, BODY_BOTTOM = 85, 740  # running section header (y <= 72) and page footer (y >= 748)


def two_column_lines(page) -> list[str]:
    left, right, _ = page_columns(page)
    return [t for y, t in left if BODY_TOP < y < BODY_BOTTOM] + [t for y, t in right if BODY_TOP < y < BODY_BOTTOM]


def parse_title_name_lists(pdf_path: Path, pages: list[int], scheme_of, kind: str) -> list[dict]:
    """IRG / Sharp sections: CAPS title lines, then 'Name, degrees, [and Name2, degrees,] institution'.
    'CLASS OF YYYY' lines set the year; scheme_of(year, caps_line) may switch the scheme."""
    recs = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        st = {"year": None, "special": None, "after_switch": False}
        for pno in pages:
            lines = two_column_lines(pdf.pages[pno - 1])
            title, names = [], []

            def flush():
                if title and names:
                    recs.append({"year": st["year"], "scheme": st["special"] or scheme_of(st["year"], None),
                                 "title": tidy(" ".join(title)), "names_raw": tidy(" ".join(names)),
                                 "page": pno, "kind": kind})
                    st["special"] = None  # a named sub-award covers exactly one entry

            for l in lines:
                m = re.match(r"^CLASS OF (\d{4})\b\s*(.*)$", l)
                if m:
                    flush()
                    title, names = [], []
                    st["year"], st["special"] = int(m.group(1)), None
                    if m.group(2):
                        title = [m.group(2)]
                    continue
                if is_caps(l):
                    if names:
                        flush()
                        title, names = [], []
                    # a named sub-award heading inside a class ('THE PHILLIP A. SHARP - LAURA ZISKIN /
                    # INNOVATION IN COLLABORATION AWARD'): its scheme applies to the next entry only
                    special = scheme_of(st["year"], " ".join(title + [l]))
                    if special and special != scheme_of(st["year"], None):
                        st["special"], st["after_switch"], title = special, True, []
                        continue
                    if st["after_switch"] and not title and re.match(r"^(RESEARCH GRANT|AWARD|INNOVATION IN COLLABORATION AWARD)$", l):
                        continue
                    st["after_switch"] = False
                    title.append(l)
                elif title:
                    names.append(l)
            flush()
    return recs


def parse_names_inst(raw: str, kind: str) -> list[dict]:
    """IRG: 'Name, MD, PhD, Institution'. Sharp: 'Name, MD, Name2, PhD, and Name3, PhD' (no institutions)."""
    raw = re.sub(r"(?<=[a-z])- (?=[a-z])", "", raw or "")  # hyphenated line breaks
    raw = re.sub(r"\bD Phil\b", "DPhil", raw)
    return people_list(raw, with_inst=(kind == "irg"))


YEAR_LIST_RE = re.compile(r"(20\d{2}):\s*(.+?)(?=\s20\d{2}:|$)")


def parse_named_awards(pdf_path: Path, pages: list[int], guide_year: int) -> list[dict]:
    """'ADDITIONAL AWARDS AND PRIZES' / 'AWARDS AND PRIZES': per award, 'RECIPIENTS: 2019: Name, MD, and Name2, PhD ...'."""
    recs = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for pno in pages:
            left, right, _ = page_columns(pdf.pages[pno - 1])
            for col in (left, right):
                lines = [t for y, t in col if BODY_TOP < y < BODY_BOTTOM]
                award, buf, desc, mode = None, [], [], None
                blocks = []
                for l in lines:
                    if is_caps(l) and not re.match(r"^(RECIPIENTS?|LEADERS):", l) and not re.match(r"^\d+$", l):
                        if mode == "recipients" and buf:
                            blocks.append((award, " ".join(buf), " ".join(desc)))
                            buf, desc, mode = [], [], None
                        award = (award + " " + l) if (award and mode == "title") else l
                        if mode != "title":
                            desc = []
                        mode = "title"
                        continue
                    if re.match(r"^(RECIPIENTS?|LEADERS):", l):
                        mode, buf = "recipients", [re.sub(r"^(RECIPIENTS?|LEADERS):\s*", "", l)]
                        continue
                    if mode == "recipients":
                        buf.append(l)
                    elif mode in ("title", "text"):
                        mode = "text"
                        desc.append(l)
                if mode == "recipients" and buf:
                    blocks.append((award, " ".join(buf), " ".join(desc)))
                for award_name, txt, dtext in blocks:
                    txt = re.sub(r"(?<=[a-z])- (?=[a-z])", "", txt)
                    found = YEAR_LIST_RE.findall(txt)
                    if not found:
                        # 'Two awards, covering the period 2014 - 2017' + 'LEADERS: A, MD, and B, PhD'
                        m = re.search(r"(20\d{2})\s*[–-]\s*(20\d{2})", dtext)
                        if m:
                            found = [(m.group(1), txt)]
                    for yr, names in found:
                        recs.append({"award": tidy(award_name), "year": int(yr), "names_raw": tidy(names),
                                     "page": pno, "guide_year": guide_year})
    return recs


NAMED_AWARDS = [  # (pattern on the printed heading, key, scheme name)
    (r"ZISKIN PRIZE", "ZISKIN", "The Ziskin Prize"),
    (r"JIM TOTH", "TOTH", "Jim Toth Sr. Breakthrough Lung Cancer Research Award"),
    (r"PEGGY PRESCOTT", "PRESCOTT", "SU2C-Peggy Prescott Early Career Scientist Award"),
    (r"GOLDEN ARROW", "GOLDEN-ARROW", "SU2C Golden Arrow Early Career Scientist Award"),
    (r"SHARP TANK", "SHARP-TANK", "SU2C Sharp Tank Early Career Scientist Award"),
    (r"SHARP CHALLENGE", "SHARP-CHALLENGE", "Pancreatic Cancer Collective Phillip A. Sharp Challenge Award"),
    (r"MAVERICK", "MAVERICK", "SU2C Maverick Award"),
    (r"CLESS FAMILY", "CLESS", "Cless Family Foundation Gastric Cancer Innovation in Collaboration Award"),
    (r"NINA NICOLAI", "NICOLAI", "SU2C Nina Nicolai Pancreatic Cancer Innovation in Collaboration Award"),
]


def parse_kimmel(pdf_path: Path, page_no: int) -> list[dict]:
    with pdfplumber.open(str(pdf_path)) as pdf:
        page = pdf.pages[page_no - 1]
        words = page.extract_words()
    # Names are 'Given Family, Degrees' tokens; lines can hold two or three names.
    lines: list[list] = []
    for w in sorted(words, key=lambda w: (w["top"], w["x0"])):
        if lines and abs(lines[-1][0] - w["top"]) < 2.5:
            lines[-1][1].append(w["text"])
        else:
            lines.append([w["top"], [w["text"]]])
    out = []
    started = False
    for _, toks in lines:
        line = " ".join(toks)
        if "Nabeel Bardeesy" in line:
            started = True
        if not started or re.match(r"^\d+$|SCIENTIFIC SUMMIT", line):
            continue
        for m in re.finditer(r"([A-Z][A-Za-z.'’\- ]+?),((?:,?\s*(?:MD|PhD|DVM|MPH|MS|DPhil))+)(?=\s+[A-Z]|$)", line):
            given, family = split_name_particles(m.group(1).strip())
            if family:
                out.append({"given": given, "family": family})
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="SU2C Scientific Summit Program Guides -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N teams (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache PDFs here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    page = get(PORTFOLIO_PAGE)
    for _, url in GUIDES:
        if url.rsplit("/", 1)[-1] not in page:
            log(f"WARNING: {url.rsplit('/', 1)[-1]} no longer linked from the portfolio page (file still fetched)")
    paths = {}
    for gy, url in GUIDES:
        body = cached(args.cache_dir, url.rsplit("/", 1)[-1], url, binary=True)
        path = (args.cache_dir or args.output_dir) / url.rsplit("/", 1)[-1]
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_bytes(body)
        paths[gy] = path
        log(f"guide {gy}: {len(body) / 1e6:.1f} MB")

    # ---- teams
    all_teams = []
    for gy, _ in GUIDES:
        t = parse_teams(paths[gy], gy)
        log(f"guide {gy}: {len(t)} team headers")
        all_teams += t
    teams = merge_teams(all_teams)
    teams.sort(key=lambda t: t["code"])
    if args.limit:
        teams = teams[: args.limit]
    log(f"teams: {len(teams)} grant numbers")

    rows = []
    for t in teams:
        ppl = [x for x in t["people"] if x["role"] in INVESTIGATOR_ROLES]
        leads = [x for x in ppl if x["role"] in LEAD_ROLES] or [x for x in ppl if x["role"] in CO_ROLES]
        lead = leads[0] if leads else None
        co = ([x for x in leads[1:]] + [x for x in ppl if x["role"] in CO_ROLES and x is not lead])
        co = co[0] if co else None
        inst_fallback = None
        if lead and not lead["inst"] and t["team_name"] and not re.search(r"TEAM|PROGRAM", t["team_name"]):
            inst_fallback = t["team_name"].title().replace(" Of ", " of ").replace(" The ", " the ")
        canada = bool(re.search(r"\bSU2C CANADA\b|STAND UP TO CANCER CANADA", t["team_name"] or ""))
        rows.append({
            "funder_award_id": t["code"],
            "award_kind": "team",
            "route_funder_id": str(SU2C_CANADA_ID if canada else SU2C_ID),
            "title": t["title"],
            "team_name": t["team_name"],
            "funder_scheme": t["team_name"],
            "program": {"DT": "Dream Team", "RT": "Research Team", "CT": "Catalyst", "CV": "Convergence",
                        "BT": "Health Equity Breakthrough Team"}[t["code"][:2]],
            "start_date": t["start_date"], "end_date": t["end_date"],
            "start_year": t["start_date"][:4] if t["start_date"] else None,
            "lead_given_name": lead["given"] if lead else None,
            "lead_family_name": lead["family"] if lead else None,
            "lead_institution": (lead["inst"] or inst_fallback) if lead else None,
            "lead_role": lead["role"] if lead else None,
            "co_lead_given_name": co["given"] if co else None,
            "co_lead_family_name": co["family"] if co else None,
            "co_lead_institution": co["inst"] if co else None,
            "investigators_json": json.dumps([{k: x[k] for k in ("role", "given", "family", "inst")} for x in ppl]),
            "sibling_codes": ",".join(t.get("sibling_codes") or []) or None,
            "alt_codes": ",".join(sorted(t.get("alt_codes") or [])) or None,
            "amount": None,
            "source_guides": ",".join(str(g) for g in t["guides"]),
            "source_pdf": dict(GUIDES)[t["guide_year"]],
            "source_page": str(t["page"]),
        })

    # ---- individual grants and awards (2024 guide sections + 2026 awards pages)
    g24, g26 = paths[2024], paths[2026]

    def irg_scheme(year, caps):
        if caps and "SELIG" in caps and "MELANOMA" in caps:
            return "Allan H. (Bud) and Sue Selig Stand Up To Cancer Melanoma Innovative Research Grant"
        return f"SU2C Innovative Research Grant (Class of {year})" if year else None

    def sharp_scheme(year, caps):
        if caps and "LAURA ZISKIN" in caps and "INNOVATION IN COLLABORATION" in caps and "PHILLIP" in caps:
            return "Phillip A. Sharp - Laura Ziskin Innovation in Collaboration Award"
        return f"Phillip A. Sharp Innovation in Collaboration Award (Class of {year})" if year else None

    singles = []
    for r in parse_title_name_lists(g24, [196, 197, 198], irg_scheme, "irg"):
        r["people"] = parse_names_inst(r["names_raw"], "irg")
        singles.append(r)
    for r in parse_title_name_lists(g24, [199, 200, 201], sharp_scheme, "sharp"):
        r["people"] = parse_names_inst(r["names_raw"], "sharp")
        singles.append(r)
    for r in parse_title_name_lists(g26, [86], sharp_scheme, "sharp"):
        if r["year"] == 2024:  # class of 2024 is only in the 2026 guide
            r["people"] = parse_names_inst(r["names_raw"], "sharp")
            singles.append(r)
    for r in singles:
        if not r["people"]:
            continue
        lead = r["people"][0]
        co = r["people"][1] if len(r["people"]) > 1 else None
        key = "IRG" if r["kind"] == "irg" else "SHARP"
        if r["scheme"] and r["scheme"].startswith("Phillip A. Sharp - Laura Ziskin"):
            key = "SHARP-ZISKIN"
        if r["scheme"] and r["scheme"].startswith("Allan H. (Bud)"):
            key = "IRG-SELIG"
        rows.append({
            "funder_award_id": f"SU2C-{key}-{r['year']}-{slug(lead['family'])}",
            "award_kind": r["kind"], "route_funder_id": str(SU2C_ID),
            "title": r["title"],  # printed in capitals; kept as printed
            "team_name": None, "funder_scheme": r["scheme"],
            "program": "Innovative Research Grant" if r["kind"] == "irg" else "Phillip A. Sharp Innovation in Collaboration Award",
            "start_date": None, "end_date": None, "start_year": str(r["year"]) if r["year"] else None,
            "lead_given_name": lead["given"], "lead_family_name": lead["family"], "lead_institution": lead["inst"],
            "lead_role": "Recipient",
            "co_lead_given_name": co["given"] if co else None, "co_lead_family_name": co["family"] if co else None,
            "co_lead_institution": co["inst"] if co else None,
            "investigators_json": json.dumps([dict(x, role="Recipient") for x in r["people"]]),
            "sibling_codes": None, "alt_codes": None, "amount": None,
            "source_guides": "2026" if r.get("page") == 86 and r["kind"] == "sharp" and r["year"] == 2024 else "2024",
            "source_pdf": dict(GUIDES)[2026 if (r["kind"] == "sharp" and r["year"] == 2024) else 2024],
            "source_page": str(r["page"]),
        })

    named = parse_named_awards(g24, [202, 203], 2024) + parse_named_awards(g26, [86, 87], 2026)
    seen = set()
    for r in named:
        people = parse_names_inst(r["names_raw"], "sharp")
        if not people or not r["award"]:
            continue
        aw = r["award"]
        hit = next(((k, s) for pat, k, s in NAMED_AWARDS if re.search(pat, aw)), None)
        if not hit:
            raise SystemExit(f"unknown named award heading (add it to NAMED_AWARDS): {aw!r}")
        akey, scheme_name = hit
        lead = people[0]
        co = people[1] if len(people) > 1 else None
        fid = f"SU2C-{akey}-{r['year']}-{slug(lead['family'])}"
        if fid.lower() in seen:
            continue
        seen.add(fid.lower())
        is_ziskin = akey == "ZISKIN"
        rows.append({
            "funder_award_id": fid, "award_kind": "named_award", "route_funder_id": str(SU2C_ID),
            "title": None, "team_name": None, "funder_scheme": scheme_name,
            "program": "Award", "start_date": None, "end_date": None, "start_year": str(r["year"]),
            "lead_given_name": lead["given"], "lead_family_name": lead["family"], "lead_institution": None,
            "lead_role": "Recipient",
            "co_lead_given_name": co["given"] if co else None, "co_lead_family_name": co["family"] if co else None,
            "co_lead_institution": None,
            "investigators_json": json.dumps([dict(x, role="Recipient") for x in people]),
            "sibling_codes": None, "alt_codes": None,
            "amount": 250000.0 if is_ziskin else None,  # "a one-year, $250,000 grant ... shared by two scientists"
            "source_guides": str(r["guide_year"]), "source_pdf": dict(GUIDES)[r["guide_year"]],
            "source_page": str(r["page"]),
        })

    for k in parse_kimmel(g24, 204):
        rows.append({
            "funder_award_id": f"SU2C-KIMMEL-{slug(k['given'] or '')}-{slug(k['family'])}",
            "award_kind": "kimmel", "route_funder_id": str(SU2C_ID), "title": None, "team_name": None,
            "funder_scheme": "SU2C Kimmel Scholars (2009-2011)", "program": "SU2C Kimmel Scholar",
            "start_date": None, "end_date": None, "start_year": None,
            "lead_given_name": k["given"], "lead_family_name": k["family"], "lead_institution": None,
            "lead_role": "Scholar", "co_lead_given_name": None, "co_lead_family_name": None,
            "co_lead_institution": None, "investigators_json": None, "sibling_codes": None, "alt_codes": None,
            "amount": None, "source_guides": "2024", "source_pdf": dict(GUIDES)[2024], "source_page": "204",
        })

    df = pd.DataFrame(rows)
    if df["funder_award_id"].str.lower().duplicated().any():
        d = df[df["funder_award_id"].str.lower().duplicated(keep=False)]
        raise SystemExit(f"duplicate funder_award_id:\n{d[['funder_award_id', 'title']].to_string()}")
    log(f"rows: {len(df)} by kind {df['award_kind'].value_counts().to_dict()}; "
        f"routed to SU2C Canada: {(df['route_funder_id'] == str(SU2C_CANADA_ID)).sum()}")
    for c in ["title", "lead_family_name", "lead_institution", "start_date", "start_year", "amount"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "su2c_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_su2c_projects.parquet"
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
