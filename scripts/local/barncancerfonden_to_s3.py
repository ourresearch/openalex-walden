#!/usr/bin/env python3
"""
Barncancerfonden (Swedish Childhood Cancer Fund) to S3 Data Pipeline
====================================================================

Barncancerfonden publishes one decision list per call ("Beviljade ansökningar
<CODE><YEAR>") on
https://www.barncancerfonden.se/for-forskare/forskningsanslag/beviljade-forskningsanslag-och-tjanster/
(current year at the top, "Beviljade anslag genom åren" accordion back to
2014). ~220 files: xlsx/xls (2019+), PDF tables (2014-2021, some 2023/2026)
and one docx. Every tabular list carries the application number ("Ans no",
e.g. PR2025-0011, KP2016-0006, Rc2022-0001), the same string grantees cite in
acknowledgements (PR2017-0082, TJ2021-0125 ... in the Crossref/Europe PMC
work-funder stubs), plus Swedish title, administering organisation and
department, the applicant, years and the granted amount in SEK.

SweCRIS (swecris-api.vr.se) does not carry Barncancerfonden (full scan
2026-10-01: 15 funding orgs, none of them Barncancerfonden).

Parsing: xlsx/xls via pandas, PDFs via pdfplumber lattice tables (the lists
are ruled tables), docx via python-docx. The header row is located by its
"Ans no" cell and columns are mapped by header name; rows without an
application number continue the previous row (wrapped PDF cells).

The 2014-2022 research-position lists ("Beviljade tjänster/forskningstjänster",
TJ/NBCNS) are prose without application numbers: parsed with
(default; --skip-positions to omit) into synthetic BCF-<code><year>-<family>-<given> keys.
Two of them (TJ2021, 2024) and Rb2019 are outlined-glyph PDFs with no text
layer; they are skipped and logged.

Scope: every Barncancerfonden call is kept (project grants, clinical
projects, medtech, planning, implementation, EU co-funding, infrastructure
programmes, research positions, start-up grants, plus travel, symposium,
methodology/course and guest-lecturer grants for childhood-cancer
researchers). funding_type distinguishes them.

Output: s3://openalex-ingest/awards/barncancerfonden/barncancerfonden_projects.parquet
"""

import argparse
import html
import io
import json
import re
import time
import unicodedata
import warnings
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (runbook 4.0 grep marker: sys.stdout.reconfigure)
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

warnings.filterwarnings("ignore")

BASE = "https://www.barncancerfonden.se"
LIST_URL = f"{BASE}/for-forskare/forskningsanslag/beviljade-forskningsanslag-och-tjanster/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/barncancerfonden/barncancerfonden_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.4
RETRIES = 3

# Decision lists Barncancerfonden still hosts on its own domain but no longer
# links from the listing page (found 2026-10-01 by the site's own URL pattern;
# the listing's 2017 and 2019 sections omit PR/KP/KF/PL/ST and PR).
_GA = f"{BASE}/globalassets/bilder/forskning/beviljade-forskningsanslag"
EXTRA_FILES = [
    ("2017", "Beviljade projektanslag PR2017 (unlinked)", f"{_GA}/hosten-2017/beviljade-projektanslag-pr2017.pdf"),
    ("2017", "Beviljade kliniska projektanslag KP2017 (unlinked)", f"{_GA}/hosten-2017/beviljade-kliniska-projektanslag-kp2017.pdf"),
    ("2017", "Beviljade kliniska forskarmånader KF2017 (unlinked)", f"{_GA}/hosten-2017/beviljade-kliniska-forskarmanader-kf2017.pdf"),
    ("2017", "Beviljade planeringsanslag PL2017 (unlinked)", f"{_GA}/hosten-2017/beviljade-planeringsanslag-pl2017.pdf"),
    ("2017", "Beviljade forskar-ST-tjänster ST2017 (unlinked)", f"{_GA}/hosten-2017/beviljade-forskar-st-tjanster-st2017.pdf"),
    ("2019", "Beviljade projektanslag PR2019 (unlinked)", f"{_GA}/hosten-2019/beviljade-pr-2019_web.xls"),
]

ID_RE = re.compile(r"^([A-Za-z]{1,5})\s?(\d{4})\s?[-/–‐]\s?(\d{3,4})$")

# application-number prefix -> (programme name, funding_type)
SCHEMES = {
    "PR": ("Projektanslag", "research"),
    "KP": ("Kliniska projektanslag", "research"),
    "MT": ("Projektanslag medicinsk teknik", "research"),
    "MTI": ("Projektanslag medicinsk teknik (med MedTech4Health)", "research"),
    "PL": ("Planeringsanslag", "research"),
    "EU": ("Medfinansiering av EU-projekt", "research"),
    "IM": ("Implementeringsanslag", "research"),
    "OB": ("Ovanliga barncancersjukdomar", "research"),
    "NCP": ("Projektanslag inom NBCNS", "research"),
    "TSK": ("Program (TSK)", "research"),
    "FSU": ("Uppstartsbidrag", "research"),
    "UBB": ("Uppstartsbidrag", "research"),
    "UB": ("Uppstartsbidrag", "research"),
    "KF": ("Kliniska forskarmånader / barncancerforskarmånader för kliniker", "fellowship"),
    "ST": ("Forskar-ST-tjänster", "fellowship"),
    "DK": ("Doktorandbidrag för kliniker", "fellowship"),
    "PD": ("Barncancerforskningspostdoktortjänster", "fellowship"),
    "FT": ("Barncancerforskartjänster", "fellowship"),
    "HFT": ("Högre barncancerforskartjänster", "fellowship"),
    "TJ": ("Forskartjänster", "fellowship"),
    "RA": ("Reseanslag", "travel"),
    "RB": ("Reseanslag", "travel"),
    "RC": ("Reseanslag", "travel"),
    "SYA": ("Symposier", "conference"),
    "SYB": ("Symposier", "conference"),
    "SYC": ("Symposier", "conference"),
    "GFA": ("Gästföreläsare", "conference"),
    "GFC": ("Gästföreläsare", "conference"),
    "MEA": ("Metodikstudier/utbildningskurser", "training"),
    "MEB": ("Metodikstudier/utbildningskurser", "training"),
    "MEC": ("Metodikstudier/utbildningskurser", "training"),
    "FKA": ("Forskarutbildningskurser", "training"),
    "FKC": ("Forskarutbildningskurser", "training"),
}

# header text (lower, collapsed) -> canonical field
HEADER_MAP = [
    (r"^ans\.?\s*n[or]", "app_no"),
    (r"^titel engelska", "title_en"),
    (r"^titel", "title"),
    (r"förv\.?\s*o\.?\s*inst", "department"),
    (r"förv\.?\s*organ|arbetsställe|lärosäte", "organisation"),
    (r"^huvudsökande|^anslagsansvarig", "applicant"),
    (r"^efternamn", "family_name"),
    (r"^förnamn", "given_name"),
    (r"^anslagsår|^antal år", "years"),
    (r"^resmål", "destination_region"),
    (r"^land", "destination"),
    (r"^tjänst", "position"),
    (r"^typ proj", "project_type"),
    (r"^grupp", "committee"),
    (r"från barncancerfonden", "amount"),
    (r"från medtech|^totalt beviljat belopp", "amount_total_cofunded"),
    (r"^sökt belopp", "amount_requested"),
    (r"^beslut", "decision"),
    (r"belopp|^beviljat", "amount"),
]


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> requests.Response:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90, allow_redirects=True)
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def clean(s) -> str | None:
    if s is None:
        return None
    s = html.unescape(str(s)).replace("\xa0", " ").replace("​", "")
    s = re.sub(r"\s+", " ", s).strip()
    return None if s in ("", "nan", "None") else s


def list_files(page: str) -> list[dict]:
    """Every decision-list link in the article body, with its accordion year."""
    body = page[page.find('<main id="content"'):]
    body = body[: body.find("facebook.com/sharer")]
    out, seen, section = [], set(), "current"
    for href, txt in re.findall(r'<a[^>]*href="([^"]*)"[^>]*>(.*?)</a>', body, re.S):
        label = clean(re.sub(r"<[^>]+>", " ", txt)) or ""
        if href.startswith("#"):
            section = label
            continue
        if "/link/" in href or "/contentassets/" in href or "/globalassets/" in href:
            url = BASE + href if href.startswith("/") else href
            if url not in seen:
                seen.add(url)
                out.append({"section": section, "label": label, "href": url})
    return out


def read_tables(b: bytes) -> tuple[str, list[list[list]]]:
    if b[:4] == b"%PDF":
        import pdfplumber
        out = []
        with pdfplumber.open(io.BytesIO(b)) as p:
            for pg in p.pages:
                out.extend(pg.extract_tables())
        return "pdf", out
    if b[:2] == b"PK" and "word/document.xml" in zipfile.ZipFile(io.BytesIO(b)).namelist():
        import docx
        d = docx.Document(io.BytesIO(b))
        return "docx", [[[c.text for c in r.cells] for r in t.rows] for t in d.tables]
    if b[:2] == b"PK" or b[:4] == b"\xd0\xcf\x11\xe0":
        xs = pd.read_excel(io.BytesIO(b), header=None, sheet_name=None, dtype=str,
                           engine="openpyxl" if b[:2] == b"PK" else "xlrd")
        return "xls", [x.fillna("").values.tolist() for x in xs.values()]
    return "unknown", []


TEXT_FIELDS = {"title", "title_en", "department", "organisation", "applicant", "family_name", "given_name",
               "destination_region", "destination", "position", "project_type"}


def map_header(row: list) -> dict[int, str] | None:
    cells = [(clean(c) or "").lower().lstrip("'") for c in row]
    if not any(re.match(r"^ans\.?\s*n[or]", c) for c in cells):
        return None
    m = {}
    for i, c in enumerate(cells):
        for pat, field in HEADER_MAP:
            if c and re.search(pat, c):
                if field not in m.values():
                    m[i] = field
                break
    # blank header right after "Efternamn" holds the given name (Rb2023)
    fam = [i for i, f in m.items() if f == "family_name"]
    if fam and "given_name" not in m.values() and fam[0] + 1 < len(cells) and not cells[fam[0] + 1]:
        m[fam[0] + 1] = "given_name"
    return m


def parse_amount(s) -> float | None:
    s = clean(s)
    if not s:
        return None
    s = re.sub(r"(?i)kr|sek|:-", "", s)
    s = re.sub(r"\.0+$", "", s.strip())
    digits = re.sub(r"[^\d]", "", s)
    if not digits or len(digits) > 10:
        return None
    return float(digits)


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|docent|med\.?\s*dr)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) with a leading honorific
    strip. Barncancerfonden mostly prints "Family, Given" (handled by the
    caller); this is the fallback for "Given Family"."""
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


def person(applicant: str | None, given: str | None, family: str | None) -> tuple[str | None, str | None]:
    if family or given:
        return clean(given), clean(family)
    a = clean(applicant)
    if not a:
        return None, None
    if "," in a:
        fam, giv = a.split(",", 1)
        return clean(giv), clean(fam)
    return split_name(a)


def norm_id(s: str | None) -> tuple[str, str, int] | None:
    s = clean(s)
    if not s:
        return None
    m = ID_RE.match(s)
    if not m:
        return None
    prefix, year, num = m.groups()
    return f"{prefix}{year}-{num}", prefix.upper(), int(year)


def parse_file(item: dict, b: bytes) -> tuple[str, list[dict]]:
    kind, tables = read_tables(b)
    recs, hdr = [], None
    for tb in tables:
        for row in tb:
            if not row:
                continue
            h = map_header(row)
            if h:
                hdr = h
                continue
            if hdr is None:
                # headerless continuation table (2021 PDFs): reuse a generic layout only
                # when the first cell is an application number; decided per file below
                continue
            vals = {f: clean(row[i]) if i < len(row) else None for i, f in hdr.items()}
            nid = norm_id(vals.get("app_no"))
            if nid:
                vals["_id"] = nid
                recs.append(vals)
            elif recs and not vals.get("app_no"):
                if any(re.search(r"(?i)^(totalt?|summa)\b", clean(c) or "") for c in row):
                    continue  # footer total row
                # wrapped PDF row: append TEXT cells to the previous record (never
                # numeric ones: a trailing row with only a number is the call total)
                prev = recs[-1]
                for f, v in vals.items():
                    if v and f in TEXT_FIELDS:
                        prev[f] = f"{prev[f]} {v}" if prev.get(f) else v
    if not recs and tables:
        # headerless 2021 PDFs: columns follow the 2021 layout
        # [Ans no, Titel, (Resmål, Land,) Förv. organ, Förv. o. inst, Efternamn, Förnamn, Belopp]
        for tb in tables:
            for row in tb:
                cells = [clean(c) for c in row]
                nid = norm_id(cells[0]) if cells else None
                if not nid:
                    continue
                n = len(cells)
                layout = (["app_no", "title", "destination_region", "destination", "organisation", "department",
                           "family_name", "given_name", "amount"] if n >= 9 else
                          ["app_no", "title", "organisation", "department", "family_name", "given_name", "amount"])
                vals = {f: cells[i] if i < n else None for i, f in enumerate(layout)}
                vals["_id"] = nid
                vals["_headerless"] = True
                recs.append(vals)
    out = []
    for v in recs:
        for f in TEXT_FIELDS:  # a merged footer cell ("Helena / Totalt") leaks the total label
            if v.get(f):
                v[f] = clean(re.sub(r"\s+Totalt?(\s.*)?$", "", v[f]))
        app_no, prefix, year = v["_id"]
        given, family = person(v.get("applicant"), v.get("given_name"), v.get("family_name"))
        amount = parse_amount(v.get("amount"))
        if amount is None and v.get("decision") and re.search(r"\d", v["decision"]):
            amount = parse_amount(v["decision"])
        scheme_key = prefix if prefix in SCHEMES else prefix[:2]
        scheme, ftype = SCHEMES.get(scheme_key, (None, "research"))
        title = v.get("title") or v.get("title_en")
        dest = ", ".join(x for x in [v.get("destination"), v.get("destination_region")] if x) or None
        out.append({
            "application_number": app_no,
            "programme_code": prefix,
            "call_year": str(year),
            "title": title,
            "title_language": "en" if v.get("title_en") and not v.get("title") else "sv",
            "organisation": v.get("organisation"),
            "department": v.get("department"),
            "applicant_raw": v.get("applicant") or ", ".join(x for x in [v.get("family_name"), v.get("given_name")] if x) or None,
            "lead_given_name": given,
            "lead_family_name": family,
            "years": v.get("years"),
            "position": v.get("position"),
            "destination": dest,
            "project_type": v.get("project_type"),
            "committee": v.get("committee"),
            "decision": v.get("decision"),
            "amount_text": v.get("amount"),
            "amount": amount,
            "amount_total_cofunded": parse_amount(v.get("amount_total_cofunded")),
            "currency": "SEK" if amount is not None else None,
            "funder_scheme": scheme,
            "funding_type": ftype,
            "source_file": item["final_url"],
            "source_label": item["label"],
            "source_section": item["section"],
            "source_kind": kind + ("+headerless" if v.get("_headerless") else ""),
        })
    return kind, out


# ---------------------------------------------------------------------------
# Prose position lists (2014-2022 "Beviljade tjänster"; NBCNS 2014/2016)
# ---------------------------------------------------------------------------

SECTION_RE = re.compile(
    r"^(Forskartjänster|Forskarassistenttjänster|Kliniska forskarmånader|Postdoktor|Forskar-ST|"
    r"Högre barncancerforskartjänster|Barncancerforskartjänster|Barncancerforskarmånader|"
    r"Barncancerforskningspostdoktor|Doktorandbidrag|Doktorandtjänst|Beviljade projektansökningar|"
    r"Beviljade tjänster inom|Tjänster inom|Projektanslag|Forskningsprojekt)", re.I)
POSITION_RE = re.compile(
    r"(heltid|halvtid|deltid|\d+\s*%|månader per år|forskartjänst|postdoc|postdoktor|stipendium|"
    r"forskningsprojekt, \d|driftsbidrag|år$|\d år)", re.I)
SKIP_RE = re.compile(
    r"^(Beviljade|Vid forskningsnämnd|Av \d+ inkomna|Anslagstiden|Sida \d|Barncancerfonden$|Reserv|"
    r"Heltidstjänster|Deltidstjänster|Totalt beviljat belopp|ansökningar om totalt|om totalt|Tjänsterna)", re.I)
UNI_RE = re.compile(
    r"(universitet|institutet|högskola|sjukhus|sjukhuset|universitetssjukhus|Region|Akademi|"
    r"Stockholm|Göteborg|Lund|Uppsala|Umeå|Linköping|Örebro|Malmö|Huddinge|Solna)", re.I)


def prose_lines(b: bytes) -> list[str]:
    if b[:4] == b"%PDF":
        import pdfplumber
        with pdfplumber.open(io.BytesIO(b)) as p:
            text = "\n".join(pg.extract_text() or "" for pg in p.pages)
    else:
        import docx
        text = "\n".join(p.text for p in docx.Document(io.BytesIO(b)).paragraphs)
    return [clean(x) for x in text.split("\n") if clean(x)]



NAME_TOK = r"[A-ZÅÄÖÉÜØÆÇŠŽ][\w'’\-\.]*"
INST_WORD_RE = re.compile(r"(universitet|institutet|högskola|sjukhus|akademi|hospital|klinik|centrum|rehabmottagning)", re.I)
ORG_HEADS = {"Karolinska", "Lunds", "Uppsala", "Umeå", "Göteborgs", "Linköpings", "Örebro", "Stockholms",
             "Skånes", "Sahlgrenska", "Akademiska", "Kungl.", "Kungliga", "Marie", "Ersta", "Sophiahemmet",
             "Region", "Danderyds", "Norrlands", "Astrid", "Chalmers", "KTH", "Högskolan", "Rigshospitalet",
             "Närhälsan", "Drottning", "Malmö", "Mälardalens", "Karlstads", "Jönköping", "Linnéuniversitetet",
             "Mittuniversitetet", "Universitetssjukhuset", "Södersjukhuset", "Barnsjukhuset", "Röda"}
CITIES = {"Stockholm", "Göteborg", "Lund", "Uppsala", "Umeå", "Linköping", "Örebro", "Malmö", "Huddinge",
          "Solna", "Helsingfors", "Köpenhamn", "Oslo", "Aarhus"}


def org_start(toks: list[str], lo: int) -> int | None:
    for k in range(lo, len(toks)):
        t, nxt = toks[k], (toks[k + 1] if k + 1 < len(toks) else "")
        if t in ORG_HEADS or t in CITIES or INST_WORD_RE.search(t) or (t[:1].isupper() and INST_WORD_RE.match(nxt)):
            return k
    return None


def name_line(line: str) -> dict | None:
    """A grantee line in the prose position lists, in any of the layouts used
    2014-2022: 'Järås, Marcus Lunds universitet' / 'Bexell, Daniel<TAB>Lunds
    universitet' / 'Lain Sonia, Stockholm' (2014-15: family first, city) /
    'Ninib Baryawno Karolinska institutet' (2022: given first)."""
    line = line.replace("\t", " ")
    m = re.match(rf"^({NAME_TOK}(?: {NAME_TOK})*), (.+)$", line)
    if m:
        rest = m.group(2).split()
        if org_start(rest, 0) == 0 and 2 <= len(m.group(1).split()) <= 4:
            toks = m.group(1).split()  # 2014-15 layout: 'Family Given, City/Institution'
            return {"family": " ".join(toks[:-1]), "given": toks[-1], "org": m.group(2)}
        k = org_start(rest, 1)
        if k is not None and all(re.match(NAME_TOK, t) for t in rest[:k]) and k <= 3:
            return {"family": m.group(1), "given": " ".join(rest[:k]), "org": " ".join(rest[k:])}
        # 'Lain Sonia, Stockholm' / 'Vilborg Hartwig Anna, Stockholm'
        if len(rest) <= 2 and rest[0] in CITIES | ORG_HEADS | {"Umeå", "Linköping"} or (len(rest) == 1 and rest[0][:1].isupper() and rest[0] not in ("Halvtid", "Heltid")):
            toks = m.group(1).split()
            if 2 <= len(toks) <= 4:
                return {"family": " ".join(toks[:-1]), "given": toks[-1], "org": m.group(2)}
        return None
    toks = line.split()
    if len(toks) < 3 or not all(re.match(NAME_TOK, t) for t in toks[:2]):
        return None
    k = org_start(toks, 2)
    if k is None or k > 4 or not all(re.match(NAME_TOK, t) for t in toks[:k]):
        return None
    given, family = split_name(" ".join(toks[:k]))
    return {"family": family, "given": given, "org": " ".join(toks[k:])}


def parse_positions(item: dict, b: bytes, year: int, code: str) -> list[dict]:
    lines = prose_lines(b)
    if sum(len(x) for x in lines) < 200:
        return []
    out, section = [], None

    def is_header(k: int) -> bool:
        """A section heading is followed by 'Beviljade(:)' / 'Av N inkomna ...' /
        'Heltidstjänster:'; a position line ('Kliniska forskarmånader, 3 månader
        per år ...') that merely starts like a heading is not."""
        nxt = lines[k + 1] if k + 1 < len(lines) else ""
        return (not name_line(lines[k]) and len(lines[k]) < 80 and not SKIP_RE.match(lines[k])
                and bool(re.match(r"^(Beviljade:?$|Av \d+ inkomna)", nxt)))

    i = 0
    while i < len(lines):
        ln = lines[i]
        if ln in ("Beviljade", "Beviljade:"):
            i += 1
            continue
        if is_header(i) or (SECTION_RE.match(ln) and not name_line(ln) and not POSITION_RE.search(ln.split(",")[0])
                            and not re.search(r",\s*(\d|heltid|halvtid|deltid)", ln, re.I)):
            section = re.sub(r"^Beviljade\s+", "", re.sub(r",.*$", "", ln))
            i += 1
            continue
        if SKIP_RE.match(ln):
            i += 1
            continue
        rec = name_line(ln)
        if rec is None:
            i += 1
            continue
        # following lines until next name/section: title + position description
        j, title_parts, pos_parts = i + 1, [], []
        while j < len(lines):
            nx = lines[j]
            if SKIP_RE.match(nx) or is_header(j) or name_line(nx) or nx in ("Beviljade", "Beviljade:"):
                break
            if SECTION_RE.match(nx) and not re.search(r",\s*(\d|heltid|halvtid|deltid)|\d+ år", nx, re.I):
                break
            if (POSITION_RE.search(nx) and len(nx) < 90 and re.match(r"^(Heltid|Halvtid|Deltid|\d|Forskar|Postdoc|Postdoktor|Barncancer|Kliniska|Doktorand|Forskningsprojekt|Halvtid|80%|\d+ ?%|Forskar-ST|ST-)", nx, re.I)):
                pos_parts.append(nx)
            elif title_parts and pos_parts:
                break
            else:
                title_parts.append(nx)
            j += 1
        amount = None
        tot = re.search(r"Totalt beviljat belopp:\s*([\d  ]+)\s*kr", " ".join(lines[j:j + 1]))
        if tot:
            amount = parse_amount(tot.group(1))
            j += 1
        title = clean(" ".join(title_parts))
        out.append({
            "section": section, "family": clean(rec["family"]), "given": clean(rec["given"]), "org": clean(rec["org"]),
            "title": title, "position": clean("; ".join(pos_parts)), "amount": amount,
        })
        i = j
    recs, used = [], {}
    for k, r in enumerate(out):
        slug = re.sub(r"[^a-z0-9]+", "-", unicodedata.normalize("NFKD", f"{r['family']}-{r['given'] or ''}")
                      .encode("ascii", "ignore").decode().lower()).strip("-")
        sec = (r["section"] or "").lower()
        ncp = code.upper().startswith("NC") or "nbcns" in item["label"].lower()
        key = f"BCF-{code}{year}-{slug}"
        if r["title"] and any(x["title"] == r["title"] for x in recs if x["application_number"].startswith(key)):
            continue  # repeated entry (same person, same title) in one list
        used[key] = used.get(key, 0) + 1
        if used[key] > 1:
            key = f"{key}-{used[key]}"  # same person granted twice in one list (Grillner ST2022)
        recs.append({
            "application_number": key,
            "programme_code": code,
            "call_year": str(year),
            "title": r["title"],
            "title_language": "sv",
            "organisation": r["org"],
            "department": None,
            "applicant_raw": f"{r['family']}, {r['given']}",
            "lead_given_name": r["given"],
            "lead_family_name": r["family"],
            "years": None,
            "position": r["position"],
            "destination": None,
            "project_type": r["section"],
            "committee": None,
            "decision": None,
            "amount_text": None,
            "amount": r["amount"],
            "amount_total_cofunded": None,
            "currency": "SEK" if r["amount"] is not None else None,
            "funder_scheme": ("NBCNS: " if ncp else "") + (r["section"] or "Forskartjänster"),
            "funding_type": "research" if ("projekt" in sec and "tjänst" not in sec) else "fellowship",
            "source_file": item["final_url"],
            "source_label": item["label"],
            "source_section": item["section"],
            "source_kind": "prose",
        })
    return recs


def main() -> None:
    p = argparse.ArgumentParser(description="Barncancerfonden decision lists -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N decision-list files")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache downloaded files here")
    p.add_argument("--skip-positions", action="store_true", help="do not parse the prose position lists")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    files = list_files(get(LIST_URL).text)
    log(f"Listing: {len(files)} decision-list files (+{len(EXTRA_FILES)} unlinked)")
    files += [{"section": sec, "label": lab, "href": url} for sec, lab, url in EXTRA_FILES]
    if args.limit:
        files = files[: args.limit]

    rows, skipped = [], []
    for i, it in enumerate(files):
        cache = None
        if args.cache_dir:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache = args.cache_dir / (re.sub(r"[^\w.-]", "_", it["href"].rsplit("/", 1)[-1])[:80])
        meta = cache.with_suffix(cache.suffix + ".json") if cache else None
        if cache and cache.exists() and meta.exists():
            b = cache.read_bytes()
            it["final_url"] = json.loads(meta.read_text())["final_url"]
        else:
            r = get(it["href"])
            b, it["final_url"] = r.content, r.url
            if cache:
                cache.write_bytes(b)
                meta.write_text(json.dumps({"final_url": r.url}))
            time.sleep(REQUEST_DELAY)
        try:
            kind, recs = parse_file(it, b)
        except Exception as e:  # noqa: BLE001
            kind, recs = f"error {e}", []
        if not recs and not args.skip_positions:
            m = re.search(r"(TJ|NBCNS|NCp)?\s*(20\d\d)", it["label"])
            year = int(m.group(2)) if m else None
            code = "NC" if "nbcns" in it["label"].lower() else "TJ"
            if year and re.search(r"tjänster|nbcns|TJ20\d\d", it["label"], re.I):
                recs = parse_positions(it, b, year, code)
                kind = "prose"
        if not recs:
            skipped.append((it["label"], it["final_url"], kind))
        rows.extend(recs)
        log(f"  [{i + 1}/{len(files)}] {kind:6s} {len(recs):3d} rows  {it['section']} | {it['label']}")

    df = pd.DataFrame(rows)
    before = len(df)
    key = df["application_number"].str.lower()
    dup = df[key.duplicated(keep=False)].sort_values("application_number")
    for an, g in dup.groupby("application_number"):
        log(f"  duplicate {an}: {g['source_label'].tolist()} titles equal={g['title'].nunique() == 1}")
    df = df.loc[~key.duplicated(keep="first")].reset_index(drop=True)
    log(f"Parsed {before} rows, {before - len(df)} duplicate application numbers dropped -> {len(df)}")
    for lab, url, kind in skipped:
        log(f"  no rows: {lab} ({kind}) {url}")
    for c in ["title", "organisation", "lead_family_name", "lead_given_name", "amount"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total SEK {df['amount'].sum():,.0f}")
    log("  by funding_type: " + json.dumps(df["funding_type"].value_counts().to_dict()))

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "barncancerfonden_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_barncancerfonden_projects.parquet"
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
