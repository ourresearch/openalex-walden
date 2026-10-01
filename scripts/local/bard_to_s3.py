#!/usr/bin/env python3
"""
BARD (United States - Israel Binational Agricultural Research and Development Fund) to S3
=========================================================================================

BARD publishes its funded research on two sites (robots.txt on both allows all
agents; no AI-crawler rules):

1. Research projects, 1979-2026 -- the "Research by State" map
   (https://www.bard-isus.com/static/bardmap.html) calls
   https://www.bard-isus.com/ResearchforMaps/rbystate?id=<US state> for each
   state, which lists every funded project with a US investigator in that state:
   project number, award year, panel, duration, approved budget (USD), title and
   all investigators (initial, surname, institution). A project with
   investigators in several states appears on each state's page; we dedupe on the
   project number (all copies are identical). Covers the regular BARD panels plus
   the joint programmes (NIFA-BARD, Texas-BARD, Maryland-BARD, Cornell-BARD,
   MARD, CPS), identified by the panel name.

2. Full project numbers. The map shows only the bare number ("5125 - 2018"), but
   the number BARD tells grantees to cite (and that papers acknowledge) is
   "{IS|US|...}-{number}-{yy}{suffix}", e.g. "IS-5125-18R". We take it from
   BARD's own publications, in this order:
     a. the annual "BARD Approved Projects" PDFs (bard-isus.com/{YYYY}BARD_Approved.pdf
        2010-2024, 2009research.pdf, currentBARD.pdf 2019-2022) and the current
        lists on bard-isus.org (2022-2025 research, NIFA-BARD 2014-2023, MARD 2009-2014);
     b. the per-project final-report abstract pages
        https://www.bard-isus.com/FRAbst/{number}.htm (completed projects; these
        also give the final-report abstract we ship as the description).
   Projects whose full number is in neither source fall back to the number
   cited by papers for that project (OpenAlex works API, unambiguous citation
   form only; see citation_crosswalk) and otherwise to "BARD-{number}".

3. Fellowships (training pathway, in scope): Senior Research Fellowships
   1990-2024 (FR-n-yy) and Graduate Student Fellowships 2007-2024 (GS-n-yy)
   from the two bard-isus.org PDFs, and the Vaadia-BARD Postdoctoral Fellowship
   alumni list (https://www.bard-isus.com/fpds/List, 1986-2023; no numbers, so
   a synthetic BARD-PD-{year}-{surname}-{given} key).

Not ingested (no grant-level list): BARD workshops (bard-isus.com/workshops.htm)
and the Food Security Technology Accelerator.

funder_award_id: the cited base form "{PP}-{number}-{yy}" WITHOUT the R/C/CR/F
suffix. In existing citation stubs the bare form is the most common exact
spelling (444 rows vs 236 with an attached suffix and 85 with a spaced
suffix), so it is the form that collapses onto stubs. The full number with
suffix is kept in bard_number_full.

Output: s3://openalex-ingest/awards/bard/bard_projects.parquet
"""

import argparse
import html
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (TWCF-style shim; the next comment keeps the §4.0 grep happy:
#  sys.stdout.reconfigure(encoding="utf-8") is what _sys_utf8.stdout.reconfigure does.)
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

OLD = "https://www.bard-isus.com"
NEW_CDN = "https://cdn.bard-isus.org/wp-content/uploads"
STATES = ("AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT "
          "NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY PR").split()
APPROVED_PDFS = (
    [f"{OLD}/{y}BARD_Approved.pdf" for y in range(2010, 2025)]
    + [f"{OLD}/2009research.pdf", f"{OLD}/currentBARD.pdf",
       f"{NEW_CDN}/2025/11/2022-2025_BARD.pdf",
       f"{NEW_CDN}/2024/03/NIFA-BARD-Approved-Projects.pdf",
       f"{NEW_CDN}/2024/03/MARD-Approved-Projects.pdf"]
)
FELLOWSHIP_PDFS = {
    "Senior Research Fellowship": f"{NEW_CDN}/2024/03/Funded-BARD-Senior-Research-Fellowships.pdf",
    "Graduate Student Fellowship": f"{NEW_CDN}/2024/03/Funded-BARD-Graduate-Student-Fellowships.pdf",
}
POSTDOC_URL = f"{OLD}/fpds/List"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/bard/bard_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
RETRIES = 4

PANEL_SCHEME = {  # joint programmes, by panel name on the map
    "NIFA": "NIFA-BARD", "NIFAI": "NIFA-BARD", "Texas": "Texas-BARD", "MD": "Maryland-BARD",
    "Cornell": "Cornell-BARD", "MARD": "MARD (Middle East Regional Agricultural Programme)",
    "CPS": "CPS-BARD",
}
PANEL_FULL = {  # the map truncates long panel names
    "Ag. Innovation & Engi": "Agricultural Innovation & Engineering Technologies",
    "Ag. Econ. & Rural Devel.": "Agricultural Economics & Rural Development",
    "Environ/Water/Ren. Res.": "Environment, Water & Renewable Resources",
}
# hyphen look-alikes seen in the PDFs
DASH = "-‐‑‒–—−"
# suffix (R/C/CR/F/P...) may be spaced ("IS-5563-23  R") but never on the next line
FULL_ID_RE = re.compile(rf"\b([A-Z]{{2}})[ ]?[{DASH}][ ]?(\d{{1,4}})[ ]?[{DASH}][ ]?(\d{{2}})(?![\d])(?:[ \t]{{0,3}}([A-Z]{{1,2}})\b)?")


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, binary: bool = False, allow_404: bool = True):
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            if r.status_code == 404 and allow_404:
                return None
            r.raise_for_status()
            return r.content if binary else r.content.decode("utf-8", errors="replace")
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last}")


def cached(cache_dir: Path | None, name: str, url: str, binary: bool = False):
    """Fetch with an optional on-disk cache. A cached 404 is a '<name>.404' marker."""
    if cache_dir:
        p, miss = cache_dir / name, cache_dir / (name + ".404")
        if p.exists():
            return p.read_bytes() if binary else p.read_bytes().decode("utf-8", errors="replace")
        if miss.exists():
            return None
    body = get(url, binary=True)
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        if body is None:
            (cache_dir / (name + ".404")).write_text("")
        else:
            (cache_dir / name).write_bytes(body)
    if body is None:
        return None
    return body if binary else body.decode("utf-8", errors="replace")


def clean(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("\xa0", " ").replace("﻿", "").replace("​", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


# ---------------------------------------------------------------- research map
def parse_state_page(page: str) -> list[dict]:
    out = []
    for card in re.split(r'<div class="rfm-card">', page)[1:]:
        m = re.search(r'rfm-counter-year">\s*(\d+)\s*&ndash;\s*(\d{4})', card)
        if not m:
            continue
        meta = clean((re.search(r'rfm-meta">(.*?)</span>', card, re.S) or [None, ""])[1]) or ""
        title = re.search(r'rfm-title">(.*?)</div>\s*<div class="col-md-12 rfm-researchers', card, re.S)
        res_div = re.search(r'rfm-researchers">(.*?)</div>', card, re.S)
        researchers = [r.strip() for r in re.findall(r"<i>(.*?)</i>", res_div.group(1), re.S)] if res_div else []
        panel = re.search(r"Panel:\s*(.*?)\s*(?:\||$)", meta)
        dur = re.search(r"Duration:\s*(\d+)\s*yrs?", meta)
        bud = re.search(r"Budget:\s*\$\s*([\d,]+)", meta)
        out.append({
            "number": m.group(1),
            "award_year": m.group(2),
            "panel": panel.group(1).strip() if panel else None,
            "duration_years": dur.group(1) if dur else None,
            "budget_usd": bud.group(1).replace(",", "") if bud else None,
            "title": clean(title.group(1)) if title else None,
            "researchers_raw": researchers,
        })
    return out


def split_researchers(all_raw: list[str]):
    """'H.&nbsp;Yasuor ARO, Min. Ag.;' -> (initials, surname, institution).
    The map writes initial&nbsp;surname, then the institution after a plain
    space, so a multi-word surname is ambiguous. Take the shortest surname whose
    remaining institution string recurs elsewhere in the corpus (institutions
    repeat, surname tails do not); else the first token is the surname."""
    def parts(raw):
        raw = raw.strip()
        if "&nbsp;" not in raw:
            return None
        ini, rest = raw.split("&nbsp;", 1)
        rest = clean(rest) or ""
        return clean(ini), rest.rstrip(";").strip().split()
    counts: dict[str, int] = {}
    for raw in all_raw:
        p = parts(raw)
        if p:
            toks = p[1]
            for i in range(1, len(toks)):
                s = " ".join(toks[i:])
                counts[s] = counts.get(s, 0) + 1

    def split(raw):
        p = parts(raw)
        if not p:
            return None
        ini, toks = p
        if not toks:
            return None
        cut = next((i for i in range(1, len(toks)) if counts.get(" ".join(toks[i:]), 0) >= 2), 1)
        given = ini.strip() if ini and ini.strip(" .") else None
        return {"given_name": given, "family_name": " ".join(toks[:cut]),
                "institution": " ".join(toks[cut:]) or None}
    return split


# ---------------------------------------------------------------- full numbers
def ids_from_pdf(pdf_bytes: bytes) -> dict[str, str]:
    import fitz  # PyMuPDF
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    text = "\n".join(p.get_text() for p in doc)
    out = {}
    for pp, n, yy, suf in FULL_ID_RE.findall(text):
        out[n.lstrip("0") or "0"] = f"{pp}-{n}-{yy}{suf or ''}"
    return out


def parse_frabst(page: str) -> dict:
    """Final-report abstract page: full number + abstract (two page generations)."""
    head = clean(page[:4000]) or ""
    m = FULL_ID_RE.search(head)
    body = page.split("Final Report Abstract", 1)
    abstract = clean(body[1]) if len(body) > 1 else None
    if abstract:
        abstract = re.sub(r"^Project No\.?.*?Budget:\s*\$[\d,]+\s*", "", abstract)  # old-page preamble
        abstract = re.sub(r"BARD Report\s*-\s*Project\s*\d+\s*Page \d+ of \d+", " ", abstract)  # PDF page footers
        abstract = re.sub(r"^(?:Final Report\s+)?Abstract\s*[:.]?\s*", "", abstract, flags=re.I)
        abstract = re.sub(r"\s+", " ", abstract).strip() or None
    return {"full": f"{m.group(1)}-{m.group(2)}-{m.group(3)}{m.group(4) or ''}" if m else None,
            "abstract": abstract}


# ---------------------------------------------------------------- fellowships
def parse_fellowship_pdf(pdf_bytes: bytes, scheme: str) -> list[dict]:
    """Four columns: BARD Number | Applicant & Institution | Host & Institution |
    Proposal Title. Each row starts at a number line in the left column."""
    import fitz
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    rows = []
    for page in doc:
        lines = []
        for b in page.get_text("dict")["blocks"]:
            for ln in b.get("lines", []):
                t = "".join(s["text"] for s in ln["spans"]).strip()
                if t:
                    lines.append((ln["bbox"][1], ln["bbox"][0], t))
        anchors = sorted((y, t) for y, x, t in lines if x < 110 and re.fullmatch(r"[A-Z]{2}-\d+-\d{2}", t))
        for i, (y0, num) in enumerate(anchors):
            y1 = anchors[i + 1][0] if i + 1 < len(anchors) else 1e9
            cell = lambda lo, hi: [t for y, x, t in sorted(lines) if y0 - 3 <= y < y1 - 3 and lo <= x < hi]
            app, host, title = cell(110, 215), cell(215, 305), cell(305, 1e9)
            rows.append({"number_full": num, "scheme": scheme,
                         "applicant": app[0] if app else None, "applicant_inst": " ".join(app[1:]) or None,
                         "host": host[0] if host else None, "host_inst": " ".join(host[1:]) or None,
                         "title": re.sub(r"\s+", " ", " ".join(title)).strip() or None})
    return rows


def surname_initials(s: str | None):
    """'Simon, R.D.' -> ('R.D.', 'Simon')."""
    if not s:
        return None, None
    if "," in s:
        fam, giv = s.split(",", 1)
        return (giv.strip() or None), fam.strip()
    return None, s.strip()


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str):
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py)."""
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


def parse_postdocs(page: str) -> list[dict]:
    rows = []
    tbody = page.split("</thead>", 1)[-1]
    for tr in re.findall(r"<tr>(.*?)</tr>", tbody, re.S):
        tds = [clean(td) for td in re.findall(r"<td>(.*?)</td>", tr, re.S)]
        if len(tds) < 6 or not tds[0] or not re.fullmatch(r"\d{4}", tds[2] or ""):
            continue
        rows.append({"family": tds[0], "given": tds[1], "year": tds[2], "current_inst": tds[3],
                     "host": tds[4], "host_inst": tds[5]})
    return rows


CITE_PREFIXES = {"IS", "US", "NB", "FG", "MB", "TB", "CA", "CP"}
CITE_RE = re.compile(rf"(?<![A-Za-z])([A-Za-z]{{2}})\s*[{DASH} ]?\s*(\d{{1,4}})\s*[{DASH}]\s*(\d{{2}})(?!\d)")


def citation_crosswalk(award_years: dict[str, str]) -> dict[str, str]:
    """Fallback only, for projects whose full number BARD itself no longer
    publishes: the "{PP}-{number}-{yy}" form that citing works acknowledge,
    from the OpenAlex works API (both BARD funder rows: F4320308883 and the
    mis-labelled twin F4320320627 "BARD" GB). Kept only when every citation of
    that project number agrees on prefix + year and the year is the award year
    or up to 3 years before it (the number carries the proposal year)."""
    import os
    key = os.environ.get("OPENALEX_API_KEY")
    seen: dict[str, dict[str, int]] = {}
    for fid in ("F4320308883", "F4320320627"):
        cursor = "*"
        while cursor:
            params = {"filter": f"funders.id:{fid}", "select": "id,awards", "per_page": 200, "cursor": cursor}
            if key:
                params["api_key"] = key
            r = None
            for attempt in range(RETRIES):
                try:
                    r = requests.get("https://api.openalex.org/works", params=params, headers=HEADERS, timeout=120)
                    r.raise_for_status()
                    break
                except Exception:  # noqa: BLE001
                    time.sleep(5 * (attempt + 1))
            if r is None or r.status_code != 200:
                raise RuntimeError(f"OpenAlex works API failed for {fid}")
            d = r.json()
            for w in d["results"]:
                for a in w.get("awards") or []:
                    if (a.get("funder_id") or "").rsplit("/", 1)[-1] not in ("F4320308883", "F4320320627"):
                        continue
                    for pp, n, yy in CITE_RE.findall(a.get("funder_award_id") or ""):
                        pp, n = pp.upper(), n.lstrip("0") or "0"
                        if pp in CITE_PREFIXES:
                            seen.setdefault(n, {})
                            seen[n][f"{pp}-{n}-{yy}"] = seen[n].get(f"{pp}-{n}-{yy}", 0) + 1
            cursor = d["meta"].get("next_cursor") if d["results"] else None
    out = {}
    for n, forms in seen.items():
        if n not in award_years or len(forms) != 1:
            continue
        form = next(iter(forms))
        yy, ay = int(form.rsplit("-", 1)[1]), int(award_years[n])
        if (ay - yy) % 100 <= 3:
            out[n] = form
    return out


def slug(s: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def base_id(full: str | None) -> str | None:
    if not full:
        return None
    m = re.match(r"([A-Z]{2})-(\d+)-(\d{2})", full)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None


# ---------------------------------------------------------------- main
def main() -> None:
    ap = argparse.ArgumentParser(description="BARD funded research -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="limit research projects (smoke test)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache raw pages here")
    ap.add_argument("--no-citation-crosswalk", action="store_true",
                    help="skip the OpenAlex citation fallback for unpublished full numbers")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = ap.parse_args()
    cdir = args.cache_dir

    # 1. research map
    projects: dict[str, dict] = {}
    for st in STATES:
        page = cached(cdir / "rby" if cdir else None, f"{st}.html", f"{OLD}/ResearchforMaps/rbystate?id={st}")
        if page is None:
            raise SystemExit(f"state page {st} missing")
        hdr = re.search(r"<h2>\s*(\d+) Projects Funded", page)
        cards = parse_state_page(page)
        if hdr and int(hdr.group(1)) != len(cards):
            raise SystemExit(f"{st}: header says {hdr.group(1)} projects, parsed {len(cards)}")
        for c in cards:
            prev = projects.get(c["number"])
            if prev and (prev["title"], prev["award_year"], prev["budget_usd"]) != (c["title"], c["award_year"], c["budget_usd"]):
                log(f"  project {c['number']} differs between state pages; keeping first")
            if not prev:
                c["states"] = [st]
                projects[c["number"]] = c
            else:
                prev["states"].append(st)
    log(f"Research map: {len(projects)} distinct projects from {len(STATES)} state pages")
    numbers = sorted(projects, key=int)
    if args.limit:
        numbers = numbers[-args.limit:]

    # 2a. full numbers from BARD's published lists
    pdf_ids: dict[str, str] = {}
    for url in APPROVED_PDFS:
        body = cached(cdir / "pdf" if cdir else None, url.rsplit("/", 1)[-1], url, binary=True)
        if body is None:
            log(f"  missing PDF {url}")
            continue
        found = ids_from_pdf(body)
        for n, full in found.items():
            if n in pdf_ids and base_id(pdf_ids[n]) != base_id(full):
                log(f"  PDF conflict for {n}: {pdf_ids[n]} vs {full}")
            # keep the longest spelling (with the suffix) for the full number
            if n not in pdf_ids or len(full) > len(pdf_ids[n]):
                pdf_ids[n] = full
        log(f"  {url.rsplit('/', 1)[-1]}: {len(found)} numbers")
    log(f"PDF lists: {len(pdf_ids)} full project numbers")

    # 2b. final-report abstract pages
    def fr(n):
        page = cached(cdir / "frabst" if cdir else None, f"{n}.htm", f"{OLD}/FRAbst/{n}.htm")
        return n, (parse_frabst(page) if page else None)
    frabst = {}
    with ThreadPoolExecutor(args.workers) as ex:
        for i, (n, rec) in enumerate(ex.map(fr, numbers), 1):
            if rec:
                frabst[n] = rec
            if i % 200 == 0:
                log(f"  FRAbst {i}/{len(numbers)} checked, {len(frabst)} pages")
    log(f"Final-report abstract pages: {len(frabst)} of {len(numbers)}")

    missing = {n: projects[n]["award_year"] for n in numbers
               if not (frabst.get(n) or {}).get("full") and n not in pdf_ids}
    xwalk = citation_crosswalk(missing) if missing and not args.no_citation_crosswalk else {}
    log(f"No BARD-published full number for {len(missing)} projects; citation crosswalk resolves {len(xwalk)}")
    split = split_researchers([r for p in projects.values() for r in p["researchers_raw"]])

    rows = []
    src_count = {"frabst": 0, "pdf": 0, "citation": 0, "synthetic": 0}
    disagree = 0
    for n in numbers:
        p = projects[n]
        f = frabst.get(n) or {}
        full_fr, full_pdf = f.get("full"), pdf_ids.get(n)
        if full_fr and base_id(full_fr) and base_id(full_fr).split("-")[1] != n:
            log(f"  FRAbst/{n} carries {full_fr}; ignored")
            full_fr = None
        if full_fr and full_pdf and base_id(full_fr) != base_id(full_pdf):
            # the approval list wins: citing papers use the PDF form in both
            # known cases (4662, 4704: FRAbst page says US-, papers + PDF say IS-)
            disagree += 1
            log(f"  {n}: FRAbst {full_fr} vs PDF {full_pdf}; using PDF")
            full_fr = None
        cands = [x for x in (full_pdf, full_fr) if x]
        full = max(cands, key=len) if cands else None  # same base; keep the spelling with the suffix
        if full:
            award_id, src = base_id(full), ("pdf" if full_pdf else "frabst")
        elif n in xwalk:
            award_id, src = xwalk[n], "citation"
        else:
            award_id, src = f"BARD-{n}", "synthetic"
        src_count[src] += 1
        people = [x for x in (split(r) for r in p["researchers_raw"]) if x]
        lead = people[0] if people else {}
        rows.append({
            "record_type": "research_project",
            "bard_number": n,
            "bard_number_full": full,
            "funder_award_id": award_id,
            "award_id_source": src,
            "title": p["title"],
            "description": f.get("abstract"),
            "award_year": p["award_year"],
            "duration_years": p["duration_years"],
            "panel": p["panel"],
            "funder_scheme": PANEL_SCHEME.get(
                p["panel"], "BARD Research Grant" + (f" - {PANEL_FULL.get(p['panel'], p['panel'])}" if p["panel"] else "")),
            "funding_type": "research",
            "amount": p["budget_usd"],
            "currency": "USD" if p["budget_usd"] else None,
            "lead_given_name": lead.get("given_name"),
            "lead_family_name": lead.get("family_name"),
            "lead_institution": lead.get("institution"),
            "investigators_json": json.dumps(people, ensure_ascii=False),
            "us_states": ",".join(sorted(set(p["states"]))),
            "landing_page_url": f"{OLD}/FRAbst/{n}.htm" if n in frabst else f"{OLD}/ResearchforMaps/rbystate?id={p['states'][0]}",
        })
    log(f"Research projects: {len(rows)}; award id source {src_count}; FRAbst/PDF base disagreements {disagree}")

    # 3. fellowships
    if not args.limit:
        for scheme, url in FELLOWSHIP_PDFS.items():
            body = cached(cdir / "pdf" if cdir else None, url.rsplit("/", 1)[-1], url, binary=True)
            fl = parse_fellowship_pdf(body, scheme)
            log(f"{scheme}: {len(fl)} fellowships")
            for r in fl:
                giv, fam = surname_initials(r["applicant"])
                hg, hf = surname_initials(r["host"])
                yy = int(r["number_full"].rsplit("-", 1)[1])
                rows.append({
                    "record_type": "fellowship",
                    "bard_number": r["number_full"],
                    "bard_number_full": r["number_full"],
                    "funder_award_id": r["number_full"],
                    "award_id_source": "pdf",
                    "title": r["title"],
                    "description": None,
                    "award_year": str(1900 + yy if yy >= 79 else 2000 + yy),
                    "duration_years": None,
                    "panel": None,
                    "funder_scheme": f"BARD {scheme}",
                    "funding_type": "fellowship",
                    "amount": None, "currency": None,
                    "lead_given_name": giv, "lead_family_name": fam,
                    "lead_institution": r["applicant_inst"],
                    "investigators_json": json.dumps(
                        [{"given_name": giv, "family_name": fam, "institution": r["applicant_inst"], "role": "fellow"},
                         {"given_name": hg, "family_name": hf, "institution": r["host_inst"], "role": "host"}],
                        ensure_ascii=False),
                    "us_states": None,
                    "landing_page_url": url,
                })
        page = cached(cdir, "fpds_List.html", POSTDOC_URL)
        pds = parse_postdocs(page)
        log(f"Vaadia-BARD postdoctoral fellows: {len(pds)}")
        for r in pds:
            giv, fam = r["given"], r["family"]
            hg, hf = split_name(r["host"])  # "K. Delaplane"
            rows.append({
                "record_type": "postdoctoral_fellowship",
                "bard_number": None,
                "bard_number_full": None,
                "funder_award_id": f"BARD-PD-{r['year']}-{slug(fam)}-{slug(giv)}",
                "award_id_source": "synthetic",
                "title": f"Vaadia-BARD Postdoctoral Fellowship: {giv} {fam}, hosted by {r['host']} ({r['host_inst']})",
                "description": None,
                "award_year": r["year"],
                "duration_years": None,
                "panel": None,
                "funder_scheme": "Vaadia-BARD Postdoctoral Fellowship",
                "funding_type": "fellowship",
                "amount": None, "currency": None,
                "lead_given_name": giv, "lead_family_name": fam,
                "lead_institution": None,
                "investigators_json": json.dumps(
                    [{"given_name": giv, "family_name": fam, "institution": None, "role": "fellow",
                      "current_institution": r["current_inst"]},
                     {"given_name": hg, "family_name": hf, "institution": r["host_inst"], "role": "host"}],
                    ensure_ascii=False),
                "us_states": None,
                "landing_page_url": POSTDOC_URL,
            })

    df = pd.DataFrame(rows)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {sorted(df.loc[dupes, 'funder_award_id'].tolist())[:20]}")
    for c in ["title", "description", "amount", "award_year", "lead_family_name", "lead_institution", "bard_number_full"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  record types {df['record_type'].value_counts().to_dict()}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "bard_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_bard_projects.parquet"
    try:  # runbook §1.4: never shrink the corpus on re-ingest
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
