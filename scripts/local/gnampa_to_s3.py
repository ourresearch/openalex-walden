#!/usr/bin/env python3
"""
INdAM-GNAMPA (Gruppo Nazionale per l'Analisi Matematica, la Probabilità e le
loro Applicazioni) Progetti di Ricerca to S3 Data Pipeline
================================================================================

GNAMPA, one of the national research groups of the Istituto Nazionale di Alta
Matematica "F. Severi" (INdAM), funds one-year research projects ("Progetti di
Ricerca GNAMPA") led by a coordinator with 3-8 participants from at least two
INdAM research units. The group publishes the funded list for each year as a
PDF on https://www.altamatematica.it/gnampa/attivita/progetti-di-ricerca/
(Allegati: PROGETTI GNAMPA 2012 ... 2026; there was no 2021 call - the 2020
list was carried into 2021). No CSV/API exists; method 4/5 (bulk files).

Fields per year (they vary by year): coordinator (given/family, or one cell),
institution (2012-2019), title, amount in EUR (2022-2025), participants.
Coordinator names printed in a single cell are split using the GNAMPA members
lists (aderenti, COGNOME/NOME separate) and, where the PDF has no institution
column, the coordinator's institution is taken from that year's members list
(lead_institution_source records which).

The PDFs come in two shapes: ruled tables (pdfplumber table finder) and
text-only Excel exports (2015-2018), whose rows are rebuilt from word positions;
see LAYOUT. 2024's PDF uses unmapped ligature glyphs ((cid:415) = "ti", ...).

No per-project code is published (citing works write "GNAMPA 2020" or the
year's shared CUP code), so funder_award_id is synthetic:
GNAMPA-{year}-{coordinator-family-slug}.

Output: s3://openalex-ingest/awards/gnampa/gnampa_projects.parquet
"""

import argparse
import html
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import pdfplumber
import requests


# (self-check marker: this block is the runbook sys.stdout.reconfigure shim, with sys renamed)
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


# --- PDF parsing (shared shape for INdAM group project lists) ---
CID = {"(cid:415)": "ti", "(cid:425)": "tt", "(cid:427)": "tti"}

FIELD_PATTERNS = [
    ("coord", r"^COORDINATORE$"),
    ("family", r"^COGNOME$"),
    ("given", r"^NOME$"),
    ("given_family", r"^NOME\s+COGNOME$|^NOME COGNOME$"),
    ("inst", r"^(UNIVERSITA'?( DI APPARTENENZA)?|ATENEO/ENTE|AFFILIAZIONE|UNIVERSIT[AÀ])$"),
    ("dept", r"^DIPARTIMENTO$"),
    ("role", r"^QUALIFICA$"),
    ("title", r"^TITOLO( PROGETTO| DEL PROGETTO)?$"),
    ("amount", r"^(FONDI|FINANZIAMENTO|FINANZIAM\.?)$"),
    ("participants", r"^PARTECIPANTI$"),
]


def clean(s):
    if s is None:
        return ""
    for k, v in CID.items():
        s = s.replace(k, v)
    return s


def classify(cell):
    c = re.sub(r"\s+", " ", clean(cell)).strip().upper()
    for f, pat in FIELD_PATTERNS:
        if re.match(pat, c):
            return f
    return None


def header_map(texts, bboxes):
    """[(x_center_start, field)] if this row is a header row, else None."""
    hits = [(b[0], classify(t)) for t, b in zip(texts, bboxes) if t and b]
    fields = [f for _, f in hits if f]
    if "title" in fields and len(fields) >= 2:
        return sorted((x, f) for x, f in hits if f)
    return None


def field_for(x0, x1, hmap):
    cx = (x0 + x1) / 2
    f = hmap[0][1]
    for hx, fld in hmap:
        if hx - 3 <= cx:
            f = fld
    return f


def parse_tables(path, default_hmap=None):
    recs, hmap = [], default_hmap
    with pdfplumber.open(path) as pdf:
        for pg in pdf.pages:
            for tb in pg.find_tables():
                data = tb.extract()
                for row, trow in zip(data, tb.rows):
                    bboxes = trow.cells
                    texts = [clean(c).strip() for c in row]
                    h = header_map(texts, bboxes)
                    if h:
                        hmap = h
                        continue
                    if hmap is None:
                        continue
                    flds = {}
                    for t, b in zip(texts, bboxes):
                        if not t or not b:
                            continue
                        if re.fullmatch(r"\d{1,3}\.?", t):  # row numbers
                            continue
                        if re.search(r"PROGETTI \d{4}|Gruppo Nazionale per", t):
                            continue
                        flds.setdefault(field_for(b[0], b[2], hmap), []).append(t)
                    if not flds:
                        continue
                    # notes such as "fine contr 13/09/22" sit in the name columns
                    for k in ("coord", "family", "given"):
                        if k in flds and any(re.search(r"\d|fine contr", v) for v in flds[k]):
                            flds.setdefault("note", []).extend(flds.pop(k))
                    if any(k in flds for k in ("coord", "family", "given_family")):
                        # A record's first row can carry a None cell when its text
                        # box runs over the page break (2025 'Devillanova'): read
                        # that column's region of the row directly.
                        xs = [hx for hx, _ in hmap] + [pg.width + 3]
                        for k, (hx, fld) in enumerate(hmap):
                            if fld in flds or fld in ("participants",):
                                continue
                            box = (max(hx - 3, 0), trow.bbox[1], min(xs[k + 1] - 3, pg.width), trow.bbox[3])
                            txt = clean(pg.crop(box).extract_text() or "").strip()
                            if txt:
                                flds[fld] = [txt]
                        recs.append(flds)
                    elif recs:
                        for k, v in flds.items():
                            # names never continue on a later table row; stray
                            # name-column text there is kept as a note only
                            k = "note" if k in ("given", "family", "coord") else k
                            recs[-1].setdefault(k, []).extend(v)
    return recs


HEADER_WORDS = {"NOME": "given", "COGNOME": "family", "COORDINATORE": "coord", "UNIVERSITÀ": "inst",
                "ATENEO/ENTE": "inst", "TITOLO": "title", "PARTECIPANTI": "participants"}


def parse_words(path, align="bottom", name_fields=("given", "family", "coord")):
    """Text-only layouts: columns from the header words, records anchored on the
    line that carries the coordinator's name. Excel exports bottom-align every
    cell, so lines between two name lines belong to the NEXT record
    (align='bottom'); in top-aligned years they belong to the previous one
    (align='top')."""
    recs = []
    cols = None
    with pdfplumber.open(path) as pdf:
        for pg in pdf.pages:
            words = pg.extract_words()
            hdr = [w for w in words if w["text"].upper() in HEADER_WORDS]
            htop = None
            if hdr:
                htop = min(w["top"] for w in hdr)
                hdr = [w for w in hdr if abs(w["top"] - htop) < 3]
                seen, cols = set(), []
                for w in sorted(hdr, key=lambda w: w["x0"]):
                    f = HEADER_WORDS[w["text"].upper()]
                    if f not in seen:
                        seen.add(f)
                        cols.append((w["x0"] - 8, f))
            if cols is None:
                continue
            body = [w for w in words if htop is None or w["top"] > htop + 5]
            body = [w for w in body if not re.search(r"^PROGETTI$|^\d{4}$", w["text"]) or w["top"] > 120]

            def colf(w):
                f = cols[0][1]
                for x, fld in cols:
                    if w["x0"] >= x:
                        f = fld
                return f

            # group words into lines per column
            lines = {}
            for w in body:
                f = colf(w)
                lines.setdefault(f, []).append(w)
            col_lines = {}
            for f, ws in lines.items():
                ws.sort(key=lambda w: (round(w["top"]), w["x0"]))
                grouped = []
                for w in ws:
                    if grouped and abs(grouped[-1][0] - w["top"]) < 3:
                        grouped[-1][1].append(w)
                    else:
                        grouped.append([w["top"], [w]])
                col_lines[f] = [(t, " ".join(x["text"] for x in sorted(g, key=lambda x: x["x0"]))) for t, g in grouped]
            # Anchor = the coordinator's line. With separate NOME/COGNOME columns a
            # line must carry both, so a wrapped given name ("Massimiliano" /
            # "Daniele Rosini") or family name ("Passarelli di" / "Napoli") is a
            # continuation, not a new record.
            if "given" in col_lines and "family" in col_lines:
                g_tops = [t for t, _ in col_lines["given"]]
                anchors = sorted({round(t) for t, _ in col_lines["family"]
                                  if any(abs(t - gt) < 3 for gt in g_tops)})
            else:
                anchors = sorted({round(t) for f in name_fields for t, _ in col_lines.get(f, [])})
            # merge anchors closer than 3pt
            anc = []
            for a in anchors:
                if anc and a - anc[-1] < 3:
                    continue
                anc.append(a)
            if not anc:
                continue
            page_recs = [{} for _ in anc]
            for f, ls in col_lines.items():
                # bucket: on-anchor lines, and lines in gaps (before first, between, after last)
                gaps = [[] for _ in range(len(anc) + 1)]
                for t, txt in ls:
                    on = [i for i, a in enumerate(anc) if abs(t - a) < 3]
                    if on:
                        page_recs[on[0]].setdefault(f, []).append((t, txt))
                        continue
                    gi = sum(1 for a in anc if a < t)
                    gaps[gi].append((t, txt))
                if align == "bottom":
                    for gi, g in enumerate(gaps):
                        tgt = min(gi, len(anc) - 1)
                        for item in g:
                            page_recs[tgt].setdefault(f, []).append(item)
                else:  # top
                    for gi, g in enumerate(gaps):
                        tgt = max(gi - 1, 0)
                        for item in g:
                            page_recs[tgt].setdefault(f, []).append(item)
            for r in page_recs:
                recs.append({f: [txt for _, txt in sorted(v)] for f, v in r.items()})
    return recs


BASE = "https://www.altamatematica.it"
PROJECTS_PAGE = f"{BASE}/gnampa/attivita/progetti-di-ricerca/"
ADERENTI_URL = f"{BASE}/gnampa/aderenti/aderenti-{{year}}/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gnampa/gnampa_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 3

# How each yearly PDF is laid out (all are static historical files). "table" =
# ruled table, read with pdfplumber's table finder; ("words", align) = text-only
# Excel export, rows rebuilt from word positions: in "bottom" years every cell is
# bottom-aligned (multi-line titles sit ABOVE the coordinator's line), in "top"
# years they hang below it. Unknown future years default to "table".
LAYOUT = {2012: "table", 2013: "table", 2014: "table", 2015: ("words", "bottom"),
          2016: ("words", "top"), 2017: ("words", "bottom"), 2018: ("words", "bottom"),
          2019: "table", 2020: "table", 2022: "table", 2023: "table", 2024: "table",
          2025: "table", 2026: "table"}
# Order of the single COORDINATORE cell where the PDF does not split given/family.
COORD_ORDER = {2013: "given_first", 2020: "given_first"}  # default: family_first (2018, 2024-2026)

PARTICLES = {"di", "de", "del", "della", "dello", "delle", "degli", "dei", "da", "dal", "dalla",
             "dall'", "d'", "lo", "la", "li", "le", "van", "von", "mc"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, binary: bool = False):
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            r.raise_for_status()
            if binary:
                return r.content
            r.encoding = r.encoding if r.encoding and r.encoding.lower() != "iso-8859-1" else "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2 * (attempt + 1))
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


def norm(s: str) -> str:
    s = (s or "").replace("’", "'").replace("`", "'")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"(?<=[aeiou])'(?=\s|$)", "", s)  # "Giuffre'", "CHIADO' PIAT": accent typed as apostrophe
    return re.sub(r"[^a-z']+", " ", s).strip()


def tidy(s: str | None) -> str | None:
    if not s:
        return None
    s = html.unescape(s).replace("‐", "-").replace("‑", "-").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip(" |;,")
    return s or None


# Stray text that sits inside a title cell of the source PDF (not part of the title).
TITLE_ARTIFACTS = [r"\s+Come membro del$"]  # 2020 Flandoli: clipped side note


def tidy_title(s: str | None) -> str | None:
    s = tidy(s)
    for pat in TITLE_ARTIFACTS:
        s = re.sub(pat, "", s or "") or None
    return s


def tidy_name(s: str | None) -> str | None:
    # footnote markers ("Rinaldo M. 1")
    return tidy(re.sub(r"\s+\d+$", "", s or ""))


def proper(s: str | None) -> str | None:
    """'ADDONA' -> 'Addona', "D'ANCONA" -> "D'Ancona"; mixed-case input is kept as printed."""
    if not s:
        return s
    return s.title() if (s.isupper() or s.islower()) else s


def project_pdfs(page: str) -> list[tuple[int, str]]:
    """(project year, PDF URL) from the 'Allegati' list: link text 'PROGETTI GNAMPA YYYY'."""
    out = {}
    for url, label in re.findall(r'<a [^>]*href="([^"]+\.pdf)"[^>]*>(.*?)</a>', page, re.S | re.I):
        m = re.search(r"(20\d{2})", re.sub(r"<[^>]+>", " ", label)) or re.search(r"(20\d{2})", url)
        if m and re.search(r"progett", label + url, re.I):
            out.setdefault(int(m.group(1)), html.unescape(url))
    return sorted(out.items())


def members(cache_dir: Path | None, years) -> dict[int, list[tuple[str, str, str]]]:
    """GNAMPA aderenti (members) per year: (FAMILY, Given, affiliation). Used to split
    single-cell coordinator names correctly and to give coordinators an affiliation
    in the years whose project PDF has no institution column."""
    out = {}
    for y in years:
        try:
            page = cached(cache_dir, f"aderenti_{y}.html", ADERENTI_URL.format(year=y))
        except RuntimeError as e:
            log(f"  aderenti {y}: {e}")
            continue
        rows = []
        for fam_given, aff in re.findall(
                r"<tr[^>]*>\s*<td[^>]*>(?:<span[^>]*>)?\s*\d+\.\s*(?:</span>)?</td>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>",
                page, re.S):
            fg = tidy(re.sub(r"<[^>]+>", "", fam_given)) or ""
            if "," in fg:
                fam, given = fg.split(",", 1)
                rows.append((fam.strip(), given.strip(), tidy(re.sub(r"<[^>]+>", "", aff))))
        for tr in re.findall(r'<tr class="row-\d+">(.*?)</tr>', page, re.S):
            cells = [tidy(re.sub(r"<[^>]+>", "", c)) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
            cells = [c for c in cells if c is not None]
            if cells and re.fullmatch(r"\d+\.?", cells[0] or ""):
                cells = cells[1:]
            if len(cells) >= 2 and cells[0].upper() != "COGNOME":
                rows.append((cells[0], cells[1], cells[2] if len(cells) > 2 else None))
        out[y] = rows
        log(f"  aderenti {y}: {len(rows)} members")
    return out


def split_coord(name: str, order: str, member_keys: set[tuple[str, str]]) -> tuple[str | None, str | None]:
    """Split a one-cell coordinator name ('Rosazza Gianin Emanuela', 'LAURA ABATANGELO')
    into (given, family). First choice: the split that matches a GNAMPA member (the
    member lists carry COGNOME and NOME separately). Fallback: the PDF's column order,
    keeping Italian surname particles (Di, De, Della, ...) with the surname. Mirrors
    runbook §2.4.1: degree suffixes are stripped first."""
    tokens = [t for t in re.split(r"\s+", name.strip()) if t]
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, proper(tokens[0])
    # 2020's caps names come out of the PDF as "E UGENIO VECCHI": re-join a stray
    # leading letter when the joined form is a GNAMPA member.
    joined = [t for t in tokens]
    for i in range(len(joined) - 1):
        if len(joined[i]) == 1 and joined[i].isalpha() and len(joined[i + 1]) > 1:
            alt = joined[:i] + [joined[i] + joined[i + 1]] + joined[i + 2:]
            if any((norm(" ".join(alt[k:])), norm(" ".join(alt[:k]))) in member_keys
                   or (norm(" ".join(alt[:k])), norm(" ".join(alt[k:]))) in member_keys
                   for k in range(1, len(alt))):
                tokens = alt
                break
    cands = []
    for k in range(1, len(tokens)):
        cands.append((" ".join(tokens[k:]), " ".join(tokens[:k])))      # family first
        cands.append((" ".join(tokens[:k]), " ".join(tokens[k:])))      # given first
    for given, family in cands:
        if (norm(family), norm(given)) in member_keys:
            return proper(given), proper(family)
    if order == "given_first":
        k = len(tokens) - 1
        while k > 1 and tokens[k - 1].lower() in PARTICLES:
            k -= 1
        return proper(" ".join(tokens[:k])), proper(" ".join(tokens[k:]))
    k = 1
    while k < len(tokens) - 1 and tokens[k - 1].lower() in PARTICLES:
        k += 1
    return proper(" ".join(tokens[k:])), proper(" ".join(tokens[:k]))


def parse_amount(s: str | None) -> float | None:
    # "4.500,00", "3.000,00 €", "€ 2.500", "2.000" -> euros
    if not s:
        return None
    digits = re.sub(r"[^\d,.]", "", s).strip(",.")
    if not digits:
        return None
    m = re.search(r",(\d{1,2})$", digits)
    whole = re.sub(r"[.,]", "", digits[: m.start()] if m else digits)
    try:
        return float(f"{whole}.{m.group(1)}" if m else whole)
    except ValueError:
        return None


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def records_for_year(year: int, pdf_path: Path) -> list[dict]:
    layout = LAYOUT.get(year, "table")
    if layout == "table":
        recs = parse_tables(str(pdf_path))
    else:
        recs = parse_words(str(pdf_path), align=layout[1])
    return [r for r in recs if r.get("title") and any(k in r for k in ("coord", "family", "given"))]


def main() -> None:
    p = argparse.ArgumentParser(description="INdAM-GNAMPA Progetti di Ricerca PDFs -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the N most recent years (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache PDFs/HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    pdfs = project_pdfs(get(PROJECTS_PAGE))
    log(f"Projects page lists {len(pdfs)} yearly PDFs: {[y for y, _ in pdfs]}")
    if len(pdfs) < 13:
        raise SystemExit(f"only {len(pdfs)} project PDFs linked (expected 14: 2012-2020, 2022-2026); layout changed?")
    if args.limit:
        pdfs = pdfs[-args.limit:]

    mem = members(args.cache_dir, sorted({y for y, _ in pdfs} | {y - 1 for y, _ in pdfs}))
    member_keys = {(norm(f), norm(g)) for rows in mem.values() for f, g, _ in rows}
    member_aff = {}
    for y, rows in mem.items():
        for f, g, a in rows:
            member_aff[(y, norm(f), norm(g))] = a

    rows = []
    for year, url in pdfs:
        pdf_bytes = cached(args.cache_dir, f"progetti_{year}.pdf", url, binary=True)
        tmp = (args.cache_dir or args.output_dir) / f"_progetti_{year}.pdf"
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_bytes(pdf_bytes)
        recs = records_for_year(year, tmp)
        n_amt = 0
        for seq, r in enumerate(recs, 1):
            j = lambda k, sep=" ": tidy(sep.join(r.get(k, [])))  # noqa: E731
            if "coord" in r:
                parts = [c for c in r["coord"] if c]
                if len(parts) >= 2 and COORD_ORDER.get(year) == "given_first" and year == 2013:
                    given, family = proper(parts[0]), proper(" ".join(parts[1:]))
                else:
                    given, family = split_coord(" ".join(parts), COORD_ORDER.get(year, "family_first"), member_keys)
            else:
                given, family = proper(tidy_name(j("given"))), proper(tidy_name(j("family")))
                # a few rows have NOME/COGNOME swapped (2012 'Romito | Marco')
                if given and family and (norm(family), norm(given)) not in member_keys                         and (norm(given), norm(family)) in member_keys:
                    given, family = family, given
            inst = j("inst")
            aff_src = "pdf" if inst else None
            if not inst and family:
                for yy in (year, year - 1, year + 1):
                    a = member_aff.get((yy, norm(family), norm(given or "")))
                    if a:
                        inst, aff_src = a, f"aderenti_{yy}"
                        break
            amount = parse_amount(j("amount"))
            n_amt += amount is not None
            rows.append({
                "project_year": str(year),
                "page_seq": str(seq),
                "title": tidy_title(" ".join(r.get("title", []))),
                "coordinator_raw": tidy(" ".join(r.get("coord", []) or (r.get("given", []) + r.get("family", [])))),
                "lead_given_name": given,
                "lead_family_name": family,
                "lead_institution": inst,
                "lead_institution_source": aff_src,
                "lead_role": j("role"),
                "lead_department": j("dept"),
                "amount_text": j("amount"),
                "amount": amount,
                "participants_raw": j("participants", " | "),
                "note": j("note"),
                "source_pdf": url,
            })
        log(f"  {year}: {len(recs)} projects ({n_amt} with amount) from {url.rsplit('/', 1)[-1]}")

    df = pd.DataFrame(rows)
    df = df[df["title"].notna() & df["lead_family_name"].notna()].copy()
    base = [f"GNAMPA-{y}-{slug(f)}" for y, f in zip(df["project_year"], df["lead_family_name"])]
    dup = pd.Series(base).duplicated(keep=False).values
    df["funder_award_id"] = [f"{b}-{slug(g)}" if d else b for b, d, g in zip(base, dup, df["lead_given_name"].fillna(""))]
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        # same coordinator listed twice in one year: keep both, disambiguate by page order
        df.loc[dupes, "funder_award_id"] = df.loc[dupes, "funder_award_id"] + "-" + df.loc[dupes, "page_seq"]
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate funder_award_id after disambiguation")

    log(f"Parsed {len(df)} projects across {df['project_year'].nunique()} years")
    for c in ["title", "lead_given_name", "lead_family_name", "lead_institution", "amount", "participants_raw"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total amount EUR {df['amount'].sum():,.0f}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "gnampa_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_gnampa_projects.parquet"
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
