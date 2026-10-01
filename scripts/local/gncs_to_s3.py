#!/usr/bin/env python3
"""
INdAM-GNCS (Gruppo Nazionale per il Calcolo Scientifico) funded projects to S3
==============================================================================

GNCS, one of the national research groups of the Istituto Nazionale di Alta
Matematica "F. Severi" (INdAM), funds yearly research projects ("Progetti di
Ricerca GNCS", a coordinator plus participating GNCS members; missions and
visiting professors) and small young-researcher grants ("Finanziamento Giovani
Ricercatori"). Public sources, all on altamatematica.it:

  * one "PROGETTI FINANZIATI NELL'ANNO YYYY" PDF per year 2011-2020 (coordinator,
    participants, title, amount awarded), formerly linked from the group's
    "Storico Progetti di Ricerca" page (index preserved in the Wayback Machine;
    the PDFs themselves are still served live and are fetched from the site);
  * FINANZIAMENTO GIOVANI RICERCATORI 2018/19 (13 grants of EUR 1,200);
  * the group's 2025 annual report (Relazione Annuale), which lists the 52
    projects funded for 2025 (coordinator, university, title; no amounts).
2021-2024 project lists are not published (council minutes are login-only).

Coordinator names appear in many forms ('Prof. Aimi A.', 'G. ALBI', 'DE MARCHI',
'STEFANIA BELLAVIA (8+7)', 'Stefano BERRONE'); they are resolved against the GNCS
members lists (aderenti pages, COGNOME/NOME separate), which also provide the
coordinator's institution where the list has none.

No per-project code is published (citing works write "GNCS 2019" or the year's
shared INdAM CUP), so funder_award_id is synthetic: GNCS-{year}-{coordinator-slug}
(GNCS-GIOVANI-{year}-... for the young-researcher grants).

Output: s3://openalex-ingest/awards/gncs/gncs_projects.parquet
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


def tidy_name(s: str | None) -> str | None:
    # footnote markers ("Rinaldo M. 1")
    return tidy(re.sub(r"\s+\d+$", "", s or ""))


def proper(s: str | None) -> str | None:
    """'ADDONA' -> 'Addona', "D'ANCONA" -> "D'Ancona"; mixed-case input is kept as printed."""
    if not s:
        return s
    return s.title() if (s.isupper() or s.islower()) else s


def members(cache_dir: Path | None, years) -> dict[int, list[tuple[str, str, str]]]:
    """GNCS aderenti (members) per year: (FAMILY, Given, affiliation). Used to split
    single-cell coordinator names correctly and to give coordinators an affiliation
    in the years whose project PDF has no institution column."""
    out = {}
    for y in years:
        try:
            page = cached(cache_dir, f"aderenti_{y}.html", ADERENTI_URL.get(y, ADERENTI_URL["default"]).format(year=y))
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


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")




BASE = "https://www.altamatematica.it"
FILES = f"{BASE}/gncs/sites/www.altamatematica.it.gncs/files"
UPLOADS = f"{BASE}/gncs/wp-content/uploads/sites/4"
# The group's "Storico Progetti di Ricerca" page (gncs/archivi/storico-progetti-di-ricerca/,
# last captured https://web.archive.org/web/20220303193927/...) linked one funded-project
# PDF per year 2011-2020. The page was dropped in a site rebuild but the PDFs are still
# served from altamatematica.it, so they are fetched live. 2025's projects are listed in
# the group's 2025 annual report ("Relazione Annuale"); 2021-2024 lists are not public.
SOURCES = [
    # (year, scheme, url, kind)
    (2011, "Progetti di Ricerca GNCS", f"{FILES}/ProgettiRicercaGNCS2011.pdf", "table"),
    (2012, "Progetti di Ricerca GNCS", f"{FILES}/ProgettiRicercaGNCS2012.pdf", "text2012"),
    (2013, "Progetti di Ricerca GNCS", f"{FILES}/PROGETTI%20DI%20RICERCA2013.pdf", "table"),
    (2014, "Progetti di Ricerca GNCS", f"{FILES}/PROGETTI%20DI%20RICERCA2014_0.pdf", "table"),
    (2015, "Progetti di Ricerca GNCS", f"{FILES}/PROGETTI2015.pdf", "table"),
    (2016, "Progetti di Ricerca GNCS", f"{FILES}/PROGETTI2016.pdf", "table"),
    (2017, "Progetti di Ricerca GNCS", f"{FILES}/PROGETTI2017.pdf", "table"),
    (2018, "Progetti di Ricerca GNCS", f"{FILES}/PROGETTI2018_0.pdf", "table"),
    (2019, "Progetti di Ricerca GNCS", f"{UPLOADS}/2019/02/PROGETTI2019.pdf", "table"),
    (2020, "Progetti di Ricerca GNCS", f"{UPLOADS}/2020/03/PROGETTI2020.pdf", "table"),
    (2018, "Finanziamento Giovani Ricercatori GNCS", f"{UPLOADS}/2019/09/GIOVANI_RICERCATORI_2018_19.pdf", "table"),
    (2025, "Progetti di Ricerca GNCS", f"{UPLOADS}/2026/01/Attivita_GNCS_2025.pdf", "report2025"),
]
# Headline counts printed at the top of each list ("Sono stati finanziati N progetti").
EXPECTED = {2012: 15, 2013: 16, 2014: 20, 2015: 21, 2016: 27, 2017: 19, 2018: 22, 2019: 25, 2020: 30,
            ("Finanziamento Giovani Ricercatori GNCS", 2018): 13, 2025: 52}
ADERENTI_URL = {"default": f"{BASE}/gncs/aderenti/aderenti-{{year}}/",
                2025: f"{BASE}/gncs/aderenti/aderenti-2024-2/"}
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gncs/gncs_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 3

PARTICLES = {"di", "de", "del", "della", "dello", "delle", "degli", "dei", "da", "dal", "dalla",
             "dall'", "d'", "lo", "la", "li", "le", "van", "von"}
TITLES_RE = re.compile(r"^(?:(?:prof\.?ssa|prof|dott\.?ssa|dott|dr|ing)\.?\s+)+", re.I)

HEADER_FIELDS = [
    ("coordblock", r"^RESPONSABILE E PARTECIPANTI"),
    ("coord", r"^(RESPONSABILE( DEL PROGETTO)?|RICHIEDENTE|NOME E COGNOME)$"),
    ("family", r"^COGNOME$"),
    ("given", r"^NOME$"),
    ("title", r"^TITOLO"),
    ("participants", r"^PARTECIPANTI$"),
    ("visitor_amount", r"^ASSEGNAZIONE IN EURO PER VISITATORE"),
    ("amount", r"^(CONTRIBUTO ASSEGNATO|ASSEGNAZIONE|FINANZIAMENTO)"),
    ("inst", r"^(AFFILIAZIONE|UNIVERSIT)"),
    ("role", r"^POSIZIONE$"),
    ("score", r"^PUNTEGGIO$"),
]


def classify(cell: str) -> str | None:
    c = re.sub(r"\s+", " ", cell or "").strip().upper()
    for f, pat in HEADER_FIELDS:
        if re.search(pat, c):
            return f
    return None


def _field(cx: float, hmap: list) -> str:
    f = hmap[0][1]
    for hx, fld in hmap:
        if hx - 3 <= cx:
            f = fld
    return f


def _add(recs: list, flds: dict, hmap: list) -> None:
    # Where the list has an amount column, every project row carries an amount; a
    # row without one is the tail of the previous project split over a page break.
    needs_amount = any(f == "amount" for _, f in hmap)
    if "title" in flds and any(k in flds for k in ("coordblock", "coord", "family"))             and (not needs_amount or "amount" in flds):
        recs.append(flds)
    elif recs:  # a row split over a page break
        for k, v in flds.items():
            recs[-1].setdefault(k, []).extend(v)


def parse_table_pdf(path: Path, stop_on_dates: bool = False) -> list[dict]:
    """One record per table row; columns mapped to the header row by x-position
    (continuation pages repeat or omit the header and split cells differently)."""
    recs, hmap = [], None
    with pdfplumber.open(str(path)) as pdf:
        for pg in pdf.pages:
            tables = pg.find_tables()
            if hmap and tables:
                # 2016 p.2: the first row sits above the detected table box; rebuild
                # it from word positions (accepted only if its amount cell is numeric)
                top = min(tb.bbox[1] for tb in tables)
                words = [w for w in pg.extract_words() if 40 < w["top"] and w["bottom"] < top]
                if words:
                    flds = {}
                    for w in sorted(words, key=lambda w: (round(w["top"]), w["x0"])):
                        flds.setdefault(_field((w["x0"] + w["x1"]) / 2, hmap), []).append(w["text"])
                    has_amount_col = any(f == "amount" for _, f in hmap)
                    numeric = re.fullmatch(r"[\d.,€ +]+", " ".join(flds.get("amount", ["x"])))
                    if (numeric if has_amount_col else len(words) < 60):
                        _add(recs, flds, hmap)
            for tb in tables:
                for row, trow in zip(tb.extract(), tb.rows):
                    texts = [(c or "").strip() for c in row]
                    if any(t.upper() == "PERIODO" for t in texts):
                        return recs  # 2019: the visiting-professor table follows the projects
                    hits = [(b[0], classify(t)) for t, b in zip(texts, trow.cells) if t and b]
                    if sum(1 for _, f in hits if f) >= 2 and any(f == "title" for _, f in hits):
                        hmap = sorted((x, f) for x, f in hits if f)
                        continue
                    if hmap is None or not any(texts):
                        continue
                    if stop_on_dates and any(re.fullmatch(r"\d{2}/\d{2}/\d{2,4}", t) for t in texts):
                        return recs  # 2025 report: the workshops table follows the projects table
                    flds = {}
                    for t, b in zip(texts, trow.cells):
                        if t and b:
                            flds.setdefault(_field((b[0] + b[2]) / 2, hmap), []).append(t)
                    _add(recs, flds, hmap)
    return recs


def parse_2012(path: Path) -> list[dict]:
    """2012 is plain text: 'Family' / title lines / 'Contributo: N €'."""
    with pdfplumber.open(str(path)) as pdf:
        text = "\n".join(pg.extract_text() or "" for pg in pdf.pages)
    body = text[text.find("nel bando.") + len("nel bando."):] if "nel bando." in text else text
    recs, buf = [], []
    for line in (l.strip() for l in body.splitlines()):
        if not line:
            continue
        if line.startswith("Contributo:"):
            if len(buf) >= 2:
                recs.append({"coord": [buf[0]], "title": [" ".join(buf[1:]).lstrip("-")], "amount": [line]})
            buf = []
        else:
            buf.append(line)
    return recs


def parse_amount(s: str | None) -> float | None:
    """'€ 6.500,00', '6.500 €', '€ 8000', '3500+700' (project + visiting professor),
    'Contributo: 4000 €', '1500 Sjoerd Verduyn LUNEL ...' (leading number only)."""
    if not s:
        return None
    total, found = 0.0, False
    for part in re.findall(r"\d[\d.,]*(?:\s*\+\s*\d[\d.,]*)*", s.split("\n")[0])[:1]:
        for num in part.split("+"):
            num = num.strip().strip(",.")
            m = re.search(r",(\d{1,2})$", num)
            whole = re.sub(r"[.,]", "", num[: m.start()] if m else num)
            if whole:
                total += float(f"{whole}.{m.group(1)}" if m else whole)
                found = True
    return total if found else None


def resolve_name(raw: str, order: str, mem_by_family: dict, member_keys: set) -> tuple[str | None, str | None]:
    """Coordinator string -> (given, family). Handles 'Prof. Aimi A.', 'G. ALBI',
    'DE MARCHI', 'STEFANIA BELLAVIA (8+7)', 'Stefano BERRONE', 'Di Serafino'.
    Uses the GNCS members lists (COGNOME/NOME separate) to fix the split and to
    expand initials / bare surnames when exactly one member matches."""
    s = re.sub(r"\(.*?\)", " ", raw or "")
    s = TITLES_RE.sub("", s.strip()).replace("’", "'")
    tokens = [t for t in s.split() if t]
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    initials = [t for t in tokens if re.fullmatch(r"(?:[A-Za-z]\.)+", t)]  # "A.", "M.L."
    words = [t for t in tokens if t not in initials]
    # exact (family, given) member match in either order
    for k in range(1, len(words)):
        for given, family in ((" ".join(words[k:]), " ".join(words[:k])), (" ".join(words[:k]), " ".join(words[k:]))):
            if (norm(family), norm(given)) in member_keys:
                return proper(given), proper(family)
    # bare surname (+ optional initial): expand when one member fits
    if len(words) == 1 or initials or order == "family_only":
        fam = " ".join(words)
        cands = {}
        for g in mem_by_family.get(norm(fam), set()):  # same name in CAPS and Title case
            if not initials or norm(g)[:1] == initials[0][0].lower():
                cands.setdefault(norm(g), set()).add(g)
        if len(cands) == 1:
            spellings = sorted(next(iter(cands.values())), key=lambda g: g.isupper())
            return proper(spellings[0]), proper(fam)
    if initials or order == "family_only" or len(words) == 1:
        return (initials[0] if initials else None), proper(" ".join(words))
    # mixed case 'Stefano BERRONE': the all-caps tokens are the surname
    caps = [w for w in words if w.isupper() and len(w) > 1]
    if caps and len(caps) < len(words):
        return proper(" ".join(w for w in words if w not in caps)), proper(" ".join(caps))
    if order == "family_first":
        k = 1
        while k < len(words) - 1 and words[k - 1].lower() in PARTICLES:
            k += 1
        return proper(" ".join(words[k:])), proper(" ".join(words[:k]))
    k = len(words) - 1
    while k > 1 and words[k - 1].lower() in PARTICLES:
        k -= 1
    return proper(" ".join(words[:k])), proper(" ".join(words[k:]))


NAME_ORDER = {2011: "family_first", 2012: "family_only", 2017: "family_only"}  # default given_first


def main() -> None:
    p = argparse.ArgumentParser(description="INdAM-GNCS funded research projects (PDF lists) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N source files (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache PDFs/HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    sources = SOURCES[: args.limit] if args.limit else SOURCES
    mem = members(args.cache_dir, range(2013, 2027))
    member_keys = {(norm(f), norm(g)) for rows in mem.values() for f, g, _ in rows}
    mem_by_family: dict[str, set] = {}
    for rows in mem.values():
        for f, g, _ in rows:
            mem_by_family.setdefault(norm(f), set()).add(g.strip())
    member_aff = {(y, norm(f), norm(g)): a for y, rows in mem.items() for f, g, a in rows}

    rows = []
    for year, scheme, url, kind in sources:
        pdf_bytes = cached(args.cache_dir, url.rsplit("/", 1)[-1].replace("%20", "_"), url, binary=True)
        tmp = (args.cache_dir or args.output_dir) / f"_gncs_{kind}_{year}_{slug(scheme)}.pdf"
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_bytes(pdf_bytes)
        if kind == "text2012":
            recs = parse_2012(tmp)
        else:
            recs = parse_table_pdf(tmp, stop_on_dates=(kind == "report2025"))
        exp = EXPECTED.get((scheme, year)) if "Giovani" in scheme else EXPECTED.get(year)
        flag = "" if exp is None or exp == len(recs) else f"  <-- list header says {exp}"
        log(f"  {year} {scheme}: {len(recs)} records{flag}")
        if exp and abs(len(recs) - exp) > max(1, exp // 10):
            raise SystemExit(f"{year} {scheme}: parsed {len(recs)} records but the list says {exp}")
        for seq, r in enumerate(recs, 1):
            def j(k, sep=" "):
                return tidy(sep.join(r.get(k, [])))
            participants = j("participants", ", ")
            if "coordblock" in r:
                lines = [l.strip() for l in "\n".join(r["coordblock"]).split("\n") if l.strip()]
                coord_raw, rest = lines[0], " ".join(lines[1:])
                participants = tidy(rest) or participants
            else:
                coord_raw = " ".join(" ".join(r.get("coord", [])).splitlines())
            if "family" in r:
                given, family = proper(tidy_name(j("given"))), proper(tidy_name(j("family")))
                if given and family and (norm(family), norm(given)) not in member_keys \
                        and (norm(given), norm(family)) in member_keys:
                    given, family = family, given
                coord_raw = tidy(f"{j('given') or ''} {j('family') or ''}")
            else:
                given, family = resolve_name(coord_raw, NAME_ORDER.get(year, "given_first"), mem_by_family, member_keys)
            inst = tidy(" ".join(" ".join(r.get("inst", [])).split("\n")))
            aff_src = "pdf" if inst else None
            if not inst and family and given and not given.endswith("."):
                for yy in (year, year - 1, year + 1):
                    a = member_aff.get((yy, norm(family), norm(given)))
                    if a:
                        inst, aff_src = a, f"aderenti_{yy}"
                        break
            amount_proj = parse_amount(j("amount", "\n") if r.get("amount") else None)
            amount_vis = parse_amount(r["visitor_amount"][0]) if r.get("visitor_amount") else None
            amount = None if amount_proj is None and amount_vis is None else (amount_proj or 0) + (amount_vis or 0)
            if "Giovani" in scheme and amount is None:
                amount = 1200.0  # list header: every Giovani Ricercatori 2018/19 grant was EUR 1,200
            rows.append({
                "year": str(year),
                "scheme": scheme,
                "page_seq": str(seq),
                "title": tidy(" ".join(" ".join(r.get("title", [])).split("\n"))),
                "coordinator_raw": tidy(coord_raw),
                "lead_given_name": given,
                "lead_family_name": family,
                "lead_institution": inst,
                "lead_institution_source": aff_src,
                "lead_role": tidy(" ".join(r.get("role", []))),
                "amount_text": tidy(" | ".join(r.get("amount", []) + r.get("visitor_amount", []))),
                "amount": amount,
                "amount_visitor": amount_vis,
                "participants_raw": tidy(participants),
                "source_pdf": url,
            })

    df = pd.DataFrame(rows)
    df = df[df["title"].notna() & df["lead_family_name"].notna()].copy()
    prefix = df["scheme"].map(lambda s: "GNCS-GIOVANI" if "Giovani" in s else "GNCS")
    base = [f"{p}-{y}-{slug(f)}" for p, y, f in zip(prefix, df["year"], df["lead_family_name"])]
    dup = pd.Series(base).duplicated(keep=False).values
    df["funder_award_id"] = [f"{b}-{slug(g)}" if d and g else b for b, d, g in zip(base, dup, df["lead_given_name"].fillna(""))]
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        df.loc[dupes, "funder_award_id"] = df.loc[dupes, "funder_award_id"] + "-" + df.loc[dupes, "page_seq"]
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate funder_award_id after disambiguation")

    log(f"Parsed {len(df)} awards ({df.groupby('scheme').size().to_dict()})")
    for c in ["title", "lead_given_name", "lead_family_name", "lead_institution", "amount", "participants_raw"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total amount EUR {df['amount'].sum():,.0f}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "gncs_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_gncs_projects.parquet"
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
