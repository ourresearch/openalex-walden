#!/usr/bin/env python3
"""
Mochida Memorial Foundation for Medical and Pharmaceutical Research to S3
(公益財団法人 持田記念医学薬学振興財団)
==========================================================================

Source: the foundation's recipients page https://www.mochidazaidan.or.jp/r_list.html
links one PDF per year or multi-year block for each programme:

- 研究助成金交付対象者 (research grants), 1983-2025
- 留学補助金交付対象者 (study-abroad grants), 1984-2025

Each list is a table: № / 氏名 (name) / 所属機関名 (institution) / 研究テーマ or
研修テーマ (theme), grouped under year headings ('2023年度…', '平成21年度…') and
research-area headings ('1) バイオ技術を基盤とする…'). Lists from 1999 on are text
PDFs and are parsed here; 1983-1998 lists are scanned images (no text layer) and
are skipped (OCR not attempted).

Ruled tables (2009+) are read cell-by-cell; the 1999-2008 tables have column rules
but no row rules, so rows are cut at the vertical gaps between entries. Columns
come from the vertical rules at each header row; the header text says which
column holds which field (the order changes between eras).

Amounts are not printed in the lists. The foundation's own annual business
reports (事業報告, public_information.html) state a uniform per-grant amount and
the number of grants for each year; those are applied per (programme, year):
research grant JPY 2,000,000 in FY2008 and JPY 3,000,000 in FY2010-2025, the
FY2013 30th-anniversary research grant JPY 10,000,000 (over 3 years), study-abroad
grant JPY 500,000 in FY2008 and FY2010-2025. Other years: NULL (no report online).
The report grant counts are also used to check the parsed row counts.

Names are printed family-first ('岩崎 未央'); kanji names without a space stay
whole in lead_family_name (sumitomo_foundation_to_s3.py precedent).

funder_award_id: none is published; synthetic
'MOCHIDA-<programme>-<year>-<nn>' from the list's own entry number within the
year (and research area), e.g. MOCHIDA-RG-2023-1-07.

Output: s3://openalex-ingest/awards/mochida/mochida_projects.parquet
"""

import argparse
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import pdfplumber
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

SITE = "https://www.mochidazaidan.or.jp"
INDEX = f"{SITE}/r_list.html"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mochida/mochida_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0

PROGRAMS = {
    "kenkyu": ("RG", "Research Grant (研究助成金)", "research"),
    "ryugak": ("SA", "Study Abroad Grant (留学補助金)", "fellowship"),
}
ANNIV = ("AG", "30th Anniversary Research Grant (財団設立30周年記念研究助成金)", "research")

# Per-grant amount (JPY) and number of grants, from the foundation's annual business
# reports (公益財団法人 持田記念医学薬学振興財団 事業報告, public_information.html):
# 'jigyo_h3.pdf' FY2008 ('研究助成金(1件200万円)を82名に交付', '留学補助金…1件50万円'),
# 'jigyo_h22.pdf'..'jigyo_h36.pdf' + 'jigyo_2019.pdf' FY2010-2025 ('研究助成金(1件300万円)を80名に交付'
# … '研究助成金交付対象者100名及び交付金額を1件300万円'); FY2013 also '財団設立30周年記念
# 研究助成金(1件1,000万円 …)を8名に交付'. Study-abroad counts are the numbers finally paid.
REPORT = {
    "RG": {2001: (1_000_000, None), 2008: (2_000_000, 82), 2010: (3_000_000, 80), 2011: (3_000_000, 80), 2012: (3_000_000, 80),
           2013: (3_000_000, 80), 2014: (3_000_000, 90), 2015: (3_000_000, 91), 2016: (3_000_000, 95),
           2017: (3_000_000, 86), 2018: (3_000_000, 96), 2019: (3_000_000, 121), 2020: (3_000_000, 115),
           2021: (3_000_000, 121), 2022: (3_000_000, 137), 2023: (3_000_000, 150), 2024: (3_000_000, 100),
           2025: (3_000_000, 100)},
    "AG": {2013: (10_000_000, 8)},
    # FY2001 from the list itself: '※研究助成金は1件100万円とする。' (11-15kenkyulist.pdf, 平成13年度 section)
    "SA": {2008: (500_000, 14), 2010: (500_000, 20), 2011: (500_000, 19), 2012: (500_000, 19),
           2013: (500_000, 17), 2014: (500_000, 19), 2015: (500_000, 20), 2016: (500_000, 20),
           2017: (500_000, 20), 2018: (500_000, 20), 2019: (500_000, 20), 2020: (500_000, 20),
           2021: (500_000, 20), 2022: (500_000, 20), 2023: (500_000, 30), 2024: (500_000, 20),
           2025: (500_000, 20)},
}

YEAR_RE = re.compile(r"(?:(?P<ad>(?:19|20)\d{2})|平成\s*(?P<h>元|\d{1,2})|令和\s*(?P<r>元|\d{1,2}))\s*年度")
AREA_RE = re.compile(r"^\(?\s*(?P<k>\d{1,2})\s*[)）]\s*(?P<t>\S.*)$")      # '1) バイオ技術…' / '(1) …'
SECTION_RE = re.compile(r"^(?P<k>\d{1,2})\s*[.．]\s*(?P<t>\S.*)$")          # '1.生命科学と医療応用の研究'
BENIGN_RE = re.compile(r"^(No.*テーマ|※.*|研究助成金|留学補助金|研究助成金対象者|留学補助金対象者|交付対象者なし|該当者なし)$")


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, dest: Path | None = None) -> bytes:
    if dest and dest.exists():
        return dest.read_bytes()
    last = None
    for attempt in range(4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            log(f"GET {url} -> {r.status_code} ({len(r.content)} bytes)")
            r.raise_for_status()
            time.sleep(REQUEST_DELAY)
            if dest:
                dest.write_bytes(r.content)
            return r.content
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after retries: {last}")


def nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")


def to_year(m: re.Match) -> int:
    if m.group("ad"):
        return int(m.group("ad"))
    if m.group("h"):
        return 1988 + (1 if m.group("h") == "元" else int(m.group("h")))
    return 2018 + (1 if m.group("r") == "元" else int(m.group("r")))


def is_cjk(ch: str) -> bool:
    return bool(ch) and (ord(ch) > 0x2E80)


def join_lines(lines: list[str]) -> str | None:
    out = ""
    for ln in (x.strip() for x in lines):
        if not ln:
            continue
        if out and not (is_cjk(out[-1]) or is_cjk(ln[0]) or out.endswith("-")):
            out += " "
        out += ln
    return re.sub(r"\s+", " ", out).strip() or None


def join_words(ws: list[str]) -> str:
    """Words on one line: no space between two CJK characters, a space otherwise."""
    out = ""
    for w in ws:
        if out and not (is_cjk(out[-1]) and is_cjk(w[0])):
            out += " "
        out += w
    return out


def cluster(xs: list[float], tol: float = 3.0) -> list[float]:
    out = []
    for x in sorted(xs):
        if not out or x - out[-1] > tol:
            out.append(x)
    return out


def parse_pdf(path: Path, prog_key: str) -> tuple[list[dict], list[str]]:
    """Return grant rows (year, area, no, name, institution, theme) in document order."""
    rows, notes = [], []
    year, section, area, anniv, cols, fields = None, None, None, False, None, None
    expected = {}  # (year, section) -> count printed in the section heading ('1.生命科学関係(22件)')
    with pdfplumber.open(path) as pdf:
        for pno, page in enumerate(pdf.pages, start=1):
            words = [dict(w, text=nfkc(fix_cid(w["text"]))) for w in page.extract_words(keep_blank_chars=False, return_chars=True)
                     if not (w["top"] > page.height - 60 and re.fullmatch(r"[-–]?\s*\d{0,3}\s*[-–]?", w["text"]))]  # page numbers
            lines = page.extract_text_lines()
            v_edges = [e for e in page.edges if e["orientation"] == "v"]
            h_edges = [e for e in page.edges if e["orientation"] == "h" and e["x1"] - e["x0"] > 200]
            long_v = [e for e in v_edges if e["bottom"] - e["top"] > 30]
            left = cluster([e["x0"] for e in long_v])[:1] or [0.0]

            def in_table(ln) -> bool:
                """A wrapped theme line such as '1) の分解制御…' sits inside a ruled table, right of its № column."""
                return ln["x0"] > left[0] + 8 and any(e["top"] < ln["top"] and e["bottom"] > ln["bottom"] for e in long_v)

            # events in vertical order: year / section / area headings and table header rows
            events = []
            for ln in lines:
                t = nfkc(ln["text"]).strip()
                if re.search(r"(テーマ|課\s*題)", t) and re.search(r"(申請者|氏\s*名|研究者名)", t):
                    events.append((ln["top"], "header", None))
                    continue
                if "30周年記念" in t and "交付対象" in t:
                    events.append((ln["top"], "anniv", t))
                    continue
                ym = YEAR_RE.search(t)
                if ym and re.search(r"(研究助成|留学補助|交付対象)", t):
                    events.append((ln["top"], "year", to_year(ym)))
                    continue
                sm = SECTION_RE.match(t)
                if sm and len(t) > 6 and not in_table(ln):
                    cnt = re.search(r"\((\d+)件\)", t)
                    events.append((ln["top"], "section", (sm.group("k"), int(cnt.group(1)) if cnt else None)))
                    continue
                am = AREA_RE.match(t)
                if am and not re.match(r"^\d{1,3}\s", t) and len(t) > 6 and not in_table(ln):
                    events.append((ln["top"], "area", f"{am.group('k')}) {am.group('t')}"))
            events.sort(key=lambda e: e[0])
            # vertical bands: each band runs from an event down to the next one
            marks = [e[0] for e in events] + [page.height]
            band_starts = [0.0] + marks[:-1] if not events or events[0][0] > 40 else []
            segments = []
            if not events or events[0][0] > 40:
                segments.append((0.0, events[0][0] if events else page.height, None))
            for i, ev in enumerate(events):
                segments.append((ev[0], marks[i + 1], ev))
            for top, bottom, ev in segments:
                kind = ev[1] if ev is not None else None
                if kind in ("year", "anniv", "section", "area"):
                    top += 8   # below the heading line; a table continued from the previous page may follow
                if kind == "year":
                    if ev[2] != year:
                        year, section, area = ev[2], None, None
                    anniv = False
                elif kind == "anniv":
                    # '財団設立30周年記念助成交付対象者' carries no year; the FY2013 business report
                    # records these 8 grants ('平成25年10月25日 … 30周年記念研究助成金 … 8名に交付')
                    year, anniv, section, area = 2013, True, None, None
                elif kind == "section":
                    section, area = ev[2][0], None
                    if ev[2][1] is not None and year is not None:
                        expected[(year, section)] = ev[2][1]
                elif kind == "area":
                    area = ev[2]
                elif kind == "header":
                    # header row: columns from the vertical rules crossing it, fields from its words
                    hw = [w for w in words if abs(w["top"] - top) < 4]
                    xs = cluster([e["x0"] for e in v_edges if e["top"] <= top + 4 and e["bottom"] >= top + 6])
                    if len(xs) < 4:
                        notes.append(f"{path.name} p{pno}: header at {top:.0f} has {len(xs)} column rules")
                        continue
                    cols = xs
                    fields = {}
                    for i in range(len(cols) - 1):
                        txt = "".join(w["text"] for w in hw if cols[i] - 2 <= (w["x0"] + w["x1"]) / 2 < cols[i + 1] + 2)
                        if "№" in txt or txt.startswith("No"):
                            fields[i] = "no"
                        elif "氏" in txt or "申請者" in txt or "研究者名" in txt:
                            fields[i] = "name"
                        elif "所属" in txt or "機関" in txt or "所" in txt:
                            fields[i] = "institution"
                        elif "テーマ" in txt or "題" in txt:
                            fields[i] = "theme"
                    if 0 not in fields and cols[1] - cols[0] < 45:   # 2009-10: the № header cell is blank
                        fields[0] = "no"
                    if set(fields.values()) != {"no", "name", "institution", "theme"}:
                        notes.append(f"{path.name} p{pno}: unrecognised header {[w['text'] for w in hw]}")
                        cols = None
                        continue
                    top = max(w["bottom"] for w in hw) + 1
                if cols is None or year is None:
                    continue
                # a table continued on a new page can sit on a different grid: re-read its column rules
                xs = cluster([e["x0"] for e in v_edges if top - 2 <= (e["top"] + e["bottom"]) / 2 <= bottom + 2])
                if len(xs) == len(cols) and xs != cols:
                    cols = xs
                body = [w for w in split_at(words, cols) if top <= w["top"] < bottom - 1 and cols[0] - 2 <= w["x0"] < cols[-1] + 2]
                if not body:
                    continue
                rows += cut_rows(body, cols, fields, h_edges, top, bottom, year, section, area, anniv, prog_key, path.name, pno,
                                 after_heading=kind in ("year", "anniv", "section", "area"))
    return rows, notes, expected


def cut_rows(body, cols, fields, h_edges, top, bottom, year, section, area, anniv, prog_key, fname, pno,
             after_heading=False) -> list[dict]:
    """Split the words of one table band into entries and cells."""
    col_of = lambda w: next((i for i in range(len(cols) - 1) if cols[i] - 2 <= w["x0"] < cols[i + 1] - 1), None)
    no_col = next(i for i, f in fields.items() if f == "no")
    rules = sorted({round(e["top"]) for e in h_edges if top - 2 <= e["top"] <= bottom + 2 and e["x0"] <= cols[0] + 5})
    nums = [w for w in body if col_of(w) == no_col and re.fullmatch(r"\d{1,3}", w["text"])]
    bounds = rules if len(rules) >= 2 else None
    if bounds and (any(sum(a - 1 <= w["top"] < b - 1 for w in nums) > 1 for a, b in zip(bounds, bounds[1:]))
                   or sum(any(a - 1 <= w["top"] < b - 1 for a, b in zip(bounds, bounds[1:])) for w in nums) < len(nums)):
        bounds = None  # only the outer rules, or entries outside them: fall back to gaps
    if bounds is None:  # no row rules: cut at vertical gaps wider than ~1.5 lines
        tops = sorted({round(w["top"], 1) for w in body})
        heights = [w["bottom"] - w["top"] for w in body]
        lh = sorted(heights)[len(heights) // 2] if heights else 10
        bounds, prev = [tops[0] - 1], tops[0]
        for t in tops[1:]:
            if t - prev > 1.55 * lh + 2:
                bounds.append((prev + t) / 2 + lh / 2)
            prev = t
        bounds.append(bottom)
    def lines_of(ws, plain=False):
        lines_ = []
        for w in sorted(ws, key=lambda w: (round(w["top"]), w["x0"])):
            if lines_ and abs(lines_[-1][0] - w["top"]) < 3:
                lines_[-1][1].append(w["text"])
            else:
                lines_.append([w["top"], [w["text"]]])
        return [(" ".join(t) if plain else join_words(t)) for _, t in lines_]

    entries, fragments = [], []
    for a, b in zip(bounds, bounds[1:]):
        cell_words = [w for w in body if a - 1 <= w["top"] < b - 1]
        if not cell_words:
            continue
        cells = {}
        for w in cell_words:
            c = col_of(w)
            if c is not None:
                cells.setdefault(c, []).append(w)
        no_txt = "".join(lines_of(cells.get(no_col, []))).strip()
        mid = sum(w["top"] for w in cell_words) / len(cell_words)
        if re.fullmatch(r"\d{1,3}", no_txt):
            entries.append({"no": int(no_txt), "mid": mid, "cells": cells})
        elif cells:
            fragments.append({"mid": mid, "cells": cells})
    out = []
    for fr in fragments:
        txt = "".join("".join(lines_of(ws)) for ws in fr["cells"].values())
        if BENIGN_RE.match(txt) or not entries:
            if not entries and not BENIGN_RE.match(txt) and not after_heading:
                out.append({"error": f"{fname} p{pno} y{year}: row without a number: {txt[:120]}"})
            continue
        # a wrapped cell line that fell into its own gap band: give it to the nearest entry
        near = min(entries, key=lambda e: abs(e["mid"] - fr["mid"]))
        for c, ws in fr["cells"].items():
            near["cells"].setdefault(c, []).extend(ws)
    for e in entries:
        rec = {"year": year, "section": section, "area": area, "anniv": anniv, "no": e["no"], "page": pno, "file": fname}
        for c, f in fields.items():
            if f == "no":
                continue
            ls = lines_of(e["cells"].get(c, []), plain=(f == "name"))
            rec[f] = (" ".join(ls) if f == "name" else join_lines(ls))
        out.append(rec)
    return out


def fix_cid(t: str) -> str:
    # one unmapped glyph in the 2001-2003 lists: (cid:8443) is 﨑 ('柚﨑 通介', '山﨑 哲男')
    return re.sub(r"\(cid:\d+\)", "", t.replace("(cid:8443)", "﨑"))


def split_at(words: list[dict], cols: list[float]) -> list[dict]:
    """Split words that run across a column rule (2001: 'アンタゴニ斎藤' = theme tail + name)."""
    out = []
    for w in words:
        inner = [x for x in cols[1:-1] if w["x0"] + 1 < x < w["x1"] - 1]
        if not inner or not w.get("chars"):
            out.append(w)
            continue
        edges = [w["x0"] - 1] + inner + [w["x1"] + 1]
        for a, b in zip(edges, edges[1:]):
            cs = [c for c in w["chars"] if a <= (c["x0"] + c["x1"]) / 2 < b]
            if cs:
                out.append(dict(w, text=nfkc(fix_cid("".join(c["text"] for c in cs))), x0=cs[0]["x0"], x1=cs[-1]["x1"]))
    return out


GIVEN_FIRST = {"Richard Wong", "Horacio Cabral", "Jean-Michel Fustin", "Adrian Moore"}   # Latin names printed given-first


def clean_name(name: str | None) -> str | None:
    """Drop a leading hiragana reading ('かわべ ひろし 川辺 浩志' -> '川辺 浩志'), maiden names
    in parentheses ('齊藤 (後藤) 紗希' -> '齊藤 紗希') and stray commas."""
    toks = [t for t in re.split(r"\s+", re.sub(r"\([^)]*\)", " ", name or "").replace(",", " ").strip()) if t]
    toks = [t for t in toks if not re.fullmatch(r"[A-Za-z]?-", t)]   # 2001: 'P-' spilled over from a theme cell
    k = 0
    while k < len(toks) and re.fullmatch(r"[ぁ-ゖー]+", toks[k]):
        k += 1
    if 0 < k < len(toks):
        toks = toks[k:]
    return " ".join(toks) or None


def split_name_ja(name: str | None) -> tuple[str | None, str | None]:
    """Family-first names ('岩崎 未央', 'HEISSIG BEATE'): family = first token, given = rest.
    A name printed without a space stays whole in family (sumitomo precedent). Family-first
    order, so the canonical given-first split_name (runbook 2.4.1) does not apply; Latin names
    in GIVEN_FIRST are the only given-first entries."""
    toks = [t for t in re.split(r"\s+", (name or "").strip()) if t]
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    if " ".join(toks) in GIVEN_FIRST:
        return " ".join(toks[:-1]), toks[-1]
    return " ".join(toks[1:]), toks[0]


def main() -> None:
    p = argparse.ArgumentParser(description="Mochida Memorial Foundation recipient PDFs -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="parse only the N newest PDFs per programme (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/mochida"))
    p.add_argument("--cache-dir", type=Path, default=None)
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-mismatch", action="store_true", help="debug only: do not fail on count mismatches")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    cache = args.cache_dir or (args.output_dir / "pdf")
    cache.mkdir(parents=True, exist_ok=True)

    index = get(INDEX).decode("cp932", errors="replace")
    links = sorted(set(re.findall(r'href="(pdf/[^"]+\.pdf)"', index)))
    by_prog = {k: [l for l in links if k in l] for k in PROGRAMS}
    for k, v in by_prog.items():
        log(f"{k}: {len(v)} PDFs listed")
        if not v:
            raise SystemExit(f"no {k} PDFs found on {INDEX}")

    all_rows, problems = [], []
    for k, files in by_prog.items():
        code, label, ftype = PROGRAMS[k]
        files = sorted(files, key=lambda f: -max(int(x) for x in re.findall(r"\d+", Path(f).stem)) if re.findall(r"\d+", Path(f).stem) else 0)
        if args.limit:
            files = files[: args.limit]
        for f in files:
            dest = cache / Path(f).name
            get(f"{SITE}/{f}", dest)
            with pdfplumber.open(dest) as pdf:
                has_text = any(pg.chars for pg in pdf.pages)
            if not has_text:
                log(f"  {f}: scanned image, no text layer - skipped")
                continue
            rows, notes, expected = parse_pdf(dest, k)
            got = {}
            for r in rows:
                if "error" not in r:
                    got[(r["year"], r["section"])] = got.get((r["year"], r["section"]), 0) + 1
            for key, n in expected.items():
                if got.get(key, 0) != n:
                    problems.append(f"{f} {key}: section heading says {n}, parsed {got.get(key, 0)}")
            for n in notes:
                log(f"  NOTE {n}")
            errs = [r["error"] for r in rows if "error" in r]
            problems += errs
            rows = [r for r in rows if "error" not in r]
            for r in rows:
                r["prog"] = ANNIV[0] if (r["anniv"] and k == "kenkyu") else code
                r["url"] = f"{SITE}/{f}"
            yrs = sorted({r["year"] for r in rows})
            log(f"  {f}: {len(rows)} rows, years {yrs[0] if yrs else '-'}-{yrs[-1] if yrs else '-'}, {len(errs)} errors")
            all_rows += rows

    vacant = [r for r in all_rows if (r.get("name") or "").replace(" ", "") == "欠番"]
    for r in vacant:
        log(f"  vacant entry (欠番) {r['prog']} {r['year']} no {r['no']} - not a grant, dropped")
    # checks: entry numbers restart per (prog, year, area) and run 1..n; report counts per year
    groups = {}
    for r in all_rows:
        groups.setdefault((r["prog"], r["year"], r["section"], r["area"]), []).append(r["no"])
    for g, nos in groups.items():
        if sorted(nos) != list(range(1, len(nos) + 1)) and sorted(nos) != list(range(nos[0], nos[0] + len(nos))):
            problems.append(f"{g}: entry numbers not consecutive: {sorted(nos)[:60]}")
    per_year = {}
    for r in all_rows:
        per_year[(r["prog"], r["year"])] = per_year.get((r["prog"], r["year"]), 0) + 1
    for (prog, y), n in sorted(per_year.items()):
        rep = REPORT.get(prog, {}).get(y)
        flag = "" if rep is None or rep[1] is None else ("ok" if rep[1] == n else f"REPORT SAYS {rep[1]}")
        log(f"  {prog} {y}: {n} rows {flag}")
        if rep is not None and rep[1] is not None and rep[1] != n and not (prog == "SA" and n >= rep[1]):
            problems.append(f"{prog} {y}: parsed {n}, business report {rep[1]}")
    for pr in problems:
        log(f"  PROBLEM {pr}")
    if problems and not args.allow_mismatch:
        raise SystemExit(f"{len(problems)} problems; fix the parser (or --allow-mismatch to inspect)")

    all_rows = [r for r in all_rows if r not in vacant]
    recs, seen = [], set()
    labels = {PROGRAMS[k][0]: PROGRAMS[k] for k in PROGRAMS} | {ANNIV[0]: ANNIV}
    for r in all_rows:
        area_no = (re.match(r"(\d+)\)", r["area"] or "") or [None, "0"])[1]
        fid = f"MOCHIDA-{r['prog']}-{r['year']}-{r['section'] or 0}.{area_no}-{r['no']:02d}"
        if fid.lower() in seen:
            raise SystemExit(f"duplicate funder_award_id {fid}")
        seen.add(fid.lower())
        given, family = split_name_ja(clean_name(r.get("name")))
        rep = REPORT.get(r["prog"], {}).get(r["year"])
        recs.append({
            "funder_award_id": fid,
            "programme": labels[r["prog"]][1],
            "funding_type": labels[r["prog"]][2],
            "grant_year": r["year"],
            "research_area": r["area"],
            "entry_no": r["no"],
            "grantee": r.get("name"),
            "lead_given_name": given,
            "lead_family_name": family,
            "institution": r.get("institution"),
            "title": r.get("theme"),
            "amount": float(rep[0]) if rep else None,
            "currency": "JPY",
            "amount_source": "annual business report per-grant amount" if rep else None,
            "pdf_page": r["page"],
            "landing_page_url": r["url"],
        })
    df = pd.DataFrame(recs)
    log(f"{len(df)} grants, {df['grant_year'].min()}-{df['grant_year'].max()}; by programme {df['programme'].value_counts().to_dict()}")
    for c in ["title", "institution", "lead_family_name", "lead_given_name", "amount", "research_area"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    top = df.groupby(["lead_family_name", "lead_given_name"], dropna=False).size().sort_values(ascending=False).head(8)
    log(f"  6.4a top grantees: {top.to_dict()}")

    df = df.astype("string")
    df["amount"] = pd.to_numeric(df["amount"])
    out = args.output_dir / "mochida_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {len(df)} rows to {out}")
    if args.skip_upload:
        return
    if args.limit:
        raise SystemExit("refusing to upload a --limit smoke file to S3")

    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_mochida_projects.parquet"
    try:  # runbook 1.4: never shrink the corpus on re-ingest
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev}, new {len(df)}")
        if len(df) < prev and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(out), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
