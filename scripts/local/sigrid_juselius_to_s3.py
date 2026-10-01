#!/usr/bin/env python3
"""
Sigrid Jusélius Foundation (Sigrid Juséliuksen Säätiö) to S3
=============================================================

The foundation (Finland's largest private funder of medical research)
publishes its grant decisions as PDF lists linked from its grant pages at
https://www.sigridjuselius.fi/en/apurahat/<grant type>/. This script reads:

1. **1-3-year grants** (the core programme, ~250 decisions a year), one or
   two PDFs per year 2012-2026 ("Awarded grants for Senior researchers /
   Young group leaders", earlier "domestic grants"). Formats differ by era:
     - 2012-2014: one long list; group leaders marked '*' with title and
       amount, followed by group members (not shipped). '(H)' = Homing grant.
       No grant-year marker.
     - 2015: one 4-column sheet (senior new / senior continuation / young
       new / young continuation); column read from the amount's x position.
     - 2016-2026: one PDF per target group, tables 'Name | [Institution] |
       Amount | (k/N)'. From 2020 the institution is printed.
   Each row is one ANNUAL instalment of a 1-3 year grant: '(2/3)' = second
   year of a three-year grant. Instalments are chained into one award per
   grant (same lead, consecutive years, k-1 -> k); amount = sum of the
   instalments seen. A chain whose first instalment is not in the lists
   (started before 2012) ships with amount NULL. The grant period runs
   1 May - 30 April (stated on the grant page), so start_date = <first
   year>-05-01 and end_date = <first year + N>-04-30 when N is printed.
2. **Senior researcher / Senior clinical researcher posts** (3-4 year
   posts, 2016-2025): one cumulative PDF, 'Doc. Name, Institution amount'.
3. **Large grants** (5 years, 2013-2026): one cumulative PDF,
   'Names, Institution, YYYY-YYYY, EUR N (annually)'; amount = annual x years.

Not parsed (follow-up): Fellowship, Senior Fellowship and Visiting
Professor cumulative lists (free-text multi-line entries) and the four
named professorships (news posts only).

Every row is medical research funding; nothing is filtered out.
Names are printed family-first ('Aaltonen Lauri'); split_family_first
handles that with the canonical suffix set (runbook 2.4.1).

funder_award_id: the foundation publishes no grant number (citing works
write 6-digit application numbers such as 230181 that the lists never
show), so a stable synthetic key is used:
'SJS-<first year>-<scheme code>-<lead family>-<lead given>' (ASCII slug).
Collisions raise.

Output: s3://openalex-ingest/awards/sigrid_juselius/sigrid_juselius_projects.parquet
"""

import argparse
import json
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
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). Production
# runs on Linux/Databricks where UTF-8 is the default. See runbook 1.2.
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

SITE = "https://www.sigridjuselius.fi"
PAGES = {
    "one_to_three": f"{SITE}/en/apurahat/1-3-year-grants/",
    "senior_posts": f"{SITE}/en/apurahat/senior-researcher-4-years/",
    "large": f"{SITE}/en/apurahat/large-grant-35-years/",
}
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/sigrid_juselius/sigrid_juselius_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5

SCHEMES = {
    "SR": "1-3-year grant, senior researchers",
    "YG": "1-3-year grant, young group leaders",
    "DG": "1-3-year grant (domestic grant)",
    "HG": "1-3-year grant (Homing grant)",
    "SRP": "Senior researcher post",
    "SCRP": "Senior clinical researcher post",
    "LG": "Large grant",
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch(url: str, cache_dir: Path, binary: bool = False):
    # short names: long cache paths break on Windows MAX_PATH
    name = url.rstrip("/").rsplit("/", 1)[-1][-90:] + ("" if binary else ".html")
    f = cache_dir / name
    if f.exists():
        return f.read_bytes() if binary else f.read_text()
    last = None
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            r.raise_for_status()
            time.sleep(REQUEST_DELAY)
            if binary:
                f.write_bytes(r.content)
                return r.content
            r.encoding = "utf-8"
            f.write_text(r.text)
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def pdf_links(page_html: str, text_re: str) -> list[tuple[str, str]]:
    out = []
    for href, txt in re.findall(r'<a[^>]+href="([^"]+\.pdf)"[^>]*>(.*?)</a>', page_html, re.S):
        t = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", txt)).strip()
        if re.search(text_re, t, re.I):
            out.append((href, t))
    return out


def norm(s: str | None) -> str | None:
    if s is None:
        return None
    s = s.replace("­", "").replace("’", "'")
    s = re.sub(r"[‐‑‒–-]+", "-", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def amount(s: str | None) -> float | None:
    if not s:
        return None
    d = re.sub(r"[^\d]", "", s)
    return float(d) if d else None


SUFFIXES = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
PARTICLES = {"von", "van", "de", "der", "den", "la", "le", "di", "da", "af", "du"}


def split_family_first(name: str) -> tuple[str | None, str | None]:
    """'Aaltonen Lauri' -> ('Lauri', 'Aaltonen'); 'Jacobs Howard Trevor' ->
    ('Howard Trevor', 'Jacobs'); 'von Knorring Anna' -> ('Anna', 'von Knorring').
    Strips the canonical trailing suffixes first (runbook 2.4.1)."""
    tokens = (name or "").replace(",", " ").split()
    while tokens and tokens[-1].lower().strip(",.") in SUFFIXES:
        tokens.pop()
    if not tokens:
        return None, None
    i = 0
    while i < len(tokens) - 1 and tokens[i].lower() in PARTICLES:
        i += 1
    family = " ".join(tokens[: i + 1])
    given = " ".join(tokens[i + 1:]) or None
    return given, family


def split_given_first(name: str) -> tuple[str | None, str | None]:
    """'Carlos Figueiredo' -> ('Carlos', 'Figueiredo') (canonical split_name)."""
    tokens = (name or "").split()
    while tokens and tokens[-1].lower().strip(",.") in SUFFIXES:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def people_from(names: str, family_first: bool = True) -> list[dict]:
    out = []
    for n in re.split(r",\s*|\s+&\s+|\s+ja\s+|\s+och\s+", names or ""):
        n = norm(n)
        if not n or len(n) < 3 or n.startswith("N.N"):  # 'N.N PhD student' = unnamed post
            continue
        given, family = (split_family_first if family_first else split_given_first)(n)
        out.append({"name": n, "given_name": given, "family_name": family})
    return out


# ------------------------------------------------- 1-3-year grant lists ----

KN_RE = re.compile(r"\(?\s*(\d)\s*/\s*(\d)\s*\)?")


def header_info(text: str) -> tuple[int, str | None, str | None]:
    m = re.search(r"(?:apurahat|understöd)\s+(20\d\d),\s*myöntöpäivä\s+(\d{1,2})\.(\d{1,2})\.(20\d\d)", text)
    if not m:
        raise ValueError(f"no year/decision date in header: {text[:200]!r}")
    year = int(m.group(1))
    ddate = f"{m.group(4)}-{int(m.group(3)):02d}-{int(m.group(2)):02d}"
    group = None
    if re.match(r"\s*Varttuneet tutkijat", text):
        group = "SR"
    elif re.match(r"\s*Nuoret tutkijat", text):
        group = "YG"
    return year, ddate, group


def parse_2012_2014(pdf, year, ddate, src) -> list[dict]:
    raw_lines = [norm(l) for pg in pdf.pages for l in (pg.extract_text() or "").split("\n")]
    lines = []
    for l in raw_lines:
        # re-join a wrapped leader line: '* Raivio Jaakko ..., Dosentti (Professori' + '2013->) 46.500'
        if (l and lines and lines[-1] and lines[-1].startswith("*") and not re.search(r"\d{1,3}\.\d{3}", lines[-1])
                and re.match(r"^[^*A-ZÅÄÖ].*\d{1,3}\.\d{3}(\s*\(H\))?$", l)):
            lines[-1] = f"{lines[-1]} {l}"
        else:
            lines.append(l)
    expected = None
    rows = []
    lead_re = re.compile(r"^\*\s+(?P<name>[^,]+?)(?:,\s*(?P<title>.*?))?\s+(?P<amt>\d{1,3}(?:\.\d{3})+)\s*(?P<h>\(H\))?$")
    co_re = re.compile(r"^\*\s+(?P<name>[^,]+?)(?:,\s*(?P<title>.*))?$")
    for l in lines:
        if not l:
            continue
        m = re.match(r"Myönnetyt/[Bb]eviljade\s+(\d+)", l)
        if m:
            expected = int(m.group(1))
            continue
        if l.startswith("*)"):
            continue
        m = lead_re.match(l)
        if m:
            rows.append({"year": year, "decision_date": ddate, "scheme": "HG" if m.group("h") else "DG",
                         "names": [m.group("name")], "titles": [m.group("title")],
                         "institution": None, "amount": amount(m.group("amt")), "k": None, "n": None,
                         "source_pdf": src})
            continue
        m = co_re.match(l)
        if m and rows:
            rows[-1]["names"].append(m.group("name"))
            rows[-1]["titles"].append(m.group("title"))
    if expected is not None and expected != len(rows):
        raise SystemExit(f"{src}: parsed {len(rows)} leaders, list says {expected}")
    return rows


def parse_2015(pdf, year, ddate, src) -> list[dict]:
    rows = []
    for pg in pdf.pages:
        lines = {}
        for w in pg.extract_words():
            lines.setdefault(round(w["top"] / 3), []).append(w)
        # (k/N) sits ~1pt below its name line: merge into the nearest line
        keys = sorted(lines)
        merged = []
        for k in keys:
            if merged and k - merged[-1][0] <= 1:
                merged[-1][1].extend(lines[k])
            else:
                merged.append((k, list(lines[k])))
        for _, ws in merged:
            ws.sort(key=lambda w: w["x0"])
            kn = [w for w in ws if KN_RE.fullmatch(w["text"])]
            amt_ws = [w for w in ws if re.fullmatch(r"\d{1,3}", w["text"]) and w["x0"] > 480]
            name_ws = [w for w in ws if w["x0"] < 400 and not KN_RE.fullmatch(w["text"])]
            if not kn or not amt_ws or not name_ws:
                continue
            x = amt_ws[0]["x0"]
            k, n = map(int, KN_RE.fullmatch(kn[0]["text"]).groups())
            rows.append({"year": year, "decision_date": ddate, "scheme": "SR" if x < 790 else "YG",
                         "names": [" ".join(w["text"] for w in name_ws)], "titles": [None],
                         "institution": None, "amount": amount("".join(w["text"] for w in amt_ws)),
                         "k": k, "n": n, "source_pdf": src})
    return rows


def parse_tables(pdf, year, ddate, group, src) -> list[dict]:
    rows = []
    amt_re = r"\d{1,3}(?:[ .]\d{3})*"
    for pg in pdf.pages:
        for t in pg.extract_tables():
            queued_amt, queued_kn = [], []
            for r in t:
                # pdfplumber sometimes merges the amount / (k/N) cells of several
                # consecutive rows into one newline-joined cell, leaving the
                # following rows' cells empty: hand the extra values down.
                raw = [c or "" for c in r]
                amt_parts = [norm(x) for x in raw[-2].split("\n") if norm(x)] if len(raw) >= 3 else []
                kn_parts = [norm(x) for x in raw[-1].split("\n") if norm(x)]
                if len(amt_parts) > 1 and all(re.fullmatch(amt_re, x) for x in amt_parts):
                    queued_amt = amt_parts[1:]
                    amt_parts = amt_parts[:1]
                if len(kn_parts) > 1 and all(KN_RE.fullmatch(x) for x in kn_parts):
                    queued_kn = kn_parts[1:]
                    kn_parts = kn_parts[:1]
                cells = [norm(c.replace("\n", " ")) if c else None for c in r]
                if cells[0] and not amt_parts and queued_amt:
                    amt_parts = [queued_amt.pop(0)]
                    if not kn_parts and queued_kn:
                        kn_parts = [queued_kn.pop(0)]
                cells[-2] = amt_parts[0] if amt_parts else cells[-2]
                cells[-1] = kn_parts[0] if kn_parts else cells[-1]
                kn = next((KN_RE.fullmatch(c) for c in cells[1:] if c and KN_RE.fullmatch(c)), None)
                amt = next((c for c in cells[1:] if c and re.fullmatch(amt_re, c)), None)
                if not cells[0] or not amt or re.match(r'(Totalt|Yhteensä|Total)', cells[0]):
                    continue
                if kn is None and cells[-1] and re.fullmatch(r"\(?1\)?", cells[-1]):
                    kn = KN_RE.fullmatch("1/1")  # one-year grant
                inst = cells[1] if len(cells) == 4 else None
                rows.append({"year": year, "decision_date": ddate, "scheme": group,
                             "names": [cells[0]], "titles": [None], "institution": inst,
                             "amount": amount(amt),
                             "k": int(kn.group(1)) if kn else None, "n": int(kn.group(2)) if kn else None,
                             "source_pdf": src})
    return rows


# '(2/3)' or '2/3'; a bare '1' marks a one-year grant (= 1/1)
TEXT_REC_RE = re.compile(r"(?P<pre>[A-ZÅÄÖÜa-zåäöü].*?)\s+(?P<amt>\d{1,3}(?: \d{3})+)\s+"
                         r"(?:\(?(?P<k>\d)\s*/\s*(?P<n>\d)\)?|(?P<one>1)(?=\s|$))")
# first token of an institution name in the 2020+ lists ('Turun yliopisto / Åbo universitet')
INST_START_RE = re.compile(
    r"^(Helsingin|Turun|Tampereen|Oulun|Itä-Suomen|Jyväskylän|Lapin|Vaasan|Åbo|Aalto|HUS|HYKS|TYKS|TAYS|KYS|OYS|"
    r"Samfundet|Folkhälsan|Terveyden|THL|VTT|Wihuri|Minerva|Biomedicum|Tampere|Turku|Helsinki|Oulu|University|"
    r"Itä|Hyvinvointialue|Pirkanmaan|Varsinais-Suomen|Pohjois-|Keski-|Etelä-|Kuopion|Seinäjoen|Satakunnan|"
    r"Lahden|Kymenlaakson|Kanta-Hämeen|Päijät-Hämeen|Folkhälsanin|Suomen|Lääkäriseura|Duodecim|Karolinska|FIMM)\b")


def parse_text_records(pdf, year, ddate, group, src) -> list[dict]:
    rows = []
    for pg in pdf.pages:
        for line in (pg.extract_text() or "").split("\n"):
            for m in TEXT_REC_RE.finditer(norm(line) or ""):
                pre = m.group("pre").strip()
                # a column total can share a line with the next record:
                # 'Totalt 864 000 Pasonen-Seppänen Sanna ...' / '4 174 000 Olkkonen Vesa ...'
                pre = re.sub(r"^(?:(?:Yhteensä\s*/\s*)?Totalt\s+)?\d{1,3}(?: \d{3})+\s+(?=[A-ZÅÄÖ])", "", pre)
                if re.match(r"(Totalt|Yhteensä|Total)\b", pre):
                    continue
                k, n = (1, 1) if m.group("one") else (int(m.group("k")), int(m.group("n")))
                toks = pre.split()
                cut = next((i for i, t in enumerate(toks) if i >= 2 and INST_START_RE.match(t)), len(toks))
                names, inst = " ".join(toks[:cut]), " ".join(toks[cut:]) or None
                rows.append({"year": year, "decision_date": ddate, "scheme": group,
                             "names": [names], "titles": [None], "institution": inst,
                             "amount": amount(m.group("amt")), "k": k, "n": n,
                             "source_pdf": src})
    return rows


def rec_key(r: dict) -> tuple:
    first = (r["names"][0] or "").split()
    return (first[0] if first else "", r["amount"], r["k"], r["n"])


def reconcile(table_rows: list[dict], text_rows: list[dict], expected: int | None) -> list[dict]:
    """Tables carry a clean name/institution split but pdfplumber drops the
    odd row at a page break; the text regex sees every row but has to guess
    where the institution starts. Use the tables, plus any text record the
    tables do not have."""
    if expected is not None and len(table_rows) == expected:
        return table_rows
    if expected is not None and len(text_rows) == expected:
        # every row seen by the text regex: take the name/institution split
        # from the matching table row where there is one
        pool = list(table_rows)
        for r in text_rows:
            full = " ".join(filter(None, [r["names"][0], r["institution"]]))
            for i, t in enumerate(pool):
                if (t["amount"], t["k"], t["n"]) == (r["amount"], r["k"], r["n"]) and full.startswith(t["names"][0] or "\0"):
                    r["names"], r["institution"] = t["names"], t["institution"]
                    pool.pop(i)
                    break
        return text_rows
    have = {}
    for r in table_rows:
        have[rec_key(r)] = have.get(rec_key(r), 0) + 1
    extra = []
    for r in text_rows:
        k = rec_key(r)
        if have.get(k, 0) > 0:
            have[k] -= 1
        else:
            extra.append(r)
    return table_rows + extra


def printed_totals(pdf) -> list[float]:
    """Column totals printed under the tables ('Totalt 8 763 400', a bare
    '7 989 000', or a total glued to the start of a row line)."""
    out = []
    for pg in pdf.pages:
        for line in (pg.extract_text() or "").split("\n"):
            line = norm(line) or ""
            m = re.match(r"^(?:Yhteensä\s*/\s*)?(?:Totalt\s+)?(\d{1,3}(?: \d{3})+)(?=\s+[A-ZÅÄÖ]|\s*(?:\d{1,3})?\s*$)", line)
            if m and (line.startswith(("Totalt", "Yhteensä")) or not re.search(r"\d/\d", line[: m.end()])):
                if line.startswith(("Totalt", "Yhteensä")) or m.end() == len(line) or re.match(r"^\d{1,3}(?: \d{3}){2,}\s+[A-ZÅÄÖ]", line):
                    out.append(amount(m.group(1)))
    return out


def expected_count(text: str) -> int | None:
    m = re.search(r"uusia myöntöjä\s*\(?(\d+)\s*kpl\)?\s*ja jatkomyöntöjä\s*\(?(\d+)", text)
    if m:
        return int(m.group(1)) + int(m.group(2))
    m = re.search(r"myöntöjä\s*\(?(\d+)\s*kpl", text)
    return int(m.group(1)) if m else None


def parse_one_to_three(path: Path, src: str) -> list[dict]:
    with pdfplumber.open(path) as pdf:
        head = norm((pdf.pages[0].extract_text() or "")[:800].replace("\n", " "))
        year, ddate, group = header_info(head)
        if year <= 2014:
            rows = parse_2012_2014(pdf, year, ddate, src)
        elif year == 2015:
            rows = parse_2015(pdf, year, ddate, src)
        else:
            if group is None:
                raise SystemExit(f"{src}: no target group in header")
            exp = expected_count(head)
            text_rows = parse_text_records(pdf, year, ddate, group, src)
            if year < 2020:  # no institution column: the text regex is exact
                rows = text_rows
            else:
                rows = reconcile(parse_tables(pdf, year, ddate, group, src), text_rows, exp)
            if exp is not None and exp != len(rows):
                # the printed count is occasionally off by one; the printed
                # column totals are the stronger check
                totals = printed_totals(pdf)
                got = sum(r["amount"] or 0 for r in rows)
                if not totals or abs(sum(totals) - got) > 0.5:
                    raise SystemExit(f"{src}: parsed {len(rows)} rows (EUR {got:,.0f}), header says {exp}, totals {totals}")
                log(f"  note: {src.rsplit('/', 1)[-1]} header count {exp} but {len(rows)} rows; "
                    f"amounts match the printed totals (EUR {got:,.0f})")
    return rows


def name_key(name: str) -> str:
    s = unicodedata.normalize("NFKD", norm(name) or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z]+", "-", s).strip("-")


def chain(decisions: list[dict]) -> list[dict]:
    """Group annual instalments into grants. A (k/N) row with k>1 joins the
    chain of the same lead whose last instalment was the previous year;
    otherwise it opens an incomplete chain (first instalment not listed)."""
    decisions = sorted(decisions, key=lambda r: (r["year"], r["source_pdf"]))
    open_by_lead: dict[str, dict] = {}
    chains = []
    for r in decisions:
        # first person only: co-applicants are listed in varying order/forms
        lead = name_key(re.split(r",\s*|\s+&\s+", r["names"][0] or "")[0])
        c = open_by_lead.get(lead)
        if r["k"] and r["k"] > 1 and c and c["last_year"] == r["year"] - 1 and (c["last_k"] in (None, r["k"] - 1)):
            c["rows"].append(r)
        else:
            c = {"rows": [r], "complete": not (r["k"] and r["k"] > 1)}
            chains.append(c)
        c["last_year"], c["last_k"] = r["year"], r["k"]
        open_by_lead[lead] = c
    return chains


def awards_from_chains(chains: list[dict], src_page: str) -> list[dict]:
    out = []
    for c in chains:
        rows = c["rows"]
        first, last = rows[0], rows[-1]
        start_year = first["year"] - ((first["k"] or 1) - 1)
        n = last["n"] or first["n"]
        scheme = next((r["scheme"] for r in reversed(rows) if r["scheme"] in ("SR", "YG")), first["scheme"])
        people = people_from(", ".join(rows[-1]["names"]))
        if len(rows[0]["names"]) > 1 and len(people) < len(rows[0]["names"]):
            people = [p for nm in rows[0]["names"] for p in people_from(nm)]
        out.append({
            "scheme_code": scheme,
            "start_year": start_year,
            "first_decision_date": first["decision_date"] if c["complete"] else None,
            "start_date": f"{start_year}-05-01",
            "end_date": f"{start_year + n}-04-30" if n else None,
            "duration_years": n,
            "amount": sum(r["amount"] or 0 for r in rows) if c["complete"] else None,
            "instalments": json.dumps([{"year": r["year"], "k": r["k"], "n": r["n"], "amount": r["amount"],
                                        "decision_date": r["decision_date"], "scheme": r["scheme"]} for r in rows]),
            "n_instalments": len(rows),
            "chain_complete": c["complete"],
            "people": people,
            "titles": [t for t in rows[0]["titles"] if t],
            "institution": next((r["institution"] for r in reversed(rows) if r["institution"]), None),
            "project_title": None,
            "landing_page_url": src_page,
            "source_pdfs": json.dumps(sorted({r["source_pdf"] for r in rows})),
        })
    return out


# ---------------------------------------------- senior posts / large ----

TITLE_PREFIX_RE = re.compile(
    r"^(?:(?:Adj\.?\s*Prof\.?|Prof\.?|Professor|Research Prof\.?|Doc\.?|Docent|PhD\.?|Ph\.D\.?|DMedSc\.?|"
    r"MD\.?|FD|Dr\.?|D\.Sc\.?)[\s,]*)+")


def order_checked(gf: tuple, known: set) -> tuple:
    """The cumulative lists mix 'Given Family' and 'Family Given'. If only the
    swapped order matches a (family, given) pair from the 1-3-year grant
    lists (always family-first), swap."""
    given, family = gf
    if given and family and (family, given) not in known and (given, family) in known:
        return family, given
    return given, family


def parse_senior_posts(path: Path, src: str, src_page: str, known: set) -> list[dict]:
    with pdfplumber.open(path) as pdf:
        raw = [norm(l) for pg in pdf.pages for l in (pg.extract_text() or "").split("\n")]
    lines = []
    for l in raw:
        # 'Doc. Sarka Lehtonen (...), A.I.Virtanen Institute for' + '500 000€'
        if l and lines and re.fullmatch(r"\d{1,3}(?:[ .]\d{3})+\s*€", l) and not re.search(r"€", lines[-1] or ""):
            lines[-1] = f"{lines[-1]} {l}"
        else:
            lines.append(l)
    out, ddate, scheme, years = [], None, None, None
    for l in lines:
        if not l:
            continue
        m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(20\d\d)\b(.*)$", l)
        if m:
            ddate = f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
            ym = re.search(r"\((\d) years\)", m.group(4))
            if ym:
                scheme, years = "SRP", int(ym.group(1))
            continue
        m = re.match(r"^Senior (Clinical )?researcher \((\d) years\)", l, re.I)
        if m:
            scheme, years = ("SCRP" if m.group(1) else "SRP"), int(m.group(2))
            continue
        m = re.match(r"^(?P<rest>.+?)\s+(?P<amt>\d{1,3}(?:[ .]\d{3})+)\s*€", l)
        if m and ddate:
            rest = TITLE_PREFIX_RE.sub("", m.group("rest"))
            name, _, inst = rest.partition(",")
            name = norm(re.sub(r"\s*\((?:född|o\.s\.|née?)[^)]*\)", "", name))
            inst = norm(inst.strip(" .")) or None
            year = int(ddate[:4])
            # older entries are family-first ('Doc. Leppänen Jukka')
            fam_first = year <= 2016
            given, family = order_checked((split_family_first if fam_first else split_given_first)(name), known)
            out.append({
                "scheme_code": scheme, "start_year": year + 1 if ddate[5:7] == "12" else year,
                "first_decision_date": ddate, "start_date": None, "end_date": None,
                "duration_years": years, "amount": amount(m.group("amt")),
                "instalments": None, "n_instalments": 1, "chain_complete": True,
                "people": [{"name": name, "given_name": given, "family_name": family}],
                "titles": [], "institution": inst, "project_title": None,
                "landing_page_url": src_page, "source_pdfs": json.dumps([src]),
            })
    return out


def parse_large(path: Path, src: str, src_page: str, known: set) -> list[dict]:
    with pdfplumber.open(path) as pdf:
        text = " ".join(norm(pg.extract_text() or "") for pg in pdf.pages)
    text = text.split("holders:", 1)[-1]
    out = []
    for m in re.finditer(r"(?P<who>[^,]+(?:&[^,]+)?),\s*(?P<inst>.+?),\s*(?P<y0>20\d\d)-(?P<y1>20\d\d),\s*EUR\s*(?P<amt>[\d ]+?)\s*\(annually\)", text):
        y0, y1 = int(m.group("y0")), int(m.group("y1"))
        who = norm(m.group("who"))
        # 'Sirpa Leppä & Päivi Ojala' is given-first, 'Knip Mikael' family-first
        parts = [norm(x) for x in who.split("&")]
        people = []
        for p in parts:
            # the list switches to given-first names from the 2019 cohort on
            g, f = order_checked((split_family_first if y0 <= 2018 else split_given_first)(p), known)
            people.append({"name": p, "given_name": g, "family_name": f})
        annual = amount(m.group("amt"))
        out.append({
            "scheme_code": "LG", "start_year": y0, "first_decision_date": None,
            "start_date": f"{y0}-05-01", "end_date": f"{y1}-04-30", "duration_years": y1 - y0,
            "amount": annual * (y1 - y0) if annual else None,
            "instalments": json.dumps({"annual_amount": annual}), "n_instalments": y1 - y0,
            "chain_complete": True, "people": people, "titles": [],
            "institution": norm(m.group("inst")), "project_title": None,
            "landing_page_url": src_page, "source_pdfs": json.dumps([src]),
        })
    return out


def slug(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-") or "x"


def main() -> None:
    p = argparse.ArgumentParser(description="Sigrid Jusélius Foundation grant lists -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="parse only the first N PDFs (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/sigrid_juselius"))
    p.add_argument("--cache-dir", type=Path, default=Path("/tmp/sigrid_juselius/cache"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()
    args.cache_dir.mkdir(parents=True, exist_ok=True)

    page = fetch(PAGES["one_to_three"], args.cache_dir)
    lists = pdf_links(page, r"^Awarded grants")
    log(f"1-3-year grant lists: {len(lists)} PDFs")
    if args.limit:
        lists = lists[: args.limit]
    decisions = []
    for href, label in lists:
        path = args.cache_dir / href.rsplit("/", 1)[-1]
        fetch(href, args.cache_dir, binary=True)
        rows = parse_one_to_three(path, href)
        log(f"  {label[:60]:60s} {len(rows):4d} rows, EUR {sum(r['amount'] or 0 for r in rows):,.0f}")
        decisions += rows
    awards = awards_from_chains(chain(decisions), PAGES["one_to_three"])
    log(f"1-3-year grants: {len(decisions)} annual decisions -> {len(awards)} grants "
        f"({sum(not a['chain_complete'] for a in awards)} without their first instalment)")

    # (family, given) pairs from the family-first 1-3-year lists, used to
    # catch name-order switches in the cumulative lists below
    known = {(q["family_name"], q["given_name"]) for r in decisions
             for nm in r["names"] for q in people_from(nm)}

    if not args.limit:
        sp = fetch(PAGES["senior_posts"], args.cache_dir)
        for href, label in pdf_links(sp, r"grant holders"):
            path = args.cache_dir / href.rsplit("/", 1)[-1]
            fetch(href, args.cache_dir, binary=True)
            rows = parse_senior_posts(path, href, PAGES["senior_posts"], known)
            log(f"senior researcher posts: {len(rows)}")
            awards += rows
        lp = fetch(PAGES["large"], args.cache_dir)
        for href, label in pdf_links(lp, r"Large grant holders"):
            path = args.cache_dir / href.rsplit("/", 1)[-1]
            fetch(href, args.cache_dir, binary=True)
            rows = parse_large(path, href, PAGES["large"], known)
            log(f"large grants: {len(rows)}")
            awards += rows

    recs, seen, suffixed = [], set(), 0
    for a in awards:
        lead = a["people"][0] if a["people"] else {}
        fid = f"SJS-{a['start_year']}-{a['scheme_code']}-{slug(lead.get('family_name'))}-{slug(lead.get('given_name'))}"
        # the lists occasionally mis-number an instalment (e.g. 1/3 then 3/3),
        # which leaves a second chain with the same lead/start year: suffix it
        # deterministically (awards are in year/source order)
        base, i = fid, 0
        while fid.lower() in seen:
            i += 1
            fid = f"{base}-{chr(ord('a') + i)}"
        suffixed += i > 0
        seen.add(fid.lower())
        # the lists carry no project titles: compose a descriptive one
        who = " & ".join(" ".join(filter(None, [q.get("given_name"), q.get("family_name")])) for q in a["people"])
        title = f"{SCHEMES[a['scheme_code']]}: {who or 'unnamed'} ({a['start_year']})"
        parts = []
        if a["scheme_code"] in ("SRP", "SCRP", "LG"):
            parts.append(f"Sigrid Jusélius Foundation {SCHEMES[a['scheme_code']].lower()}"
                         + (f", {a['duration_years']} years" if a["duration_years"] else "")
                         + (f", decided {a['first_decision_date']}" if a["first_decision_date"] else "") + ".")
        else:
            inst = json.loads(a["instalments"])
            parts.append("Sigrid Jusélius Foundation 1-3-year grant; annual instalments: " + "; ".join(
                f"{x['year']}" + (f" ({x['k']}/{x['n']})" if x["k"] else "") + (f" EUR {x['amount']:,.0f}" if x["amount"] else "")
                for x in inst) + ".")
            if not a["chain_complete"]:
                parts.append("First instalment not in the published lists; total amount unknown.")
        if a["institution"]:
            parts.append(f"Institution: {a['institution']}.")
        recs.append({
            "title": title,
            "description": " ".join(parts),
            "funder_award_id": fid,
            "scheme_code": a["scheme_code"],
            "funder_scheme": SCHEMES[a["scheme_code"]],
            "start_year": a["start_year"],
            "first_decision_date": a["first_decision_date"],
            "start_date": a["start_date"],
            "end_date": a["end_date"],
            "duration_years": a["duration_years"],
            "amount": a["amount"],
            "currency": "EUR",
            "n_instalments": a["n_instalments"],
            "chain_complete": a["chain_complete"],
            "instalments": a["instalments"],
            "lead_name": lead.get("name"),
            "lead_given_name": lead.get("given_name"),
            "lead_family_name": lead.get("family_name"),
            "lead_title": a["titles"][0] if a["titles"] else None,
            "institution": a["institution"],
            "people": json.dumps(a["people"], ensure_ascii=False),
            "landing_page_url": a["landing_page_url"],
            "source_pdfs": a["source_pdfs"],
        })
    df = pd.DataFrame(recs)
    dup = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dup.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dup, 'funder_award_id'].tolist()[:10]}")

    log(f"{len(df)} awards, {df['start_year'].min()}-{df['start_year'].max()} ({suffixed} ids suffixed after a mis-numbered instalment)")
    log(f"  schemes {df['scheme_code'].value_counts().to_dict()}")
    for c in ["amount", "lead_family_name", "lead_given_name", "institution", "start_date", "end_date"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total EUR {df['amount'].sum():,.0f}")
    top = df.groupby(["lead_given_name", "lead_family_name"]).size().sort_values(ascending=False).head(5)
    log(f"  6.4a top PI combos: {top.to_dict()}")

    df = df.astype("string")
    df["amount"] = pd.to_numeric(df["amount"])
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "sigrid_juselius_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {len(df)} rows to {out}")
    if args.skip_upload:
        return
    if args.limit:
        raise SystemExit("refusing to upload a --limit smoke file to S3")

    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_sigrid_juselius_projects.parquet"
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
