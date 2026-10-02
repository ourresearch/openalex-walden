#!/usr/bin/env python3
"""
Foundation for Polish Science (FNP, Fundacja na rzecz Nauki Polskiej) to S3
===========================================================================

FNP publishes its laureates on one server-rendered page,
https://www.fnp.org.pl/en/component/fnp_pages/page/about-laureates, as two
HTML tables (client-side DataTables over the full data; no export/API exists):

1. "Laureates of FNP programmes financed by internal funds of the Foundation"
   (Year, Program name, Name, Institution, Area): START stipends 1993-2026,
   the FNP Prize 1992-2025, FOR UKRAINE, Copernicus / Curie / Poland-U.S.
   awards, Kolakowski fellowship. Names are "Surname Given".
2. "Research projects implemented under the FENG" (Year, Programme, Call,
   Project name, Head researcher, Beneficiary, Funding in PLN): FIRST TEAM,
   TEAM NET, MAB (IRAP), PoC and PRIME FENG projects 2023-2026.

FNP publishes no citable grant number for these awards (START agreements are
cited as e.g. "START 38.2016", which the site does not show), so
funder_award_id is a synthetic key "FNP-{programme}-{laureate page id}" or,
for rows without a detail page, "FNP-{programme}-{year}-{SURNAME}-{GIVEN}".

NOT covered: the 2007-2013 / 2014-2020 EU-funded programmes (TEAM, HOMING,
POWROTY, MAB, TEAM-TECH, TEAM NET under POIG/POIR). FNP no longer lists their
laureates on its site (archiwalna.fnp.org.pl has only competition-document PDFs).

Output: s3://openalex-ingest/awards/fnp/fnp_projects.parquet
"""

import argparse
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (runbook §1.2 item 7; equivalent to sys.stdout.reconfigure(encoding="utf-8") + open() default utf-8)
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
URL = "https://www.fnp.org.pl/en/component/fnp_pages/page/about-laureates"
BASE = "https://www.fnp.org.pl"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fnp/fnp_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
RETRIES = 3

# programme name on the page -> (code used in funder_award_id, funding_type)
PROGRAMMES = {
    "FNP START": ("START", "fellowship"),
    "THE FNP PRIZE": ("PRIZE", "prize"),
    "FOR UKRAINE Programme": ("UKRAINE", "research"),
    "Nicolaus Copernicus Polish-German Research Award": ("COPERNICUS", "prize"),
    "Marie Skłodowska-Curie and Pierre Curie – Polish – French Science Award": ("CURIE", "prize"),
    "The Leszek Kołakowski Honorary Fellowship": ("KOLAKOWSKI", "fellowship"),
    "The Poland – U.S. Science Award": ("PL-US", "prize"),
    "FIRST TEAM FENG": ("FIRST-TEAM-FENG", "research"),
    "Proof of Concept (PoC FENG)": ("POC-FENG", "research"),
    "International Research Agendas Programme FENG": ("MAB-FENG", "research"),
    "PRIME Project – Science Commercialisation Support": ("PRIME-FENG", "research"),
    "TEAM NET FENG": ("TEAM-NET-FENG", "research"),
}

TITLE_RE = re.compile(
    r"\b(?:prof|dr|hab|inż|n\. med|med|phd|ph\.d|d\.sc|dsc|eng|m\.sc|msc|pharm\.d|habil|mgr|uczelni|uj|"
    r"university professor|n\. o zdr)\b\.?", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def parse_tables(page: str) -> list[list[dict]]:
    """Each <table> -> list of {header: text} rows, plus 'href' of the first link in the row."""
    tables = []
    for tb in re.findall(r"<table.*?</table>", page, re.S):
        trs = re.findall(r"<tr.*?</tr>", tb, re.S)
        if not trs:
            continue
        head = [text(c) for c in re.findall(r"<th[^>]*>(.*?)</th>", trs[0], re.S)]
        rows = []
        for tr in trs[1:]:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
            if len(cells) != len(head):
                continue
            row = {h: text(c) for h, c in zip(head, cells)}
            # two-laureate awards put one person / institution per inner <div>
            for h, c in zip(head, cells):
                row[h + "__list"] = [x for x in (text(d) for d in re.findall(r"<div>(.*?)</div>", c, re.S)) if x]
            href = re.search(r'href="([^"]+/laureaci/(\d+))"', tr)
            row["_href"] = BASE + href.group(1) if href else None
            row["_laureate_id"] = href.group(2) if href else None
            rows.append(row)
        tables.append(rows)
    return tables


def split_people(s: str | None) -> list[str]:
    """Prize rows can hold two laureates in one cell:
    'Gryko Daniel, Prof. Jacquemin Denis, Prof.' -> ['Gryko Daniel', 'Jacquemin Denis']."""
    if not s:
        return []
    titles = r"(?:(?:Prof|prof|PhD|Ph\.D|Dr|dr|hab|UJ|D\.Sc|DSc|Eng|inż|n\. med|med|i n\. o zdr|uczelni)\.?[\s,]*)+"
    found = re.findall(r"([A-ZĄĆĘŁŃÓŚŹŻ][^,]*?)\s*,\s*" + titles, s)
    out = [f.strip() for f in found if f.strip()]
    return out or [s.split(",")[0].strip()]


def clean_name(s: str) -> str:
    s = s.split(",")[0]
    s = TITLE_RE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip(" .,")


def split_polish(name: str, given_names: set[str], surname_first: bool) -> tuple[str | None, str | None]:
    """FNP lists are 'Surname Given [Middle]' (laureate table) but the FENG table mixes
    'Given Surname' and 'Surname Given'. Order is decided with a set of given names
    learned from the laureate table's (consistent) surname-first rows. No degree
    suffixes survive clean_name (runbook §2.4.1 suffix set is covered by TITLE_RE)."""
    toks = clean_name(name).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while toks and toks[-1].lower().strip(",.") in suffixes:
        toks.pop()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    first_g, last_g = toks[0] in given_names, toks[-1] in given_names
    if first_g and not last_g:
        given_first = True
    elif last_g and not first_g:
        given_first = False
    else:
        given_first = not surname_first
    if given_first:
        return " ".join(toks[:-1]), toks[-1]
    # 'Surname Given [Middle]' -> family = first token (Polish two-part surnames are hyphenated)
    return " ".join(toks[1:]), toks[0]


def parse_amount(s: str | None) -> float | None:
    if not s:
        return None
    t = re.sub(r"[^\d,.]", "", s)
    if not t:
        return None
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    elif re.search(r"\.\d{3}(\.|$)", t):
        t = t.replace(".", "")
    try:
        return float(t)
    except ValueError:
        return None


def slug(s: str) -> str:
    import unicodedata
    s = unicodedata.normalize("NFKD", s.replace("ł", "l").replace("Ł", "L"))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^A-Za-z0-9]+", "-", s).strip("-").upper()


def main() -> None:
    p = argparse.ArgumentParser(description="FNP laureates + FENG projects -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="keep only the first N rows of each table")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None)
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    cache = args.cache_dir / "about-laureates.html" if args.cache_dir else None
    if cache and cache.exists():
        page = cache.read_text()
    else:
        log(f"GET {URL}")
        page = get(URL)
        if cache:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache.write_text(page)
    log(f"  {len(page):,} bytes")
    tables = parse_tables(page)
    laureates = next((t for t in tables if t and "Program name" in t[0]), None)
    feng = next((t for t in tables if t and "Project name" in t[0]), None)
    if laureates is None or feng is None:
        raise RuntimeError(f"expected the laureate and FENG tables; found {[list(t[0]) for t in tables if t]}")
    log(f"Laureate table: {len(laureates)} rows; FENG project table: {len(feng)} rows")
    if len(laureates) < 3000 or len(feng) < 100:
        raise RuntimeError("tables smaller than expected (3,868 / 149 on 2026-09-30); refusing to continue")

    # given-name vocabulary from the surname-first laureate table ('Kowalski Jan [Maria]')
    given_names: dict[str, int] = {}
    for r in laureates:
        for nm in split_people(r.get("Name")):
            toks = clean_name(nm).split()
            if len(toks) >= 2:
                given_names[toks[-1]] = given_names.get(toks[-1], 0) + 2  # last token: a given name
            for t in toks[1:-1]:
                given_names[t] = given_names.get(t, 0) + 1  # middle tokens: given or 2nd surname
    given_set = {k for k, v in given_names.items() if v >= 2}
    log(f"  learned {len(given_set)} given names")

    if args.limit:
        laureates, feng = laureates[: args.limit], feng[: args.limit]

    rows = []
    for r in laureates:
        prog = r["Program name"]
        code, ftype = PROGRAMMES.get(prog, (slug(prog), "research"))
        names = [n for x in r["Name__list"] for n in split_people(x)] or split_people(r.get("Name"))
        insts = r["Institution__list"] if len(r["Institution__list"]) == len(names) else [r.get("Institution")] * len(names)
        people = []
        for nm, inst in zip(names, insts):
            g, f = split_polish(nm, given_set, surname_first=True)
            people.append({"name": clean_name(nm), "given_name": g, "family_name": f, "institution": inst})
        lead = people[0] if people else {}
        key = r["_laureate_id"] or f"{r['Year']}-{slug(lead.get('family_name') or '')}-{slug(lead.get('given_name') or '')}"
        rows.append({
            "source_table": "laureates_internal_funds",
            "funder_award_id": f"FNP-{code}-{key}",
            "programme": prog, "programme_code": code, "funding_type": ftype,
            "year": r["Year"], "call": None,
            "title": None,
            "name_raw": r.get("Name"),
            "lead_given_name": lead.get("given_name"), "lead_family_name": lead.get("family_name"),
            "lead_institution": lead.get("institution") or r.get("Institution"),
            "people_json": json.dumps(people, ensure_ascii=False) if people else None,
            "research_area": r.get("Area"),
            "amount_text": None, "amount": None, "currency": None,
            "laureate_id": r["_laureate_id"], "landing_page_url": r["_href"] or URL,
        })
    # FENG name order is consistent within a (programme, year) block but differs between
    # blocks; decide each block's default by majority of its unambiguous names.
    votes: dict[tuple, list[int]] = {}
    for r in feng:
        toks = clean_name(r.get("Head Researcher/ Scientific Leader PRIME") or "").split()
        if len(toks) >= 2:
            a, b = toks[0] in given_set, toks[-1] in given_set
            if a != b:
                v = votes.setdefault((r["Programme name"], r["Year"]), [0, 0])
                v[0 if a else 1] += 1
    for r in feng:
        prog = r["Programme name"]
        code, ftype = PROGRAMMES.get(prog, (slug(prog), "research"))
        nm = r.get("Head Researcher/ Scientific Leader PRIME")
        gf, sf = votes.get((prog, r["Year"]), [1, 0])
        g, f = split_polish(nm, given_set, surname_first=sf > gf) if nm else (None, None)
        people = [{"name": clean_name(nm), "given_name": g, "family_name": f,
                   "institution": r.get("Beneficiery/ PRIME Grantee")}] if nm else []
        amt = parse_amount(r.get("Funding"))
        key = r["_laureate_id"] or f"{r['Year']}-{slug(f or '')}-{slug(r.get('Project name') or '')[:40]}"
        rows.append({
            "source_table": "feng_projects",
            "funder_award_id": f"FNP-{code}-{key}",
            "programme": prog, "programme_code": code, "funding_type": ftype,
            "year": r["Year"], "call": r.get("Call"),
            "title": r.get("Project name"),
            "name_raw": nm,
            "lead_given_name": g, "lead_family_name": f,
            "lead_institution": r.get("Beneficiery/ PRIME Grantee"),
            "people_json": json.dumps(people, ensure_ascii=False) if people else None,
            "research_area": None,
            "amount_text": r.get("Funding"), "amount": amt, "currency": "PLN" if amt else None,
            "laureate_id": r["_laureate_id"], "landing_page_url": r["_href"] or URL,
        })

    df = pd.DataFrame(rows)
    # Homonym laureates in the same year/programme (e.g. two "Nowak Mikolaj" START 2023,
    # different institutions): disambiguate name-based keys with the institution.
    homonym = df["funder_award_id"].str.lower().duplicated(keep=False) & df["laureate_id"].isna()
    df.loc[homonym, "funder_award_id"] = [
        f"{k}-{slug(i or '')[:30]}" for k, i in zip(df.loc[homonym, "funder_award_id"], df.loc[homonym, "lead_institution"])]
    log(f"  {int(homonym.sum())} homonym keys disambiguated by institution")
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")
    log(f"{len(df)} awards; by programme:")
    for k, v in df["programme"].value_counts().items():
        log(f"  {v:5d} {k}")
    for c in ["title", "lead_family_name", "lead_given_name", "lead_institution", "amount"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "fnp_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_fnp_projects.parquet"
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
