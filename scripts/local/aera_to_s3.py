#!/usr/bin/env python3
"""
American Educational Research Association (AERA) grants and fellowships to S3
=============================================================================

AERA awards research money through several pathways, all in scope here:

1. AERA(-NSF) Grants Program, Research Grants and Dissertation Grants,
   1991-2013: AERA's own "Funded Research Grants" and "Funded Dissertation
   Grants" pages (static HTML tables exported from AERA's FileMaker database:
   name, affiliation, project title, data set used, project dates).
2. Newer recipients, announced in AERA Highlights e-newsletter items
   (www.aera.net/Newsroom/...): AERA-NSF Dissertation and Research Grantees
   (2020-2026), AERA Minority Dissertation Fellowships in Education Research and
   their Travel Awards (2015-2026), AERA Fellowship Program on the Study of
   Deeper Learning cohorts (2019-2025). (The AERA-SRCD Early Career Fellows item
   names no fellows, so that program is not included.)
   The announcement URLs were found through the Internet Archive's CDX index of
   aera.net/Newsroom/* and are fetched live from aera.net; ANNOUNCEMENTS lists them.

robots.txt: aera.net disallows /Portals/ (where the per-grant abstract pages
live) for all agents, so abstracts are NOT fetched; only the abstract link's
record number is read off the allowed listing page. No AI crawler is blocked.

Money: the AERA Grants Program is funded by NSF (DRL-0941014, DRL-1749275 ...)
and AERA makes the awards (citing works write "AERA Grants Program under NSF
Grant #DRL-0941014"); the Deeper Learning fellowships are funded by the Hewlett
Foundation, awarded by AERA. All rows go to AERA as the awarding body.

funder_award_id: the 1991-2013 tables link each abstract as
Abs-RG-00033204.html / Abs-DG-00032997.html: that record number is used
(AERA-RG-00033204); rows without an abstract link and all announcement rows
get a synthetic key AERA-{RG|DG|MDF|MDF-TRAVEL|SDL|SRCD}-{year}-{name-slug}.
No AERA-specific number appears in citing works (they cite the NSF grant).

Output: s3://openalex-ingest/awards/aera/aera_projects.parquet
"""

import argparse
import html
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
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


BASE = "https://www.aera.net"
FO = f"{BASE}/Professional-Opportunities-Funding/AERA-Funding-Opportunities/AERA-NSF-Grants-Program"
TABLE_PAGES = [
    ("RG", f"{FO}/Research-Grants/Funded-Research-Grants"),
    ("DG", f"{FO}/Dissertation-Grants/Funded-Dissertation-Grants"),
]
NR = f"{BASE}/Newsroom/AERA-Highlights-E-newsletter"
# (program, award year, URL). program GRANTS = page lists Dissertation and/or Research grantees by section.
ANNOUNCEMENTS = [
    ("MDF", 2015, f"{NR}/AERA-Highlights-August-2015/AERA-Announces-Recipients-of-2015-2016-Minority-Dissertation-Program"),
    ("MDF", 2016, f"{NR}/AERA-Highlights-June-2016/AERA-Announces-2016-17-Minority-Dissertation-Fellows"),
    ("MDF", 2017, f"{NR}/-em-AERA-Highlights-em-June-2017/AERA-Announces-201718-Minority-Dissertation-Fellows"),
    ("MDF", 2018, f"{NR}/AERA-Highlights-June-2018/AERA-Announces-201819-Minority-Dissertation-Fellows"),
    ("MDF", 2019, f"{NR}/AERA-Highlights-June-2019/AERA-Announces-201920-Minority-Dissertation-Fellows"),
    ("MDF", 2020, f"{NR}/AERA-Highlights-June-2020/AERA-Announces-202021-Minority-Dissertation-Fellows"),
    ("MDF", 2021, f"{NR}/AERA-Highlights-June-2021/AERA-Announces-202122-Minority-Dissertation-Fellows"),
    ("MDF", 2022, f"{NR}/AERA-Highlights-June-2022/AERA-Announces-202223-Minority-Dissertation-Fellows-and-Travel-Awardees"),
    ("MDF", 2023, f"{NR}/AERA-Highlights-June-2023/AERA-Announces-Minority-Dissertation-Fellows-and-Travel-Awardees"),
    ("MDF", 2024, f"{NR}/AERA-Highlights-May-2024/AERA-Announces-Minority-Dissertation-Fellowships-and-Travel-Awards"),
    ("MDF", 2025, f"{NR}/AERA-Highlights-July-2025/AERA-Announces-202526-Minority-Dissertation-Fellowships-and-Travel-Awards"),
    ("MDF", 2026, f"{NR}/AERA-Highlights-June-2026/AERA-Announces-202627-Minority-Dissertation-Fellowships-and-Travel-Awards"),
    ("GRANTS", 2020, f"{NR}/AERA-Highlights-January-2021/AERA-Announces-2020-Recipients-of-Dissertation-and-Research-Grant-Awards"),
    ("GRANTS", 2021, f"{NR}/AERA-Highlights-July-2021/AERA-Announces-Recipients-of-Dissertation-and-Research-Grant-Awards"),
    ("GRANTS", 2022, f"{NR}/AERA-Highlights-February-2022/AERA-Announces-Dissertation-and-Research-Grant-Awardees"),
    ("GRANTS", 2022, f"{NR}/AERA-Highlights-October-2022/AERA-Announces-Recipients-of-Dissertation-and-Research-Grant-Awards"),
    ("GRANTS", 2023, f"{NR}/AERA-Highlights-March-2023/AERA-Names-Recipients-of-Dissertation-and-Research-Grant-Awards"),
    ("GRANTS", 2023, f"{NR}/AERA-Highlights-November-2023/AERA-Announces-Recipients-of-Dissertation-and-Research-Grant-Awards"),
    ("GRANTS", 2024, f"{NR}/AERA-Highlights-September-2024/AERA-Announces-Dissertation-and-Research-Grantees"),
    ("GRANTS", 2026, f"{NR}/AERA-Highlights-March-2026/AERANSF-Grants-Program-Announces-New-Award-Recipients"),
    ("SDL", 2019, f"{NR}/AERA-Highlights-December-2019/AERA-Announces-New-Cohort-of-Deeper-Learning-Fellows"),
    ("SDL", 2020, f"{NR}/AERA-Highlights-November-2020/New-Cohort-of-AERA-Deeper-Learning-Fellows-Selected"),
    ("SDL", 2021, f"{NR}/AERA-Highlights-December-2021/AERA-Announces-Fourth-Cohort-of-Deeper-Learning-Fellows"),
    ("SDL", 2024, f"{NR}/AERA-Highlights-January-2024/AERA-Announces-New-Cohort-of-Deeper-Learning-Fellows"),
    ("SDL", 2025, f"{NR}/AERA-Highlights-January-2025/AERA-Awards-Five-New-Deeper-Learning-Fellowships"),
]
SCHEMES = {
    "RG": "AERA Grants Program: Research Grant", "DG": "AERA Grants Program: Dissertation Grant",
    "MDF": "AERA Minority Dissertation Fellowship in Education Research",
    "MDF-TRAVEL": "AERA Minority Dissertation Fellowship Program: Travel Award",
    "SDL": "AERA Fellowship Program on the Study of Deeper Learning",
    "SRCD": "AERA-SRCD Early Career Fellowship in Early Childhood Education and Development",
}
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/aera/aera_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.5
RETRIES = 3
INST_RE = re.compile(r"Universit|College|Institut|School|Foundation|RAND|Center|Centre|SUNY|CUNY|Academy|"
                     r"Laborator|Corporation|Research|Polytechnic|Department|Teachers|Seminary|Hospital|Inc\b|"
                     r"Council|Organization|Board|District|Bureau|Agency|Commission", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def cached(cache_dir: Path | None, url: str) -> str:
    import hashlib
    name = hashlib.md5(url.encode()).hexdigest()[:10] + "_" + re.sub(r"[^A-Za-z0-9]+", "_", url.rsplit("/", 1)[-1])[:40] + ".html"
    path = cache_dir / name if cache_dir else None
    if path and path.exists():
        return path.read_text()
    body = get(url)
    if path:
        cache_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    time.sleep(REQUEST_DELAY)
    return body


def tidy(s: str | None) -> str | None:
    if not s:
        return None
    s = html.unescape(s).replace("\xa0", " ").replace("​", "").replace("‐", "-")
    s = re.sub(r"\s+", " ", s).strip(" |;,")
    return s or None


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical wolf_to_s3.py helper (runbook §2.4.1), plus surname particles
    ('de Novais', 'Van Allen') kept with the family name."""
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
    given, family = " ".join(tokens[:-1]), tokens[-1]
    g = given.split()
    while len(g) > 1 and g[-1].lower() in {"de", "del", "della", "di", "da", "van", "von", "der", "la", "le", "dos"}:
        family = g.pop() + " " + family
    return " ".join(g), family


def content_block(page: str) -> str:
    m = re.search(r'id="dnn_ctr\d+_HtmlModule_lblContent"(.*?)<!-- End_Module', page, re.S)
    return m.group(1) if m else ""


def cell_lines(cell: str) -> list[str]:
    c = re.sub(r"<br\s*/?>|</p>|</div>|</li>", "\n", cell, flags=re.I)
    c = re.sub(r"<[^>]+>", " ", c)
    out = []
    for x in html.unescape(c).replace("\xa0", " ").replace("​", "").split("\n"):
        x = re.sub(r"\s+", " ", x).strip()
        if x and x not in ("View Full Abstract", "Abstract"):
            out.append(x)
    return out


def parse_dates(s: str | None) -> tuple[str | None, str | None]:
    """'6/1/04-9/30/06' -> ('2004-06-01', '2006-09-30')."""
    out = []
    for m, d, y in re.findall(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", s or ""):
        y = int(y)
        y = y + (2000 if y < 50 else 1900) if y < 100 else y
        try:
            out.append(datetime(y, int(m), int(d)).strftime("%Y-%m-%d"))
        except ValueError:
            out.append(None)
    return (out[0] if out else None), (out[1] if len(out) > 1 else None)


def parse_table_page(kind: str, url: str, page: str) -> list[dict]:
    m = re.search(r"DATA EXPORTED FROM FILEMAKER STARTS HERE(.*?)</table>", page, re.S)
    if not m:
        raise SystemExit(f"{url}: FileMaker table marker not found (layout changed?)")
    recs = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", m.group(1), re.S):
        tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
        if len(tds) != 4:
            continue
        who = cell_lines(tds[0])
        if len(who) == 1 and "|" in who[0]:
            who = [x.strip() for x in who[0].split("|")]
        name = tidy(who[0]) if who else None
        aff = tidy(" ".join(who[1:])) if len(who) > 1 else None
        title = tidy(re.sub(r"\s*Abstract\s*$", "", " ".join(cell_lines(tds[1]))))
        datasets = tidy("; ".join(cell_lines(tds[2])))
        start, end = parse_dates(" ".join(cell_lines(tds[3])))
        ab = re.search(r"Abs-([A-Z]{2})-(\d{6,10})", tr)
        recs.append({"program": kind, "name": name, "institution": aff, "title": title, "datasets": datasets,
                     "start_date": start, "end_date": end, "dates_raw": tidy(" ".join(cell_lines(tds[3]))),
                     "abstract_id": f"{ab.group(1)}-{ab.group(2)}" if ab else None, "source_url": url})
    return recs


HEADER_CELLS = re.compile(r"^(Name|Recipients?|Award Recipient|Institution|Doctoral Institution|Project Title|"
                          r"Dissertation Title|Title|Fellow|Name/Institution)$", re.I)


def section_of(text: str, current: str | None) -> str | None:
    t = text.lower()
    if "travel" in t and "fellow" in t:
        return "fellow"  # 'Minority Dissertation Fellows and Travel Awardees' table title: '*' marks travel
    if "travel" in t:
        return "travel"
    if "dissertation fellow" in t:
        return "fellow"
    granty = re.search(r"grant|awardee|recipient", t)
    if "dissertation" in t and "research" in t:
        return current  # 'Dissertation and Research Grants' heading: rows below say which
    if "dissertation" in t and granty:
        return "DG"
    if "research" in t and granty:
        return "RG"
    return current


def split_record(lines: list[str], cells: list[list[str]]) -> tuple[str | None, str | None, str | None]:
    """(name, institution, title) from one table row."""
    if len(cells) >= 3:
        return " ".join(cells[0]), " ".join(cells[1]), " ".join(cells[2])
    if len(cells) == 2:
        c0, c1 = cells
        if len(c0) >= 2:
            return c0[0] if not c0[0].endswith("&") else " ".join(c0[:2]), " ".join(c0[1:] if not c0[0].endswith("&") else c0[2:]), " ".join(c1)
        if len(c0) == 1 and "," in c0[0]:
            n, i = c0[0].split(",", 1)
            return n, i, " ".join(c1)
        return c0[0] if c0 else None, None, " ".join(c1)
    if len(lines) >= 3:
        return lines[0], lines[1], " ".join(lines[2:])
    if len(lines) == 2 and "," in lines[0]:
        n, i = lines[0].split(",", 1)
        return n, i, lines[1]
    return None, None, None


def parse_announcement(program: str, year: int, url: str, page: str) -> list[dict]:
    b = content_block(page)
    recs = []
    # Walk table rows and the short headings between tables in document order; a
    # heading or a one-cell row ('Research Grantees', 'Travel Awardees') sets the section.
    section, pre = None, ""
    tokens = re.finditer(r"<tr[^>]*>(.*?)</tr>|<(h\d|p|strong)[^>]*>(.*?)</\2>", b, re.S)
    for tok in tokens:
        if tok.group(1) is None:
            txt = tidy(re.sub(r"<[^>]+>", " ", tok.group(3) or "")) or ""
            if txt and len(txt) < 80:
                section = section_of(txt, section)
                pre = txt
            continue
        tr = tok.group(1)
        if True:
            cells = [c for c in (cell_lines(x) for x in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.S)) if c]
            lines = [l for c in cells for l in c]
            if not lines:
                continue
            if all(HEADER_CELLS.match(l) for l in lines):
                continue
            if len(lines) == 1 or (len(lines) <= 2 and re.search(r"Fellows|Grantees|Awardees|Cohort|Program|Recently Awarded", " ".join(lines))):
                section = section_of(" ".join(lines), section)
                continue
            # one row can hold two people side by side (two cells, each name/institution/title)
            groups = [[c] for c in cells] if (len(cells) == 2 and all(len(c) >= 3 for c in cells)) else [cells]
            for g in groups:
                gl = [l for c in g for l in c]
                name, inst, title = split_record(gl, g if len(g) > 1 else [])
                if not name or not title:
                    continue
                travel = "*" in name or section == "travel"
                name = re.sub(r"[*†]", "", name)
                names = [n for n in re.split(r"\s*&\s*|\s+and\s+", name) if n.strip()]
                kind = program
                if program == "MDF":
                    kind = "MDF-TRAVEL" if travel else "MDF"
                elif program == "GRANTS":
                    kind = section if section in ("DG", "RG") else ("DG" if "dissertation" in pre.lower() else None)
                    if kind is None:
                        raise SystemExit(f"{url}: cannot tell dissertation vs research grantee for {name!r}")
                note = None
                m = re.search(r"\(\s*(Joint [^)]*)\)", title)
                if m:
                    note = tidy(m.group(1))
                    title = title.replace(m.group(0), " ")
                title = tidy(title).strip('"“”') if title else None
                recs.append({"program": kind, "name": tidy(names[0]), "co_name": tidy(names[1]) if len(names) > 1 else None,
                             "institution": tidy(inst), "title": tidy(title), "year": year, "note": note,
                             "source_url": url})
    return recs


def main() -> None:
    p = argparse.ArgumentParser(description="AERA grants and fellowships -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only N rows per source page (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache pages here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    rows = []
    for kind, url in TABLE_PAGES:
        recs = parse_table_page(kind, url, cached(args.cache_dir, url))
        if len(recs) < 200:
            raise SystemExit(f"{url}: only {len(recs)} rows (expected 249 / 267); layout changed?")
        log(f"{kind} table: {len(recs)} rows")
        rows += recs[: args.limit] if args.limit else recs
    for program, year, url in ANNOUNCEMENTS:
        recs = parse_announcement(program, year, url, cached(args.cache_dir, url))
        if not recs:
            raise SystemExit(f"{url}: no recipients parsed")
        log(f"{program} {year}: {len(recs)} rows  ({url.rsplit('/', 1)[-1][:70]})")
        rows += recs[: args.limit] if args.limit else recs

    df = pd.DataFrame(rows)
    df["year"] = df["year"].where(df["year"].notna(), df["start_date"].str[:4].astype("float"))
    names = [split_name(n or "") for n in df["name"]]
    df["lead_given_name"] = [g for g, _ in names]
    df["lead_family_name"] = [f for _, f in names]
    co = [split_name(n or "") for n in df.get("co_name", pd.Series([None] * len(df))).fillna("")]
    df["co_given_name"] = [g for g, _ in co]
    df["co_family_name"] = [f for _, f in co]
    # the same announcement can sit under two URLs, and a grantee can appear in two items: one row each
    df["dedup"] = [f"{p}|{int(y) if y == y else ''}|{slug(n or '')}|{slug(t or '')[:40]}"
                   for p, y, n, t in zip(df["program"], df["year"], df["name"], df["title"])]
    before = len(df)
    df = df.drop_duplicates("dedup").drop(columns=["dedup"])
    log(f"dropped {before - len(df)} duplicate announcement rows")

    def key(r):
        if isinstance(r.get("abstract_id"), str) and r["abstract_id"]:
            return f"AERA-{r['abstract_id']}"
        y = int(r["year"]) if r["year"] == r["year"] and r["year"] is not None else "nd"
        return f"AERA-{r['program']}-{y}-{slug(r['name'] or '')}"
    df["funder_award_id"] = [key(r) for r in df.to_dict("records")]
    dup = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dup.any():
        df.loc[dup, "funder_award_id"] = df.loc[dup, "funder_award_id"] + "-" + \
            df.loc[dup, "title"].fillna("").map(lambda t: slug(t)[:20])
    if df["funder_award_id"].str.lower().duplicated().any():
        d = df[df["funder_award_id"].str.lower().duplicated(keep=False)]
        raise SystemExit(f"duplicate funder_award_id after disambiguation:\n"
                         f"{d[['funder_award_id', 'name', 'title', 'source_url']].to_string()}")
    df["funder_scheme"] = df["program"].map(SCHEMES)
    df["funding_type"] = df["program"].map({"RG": "research", "DG": "fellowship", "MDF": "fellowship",
                                            "MDF-TRAVEL": "travel", "SDL": "fellowship", "SRCD": "fellowship"})
    df["year"] = df["year"].map(lambda y: str(int(y)) if y == y and y is not None else None)

    log(f"rows: {len(df)} by program {df['program'].value_counts().to_dict()}")
    for c in ["title", "lead_family_name", "institution", "start_date", "year"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "aera_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_aera_projects.parquet"
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
