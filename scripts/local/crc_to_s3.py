#!/usr/bin/env python3
"""
Canada Research Chairs (CRC) + Canada 150 Research Chairs to S3 Data Pipeline
=============================================================================

The Canada Research Chairs Program (Tri-agency Institutional Programs
Secretariat, chairs-chaires.gc.ca) publishes:

1. Recipients lists, one per allocation announcement
   (/media-medias/lists-listes/{year}/{month-mois}-eng.aspx, 2015-04 onwards;
   e.g. "Spring 2026 Recipients (cycle 2025-1)"). One table row per awarded chair:
   Chairholder ("Last, First"), University, Chair title, Agency, Tier,
   Chair type (New / Renewal). These are award events with an announcement date.
   The site has no index of these lists, so LIST_PATHS below was enumerated by
   probing every {year}/{month|season} URL for 2000-2026 on 2026-09-30; the script
   also probes the current and next year for new lists.

2. The chairholders database (/chairholders-titulaires/index-eng.aspx): every
   CURRENT chair (~1,950): Chairholder, Chair title, Tier, Institution, Agency.
   No dates. Chairs already covered by a recipients-list row (same person and
   tier) are not repeated; the rest (mostly awarded before April 2015) are kept
   as undated awards.

3. Canada 150 Research Chairs laureates (canada150.chairs-chaires.gc.ca, 24
   chairs, one-time 2017-18 program run by the same secretariat): name,
   institution, agency, chair title, description, "Award amount: $N per year
   for seven years". Canada 150 has no OpenAlex funder record, so these rows are
   attributed to CRC (F4320320994) with funder_scheme "Canada 150 Research
   Chairs" and their own key prefix so they can be split out later. NOTE: that
   host's TLS certificate had expired on 2026-09-30, so it is fetched with
   certificate verification disabled (read-only GET of a public page).

Amounts: the program's published per-tier values (About the program page):
Tier 1 = $200,000/yr for 7 years (CAD 1,400,000), Tier 2 = $100,000/yr for 5
years (CAD 500,000; first-term Tier 2 chairs get a further $20,000/yr stipend that
the lists do not flag, so it is not added). Canada 150: the per-chair annual
amount x 7.

funder_award_id: CRC publishes no award numbers, so the key is synthetic and
prefixed so it can never collide with the acknowledgement-derived CRC ids
already in openalex_awards_raw (free text like "Tier1", "CRC-2019-00019",
"950-232424"):
    list rows      CRC-T{tier}-{YYYY}-{MM}-{name-slug}
    database-only  CRC-T{tier}-{name-slug}
    Canada 150     C150-{name-slug}

Output: s3://openalex-ingest/awards/crc/crc_chairs.parquet
"""

import argparse
import html
import re
import time
import unicodedata
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
import urllib3

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

BASE = "https://www.chairs-chaires.gc.ca"
DB_URL = BASE + "/chairholders-titulaires/index-eng.aspx"
LIST_URL = BASE + "/media-medias/lists-listes/{path}-eng.aspx"
C150_URL = "https://canada150.chairs-chaires.gc.ca/chairholders-titulaires/index-eng.aspx"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/crc/crc_chairs.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 4

# Enumerated 2026-09-30 by probing {2000..2026}/{month|season} (see docstring).
LIST_PATHS = [
    "2015/april-avril", "2016/february-fevrier", "2016/december-decembre",
    "2017/may-mai", "2017/october-octobre", "2018/may-mai", "2018/october-octobre",
    "2019/april-avril", "2019/october-octobre", "2020/april-avril", "2020/october-octobre",
    "2021/june-juin", "2022/january-janvier", "2022/may-mai", "2022/november-novembre",
    "2023/june-juin", "2024/winter-hiver", "2024/spring-printemps", "2024/fall-automne",
    "2025/spring-printemps", "2026/spring-printemps",
]
PERIODS = {
    "january-janvier": 1, "february-fevrier": 2, "march-mars": 3, "april-avril": 4,
    "may-mai": 5, "june-juin": 6, "july-juillet": 7, "august-aout": 8,
    "september-septembre": 9, "october-octobre": 10, "november-novembre": 11,
    "december-decembre": 12,
    # season lists: month of the announcement season
    "winter-hiver": 2, "spring-printemps": 5, "summer-ete": 7, "fall-automne": 10, "autumn-automne": 10,
}
TIER_AMOUNT = {"1": 1_400_000.0, "2": 500_000.0}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, verify: bool = True, method: str = "GET", **kw) -> requests.Response | None:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.request(method, url, headers=HEADERS, timeout=120, verify=verify, **kw)
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                r.encoding = "utf-8"
                time.sleep(REQUEST_DELAY)
                return r
            last_err = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{method} {url} failed after {RETRIES} tries: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>|</p>", "\n", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"[ \t\r\f\v]+", " ", t)
    t = re.sub(r"\s*\n\s*", "\n", t).strip()
    return t or None


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", s).strip()


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", fold(s)).strip("-")


def split_last_first(name: str) -> tuple[str | None, str | None]:
    """'Abdul-Mageed,  Muhammad' -> ('Muhammad', 'Abdul-Mageed'). Names without a comma
    fall back to the canonical last-token rule (runbook §2.4.1)."""
    if not name:
        return None, None
    name = re.sub(r"\s+", " ", name).strip().strip(",")
    if "," in name:
        family, given = [x.strip() for x in name.split(",", 1)]
        given = re.sub(r"^(?:dr|prof)\.?\s+", "", given, flags=re.I)
        return (given or None), (family or None)
    tokens = re.sub(r"^(?:dr|prof)\.?\s+", "", name, flags=re.I).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def table_rows(page: str, table_id: str | None = None) -> tuple[list[str], list[list[str]]]:
    if table_id:
        m = re.search(rf'<table[^>]*id="{table_id}".*?</table>', page, re.S)
    else:
        m = max(re.finditer(r"<table.*?</table>", page, re.S), key=lambda x: x.group(0).count("<tr"), default=None)
    if not m:
        return [], []
    t = m.group(0)
    heads = [text(h) or "" for h in re.findall(r"<th[^>]*>(.*?)</th>", t, re.S)]
    rows = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", t, re.S):
        cells = [text(c) or "" for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        if cells:
            rows.append(cells)
    return heads, rows


def col(heads: list[str], *names: str) -> int:
    hs = [fold(h) for h in heads]
    for n in names:  # exact header match first ("chairholder" vs "chairholder title")
        if n in hs:
            return hs.index(n)
    for n in names:
        for i, h in enumerate(hs):
            if h.startswith(n):
                return i
    raise SystemExit(f"column {names} not in {heads}")


def discover_lists() -> list[str]:
    """Known lists + probe the latest known year onward for newly published ones."""
    paths = list(LIST_PATHS)
    last_year = max(int(p.split("/")[0]) for p in paths)
    for y in range(last_year, date.today().year + 2):
        for per in PERIODS:
            p = f"{y}/{per}"
            if p in paths:
                continue
            if get(LIST_URL.format(path=p)) is not None:
                log(f"  new recipients list found: {p}")
                paths.append(p)
    return sorted(paths, key=lambda p: (int(p.split("/")[0]), PERIODS[p.split("/")[1]]))


def parse_list(path: str, page: str) -> list[dict]:
    heads, rows = table_rows(page)
    iname = col(heads, "chairholder", "applicant", "nominee")
    iuni = col(heads, "university", "institution")
    ititle = col(heads, "chair title", "chairholder title", "chair")
    iag = col(heads, "agency")
    itier = col(heads, "tier")
    itype = col(heads, "chair type", "type")
    title = re.search(r"<title>(.*?)</title>", page, re.S)
    cycle = re.search(r"cycle\s+(\d{4}-\d)", title.group(1) if title else "")
    year, per = path.split("/")
    out = []
    for r in rows:
        if len(r) <= max(iname, iuni, ititle, iag, itier, itype):
            continue
        tier = re.sub(r"\D", "", r[itier])
        out.append({
            "source": "recipients_list",
            "list_path": path,
            "list_url": LIST_URL.format(path=path),
            "list_title": text(title.group(1)) if title else None,
            "cycle": cycle.group(1) if cycle else None,
            "announcement_date": f"{year}-{PERIODS[per]:02d}-01",
            "announcement_year": year,
            "chairholder": r[iname],
            "institution": r[iuni],
            "chair_title": r[ititle],
            "agency": r[iag],
            "tier": tier or None,
            "chair_type": r[itype],
        })
    return out


def parse_database(page: str) -> list[dict]:
    heads, rows = table_rows(page, "gv_results")
    iname, ititle, itier = col(heads, "chairholder"), col(heads, "chairholder title", "chair title"), col(heads, "tier")
    iinst, iag = col(heads, "institution", "university"), col(heads, "agency")
    return [{
        "source": "chairholders_database",
        "chairholder": r[iname], "chair_title": r[ititle], "tier": re.sub(r"\D", "", r[itier]) or None,
        "institution": r[iinst], "agency": r[iag],
    } for r in rows if len(r) > max(iname, ititle, itier, iinst, iag)]


def parse_c150(page: str) -> list[dict]:
    """One <tr> per laureate: <span class="wb-inv">Surname</span>, <p><strong>Full Name</strong><br/>
    Institution | <abbr>AGENCY</abbr></p>, <p class="text-danger"><strong>Chair title</strong></p>,
    description <p>s, <p><strong>Award amount:</strong> $N per year for seven years</p>."""
    out = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.S):
        head = re.search(r"<p>\s*<strong>(.*?)</strong>\s*<br\s*/?>(.*?)</p>", tr, re.S)
        if not head:
            continue
        surname = re.search(r'<span class="wb-inv">(.*?)</span>', tr, re.S)
        inst_ag = text(head.group(2)) or ""
        inst, _, agency = inst_ag.partition("|")
        title = re.search(r'class="text-danger"[^>]*>(.*?)</(?:p|span)>', tr, re.S)
        title_txt = text(title.group(1)) if title else None
        paras = [text(x) for x in re.findall(r"<p[^>]*>(.*?)</p>", tr[head.end():], re.S)]
        desc = [x for x in paras if x and x != title_txt and not x.startswith("Award amount")]
        amt = re.search(r"Award amount:\s*</strong>\s*\$\s*([\d,]+)\s*per(?:\s|&nbsp;)*year\s*for\s*(\w+)", tr.replace("&nbsp;", " "), re.I)
        full = text(head.group(1))
        sn = text(surname.group(1)) if surname else None
        # "Last, First" form so split_last_first uses the site's own surname
        if full and sn and full.endswith(sn):
            name = f"{sn}, {full[: -len(sn)].strip()}"
        else:
            name = full
        out.append({
            "source": "canada150_laureates",
            "chairholder": name,
            "institution": inst.strip() or None,
            "agency": agency.strip() or None,
            "chair_title": title_txt,
            "description": "\n\n".join(desc) or None,
            "tier": None,
            "c150_amount_per_year": amt.group(1).replace(",", "") if amt else None,
            "c150_years": str({"five": 5, "seven": 7}.get(amt.group(2).lower())) if amt else None,
        })
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Canada Research Chairs (+ Canada 150) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only fetch the first N recipients lists")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch(url: str, name: str, verify: bool = True) -> str:
        cache = args.cache_dir / name if args.cache_dir else None
        if cache and cache.exists():
            return cache.read_text()
        r = get(url, verify=verify)
        if r is None:
            raise SystemExit(f"404: {url}")
        if cache:
            cache.write_text(r.text)
        return r.text

    paths = discover_lists() if not args.limit else LIST_PATHS[: args.limit]
    events = []
    for path in paths:
        rows = parse_list(path, fetch(LIST_URL.format(path=path), "list_" + path.replace("/", "_") + ".html"))
        log(f"  list {path}: {len(rows)} chairs")
        if not rows:
            raise SystemExit(f"no rows parsed from recipients list {path}; layout changed?")
        events += rows
    ev = pd.DataFrame(events)
    log(f"Recipients lists: {len(paths)} lists, {len(ev)} chair awards; chair types {ev['chair_type'].value_counts().to_dict()}")

    db = pd.DataFrame(parse_database(fetch(DB_URL, "chairholders_database.html")))
    log(f"Chairholders database: {len(db)} current chairs")
    if len(db) < 1000:
        raise SystemExit("chairholders database parse looks truncated")

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    c150 = pd.DataFrame(parse_c150(fetch(C150_URL, "canada150_laureates.html", verify=False)))
    log(f"Canada 150 laureates: {len(c150)}")
    if not args.limit and len(c150) < 20:
        raise SystemExit("Canada 150 laureates parse looks truncated (expected 24)")

    # database chairs already represented by a list row (same person + tier) are not repeated
    for d in (ev, db, c150):
        # family-then-given, so "Persaud, Navindra" and "Navindra Persaud" get one key
        d["name_key"] = d["chairholder"].fillna("").map(
            lambda s: slug(" ".join(x for x in reversed(split_last_first(s)) if x)))
    seen = set(zip(ev["name_key"], ev["tier"]))
    db["in_lists"] = [(n, t) in seen for n, t in zip(db["name_key"], db["tier"])]
    db_only = db[~db["in_lists"]].copy()
    log(f"  database chairs matched to a list row: {db['in_lists'].sum()}; database-only (pre-2015 or unlisted): {len(db_only)}")

    ev["funder_award_id"] = [f"CRC-T{t or 'X'}-{d[:4]}-{d[5:7]}-{n}" for t, d, n in zip(ev["tier"], ev["announcement_date"], ev["name_key"])]
    db_only["funder_award_id"] = [f"CRC-T{t or 'X'}-{n}" for t, n in zip(db_only["tier"], db_only["name_key"])]
    c150["funder_award_id"] = [f"C150-{n}" for n in c150["name_key"]]
    ev["funder_scheme"] = "Canada Research Chairs"
    db_only["funder_scheme"] = "Canada Research Chairs"
    c150["funder_scheme"] = "Canada 150 Research Chairs"
    ev["landing_page_url"] = ev["list_url"]
    db_only["landing_page_url"] = DB_URL
    c150["landing_page_url"] = C150_URL

    df = pd.concat([ev, db_only, c150], ignore_index=True)
    df = df[df["name_key"] != ""]
    df["currently_active"] = df["name_key"].isin(set(db["name_key"])).map({True: "true", False: "false"})
    tier_amt = df["tier"].map(TIER_AMOUNT)
    c150_amt = pd.to_numeric(df.get("c150_amount_per_year"), errors="coerce") * pd.to_numeric(df.get("c150_years"), errors="coerce")
    df["amount"] = tier_amt.fillna(c150_amt)
    df["amount_basis"] = [
        "program tier value (Tier 1 $200k x 7y / Tier 2 $100k x 5y)" if t in TIER_AMOUNT
        else ("published annual amount x years" if pd.notna(a) else None)
        for t, a in zip(df["tier"], df["amount"])]
    df["currency"] = df["amount"].map(lambda v: "CAD" if pd.notna(v) else None)
    split = df["chairholder"].fillna("").map(split_last_first)
    df["lead_given_name"] = split.map(lambda x: x[0])
    df["lead_family_name"] = split.map(lambda x: x[1])

    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        # same person twice in one list at one tier: keep first, log
        log(f"  dropping {dupes.sum() - df.loc[dupes, 'funder_award_id'].str.lower().nunique()} duplicate keys: "
            f"{df.loc[dupes, 'funder_award_id'].unique().tolist()[:10]}")
        df = df[~df["funder_award_id"].str.lower().duplicated(keep="first")]
    log(f"Total {len(df)} awards: {df['source'].value_counts().to_dict()}")
    for c in ["chair_title", "institution", "tier", "announcement_date", "amount", "lead_family_name"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    df["amount"] = df["amount"].map(lambda v: f"{v:.0f}" if pd.notna(v) else None)
    df = df.drop(columns=["in_lists"], errors="ignore").astype("string")  # runbook §1.2 item 5
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "crc_chairs.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_crc_chairs.parquet"
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
