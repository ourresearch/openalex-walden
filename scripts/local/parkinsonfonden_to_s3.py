#!/usr/bin/env python3
"""
Parkinsonfonden (Sweden) to S3 Data Pipeline
=============================================

Parkinsonfonden publishes every grant decision on one WordPress page,
https://www.parkinsonfonden.se/forskning/beviljade-anslag/ :
one block per grant, "<Name>, <Institution>, <amount> kronor" followed by the
project title (project grants) or the trip purpose (travel grants), grouped by
call ("Projektanslag 2025", "Reseanslag 2026 / Juni 2026", older layout
"Beviljade projektanslag juni 2019", back to "Beviljade anslag ... under 2010").

The live page (2026 redesign) only shows 2021-2026. The previous layout of the
same page listed the full history 2010-2024; it is read from the Internet
Archive capture of the fund's own page (web.archive.org/web/20241210014650,
raw id_ capture). Rows present on both are deduplicated on (kind, year,
normalised name, amount); the project-grant counts for the overlap years agree
(2022: 30/30, 2023: 36/36, 2024: 42/41).

funder_award_id: citing works write Parkinsonfonden's application numbers
("682/14", "1115/18", "1443/2022"), which the page prints only for the
October 2014 call ("Nr 682/14, ..."); those rows ship that number. All other
rows get a synthetic key PF-<P|R|A><year>-<family>-<given> (P project, R
travel, A equipment; '-2' for a second distinct grant to the same person in
the same call).

Scope: project grants, equipment grants and travel grants (all research
support for Parkinson's researchers). The Åke Ljungdahl / Elsa och Inge
Andersson prizes live on separate pages and are not included.

Output: s3://openalex-ingest/awards/parkinsonfonden/parkinsonfonden_projects.parquet
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

LIVE_URL = "https://www.parkinsonfonden.se/forskning/beviljade-anslag/"
ARCHIVE_URL = "http://web.archive.org/web/20241210014650id_/http://www.parkinsonfonden.se/forskning/beviljade-anslag/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/parkinsonfonden/parkinsonfonden_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
RETRIES = 4

MONTHS = "januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december"
KIND_CODE = {"project": "P", "travel": "R", "equipment": "A"}
INST_HEAD_RE = re.compile(
    r"\b(Lunds|Karolinska|Uppsala|Göteborgs?|Linköpings|Umeå|Örebro|Stockholms?|Skånes|Sahlgrenska|Chalmers|"
    r"Chamlers|Kungliga|Högskolan|Region|Luleå|Malmö|Akademiska|NIH|Kristianstad)\b")


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def page_lines(page: str) -> list[str]:
    page = re.sub(r"<script.*?</script>|<style.*?</style>|<head.*?</head>", "", page, flags=re.S)
    page = re.sub(r"(?i)</?(p|h[1-6]|summary|li|div|br|details|tr)[^>]*>", "\n", page)
    text = html.unescape(re.sub(r"<[^>]+>", "", page)).replace("\xa0", " ").replace("​", "")
    return [re.sub(r"\s+", " ", x).strip() for x in text.split("\n") if x.strip()]


GRANT_RE = re.compile(
    r"^(?P<head>.+?)[,.]?\s+(?P<amt>\d{1,3}(?:[ .]\d{3})+|\d+)\s*(?:kronor|kr|SEK)(?=\W|till|för|$)\.?\s*(?P<rest>.*)$")
SECTION_RE = re.compile(
    r"(?i)(projekts?anslag|reseanslag|apparaturanslag|anslag)\b.*?(?:(" + MONTHS + r")\s+)?(\d{4})\s*$")
TRAVEL_NOKR_RE = re.compile(r"^(?P<head>.+?),\s+(?P<amt>\d{1,3}(?:[ .]\d{3})+),\s*(?P<rest>.+)$")
NAME_CITY_RE = re.compile(r"^[A-ZÅÄÖÉ][\w\-]+(?: [A-ZÅÄÖÉ][\w\-]+){1,3}, (Göteborg|Lund|Uppsala|Stockholm|Umeå|Linköping|Malmö|Örebro)$")
STOP_RE = re.compile(r"(?i)^(beviljade anslag$|söka |kontakt$|stöd oss$|snabblänkar|ge en |vill du)")


def classify(kind: str, text: str) -> str:
    if kind in ("project", "travel", "equipment"):
        return kind
    t = text.lower()
    if "apparatur" in t:
        return "equipment"
    if re.search(r"deltagande|konferens|kongress|resa |reseanslag|möte|meeting|congress|symposium", t):
        return "travel"
    return "project"


def parse_page(page: str, source: str) -> list[dict]:
    out, kind, year, month, last = [], None, None, None, None
    for x in page_lines(page):
        x = re.sub(r"(\d) ?[oO]{2}\b", r"\g<1>00", x)  # '7 6oo kronor' typo
        if STOP_RE.match(x):
            last = None
            continue
        m = SECTION_RE.search(x)
        if m and not re.search(r"\d\s*(kronor|kr)\b", x) and len(x) < 100:
            k = m.group(1).lower()
            kind = {"projektanslag": "project", "projektsanslag": "project", "reseanslag": "travel",
                    "apparaturanslag": "equipment"}.get(k, "mixed")
            year, month, last = int(m.group(3)), (m.group(2) or "").lower() or None, None
            continue
        if re.match(r"(?i)^under \d{4} har också reseanslag", x) or re.match(r"(?i)^resor:?$", x):
            kind, last = "travel", None
            continue
        m = re.match(r"(?i)^(" + MONTHS + r")\s+(\d{4})$", x)
        if m:
            month, year, last = m.group(1).lower(), int(m.group(2)), None
            continue
        g = GRANT_RE.match(x) or (kind == "travel" and TRAVEL_NOKR_RE.match(x))
        if not g and kind == "travel" and year and NAME_CITY_RE.match(x):
            # 2010 travel list: names and home city only, no amount or purpose
            last = {"kind_raw": kind, "year": year, "month": month, "head": x, "amount_text": None,
                    "inline": None, "desc": None, "line": x, "source": source}
            out.append(last)
            last = None
            continue
        if g and year:
            last = {"kind_raw": kind, "year": year, "month": month, "head": g.group("head").strip(),
                    "amount_text": g.group("amt"), "inline": g.group("rest").strip() or None,
                    "desc": None, "line": x, "source": source}
            out.append(last)
            continue
        if last is not None and last["desc"] is None and not last["inline"]:
            last["desc"] = x
    return out


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|docent)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) with a leading honorific strip."""
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


def split_head(head: str) -> tuple[str | None, list[str], str | None]:
    """'Nr 682/14, Jan Lexell, Lunds universitet' -> ('682/14', ['Jan Lexell'], 'Lunds universitet')."""
    nr = None
    m = re.match(r"^Nr\s*(\d{2,4}\s*/\s*\d{2,4})\s*[,.]?\s*(.*)$", head)
    if m:
        nr, head = re.sub(r"\s", "", m.group(1)), m.group(2)
    head = head.strip(" ,.")
    if "," in head:
        person, inst = head.split(",", 1)
    elif ". " in head:
        person, inst = head.split(". ", 1)
    else:
        m = INST_HEAD_RE.search(head)
        person, inst = (head[: m.start()], head[m.start():]) if m and m.start() > 0 else (head, None)
    persons = [p.strip(" .") for p in re.split(r"\s+och\s+|\s+&\s+", person) if p.strip(" .")]
    inst = re.sub(r"\s+", " ", inst).strip(" ,.´`'") if inst else None
    return nr, persons, inst or None


def title_from(kind: str, text: str | None) -> str | None:
    if not text:
        return None
    t = re.sub(r"(?i)^(till|för)\s+(apparatur\s+(i|till)\s+)?projektet\s*:?\s*", "", text).strip()
    t = re.sub(r"(?i)^(till|för)\s+apparatur\s*:?\s*", "", t).strip()
    t = re.sub(r"[”“\"]+\s*\.?$", "", t).strip(" ”“\"")  # '"Title".' -> 'Title'
    return t or None


def slug(*parts: str | None) -> str:
    s = "-".join(p for p in parts if p)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def main() -> None:
    p = argparse.ArgumentParser(description="Parkinsonfonden beviljade anslag -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="keep only the first N grants (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache the two HTML pages here")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    pages = {}
    for name, url in (("live", LIVE_URL), ("archive_20241210", ARCHIVE_URL)):
        cache = args.cache_dir / f"{name}.html" if args.cache_dir else None
        if cache and cache.exists():
            pages[name] = (url, cache.read_text())
        else:
            pages[name] = (url, get(url))
            if cache:
                args.cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(pages[name][1])
            time.sleep(3)

    raw = []
    for name, (url, page) in pages.items():
        rows = parse_page(page, name)
        log(f"{name}: {len(rows)} grant lines ({url})")
        raw += rows

    recs = []
    for r in raw:
        nr, persons, inst = split_head(r["head"])
        text = r["inline"] or r["desc"]
        kind = classify(r["kind_raw"], f"{r['line']} {text or ''}")
        given, family = split_name(persons[0]) if persons else (None, None)
        co = split_name(persons[1]) if len(persons) > 1 else (None, None)
        amount = float(re.sub(r"[ .]", "", r["amount_text"])) if r["amount_text"] else None
        recs.append({
            "application_number": nr,
            "kind": kind,
            "call_year": r["year"],
            "call_month": r["month"],
            "lead_name": persons[0] if persons else None,
            "lead_given_name": given,
            "lead_family_name": family,
            "co_lead_given_name": co[0],
            "co_lead_family_name": co[1],
            "institution": inst,
            "amount": amount,
            "currency": "SEK" if amount is not None else None,
            "title": title_from(kind, text) if kind != "travel" else None,
            "purpose": text if kind == "travel" else None,
            "raw_line": r["line"],
            "raw_description": r["desc"],
            "section_kind_raw": r["kind_raw"],
            "source": r["source"],
            "source_url": pages[r["source"]][0],
        })
    df = pd.DataFrame(recs)

    # Exact repeats inside one page (the live page repeats a few travel entries) and the
    # 2021-2024 overlap between the live page and the archived layout: one row per
    # (kind, year, name, amount, purpose/title), preferring the live page.
    df["_name"] = df["lead_name"].map(lambda s: slug(s or ""))
    df["_text"] = (df["title"].fillna(df["purpose"]).fillna("")).map(lambda s: slug(s)[:40])
    df["_src"] = (df["source"] != "live").astype(int)
    df = df.sort_values(["_src"]).reset_index(drop=True)
    before = len(df)
    within = df.duplicated(subset=["source", "kind", "call_year", "call_month", "_name", "amount", "_text"])
    df = df[~within]
    live_keys = set(map(tuple, df.loc[df.source == "live", ["kind", "call_year", "_name", "amount"]].values.tolist()))
    overlap = (df.source != "live") & df[["kind", "call_year", "_name", "amount"]].apply(tuple, axis=1).isin(live_keys)
    df = df[~overlap].reset_index(drop=True)
    log(f"{before} lines -> {len(df)} grants ({int(within.sum())} repeated within a page, "
        f"{int(overlap.sum())} archive rows also on the live page)")

    keys, used = [], {}
    for r in df.itertuples():
        if r.application_number:
            keys.append(r.application_number)
            continue
        k = f"PF-{KIND_CODE[r.kind]}{r.call_year}-{slug(r.lead_family_name, r.lead_given_name) or 'unknown'}"
        used[k] = used.get(k, 0) + 1
        keys.append(k if used[k] == 1 else f"{k}-{used[k]}")
    df["funder_award_id"] = keys
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    df = df.drop(columns=["_name", "_text", "_src"])
    if args.limit:
        df = df.head(args.limit)

    for c in ["title", "purpose", "institution", "lead_family_name", "lead_given_name", "application_number"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  by kind/year: {df.groupby(['kind']).size().to_dict()}  years {df.call_year.min()}-{df.call_year.max()}")
    log(f"  total SEK {df['amount'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "parkinsonfonden_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_parkinsonfonden_projects.parquet"
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
