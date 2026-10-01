#!/usr/bin/env python3
"""
Whitehall Foundation to S3 Data Pipeline
========================================

The Whitehall Foundation (US, basic neurobiology) publishes its grant recipients as a
plain list, one grant per line:

    Surname, Given, Institution[, City], "Project Title." Three Year Research Grant
    totaling $300,000 [awarded Spring 2025].

Two sources, both the foundation's own publication:

1. The live "Active Grants" page (https://whitehall.org/#active-grants, content file
   https://whitehall.org/pages/active-grants.html): every currently active grant with the
   season + year it was awarded.
2. The foundation's earlier "Grant Recipients" pages (whitehall.org/recipien.htm 1998-2001,
   whitehall.org/recipients/ 2002-2026), which listed each year's new grants under a
   "YYYY Grant Recipients" heading. These pages are no longer online, so they are read from
   the Internet Archive's Wayback Machine (every distinct capture, via the CDX API with
   collapse=digest; fetched sequentially with backoff because the Archive throttles).

Grants are deduplicated across captures on (surname, given-name initial, normalised
title); the award year is the page heading year (recipients pages) or the "awarded
Season YYYY" text (active page). Years with no surviving capture are simply missing.

Whitehall publishes no grant numbers (citing works quote "2017-12-98"-style internal
numbers that never appear on the site), so funder_award_id is a synthetic
"WHF-{year}-{surname slug}-{title hash6}" key.

whitehall.org serves no robots.txt (404).

Output: s3://openalex-ingest/awards/whitehall/whitehall_projects.parquet
"""

import argparse
import hashlib
import json
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22; see runbook 1.2) ---
# (renamed-sys variant; the canonical call is sys.stdout.reconfigure(encoding="utf-8"))
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

LIVE_URL = "https://whitehall.org/pages/active-grants.html"
LIVE_LANDING = "https://whitehall.org/#active-grants"
CDX = "https://web.archive.org/cdx/search/cdx"
WAYBACK_PATTERNS = ["whitehall.org/recipien.htm", "whitehall.org/whitehall/recipien.htm",
                    "whitehall.org/whitehall/recipients/", "whitehall.org/recipients/"]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/whitehall/whitehall_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
WAYBACK_DELAY = 4.0
RETRIES = 5

OPEN_Q = "\"“"
CLOSE_Q = "\"”"
AMOUNT_RE = re.compile(r"totaling\s*\$\s*([\d,]+)", re.I)
AWARDED_RE = re.compile(r"awarded\s+(Spring|Summer|Fall|Winter|Autumn)?\s*((?:19|20)\d{2})", re.I)
PROGRAM_RE = re.compile(
    r"((?:One|Two|Three|Four|Five|\d)[- ]Year\s+(?:Research\s+|Renewal\s+)?Grant(?:[- ]in[- ]Aid)?|"
    r"(?:One|Two|Three|\d)[- ]Year\s+Grants?[- ]In[- ]Aid|(?:One|Two|Three|\d)[- ]Year\s+Renewal|"
    r"Grants?[- ]in[- ]Aid|Research\s+Grant|Renewal\s+Grant)", re.I)
HEADING_RE = re.compile(r"^((?:19|20)\d{2})\s+(?:Active\s+)?(?:Grant\s+)?(?:Recipients|Grants|Awards)$", re.I)
INST_RE = re.compile(r"universit|college|institut|school|hospital|cent(er|re)|laborator|foundation|clinic|"
                     r"medical|research|salk|scripps|rockefeller|jackson|harbor|mount sinai|polytechnic", re.I)
HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, params=None, delay_on_fail: float = 10.0) -> requests.Response:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=120)
            if r.status_code in (429, 503, 502, 504):
                raise RuntimeError(f"HTTP {r.status_code}")
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            log(f"  GET {url} attempt {attempt + 1} failed: {e}; backing off")
            time.sleep(delay_on_fail * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook 2.4.1 helper (wolf_to_s3.py). Used only as a fallback: the
    Whitehall lists print 'Surname, Given' so the family name is the first field."""
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


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def slug(s: str) -> str:
    return norm(s).replace(" ", "-")


def page_lines(html_text: str) -> list[str]:
    s = BeautifulSoup(html_text, "html.parser")
    for t in s(["script", "style"]):
        t.decompose()
    # one grant per <p>/<li>/<br>-separated line
    txt = s.get_text("\n")
    lines = [re.sub(r"\s+", " ", l).strip() for l in txt.split("\n")]
    return [l for l in lines if l]


def split_record(rec: str):
    """Split one list entry into (prefix, title, suffix) around the quoted project title.
    The title runs from the first opening quote to the last closing quote that comes
    before the programme text ("Three year grant totaling ..."); a few entries have a
    stray quote inside the title."""
    i = min([rec.find(q) for q in OPEN_Q if rec.find(q) >= 0] or [-1])
    if i < 0:
        return None
    m = re.search(r"(One|Two|Three|Four|Five|\d)[- ]year|Grants?[- ]in[- ]Aid|totaling", rec[i:], re.I)
    end = i + m.start() if m else len(rec)
    j = max(rec.rfind(q, i + 1, end) for q in CLOSE_Q)
    if j <= i:
        return None
    title = rec[i + 1:j].strip().strip('"“”').strip()
    title = re.sub(r"\s+", " ", title).rstrip(".").strip()
    return rec[:i].strip().rstrip(",;").strip(), title, rec[j + 1:].strip().lstrip(".,;").strip()


def parse_who(prefix: str, page_layout: str | None = None):
    """Return (family, given, institution, layout) from the text before the title.
    Layouts seen on whitehall.org since 1998:
      A  'Family, Given, Institution[, City]'          (2012-present)
      B  'Family, Given; Institution[, City]'          (1999-2011)
      C  'Institution, Given Family'                   (1998 list)
    page_layout is the majority layout of the page, used for entries whose institution
    has no tell-tale word ('Southwestern Louisiana State, Lewis E. Deaton')."""
    prefix = re.sub(r",\s*(?:19|20)\d{2}\s*$", "", prefix).strip()  # 'Drexel University, 2021'
    if ";" in prefix:
        names, inst = prefix.split(";", 1)
        parts = [x.strip() for x in names.split(",") if x.strip()]
        if len(parts) == 1:  # 'Epstein Russell A' / 'Feller. Marla B.' (surname first, comma lost)
            toks = re.split(r"[.\s]+", parts[0].strip(), maxsplit=1)
            parts = [toks[0]] + ([toks[1]] if len(toks) > 1 and toks[1] else [])
        family = parts[0] if parts else None
        given = ", ".join(parts[1:]) or None
        return family, given, inst.strip().strip(",") or None, "B"
    parts = [x.strip() for x in prefix.split(",") if x.strip()]
    if not parts:
        return None, None, None, None
    if len(parts) >= 2 and not INST_RE.search(parts[-1]) and (
            INST_RE.search(parts[0]) or page_layout == "C"):
        if "&" in parts[-1]:  # 'Dickinson & Johnson' (two PIs, surnames only): first one leads
            return parts[-1].split("&")[0].strip(), None, ", ".join(parts[:-1]), "C"
        given, family = split_name(parts[-1])
        return family, given, ", ".join(parts[:-1]), "C"
    family = parts[0]
    given = parts[1] if len(parts) > 1 and not INST_RE.search(parts[1]) else None
    inst = ", ".join(parts[2:] if given else parts[1:]) or None
    return family, given, inst, "A"


def parse_page(html_text: str, source: str, capture: str | None) -> list[dict]:
    lines = page_lines(html_text)
    # A record starts at a line that carries an opening quote; older captures wrap a
    # record over several lines, so following lines without a quote are appended
    # until the programme / amount text closes it.
    records, buf, year = [], [], None
    for l in lines:
        h = HEADING_RE.match(l)
        if h:
            if buf:
                records.append((year, " ".join(buf)))
                buf = []
            year = h.group(1)
            continue
        has_q = any(q in l for q in OPEN_Q)
        if has_q and buf and AMOUNT_RE.search(" ".join(buf)):
            records.append((year, " ".join(buf)))
            buf = []
        if has_q or buf:
            buf.append(l)
            if AMOUNT_RE.search(" ".join(buf)) and re.search(r"\.\s*$", l):
                records.append((year, " ".join(buf)))
                buf = []
    if buf:
        records.append((year, " ".join(buf)))

    out = []
    split = [(y, split_record(re.sub(r"\s+", " ", r).strip()), re.sub(r"\s+", " ", r).strip())
             for y, r in records]
    layouts = [parse_who(sp[0])[3] for _, sp, _ in split if sp]
    page_layout = max(set(layouts), key=layouts.count) if layouts else None
    for heading_year, sp, rec in split:
        if not sp:
            continue
        prefix, title, rest = sp
        amt = AMOUNT_RE.search(rest)
        prog = PROGRAM_RE.search(rest)
        if not amt and not prog:
            continue  # not a grant entry
        family, given, inst, layout = parse_who(prefix, page_layout)
        if not family or not title:
            continue
        aw = AWARDED_RE.search(rest)
        out.append({
            "family_name": family,
            "given_name": given,
            "institution": inst,
            "title": title,
            "program": re.sub(r"\s+", " ", prog.group(1)) if prog else None,
            "renewal": bool(re.search(r"renewal", rest, re.I)),
            "amount": float(amt.group(1).replace(",", "")) if amt else None,
            "award_season": aw.group(1).title() if aw and aw.group(1) else None,
            "awarded_year": aw.group(2) if aw else None,
            "heading_year": heading_year,
            "name_layout": layout,
            "source": source,
            "capture": capture,
            "raw_line": rec,
        })
    return out


def wayback_captures() -> list[tuple[str, str]]:
    caps = {}
    for pat in WAYBACK_PATTERNS:
        r = get(CDX, params={"url": pat, "output": "json", "fl": "timestamp,original,digest",
                             "filter": "statuscode:200", "collapse": "digest"})
        rows = r.json()[1:] if r.text.strip() else []
        for ts, orig, dig in rows:
            caps.setdefault(dig, (ts, orig))
        log(f"CDX {pat}: {len(rows)} distinct captures")
        time.sleep(WAYBACK_DELAY)
    return sorted(caps.values())


def main() -> None:
    p = argparse.ArgumentParser(description="Whitehall Foundation grant recipients -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the live page + first N Wayback captures")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache Wayback captures here")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()

    live = get(LIVE_URL)
    live.raise_for_status()
    live.encoding = "utf-8"
    rows = parse_page(live.text, "live_active_grants", None)
    log(f"live active-grants page: {len(rows)} grants")
    if not rows:
        raise SystemExit("live page parsed to 0 grants; layout changed?")

    caps = wayback_captures()
    if args.limit is not None:
        caps = caps[: args.limit]
    log(f"{len(caps)} distinct Wayback captures to read")
    for i, (ts, orig) in enumerate(caps, 1):
        cache = args.cache_dir / f"{ts}.html" if args.cache_dir else None
        if cache and cache.exists():
            text = cache.read_text()
        else:
            r = get(f"https://web.archive.org/web/{ts}id_/{orig}")
            if r.status_code != 200:
                log(f"  capture {ts}: HTTP {r.status_code}, skipped")
                continue
            # old captures are windows-1252, newer ones UTF-8
            text = r.content.decode("utf-8") if _is_utf8(r.content) else r.content.decode("cp1252", errors="replace")
            if cache:
                args.cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(text)
            time.sleep(WAYBACK_DELAY)
        got = parse_page(text, "wayback_recipients", ts)
        log(f"  [{i}/{len(caps)}] {ts} {orig}: {len(got)} grants, heading years {sorted({g['heading_year'] for g in got if g['heading_year']})}")
        rows += got

    first = consolidate(rows)
    log(f"{len(first)} distinct grants from {len(rows)} list entries")
    for c in ["title", "given_name", "institution", "amount", "program", "award_season", "award_year"]:
        log(f"  {c:14s} {first[c].notna().mean():6.1%}")
    log("  per award year: " + ", ".join(f"{y}:{n}" for y, n in first["award_year"].fillna("unbounded").value_counts().sort_index().items()))
    log("  per first-listed year: " + ", ".join(f"{y}:{n}" for y, n in first["first_listed_year"].fillna("live").value_counts().sort_index().items()))
    log("  programmes: " + json.dumps(first["program"].fillna("?").str.title().value_counts().to_dict()))

    out = first.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "whitehall_projects.parquet"
    out.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(out)} rows to {parquet_path}")

    if args.skip_upload or args.limit is not None:
        log("Upload skipped")
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_whitehall_projects.parquet"
    try:  # runbook 1.4: never shrink the corpus on re-ingest
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(out)}")
        if len(out) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(out)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


def consolidate(rows: list[dict]) -> pd.DataFrame:
    """One row per grant. Entries are the same grant when the surname and first given
    initial match, the renewal flag matches, and the titles match after normalisation
    (or are >= 0.9 similar, which absorbs typo fixes between yearly lists).
    Award year: the live page's 'awarded Season YYYY' when present; otherwise the
    first list year the grant appears in ('first_listed_year'), which is used only
    when the previous year's list was also captured (year_bounded = True), i.e. the
    grant was demonstrably not yet on the list a year earlier."""
    import difflib
    rows = sorted(rows, key=lambda r: (r["source"] != "live_active_grants", r["capture"] or ""))
    list_years = {int(r["heading_year"]) for r in rows if r["heading_year"]}
    groups: dict[tuple, list[list[dict]]] = {}
    for r in rows:
        key = (norm(r["family_name"]), norm(r["given_name"] or "")[:1], r["renewal"])
        tk = norm(r["title"])
        for g in groups.setdefault(key, []):
            gt = norm(g[0]["title"])
            if gt == tk or difflib.SequenceMatcher(None, gt, tk).ratio() >= 0.9:
                g.append(r)
                break
        else:
            groups[key].append([r])
    out = []
    for gs in groups.values():
        for g in gs:
            heading = sorted(int(x["heading_year"]) for x in g if x["heading_year"])
            awarded = next((x["awarded_year"] for x in g if x["awarded_year"]), None)
            first_listed = heading[0] if heading else None
            bounded = first_listed is not None and (first_listed - 1) in list_years
            rec = dict(g[0])
            rec.update({
                "given_name": next((x["given_name"] for x in g if x["given_name"]), None),
                "institution": next((x["institution"] for x in g if x["institution"]), None),
                "amount": next((x["amount"] for x in g if x["amount"]), None),
                "program": next((x["program"] for x in g if x["program"]), None),
                "award_season": next((x["award_season"] for x in g if x["award_season"]), None),
                "awarded_year": awarded,
                "first_listed_year": str(first_listed) if first_listed else None,
                "last_listed_year": str(heading[-1]) if heading else None,
                "year_bounded": bool(awarded) or bounded,
                "award_year": awarded or (str(first_listed) if bounded else None),
                "n_list_entries": len(g),
                "list_years": ",".join(sorted({x["heading_year"] for x in g if x["heading_year"]})),
                "captures": ",".join(sorted({x["capture"] for x in g if x["capture"]})),
            })
            out.append(rec)
    df = pd.DataFrame(out)
    year_part = [a or f"c{f}" for a, f in zip(df["award_year"], df["first_listed_year"])]
    df["funder_award_id"] = [
        f"WHF-{y}-{slug(f)}-{hashlib.sha1((norm(t) + ('|renewal' if rn else '')).encode()).hexdigest()[:6]}"
        for y, f, t, rn in zip(year_part, df["family_name"], df["title"], df["renewal"])]
    df["lead_family_name"] = df["family_name"]
    df["lead_given_name"] = df["given_name"]
    df["currency"] = df["amount"].map(lambda a: "USD" if a is not None and a == a else None)
    df["landing_page_url"] = [LIVE_LANDING if s == "live_active_grants" else
                              f"https://web.archive.org/web/{c}/http://www.whitehall.org/recipients/"
                              for s, c in zip(df["source"], df["capture"])]
    df["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dupes = df["funder_award_id"].duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:10]}")
    return df.sort_values(["first_listed_year", "family_name"], na_position="last")


def _is_utf8(b: bytes) -> bool:
    try:
        b.decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False


if __name__ == "__main__":
    main()
