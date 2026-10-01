#!/usr/bin/env python3
"""
Royal Geographical Society (with IBG) grants to S3
==================================================

The RGS-IBG grants programme funds fieldwork and expedition research (Small
Research Grants, Postgraduate Research Awards, Monica Cole, Henrietta Hutton,
Gilchrist Fieldwork, Ralph Brown Expedition, Gino Watkins Fund, Walters
Kundert, Thesiger-Oman, RGS Explore, Neville Shulman Challenge, Ran and Ginny
Fiennes, Journey in Audio, Frederick Soddy Postgraduate, Fieldwork
Apprenticeships ...).

Source: the Society's own "Projects supported" page,
https://www.rgs.org/exploration/grants/projects-supported ("Grants awarded in
YYYY": one <h3> per scheme, one <li> per award: <strong>Name</strong>
(Institution), Project title [<em>named sub-award</em>]). The page only shows
the current round, so earlier rounds are read from Internet Archive captures of
the same page (discovered with the CDX API, fetched sequentially with backoff).
Each "Grants awarded in YYYY" section is parsed once (the newest capture wins).

Scope: expedition and fieldwork research grants are kept (batch brief).
Excluded: "Frederick Soddy Schools Award" and any "Teaching"/"Schools" scheme
(grants to schoolteachers for pupils' fieldwork trips: education, not
research). Kept and flagged: "Journey in Audio" (place-based audio projects).

No grant number is printed (citing works write e.g. "SRG 23.13", "PRA 13.24",
RGS's internal references, which are not published), so funder_award_id is
synthetic: RGS-{year}-{scheme-code}-{name-slug}.

Output: s3://openalex-ingest/awards/rgs/rgs_projects.parquet
"""

import argparse
import html
import json
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


LIVE_URL = "https://www.rgs.org/exploration/grants/projects-supported"
CDX = ("https://web.archive.org/cdx/search/cdx?url=rgs.org/exploration/grants/projects-supported"
       "&fl=timestamp,statuscode&collapse=digest&filter=statuscode:200")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rgs/rgs_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
EXCLUDE_SCHEME = re.compile(r"Schools?\b|Teach", re.I)
FLAG_SCHEME = re.compile(r"Journey in Audio", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, retries: int = 4, pause: float = 5.0) -> str:
    last_err = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(pause * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def cached(cache_dir: Path | None, name: str, url: str, **kw) -> str:
    path = cache_dir / name if cache_dir else None
    if path and path.exists():
        return path.read_text()
    body = get(url, **kw)
    if path:
        cache_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    return body


def tidy(s: str | None) -> str | None:
    if not s:
        return None
    s = html.unescape(re.sub(r"<[^>]+>", " ", s)).replace("\xa0", " ").replace("​", "")
    s = re.sub(r"\s+", " ", s).strip(" ,;:|")
    return s or None


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical wolf_to_s3.py helper (runbook §2.4.1): strip trailing degree /
    suffix tokens, last remaining token = family name."""
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
    given, family = tokens[:-1], tokens[-1]
    # surname particles stay with the family name ('Teun De Jong', 'Sobreiro e Cruz', 'El hichou')
    while len(given) > 1 and given[-1].lower() in {"de", "del", "della", "di", "da", "van", "von", "der", "la", "le",
                                                    "el", "al", "e", "dos", "das", "du"}:
        family = given.pop() + " " + family
    return " ".join(given), family


SCHEME_CODES = {  # short codes for the synthetic key (RGS's own abbreviations where known)
    "Small Research Grants": "SRG", "Postgraduate Research Awards": "PRA", "Monica Cole Research Grant": "MC",
    "Henrietta Hutton Research Grant": "HH", "Gilchrist Fieldwork Award": "GFA", "Ralph Brown Expedition Award": "RBEA",
    "Gino Watkins Fund Awards": "GW", "Walters Kundert Fellowship": "WKF", "Thesiger-Oman International Fellowships": "TOIF",
    "RGS Explore Grants": "EXPLORE", "Neville Shulman Challenge Award": "NSCA", "Ran and Ginny Fiennes Award": "RGF",
    "Journey in Audio": "AUDIO", "Frederick Soddy Postgraduate Award": "FSPA", "Fieldwork Apprenticeships": "FA",
    "Geographical Fieldwork Grants": "GFG", "Geographical Club Award": "GCA", "Hong Kong Research Grant": "HK",
    "Ray Y Gildea Jr Award": "RYG", "Peter Smith Award": "PSA", "Jasmin Leila Award": "JLA",
}


def parse_rounds(page: str) -> dict[int, list[dict]]:
    """{year: [records]} for every 'Grants awarded in YYYY' section on the page."""
    out = {}
    parts = re.split(r"<h2[^>]*>\s*Grants awarded in (\d{4})\s*</h2>", page)
    for k in range(1, len(parts), 2):
        year, body = int(parts[k]), parts[k + 1]
        body = re.split(r"<h2", body)[0]
        recs = []
        for m in re.finditer(r"<h3[^>]*>(.*?)</h3>\s*<ul[^>]*>(.*?)</ul>", body, re.S):
            scheme = tidy(m.group(1))
            for li in re.findall(r"<li[^>]*>(.*?)</li>", m.group(2), re.S):
                strong = re.search(r"<strong>(.*?)</strong>", li, re.S)
                bare = tidy(re.sub(r"</?em>", "", li)) or ""
                if not strong and re.fullmatch(r"\[.*\]", bare) and recs and recs[-1]["scheme"] == scheme:
                    # a named sub-award printed as its own bullet belongs to the award above it
                    tag = tidy(bare.strip("[]"))
                    recs[-1]["sub_award"] = "; ".join(x for x in [recs[-1]["sub_award"], tag] if x)
                    continue
                if not strong:
                    continue
                li_clean = re.sub(r"</?em>", "", li)
                sub = [tidy(x) for x in re.findall(r"\[(.*?)\]", li_clean, re.S)]
                li_clean = re.sub(r"\[.*?\]", " ", li_clean, flags=re.S)
                # <strong>Names</strong> (Institution) [and <strong>Names</strong> (Institution)] Title
                people, current, title_parts = [], [], []
                for seg in re.split(r"(<strong>.*?</strong>)", li_clean, flags=re.S)[1:]:
                    if seg.startswith("<strong>"):
                        names = tidy(seg) or ""
                        names = re.sub(r"\s*[-–—,:]\s*$", "", names)
                        current = [{"name": n.strip(), "inst": None}
                                   for n in re.split(r",\s*|\s+and\s+|\s*&\s*", names) if n.strip()]
                        people.extend(current)
                        continue
                    txt = tidy(seg) or ""
                    mi = re.match(r"^\((.*?)\)\s*(.*)$", txt, re.S)
                    if mi:
                        for p_ in current:
                            p_["inst"] = tidy(mi.group(1))
                        txt = mi.group(2)
                    txt = re.sub(r"^(?:and\b|[,\-–—:])\s*", "", txt).strip()
                    if txt and txt.lower() != "and":
                        title_parts.append(txt)
                title = tidy(" ".join(title_parts))
                if not people or not title:
                    continue
                recs.append({"year": year, "scheme": scheme, "name": people[0]["name"],
                             "institution": people[0]["inst"],
                             "co_name": people[1]["name"] if len(people) > 1 else None,
                             "co_institution": people[1]["inst"] if len(people) > 1 else None,
                             "team_json": json.dumps(people, ensure_ascii=False) if len(people) > 1 else None,
                             "title": title, "sub_award": "; ".join(x for x in sub if x) or None})
        out[year] = recs
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="RGS-IBG projects supported -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only N rows per round (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache pages here (re-runs skip fetch)")
    p.add_argument("--live-only", action="store_true", help="skip Internet Archive captures")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    rounds: dict[int, tuple[str, list[dict]]] = {}
    live = parse_rounds(cached(args.cache_dir, "projects_supported_live.html", LIVE_URL))
    for y, recs in live.items():
        rounds[y] = (LIVE_URL, recs)
    log(f"live page: rounds {sorted(live)}")
    caps = []
    if not args.live_only:
        try:
            cdx = get(CDX, retries=3, pause=10)
            caps = [l.split() for l in cdx.splitlines() if re.match(r"^\d{14} ", l)]
        except RuntimeError as e:
            # Archive outage: ship the live round; the §1.4 shrink guard keeps earlier rounds
            # already in S3 from being overwritten by a smaller file.
            log(f"WARNING: Internet Archive CDX unavailable ({str(e)[:120]})")
            if args.cache_dir:  # captures fetched by an earlier run are still usable
                caps = [[p_.stem[3:], "200"] for p_ in args.cache_dir.glob("wb_*.html")]
                log(f"  using {len(caps)} cached captures")
        log(f"Internet Archive: {len(caps)} distinct captures")
        for ts, _ in sorted(caps, reverse=True):  # newest first: a round's newest capture wins
            url = f"https://web.archive.org/web/{ts}id_/{LIVE_URL}"
            try:
                page = cached(args.cache_dir, f"wb_{ts}.html", url, retries=5, pause=15)
            except RuntimeError as e:
                log(f"  capture {ts}: {e}")
                continue
            for y, recs in parse_rounds(page).items():
                if y not in rounds and recs:
                    rounds[y] = (f"https://web.archive.org/web/{ts}/{LIVE_URL}", recs)
                    log(f"  capture {ts}: round {y} ({len(recs)} awards)")
            time.sleep(3)

    rows = []
    for y, (src, recs) in sorted(rounds.items()):
        for r in recs[: args.limit] if args.limit else recs:
            rows.append(dict(r, source_url=src))
    df = pd.DataFrame(rows)
    excluded = df["scheme"].str.contains(EXCLUDE_SCHEME)
    log(f"excluded {int(excluded.sum())} school/teaching awards: {df.loc[excluded, 'scheme'].value_counts().to_dict()}")
    df = df[~excluded].copy()
    df["flag"] = df["scheme"].map(lambda s: "kept_flagged_non_research_format" if FLAG_SCHEME.search(s or "") else None)
    names = [split_name(n) for n in df["name"]]
    df["lead_given_name"] = [g for g, _ in names]
    df["lead_family_name"] = [f for _, f in names]
    co = [split_name(n) if isinstance(n, str) else (None, None) for n in df["co_name"]]
    df["co_given_name"] = [g for g, _ in co]
    df["co_family_name"] = [f for _, f in co]
    df["investigators_json"] = [
        json.dumps([dict(zip(("given", "family"), split_name(p_["name"])), inst=p_["inst"]) for p_ in json.loads(t)],
                   ensure_ascii=False) if isinstance(t, str) else None
        for t in df["team_json"]]
    code = df["scheme"].map(lambda s: SCHEME_CODES.get(s) or slug(s).upper()[:20])
    df["funder_award_id"] = [f"RGS-{y}-{c}-{slug(n)}" for y, c, n in zip(df["year"], code, df["name"])]
    dup = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dup.any():
        df.loc[dup, "funder_award_id"] = df.loc[dup, "funder_award_id"] + "-" + df.loc[dup, "title"].map(lambda t: slug(t)[:24])
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate funder_award_id after disambiguation")
    df["year"] = df["year"].astype(str)

    log(f"rows: {len(df)} across rounds {sorted(df['year'].unique())}; by scheme {df['scheme'].value_counts().to_dict()}")
    for c in ["title", "lead_family_name", "institution", "sub_award"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "rgs_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_rgs_projects.parquet"
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
