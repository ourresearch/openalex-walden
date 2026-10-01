#!/usr/bin/env python3
"""
Jenny and Antti Wihuri Foundation (Jenny ja Antti Wihurin rahasto) to S3
========================================================================

Wihuri publishes every grant from its general call (Yleinen apurahahaku) for
the last ten-plus years at https://wihurinrahasto.fi/myonnot/ ("Rahoitetut
hankkeet"): a server-rendered, paginated list (20 rows per page) with
filters for year, grant category (Taide = arts, Tiede = science,
Yhteiskunnallinen toiminta = societal activity) and applicant type. Each row
carries: year, grantee (person with degree title, several persons, or an
organisation), a purpose line (e.g. 'vaitoskirjatyohon "<thesis title>"'),
and the amount in euros. No grant numbers, no per-grant pages, no export
(the WP REST API is 401). Method 5 (static HTML) on the runbook ladder.

Scope filter: ONLY the foundation's own science category
(?myonto_kategoria=tiede). Arts (taide) and societal-activity (yht) grants
are a mixed foundation's non-research money and are excluded. Within the
science category every row is kept, including the ~12% paid to
organisations (societies, seminars, journals), per the batch scope rule
(keep and flag when unsure).

Grantee parsing (Python side, runbook 2.4.1): the grantee string is
'<degree title> <given names> <family name>' (Finnish 'Filosofian maisteri
Ella Ahti' or English 'Master of Science in X Firstname Lastname'), possibly
several comma-separated people, or an organisation (optionally
'Org /Surname'). The title is stripped (last lowercase token + degree
vocabulary), then the canonical split_name helper splits given/family.

funder_award_id: Wihuri publishes no grant number (citing works write the
8-digit application number, e.g. 00180353, which the site never shows), so
a stable synthetic key 'WIHURI-<year>-<sha1(year|grantee|purpose)[:10]>'
is used. Collisions raise.

Output: s3://openalex-ingest/awards/wihuri/wihuri_projects.parquet
"""

import argparse
import hashlib
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
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

BASE = "https://wihurinrahasto.fi/myonnot/"
CATEGORY = "tiede"  # science only; taide (arts) and yht (societal) excluded
FIRST_YEAR = 2014
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/wihuri/wihuri_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.7
MAX_CONSECUTIVE_NON200 = 5
MAX_PAGES_PER_YEAR = 60


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> tuple[int, str]:
    last = None
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.encoding = "utf-8"
            return r.status_code, r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def clean(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = html.unescape(re.sub(r"<[^>]+>", " ", fragment))
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def parse_rows(page: str) -> list[dict]:
    out = []
    for it in re.findall(r'<div class="grant-row">(.*?)</div>\s*</li>', page, re.S):
        def g(cls):
            m = re.search(r'class="grant-row__' + cls + r'[^"]*">(.*?)</span>', it, re.S)
            return clean(m.group(1)) if m else None
        out.append({"year": g("year"), "grantee": g("name"), "purpose": g("description"), "amount_text": g("sum")})
    return out


def fetch_year(year: int, cache_dir: Path | None) -> list[dict]:
    """Follow the list's own 'next page' links for one year. A page with no
    rows does not end the year; only the absence of a next link does."""
    url = f"{BASE}?myonto_kategoria={CATEGORY}&grant_year={year}"
    rows, page, non200 = [], 1, 0
    while url and page <= MAX_PAGES_PER_YEAR:
        cache = cache_dir / f"{CATEGORY}_{year}_p{page}.html" if cache_dir else None
        if cache and cache.exists():
            status, text = 200, cache.read_text()
        else:
            status, text = get(url)
            time.sleep(REQUEST_DELAY)
            if status == 200 and cache:
                cache.write_text(text)
        if status != 200:
            non200 += 1
            log(f"  {year} page {page}: HTTP {status} ({non200}/{MAX_CONSECUTIVE_NON200}); retrying")
            if non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(f"{url}: {non200} consecutive non-200 responses")
            time.sleep(5)
            continue
        non200 = 0
        got = parse_rows(text)
        for r in got:
            r["list_page"] = page
        rows += got
        nxt = [m for m in re.finditer(r'href="([^"]*/page/(\d+)/\?[^"]*)"', text) if int(m.group(2)) == page + 1]
        url = html.unescape(nxt[0].group(1)) if nxt else None
        page += 1
    return rows


# ---------------------------------------------------------------- names ----

PARTICLES = {"van", "von", "de", "der", "den", "da", "di", "del", "la", "le", "du", "dos", "das",
             "bin", "al", "el", "ter", "ten", "af", "zu", "y", "e", "os."}
# capitalised single-word titles that precede a name
LEAD_TITLES = {
    "arkkitehti", "maisema-arkkitehti", "diplomi-insinööri", "tutkimusprofessori", "professori",
    "dosentti", "ll.m.", "ll.m", "llm", "msc", "msc,", "m.sc.", "phd", "ph.d.", "dr.", "dr", "prof.",
    "ekonomi", "insinööri", "lääkäri", "agronomi", "proviisori", "proviisoriopiskelija", "farmaseutti",
    "toimittaja", "muusikko", "mba", "m.a.", "b.sc.", "bsc", "sosionomi", "tohtori", "medianomi",
    "konservaattori", "psykoterapeutti", "varatuomari", "ylioppilas", "akateemikko", "asianajaja",
    "diplom-kauffrau", "diplomkauffrau", "dip", "tradenomi", "merkonomi", "metsänhoitaja",
    "hammaslääkäri", "eläinlääkäri", "psykologi", "teologi", "pastori", "rovasti", "tutkija",
    "yliopistonlehtori", "apulaisprofessori", "akatemiatutkija", "akatemiaprofessori",
    "emeritusprofessori", "master", "masters", "master's", "doctor", "bachelor", "licentiate",
}
DEG_WORDS = set("""science sciences philosophy philosophical arts art business administration economics
engineering social studies management technology research biology health history physics public
marketing neuroscience finance international architecture doctoral degree degree, information
communication communications quantitative methods church space robotics automation wood material
east european law laws education medicine mathematics chemistry computer environmental political
psychology psychological theology music fine design media culture culture, cultural development
applied natural agriculture forestry forest pathology food nutrition sustainable sustainability
energy data analytics statistics linguistics languages language literature translation sociology
anthropology geography geology physiology pharmacy pharmaceutical biotechnology nursing dentistry
veterinary clinical molecular cell cellular medical biomedical materials mechanical electrical
electronic civil chemical industrial software systems system computing computational artificial
intelligence security cyber environment ecology evolutionary evolution marine water urban planning
regional rural tourism hospitality accounting entrepreneurship innovation strategy strategic global
human resources organizational leadership governance policy peace peace, mediation conflict gender
comparative religion religious biblical philology classics archaeology musicology humanities
theatre film journalism advanced professional integrated interdisciplinary mres mphil process
interactive cognitive curatorial asian early modern theoretical service consumption ethics biolaw
field the solid state physics-solid 2 for of in and &""".split())
ORG_RE = re.compile(
    r"(\bry\b|\br\.y\.|\brf\b|\bsr\b|\boy\b|\bOy\b|\bAMK\b|yliopisto|korkeakoulu|högskolan|Akademi\b|"
    r"säätiö|[Yy]hdistys|[Ss]eura\b|seura\b|liitto|laitos|[Ss]ociety|[Uu]niversity|[Ii]nstitute|"
    r"[Ii]nstituutti|[Ff]oundation|[Aa]ssociation|[Cc]entre\b|[Cc]enter\b|keskus|[Aa]kademia|lehti|"
    r"AIESEC|KATAJA|[Ff]orum\b|päivät|[Nn]etwork|[Cc]ollege|kollegium|museo|kirjasto|arkisto|"
    r"[Ss]chool\b|seminaari|[Ss]ymposium|[Cc]onference|toimikunta|[Cc]ommittee|neuvosto|"
    r"[Mm]inisteriö|[Oo]suuskunta|Ajatuspaja|Kansanlähetys|juhla|^Junction$|[Tt]iedekunta)")
PERSON_TITLE_RE = re.compile(
    r"^(\S+\s+){0,2}\S*(maisteri|tohtori|lisensiaatti|kandidaatti|insinööri|arkkitehti|ekonomi|"
    r"professori|dosentti|lääkäri|tutkija)\b|^(Master|Masters|Master.s|Doctor|Bachelor|Licentiate|"
    r"LL\.?M|MSc|M\.Sc|PhD|Ph\.D|MBA|Dr|Prof|Diplom)", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook 2.4.1 helper (wolf_to_s3.py): strip trailing
    degree/suffix tokens, last token = family, the rest = given."""
    if not name:
        return None, None
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def strip_title(segment: str) -> tuple[str, str]:
    """'Filosofian maisteri Ella Ahti' -> ('Filosofian maisteri', 'Ella Ahti');
    'Master of Science in Asian Studies Junhua Zhu' -> (..., 'Junhua Zhu')."""
    toks = segment.split()
    end = -1
    for i, t in enumerate(toks[:-1]):
        tl = t.lower()
        if (t[0].islower() and tl not in PARTICLES) or tl in LEAD_TITLES:
            end = i
    rest = toks[end + 1:]
    while len(rest) > 2 and (rest[0].lower().strip(",()") in DEG_WORDS or rest[0].startswith("(")):
        rest = rest[1:]
    return " ".join(toks[: len(toks) - len(rest)]), " ".join(rest)


def parse_grantee(grantee: str) -> dict:
    g = re.sub(r"\s+", " ", grantee or "").strip()
    if not PERSON_TITLE_RE.match(g) and ORG_RE.search(g):
        # 'Aalto-yliopiston kauppakorkeakoulu /Piekkari': org with a named lead
        m = re.search(r"^(.*?)\s*/\s*([A-ZÅÄÖ][\w\-]+)(?:\s+([A-ZÅÄÖ][\w\-]+))?\s*$", g)
        people = []
        org = g
        if m and not ORG_RE.search(m.group(2)):
            org = m.group(1).strip()
            # 'Turun yliopisto/Kuortti Joel' is family-first; a lone token is a family name
            family, given = m.group(2), m.group(3)
            people.append({"title": None, "name": f"{given} {family}" if given else family,
                           "given_name": given, "family_name": family})
        return {"grantee_type": "organisation", "organisation": org, "people": people}
    people = []
    for seg in re.split(r",\s+|(?<!-)\s+ja\s+(?=[A-ZÅÄÖ])", g):
        title, name = strip_title(seg)
        toks = name.split()
        # a segment that is only a degree title or a degree-subject fragment
        if not toks or all(t.lower().strip(",.") in DEG_WORDS | LEAD_TITLES for t in toks):
            continue
        given, family = split_name(name)
        people.append({"title": title or None, "name": name, "given_name": given, "family_name": family})
    return {"grantee_type": "person" if people else "organisation",
            "organisation": None if people else g, "people": people}


# -------------------------------------------------------------- purpose ----

def scheme_for(purpose: str | None) -> str:
    p = (purpose or "").lower()
    if p.startswith("väitöskirjan jälkeiseen"):
        return "Postdoctoral research grant"
    if p.startswith("väitöskirja"):
        return "Doctoral research grant"
    return "Research grant"


def title_for(purpose: str | None) -> str | None:
    """The quoted work title when the purpose line has one, else the purpose
    line itself (Finnish, e.g. 'matalien lämpötilojen perustutkimukseen')."""
    if not purpose:
        return None
    m = re.search(r"[”“\"]([^”“\"]{4,})[”“\"]", purpose)
    if m:
        return m.group(1).strip()
    return purpose[0].upper() + purpose[1:]


def parse_amount(text: str | None) -> float | None:
    if not text:
        return None
    digits = re.sub(r"[^\d,]", "", text).replace(",", ".")
    try:
        v = float(digits)
    except ValueError:
        return None
    return v if v > 0 else None


def main() -> None:
    p = argparse.ArgumentParser(description="Wihuri Foundation science grants -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="keep only the first N rows (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/wihuri"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache list pages here (re-runs skip fetch)")
    p.add_argument("--first-year", type=int, default=FIRST_YEAR)
    p.add_argument("--last-year", type=int, default=datetime.now().year)
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    raw = []
    for year in range(args.first_year, args.last_year + 1):
        got = fetch_year(year, args.cache_dir)
        bad = [r for r in got if r["year"] != str(year)]
        if bad:
            raise SystemExit(f"{year}: {len(bad)} rows carry another year, e.g. {bad[0]}")
        log(f"{year}: {len(got)} science grants")
        raw += got
        if args.limit and len(raw) >= args.limit:
            break
    if args.limit:
        raw = raw[: args.limit]

    recs = []
    for r in raw:
        g = parse_grantee(r["grantee"])
        lead = g["people"][0] if g["people"] else None
        key = "|".join([r["year"], r["grantee"] or "", r["purpose"] or ""])
        recs.append({
            "funder_award_id": f"WIHURI-{r['year']}-{hashlib.sha1(key.encode('utf-8')).hexdigest()[:10]}",
            "grant_year": r["year"],
            "grantee": r["grantee"],
            "grantee_type": g["grantee_type"],
            "organisation": g["organisation"],
            "purpose": r["purpose"],
            "title": title_for(r["purpose"]),
            "funder_scheme": scheme_for(r["purpose"]),
            "amount_text": r["amount_text"],
            "amount": parse_amount(r["amount_text"]),
            "currency": "EUR",
            "lead_title": lead["title"] if lead else None,
            "lead_name": lead["name"] if lead else None,
            "lead_given_name": lead["given_name"] if lead else None,
            "lead_family_name": lead["family_name"] if lead else None,
            "lead_affiliation": g["organisation"] if lead else None,
            "people": json.dumps(g["people"], ensure_ascii=False),
            "category": CATEGORY,
            "landing_page_url": f"{BASE}?myonto_kategoria={CATEGORY}&grant_year={r['year']}",
        })
    df = pd.DataFrame(recs)
    dup = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dup.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dup, ['funder_award_id', 'grantee']].head().to_dict('records')}")

    log(f"{len(df)} grants, {df['grant_year'].min()}-{df['grant_year'].max()}")
    for c in ["title", "amount", "lead_family_name", "lead_given_name", "organisation"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  grantee_type {df['grantee_type'].value_counts().to_dict()}")
    log(f"  funder_scheme {df['funder_scheme'].value_counts().to_dict()}")
    log(f"  total EUR {df['amount'].sum():,.0f}")
    top = df.groupby(["lead_given_name", "lead_family_name"]).size().sort_values(ascending=False).head(5)
    log(f"  6.4a top PI combos: {top.to_dict()}")

    df = df.astype("string")
    df["amount"] = pd.to_numeric(df["amount"])
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "wihuri_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {len(df)} rows to {out}")
    if args.skip_upload:
        return
    if args.limit:
        raise SystemExit("refusing to upload a --limit smoke file to S3")

    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_wihuri_projects.parquet"
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
