#!/usr/bin/env python3
"""
Technology Agency of the Czech Republic (TA ČR) to S3 Data Pipeline
===================================================================

TA ČR's own R&D search engine STARFOS (https://starfos.tacr.cz) indexes every
Czech publicly funded R&D project from the national CEP register (IS VaVaI),
for ALL Czech providers (GAČR, ministries, TA ČR, regions ...). This script
routes by provider (runbook §2.3.2): it keeps only the programmes that STARFOS
files under the provider "TA ČR - Technology Agency of the Czech Republic"
(ALFA, BETA, GAMA, DELTA, EPSILON, ZETA, ETA, THETA, KAPPA, SIGMA, NCK,
Centra kompetence, TREND, DOPRAVA, Prostředí pro život, PRODEF ...).
TA ČR is the formal provider (poskytovatel) of all of these, including the
programmes it runs on behalf of ministries (TREND for MPO, DOPRAVA for MD,
Prostředí pro život for MŽP); citing works acknowledge them to TA ČR.

Source API (the JSON backend of the STARFOS Next.js app, old.starfos.tacr.cz,
robots.txt "Allow: /"):
  - GET  /api/starfos/settings/isvav_project/facets/en  -> provider > programme tree
  - POST /api/starfos/export   {collection, filters, columns, format: csv}
         the site's own "Export" button (CSV/XLSX, <= 20,000 rows per export);
         one export per TA ČR programme
  - POST /api/starfos/search   (limit <= 15) -> per-project "Project
         implementation: Jan 1, 2011 - Dec 31, 2013" exact dates (the export
         only carries years)

Every exported programme is checked against the search API's num_found for the
same filter; a short export raises (no silent truncation).

Fields: project code (the citable reference, e.g. TK01010001 -- what papers
put in acknowledgements), Czech + English title and annotation, programme,
main participant (institution + IČO), main project researchers (name with
Czech academic titles, vedidk, ORCID), recognised costs and public support
(thousand CZK -> CZK), solve begin/end year, exact implementation dates.

Output: s3://openalex-ingest/awards/tacr/tacr_projects.parquet

Usage:
    py -3.13 tacr_starfos_to_s3.py --skip-upload --limit 30   # smoke test
    py -3.13 tacr_starfos_to_s3.py                             # full run + upload
"""

import argparse
import io
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22, runbook §1.2) ---
# (equivalent of sys.stdout.reconfigure(encoding="utf-8") under an alias)
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

API = "https://old.starfos.tacr.cz/api/starfos"
LANDING = "https://starfos.tacr.cz/en/projekty/{code}"
COLLECTION = "isvav_project"
PROVIDER_PREFIX = "TA ČR"  # facet option label "TA ČR - Technology Agency of the Czech Republic"
# Fallback if the facet tree can't be read (as listed on 2026-10-01).
FALLBACK_PROGRAMMES = ["CK", "CL", "FW", "OZ", "SQ", "SS", "TA", "TB", "TD", "TE", "TF", "TG",
                       "TH", "TI", "TJ", "TK", "TL", "TM", "TN", "TO", "TP", "TQ", "TS", "TT"]
EXPORT_COLUMNS = [
    "code", "name", "name_en", "anot", "anot_en", "exp_programme__funder", "exp_programme",
    "exp_organization_main", "exp_organization_others", "exp_organization_all",
    "exp_solver_main", "exp_solver_others", "exp_cep_main", "exp_ford_main",
    "keywords_en_parsed", "fin_total", "fin_state_budget", "fin_other_public", "fin_non_public",
    "x_solve_begin_year", "x_solve_end_year", "url",
]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/tacr/tacr_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)",
           "Content-Type": "application/json", "Accept": "application/json, text/csv, */*"}
REQUEST_DELAY = 0.4
RETRIES = 6
PAGE = 15  # API maximum for /search

MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}

T0 = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - T0:7.1f}s] {msg}", flush=True)


CACHE_DIR: Path | None = None  # --cache-dir: responses cached so an interrupted run resumes cheaply


def call(method: str, path: str, body: dict | None = None) -> bytes:
    url = f"{API}/{path}"
    payload = json.dumps(body, sort_keys=True) if body is not None else None
    cache = None
    if CACHE_DIR is not None:
        import hashlib
        cache = CACHE_DIR / (hashlib.sha1(f"{method} {url} {payload}".encode()).hexdigest() + ".bin")
        if cache.exists():
            return cache.read_bytes()
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.request(method, url, headers=HEADERS, data=payload, timeout=180)
            time.sleep(REQUEST_DELAY)
            if r.status_code == 200:
                if cache is not None:
                    cache.write_bytes(r.content)
                return r.content
            last = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as e:  # transient network error
            last = repr(e)
        log(f"  {method} {path} attempt {attempt}/{RETRIES} failed: {last}")
        time.sleep(5 * attempt)
    raise RuntimeError(f"{method} {url} failed after {RETRIES} attempts: {last}")


def programmes() -> dict[str, str]:
    try:
        facets = json.loads(call("GET", f"settings/{COLLECTION}/facets/en"))["facets"]
        tree = next(f for f in facets if f["name"] == "program__code")["options"]
        group = next(o for o in tree if o["option_label"].startswith(PROVIDER_PREFIX))
        progs = {c["option_code"]: c["option_label"] for c in group["children"]}
        log(f"Provider group: {group['option_label']} -> {len(progs)} programmes")
        return progs
    except Exception as e:  # noqa: BLE001
        log(f"WARNING facet tree unreadable ({e!r}); using fallback programme list")
        return {c: c for c in FALLBACK_PROGRAMMES}


def prog_filter(code: str) -> dict:
    return {"program__code": {"option_codes": [code]}}


def num_found(code: str) -> int:
    body = {"collection": COLLECTION, "text": "", "language_ui": "en", "language_search": "en",
            "limit": 0, "offset": 0, "filters": prog_filter(code)}
    return int(json.loads(call("POST", "search", body))["meta"]["num_found"])


def export(code: str) -> pd.DataFrame:
    body = {"collection": COLLECTION, "language_ui": "en", "format": "csv", "limit": 0,
            "columns": EXPORT_COLUMNS, "filters": prog_filter(code)}
    content = call("POST", "export", body)
    return pd.read_csv(io.StringIO(content.decode("utf-8-sig")), dtype=str, keep_default_na=False)


DATE_RE = re.compile(r"Project implementation:\s*([A-Za-z]{3})\w*\.? (\d{1,2}), (\d{4})\s*-\s*([A-Za-z]{3})\w*\.? (\d{1,2}), (\d{4})")


def iso(mon: str, day: str, year: str) -> str | None:
    m = MONTHS.get(mon.lower()[:3])
    if not m:
        return None
    try:
        return datetime(int(year), m, int(day)).strftime("%Y-%m-%d")
    except ValueError:
        return None


def search_dates(code: str, total: int, ordering: tuple[str, bool] | None = None) -> dict[str, tuple]:
    """Page through /search (15 per page) collecting exact implementation dates."""
    out: dict[str, tuple] = {}
    offset = 0
    consecutive_empty = 0
    while offset < total:
        body = {"collection": COLLECTION, "text": "", "language_ui": "en", "language_search": "en",
                "limit": PAGE, "offset": offset, "filters": prog_filter(code)}
        if ordering:
            body["ordering_field"], body["ordering_asc"] = ordering
        rs = json.loads(call("POST", "search", body)).get("results") or []
        if not rs:
            consecutive_empty += 1
            log(f"    {code} offset {offset}: empty page ({consecutive_empty}/3); continuing")
            if consecutive_empty >= 3:
                break
            offset += PAGE
            continue
        consecutive_empty = 0
        for r in rs:
            pcode = r.get("detail_url", "").split("/")[-1]
            for b in r.get("info_bits", []):
                m = DATE_RE.search(b.get("tooltip", ""))
                if m:
                    out[pcode] = (iso(*m.group(1, 2, 3)), iso(*m.group(4, 5, 6)))
        offset += PAGE
    return out


# ---------------------------------------------------------------- names
# Czech (and common foreign) academic titles, written before or after the name
# ("Ing. Jan Novák, Ph.D.", "prof. RNDr. Eva Malá, CSc.").
TITLE_TOKENS = {
    "bc.", "bca.", "mgr.", "mga.", "ing.", "arch.", "ing.arch.", "mudr.", "mddr.", "mvdr.", "pharmdr.",
    "phdr.", "rndr.", "judr.", "thdr.", "thlic.", "thmgr.", "paeddr.", "rsdr.", "dr.", "doc.", "prof.",
    "akad.", "mal.", "soch.", "arch", "et", "ph.d.", "ph.d", "phd.", "phd", "csc.", "csc", "drsc.", "drsc",
    "dsc.", "th.d.", "dis.", "dr.h.c.", "mba", "mba.", "emba", "ll.m.", "llm", "m.a.", "ma", "m.sc.",
    "msc.", "msc", "mph", "mpa", "b.a.", "ba", "bsc.", "bsc", "dipl.", "dipl.-ing.", "dr.-ing.",
    "univ.", "dr.rer.nat.", "mult.", "dba", "d.phil.", "ph.dr.", "m.d.", "md", "mpharm", "pharm.d.",
    "dr.techn.", "dr.ing.", "ing.-paed.", "igip", "mudr.,", "mbbs", "frcs", "frcp", "feng", "freng",
    "dr.h.c", "h.c.", "mgr.et", "dipl.ing.", "ing.,", "paedr.", "artd.", "mgr.art.", "doc.,", "prof.,",
    # junior/senior and post-nominals seen in the 2026-10-01 full run
    "ml.", "st.", "feng.", "paed.igip", "techn.", "dr.-techn.", "phmr.", "ch.e", "dt.", "ceng.", "ceng",
    "mice", "et.", "ing", "dr", "prof", "doc", "mgr", "bc", "rndr", "phdr", "judr", "mudr", "mvdr", "dis",
    "e.ma", "mphil.", "mphil", "ma.", "dipl.-ing", "dipl.-biol.", "phil.", "plk.", "gšt.", "assoc.", "eng.", "rer.", "nat.", "m.s", "m.a", "m.p.a", "theol.", "és", "sc.",
}
GLUED_TITLE_RE = re.compile(r"^(prof|doc|ing|mgr|dr|rndr|phdr|judr|mudr|mvdr|bc|paeddr)\.[A-Za-z.]+$", re.I)
HONORIS_RE = re.compile(r"(?<![A-Za-z])(?:dr\.?\s*)?h\.\s*c\.?(?=\s|,|$)", re.I)  # "dr. h. c." / "h. c."
# multi-dot abbreviations like "Ph.D.", "Th.D.", "LL.M." (but NOT single initials like "J.")
TITLE_RE = re.compile(r"^(?:[A-Za-z]{1,6}\.){2,}[A-Za-z]{0,4}\.?$")


def is_title(tok: str) -> bool:
    t = tok.strip(",;").lower()
    return t in TITLE_TOKENS or bool(TITLE_RE.match(tok.strip(",;"))) or bool(GLUED_TITLE_RE.match(t))


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), after removing Czech academic titles,
    which can sit on both sides of the name."""
    if not name:
        return None, None
    name = re.sub(r"Ph\s*[.:]\s*D\s*[.:]?", "Ph.D.", HONORIS_RE.sub(" ", name))  # "Ph. D." / "Ph:D:"
    name = re.sub(r",(?=\S)", ", ", name)                                           # "PhD.,MSc."
    tokens = [t.strip(",") for t in name.split() if not is_title(t)]
    tokens = [t for t in tokens if t]
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def split_top(s: str, sep: str = ";") -> list[str]:
    """Split on sep only outside parentheses."""
    parts, depth, cur = [], 0, []
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == sep and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        parts.append("".join(cur).strip())
    return [p for p in parts if p]


def parse_people(s: str) -> list[dict]:
    """'Mgr. Martin Švec Ph.D. (vedidk=2167058, ORCID=0000-0002-1692-5951, ScopusID=...)'."""
    people = []
    for part in split_top(s or ""):
        m = re.match(r"^(.*?)\s*(?:\((.*)\))?\s*$", part, re.S)
        raw_name = (m.group(1) if m else part).strip()
        attrs = {}
        if m and m.group(2):
            for kv in m.group(2).split(","):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    attrs[k.strip().lower()] = v.strip()
        given, family = split_name(raw_name)
        orcid = attrs.get("orcid")
        if orcid and not re.match(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$", orcid):
            orcid = None
        people.append({"name_raw": raw_name, "given_name": given, "family_name": family,
                       "orcid": orcid, "vedidk": attrs.get("vedidk")})
    return people


def parse_org(s: str) -> dict:
    """Main participant: 'Masarykova univerzita / Právnická fakulta (parent IČO: 00216224)'."""
    first = split_top((s or "").replace("\n", ";"))
    if not first:
        return {"name": None, "ico": None}
    m = re.match(r"^(.*?)\s*(?:\((.*)\))?\s*$", first[0], re.S)
    name = (m.group(1) if m else first[0]).strip() or None
    ico = None
    if m and m.group(2):
        mi = re.search(r"I[CČ]O:?\s*([0-9A-Za-z]+)", m.group(2), re.I)
        ico = mi.group(1) if mi else None
    return {"name": name, "ico": ico}


def org_country(detail: str, main_name: str | None) -> str | None:
    """Country of the main participant from 'Detailed participants' information'
    ('... adresa: 70, Veveří 158, 611 80 Brno-střed, Česko)')."""
    for line in (detail or "").splitlines():
        if main_name and line.strip().startswith(main_name):
            m = re.search(r"adresa:[^)]*,\s*([^,()]+)\)\s*$", line.strip())
            return m.group(1).strip() if m else None
    return None


COUNTRY_ISO = {"česko": "CZ", "česká republika": "CZ", "slovensko": "SK", "německo": "DE",
               "rakousko": "AT", "polsko": "PL", "spojené království": "GB", "francie": "FR",
               "nizozemsko": "NL", "belgie": "BE", "itálie": "IT", "španělsko": "ES",
               "švýcarsko": "CH", "maďarsko": "HU", "izrael": "IL", "švédsko": "SE",
               "dánsko": "DK", "finsko": "FI", "norsko": "NO", "spojené státy": "US"}


def kczk(v: str) -> float | None:
    v = (v or "").replace("\xa0", "").replace(" ", "").replace(",", "")
    try:
        return float(v) * 1000.0
    except ValueError:
        return None


def main() -> None:
    p = argparse.ArgumentParser(description="TA ČR projects from STARFOS -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="smoke test: keep only N projects")
    p.add_argument("--programmes", default=None, help="comma list of programme codes (default: all TA ČR)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/tacr"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache API responses (resumable re-runs)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    global CACHE_DIR
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)
        CACHE_DIR = args.cache_dir

    progs = programmes()
    if args.programmes:
        progs = {c: progs.get(c, c) for c in args.programmes.split(",")}

    frames, dates, expected = [], {}, {}
    for i, (code, label) in enumerate(sorted(progs.items()), 1):
        n = num_found(code)
        expected[code] = n
        df = export(code)
        log(f"[{i}/{len(progs)}] {code} {label[:60]!r}: search num_found={n}, export rows={len(df)}")
        if len(df) < n:
            raise RuntimeError(f"export for {code} returned {len(df)} < num_found {n}; refusing to truncate")
        df["programme_code"] = code
        df["programme_label"] = label
        frames.append(df)
        if args.limit and sum(len(f) for f in frames) >= args.limit:
            break
    raw = pd.concat(frames, ignore_index=True)
    if args.limit:
        raw = raw.head(args.limit)
    log(f"Exported {len(raw)} rows ({len(raw['Project code'].unique())} distinct codes)")

    # exact implementation dates via /search (two passes with different orderings,
    # since equal-score pagination is not guaranteed stable)
    for code in sorted(set(raw["programme_code"])):
        codes = set(raw.loc[raw["programme_code"] == code, "Project code"])
        n = expected[code] if not args.limit else min(expected[code], args.limit)
        dates.update(search_dates(code, n))
        missing = [c for c in codes if c not in dates]
        if missing and not args.limit:
            log(f"  dates {code}: {len(missing)} missing after pass 1; second pass with explicit ordering")
            dates.update(search_dates(code, expected[code], ordering=("x_solve_begin_year", True)))
        log(f"  dates {code}: {sum(1 for c in codes if c in dates)}/{len(codes)}")

    rows = []
    for _, r in raw.iterrows():
        mains = parse_people(r.get("Main project researchers", ""))
        others = parse_people(r.get("Other project researchers", ""))
        org = parse_org(r.get("Main participants", ""))
        country_name = org_country(r.get("Detailed participants' information", ""), org["name"])
        d = dates.get(r["Project code"], (None, None))
        lead = mains[0] if mains else {}
        rows.append({
            "project_code": r["Project code"].strip(),
            "title_en": r.get("Name english") or None,
            "title_cs": r.get("Name czech") or None,
            "description_en": r.get("Annotation english") or None,
            "description_cs": r.get("Annotation czech") or None,
            "provider": r.get("Provider") or None,
            "programme_code": r["programme_code"],
            "programme": r.get("Programme") or r["programme_label"],
            "cep_main": r.get("Main CEP area") or None,
            "ford_main": r.get("Main FORD area") or None,
            "keywords": r.get("Keywords") or None,
            "main_participants_raw": r.get("Main participants") or None,
            "other_participants_raw": r.get("Other participants") or None,
            "participants_detail_raw": r.get("Detailed participants' information") or None,
            "main_researchers_raw": r.get("Main project researchers") or None,
            "other_researchers_raw": r.get("Other project researchers") or None,
            "lead_name_raw": lead.get("name_raw"),
            "lead_given_name": lead.get("given_name"),
            "lead_family_name": lead.get("family_name"),
            "lead_orcid": lead.get("orcid"),
            "lead_vedidk": lead.get("vedidk"),
            "lead_institution": org["name"],
            "lead_institution_ico": org["ico"],
            "lead_country_name": country_name,
            "lead_country": COUNTRY_ISO.get((country_name or "").lower()),
            "investigators_json": json.dumps(mains + others, ensure_ascii=False) if (mains or others) else None,
            "n_main_researchers": len(mains),
            "recognised_costs_czk": kczk(r.get("Recognised costs", "")),
            "public_support_czk": kczk(r.get("Public support", "")),
            "other_public_czk": kczk(r.get("Other public funding", "")),
            "non_public_czk": kczk(r.get("Non public funding", "")),
            "start_year": r.get("Solve beginning") or None,
            "end_year": r.get("Solve end") or None,
            "start_date": d[0],
            "end_date": d[1],
            "landing_page_url": LANDING.format(code=r["Project code"].strip()),
            "starfos_url_old": r.get("Starfos URL") or None,
        })
    df = pd.DataFrame(rows)

    # provider routing guard (§2.3.2): every row must be filed under TA ČR
    bad = df[~df["provider"].fillna("").str.startswith(PROVIDER_PREFIX)]
    if len(bad):
        raise SystemExit(f"{len(bad)} rows not under provider {PROVIDER_PREFIX}: {bad['provider'].value_counts().to_dict()}")
    dupes = df["project_code"].str.lower().duplicated(keep=False)
    if dupes.any():
        log(f"  {dupes.sum()} duplicate-code rows (same project in 2 programmes?): {df.loc[dupes, 'project_code'].tolist()[:10]}")
        df = df.drop_duplicates("project_code", keep="first")

    for c in ["title_en", "title_cs", "description_en", "lead_family_name", "lead_orcid", "lead_institution",
              "lead_country", "public_support_czk", "start_year", "start_date", "end_date"]:
        log(f"  {c:22s} {df[c].notna().mean():6.1%}")
    log(f"  total public support CZK {df['public_support_czk'].sum():,.0f}")
    log(f"  programmes: {df['programme_code'].value_counts().to_dict()}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "tacr_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_tacr_projects.parquet"
    try:  # runbook §1.4: never shrink the corpus on re-ingest
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
