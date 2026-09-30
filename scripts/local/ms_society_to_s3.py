#!/usr/bin/env python3
"""
MS Society (UK) to S3 Data Pipeline (Europe PMC grant list + funder website)
============================================================================

Multiple Sclerosis Society of Great Britain and Northern Ireland (OpenAlex
F4320320084, GB). Two of the funder's own publications, merged:

1. **Europe PMC GRIST deposit** (primary). The MS Society is a Europe PMC
   funder and deposits its grant list there:
   https://www.ebi.ac.uk/europepmc/GristAPI/rest/get/query=ga:"Multiple Sclerosis Society"&resultType=core&format=json
   One record per grant holder: MS Society grant number (``Id``), title,
   abstract (~39%), award type, stream, start/end dates, GBP amount, holder
   name (+ ORCID) and institution with ROR. ~326 records / ~307 grants,
   1999-2020 (the deposit stops at 2020). GRIST paging is unstable, so full
   passes are repeated and unioned until two passes add nothing.

2. **"Search our research projects"** on mssociety.org.uk (supplement):
   https://www.mssociety.org.uk/research/explore-our-research/search-our-research-projects
   ~182 project pages, each with lead researcher, institution, "MS Society
   funding" (GBP) and status, but no grant number and no dates. Pages that
   match a GRIST grant (same lead family name and amount within GBP 2, or an
   identical non-round amount unique in GRIST) only lend their URL as the
   landing page; the rest (mostly 2020+ grants) are added as website-only
   awards keyed ``MSSOC-WEB-<Drupal node id>`` (synthetic; no number is
   published for them).

MS Society publishes no 360Giving file (360Giving registry checked 2026-09-30).

``funder_award_id`` (runbook §2.1.1): GRIST ``Id`` is the MS Society grant
number (float artefacts like ``37.0`` normalised to ``37``). Citing works
write old-scheme grants (numbers 491-996, 1999-2012) as ``NNN/YY`` where YY
is the award-round year, which cannot be derived from the start date (offset
0 to -2 years). So the script crosswalks: when an existing OpenAlex award stub
for F4320320084 carries ``NNN/YY`` for a GRIST number NNN, that exact cited
string is shipped (the TWCF / DNRF acknowledgement-crosswalk pattern), else
the bare number. New-scheme grants (7-130, 2014-2020) are cited as "Grant 76",
"Ref 71" etc.; the bare number is shipped.

Output: s3://openalex-ingest/awards/ms_society/ms_society_projects.parquet
"""

import argparse
import html
import json
import os
import re
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (renamed-sys variant; equivalent to sys.stdout.reconfigure(...) — runbook §1.2)
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). No-op on
# Linux/Databricks. See runbook §1.2.
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

AGENCY = "Multiple Sclerosis Society"
FUNDREF = "10.13039/501100000381"  # MS Society (UK), OpenAlex F4320320084
FUNDER_ID = "F4320320084"
SLUG = "ms_society"
S3_BUCKET = "openalex-ingest"
S3_KEY = f"awards/{SLUG}/{SLUG}_projects.parquet"
GRIST = "https://www.ebi.ac.uk/europepmc/GristAPI/rest/get/query={q}&resultType=core&format=json&page={p}"
WEB = "https://www.mssociety.org.uk"
WEB_LIST = f"{WEB}/research/explore-our-research/search-our-research-projects"
OA_AWARDS = "https://api.openalex.org/awards"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org; team@ourresearch.org)"}
BROWSER_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                 "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"}
REQUEST_DELAY = 0.5
RETRIES = 4
MAX_CONSECUTIVE_EMPTY = 3
QUERY_VARIANTS = ['ga:"Multiple Sclerosis Society"', 'ga:"MS Society"', 'ga:"multiple sclerosis society"', 'ga:"ms society"']
MAX_LIST_PAGES = 60


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, params: dict | None = None, headers: dict | None = None) -> requests.Response:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=headers or HEADERS, timeout=90)
            shown = re.sub(r"api_key=[^&]+", "api_key=***", r.url)
            log(f"  GET {shown[-70:]} -> {r.status_code} ({len(r.content)} bytes)")
            if r.status_code == 404:
                return r
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001
            last_err = re.sub(r"api_key=[^&\s]+", "api_key=***", str(e))
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last_err}")


# ---------------------------------------------------------------- GRIST ----

def fetch_grist_pass(query: str, limit: int | None) -> tuple[int, list[dict]]:
    q = urllib.parse.quote(query)
    records, page, hit_count, empty = [], 1, None, 0
    while True:
        d = get(GRIST.format(q=q, p=page)).json()
        hit_count = int(d.get("HitCount") or 0)
        recs = (d.get("RecordList") or {}).get("Record") or []
        if isinstance(recs, dict):
            recs = [recs]
        if recs:
            empty = 0
            records += recs
        else:
            empty += 1
            log(f"  page {page}: empty ({empty}/{MAX_CONSECUTIVE_EMPTY})")
        if len(records) >= hit_count or empty >= MAX_CONSECUTIVE_EMPTY:
            break
        if limit and len(records) >= limit:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
    if not limit and len(records) < hit_count:
        raise RuntimeError(f"GRIST returned {len(records)} of {hit_count} records; refusing a partial corpus")
    return hit_count, records


def fetch_union(limit: int | None, max_rounds: int = 6) -> list[dict]:
    """GRIST paging is NOT stable: a full pass returns HitCount records but
    repeats some and silently skips others, and repeating the SAME query
    tends to skip the same ones (2026-09-30: three identical passes all
    missed the same 5 grants). Different spellings of the query (case,
    alias) page in a different order, so union full passes over several
    query variants until the distinct-record count reaches HitCount.
    Tolerates a <=1% shortfall (genuinely identical duplicate records can
    never be counted twice); anything bigger raises."""
    if limit:
        return fetch_grist_pass(QUERY_VARIANTS[0], limit)[1]
    seen: dict[str, dict] = {}
    hit_count = 0
    for rnd in range(1, max_rounds + 1):
        for q in QUERY_VARIANTS:
            hit_count, recs = fetch_grist_pass(q, None)
            before = len(seen)
            for r in recs:
                seen.setdefault(json.dumps(r, sort_keys=True, ensure_ascii=False), r)
            log(f"GRIST round {rnd} [{q}]: +{len(seen) - before} -> {len(seen)}/{hit_count} distinct records")
            if len(seen) >= hit_count:
                return list(seen.values())
    if len(seen) >= 0.99 * hit_count:
        log(f"WARNING: {len(seen)}/{hit_count} distinct records after {max_rounds} rounds; proceeding (<=1% short)")
        return list(seen.values())
    raise RuntimeError(f"GRIST: only {len(seen)}/{hit_count} distinct records after {max_rounds} rounds")


def clean(s) -> str | None:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", html.unescape(str(s))).strip()
    return s or None


def norm_id(s: str) -> str:
    return re.sub(r"\.0$", "", s.strip())


def person(rec: dict) -> dict:
    p = rec.get("Person") or {}
    inst = rec.get("Institution") or {}
    aliases = p.get("Alias") or []
    if isinstance(aliases, dict):
        aliases = [aliases]
    orcid = next((a.get("value") for a in aliases
                  if (a.get("Source") or "").upper() == "ORCID"
                  and re.fullmatch(r"\d{4}-\d{4}-\d{4}-\d{3}[\dX]", a.get("value") or "")), None)
    ror = clean(inst.get("RORID"))
    return {
        "given_name": clean(p.get("GivenName")),
        "family_name": clean(p.get("FamilyName")),
        "title": clean(p.get("Title")),
        "orcid": orcid,
        "institution": clean(inst.get("Name")),
        "institution_ror": f"https://ror.org/{ror}" if ror and not ror.startswith("http") else ror,
    }


def funding_type(award_type: str | None) -> str:
    t = (award_type or "").lower()
    if "fellow" in t:
        return "fellowship"
    if "studentship" in t or "phd" in t:
        return "training"
    return "research"


def mojibake_score(s: str | None) -> int:
    return len(re.findall("[√Ãâ�]", s or ""))


def grist_rows(records: list[dict]) -> list[dict]:
    by_id: dict[str, list[dict]] = {}
    for r in records:
        g = r.get("Grant") or {}
        if not ((g.get("Funder") or {}).get("FundRefID") or "").lower().endswith(FUNDREF):
            continue
        gid = clean(g.get("Id"))
        if gid:
            by_id.setdefault(norm_id(gid), []).append(r)
    rows = []
    for gid, recs in by_id.items():
        # several records per number = same grant (co-holders, or a re-deposit
        # with a float id / mis-encoded title): primary = cleanest, fullest one
        recs.sort(key=lambda r: (mojibake_score(r["Grant"].get("Title")),
                                 0 if r["Grant"].get("Abstract") else 1,
                                 0 if r["Grant"].get("StartDate") else 1,
                                 1 if str(r["Grant"].get("Id", "")).endswith(".0") else 0))
        g = recs[0]["Grant"]
        people, seen = [], set()
        for r in recs:
            p = person(r)
            key = ((p["given_name"] or "").lower(), (p["family_name"] or "").lower())
            if key in seen or not p["family_name"]:
                continue
            seen.add(key)
            people.append(p)
        lead = people[0] if people else {}
        amt = g.get("Amount") or {}
        abstract = g.get("Abstract") or {}
        if isinstance(abstract, list):
            abstract = abstract[0] if abstract else {}
        rows.append({
            "grant_id": gid,
            "grist_id_raw": clean(g.get("Id")),
            "title": clean(g.get("Title")),
            "abstract": clean(abstract.get("value")) if isinstance(abstract, dict) else clean(abstract),
            "award_type": clean(g.get("Type")),
            "funding_type": funding_type(g.get("Type")),
            "stream": clean(g.get("Stream")),
            "start_date": clean(g.get("StartDate")),
            "end_date": clean(g.get("EndDate")),
            "amount": amt.get("value"),
            "currency": clean(amt.get("Currency")),
            "lead_given_name": lead.get("given_name"),
            "lead_family_name": lead.get("family_name"),
            "lead_orcid": lead.get("orcid"),
            "lead_institution": lead.get("institution"),
            "lead_institution_ror": lead.get("institution_ror"),
            "people": people,
            "n_source_records": len(recs),
            "source": "europepmc_grist",
        })
    return rows


# ------------------------------------------------------ citation crosswalk ----

def cited_old_scheme_ids() -> dict[str, str]:
    """GRIST number -> cited 'NNN/YY' string, from the existing OpenAlex award
    stubs of F4320320084 (Crossref / Europe PMC / DataCite acknowledgements).
    Only unambiguous numbers (exactly one YY) are kept."""
    found: dict[str, set[str]] = {}
    params = {"filter": f"funder.id:{FUNDER_ID}", "per_page": 200, "cursor": "*",
              "select": "funder_award_id"}
    if os.environ.get("OPENALEX_API_KEY"):
        params["api_key"] = os.environ["OPENALEX_API_KEY"]
    pages = 0
    while params["cursor"] and pages < 100:
        d = get(OA_AWARDS, params=params).json()
        pages += 1
        for a in d.get("results") or []:
            m = re.fullmatch(r"\(?\s*(\d{3})\s*/\s*(\d{2})\s*\)?", (a.get("funder_award_id") or "").strip())
            if m:
                found.setdefault(m.group(1), set()).add(f"{m.group(1)}/{m.group(2)}")
        params["cursor"] = (d.get("meta") or {}).get("next_cursor")
    out = {n: next(iter(v)) for n, v in found.items() if len(v) == 1}
    log(f"Citation crosswalk: {len(out)} unambiguous NNN/YY stubs ({len(found) - len(out)} ambiguous dropped)")
    return out


# --------------------------------------------------------------- website ----

HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|sir|dame|mr|mrs|ms|miss)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) + honorific strip."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "frs", "frse", "fmedsci", "obe", "cbe", "mbe"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_amount(s: str | None) -> float | None:
    """First GBP figure = the MS Society's share ("£370,000. Total cost: ...");
    handles "£12.9 million"."""
    m = re.search(r"(\d[\d,]*(?:\.\d+)?)\s*(million|m\b)?", s or "", re.I)
    if not m:
        return None
    v = float(m.group(1).rstrip(".").replace(",", ""))
    return v * 1_000_000 if m.group(2) else v


def first_person(s: str | None) -> str | None:
    """'Professor Jeremy Chataway & Professor Mahesh Parmar',
    'Professors Anna Williams and David Lyons', 'Dr A, Prof. B, Dr C' ->
    the first-named researcher. 'Multiple researchers' -> None."""
    if not s or re.search(r"multiple researchers", s, re.I):
        return None
    s = re.sub(r"^professors\s+", "Professor ", s.strip(), flags=re.I)
    return re.split(r"\s*(?:&|,|\band\b)\s*", s)[0].strip() or None


def web_projects(limit: int | None, cache_dir: Path | None) -> list[dict]:
    slugs: list[str] = []
    empty = 0
    for page in range(MAX_LIST_PAGES):
        r = get(WEB_LIST, params={"page": page}, headers=BROWSER_HEADERS)
        found = re.findall(r'href="/research/explore-our-research/search-our-research-projects/([^"?#/]+)"', r.text)
        new = [s for s in dict.fromkeys(found) if s not in slugs]
        total = re.search(r"(\d+)\s+results", r.text)
        if not new:
            empty += 1
            log(f"  list page {page}: no new slugs ({empty}/{MAX_CONSECUTIVE_EMPTY})")
            if empty >= MAX_CONSECUTIVE_EMPTY:
                break
        else:
            empty = 0
            slugs += new
        log(f"  list page {page}: {len(slugs)} project slugs (site reports {total.group(1) if total else '?'})")
        if total and len(slugs) >= int(total.group(1)):
            break
        if limit and len(slugs) >= limit:
            break
        time.sleep(REQUEST_DELAY)
    if limit:
        slugs = slugs[:limit]
    out = []
    for i, slug in enumerate(slugs, 1):
        url = f"{WEB_LIST}/{slug}"
        cache = cache_dir / f"{slug[:80]}.html" if cache_dir else None
        if cache and cache.exists():
            t = cache.read_text()
        else:
            t = get(url, headers=BROWSER_HEADERS).text
            if cache:
                cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(t)
            time.sleep(REQUEST_DELAY)
        info = {clean(re.sub(r"<[^>]+>", "", a)): clean(re.sub(r"<[^>]+>", "", b)) for a, b in re.findall(
            r'<li class="text-with-icon-list__item[^"]*">\s*<div>\s*<div>(.*?)</div>\s*<div>(.*?)</div>', t, re.S)}
        h1 = re.search(r"<h1[^>]*>(.*?)</h1>", t, re.S)
        node = re.search(r"flag/bookmark/(\d+)", t)
        desc = re.search(r'<meta name="description" content="([^"]*)"', t)
        lead_raw = info.get("Lead researcher")
        lead = first_person(lead_raw)
        given, family = split_name(lead or "")
        out.append({
            "web_node_id": node.group(1) if node else None,
            "web_url": url,
            "title": clean(re.sub(r"<[^>]+>", "", h1.group(1))) if h1 else None,
            "description": clean(desc.group(1)) if desc else None,
            "lead_name": lead,
            "lead_raw": lead_raw,
            "lead_given_name": given,
            "lead_family_name": family,
            "lead_institution": info.get("Based at"),
            "amount": parse_amount(info.get("MS Society funding")),
            "status": info.get("Status"),
        })
        if i % 25 == 0:
            log(f"  {i}/{len(slugs)} project pages parsed")
    return out


def match_web(web: list[dict], grist: list[dict]) -> None:
    """Attach website pages to GRIST grants in place: the grant gets the page
    URL as landing page, the page is marked matched (not added separately)."""
    by_amount: dict[int, list[dict]] = {}
    for g in grist:
        if g["amount"] is not None:
            by_amount.setdefault(round(g["amount"]), []).append(g)
    for w in web:
        w["matched_grant_id"] = None
        if w["amount"] is None:
            continue
        lead = (w["lead_name"] or "").lower()
        same_pi = [g for g in grist if g["amount"] is not None and g["lead_family_name"]
                   and lead.endswith(g["lead_family_name"].lower())]
        cands = [g for g in same_pi if abs(g["amount"] - w["amount"]) <= 2]
        if not cands:  # same PI, amount revised by <=3% (e.g. 33,960 vs 33,160)
            cands = [g for g in same_pi if abs(g["amount"] - w["amount"]) <= 0.03 * max(g["amount"], 1)]
            for g in cands[:1]:
                log(f"  near-amount match: '{w['title']}' ({w['lead_name']}, {w['amount']}) -> GRIST {g['grant_id']} ({g['amount']})")
        if not cands:
            same = by_amount.get(round(w["amount"]), [])
            if len(same) == 1 and round(w["amount"]) % 1000 != 0:
                cands = same
                log(f"  amount-only match: '{w['title']}' ({w['lead_name']}) -> GRIST {same[0]['grant_id']} ({same[0]['lead_family_name']})")
        if cands:
            g = cands[0]
            w["matched_grant_id"] = g["grant_id"]
            g.setdefault("web_urls", []).append(w["web_url"])


def main() -> None:
    ap = argparse.ArgumentParser(description="MS Society (UK) grants: Europe PMC GRIST + mssociety.org.uk -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="smoke test: ~N GRIST records and N web pages")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache web project HTML here")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = ap.parse_args()

    grist = grist_rows(fetch_union(args.limit))
    log(f"GRIST: {len(grist)} grants")
    xwalk = cited_old_scheme_ids()
    for g in grist:
        g["funder_award_id"] = xwalk.get(g["grant_id"], g["grant_id"])
        g["award_id_source"] = "cited_stub_crosswalk" if g["grant_id"] in xwalk else "grist_number"
    log(f"  {sum(g['award_id_source'] == 'cited_stub_crosswalk' for g in grist)} grants ship the cited NNN/YY form")

    web = web_projects(args.limit, args.cache_dir)
    log(f"Website: {len(web)} project pages")
    match_web(web, grist)
    unmatched = [w for w in web if not w["matched_grant_id"]]
    # Completed projects mostly predate 2021 and so sit in the GRIST deposit
    # (which runs to 2020); an unmatched completed page is more likely a GRIST
    # grant whose amount/PI drifted, or a trial-results page, than a new grant.
    # Only active / approved / status-less (new) pages are added.
    web_only = [w for w in unmatched if (w["status"] or "").lower() not in {"complete", "completed", "closed"}]
    log(f"  {len(web) - len(unmatched)} pages matched to GRIST grants; {len(unmatched)} unmatched, "
        f"{len(unmatched) - len(web_only)} of them completed/closed (skipped), {len(web_only)} added website-only")

    rows = []
    for g in grist:
        urls = g.pop("web_urls", [])
        g["landing_page_url"] = urls[0] if urls else (
            "https://europepmc.org/grantfinder/grantdetails?query="
            + urllib.parse.quote(f'gid:"{g["grist_id_raw"]}" ga:"{AGENCY}"'))
        g["people_json"] = json.dumps(g.pop("people"), ensure_ascii=False)
        rows.append(g)
    for w in web_only:
        if not w["web_node_id"]:
            log(f"  no node id, skipped: {w['web_url']}")
            continue
        lead_person = {"given_name": w["lead_given_name"], "family_name": w["lead_family_name"], "title": None,
                       "orcid": None, "institution": w["lead_institution"], "institution_ror": None}
        rows.append({
            "grant_id": None,
            "funder_award_id": f"MSSOC-WEB-{w['web_node_id']}",
            "award_id_source": "synthetic_web_node",
            "title": w["title"],
            "abstract": w["description"],
            "award_type": None,
            "funding_type": "research",
            "stream": None,
            "start_date": None,
            "end_date": None,
            "amount": w["amount"],
            "currency": "GBP" if w["amount"] is not None else None,
            "lead_given_name": w["lead_given_name"],
            "lead_family_name": w["lead_family_name"],
            "lead_orcid": None,
            "lead_institution": w["lead_institution"],
            "lead_institution_ror": None,
            "people_json": json.dumps([lead_person] if w["lead_family_name"] else [], ensure_ascii=False),
            "n_source_records": 1,
            "web_status": w["status"],
            "landing_page_url": w["web_url"],
            "source": "mssociety_web",
        })
    df = pd.DataFrame(rows)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")
    log(f"Total {len(df)} awards: {df['source'].value_counts().to_dict()}")
    for c in ["title", "abstract", "start_date", "amount", "lead_family_name", "lead_orcid",
              "lead_institution", "lead_institution_ror"]:
        log(f"  {c:22s} {df[c].notna().mean():6.1%}")

    df["amount"] = df["amount"].map(lambda v: None if v is None or pd.isna(v) else repr(float(v)))
    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / f"{SLUG}_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload or args.limit:
        log("Upload skipped")
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / f"_previous_{SLUG}_projects.parquet"
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
