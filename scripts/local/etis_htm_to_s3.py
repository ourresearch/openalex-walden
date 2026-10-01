#!/usr/bin/env python3
"""
ETIS -> Estonian Ministry of Education and Research (HTM) awards -> S3
=====================================================================

Estonia's national research information system ETIS (www.etis.ee) carries every
Estonian R&D project with its financier(s). Its key-less "ETIS2 Open Data" REST
API (https://www.etis.ee:7443/, landing page titled "ETIS2 Open Data POST
request test") returns the full project register as JSON:

    count: https://www.etis.ee:7443/api/project/getcount?Format=json&SearchType=3
    items: https://www.etis.ee:7443/api/project/getitems?Format=json&SearchType=3
           &Take={n}&Skip={offset}            (~26,600 projects, 2026-10-01)

ETIS aggregates ALL Estonian funders, so this script keeps only the projects the
**Ministry of Education and Research** (Haridus- ja Teadusministeerium, HTM;
OpenAlex F4320321976) financed, routed by `FinancingInstitutions[].Name`
(runbook §2.3.2). The two research councils already have their own ingests from
the same API (scripts/local/etis_to_s3.py): Estonian Research Council / ETAg
(F4320321090, provenance etis_etag) and Estonian Science Foundation / ETF
(F4320321018, provenance etis_etf).

Routing of projects with an HTM financier (every rule is logged):
  * HTM is the only financier, or co-finances with other Estonian bodies
    -> HTM (amount = HTM's own TotalFinancing / Proportion share when ETIS
       gives one, else the project total for sole-financier projects, else NULL)
  * also financed by ETAg or ETF -> NOT routed here (already in etis_etag /
    etis_etf)
  * also financed by the European Commission or an EU executive agency (Horizon,
    CEF, ...) -> NOT routed here (EU-led)
  * another financier has a larger published Proportion -> NOT routed here

Roof/sub-projects: Centres of Excellence (TKnnn) and some consortia have a roof
record plus one record per partner (TKnnnUk) whose amounts sum to the roof's.
All records are shipped (partners carry their own PI/institution), but partner
records under a shipped roof have amount NULL (own share kept in
project_total_eur) so the ministry's total is not double counted.

Scope (Kyle 2026-10-01: keep every research pathway, drop only money that is
clearly not research): every routed project is written to the parquet with an
`in_scope` flag. ETIS project type "Õppearendusprojekt" (teaching/study-
development project: adult-education courses, preliminary vocational training
in gymnasiums, assistant-teacher training, youth competitions) and the three
"Muu"-type rows under the generic ministry programme 200 (EdTech Hack, assistant
teachers) are in_scope = false. Research-infrastructure grants (Taristuprojekt:
equipment purchase support AP, TAP modernisation) and government-commissioned
studies (Riigiasutuse tellimus) are KEPT. One teaching-development row that is a
research survey (EU Kids Online Survey in Estonia) is kept by explicit override.

funder_award_id (§2.1.1): ETIS `FinancierProjectNr` is the number citing works
write (e.g. targeted-financing themes SF0180089s08, Centres of Excellence TK117,
national programme EKKM14-300). It is used when it looks like an identifier
(no whitespace/lists, not a bare year); a leading "Nr"/"Lepingu nr" is stripped.
When the number is missing, free text (contract/decision registrations such as
"EMÜ nõukogu otsus 26.01.2018 nr 1-4/10", or a bare word like
"Töövõtuleping"), or shared by several in-scope ETIS
records, the record gets the synthetic key HTM-ETIS-{ETIS project GUID} (for a
shared number, the record with the largest amount keeps the number). The raw
number is kept in `financier_project_nr`.

Output: s3://openalex-ingest/awards/etis_htm/etis_htm_projects.parquet
"""

import argparse
import gzip
import json
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22; TWCF form) ---
# Equivalent of sys.stdout.reconfigure(encoding="utf-8", ...) plus utf-8
# defaults for Path.write_text / read_text / open on Windows. See runbook §1.2.
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

BASE = "https://www.etis.ee:7443/api/project"
PORTAL = "https://www.etis.ee/Portal/Projects/Display/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/etis_htm/etis_htm_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
PAGE_SIZE = 500
MAX_CONSECUTIVE_FAIL = 6

# Financier-name matching (ETIS `Name`, Estonian). Variants seen 2026-10-01:
# "Haridus- ja Teadusministeerium" (2,228), "Haridus-ja Teadusministeerium" (4),
# "Hariduse- ja teadusministeerium" (1), plus two combined strings
# ("Riigi Tugiteenuste Keskus; Eesti Haridus- ja Teadusministeerium", ...).
HTM_RE = re.compile(r"haridus(e)?-\s?ja\s+teadusministeerium", re.I)
FOREIGN_MINISTRY_RE = re.compile(r"saksamaa|thüringi|soome", re.I)  # German/Finnish ministries
ETAG_ETF_RE = re.compile(r"^(sihtasutus\s+)?eesti\s+teadus(agentuur|fond)\b", re.I)
EU_DIRECT_RE = re.compile(r"euroopa komisjon|european commission|executive agency|rakendusamet|cost office", re.I)

NON_RESEARCH_TYPES = {"Õppearendusprojekt"}           # teaching/study-development projects
KEEP_OVERRIDE = {"EU Kids Online Survey in Estonia"}    # research survey filed as teaching-development
PLACEHOLDER_TITLES = {"lisada"}                         # Estonian "to add": English title never filled in

FUNDING_TYPE = {
    "Teadus- ja arendusprojekt": "research",
    "Taristuprojekt": "infrastructure",
    "Riigiasutuse tellimus": "contract",
    "Õppearendusprojekt": "education",
}

_DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
_NR_PREFIX_RE = re.compile(r"^(lepingu\s+nr\.?|leping\s+nr\.?|leping\s+number|nr\.?|no\.?)\s*", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return None if s.lower() in {"", "nan", "none", "n/a", "-", "0"} else s


def iso(v):
    m = _DATE_RE.search(v or "")
    return f"{int(m.group(3)):04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}" if m else None


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical helper, verbatim from scripts/local/wolf_to_s3.py (runbook §2.4.1)."""
    if not name:
        return None, None
    # Drop trailing degree/suffix tokens
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


# ---------------------------------------------------------------- fetch
def fetch_all(session, limit=None) -> list:
    count = session.get(f"{BASE}/getcount?Format=json&SearchType=3", timeout=60).json()["Count"]
    log(f"ETIS reports {count} projects")
    target = min(count, limit) if limit else count
    docs, skip, fails = [], 0, 0
    while skip < target:
        take = min(PAGE_SIZE, target - skip)
        url = f"{BASE}/getitems?Format=json&SearchType=3&Take={take}&Skip={skip}"
        try:
            r = session.get(url, timeout=180)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            page = r.json()
        except Exception as e:  # noqa: BLE001 - flake: retry the same page, never treat as EOF
            fails += 1
            log(f"  skip={skip}: {e} ({fails}/{MAX_CONSECUTIVE_FAIL}); retrying")
            if fails >= MAX_CONSECUTIVE_FAIL:
                raise RuntimeError(f"ETIS fetch failed {fails}x in a row at skip={skip}")
            time.sleep(5 * fails)
            continue
        fails = 0
        if not page:
            # empty page before the reported total is a flake, not EOF (runbook §1)
            raise RuntimeError(f"empty page at skip={skip} < reported count {target}")
        docs.extend(page)
        skip += take
        log(f"  fetched {len(docs)}/{target}")
        time.sleep(0.5)
    guids = {d.get("Guid") for d in docs}
    if len(guids) != len(docs):
        raise RuntimeError(f"pagination overlap: {len(docs)} docs but {len(guids)} distinct GUIDs")
    return docs


# ---------------------------------------------------------------- routing
def is_htm(fi) -> bool:
    n = fi.get("Name") or ""
    return bool(HTM_RE.search(n)) and not FOREIGN_MINISTRY_RE.search(n)


def route(doc) -> tuple[str, dict | None]:
    """-> (route_label, htm_financier_record or None)."""
    fins = doc.get("FinancingInstitutions") or []
    htm = [f for f in fins if is_htm(f)]
    if not htm:
        return "not_htm", None
    others = [f for f in fins if not is_htm(f)]
    if any(ETAG_ETF_RE.search((f.get("Name") or "").strip()) for f in others):
        return "etag_etf_cofinanced", htm[0]
    if any(EU_DIRECT_RE.search(f"{f.get('Name') or ''} {f.get('NameEng') or ''}") for f in others):
        return "eu_cofinanced", htm[0]
    hp = htm[0].get("Proportion")
    if hp is not None and any((f.get("Proportion") or 0) > hp for f in others):
        return "other_majority", htm[0]
    return "htm", htm[0]


def responsible(persons) -> list[str]:
    names = [clean(p.get("Name")) for p in (persons or [])
             if (p.get("RoleNameEng") or "").strip().lower() in ("responsible executor", "principal investigator")]
    out = []
    for n in names:
        if n and n not in out:
            out.append(n)
    return out


def coordinator(insts):
    insts = insts or []
    for i in insts:
        if (i.get("RoleNameEng") or "").lower() == "coordinator":
            return i
    return insts[0] if insts else {}


def nr_candidate(raw):
    s = clean(raw)
    if not s:
        return None
    s = _NR_PREFIX_RE.sub("", s).strip()
    if not s or re.search(r"[\s;,]", s) or len(s) > 40:
        return None
    if re.fullmatch(r"\d{1,4}", s):  # bare year / tiny registry number, not an award id
        return None
    if not re.search(r"\d", s):  # a word, e.g. "Töövõtuleping" (= contract for services)
        return None
    return s


def to_record(doc, route_label, htm_fi):
    fins = doc.get("FinancingInstitutions") or []
    total = doc.get("FinancingInPeriodsTotal")
    total = float(total) if isinstance(total, (int, float)) and total > 0 else None
    htm_total = htm_fi.get("TotalFinancing")
    htm_prop = htm_fi.get("Proportion")
    if isinstance(htm_total, (int, float)) and htm_total > 0:
        amount, amount_basis = float(htm_total), "htm_total_financing"
    elif len(fins) == 1:
        amount, amount_basis = total, "project_total"
    elif htm_prop and total:
        amount, amount_basis = round(total * htm_prop / 100.0, 2), "project_total_x_htm_proportion"
    else:
        amount, amount_basis = None, "cofinanced_share_unknown"

    pis = responsible(doc.get("Persons"))
    split = [split_name(n) for n in pis]
    inst = coordinator(doc.get("Institutions"))
    regno = clean(inst.get("BusinessRegNo"))
    ptype = clean(doc.get("ProjectTypeNew")) or clean(doc.get("ProjectType"))
    title_en = clean(doc.get("TitleEng"))
    if title_en and title_en.lower() in PLACEHOLDER_TITLES:
        title_en = None
    in_scope = not (ptype in NON_RESEARCH_TYPES or (ptype == "Muu" and clean(doc.get("ProgrammeCode")) == "200"))
    if title_en in KEEP_OVERRIDE:
        in_scope = True
    roof = next((r.get("Guid") for r in (doc.get("RoofProjects") or []) if r.get("Guid") != doc["Guid"]), None)
    return {
        "etis_guid": doc["Guid"],
        "roof_guid": roof,
        "route": route_label,
        "in_scope": in_scope,
        "financier_project_nr": clean(doc.get("FinancierProjectNr")),
        "nr_candidate": nr_candidate(doc.get("FinancierProjectNr")),
        "title": title_en or clean(doc.get("Title")),
        "title_et": clean(doc.get("Title")),
        "description": clean(doc.get("AnnotationEng")) or clean(doc.get("Annotation")),
        "acronym": clean(doc.get("Acronym")),
        "programme_code": clean(doc.get("ProgrammeCode")),
        "programme_name": clean(doc.get("ProgrammeNameEng")) or clean(doc.get("ProgrammeName")),
        "project_type": ptype,
        "project_type_en": clean(doc.get("ProjectTypeNewEng")) or clean(doc.get("ProjectTypeEng")),
        "funding_type": FUNDING_TYPE.get((ptype or "").split(";")[0].strip(), "grant"),
        "start_date": iso(doc.get("ProjectStartDate")),
        "end_date": iso(doc.get("ProjectEndDate")),
        "amount": amount,
        "amount_basis": amount_basis,
        "project_total_eur": total,
        "currency": "EUR" if amount else None,
        "financiers_json": json.dumps([{"name": f.get("Name"), "name_en": f.get("NameEng"),
                                        "proportion": f.get("Proportion"), "total": f.get("TotalFinancing")}
                                       for f in fins], ensure_ascii=False),
        "lead_name": pis[0] if pis else None,
        "lead_given_name": split[0][0] if split else None,
        "lead_family_name": split[0][1] if split else None,
        "co_lead_name": pis[1] if len(pis) > 1 else None,
        "co_lead_given_name": split[1][0] if len(split) > 1 else None,
        "co_lead_family_name": split[1][1] if len(split) > 1 else None,
        "responsible_executors_json": json.dumps(
            [{"name": n, "given_name": g, "family_name": f} for n, (g, f) in zip(pis, split)], ensure_ascii=False),
        "institution": clean(inst.get("NameEng")) or clean(inst.get("Name")),
        "institution_head": clean(inst.get("HeadInstitutionNameEng")) or clean(inst.get("HeadInstitutionName")),
        "institution_country": "EE" if regno and re.fullmatch(r"\d{8}", regno) else None,
        "research_area": clean(((doc.get("ResearchAreasFrascati") or [{}])[0] or {}).get("NameEng")),
        "landing_page_url": PORTAL + doc["Guid"],
        "date_modified": clean(doc.get("DateModified")),
    }


def assign_award_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Citable number where unique among in-scope rows; else synthetic HTM-ETIS-{guid}."""
    df = df.copy()
    df["funder_award_id"] = None
    scope = df["in_scope"]
    key = df["nr_candidate"].str.lower()
    order = df.assign(_amt=df["amount"].fillna(-1)).sort_values(
        ["_amt", "start_date", "etis_guid"], ascending=[False, True, True])
    seen = set()
    shared = 0
    for idx in order.index:
        k = key[idx]
        if not scope[idx] or k is None or (isinstance(k, float)):
            continue
        if k in seen:
            shared += 1
            continue
        seen.add(k)
        df.at[idx, "funder_award_id"] = df.at[idx, "nr_candidate"]
    miss = df["funder_award_id"].isna()
    df.loc[miss, "funder_award_id"] = "HTM-ETIS-" + df.loc[miss, "etis_guid"]
    df["award_id_kind"] = ["synthetic" if a.startswith("HTM-ETIS-") else "financier_project_nr"
                           for a in df["funder_award_id"]]
    log(f"  award ids: {int((df['award_id_kind'] == 'financier_project_nr').sum())} FinancierProjectNr, "
        f"{int((df['award_id_kind'] == 'synthetic').sum())} synthetic ({shared} because the number is shared)")
    return df


# ---------------------------------------------------------------- main
def main() -> None:
    p = argparse.ArgumentParser(description="ETIS -> Ministry of Education and Research projects -> S3")
    p.add_argument("--limit", type=int, default=None, help="only fetch the first N ETIS projects (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/etis_htm"))
    p.add_argument("--cache", type=Path, default=None, help="gzip JSON of the raw ETIS pull (read if present, else written)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.cache and args.cache.exists():
        docs = json.load(gzip.open(args.cache, "rt", encoding="utf-8"))
        log(f"Loaded {len(docs)} ETIS projects from cache {args.cache}")
        if args.limit:
            docs = docs[: args.limit]
    else:
        s = requests.Session()
        s.headers.update(HEADERS)
        docs = fetch_all(s, args.limit)
        if args.cache and not args.limit:
            json.dump(docs, gzip.open(args.cache, "wt", encoding="utf-8"), ensure_ascii=False)
            log(f"Cached raw pull to {args.cache}")

    # Financier census (lessons: who else ETIS covers)
    fin = Counter()
    for d in docs:
        for f in {(fi.get("NameEng") or fi.get("Name") or "?").strip() for fi in (d.get("FinancingInstitutions") or [])}:
            fin[f] += 1
    log("Top financiers (projects):")
    for n, c in fin.most_common(15):
        log(f"  {c:>6}  {n}")

    routes = Counter()
    rows = []
    for d in docs:
        label, htm_fi = route(d)
        routes[label] += 1
        if label == "htm":
            rows.append(to_record(d, label, htm_fi))
    log(f"Routing: {dict(routes)}")
    if not rows:
        raise SystemExit("no HTM-financed projects found")

    df = pd.DataFrame(rows)
    # Roof/sub-project structures (Centres of Excellence TKnnn -> TKnnnUk partner
    # records, HARTA consortia): the roof's amount is the sum of its partners'
    # shares. When the roof itself is shipped, the partner records keep their own
    # share only in project_total_eur and ship amount NULL, so funder totals are
    # not double counted.
    shipped_amt = {g: a for g, a, sc in zip(df["etis_guid"], df["amount"], df["in_scope"]) if sc and a}
    in_roof = df["roof_guid"].map(lambda g: g in shipped_amt if g else False) & df["in_scope"]
    df.loc[in_roof, "amount"] = None
    df.loc[in_roof, "currency"] = None
    df.loc[in_roof, "amount_basis"] = "included_in_roof_project"
    log(f"Sub-project records whose amount is already in a shipped roof project: {int(in_roof.sum())}")
    log(f"HTM-routed projects: {len(df)}; in_scope {int(df['in_scope'].sum())}, "
        f"out of scope {int((~df['in_scope']).sum())} (teaching-development / education delivery)")
    df = assign_award_ids(df)
    ins = df[df["in_scope"]]
    dup = ins["funder_award_id"].str.lower().duplicated(keep=False)
    if dup.any():
        raise SystemExit(f"duplicate funder_award_id among in-scope rows: {ins.loc[dup, 'funder_award_id'].tolist()}")
    log(f"In-scope: {len(ins)} rows, years "
        f"{ins['start_date'].dropna().str[:4].min()}-{ins['start_date'].dropna().str[:4].max()}")
    for c in ["title", "description", "amount", "start_date", "end_date", "lead_family_name", "institution"]:
        log(f"  {c:18s} {ins[c].notna().mean():6.1%}")
    log(f"  amount basis: {ins['amount_basis'].value_counts().to_dict()}")
    log(f"  total EUR {ins['amount'].astype(float).sum():,.0f}")
    log(f"  project types: {ins['project_type'].value_counts().to_dict()}")

    df = df.astype("string")
    parquet_path = args.output_dir / "etis_htm_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload or args.limit:
        log("Upload skipped" + (" (--limit run)" if args.limit else ""))
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_etis_htm_projects.parquet"
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
