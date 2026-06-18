#!/usr/bin/env python3
"""
BELSPO (Belgian Federal Science Policy Office) awards.

Source: FRIS (Flanders Research Information Space) public SOAP web service
  POST https://frisr4.researchportal.be/ws/ProjectServiceFRIS   (operation getProjects)

FRIS holds ALL Flemish research projects (~93k) across many funders (FWO, BOF,
VLAIO, EU, ...). We keep ONLY projects whose funding **Funding Party** is
"Research Foundation Flanders" (= FWO). IMPORTANT: the string "FWO" also appears
on non-FWO projects via the `fwoDisciplines` classification scheme (a discipline
taxonomy applied to every Flemish project) — so FWO membership is decided by the
funding-organisation, never by a substring match.

Method 2 (XML/SOAP API). Per FWO project FRIS exposes:
  - real FWO grant id          -> funder_award_id   (fundingIdentifier @authority='FWO Contract Id')
  - EN (fallback NL) title     -> display_name
  - EN abstract                -> description
  - funding scheme             -> funder_scheme      (e.g. "FWO junior postdoctoral fellowship")
  - start / end dates          -> start_date/end_date (+ years)
  - promoter / co-promoter     -> lead / co_lead_investigator (given/family are explicit; no name-splitting)
  - host university            -> affiliation.name (country = BE)
amount/currency are NULL: FRIS does not publish a per-project budget for FWO
grants (`nullBudgetAllowed=true`) -> §6.7 amount waiver, never imputed.

Awarding body funder: F4320321730 (Research Foundation - Flanders), BE.
provenance = 'fwo_fris'.
"""
import sys

# --- Fleet UTF-8 compatibility shim (must run before any printing / file I/O) ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass
if sys.platform == "win32":  # default cp1252 crashes on Mihály/Béla/ł/µ etc.
    import builtins
    from pathlib import Path as _P
    _open = builtins.open
    def _utf8_open(f, mode="r", *a, **k):
        if "b" not in mode and k.get("encoding") is None:
            k["encoding"] = "utf-8"
        return _open(f, mode, *a, **k)
    builtins.open = _utf8_open
    _wt, _rt = _P.write_text, _P.read_text
    _P.write_text = lambda self, data, encoding="utf-8", **k: _wt(self, data, encoding=encoding, **k)
    _P.read_text = lambda self, encoding="utf-8", **k: _rt(self, encoding=encoding, **k)

import argparse
import re
import time
import xml.etree.ElementTree as ET
from html import unescape
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# =============================================================================
# Constants
# =============================================================================
WS_URL = "https://frisr4.researchportal.be/ws/ProjectServiceFRIS"
FUNDER_ID = 4320321454            # Belgian Federal Science Policy Office (BELSPO), in openalex.common.funder
# BELSPO grants in FRIS use FED Contract Id primarily; some legacy IDs use AIO Contract Id or other authorities.
# Authority preference (case-insensitive substring match). First match wins; falls back to any non-empty id.
PREFERRED_AUTHORITIES = ["FED", "AIO", "VO", "BOF"]
PROVENANCE = "belspo_fris"
FWO_FUNDER_NAME = "federal government"   # the FRIS Funding-Party org name (lower-cased)
COUNTRY = "BE"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/belspo/belspo_projects.parquet"
PAGE_SIZE = 100
USER_AGENT = "Mozilla/5.0 (compatible; openalex-awards-ingest/1.0)"
REQUEST_DELAY_S = 0.34

_FELLOWSHIP_RE = re.compile(r"fellow|mandate|mandaat|phd|doctoral|postdoc", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")
# XML 1.0 forbids these control chars; FRIS occasionally emits one inside an
# abstract/title, which crashes ET.fromstring mid-harvest if not stripped.
_INVALID_XML_RE = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f]")

_SOAP = (
    '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"'
    ' xmlns:fris="http://fris.ewi.be/"><soapenv:Body><fris:getProjects><criteria>'
    "<window><pageSize>{ps}</pageSize><pageNumber>{pn}</pageNumber>"
    "<orderings><order><id>entity.created</id><locale>en</locale>"
    "<direction>DESCENDING</direction></order></orderings></window>"
    "</criteria></fris:getProjects></soapenv:Body></soapenv:Envelope>"
)

_session: Optional[requests.Session] = None
_last_t = 0.0


def log(msg: str) -> None:
    print(msg, flush=True)


# =============================================================================
# SOAP fetch (hardened: retries transient flakes, never silently truncates)
# =============================================================================
def _post(page_number: int, page_size: int, timeout: int = 90, max_attempts: int = 4) -> str:
    global _session, _last_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": "",
            "User-Agent": USER_AGENT,
        })
    body = _SOAP.format(ps=page_size, pn=page_number).encode("utf-8")
    last_err = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_t
        if elapsed < REQUEST_DELAY_S:
            time.sleep(REQUEST_DELAY_S - elapsed)
        try:
            r = _session.post(WS_URL, data=body, timeout=timeout)
            _last_t = time.monotonic()
            if r.status_code < 500:
                r.raise_for_status()       # 2xx -> ok; 4xx -> permanent, raise
                return _INVALID_XML_RE.sub("", r.text)
            last_err = requests.HTTPError(f"HTTP {r.status_code} (server)")
        except (requests.Timeout, requests.ConnectionError) as e:
            _last_t = time.monotonic()
            last_err = e
        if attempt < max_attempts:
            back = min(20, 2 ** attempt)
            log(f"    transient fetch issue ({type(last_err).__name__}) page {page_number}; "
                f"retry {attempt}/{max_attempts} in {back}s")
            time.sleep(back)
    raise RuntimeError(f"FRIS getProjects failed after {max_attempts} attempts (page {page_number}): {last_err}")


def _strip_ns(elem: ET.Element) -> ET.Element:
    for e in elem.iter():
        if isinstance(e.tag, str) and "}" in e.tag:
            e.tag = e.tag.split("}", 1)[1]
    return elem


def fetch_page(page_number: int, page_size: int) -> tuple[list, int]:
    """Return (list of <project> elements with namespaces stripped, total count)."""
    root = _strip_ns(ET.fromstring(_post(page_number, page_size)))
    # SOAP fault?
    fault = root.find(".//faultstring")
    if fault is not None:
        raise RuntimeError(f"FRIS SOAP fault: {fault.text}")
    qr = root.find(".//queryResult")
    total = int(qr.get("total")) if qr is not None and qr.get("total") else 0
    projects = root.findall(".//project")
    return projects, total


# =============================================================================
# Parsing
# =============================================================================
def _text_en(node: Optional[ET.Element]) -> Optional[str]:
    """From a node carrying <texts><text locale=..>, return EN (fallback first non-empty)."""
    if node is None:
        return None
    texts = node.find("texts")
    if texts is None:
        return None
    best = None
    for t in texts.findall("text"):
        val = (t.text or "").strip()
        if not val:
            continue
        if t.get("locale") == "en":
            return val
        if best is None:
            best = val
    return best


def _clean_html(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = unescape(_TAG_RE.sub(" ", s))
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def _date(s: Optional[str]) -> Optional[str]:
    """'2026-10-01T00:00:00' -> '2026-10-01'; drop sentinel 9999 dates."""
    if not s:
        return None
    d = s[:10]
    if not re.match(r"\d{4}-\d{2}-\d{2}", d) or d.startswith("9999"):
        return None
    return d


def is_fwo_funded(project: ET.Element) -> bool:
    """True iff a projectFunding block names FWO as a Funding Party."""
    for foa in project.iter("fundingOrganisationAssociation"):
        at = foa.find("associationType")
        role = _text_en(at.find("description")) if at is not None else None
        if role and role.strip().lower() == "funding party":
            org = foa.find("organisation")
            name = _text_en(org.find("name")) if org is not None else None
            if name and name.strip().lower() == FWO_FUNDER_NAME:
                return True
    return False


def funding_scheme(project: ET.Element) -> Optional[str]:
    """The EN description of the principal FWO funding code (e.g. 'FWO junior postdoctoral fellowship')."""
    for pf in project.iter("projectFunding"):
        fc = pf.find("fundingCode")
        if fc is None:
            continue
        # only consider FWO-funded codes
        for foa in fc.iter("fundingOrganisationAssociation"):
            org = foa.find("organisation")
            name = _text_en(org.find("name")) if org is not None else None
            if name and name.strip().lower() == FWO_FUNDER_NAME:
                sch = _text_en(fc.find("description"))
                if sch:
                    return sch
    return None


def fwo_grant_id(project: ET.Element) -> Optional[str]:
    """Pull the best fundingIdentifier per PREFERRED_AUTHORITIES order; falls back to any non-empty value."""
    fis = project.find("fundingIdentifiers")
    if fis is None:
        return None
    by_pref = {}
    fallback = None
    for fi in fis.findall("fundingIdentifier"):
        val = (fi.text or "").strip()
        if not val:
            continue
        auth = (fi.get("authority") or "").upper()
        for pref in PREFERRED_AUTHORITIES:
            if pref.upper() in auth:
                by_pref.setdefault(pref, val)
                break
        fallback = fallback or val
    for pref in PREFERRED_AUTHORITIES:
        if pref in by_pref:
            return by_pref[pref]
    return fallback


def participants(project: ET.Element) -> list[tuple[str, Optional[str], Optional[str]]]:
    """Return [(role, given, family), ...] for each person participant."""
    out = []
    ps = project.find("participants")
    if ps is None:
        return out
    for p in ps.findall("participant"):
        at = p.find("associationType")
        role = (_text_en(at.find("description")) if at is not None else "") or ""
        assign = p.find("assignment")
        person = assign.find("person") if assign is not None else None
        nm = person.find("name") if person is not None else None
        if nm is None:
            continue
        given = (nm.findtext("firstName") or "").strip() or None
        family = (nm.findtext("lastName") or "").strip() or None
        if given or family:
            out.append((role.strip(), given, family))
    return out


_LEAD_ROLES = {"promoter", "promotor", "fellow", "phd fellow", "supervisor", "holder", "applicant"}


def parse_project(project: ET.Element) -> Optional[dict]:
    if not is_fwo_funded(project):
        return None
    uuid = project.get("uuid")
    title = _text_en(project.find("name"))
    if not title and not uuid:
        return None
    grant_id = fwo_grant_id(project)
    funder_award_id = grant_id or (f"belspo-{uuid}" if uuid else None)
    if not funder_award_id:
        return None

    parts = participants(project)
    lead = next((p for p in parts if p[0].lower() in _LEAD_ROLES), None)
    if lead is None:
        lead = next((p for p in parts if "co-" not in p[0].lower() and p[0]), None) or (parts[0] if parts else None)
    co = next((p for p in parts if "co-promoter" in p[0].lower() or "co-promotor" in p[0].lower()), None)
    if co is lead:
        co = None

    scheme = funding_scheme(project)
    institution = (project.findtext("dataProvider") or "").strip() or None

    return {
        "funder_award_id": funder_award_id,
        "fwo_grant_id": grant_id,                       # real FWO contract id (may be None on old projects)
        "project_uuid": uuid,
        "title": title,
        "abstract": _clean_html(_text_en(project.find("projectAbstract"))),
        "funder_scheme": scheme,
        "funding_type": "fellowship" if (scheme and _FELLOWSHIP_RE.search(scheme)) else "grant",
        "start_date": _date(project.findtext("startDate")),
        "end_date": _date(project.findtext("endDate")),
        "lead_given_name": lead[1] if lead else None,
        "lead_family_name": lead[2] if lead else None,
        "colead_given_name": co[1] if co else None,
        "colead_family_name": co[2] if co else None,
        "institution_name": institution,
        "country": COUNTRY,
        "landing_page_url": f"https://researchportal.be/en/project/{uuid}" if uuid else None,
    }


# =============================================================================
# Scrape
# =============================================================================
def scrape(limit: Optional[int], max_pages: Optional[int], page_size: int) -> list[dict]:
    log("=" * 60)
    log("Step 1: harvest FWO-funded projects from FRIS (getProjects)")
    log("=" * 60)
    rows: list[dict] = []
    seen_ids: set[str] = set()
    consecutive_empty = 0
    skipped_pages = 0
    MAX_CONSECUTIVE_EMPTY = 3        # an empty page is a flake, not necessarily end-of-corpus
    page = 0
    total = None
    while True:
        if max_pages is not None and page >= max_pages:
            log(f"  [max-pages {max_pages}] stopping")
            break
        try:
            projects, total = fetch_page(page, page_size)
        except ET.ParseError as e:
            skipped_pages += 1
            log(f"  page {page}: XML parse error after sanitize ({e}); skipping this page")
            page += 1
            if total is not None and (page * page_size) >= total:
                break
            continue
        if not projects:
            consecutive_empty += 1
            log(f"  page {page}: 0 projects ({consecutive_empty}/{MAX_CONSECUTIVE_EMPTY} consecutive empty)")
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                break
            page += 1
            continue
        consecutive_empty = 0
        kept = 0
        for pr in projects:
            rec = parse_project(pr)
            if rec is None:
                continue
            fid = rec["funder_award_id"]
            if fid in seen_ids:
                continue   # same project can't recur within a stable snapshot; guard anyway
            seen_ids.add(fid)
            rows.append(rec)
            kept += 1
        log(f"  page {page}: {len(projects)} projects, +{kept} FWO (total kept {len(rows):,} / {total:,})")
        if limit and len(rows) >= limit:
            log(f"  [limit {limit}] reached")
            rows = rows[:limit]
            break
        page += 1
        # natural end of corpus: we've walked every page the server advertised
        if total is not None and (page * page_size) >= total:
            log(f"  reached advertised end of corpus ({total:,} projects)")
            break
    if skipped_pages:
        log(f"  WARNING: skipped {skipped_pages} page(s) with unrecoverable XML errors")
    log(f"  harvested {len(rows):,} FWO-funded project rows")
    return rows


# =============================================================================
# Build DataFrame
# =============================================================================
def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    log("\n" + "=" * 60)
    log("Step 2: build DataFrame")
    log("=" * 60)
    if not rows:
        raise RuntimeError("No FWO rows harvested — aborting (check FRIS / Funding-Party filter).")
    dup = pd.Series([r["funder_award_id"] for r in rows])
    if dup.duplicated().any():
        raise RuntimeError(f"Duplicate funder_award_id values: {dup[dup.duplicated()].tolist()[:5]}")
    df = pd.DataFrame.from_records(rows)
    # --- BELSPO scope (§2.3.2): the FRIS "federal government" funding-party bundles MANY federal
    # bodies (FOD Foreign Affairs / Social Affairs / Economy, fiscal exemptions, KCE, …). Keep only
    # Belgian Federal Science Policy Office (BELSPO) research programmes — the actual awarder. ---
    _sch = df["funder_scheme"].astype("string").fillna("")
    _inc = (r"BELSPO|BRAIN|IUAP|STEREO|FED-tWIN|Science for a sustainable|Science for Policy|"
            r"Society and Future|DWTC|Prodex|AGORA|policy note on drugs|Federal Science Policy|"
            r"POLAR|Antarctica|Teledetection|Research Vessel Belgica|JPI|Belgian Co-op")
    _exc = (r"^FOD |^Ministery |Fund Recuperation|Health Care Knowledge|"
            r"^POD Federal Science Policy Office - General")
    _before = len(df)
    df = df[_sch.str.contains(_inc, case=False, regex=True)
            & ~_sch.str.contains(_exc, case=False, regex=True)].reset_index(drop=True)
    log(f"  BELSPO programme scope: kept {len(df)}/{_before} "
        f"(dropped non-BELSPO federal bodies — FOD/Ministery/fiscal/etc.)")
    n = len(df)
    cov = lambda c: f"{df[c].notna().sum()}/{n} ({df[c].notna().mean()*100:.0f}%)"
    log(f"  rows={n}")
    for c in ("title", "fwo_grant_id", "lead_family_name", "institution_name",
              "start_date", "funder_scheme", "abstract"):
        log(f"    {c:18} {cov(c)}")
    yrs = pd.to_numeric(df["start_date"].str[:4], errors="coerce").dropna()
    if len(yrs):
        log(f"  start_year range: {int(yrs.min())}-{int(yrs.max())}")
    log(f"  funding_type: {df['funding_type'].value_counts().to_dict()}")
    log(f"  distinct real FWO grant ids: {df['fwo_grant_id'].notna().sum()}")
    # §1.2.5 — force string dtype so pyarrow never infers null-heavy cols as int
    df = df.astype("string")
    return df


# =============================================================================
# §1.4 shrink-check + upload
# =============================================================================
def check_no_shrink(df: pd.DataFrame, allow_shrink: bool) -> None:
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(
                f"§1.4 shrink-check FAILED: new {len(df):,} < existing {len(prev):,}. "
                f"Pass --allow-shrink only if this shrink is real."
            )
        log(f"  §1.4 shrink-check OK (new {len(df):,} >= existing {len(prev):,})")
    except SystemExit:
        raise
    except Exception as e:
        log(f"  §1.4 shrink-check: no prior object / not comparable ({type(e).__name__})")


def main() -> None:
    ap = argparse.ArgumentParser(description="FWO (FRIS) awards -> parquet -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/fwo"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="cap kept rows (smoke)")
    ap.add_argument("--max-pages", type=int, default=None, help="cap pages fetched (smoke)")
    ap.add_argument("--page-size", type=int, default=PAGE_SIZE)
    args = ap.parse_args()

    rows = scrape(args.limit, args.max_pages, args.page_size)
    df = build_dataframe(rows)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "belspo_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"\n  wrote {len(df):,} rows -> {out} ({out.stat().st_size//1024} KB)")

    if args.skip_upload:
        log("  --skip-upload: not uploading to S3")
        log(f"  manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
        return
    check_no_shrink(df, args.allow_shrink)
    import boto3
    boto3.client("s3").upload_file(str(out), S3_BUCKET, S3_KEY)
    log(f"  uploaded -> s3://{S3_BUCKET}/{S3_KEY}")


if __name__ == "__main__":
    main()
