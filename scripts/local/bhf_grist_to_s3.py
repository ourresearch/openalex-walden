#!/usr/bin/env python3
"""
British Heart Foundation to S3 Data Pipeline (Europe PMC funder-deposited grant list)
=====================================================================================

British Heart Foundation (OpenAlex F4320319992, GB) is a Europe PMC funder: it
deposits its own research-grant list with Europe PMC, which serves it through
the GRIST grants API:

    https://www.ebi.ac.uk/europepmc/GristAPI/rest/get/query=ga:"British Heart Foundation"&resultType=core&format=json&page=N

This is the funder's own bulk export (ladder item 0), and BHF itself pointed us
to it (Zendesk 24588, oxjob #1357: "Europe PMC's grant finder may be a richer
and more sustainable source of BHF award information than the current
website-based PDF listings"). One record per grant holder with the BHF grant
reference (``Id``, e.g. ``RG/18/13/33946``, ``FS/CRTF/25/24768``), title,
abstract, start/end dates, amount + currency, the holder's structured
given/family name and title (no degree suffixes) and the administering
institution with its ROR id. 5,389 records / 5,122 grants, 1990-2025 (2026-11-16):
abstract 42%, start/end dates 79%, amount 80%, institution ROR 78%, lead ORCID 22%
(most person Aliases are BHF's own person number, not an ORCID).

It supersedes the annual-report PDF parse (bhf_to_s3.py, provenance
bhf_annual_reports, priority 395) wherever both carry a reference: the PDF
listing has no dates, no abstracts, and PI names with degree suffixes glued on
(G2424152919: family_name "FFPH"). It sits ABOVE 395 in sources.csv so it wins
the metadata election; the PDF rows still supply the references GRIST lacks.

Records sharing a grant ``Id`` are the same grant with several holders
(co-applicants); they are collapsed to one row, holders kept in order.

``funder_award_id`` (runbook §2.1.1): the BHF reference is exactly what citing
works carry (Europe PMC grant list and Crossref funder metadata on
F4320319992: ``RG/18/13/33946``, ``FS/19/53/34887``), so it is shipped as-is.
GRIST ``Grant.Alias`` equals ``Id`` except on 35 2010 grants, where it also lists
the zero-padded form (``PG/10/040/28369`` for ``PG/10/40/28369``); ``Id`` is used.

Same grant, different reference (bhf_grist_pdf_refs.csv, next to this script; 258 rows, built 2026-11-16 from the
live API): the 2006/07 annual-report PDFs print 251 references without the serial (``PG/06/145`` for GRIST's
``PG/06/145/21955``) and the PDF parse garbled 7 more (``FS/14/18/130711`` for ``FS/14/18/30711``). Shipped as GRIST's
reference, each would mint a second direct award beside the PDF one. So those GRIST rows ship under the PDF's
reference: same staging id, same award entity (G id unchanged), GRIST's metadata wins. ``grant_id`` keeps GRIST's
reference. The PDF source is frozen, so the list never needs a refresh.

Fields GRIST lacks keep the PDF's value (bhf_grist_pdf_fallback.csv, next to this script: the 4,132 PDF records as
the API served them on 2026-11-16; frozen source, so static). The awards build elects one whole record per award, so
without it GRIST winning would blank fields the PDF has: on the 3,829 shared grants GRIST lacks the start date on 213,
the amount on 171, the institution on 297 and any scheme on 183. Filled from the PDF only where GRIST is empty: amount
+ currency, the lead holder's institution name, fallback_start_year / fallback_end_year (the mapping takes them when
GRIST has no date), and the scheme name when neither GRIST's Type nor the prefix map gives one.

About 40% of BHF GRIST records carry no Type: ``award_type`` (-> funder_scheme) then
comes from the reference's scheme prefix, named as the PDFs name it (SCHEMES below);
``funding_type`` always comes from the prefix (PhD -> training, other FS and CH ->
fellowship, else research).

Output: s3://openalex-ingest/awards/bhf/bhf_grist_projects.parquet
"""

import argparse
import json
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

AGENCY = "British Heart Foundation"
FUNDREF = "10.13039/501100000274"  # British Heart Foundation, OpenAlex F4320319992
SLUG = "bhf"
S3_BUCKET = "openalex-ingest"
S3_KEY = f"awards/{SLUG}/{SLUG}_grist_projects.parquet"   # bhf_projects.parquet is the PDF parse; never overwrite it
GRIST = "https://www.ebi.ac.uk/europepmc/GristAPI/rest/get/query={q}&resultType=core&format=json&page={p}"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org; team@ourresearch.org)"}
REQUEST_DELAY = 0.5
RETRIES = 4
MAX_CONSECUTIVE_EMPTY = 3
QUERY_VARIANTS = ['ga:"British Heart Foundation"', 'ga:"british heart foundation"',
                  'ga:"BRITISH HEART FOUNDATION"', 'ga:"British heart foundation"']


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get_json(url: str) -> dict:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            log(f"  GET {url[-60:]} -> {r.status_code} ({len(r.content)} bytes)")
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last_err}")


def fetch_grist(query: str, limit: int | None) -> tuple[int, list[dict]]:
    """Page through GRIST until the reported HitCount is reached. Empty pages
    before that are logged and skipped (runbook Step 1: empty page != EOF)."""
    q = urllib.parse.quote(query)
    records, page, hit_count, empty = [], 1, None, 0
    while True:
        d = get_json(GRIST.format(q=q, p=page))
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
        log(f"  page {page}: {len(records)}/{hit_count} records")
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
    Tolerates a <=2% shortfall (GRIST ordering varies over time: on
    2026-09-30 one Diabetes UK run reached 587/587 in 3 passes, a later one
    stalled at 578/587 after 24; the §1.4 shrink guard then refuses to
    overwrite a fuller earlier upload). Genuinely identical records can
    never be counted twice); anything bigger raises."""
    if limit:
        return fetch_grist(QUERY_VARIANTS[0], limit)[1]
    seen: dict[str, dict] = {}
    hit_count = 0
    for rnd in range(1, max_rounds + 1):
        for q in QUERY_VARIANTS:
            hit_count, recs = fetch_grist(q, None)
            before = len(seen)
            for r in recs:
                seen.setdefault(json.dumps(r, sort_keys=True, ensure_ascii=False), r)
            log(f"GRIST round {rnd} [{q}]: +{len(seen) - before} -> {len(seen)}/{hit_count} distinct records")
            if len(seen) >= hit_count:
                return list(seen.values())
    if len(seen) >= 0.98 * hit_count:
        log(f"WARNING: {len(seen)}/{hit_count} distinct records after {max_rounds} rounds; proceeding (<=2% short; the §1.4 shrink guard still refuses a smaller corpus than the last upload)")
        return list(seen.values())
    raise RuntimeError(f"GRIST: only {len(seen)}/{hit_count} distinct records after {max_rounds} rounds")


def clean(s) -> str | None:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s or None


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


# BHF scheme codes -> scheme name, for the ~40% of GRIST records with no Type. Names are GRIST's own Type for that
# prefix (the dominant Type among the prefix's typed records, 2026-11-16), so typed and untyped records read alike;
# the last few (no typed GRIST record) use the annual-report PDFs' name in GRIST's singular style (oxjob #1357
# work/scheme_by_prefix.tsv). Bare "FS" is left out on purpose: it covers studentships and many fellowship schemes.
SCHEMES = {
    "PG": "Project Grant", "PG/NC3RS": "Project Grant", "RG": "Programme Grant", "RG/F": "Programme Grant",
    "SP": "Special Project", "SP/F": "Special Project", "SP/J": "Special Project", "CH": "Personal Chair",
    "CH/F": "Personal Chair", "RE": "Research Excellence", "CS": "Clinical Study", "CS/F": "Clinical Study",
    "NH": "New Horizons Grant", "NH/F": "New Horizons Grant", "IG": "Infrastructure Grant", "IG/F": "Infrastructure Grant",
    "TG": "Translational Grant", "TA/F": "Translational Award", "SI": "Strategic Initiative", "SI/F": "Strategic Initiative",
    "RM": "Regenerative Medicine Centre", "AA": "Accelerator Award", "CC": "Cardiovascular Catalyst Award",
    "BBC": "Big Beat Challenge", "BBC/F": "Big Beat Challenge", "IA/F": "International Award",
    "FS/PHD": "Non-clinical PhD Studentship", "FS/4YPHD": "4-year PhD Studentship", "FS/4YPHD/F": "4-year PhD Studentship",
    "FS/MBPHD": "MBPhD Studentship", "FS/CRTF": "Clinical Research Training Fellowship",
    "FS/ICRF": "Intermediate Clinical Research Fellowship", "FS/SCRF": "Senior Clinical Research Fellowship",
    "FS/IBSRF": "Intermediate Basic Science Research Fellowship",
    "FS/IPBSRF": "Immediate Postdoctoral Basic Science Research Fellowship",
    "FS/SBSRF": "Senior Basic Science Research Fellowship", "FS/CRA": "Consultant Research Award",
    "FS/RTF": "Research Training Fellowship (for Nurses & Healthcare Professionals)",
    "FS/CRERF": "Career Re-Entry Research Fellowship", "FS/CDRF": "Career Development Research Fellowship (for Nurses & Healthcare Professionals)",
    "FS/CRLF": "Clinical Research Leave Fellowship", "FS/TF": "Travel Fellowship", "FS/ATA": "Advanced Training Award",
}


def scheme_prefix(gid: str) -> str:
    """'FS/CRTF/25/24768' -> 'FS/CRTF'; 'RG/F/24/110147' -> 'RG/F'; 'PG/24/11945' -> 'PG'."""
    parts = gid.upper().split("/")
    if len(parts) > 1 and not parts[1].isdigit():
        return parts[0] + "/" + parts[1]
    return parts[0]


def award_type(gid: str) -> str | None:
    pre = scheme_prefix(gid)
    return SCHEMES.get(pre) or SCHEMES.get(pre.split("/")[0]) if pre != "FS" else None


def funding_type(gid: str, title: str | None) -> str:
    pre, t = scheme_prefix(gid), (title or "").lower()
    if "PHD" in pre or "studentship" in t:
        return "training"
    if pre.startswith("FS") or pre == "CH":
        return "fellowship"
    return "research"


def pdf_refs() -> dict[str, str]:
    """GRIST reference -> the PDF parse's reference for the same grant (see the module docstring)."""
    import csv
    path = Path(__file__).with_name("bhf_grist_pdf_refs.csv")
    with open(path, encoding="utf-8", newline="") as f:
        return {r["grist_ref"].strip().upper(): r["pdf_ref"].strip() for r in csv.DictReader(f)}


CLEAN_SCHEME = re.compile(r"^[A-Za-z0-9 ()&,'/-]{4,100}(Studentships?|Fellowships?|Grants?|Awards?|Chairs?|Lectureships?"
                          r"|Centres?|Initiatives?|Excellence|Challenge|Seed Funding|Professionals\))$")


def pdf_fallback() -> dict[str, dict]:
    """lower(PDF reference) -> the PDF record's served values (see the module docstring)."""
    import csv
    path = Path(__file__).with_name("bhf_grist_pdf_fallback.csv")
    with open(path, encoding="utf-8", newline="") as f:
        return {r["pdf_ref"].strip().lower(): r for r in csv.DictReader(f)}


def build_rows(records: list[dict]) -> list[dict]:
    to_pdf = pdf_refs()
    fallback = pdf_fallback()
    filled = {"amount": 0, "institution": 0, "start_year": 0, "end_year": 0, "scheme": 0}
    by_id: dict[str, list[dict]] = {}
    dropped = 0
    for r in records:
        g = r.get("Grant") or {}
        fr = ((g.get("Funder") or {}).get("FundRefID") or "").lower()
        if not fr.endswith(FUNDREF):
            dropped += 1
            continue
        gid = clean(g.get("Id"))
        if gid:
            by_id.setdefault(gid, []).append(r)
    if dropped:
        log(f"  dropped {dropped} records with another FundRef id")
    rows = []
    for gid, recs in by_id.items():
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
        q = urllib.parse.quote(f'gid:"{gid}" ga:"{AGENCY}"')
        rows.append({
            "grant_id": gid,
            "funder_award_id": to_pdf.get(gid.upper(), gid),
            "title": clean(g.get("Title")),
            "abstract": clean(abstract.get("value")) if isinstance(abstract, dict) else clean(abstract),
            "award_type": clean(g.get("Type")) or award_type(gid),
            "funding_type": funding_type(gid, g.get("Title")),
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
            "people_json": json.dumps(people, ensure_ascii=False),
            "n_people": len(people),
            "n_source_records": len(recs),
            "landing_page_url": f"https://europepmc.org/grantfinder/grantdetails?query={q}",
            "source": "europepmc_grist",
            "fallback_start_year": None,
            "fallback_end_year": None,
        })
        row, fb = rows[-1], fallback.get(row_key(rows[-1]))
        if fb:   # GRIST wins the award; keep the PDF's value only where GRIST has none
            if row["amount"] is None and fb["amount"]:
                row["amount"], row["currency"] = float(fb["amount"]), fb["currency"] or "GBP"
                filled["amount"] += 1
            if not row["lead_institution"] and fb["institution"] and people:
                people[0]["institution"] = row["lead_institution"] = fb["institution"]
                row["people_json"] = json.dumps(people, ensure_ascii=False)
                filled["institution"] += 1
            if not row["start_date"] and fb["start_year"]:
                row["fallback_start_year"] = fb["start_year"]
                filled["start_year"] += 1
            if not row["end_date"] and fb["end_year"]:
                row["fallback_end_year"] = fb["end_year"]
                filled["end_year"] += 1
            if not row["award_type"] and CLEAN_SCHEME.match(fb["funder_scheme"] or ""):
                row["award_type"] = fb["funder_scheme"]
                filled["scheme"] += 1
    log(f"  kept the PDF's value where GRIST has none: {filled}")
    return rows


def row_key(row: dict) -> str:
    return row["funder_award_id"].strip().lower()


def main() -> None:
    ap = argparse.ArgumentParser(description=f"{AGENCY} Europe PMC GRIST grant list -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="smoke test: stop after ~N source records")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    ap.add_argument("--from-raw", type=Path, default=None, help="rebuild from a saved --raw-out file (no fetch, no upload)")
    ap.add_argument("--raw-out", type=Path, default=None, help="also write the GRIST source records as JSONL (audit)")
    args = ap.parse_args()

    if args.from_raw:
        records = [json.loads(line) for line in open(args.from_raw, encoding="utf-8") if line.strip()]
        args.skip_upload = True   # a saved corpus is for tests; production always refetches
    else:
        records = fetch_union(args.limit)
    log(f"GRIST: {len(records)} source records")
    if args.raw_out:
        args.raw_out.parent.mkdir(parents=True, exist_ok=True)
        with open(args.raw_out, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        log(f"Wrote {len(records)} source records to {args.raw_out}")
    rows = build_rows(records)
    df = pd.DataFrame(rows)
    log(f"Collapsed to {len(df)} grants")
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")
    for c in ["title", "abstract", "award_type", "start_date", "end_date", "amount",
              "lead_family_name", "lead_orcid", "lead_institution", "lead_institution_ror"]:
        log(f"  {c:22s} {df[c].notna().mean():6.1%}")
    log(f"  currencies {df['currency'].value_counts(dropna=False).to_dict()}")
    log(f"  years {df['start_date'].str[:4].min()}-{df['start_date'].str[:4].max()}")

    df["amount"] = df["amount"].map(lambda v: None if v is None or pd.isna(v) else repr(float(v)))
    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / f"{SLUG}_grist_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload or args.limit:
        log("Upload skipped")
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / f"_previous_{SLUG}_grist_projects.parquet"
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
