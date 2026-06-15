#!/usr/bin/env python3
"""
ETIS (Estonian Research Information System) to S3 Data Pipeline
===============================================================

Pulls Estonia's national research-project database (ETIS) and uploads parquets to
S3 for Databricks ingestion. ETIS aggregates ALL Estonian research funders, so this
script SPLITS projects by their financier into the two main research councils, each
a distinct OpenAlex funder (do NOT lump them — they are separate entities):
    - Estonian Science Foundation (ETF)      -> F4320321018  (provenance etis_etf)
    - Eesti Teadusagentuur / Estonian
      Research Council (ETAg)                -> F4320321090  (provenance etis_etag)
Both are largely net-new (awards 455/8,185 and 2,329/12,035 respectively).

Data source: ETIS exposes a public, key-less REST/JSON API on port 7443:
    count: https://www.etis.ee:7443/api/project/getcount?Format=json&SearchType=3
    items: https://www.etis.ee:7443/api/project/getitems?Format=json&SearchType=3
           &Take={n}&Skip={offset}   (SearchType=3 = full detail; paginate Take/Skip)
    ~26,265 projects total. Per-doc fields used:
      FinancierProjectNr  -> native grant nr (e.g. ETF6578, PRG123)  [Guid = fallback]
      TitleEng / Title    -> title (bilingual)
      Persons[] (RoleNameEng "Responsible executor" = PI) -> lead investigator
      Institutions[].NameEng -> institution
      FinancingInPeriodsTotal -> amount (EUR)
      ProjectStartDate / ProjectEndDate -> dates (DD.MM.YYYY)
      ProgrammeNameEng    -> funder_scheme
      AnnotationEng / Annotation -> abstract
      FinancingInstitutions[].NameEng -> financier (the SPLIT key)

Output: s3://openalex-ingest/awards/etis_etf/etis_etf_grants.parquet
        s3://openalex-ingest/awards/etis_etag/etis_etag_grants.parquet
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
BASE = "https://www.etis.ee:7443/api/project"
S3_BUCKET = "openalex-ingest"

# financier NameEng -> (provenance, s3 sub-path). A project is attributed to the
# first matching financier; projects with neither are skipped (other funders).
ETF_NAMES = {"estonian science foundation"}
ETAG_NAMES = {"estonian research council", "eesti teadusagentuur",
              "estonian research council (etag)"}

_DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
_NULLISH = {"", "nan", "none", "n/a", "na", "-"}


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return None if s.lower() in _NULLISH else s


def first(v):
    return v[0] if isinstance(v, list) and v else None


def parse_pi(persons):
    if not isinstance(persons, list):
        return None, None
    pick = None
    for p in persons:
        role = (p.get("RoleNameEng") or "").lower()
        if "responsible" in role:
            pick = p
            break
    if pick is None:
        for p in persons:                       # fallback: main executor / first
            if "main" in (p.get("RoleNameEng") or "").lower():
                pick = p
                break
    pick = pick or (persons[0] if persons else None)
    name = clean(pick.get("Name")) if pick else None
    if not name:
        return None, None
    parts = name.split()
    if len(parts) < 2:
        return None, name
    return " ".join(parts[:-1]), parts[-1]


def iso(v):
    s = clean(v)
    if not s:
        return None
    m = _DATE_RE.search(s)
    return f"{int(m.group(3)):04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}" if m else None


def parse_amount(v):
    try:
        n = float(v)
        return str(int(round(n))) if n > 0 else None
    except (TypeError, ValueError):
        return None


def target_funder(doc):
    for fi in (doc.get("FinancingInstitutions") or []):
        n = (fi.get("NameEng") or fi.get("Name") or "").strip().lower()
        if n in ETF_NAMES:
            return "etf"
        if n in ETAG_NAMES:
            return "etag"
    return None


def to_record(doc):
    aid = clean(doc.get("FinancierProjectNr")) or clean(doc.get("Guid"))
    if not aid:
        return None
    given, family = parse_pi(doc.get("Persons"))
    inst = first(doc.get("Institutions")) or {}
    area = first(doc.get("ResearchAreasFrascati")) or {}
    return {
        "funder_award_id": aid,
        "title": clean(doc.get("TitleEng")) or clean(doc.get("Title")),
        "pi_given": given,
        "pi_family": family,
        "institution": clean(inst.get("NameEng")) or clean(inst.get("Name")),
        "amount": parse_amount(doc.get("FinancingInPeriodsTotal")),
        "currency": "EUR",
        "scheme": clean(doc.get("ProgrammeNameEng")) or clean(doc.get("ProgrammeName")),
        "research_area": clean(area.get("NameEng")) if isinstance(area, dict) else None,
        "start_date_raw": iso(doc.get("ProjectStartDate")),
        "end_date_raw": iso(doc.get("ProjectEndDate")),
        "description": (clean(doc.get("AnnotationEng")) or clean(doc.get("Annotation")) or None),
        "landing_page_url": clean(doc.get("Url")) or "https://www.etis.ee/",
    }


def fetch_all(session) -> list:
    count = session.get(f"{BASE}/getcount?Format=json&SearchType=3", timeout=60).json().get("Count", 0)
    print(f"  total projects in ETIS: {count}")
    docs, take, skip = [], 500, 0
    while skip < count:
        page = session.get(f"{BASE}/getitems?Format=json&SearchType=3&Take={take}&Skip={skip}",
                           timeout=120).json()
        if not page:
            break
        docs.extend(page)
        skip += len(page)
        if skip % 5000 < take:
            print(f"  pulled {skip}/{count}")
        if len(page) < take:
            break
    print(f"  pulled {len(docs)} docs")
    return docs


def upload(local_path: Path, key: str) -> bool:
    s3_uri = f"s3://{S3_BUCKET}/{key}"
    try:
        subprocess.run(["aws", "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print(f"  uploaded {s3_uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  upload failed: {e.stderr}")
        return False


def main():
    ap = argparse.ArgumentParser(description="ETIS (ETF + ETAg) projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/etis_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("ETIS (Estonian Research Information System) -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    docs = fetch_all(s)

    # validation: confirm the ETF/ETAg financier names we match against are complete
    # (a missed variant would silently undercount a funder)
    from collections import Counter
    fin = Counter()
    for d in docs:
        for fi in (d.get("FinancingInstitutions") or []):
            fin[(fi.get("NameEng") or fi.get("Name") or "?").strip()] += 1
    print("\n=== distinct financiers (top 25) — confirm ETF/ETAg coverage ===")
    for n, c in fin.most_common(25):
        nl = n.lower()
        mark = " <== ETF" if nl in ETF_NAMES else (" <== ETAg" if nl in ETAG_NAMES else "")
        print(f"  {c:>6}  {n}{mark}")

    buckets = {"etf": [], "etag": []}
    seen = {"etf": set(), "etag": set()}
    for d in docs:
        tgt = target_funder(d)
        if not tgt:
            continue
        rec = to_record(d)
        if not rec or rec["funder_award_id"] in seen[tgt]:
            continue
        seen[tgt].add(rec["funder_award_id"])
        buckets[tgt].append(rec)

    ok = True
    for tgt, label, key in (("etf", "Estonian Science Foundation (F4320321018)",
                             "awards/etis_etf/etis_etf_grants.parquet"),
                            ("etag", "Estonian Research Council / ETAg (F4320321090)",
                             "awards/etis_etag/etis_etag_grants.parquet")):
        rows = buckets[tgt]
        df = pd.DataFrame(rows).astype("string")
        print(f"\n=== {label}: {len(df)} projects ===")
        for c in ("title", "pi_family", "institution", "amount", "start_date_raw", "scheme", "description"):
            nn = df[c].notna().sum() if c in df else 0
            print(f"  {c:14}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
        if len(df) < 100:
            print(f"  [WARN] only {len(df)} rows for {tgt}")
        out = args.output_dir / f"etis_{tgt}_grants.parquet"
        df.to_parquet(out, index=False)
        print(f"  wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")
        if not args.skip_upload:
            ok = upload(out, key) and ok

    if not args.skip_upload and not ok:
        sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
