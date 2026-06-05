#!/usr/bin/env python3
"""
Business Finland (research.fi) to S3 Data Pipeline
==================================================

Pulls Business Finland funding decisions from Finland's national research
information hub research.fi, via its public Elasticsearch-backed portal API.

Data Source:
  https://researchfi-api-production.2.rahtiapp.fi/portalapi/funding/_search
  The `funding` index = 23,023 Finnish funding decisions across all funders.
  We filter to funderNameEn = "Innovaatiorahoituskeskus Business Finland"
  (Business Finland, the post-2018 successor to Tekes) -> 2,342 decisions.

Output: s3://openalex-ingest/awards/business_finland/business_finland_projects.parquet

Schema notes:
  - funder_award_id = funderProjectNumber (the Business Finland diary number,
    e.g. "6867/31/2019") -- THIS is the form cited in BF-funded papers (100% present).
  - amount = amount_in_EUR (EUR; ~99% present).
  - lead_investigator = fundingContactPerson (given=FirstNames, family=LastName).
  - institution_name = fundingGroupPerson[].consortiumOrganizationNameEn
    (the specific recipient org, e.g. "Tampere University").
  - display_name = projectNameEn || projectNameFi || projectNameSv (Finnish-dominant).
  - start_year/end_year = fundingStartYear/fundingEndYear (1900 sentinel -> NULL).

OpenAlex funder: Business Finland = F4320328501 (FI, 5,543 works). NOTE: this is a
  SEPARATE OpenAlex funder from Tekes (F4320321855), its pre-2018 predecessor;
  research.fi carries the Business-Finland-era decisions. Attribute to F4320328501.

REUSABLE: research.fi covers many Finnish funders in the same `funding` index.
  To onboard another (e.g. Svenska kulturfonden F4320324257, Research Council of
  Finland), swap FUNDER_NAME + the OpenAlex funder_id in the notebook. See
  --funder-name / --provenance flags.

Usage:
    python business_finland_to_s3.py --skip-upload
"""
import argparse, json, re, sys, time, subprocess
from datetime import datetime, timezone
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd

S3_BUCKET = "openalex-ingest"
ES_BASE   = "https://researchfi-api-production.2.rahtiapp.fi/portalapi/funding/_search"
UA        = "OpenAlex-BusinessFinland/1.0 (contact@openalex.org)"
FUNDER_NAME_DEFAULT = "Innovaatiorahoituskeskus Business Finland"
PAGE = 1000

def _post(body, tries=5):
    """system /usr/bin/curl POST — robust against local OpenSSL quirks."""
    for a in range(tries):
        try:
            r = subprocess.run(
                ["/usr/bin/curl", "-sL", "--max-time", "60", "-A", UA,
                 "-H", "Content-Type: application/json", "-X", "POST", ES_BASE,
                 "-d", json.dumps(body)],
                capture_output=True, timeout=90, check=True)
            return json.loads(r.stdout.decode("utf-8", "replace"))
        except Exception:
            if a == tries - 1: raise
            time.sleep(2 ** a)

def fetch_all(funder_name):
    """search_after pagination on projectId (stable, no 10k cap)."""
    rows, after = [], None
    while True:
        body = {
            "size": PAGE,
            "query": {"match_phrase": {"funderNameEn": funder_name}},
            "sort": [{"projectId": "asc"}],
        }
        if after is not None:
            body["search_after"] = after
        d = _post(body)
        hits = d.get("hits", {}).get("hits", [])
        if not hits:
            break
        rows.extend(h["_source"] for h in hits)
        after = hits[-1].get("sort")
        total = d["hits"]["total"]
        total = total.get("value") if isinstance(total, dict) else total
        print(f"  fetched {len(rows):5d}/{total}")
        if len(hits) < PAGE:
            break
    return rows

def _first_nonblank(*vals):
    for v in vals:
        if v is not None and str(v).strip() not in ("", "-1"):
            return str(v).strip()
    return None

def _institution(src):
    grp = src.get("fundingGroupPerson")
    if isinstance(grp, list):
        for g in grp:
            inst = _first_nonblank(g.get("consortiumOrganizationNameEn"),
                                   g.get("consortiumOrganizationNameFi"))
            # skip the generic sector label ("University"/"Yliopisto")
            if inst and inst.lower() not in ("university", "yliopisto", "company", "yritys"):
                return inst
    return None

def _amount(v):
    try:
        f = float(v); return f if f > 0 else None
    except (TypeError, ValueError):
        return None

def _year(v):
    try:
        y = int(float(v))
        return y if 1960 <= y <= datetime.now(timezone.utc).year + 12 else None
    except (TypeError, ValueError):
        return None

def transform(raw, provenance):
    recs = []
    for s in raw:
        award_id = _first_nonblank(s.get("funderProjectNumber"))
        if not award_id:
            continue
        recs.append({
            "funder_award_id": award_id,
            "title": _first_nonblank(s.get("projectNameEn"), s.get("projectNameFi"),
                                     s.get("projectNameSv"), s.get("projectNameUnd")),
            "description": _first_nonblank(s.get("projectDescriptionEn"),
                                           s.get("projectDescriptionFi"),
                                           s.get("projectDescriptionSv")),
            "amount": _amount(s.get("amount_in_EUR")),
            "pi_given_name": _first_nonblank(s.get("fundingContactPersonFirstNames")),
            "pi_family_name": _first_nonblank(s.get("fundingContactPersonLastName")),
            "institution_name": _institution(s),
            "start_year": _year(s.get("fundingStartYear")),
            "end_year": _year(s.get("fundingEndYear")),
            "funding_type_raw": _first_nonblank(s.get("typeOfFundingNameEn")),
            "provenance": provenance,
        })
    df = pd.DataFrame(recs)
    if df.empty:
        raise SystemExit("no records parsed")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["currency"] = df["amount"].map(lambda a: "EUR" if pd.notna(a) and a > 0 else None)
    df["start_year"] = df["start_year"].astype("Int64")
    df["end_year"] = df["end_year"].astype("Int64")
    # slug-collision guard — funderProjectNumber is the hashed primary id in the
    # notebook, so it MUST be unique. Raise (not silently drop) on collision.
    dup = df["funder_award_id"].duplicated().sum()
    if dup:
        ex = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(5).tolist()
        raise RuntimeError(f"§slug-collision: {dup} duplicate funder_award_id (funderProjectNumber), e.g. {ex}")
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype("string")
    return df

def check_no_shrink(df, key, allow):
    try:
        import boto3
        s3 = boto3.client("s3"); s3.head_object(Bucket=S3_BUCKET, Key=key)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{key}")
        if len(df) < len(prev) and not allow:
            raise SystemExit(f"§1.4 FAILED: new {len(df):,} < prev {len(prev):,}")
        print("  §1.4 OK")
    except SystemExit: raise
    except Exception as e:
        print(f"  §1.4: no prior ({type(e).__name__})")

def main():
    ap = argparse.ArgumentParser(description="Business Finland (research.fi) -> S3")
    ap.add_argument("--funder-name", default=FUNDER_NAME_DEFAULT,
                    help="research.fi funderNameEn to filter on")
    ap.add_argument("--provenance", default="research_fi_business_finland")
    ap.add_argument("--slug", default="business_finland",
                    help="s3 path slug awards/<slug>/<slug>_projects.parquet")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/business_finland"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    a = ap.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    s3_key = f"awards/{a.slug}/{a.slug}_projects.parquet"
    print("=" * 60); print(f"Business Finland / research.fi -> S3  [{a.funder_name}]"); print("=" * 60)
    print("[1/2] Fetching funding decisions from research.fi ES")
    raw = fetch_all(a.funder_name)
    print(f"      total raw: {len(raw):,}")
    print("[2/2] Transforming")
    df = transform(raw, a.provenance)
    out = a.output_dir / f"{a.slug}_projects.parquet"
    df.to_parquet(out, index=False)
    nn = lambda c: 100 * df[c].notna().sum() / len(df)
    print(f"\nSaved {out.name}: {len(df):,} grants, {out.stat().st_size/1e6:.1f} MB")
    print("Coverage:")
    for c in ["funder_award_id", "title", "amount", "pi_family_name", "institution_name", "start_year"]:
        print(f"  {c:18s} {df[c].notna().sum():,} ({nn(c):.1f}%)")
    if df["amount"].notna().any():
        print(f"\n  EUR median: {df['amount'].median():,.0f}  max: {df['amount'].max():,.0f}")
    if df["start_year"].notna().any():
        print(f"  Year range: {int(df['start_year'].min())}-{int(df['start_year'].max())}")
    top = df.dropna(subset=['pi_given_name','pi_family_name']).groupby(['pi_given_name','pi_family_name']).size().sort_values(ascending=False).head(5)
    print(f"  §6.4a top PI combos: {dict(top)}")
    print(f"  Top institutions: {df['institution_name'].value_counts().head(5).to_dict()}")
    if not a.skip_upload:
        check_no_shrink(df, s3_key, a.allow_shrink)
        import shutil
        aws = shutil.which("aws")
        if aws: subprocess.run([aws, "s3", "cp", str(out), f"s3://{S3_BUCKET}/{s3_key}"], check=False)
        else: print(f"  [manual] aws s3 cp {out} s3://{S3_BUCKET}/{s3_key}")
    print(f"\nNext: notebooks/awards/CreateBusinessFinlandAwards.ipynb")

if __name__ == "__main__":
    main()
