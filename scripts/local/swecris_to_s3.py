#!/usr/bin/env python3
"""
SweCRIS (Sweden) to S3 Data Pipeline — multi-funder adapter
===========================================================

Pulls Swedish research funding decisions from SweCRIS (the national research
information system run by Vetenskapsrådet) via its public REST API.

Data Source:
  https://swecris-api.vr.se/v1/scp/search  (Bearer token below is the SweCRIS
  web app's own public token — SweCRIS is an open-data service). 68,843 funding
  decisions across 15 Swedish funders. We pull all once and split by
  fundingOrganisationId into the TARGET funders (those not already ingested).

Output: s3://openalex-ingest/awards/<slug>/<slug>_projects.parquet  (one per funder)

Schema notes (rich):
  - funder_award_id = projectId (e.g. "2018-001234_Energi") — cited in papers.
  - amount = fundingsSek (SEK). 0/neg -> NULL.
  - lead_investigator = peopleList[0] (fullName split given/family) + orcId +
    coordinatingOrganisation (recipient institution).
  - display_name = projectTitleEn || projectTitleSv; description = abstract.
  - start/end_year from fundingStartDate/fundingEndDate.

CHECK-FIRST: VR/Vinnova/Formas/Forte/Riksbankens Jubileumsfond are already
  ingested (skipped). This adapter targets the un-ingested SweCRIS funders.

Usage:
    python swecris_to_s3.py --skip-upload                 # all TARGET funders
    python swecris_to_s3.py --skip-upload --only swecris_energimyndigheten
"""
import argparse, json, re, sys, time, subprocess
from datetime import datetime, timezone
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd

S3_BUCKET = "openalex-ingest"
API   = "https://swecris-api.vr.se/v1/scp/search"
TOKEN = "Bearer RWNDZ3FDRVVSMmNUNlZkMkN3"   # SweCRIS web-app public token
UA    = "OpenAlex-SweCRIS/1.0 (contact@openalex.org)"
PAGE  = 2000

# org_id -> (slug, provenance, openalex_funder_id)
TARGETS = {
    "202100-5000": ("swedish_energy_agency", "swecris_energimyndigheten", 4320322711),
    "802006-0763": ("swedish_heart_lung_foundation", "swecris_hjart_lungfonden", 4320322169),
    "202100-2585": ("swedish_national_space_agency", "swecris_rymdstyrelsen", 4320321031),
    "802423-4075": ("kamprad_family_foundation", "swecris_kamprad", 4320325984),
}

def _get(url, tries=5):
    for a in range(tries):
        try:
            r = subprocess.run(["/usr/bin/curl","-sL","--max-time","90","-A",UA,
                                "-H",f"Authorization: {TOKEN}", url],
                               capture_output=True, timeout=120, check=True)
            return json.loads(r.stdout.decode("utf-8","replace"))
        except Exception:
            if a == tries-1: raise
            time.sleep(2**a)

def fetch_targets(cache):
    """Pull all pages, retain only records for TARGET funders."""
    if cache.exists():
        print(f"  [cache] {cache.name}")
        return json.loads(cache.read_text(encoding="utf-8"))
    kept, page = [], 1
    while True:
        d = _get(f"{API}?page={page}&size={PAGE}")
        rows = d.get("result", [])
        total = d.get("total", 0)
        if not rows: break
        kept.extend(r for r in rows if r.get("fundingOrganisationId") in TARGETS)
        print(f"  page {page:3d}  scanned {min(page*PAGE,total)}/{total}  kept {len(kept)}")
        if page*PAGE >= total: break
        page += 1; time.sleep(0.1)
    cache.write_text(json.dumps(kept, ensure_ascii=False))
    return kept

def _nb(*v):
    for x in v:
        if x is not None and str(x).strip() not in ("","-1","None"): return str(x).strip()
    return None

def _split_name(full):
    full = _nb(full)
    if not full: return None, None
    if "," in full:
        fam, _, giv = full.partition(","); return _nb(giv), _nb(fam)
    parts = full.split()
    if len(parts) == 1: return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]

def _year(*v):
    for x in v:
        s = str(x or "")
        m = re.search(r"(19|20)\d{2}", s)
        if m:
            y = int(m.group(0))
            if 1960 <= y <= datetime.now(timezone.utc).year + 12: return y
    return None

def _amount(v):
    try: f = float(v); return f if f > 0 else None
    except (TypeError, ValueError): return None

def transform(recs, provenance):
    out = []
    for s in recs:
        aid = _nb(s.get("projectId"))
        if not aid: continue
        ppl = s.get("peopleList") or []
        giv = fam = orc = None
        if isinstance(ppl, list) and ppl:
            giv, fam = _split_name(ppl[0].get("fullName"))
            orc = _nb(ppl[0].get("orcId"))
        out.append({
            "funder_award_id": aid,
            "title": _nb(s.get("projectTitleEn"), s.get("projectTitleSv")),
            "description": _nb(s.get("projectAbstractEn"), s.get("projectAbstractSv")),
            "amount": _amount(s.get("fundingsSek")),
            "pi_given_name": giv, "pi_family_name": fam, "pi_orcid": orc,
            "institution_name": _nb(s.get("coordinatingOrganisationNameEn"),
                                    s.get("coordinatingOrganisationNameSv")),
            "start_year": _year(s.get("fundingYear"), s.get("fundingStartDate")),
            "end_year": _year(s.get("fundingEndDate")),
            "funding_type_raw": _nb(s.get("typeOfAwardDescrEn")),
            "provenance": provenance,
        })
    df = pd.DataFrame(out)
    if df.empty: return df
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["currency"] = df["amount"].map(lambda a: "SEK" if pd.notna(a) and a > 0 else None)
    df["start_year"] = df["start_year"].astype("Int64"); df["end_year"] = df["end_year"].astype("Int64")
    dup = df["funder_award_id"].duplicated().sum()
    if dup:
        ex = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(3).tolist()
        raise RuntimeError(f"§slug-collision: {dup} dup projectId, e.g. {ex}")
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for c in df.columns:
        if df[c].dtype == object: df[c] = df[c].astype("string")
    return df

def main():
    ap = argparse.ArgumentParser(description="SweCRIS (Sweden) -> S3 (multi-funder)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/swecris"))
    ap.add_argument("--only", help="only this provenance slug")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    a = ap.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    print("="*60); print("SweCRIS (Sweden) -> S3"); print("="*60)
    recs = fetch_targets(a.output_dir / "swecris_targets_raw.json")
    print(f"  retained {len(recs):,} records for {len(TARGETS)} target funders\n")
    by_org = {}
    for r in recs: by_org.setdefault(r["fundingOrganisationId"], []).append(r)
    for org_id, (slug, prov, fid) in TARGETS.items():
        if a.only and prov != a.only: continue
        sub = by_org.get(org_id, [])
        df = transform(sub, prov)
        if df.empty:
            print(f"[{slug}] 0 records — skip"); continue
        out = a.output_dir / f"{slug}_projects.parquet"
        df.to_parquet(out, index=False)
        nn = lambda c: 100*df[c].notna().sum()/len(df)
        print(f"[{slug}]  F{fid}  {len(df):,} grants -> {out.name}")
        print(f"   id 100% | amount {nn('amount'):.0f}% | PI {nn('pi_family_name'):.0f}% | "
              f"orcid {nn('pi_orcid'):.0f}% | inst {nn('institution_name'):.0f}% | yr {nn('start_year'):.0f}%")
        if df['amount'].notna().any():
            yr = (f" | years {int(df['start_year'].min())}-{int(df['start_year'].max())}"
                  if df['start_year'].notna().any() else " | years n/a")
            print(f"   SEK median {df['amount'].median():,.0f} max {df['amount'].max():,.0f}{yr}")
        top = df.dropna(subset=['pi_given_name','pi_family_name']).groupby(['pi_given_name','pi_family_name']).size().sort_values(ascending=False).head(3)
        print(f"   §6.4a top PIs: {dict(top)}")
        if not a.skip_upload:
            import shutil
            aws = shutil.which("aws"); key = f"awards/{slug}/{slug}_projects.parquet"
            if aws: subprocess.run([aws,"s3","cp",str(out),f"s3://{S3_BUCKET}/{key}"], check=False)
            else: print(f"   [manual] aws s3 cp {out} s3://{S3_BUCKET}/{key}")

if __name__ == "__main__":
    main()
