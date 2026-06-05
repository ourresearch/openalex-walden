#!/usr/bin/env python3
"""
Villum Fonden (Denmark) to S3 Data Pipeline
============================================

Downloads the Villum/Velux Foundations' public project database from
veluxfonden.dk and writes a parquet for Databricks.

Data Source: https://veluxfonden.dk/en/data.js
  A single 6.3 MB JavaScript asset containing:
    siteData.merge(JSON.parse('<one big JSON blob>'))
  The blob contains the entire grant database — 15,518 projects across
  Villum Foundation (id=59, 2,878 projects — RESEARCH) and Velux Foundation
  (id=58, 12,640 projects — welfare/culture/heritage). We filter to Villum
  (foundation=59) for this ingest; F4320310489 (Velux Fonden) is a sibling
  ingest under a different funder_id.

Output: s3://openalex-ingest/awards/villum/villum_projects.parquet

Schema (from data.js):
  project:      {id, title, amount, grant_sub_area, organisation, country,
                 year, pa_age, pa_gender, pa_name, url}
  grant_sub_area: {id, grant_area, title}
  grant_area:   {id, foundation, title}   ← foundation FK
  organisation: {id, title}
  country:      {id, title}

  pa_age and pa_gender are PII demographic fields published by the foundation
  for transparency; we DROP them — not surfacing into the awards table.

Output columns:
  funder_award_id  → `villum-{project_id}` (the foundation has no public
                      cited grant prefix; `villum-647` is the URL-stable form)
  title (English), pa_name (PI), institution (dereferenced organisation),
  country (dereferenced; almost all Denmark), amount (DKK), year,
  funder_scheme (grant_sub_area.title), funder_area (grant_area.title)

Usage:
    python villum_to_s3.py --skip-upload
"""
import argparse, json, re, sys
from datetime import datetime, timezone
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception: pass
import urllib.request
import pandas as pd

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/villum/villum_projects.parquet"
DATA_URL  = "https://veluxfonden.dk/en/data.js"
FOUNDATION_ID_FILTER = 59   # Villum Foundation (Velux Foundation = 58, separate ingest)

def fetch_data(output_dir: Path) -> dict:
    print(f"[1/2] Downloading veluxfonden.dk/en/data.js")
    raw_path = output_dir / "velux_data.js"
    req = urllib.request.Request(DATA_URL, headers={"User-Agent": "OpenAlex-Villum/1.0 (contact@openalex.org)"})
    raw = urllib.request.urlopen(req, timeout=60).read()
    raw_path.write_bytes(raw)
    print(f"      saved {raw_path.name}: {raw_path.stat().st_size/1e6:.1f} MB")
    text = raw_path.read_text(encoding="utf-8")
    # Extract the JSON blob from `siteData.merge(JSON.parse('...'))`
    m = re.search(r"siteData\.merge\(JSON\.parse\('(.*?)'\)\)", text, re.S)
    if not m:
        raise SystemExit("could not find siteData.merge(JSON.parse('...')) in data.js")
    js_string = m.group(1)
    # Unescape JS-escapes inside the single-quoted JS string literal
    js_string = js_string.replace("\\'", "'").replace('\\\\', '\\')
    d = json.loads(js_string)
    print(f"      parsed: project={len(d.get('project',{})):,}, organisation={len(d.get('organisation',{})):,}, "
          f"grant_sub_area={len(d.get('grant_sub_area',{}))}, grant_area={len(d.get('grant_area',{}))}, "
          f"country={len(d.get('country',{}))}, foundation={len(d.get('foundation',{}))}")
    return d

def transform(d: dict) -> pd.DataFrame:
    print(f"[2/2] Transforming → Villum (foundation={FOUNDATION_ID_FILTER}) projects only")
    # Build lookup maps
    sub_to_area  = {sid: s.get("grant_area") for sid, s in d["grant_sub_area"].items()}
    sub_to_title = {sid: s.get("title") for sid, s in d["grant_sub_area"].items()}
    area_to_fnd  = {aid: a.get("foundation") for aid, a in d["grant_area"].items()}
    area_to_title= {aid: a.get("title") for aid, a in d["grant_area"].items()}
    org_to_title = {oid: o.get("title") for oid, o in d["organisation"].items()}
    ctry_to_title= {cid: c.get("title") for cid, c in d["country"].items()}
    recs = []
    yr_now = datetime.now(timezone.utc).year
    for pid, p in d["project"].items():
        sub_id = str(p.get("grant_sub_area", ""))
        area_id = str(sub_to_area.get(sub_id, ""))
        fnd_id = area_to_fnd.get(area_id)
        # Filter to Villum (some grant_sub_areas have foundation NULL — drop those)
        if str(fnd_id) != str(FOUNDATION_ID_FILTER):
            continue
        amt = p.get("amount")
        try:
            amt = float(amt) if amt not in (None, "") else None
            if amt is not None and amt <= 0: amt = None
        except (TypeError, ValueError):
            amt = None
        yr = p.get("year")
        try:
            yr = int(yr) if yr not in (None, "") else None
            if yr is not None and yr > yr_now + 1: yr = None  # future-year cap
        except (TypeError, ValueError):
            yr = None
        # PI name is a single field. Canonical split_name (runbook §2.4.1): strip trailing
        # degree/suffix tokens (PhD/MD/Jr./Sr./II/III/IV) before taking last token as family.
        pa_name = (p.get("pa_name") or "").strip() or None
        given_name, family_name = None, None
        if pa_name and not _looks_like_org_name(pa_name):
            tokens = pa_name.split()
            _SFX = {"phd", "ph.d", "md", "m.d", "dphil", "dsc", "scd", "msc", "m.sc",
                    "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
            while tokens and tokens[-1].lower().strip(",.") in _SFX:
                tokens.pop()
            if len(tokens) >= 2:
                family_name = tokens[-1]
                given_name = " ".join(tokens[:-1])
            elif len(tokens) == 1:
                family_name = tokens[0]
        recs.append({
            "funder_award_id":  f"villum-{p.get('id')}",
            "project_id":       int(p.get("id")),
            "title":            (p.get("title") or "").strip() or None,
            "amount":           amt,
            "currency":         "DKK" if amt else None,
            "year":             yr,
            "pi_name":          pa_name,
            "pi_given_name":    given_name,
            "pi_family_name":   family_name,
            "institution_name": org_to_title.get(str(p.get("organisation"))) if p.get("organisation") else None,
            "institution_country": ctry_to_title.get(str(p.get("country"))) if p.get("country") else None,
            "funder_scheme":    sub_to_title.get(sub_id),
            "funder_area":      area_to_title.get(area_id),
            "landing_url":      f"https://veluxfonden.dk{p.get('url')}" if p.get("url") else None,
        })
    df = pd.DataFrame(recs)
    print(f"      kept {len(df):,} Villum projects (filtered from {len(d['project']):,} total)")
    # Slug-collision guard
    dup = df["funder_award_id"].duplicated().sum()
    if dup:
        raise RuntimeError(f"§slug-collision: {dup} duplicate funder_award_id values")
    # All object → pandas string
    for c in df.columns:
        if df[c].dtype == object: df[c] = df[c].astype("string")
    df["provenance"] = "villum_veluxfonden"
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return df

_INST_TOKENS = re.compile(r"(?i)\b(universitet|university|institute|institut|college|hospital|skole|gymnasium|ph\.d|fond(en)?|fonden|stiftelse|stiftung|forening|forlag|publishers|kommune|kirke|menighed|museum|selskab|society|association|center|centret|centrum|library|bibliotek|biblioteket|kors|fri|union|citizens|seniors?|trust|charity|theatre|teater|opera|fonden|gymnasie|skoler|partnerskab|fellowship)\b|^(Active|Hedwig|Skriveforlaget) ")
def _looks_like_org_name(s: str) -> bool:
    """Defensive guard against institution-as-PI (§6.4a). If pa_name contains org-y tokens, treat as org and NULL out the person split."""
    return bool(_INST_TOKENS.search(s or ""))

def check_no_shrink(df, allow_shrink):
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(
                f"§1.4 shrink-check FAILED: new {len(df):,} < existing {len(prev):,}. Pass --allow-shrink only if real.")
        print(f"  §1.4 shrink-check OK (new {len(df):,} >= existing {len(prev):,})")
    except SystemExit: raise
    except Exception as e:
        print(f"  §1.4 shrink-check: no prior object / not comparable ({type(e).__name__})")

def main():
    ap = argparse.ArgumentParser(description="Villum Fonden (veluxfonden.dk/data.js) -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/villum"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    a = ap.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Villum Fonden (Denmark) -> S3")
    print("=" * 60)
    raw = fetch_data(a.output_dir)
    df = transform(raw)
    out = a.output_dir / "villum_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"\nSaved {out.name}: {len(df):,} rows, {out.stat().st_size/1e6:.1f} MB")
    nn = lambda c: 100 * df[c].notna().sum() / len(df)
    amt_cov = 100 * (df["amount"] > 0).sum() / len(df)
    print(f"\nCoverage:")
    print(f"  funder_award_id        {len(df):,} (100%)")
    print(f"  title                  {df['title'].notna().sum():,} ({nn('title'):.1f}%)")
    print(f"  DKK amount             {(df['amount']>0).sum():,} ({amt_cov:.1f}%)")
    print(f"  pi_name (any)          {df['pi_name'].notna().sum():,} ({nn('pi_name'):.1f}%)")
    print(f"  pi_family_name         {df['pi_family_name'].notna().sum():,} ({nn('pi_family_name'):.1f}%)")
    print(f"  institution_name       {df['institution_name'].notna().sum():,} ({nn('institution_name'):.1f}%)")
    print(f"  year                   {df['year'].notna().sum():,} ({nn('year'):.1f}%)")
    print(f"  funder_scheme          {df['funder_scheme'].notna().sum():,} ({nn('funder_scheme'):.1f}%)")
    if df["amount"].notna().any():
        print(f"\n  Amount: median DKK {float(df['amount'].median()):,.0f}, max DKK {float(df['amount'].max()):,.0f}")
    if df["year"].notna().any():
        print(f"  Year range: {int(df['year'].min())}-{int(df['year'].max())}")
    print(f"\nTop areas: {df['funder_area'].value_counts().head(5).to_dict()}")
    print(f"Top institutions: {df['institution_name'].value_counts().head(5).to_dict()}")
    if not a.skip_upload:
        check_no_shrink(df, a.allow_shrink)
        import subprocess, shutil
        aws = shutil.which("aws")
        if aws:
            subprocess.run([aws, "s3", "cp", str(out), f"s3://{S3_BUCKET}/{S3_KEY}"], check=False)
        else:
            print(f"  [manual] aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
    print(f"\nNext: notebooks/awards/CreateVillumAwards.ipynb in Databricks")

if __name__ == "__main__":
    main()
