#!/usr/bin/env python3
"""
BHF (British Heart Foundation) to S3 Data Pipeline
===================================================

Parses the BHF annual Research Grant Award report PDFs published on bhf.org.uk.

Data Source:
  Listing: https://www.bhf.org.uk/for-professionals/information-for-researchers/previous-awards
    — links to ~20 per-year PDF reports (2004/5 through 2024/25).
  Each PDF lists awards under scheme sections; every award row carries a real
  BHF grant reference (FS/.., PG/.., RG/.., CH/.., SP/.., etc.) + PI + institution
  + grant title + GBP amount + duration.

Output: s3://openalex-ingest/awards/bhf/bhf_projects.parquet

Schema notes:
  - funder_award_id = the BHF grant reference (e.g. FS/IBSRF/24/25222, PG/25/12329)
    — THIS is the form cited in BHF-funded papers (high work-linkage).
  - amount = the £ value (GBP).
  - lead_investigator = the PI (given=initials, family=surname) + institution.
  - display_name = the grant title.
  - PDFs have a text layer (not scanned); parse via pypdf + regex.
  - The same reference can appear across multiple annual reports if a grant
    spans years — we dedup by reference, keeping the first (award-year) record.

Usage:
    python bhf_to_s3.py --skip-upload
    python bhf_to_s3.py --limit 3       # parse only first 3 PDFs (smoke)
"""
import argparse, io, re, sys, time, subprocess
from datetime import datetime, timezone
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd
try:
    from pypdf import PdfReader
except ImportError:
    from PyPDF2 import PdfReader

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/bhf/bhf_projects.parquet"
LISTING   = "https://www.bhf.org.uk/for-professionals/information-for-researchers/previous-awards"
BASE      = "https://www.bhf.org.uk"
UA        = "OpenAlex-BHF/1.0 (contact@openalex.org)"

REF  = re.compile(r'\b([A-Z]{2,5}/(?:[A-Za-z0-9]+/){1,4}\d{4,6})\b')
AMT  = re.compile(r'£\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)')
NAME = re.compile(r'^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss|Sir|Dame)\s+((?:[A-Z]\.?\s+)*)([A-Z][A-Za-z\'’\-]+-?)')
INST = re.compile(
    r'(University of (?:East Anglia|West of England|the West of Scotland|[A-Z][a-z]+)'
    r'|Imperial College London|King’?s College London|University College London'
    r'|Queen Mary University of London|City St George’?s,? University of London'
    r'|London School of Hygiene (?:&|and) Tropical Medicine'
    r'|The Francis Crick Institute|Babraham Institute|Wellcome Sanger Institute'
    r'|[A-Z][A-Za-z’\']+(?:[ ][A-Z][A-Za-z’\']+){0,2} University(?: of London)?)'
)
DEGREE = re.compile(r'^((?:BSc|MSc|PhD|BA|MA|MPhil|MD|MBBS|MBChB|BM|BCh|DPhil|FRCP|FRCPath|FRCPE|FRCPI|FRCPCH|MRCP|MRCS|FRCS|BVSc|DVM|DipCOT|MRes|FMedSci|FAHA|FACC|FESC|FRSE|FRSB|FRSC|FRS|FLSW|FREng|FHEA|PGCE|RGN|RN|OBE|CBE|MBE|Hon|MSci|BMBS|MBA|ScD|DSc|LLD|MAE|FANZCA|FRCA|FFPM|FBPhS)\s*)+', re.I)
YEAR_IN_URL = re.compile(r'(20\d{2})[-_](\d{2,4})|grants?[-_](20\d{2})|report[-_](20\d{2})')

def _curl(url):
    r = subprocess.run(["/usr/bin/curl", "-sL", "--max-time", "60", "-A", UA, url],
                       capture_output=True, timeout=90, check=True)
    return r.stdout

def discover_pdfs():
    html = _curl(LISTING).decode("utf-8", "replace")
    paths = re.findall(r'href="(/-/media/files/[^"]*\.pdf[^"]*)"', html)
    seen, out = set(), []
    for p in paths:
        p = p.split("?")[0]
        if p in seen: continue
        seen.add(p)
        # heuristic award-year from filename
        out.append(BASE + p)
    return out

def _award_year(url, pdf_text):
    m = re.search(r'(\d{4})\s*[-/–]\s*(\d{2,4})', pdf_text[:400])
    if m:
        return int(m.group(1))
    m = re.search(r'(20\d{2})', url)
    return int(m.group(1)) if m else None

def parse_pdf(raw_bytes, url):
    try:
        reader = PdfReader(io.BytesIO(raw_bytes))
    except Exception as e:
        print(f"    [skip] unreadable PDF: {e}")
        return []
    pages = [(pg.extract_text() or "") for pg in reader.pages]
    award_year = _award_year(url, "\n".join(pages[:2]))
    text = "\n".join(t for t in pages if REF.search(t))
    if not text.strip():
        return []
    matches = list(REF.finditer(text))
    rows = []
    for i, m in enumerate(matches):
        s = m.end(); e = matches[i+1].start() if i+1 < len(matches) else len(text)
        block = re.split(r'Reference number|Total\b', re.sub(r'\s+', ' ', text[s:e]).strip())[0]
        am = AMT.search(block)
        if not am:
            continue
        amount = float(am.group(1).replace(",", ""))
        body = AMT.sub(" ", block).strip()
        nm = NAME.match(body)
        given = family = None
        rest = body
        if nm:
            given = (nm.group(2) or "").strip().replace(".", "") or None
            family = nm.group(3).rstrip("-")
            rest = body[nm.end():].strip()
            rest = DEGREE.sub('', rest).strip()
        im = INST.search(rest)
        institution = None
        title = rest
        if im:
            institution = DEGREE.sub("", im.group(1).strip()).strip()
            title = (rest[:im.start()] + " " + rest[im.end():]).strip()
        title = re.sub(r'\s*\d{1,3}\s*(?:months|years)', " ", title)
        title = re.sub(r'\s+', " ", title).strip(" :;–-")
        title = title or None
        rows.append({
            "funder_award_id": m.group(1),
            "title": title,
            "amount": amount if amount > 0 else None,
            "pi_given_name": given,
            "pi_family_name": family,
            "institution_name": institution,
            "award_year": award_year,
            "source_pdf": url.rsplit("/", 1)[-1],
        })
    return rows

def transform(rows):
    df = pd.DataFrame(rows)
    if df.empty: raise SystemExit("no awards parsed")
    # Dedup by reference — keep the earliest award_year (first appearance)
    df = df.sort_values("award_year", na_position="last").drop_duplicates(subset=["funder_award_id"], keep="first")
    yr_now = datetime.now(timezone.utc).year
    df["start_year"] = df["award_year"].astype("Int64")
    df.loc[df["start_year"] > yr_now + 1, "start_year"] = pd.NA
    df["currency"] = df["amount"].map(lambda a: "GBP" if pd.notna(a) and a > 0 else None)
    df["provenance"] = "bhf_grant_pdfs"
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for c in df.columns:
        if df[c].dtype == object: df[c] = df[c].astype("string")
    return df

def check_no_shrink(df, allow_shrink):
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(f"§1.4 FAILED: new {len(df):,} < {len(prev):,}")
        print(f"  §1.4 OK")
    except SystemExit: raise
    except Exception as e:
        print(f"  §1.4: no prior ({type(e).__name__})")

def main():
    ap = argparse.ArgumentParser(description="BHF (bhf.org.uk PDF reports) -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/bhf"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="parse only first N PDFs (smoke)")
    a = ap.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    print("="*60); print("BHF (British Heart Foundation) -> S3"); print("="*60)
    pdfs = discover_pdfs()
    if a.limit: pdfs = pdfs[:a.limit]
    print(f"[1/2] Parsing {len(pdfs)} BHF award PDFs")
    all_rows = []
    cache = a.output_dir / "pdfs"; cache.mkdir(exist_ok=True)
    for url in pdfs:
        fn = url.rsplit("/", 1)[-1].split("?")[0]
        cp = cache / fn
        if not cp.exists():
            try:
                cp.write_bytes(_curl(url)); time.sleep(0.2)
            except Exception as ex:
                print(f"    [skip] download failed {fn}: {ex}"); continue
        rows = parse_pdf(cp.read_bytes(), url)
        print(f"    {fn[:50]:50} -> {len(rows):4d} awards")
        all_rows.extend(rows)
    df = transform(all_rows)
    out = a.output_dir / "bhf_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"\n[2/2] Saved {out.name}: {len(df):,} unique grants, {out.stat().st_size/1e6:.1f} MB")
    nn = lambda c: 100*df[c].notna().sum()/len(df)
    amt_cov = 100*(df["amount"]>0).sum()/len(df)
    print(f"\nCoverage:")
    print(f"  funder_award_id  {len(df):,} (100%)")
    print(f"  title            {df['title'].notna().sum():,} ({nn('title'):.1f}%)")
    print(f"  GBP amount       {(df['amount']>0).sum():,} ({amt_cov:.1f}%)")
    print(f"  PI family_name   {df['pi_family_name'].notna().sum():,} ({nn('pi_family_name'):.1f}%)")
    print(f"  institution      {df['institution_name'].notna().sum():,} ({nn('institution_name'):.1f}%)")
    print(f"  start_year       {df['start_year'].notna().sum():,} ({nn('start_year'):.1f}%)")
    if df["amount"].notna().any():
        print(f"\n  GBP median: {float(df['amount'].median()):,.0f}, max: {float(df['amount'].max()):,.0f}")
    if df["start_year"].notna().any():
        print(f"  Year range: {int(df['start_year'].min())}-{int(df['start_year'].max())}")
    print(f"  Top institutions: {df['institution_name'].value_counts().head(5).to_dict()}")
    # §6.4a self-audit
    top = df.dropna(subset=['pi_given_name','pi_family_name']).groupby(['pi_given_name','pi_family_name']).size().sort_values(ascending=False).head(3)
    print(f"  §6.4a top PI combos: {dict(top)}")
    if not a.skip_upload:
        check_no_shrink(df, a.allow_shrink)
        import shutil
        aws = shutil.which("aws")
        if aws: subprocess.run([aws,"s3","cp",str(out),f"s3://{S3_BUCKET}/{S3_KEY}"], check=False)
        else: print(f"  [manual] aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
    print(f"\nNext: notebooks/awards/CreateBHFAwards.ipynb")

if __name__ == "__main__":
    main()
