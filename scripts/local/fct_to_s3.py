#!/usr/bin/env python3
"""
FCT (Fundação para a Ciência e a Tecnologia, Portugal) to S3 Data Pipeline
==========================================================================

Downloads ALL FCT research projects from the OpenAIRE **Graph API v1** (cursor
pagination — no 10,000-result cap) and writes a parquet for Databricks.

Data Source: https://api.openaire.eu/graph/v1/projects?fundingShortName=FCT
  - ~98,870 FCT projects
  - NOTE: the legacy /search/projects API caps at 10,000 results (page*size<=10k),
    which silently truncates large funders — the existing openaire_fwf ingest is
    capped at ~9k of FWF's ~20k for this reason. The Graph API cursor avoids that.

Output: s3://openalex-ingest/awards/fct/fct_projects.parquet
Output columns (mapped for CreateFCTAwards.ipynb): project_code, title, start_date,
  end_date, funded_amount, total_cost, currency, keywords, website_url, doi,
  funding_program, openaire_id

Usage:
    python fct_to_s3.py --skip-upload          # local
    python fct_to_s3.py --resume               # resume from checkpoint cursor
"""
import argparse, json, os, sys, time, html
from pathlib import Path
from datetime import datetime, timezone
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception: pass
import urllib.request, urllib.parse
import pandas as pd

S3_BUCKET="openalex-ingest"; S3_KEY="awards/fct/fct_projects.parquet"
API="https://api.openaire.eu/graph/v1/projects"
FUNDER="FCT"; PAGE_SIZE=100; DELAY=0.25

def _get(cursor, tries=6):
    qs=urllib.parse.urlencode({"fundingShortName":FUNDER,"pageSize":PAGE_SIZE,"cursor":cursor})
    url=f"{API}?{qs}"
    for a in range(tries):
        try:
            raw=urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"OpenAlex-FCT/1.0 (contact@openalex.org)"}),timeout=45).read()
            return json.loads(raw)
        except Exception as e:
            if a==tries-1: raise
            time.sleep(min(60,2**a))

def _v(x): return x if x not in ("",None) else None

def parse(rec):
    g=rec.get("granted") or {}
    fund=(rec.get("fundings") or [])
    fs=(fund[0].get("fundingStream") or {}) if fund else {}
    prog=fs.get("id") or fs.get("description")
    if prog: prog=html.unescape(prog)
    title=rec.get("title")
    if title: title=html.unescape(title)
    kw=rec.get("keywords")
    kw=html.unescape(kw) if isinstance(kw,str) else (";".join(kw) if isinstance(kw,list) else None)
    return {
        "project_code": _v(rec.get("code")),
        "title": _v(title),
        "start_date": _v(rec.get("startDate")),
        "end_date": _v(rec.get("endDate")),
        "funded_amount": g.get("fundedAmount"),
        "total_cost": g.get("totalCost"),
        "currency": _v(g.get("currency")),
        "keywords": _v(kw),
        "website_url": _v(rec.get("websiteUrl")),
        "doi": None,
        "funding_program": _v(prog),
        "openaire_id": _v(rec.get("id")),
    }

def download(output_dir: Path, resume=False):
    ckpt=output_dir/"fct_cursor.json"; partial=output_dir/"fct_partial.jsonl"
    cursor="*"; rows=[]
    if resume and ckpt.exists():
        st=json.load(open(ckpt)); cursor=st["cursor"]
        if partial.exists():
            rows=[json.loads(l) for l in open(partial,encoding="utf-8")]
        print(f"[resume] cursor={cursor[:30]} rows={len(rows)}")
    first=_get("*"); total=first.get("header",{}).get("numFound")
    print(f"Found {total:,} FCT projects (Graph API, cursor)")
    pf=open(partial,"a",encoding="utf-8")
    page=0; t0=time.time()
    while True:
        d=_get(cursor)
        res=d.get("results") or []
        if not res: break
        for r in res:
            rec=parse(r); rows.append(rec); pf.write(json.dumps(rec,ensure_ascii=False)+"\n")
        pf.flush()
        nxt=d.get("header",{}).get("nextCursor")
        page+=1
        if page%20==0:
            el=time.time()-t0; print(f"  page {page} | {len(rows):,}/{total:,} | {el:.0f}s",flush=True)
            json.dump({"cursor":nxt or cursor},open(ckpt,"w"))
        if not nxt or nxt==cursor: break
        cursor=nxt; time.sleep(DELAY)
    pf.close()
    df=pd.DataFrame(rows).drop_duplicates(subset=["openaire_id"])
    for c in df.columns:
        if df[c].dtype==object: df[c]=df[c].astype("string")
    df["ingested_at"]=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return df

def check_no_shrink(df, allow_shrink):
    """§1.4: refuse to upload a parquet smaller than the one currently in S3 unless --allow-shrink."""
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
        print(f"  §1.4 shrink-check OK (new {len(df):,} >= existing {len(prev):,})")
    except SystemExit:
        raise
    except Exception as e:
        print(f"  §1.4 shrink-check: no prior object / not comparable ({type(e).__name__})")

def main():
    ap=argparse.ArgumentParser(description="FCT (OpenAIRE Graph API) -> S3")
    ap.add_argument("--output-dir",type=Path,default=Path("/tmp/fct_download"))
    ap.add_argument("--skip-upload",action="store_true"); ap.add_argument("--resume",action="store_true")
    ap.add_argument("--allow-shrink",action="store_true",help="§1.4 escape hatch")
    a=ap.parse_args(); a.output_dir.mkdir(parents=True,exist_ok=True)
    print("="*60+f"\nFCT (Fundação para a Ciência e a Tecnologia) — OpenAIRE Graph API\n"+"="*60)
    df=download(a.output_dir,resume=a.resume)
    out=a.output_dir/"fct_projects.parquet"; df.to_parquet(out,index=False)
    print(f"\nSaved {out.name}: {len(df):,} rows, {out.stat().st_size/1e6:.0f} MB")
    nn=lambda c: 100*df[c].notna().mean()
    amtcov=100*(pd.to_numeric(df['funded_amount'],errors='coerce')>0).mean()
    print(f"coverage: code={nn('project_code'):.1f}% title={nn('title'):.1f}% amount={amtcov:.1f}% start={nn('start_date'):.1f}%")
    if not a.skip_upload:
        check_no_shrink(df, a.allow_shrink)
        import subprocess,shutil
        aws=shutil.which("aws")
        if aws: subprocess.run([aws,"s3","cp",str(out),f"s3://{S3_BUCKET}/{S3_KEY}"],check=False)
        else: print(f"  [manual] aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
    print("\nNext: notebooks/awards/CreateFCTAwards.ipynb in Databricks")

if __name__=="__main__": main()
