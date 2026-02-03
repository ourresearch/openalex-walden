#!/usr/bin/env python3
"""
Download Alfred P. Sloan Foundation grants and upload to S3.

Uses agent-browser for scraping. Start browser first:
    npx agent-browser --headed open "https://sloan.org/grants-database"

Output: s3://openalex-ingest/awards/sloan/sloan_projects.parquet
"""

import json
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/sloan/sloan_projects.parquet"
OUTPUT_JSON = Path("/tmp/sloan_grants.json")
OUTPUT_PARQUET = Path("/tmp/sloan_grants.parquet")

# Compact JS - use raw string with proper escaping for subprocess
# Note: \$ in raw string is passed as $ to JS, \d is passed as \d for regex
EXTRACT_JS = r'''
const g=[];
document.querySelectorAll('main li').forEach(li=>{
    const t=li.innerText;
    if(t.includes('$')&&/20\d{2}/.test(t)){
        const l=t.split('\n').map(x=>x.trim()).filter(x=>x);
        const link=li.querySelector('a[href*="grant-detail"]');
        let city='',state='',country='';
        const loc=l.find(x=>x.includes(', ')&&!x.startsWith('$'));
        if(loc){
            const p=loc.split(', ');
            city=p.slice(0,-1).join(', ');
            const last=p[p.length-1];
            if(last.length===2&&last===last.toUpperCase()){state=last;country='United States';}
            else{country=last;}
        }
        let desc='',prog='',sub='',init='',inv='';
        for(let i=4;i<l.length;i++){
            const x=l[i];
            if(x==='PERMALINK'||x==='CLOSE'||x==='MORE')break;
            if(l[i-1]==='PROGRAM')prog=x;
            else if(l[i-1]==='SUB-PROGRAM')sub=x;
            else if(l[i-1]==='INITIATIVE')init=x;
            else if(l[i-1]==='INVESTIGATOR')inv=x;
            else if(!desc&&!['PROGRAM','SUB-PROGRAM','INITIATIVE','INVESTIGATOR'].includes(x))desc=x;
        }
        g.push({
            grant_id:link?(link.href.match(/g-(\d{4}-\d+)/)||[])[1]||null:null,
            grantee:l[0]||null,
            amount:parseFloat((l.find(x=>x.startsWith('$'))||'').replace(/[$,]/g,''))||null,
            city:city||null,state:state||null,country:country||null,
            year:parseInt(l.find(x=>/^20\d{2}$/.test(x)))||null,
            description:desc||null,program:prog||null,sub_program:sub||null,
            initiative:init||null,investigator:inv||null
        });
    }
});
JSON.stringify(g);
'''


def run_cmd(cmd: list, timeout: int = 30) -> str:
    """Run command and return stdout."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        return ""


def extract_grants() -> list:
    """Extract grants from current page."""
    output = run_cmd(['npx', 'agent-browser', 'eval', EXTRACT_JS])
    try:
        raw = output.strip()
        if raw.startswith('"') and raw.endswith('"'):
            # Output is double-quoted JSON string - unescape first
            inner = json.loads(raw)
            return json.loads(inner)
        else:
            return json.loads(raw)
    except json.JSONDecodeError:
        pass
    return []


def click_next_page(current_page: int) -> bool:
    """Click to navigate to next page."""
    next_page = current_page + 1
    js = f"const l=document.querySelector('a[href*=\"page={next_page}\"]');if(l){{l.click();true;}}else{{false;}}"
    output = run_cmd(['npx', 'agent-browser', 'eval', js])
    return "true" in output.lower()


def main():
    print(f"[Start] {datetime.now().isoformat()}")
    print("=" * 60)

    # Check browser state
    title = run_cmd(['npx', 'agent-browser', 'get', 'title'])
    if "Grants Database" not in title:
        print(f"[Error] Not on grants page. Title: {title}")
        print("[Info] First run: npx agent-browser --headed open 'https://sloan.org/grants-database'")
        sys.exit(1)

    print("[OK] Browser on grants database page")

    all_grants = []
    seen_ids = set()
    max_pages = 336
    page = 1
    consecutive_empty = 0

    while page <= max_pages and consecutive_empty < 3:
        grants = extract_grants()

        # Deduplicate
        new_grants = []
        for g in grants:
            gid = g.get('grant_id') or f"{g.get('grantee')}_{g.get('year')}_{g.get('amount')}"
            if gid not in seen_ids:
                seen_ids.add(gid)
                new_grants.append(g)

        if new_grants:
            all_grants.extend(new_grants)
            consecutive_empty = 0
            print(f"[Page {page}] +{len(new_grants)} grants, total: {len(all_grants)}")
        else:
            consecutive_empty += 1
            print(f"[Page {page}] No new grants (empty: {consecutive_empty})")

        # Save checkpoint every 10 pages
        if page % 10 == 0:
            with open(OUTPUT_JSON, 'w') as f:
                json.dump(all_grants, f)
            print(f"[Checkpoint] Saved {len(all_grants)} grants")

        # Check if we have all grants
        if len(all_grants) >= 3358:
            print("[Done] Reached expected grant count")
            break

        # Navigate to next page
        if not click_next_page(page):
            print("[Done] No more pages")
            break

        page += 1
        time.sleep(2)

    # Save final JSON
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(all_grants, f, indent=2)
    print(f"\n[Saved] {OUTPUT_JSON}")

    # Convert to DataFrame
    df = pd.DataFrame(all_grants)
    df["source_url"] = "https://sloan.org/grants-database"
    df["currency"] = "USD"
    df["scraped_at"] = datetime.utcnow().isoformat()

    # Stats
    print(f"\n[Stats]")
    print(f"  Total grants: {len(df)}")
    if len(df) > 0:
        print(f"  Years: {df['year'].min()} - {df['year'].max()}")
        print(f"  Total amount: ${df['amount'].sum():,.0f}")
        print(f"  Unique grantees: {df['grantee'].nunique()}")

    # Save parquet
    df.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"[Saved] {OUTPUT_PARQUET}")

    # Upload to S3
    print(f"\n[S3] Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3 = boto3.client("s3")
    s3.upload_file(str(OUTPUT_PARQUET), S3_BUCKET, S3_KEY)
    print("[S3] Upload complete")

    print(f"\n[Complete] {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
