#!/usr/bin/env python3
"""
Scrape Sloan Foundation grants using agent-browser with click navigation.

This script uses click-based pagination to bypass Cloudflare protection.
Run agent-browser --headed first to pass the initial Cloudflare challenge.

Usage:
    # First open the grants database in headed mode:
    npx agent-browser --headed open "https://sloan.org/grants-database"

    # Then run this script:
    python3 sloan_scrape_click.py
"""

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import boto3
import pandas as pd

# Configuration
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/sloan/sloan_projects.parquet"
OUTPUT_FILE = Path("/tmp/sloan_grants.json")
PARQUET_FILE = Path("/tmp/sloan_grants.parquet")

EXTRACT_JS = '''
const grants = [];
document.querySelectorAll('main ul > li, main ol > li').forEach(li => {
    const text = li.innerText;
    if (text.includes('$') && /20\\d{2}/.test(text)) {
        const lines = text.split('\\n').map(l => l.trim()).filter(l => l);
        const permalink = li.querySelector('a[href*="grant-detail"]');

        const firstLine = lines[0] || '';
        const amountMatch = firstLine.match(/\\$([0-9,]+)/);
        const yearMatch = firstLine.match(/\\b(20\\d{2})\\b/);
        const locationMatch = firstLine.match(/\\$[0-9,]+\\s+(.+?)\\s+20\\d{2}/);
        const granteeMatch = firstLine.match(/^(.+?)\\s*\\$/);

        let description = '', program = '', subProgram = '', initiative = '', investigator = '';
        for (let i = 1; i < lines.length; i++) {
            const line = lines[i];
            if (line === 'PERMALINK' || line === 'CLOSE' || line === 'MORE') break;
            if (lines[i-1] === 'PROGRAM') program = line;
            else if (lines[i-1] === 'SUB-PROGRAM') subProgram = line;
            else if (lines[i-1] === 'INITIATIVE') initiative = line;
            else if (lines[i-1] === 'INVESTIGATOR') investigator = line;
            else if (!description && !['PROGRAM','SUB-PROGRAM','INITIATIVE','INVESTIGATOR'].includes(line)) {
                description = line;
            }
        }

        let city = '', state = '', country = '';
        if (locationMatch) {
            const loc = locationMatch[1].trim();
            if (loc.includes(', ')) {
                const parts = loc.split(', ');
                city = parts.slice(0, -1).join(', ');
                const last = parts[parts.length - 1];
                if (last.length === 2 && last === last.toUpperCase()) {
                    state = last;
                    country = 'United States';
                } else {
                    country = last;
                }
            } else {
                city = loc;
            }
        }

        grants.push({
            grant_id: permalink ? (permalink.href.match(/g-(\\d{4}-\\d+)/) || [])[1] || null : null,
            grantee: granteeMatch ? granteeMatch[1].trim() : null,
            amount: amountMatch ? parseFloat(amountMatch[1].replace(/,/g, '')) : null,
            city: city || null,
            state: state || null,
            country: country || null,
            year: yearMatch ? parseInt(yearMatch[1]) : null,
            description: description || null,
            program: program || null,
            sub_program: subProgram || null,
            initiative: initiative || null,
            investigator: investigator || null
        });
    }
});
JSON.stringify(grants);
'''


def run_agent_browser(cmd: str, timeout: int = 30) -> str:
    """Run an agent-browser command and return output."""
    result = subprocess.run(
        f"npx agent-browser {cmd}",
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout
    )
    return result.stdout.strip()


def extract_grants_from_page() -> list:
    """Extract grants from current page using JavaScript."""
    # Escape the JS for shell
    js_escaped = EXTRACT_JS.replace("'", "'\\''")
    output = run_agent_browser(f"eval '{js_escaped}'")

    # Parse the JSON output
    try:
        # Find the JSON array in the output
        match = re.search(r'\[.*\]', output, re.DOTALL)
        if match:
            return json.loads(match.group())
    except json.JSONDecodeError as e:
        print(f"[Warn] JSON parse error: {e}")

    return []


def get_next_page_ref() -> str | None:
    """Get the ref for the 'next page' link."""
    output = run_agent_browser("snapshot -i")
    match = re.search(r'link "Click for next page[^"]*" \[ref=(e\d+)\]', output)
    return match.group(1) if match else None


def click_next_page(ref: str) -> bool:
    """Click the next page link."""
    output = run_agent_browser(f"click @{ref}")
    time.sleep(2)  # Wait for page load
    return "Done" in output or "✓" in output


def scrape_all_grants():
    """Scrape all grants using click navigation."""
    all_grants = []
    page = 1
    max_pages = 68  # 3358 / 50 per page

    print(f"[Start] Scraping Sloan Foundation grants")
    print(f"[Info] Expected: ~{max_pages} pages, ~3,358 grants")

    while page <= max_pages:
        # Extract grants from current page
        grants = extract_grants_from_page()

        if not grants:
            print(f"[Page {page}] No grants found, checking if page loaded...")
            # Check title
            title = run_agent_browser("get title")
            if "Just a moment" in title:
                print(f"[Error] Cloudflare challenge on page {page}")
                break
            if "Grants Database" not in title:
                print(f"[Error] Unexpected page: {title}")
                break

        all_grants.extend(grants)
        print(f"[Page {page}/{max_pages}] Scraped {len(grants)} grants, total: {len(all_grants)}")

        # Save checkpoint every 5 pages
        if page % 5 == 0:
            with open(OUTPUT_FILE, 'w') as f:
                json.dump(all_grants, f)
            print(f"[Checkpoint] Saved {len(all_grants)} grants")

        # Check if we've scraped enough
        if len(all_grants) >= 3358:
            print("[Done] Reached expected grant count")
            break

        # Find and click next page
        next_ref = get_next_page_ref()
        if not next_ref:
            print("[Done] No more pages")
            break

        if not click_next_page(next_ref):
            print("[Error] Failed to click next page")
            break

        page += 1
        time.sleep(1)  # Rate limiting

    return all_grants


def upload_to_s3(df: pd.DataFrame):
    """Upload DataFrame to S3."""
    print(f"[S3] Uploading {len(df)} grants to s3://{S3_BUCKET}/{S3_KEY}")

    df.to_parquet(PARQUET_FILE, index=False)

    s3 = boto3.client("s3")
    s3.upload_file(str(PARQUET_FILE), S3_BUCKET, S3_KEY)

    print(f"[S3] Upload complete")


def main():
    # Check if agent-browser is on the grants page
    title = run_agent_browser("get title")
    if "Grants Database" not in title:
        print(f"[Error] Not on grants page. Current title: {title}")
        print("[Info] First run: npx agent-browser --headed open 'https://sloan.org/grants-database'")
        sys.exit(1)

    print("[OK] On grants database page")

    # Scrape all grants
    grants = scrape_all_grants()

    if not grants:
        print("[Error] No grants scraped!")
        sys.exit(1)

    # Save final JSON
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(grants, f, indent=2)
    print(f"[Saved] {OUTPUT_FILE}")

    # Convert to DataFrame
    df = pd.DataFrame(grants)
    df["source_url"] = "https://sloan.org/grants-database"
    df["currency"] = "USD"

    # Stats
    print(f"\n[Stats]")
    print(f"  Total grants: {len(df)}")
    print(f"  Years: {df['year'].min()} - {df['year'].max()}")
    print(f"  Total amount: ${df['amount'].sum():,.0f}")
    print(f"  Unique grantees: {df['grantee'].nunique()}")

    # Upload to S3
    upload_to_s3(df)

    print("\n[Complete]")


if __name__ == "__main__":
    main()
