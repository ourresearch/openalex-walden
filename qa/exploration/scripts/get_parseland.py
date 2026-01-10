#!/usr/bin/env python3
"""
Fetch Parseland data using DOIs from openalex.tsv.
1. Query Taxicab by DOI to get html_uuid
2. Call Parseland with html_uuid to get parsed data
Uses multithreading with max 10 concurrent requests.
Supports checkpointing - can resume from where it left off.

Usage:
    python get_parseland.py <input_file> <output_file>
    python get_parseland.py ../10k_random/openalex.tsv ../10k_random/parseland.tsv
"""

import argparse
import json
import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Configuration
TAXICAB_BASE = "http://harvester-load-balancer-366186003.us-east-1.elb.amazonaws.com/taxicab"
PARSELAND_BASE = "http://parseland-load-balancer-667160048.us-east-1.elb.amazonaws.com/parseland"
MAX_WORKERS = 10

# Thread-safe progress tracking
progress_lock = Lock()
progress = {
    "processed": 0,
    "success": 0,
    "no_html": 0,
    "errors": 0
}

def load_completed_ids(filepath):
    """Load already processed OpenAlex IDs from the TSV."""
    if not os.path.exists(filepath):
        return set()
    completed = set()
    with open(filepath, 'r', newline='') as f:
        next(f, None)  # skip header
        for line in f:
            if line.strip():
                completed.add(line.split('\t')[0])
    return completed

def load_input_data(filepath):
    """Load openalex_id and doi from openalex.tsv."""
    if not os.path.exists(filepath):
        return []
    
    rows = []
    with open(filepath, 'r') as f:
        next(f, None)  # skip header
        for line in f:
            if line.strip():
                parts = line.strip().split('\t')
                if len(parts) >= 3:
                    openalex_id = parts[0]
                    doi = parts[2]  # doi column
                    rows.append({"openalex_id": openalex_id, "doi": doi})
    return rows

def init_tsv(filepath):
    """Initialize TSV with header if it doesn't exist."""
    if not os.path.exists(filepath):
        with open(filepath, 'w') as f:
            f.write("openalex_id\thtml_uuid\tdata\n")

def is_pdf_url(url):
    """Check if URL points to a PDF rather than an HTML page."""
    url_lower = url.lower()
    return '/pdf' in url_lower or url_lower.endswith('.pdf')

def get_html_uuid_from_taxicab(doi):
    """Query Taxicab by DOI for HTML UUID. Returns (uuid, error).

    Skips PDF URLs and prefers actual HTML landing pages.
    """
    if not doi:
        return None, "no_doi"

    # Extract DOI path (remove https://doi.org/ prefix if present)
    if doi.startswith("https://doi.org/"):
        doi = doi[16:]

    url = f"{TAXICAB_BASE}/doi/{doi}"

    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 429:
            return None, "rate_limited"
        if response.status_code == 404:
            return None, "not_found"
        response.raise_for_status()
        data = response.json()

        # Get HTML entries, preferring non-PDF URLs
        html_items = data.get("html", [])
        for item in html_items:
            item_url = item.get("url", "")
            if is_pdf_url(item_url):
                continue  # Skip PDF URLs
            download_url = item.get("download_url", "")
            if download_url:
                return download_url.split("/")[-1], None

        # Fallback: if all entries are PDFs, try the first one anyway
        if html_items:
            download_url = html_items[0].get("download_url", "")
            if download_url:
                return download_url.split("/")[-1], None

        return None, "no_html"
    except requests.exceptions.RequestException as e:
        return None, str(e)

def fetch_parseland(html_uuid):
    """Fetch Parseland response for an html_uuid. Returns (data, error)."""
    if not html_uuid:
        return None, "no_uuid"
    
    url = f"{PARSELAND_BASE}/{html_uuid}"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 429:
            return None, "rate_limited"
        if response.status_code == 404:
            return None, "not_found"
        response.raise_for_status()
        data = response.json()
        return data, None
    except requests.exceptions.RequestException as e:
        return None, str(e)
    except json.JSONDecodeError as e:
        return None, f"json_error: {e}"

def coerce_parseland_response(parseland_data):
    """Coerce Parseland response to canonical result object schema."""
    if not parseland_data:
        return None
    
    authors = []
    for author in parseland_data.get("authors", []):
        # Convert affiliations from objects to strings
        affiliations_raw = author.get("affiliations", []) or []
        affiliations = []
        for aff in affiliations_raw:
            if isinstance(aff, dict):
                affiliations.append(aff.get("name", ""))
            elif isinstance(aff, str):
                affiliations.append(aff)
        
        author_obj = {
            "name": author.get("name", ""),
            "affiliations": affiliations,
            "is_corresponding": author.get("is_corresponding", False) or False
        }
        authors.append(author_obj)
    
    abstract = parseland_data.get("abstract")
    
    return {"authors": authors, "abstract": abstract}

def process_row(row_data, output_file, file_lock):
    """Process a single row: get html_uuid from Taxicab, then get Parseland data."""
    openalex_id = row_data["openalex_id"]
    doi = row_data["doi"]
    
    # Step 1: Get html_uuid from Taxicab
    html_uuid, taxicab_error = get_html_uuid_from_taxicab(doi)
    
    if taxicab_error == "rate_limited":
        return {"status": "rate_limited", "openalex_id": openalex_id}
    
    if not html_uuid:
        # No HTML available - still record this row
        with file_lock:
            with open(output_file, 'a') as f:
                f.write(f"{openalex_id}\t\t\n")
        with progress_lock:
            progress["processed"] += 1
            progress["no_html"] += 1
        return {"status": "no_html", "openalex_id": openalex_id}
    
    # Step 2: Get Parseland data
    parseland_data, parseland_error = fetch_parseland(html_uuid)
    
    if parseland_error == "rate_limited":
        return {"status": "rate_limited", "openalex_id": openalex_id}
    
    if parseland_error:
        # Error but we have html_uuid
        with file_lock:
            with open(output_file, 'a') as f:
                f.write(f"{openalex_id}\t{html_uuid}\t\n")
        with progress_lock:
            progress["processed"] += 1
            progress["errors"] += 1
        return {"status": "error", "openalex_id": openalex_id, "error": parseland_error}
    
    # Success - coerce and save
    coerced = coerce_parseland_response(parseland_data)
    data_json = json.dumps(coerced, ensure_ascii=False) if coerced else ""
    
    with file_lock:
        with open(output_file, 'a') as f:
            f.write(f"{openalex_id}\t{html_uuid}\t{data_json}\n")
    
    with progress_lock:
        progress["processed"] += 1
        progress["success"] += 1
    
    return {"status": "success", "openalex_id": openalex_id}

def format_time(seconds):
    """Format seconds into human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.1f}h"

def main():
    parser = argparse.ArgumentParser(description="Fetch Parseland data using DOIs from openalex.tsv")
    parser.add_argument("input_file", help="Input TSV file (openalex.tsv format with doi column)")
    parser.add_argument("output_file", help="Output TSV file")
    args = parser.parse_args()
    
    input_file = args.input_file
    output_file = args.output_file
    
    # Load data
    all_rows = load_input_data(input_file)
    completed_ids = load_completed_ids(output_file)
    init_tsv(output_file)
    
    remaining_rows = [r for r in all_rows if r["openalex_id"] not in completed_ids]
    total = len(all_rows)
    already_done = len(completed_ids)
    
    print(f"Total IDs in {input_file}: {total}")
    print(f"Already processed: {already_done}")
    print(f"Remaining: {len(remaining_rows)}")
    print(f"Max concurrent requests: {MAX_WORKERS}")
    
    if not remaining_rows:
        print("All IDs already processed!")
        return
    
    start_time = time.time()
    file_lock = Lock()
    backoff_count = 0
    
    # Process in batches with ThreadPoolExecutor
    batch_size = MAX_WORKERS * 5
    
    for batch_start in range(0, len(remaining_rows), batch_size):
        batch_end = min(batch_start + batch_size, len(remaining_rows))
        batch = remaining_rows[batch_start:batch_end]
        
        # Exponential backoff if needed
        if backoff_count > 0:
            wait_time = min(2 ** backoff_count, 60)
            print(f"  Rate limited! Waiting {wait_time}s (backoff level {backoff_count})...")
            time.sleep(wait_time)
        
        rate_limited_in_batch = False
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_row, row, output_file, file_lock): row for row in batch}
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result["status"] == "rate_limited":
                        rate_limited_in_batch = True
                except Exception as e:
                    with progress_lock:
                        progress["processed"] += 1
                        progress["errors"] += 1
                    print(f"  Error processing row: {e}")
        
        if rate_limited_in_batch:
            backoff_count += 1
            continue
        else:
            backoff_count = 0
        
        # Progress report
        elapsed = time.time() - start_time
        with progress_lock:
            p = progress.copy()
        
        if p["processed"] > 0:
            rate = p["processed"] / elapsed
            remaining = len(remaining_rows) - p["processed"]
            eta = remaining / rate if rate > 0 else float('inf')
        else:
            eta = float('inf')
        
        print(f"[{already_done + p['processed']}/{total}] success: {p['success']}, no_html: {p['no_html']}, errors: {p['errors']} | "
              f"Elapsed: {format_time(elapsed)} | ETA: {format_time(eta)}")
    
    elapsed = time.time() - start_time
    with progress_lock:
        p = progress.copy()
    print(f"\nDone! Processed {p['processed']} rows in {format_time(elapsed)}")
    print(f"Success: {p['success']}, No HTML: {p['no_html']}, Errors: {p['errors']}")

if __name__ == "__main__":
    main()
