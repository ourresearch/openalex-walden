#!/usr/bin/env python3
"""
Fetch OpenAlex data for random IDs and coerce to result object format.
Batches API calls (50 per request) for efficiency.
Supports checkpointing - can resume from where it left off.

Usage:
    python get_openalex.py <input_file> <output_file>
    python get_openalex.py ../10k_random/openalex_random_ids.txt ../10k_random/openalex.tsv
"""

import argparse
import json
import os
import time
import requests

# Configuration
OPENALEX_API = "https://api.openalex.org/works"
BATCH_SIZE = 50

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

def load_input_ids(filepath):
    """Load OpenAlex IDs from input file."""
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def init_tsv(filepath):
    """Initialize TSV with header if it doesn't exist."""
    if not os.path.exists(filepath):
        with open(filepath, 'w') as f:
            f.write("openalex_id\tyear\tdoi\tdata\n")

def append_result(filepath, openalex_id, year, doi, data):
    """Append a result to the TSV."""
    with open(filepath, 'a') as f:
        f.write(f"{openalex_id}\t{year}\t{doi}\t{data}\n")

def inverted_index_to_abstract(inverted_index):
    """Convert OpenAlex inverted abstract index to regular abstract string."""
    if not inverted_index:
        return None
    
    # Build list of (position, word) tuples
    word_positions = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))
    
    # Sort by position and join
    word_positions.sort(key=lambda x: x[0])
    return " ".join(word for _, word in word_positions)

def build_result_object(authorships, abstract_inverted_index):
    """Build result object JSON from OpenAlex data."""
    authors = []
    for authorship in (authorships or []):
        author_obj = {
            "name": authorship.get("author", {}).get("display_name", ""),
            "affiliations": authorship.get("raw_affiliation_strings", []) or [],
            "is_corresponding": authorship.get("is_corresponding", False) or False
        }
        authors.append(author_obj)
    
    abstract = inverted_index_to_abstract(abstract_inverted_index)
    
    return json.dumps({"authors": authors, "abstract": abstract}, ensure_ascii=False)

def get_works_batch(openalex_ids, api_key):
    """Fetch work data for a batch of OpenAlex IDs. Returns dict {openalex_id: work_data} and rate_limited flag."""
    # Extract work IDs (W...) from full URLs
    work_ids = [id_.split('/')[-1] for id_ in openalex_ids]
    filter_str = "|".join(work_ids)
    
    params = {
        "filter": f"openalex_id:{filter_str}",
        "select": "id,doi,publication_year,authorships,abstract_inverted_index",
        "per-page": BATCH_SIZE
    }
    if api_key:
        params["api_key"] = api_key
    
    headers = {"User-Agent": "OpenAlexEvals/2.0 (mailto:team@ourresearch.org)"}
    
    try:
        response = requests.get(OPENALEX_API, params=params, headers=headers, timeout=60)
        if response.status_code == 429:
            return None, True
        response.raise_for_status()
        data = response.json()
        
        # Build dict mapping openalex_id -> work data
        result = {}
        for item in data.get("results", []):
            oa_id = item.get("id")
            if oa_id:
                result[oa_id] = {
                    "doi": item.get("doi"),
                    "year": item.get("publication_year"),
                    "authorships": item.get("authorships"),
                    "abstract_inverted_index": item.get("abstract_inverted_index")
                }
        
        return result, False
    except requests.exceptions.RequestException as e:
        print(f"  Batch request error: {e}")
        return None, False

def format_time(seconds):
    """Format seconds into human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.1f}h"

def main():
    parser = argparse.ArgumentParser(description="Fetch OpenAlex data and coerce to result object format")
    parser.add_argument("input_file", help="Input file with OpenAlex IDs (one per line)")
    parser.add_argument("output_file", help="Output TSV file")
    args = parser.parse_args()
    
    input_file = args.input_file
    output_file = args.output_file
    
    api_key = os.environ.get("OPENALEX_API_KEY")
    if not api_key:
        print("Warning: OPENALEX_API_KEY not set, will use polite pool")
    
    # Load data
    all_ids = load_input_ids(input_file)
    completed_ids = load_completed_ids(output_file)
    init_tsv(output_file)
    
    remaining_ids = [id_ for id_ in all_ids if id_ not in completed_ids]
    total = len(all_ids)
    already_done = len(completed_ids)
    
    print(f"Total IDs: {total}")
    print(f"Already processed: {already_done}")
    print(f"Remaining: {len(remaining_ids)}")
    
    if not remaining_ids:
        print("All IDs already processed!")
        return
    
    start_time = time.time()
    processed = 0
    backoff_count = 0
    total_429_errors = 0
    
    # Process in batches
    for batch_start in range(0, len(remaining_ids), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(remaining_ids))
        batch_ids = remaining_ids[batch_start:batch_end]
        
        # Exponential backoff if needed
        if backoff_count > 0:
            wait_time = min(2 ** backoff_count, 60)
            print(f"  Rate limited! Waiting {wait_time}s (backoff level {backoff_count})...")
            time.sleep(wait_time)
        
        # Get work data for batch from OpenAlex
        works_map, rate_limited = get_works_batch(batch_ids, api_key)
        
        if rate_limited:
            backoff_count += 1
            total_429_errors += 1
            continue
        
        if works_map is None:
            # Error occurred, retry batch
            time.sleep(5)
            continue
        
        backoff_count = 0
        
        # Process each ID in batch and write to TSV (checkpoint)
        for openalex_id in batch_ids:
            work_data = works_map.get(openalex_id, {})
            doi = work_data.get("doi") or ""
            year = work_data.get("year") or ""
            authorships = work_data.get("authorships")
            abstract_inverted_index = work_data.get("abstract_inverted_index")
            
            data = build_result_object(authorships, abstract_inverted_index)
            
            # Write immediately (checkpoint)
            append_result(output_file, openalex_id, year, doi, data)
            processed += 1
        
        # Progress report (per batch)
        total_processed = already_done + processed
        elapsed = time.time() - start_time
        
        if processed > 0:
            rate = processed / elapsed
            remaining = len(remaining_ids) - processed
            eta = remaining / rate if rate > 0 else float('inf')
        else:
            eta = float('inf')
        
        backoff_status = f" | 429s: {total_429_errors}" if total_429_errors > 0 else ""
        print(f"[{total_processed}/{total}] batch {batch_start//BATCH_SIZE + 1} | "
              f"Elapsed: {format_time(elapsed)} | ETA: {format_time(eta)}{backoff_status}")
    
    elapsed = time.time() - start_time
    print(f"\nDone! Processed {processed} IDs in {format_time(elapsed)}")
    print(f"Total 429 errors: {total_429_errors}")

if __name__ == "__main__":
    main()
