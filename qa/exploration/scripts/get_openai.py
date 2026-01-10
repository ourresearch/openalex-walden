#!/usr/bin/env python3
"""
Fetch OpenAI responses for rows with html_uuid in parseland.tsv.
Uses OpenAI Responses API with stored prompt.
Rate-limited dispatcher pattern: central loop checks capacity and dispatches jobs.
Supports checkpointing - can resume from where it left off.

Usage:
    python get_openai.py <input_file> <output_file>
    python get_openai.py ../10k_random/parseland.tsv ../10k_random/openai.tsv
"""

import argparse
import json
import os
import time
import requests
import copy
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Lock
from datetime import datetime
from typing import Optional, Dict, List, Tuple

# Configuration
TAXICAB_BASE = "http://harvester-load-balancer-366186003.us-east-1.elb.amazonaws.com/taxicab"
OPENAI_API_URL = "https://api.openai.com/v1/responses"

# Rate limit config - be conservative to avoid 429s
DISPATCH_INTERVAL = 1.0  # Check capacity every N seconds
TOKEN_BUFFER = 50000     # Keep this many tokens as buffer
CHARS_PER_TOKEN = 4      # Rough estimate for token counting

# OpenAI request template
OPENAI_REQUEST_TEMPLATE = {
    "prompt": {
        "id": "pmpt_6945c2dd2bb48196b713447af032062c0ba6caf6009aa6f9"
    },
    "input": [],
    "text": {
        "format": {
            "type": "json_schema",
            "name": "scholarly_landing_page_metadata",
            "strict": True,
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "authors": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "name": {"type": "string"},
                                "affiliations": {
                                    "type": "array",
                                    "items": {"type": "string"}
                                },
                                "is_corresponding": {"type": "boolean"}
                            },
                            "required": ["name", "affiliations", "is_corresponding"]
                        }
                    },
                    "PDF URL": {
                        "anyOf": [{"type": "string"}, {"type": "null"}]
                    },
                    "license": {
                        "anyOf": [{"type": "string"}, {"type": "null"}]
                    },
                    "abstract": {
                        "anyOf": [{"type": "string"}, {"type": "null"}]
                    }
                },
                "required": ["authors", "PDF URL", "license", "abstract"]
            }
        },
        "verbosity": "low"
    },
    "reasoning": {"summary": "auto"},
    "store": True,
    "include": [
        "reasoning.encrypted_content",
        "web_search_call.action.sources"
    ]
}

# Thread-safe state
state_lock = Lock()
file_lock = Lock()
rate_limit_lock = Lock()

state = {
    "processed": 0,
    "success": 0,
    "errors": 0,
    "total_tokens_up": 0,
    "total_tokens_down": 0,
    "in_flight_tokens": 0,  # Estimated tokens currently being processed
    "in_flight_count": 0    # Number of requests in flight
}

# Rate limit tracking (updated from API responses)
rate_limits = {
    "limit_requests": 500,      # RPM limit
    "limit_tokens": 800000,     # TPM limit
    "remaining_requests": 500,
    "remaining_tokens": 800000,
    "reset_requests_at": 0,     # Unix timestamp
    "reset_tokens_at": 0,       # Unix timestamp
    "last_updated": 0
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
    """Load openalex_id and html_uuid from parseland.tsv."""
    if not os.path.exists(filepath):
        return []
    
    rows = []
    with open(filepath, 'r') as f:
        next(f, None)  # skip header
        for line in f:
            if line.strip():
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    openalex_id = parts[0]
                    html_uuid = parts[1] if len(parts) > 1 else ""
                    if html_uuid:  # Only include rows with html_uuid
                        rows.append({"openalex_id": openalex_id, "html_uuid": html_uuid})
    return rows

def init_tsv(filepath):
    """Initialize TSV with header if it doesn't exist."""
    if not os.path.exists(filepath):
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("openalex_id\ttokens_up\ttokens_down\tdata\n")

def fetch_html_from_taxicab(html_uuid):
    """Fetch HTML content from Taxicab using html_uuid."""
    if not html_uuid:
        return None, "no_uuid"
    
    url = f"{TAXICAB_BASE}/{html_uuid}"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            return None, "not_found"
        response.raise_for_status()
        # Explicitly decode as UTF-8 to avoid Latin-1 default
        response.encoding = 'utf-8'
        return response.text, None
    except requests.exceptions.RequestException as e:
        return None, str(e)

def parse_reset_time(reset_str):
    """Parse reset time string (e.g., '1s', '500ms', '1m30s') to seconds."""
    if not reset_str:
        return 60
    
    total_seconds = 0
    reset_str = reset_str.lower()
    
    # Parse minutes
    if 'm' in reset_str and 'ms' not in reset_str:
        parts = reset_str.split('m')
        try:
            total_seconds += int(parts[0]) * 60
            reset_str = parts[1] if len(parts) > 1 else ''
        except ValueError:
            pass
    
    # Parse seconds
    if 's' in reset_str and 'ms' not in reset_str:
        try:
            s_part = reset_str.replace('s', '').strip()
            if s_part:
                total_seconds += float(s_part)
        except ValueError:
            pass
    
    # Parse milliseconds
    if 'ms' in reset_str:
        try:
            ms_part = reset_str.replace('ms', '').strip()
            if ms_part:
                total_seconds += float(ms_part) / 1000
        except ValueError:
            pass
    
    return max(total_seconds, 1)

def update_rate_limits(headers):
    """Update rate limit tracking from response headers."""
    with rate_limit_lock:
        now = time.time()
        rate_limits["last_updated"] = now
        
        # Parse limit values
        if headers.get("x-ratelimit-limit-requests"):
            try:
                rate_limits["limit_requests"] = int(headers.get("x-ratelimit-limit-requests"))
            except ValueError:
                pass
        if headers.get("x-ratelimit-limit-tokens"):
            try:
                rate_limits["limit_tokens"] = int(headers.get("x-ratelimit-limit-tokens"))
            except ValueError:
                pass
        
        # Parse remaining values
        if headers.get("x-ratelimit-remaining-requests"):
            try:
                rate_limits["remaining_requests"] = int(headers.get("x-ratelimit-remaining-requests"))
            except ValueError:
                pass
        if headers.get("x-ratelimit-remaining-tokens"):
            try:
                rate_limits["remaining_tokens"] = int(headers.get("x-ratelimit-remaining-tokens"))
            except ValueError:
                pass
        
        # Parse reset times into absolute timestamps
        reset_req_str = headers.get("x-ratelimit-reset-requests")
        reset_tok_str = headers.get("x-ratelimit-reset-tokens")
        if reset_req_str:
            rate_limits["reset_requests_at"] = now + parse_reset_time(reset_req_str)
        if reset_tok_str:
            rate_limits["reset_tokens_at"] = now + parse_reset_time(reset_tok_str)

def estimate_tokens(html_content: str) -> int:
    """Estimate token count for HTML content."""
    if not html_content:
        return 0
    return len(html_content) // CHARS_PER_TOKEN + 500  # Add buffer for prompt overhead


def poll_rate_limits(api_key: str) -> bool:
    """Make a minimal API call to get fresh rate limit info from headers.
    
    Returns True if successful, False otherwise.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Minimal request - just empty HTML tags
    request_body = copy.deepcopy(OPENAI_REQUEST_TEMPLATE)
    request_body["prompt"]["variables"] = {"html": "<html></html>"}
    request_body["input"] = []
    
    try:
        response = requests.post(
            OPENAI_API_URL,
            headers=headers,
            json=request_body,
            timeout=30
        )
        # Update rate limits from headers regardless of response status
        update_rate_limits(response.headers)
        return True
    except Exception as e:
        print(f"  Rate limit poll failed: {e}")
        return False


def get_available_capacity() -> Tuple[int, int]:
    """Get available token and request capacity, accounting for in-flight requests."""
    with rate_limit_lock:
        remaining_tokens = rate_limits["remaining_tokens"]
        remaining_requests = rate_limits["remaining_requests"]
    
    with state_lock:
        in_flight_tokens = state["in_flight_tokens"]
        in_flight_count = state["in_flight_count"]
    
    # Subtract in-flight from remaining
    available_tokens = remaining_tokens - in_flight_tokens - TOKEN_BUFFER
    available_requests = remaining_requests - in_flight_count
    
    return max(0, available_tokens), max(0, available_requests)

def can_dispatch(estimated_tokens: int, debug: bool = False) -> bool:
    """Check if we have capacity to dispatch a request with estimated tokens."""
    available_tokens, available_requests = get_available_capacity()
    result = available_tokens >= estimated_tokens and available_requests >= 1
    if debug and not result:
        print(f"    can_dispatch=False: avail_tok={available_tokens}, avail_req={available_requests}, est_tok={estimated_tokens}")
    return result

def reserve_capacity(estimated_tokens: int):
    """Reserve capacity for an in-flight request."""
    with state_lock:
        state["in_flight_tokens"] += estimated_tokens
        state["in_flight_count"] += 1

def release_capacity(estimated_tokens: int, actual_tokens: int = 0):
    """Release capacity when request completes. Adjusts for actual vs estimated."""
    with state_lock:
        state["in_flight_tokens"] -= estimated_tokens
        state["in_flight_count"] -= 1

def call_openai_api(html_content, api_key):
    """Call OpenAI Responses API with HTML content."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    request_body = copy.deepcopy(OPENAI_REQUEST_TEMPLATE)
    request_body["prompt"]["variables"] = {"html": html_content}
    request_body["input"] = []
    
    try:
        response = requests.post(
            OPENAI_API_URL,
            headers=headers,
            json=request_body,
            timeout=120
        )
        
        # Update rate limit tracking
        update_rate_limits(response.headers)
        
        rate_limit_info = {
            "remaining_requests": response.headers.get("x-ratelimit-remaining-requests"),
            "remaining_tokens": response.headers.get("x-ratelimit-remaining-tokens"),
        }
        
        if response.status_code == 429:
            # Set pause time
            retry_after = response.headers.get("retry-after")
            reset_time = parse_reset_time(response.headers.get("x-ratelimit-reset-requests"))
            if retry_after:
                try:
                    reset_time = max(reset_time, float(retry_after))
                except ValueError:
                    pass
            with rate_limit_lock:
                rate_limits["pause_until"] = time.time() + reset_time
            return None, {}, "rate_limited"
        
        if response.status_code != 200:
            error_detail = response.text[:500] if response.text else f"status_{response.status_code}"
            return None, {}, error_detail
        
        data = response.json()
        
        # Extract token usage
        token_usage = {}
        if "usage" in data:
            token_usage["input"] = data["usage"].get("input_tokens", 0)
            token_usage["output"] = data["usage"].get("output_tokens", 0)
        
        # Extract the structured output from response
        output_text = None
        if "output" in data:
            for item in data.get("output", []):
                if item.get("type") == "message":
                    for content in item.get("content", []):
                        if content.get("type") == "output_text":
                            output_text = content.get("text")
                            break
        
        if output_text:
            try:
                result_json = json.loads(output_text)
                return result_json, token_usage, None
            except json.JSONDecodeError as e:
                return None, token_usage, f"json_parse_error: {e}"
        
        if "text" in data:
            try:
                result_json = json.loads(data["text"])
                return result_json, token_usage, None
            except (json.JSONDecodeError, TypeError):
                pass
        
        return None, token_usage, f"unexpected_response_format"
        
    except requests.exceptions.RequestException as e:
        return None, {}, str(e)

def strip_linebreaks(text):
    """Remove all line breaks and carriage returns from text."""
    if not text:
        return text
    return text.replace('\n', ' ').replace('\r', ' ').replace('\u2028', ' ').replace('\u2029', ' ')

def coerce_openai_response(openai_data):
    """Coerce OpenAI response to canonical result object schema (authors + abstract only)."""
    if not openai_data:
        return None
    
    # Clean authors - strip linebreaks from name and affiliations
    authors = []
    for author in openai_data.get("authors", []):
        cleaned_author = {
            "name": strip_linebreaks(author.get("name", "")),
            "affiliations": [strip_linebreaks(aff) for aff in author.get("affiliations", [])],
            "is_corresponding": author.get("is_corresponding", False)
        }
        authors.append(cleaned_author)
    
    # Clean abstract
    abstract = strip_linebreaks(openai_data.get("abstract"))
    
    return {
        "authors": authors,
        "abstract": abstract
    }

def process_single_item(row_data: Dict, html_content: str, estimated_tokens: int, 
                        api_key: str, output_file: str) -> Dict:
    """Process a single item - called by worker thread."""
    openalex_id = row_data["openalex_id"]
    
    try:
        # Call OpenAI API
        result, token_usage, api_error = call_openai_api(html_content, api_key)
        
        tokens_up = token_usage.get('input', 0)
        tokens_down = token_usage.get('output', 0)
        
        # Release capacity now that we know actual tokens
        release_capacity(estimated_tokens, tokens_up)
        
        if api_error == "rate_limited":
            return {
                "openalex_id": openalex_id,
                "status": "rate_limited",
                "tokens_up": 0,
                "tokens_down": 0,
                "data": None,
                "row_data": row_data,
                "html_content": html_content,
                "estimated_tokens": estimated_tokens
            }
        
        if api_error:
            save_result(output_file, openalex_id, tokens_up, tokens_down, None)
            return {
                "openalex_id": openalex_id,
                "status": "error",
                "error": api_error,
                "tokens_up": tokens_up,
                "tokens_down": tokens_down,
                "data": None
            }
        
        # Success - coerce response and save
        coerced = coerce_openai_response(result)
        save_result(output_file, openalex_id, tokens_up, tokens_down, coerced)
        
        return {
            "openalex_id": openalex_id,
            "status": "success",
            "tokens_up": tokens_up,
            "tokens_down": tokens_down,
            "data": coerced
        }
    except Exception as e:
        release_capacity(estimated_tokens, 0)
        return {
            "openalex_id": openalex_id,
            "status": "error",
            "error": str(e),
            "tokens_up": 0,
            "tokens_down": 0,
            "data": None
        }

def save_result(filepath, openalex_id, tokens_up, tokens_down, data):
    """Save a result to the TSV file."""
    data_json = json.dumps(data, ensure_ascii=False) if data else ""
    with file_lock:
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(f"{openalex_id}\t{tokens_up}\t{tokens_down}\t{data_json}\n")

def format_time(seconds):
    """Format seconds into human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.1f}h"

class PreparedItem:
    """An item ready to be dispatched (HTML already fetched, tokens estimated)."""
    def __init__(self, row_data: Dict, html_content: str, estimated_tokens: int):
        self.row_data = row_data
        self.html_content = html_content
        self.estimated_tokens = estimated_tokens


MAX_TOKENS_PER_REQUEST = 700000  # Skip items that would exceed this

def prefetch_html_batch(rows: List[Dict], output_file: str, batch_size: int = 20, max_retries: int = 2) -> Tuple[List[PreparedItem], int]:
    """Pre-fetch HTML for a batch of rows so we can estimate tokens.
    
    Returns: (prepared_items, error_count) - items that succeeded and count of failures saved.
    """
    prepared = []
    error_count = 0
    print(f"  Prefetching batch of {min(batch_size, len(rows))} rows...")
    for row in rows[:batch_size]:
        html_content = None
        html_error = None
        
        # Retry HTML fetch up to max_retries times
        for attempt in range(max_retries):
            html_content, html_error = fetch_html_from_taxicab(row["html_uuid"])
            if not html_error:
                break
            if attempt < max_retries - 1:
                time.sleep(0.5)  # Wait before retry
        
        if html_error:
            # Save error result after all retries failed
            print(f"    HTML fetch failed for {row['openalex_id']}: {html_error}")
            save_result(output_file, row["openalex_id"], 0, 0, None)
            error_count += 1
            continue
        
        estimated_tokens = estimate_tokens(html_content)
        
        # Skip oversized HTML that would exceed token limits
        if estimated_tokens > MAX_TOKENS_PER_REQUEST:
            print(f"    Skipping {row['openalex_id']}: HTML too large ({estimated_tokens:,} tokens)")
            save_result(output_file, row["openalex_id"], 0, 0, None)
            error_count += 1
            continue
            
        prepared.append(PreparedItem(row, html_content, estimated_tokens))
    print(f"  Prefetch done: {len(prepared)} ready, {error_count} errors")
    return prepared, error_count


def print_progress(total: int, already_done: int, start_time: float):
    """Print progress report."""
    elapsed = time.time() - start_time
    with state_lock:
        s = state.copy()
    with rate_limit_lock:
        rl = rate_limits.copy()
    
    processed = s["processed"]
    if processed > 0:
        rate = processed / elapsed
        remaining_count = total - already_done - processed
        eta = remaining_count / rate if rate > 0 else float('inf')
    else:
        eta = float('inf')
    
    print(f"[{already_done + processed}/{total}] success: {s['success']}, errors: {s['errors']} | "
          f"tokens: {s['total_tokens_up']:,}↑ {s['total_tokens_down']:,}↓ | "
          f"in-flight: {s['in_flight_count']} ({s['in_flight_tokens']:,} tok) | "
          f"capacity: {rl['remaining_tokens']:,} tok | "
          f"Elapsed: {format_time(elapsed)} | ETA: {format_time(eta)}")


def main():
    parser = argparse.ArgumentParser(description="Fetch OpenAI responses for HTML pages")
    parser.add_argument("input_file", help="Input TSV file (parseland.tsv format with html_uuid column)")
    parser.add_argument("output_file", help="Output TSV file")
    args = parser.parse_args()
    
    input_file = args.input_file
    output_file = args.output_file
    
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("Error: OPENAI_API_KEY not set in environment")
        print("Try: source ~/.zprofile")
        return
    
    # Load data
    all_rows = load_input_data(input_file)
    completed_ids = load_completed_ids(output_file)
    init_tsv(output_file)
    
    remaining_rows = [r for r in all_rows if r["openalex_id"] not in completed_ids]
    total = len(all_rows)
    already_done = len(completed_ids)
    
    print(f"Total IDs with html_uuid in {input_file}: {total}")
    print(f"Already processed: {already_done}")
    print(f"Remaining: {len(remaining_rows)}")
    print(f"Rate-limited dispatcher mode (dispatch interval: {DISPATCH_INTERVAL}s)")
    
    if not remaining_rows:
        print("All IDs already processed!")
        return
    
    start_time = time.time()
    last_progress_time = start_time
    
    # Queue of prepared items (HTML fetched, tokens estimated)
    prepared_queue: List[PreparedItem] = []
    retry_queue: List[PreparedItem] = []
    row_index = 0
    
    # Executor for fire-and-forget tasks
    executor = ThreadPoolExecutor(max_workers=50)  # High limit, dispatch controls concurrency
    active_futures: List[Future] = []
    
    try:
        while row_index < len(remaining_rows) or prepared_queue or retry_queue or active_futures:
            # 1. Collect completed futures
            still_active = []
            for future in active_futures:
                if future.done():
                    try:
                        result = future.result()
                        with state_lock:
                            state["processed"] += 1
                            state["total_tokens_up"] += result["tokens_up"]
                            state["total_tokens_down"] += result["tokens_down"]
                            if result["status"] == "success":
                                state["success"] += 1
                            elif result["status"] == "rate_limited":
                                # Re-queue for retry
                                retry_queue.append(PreparedItem(
                                    result["row_data"],
                                    result["html_content"],
                                    result["estimated_tokens"]
                                ))
                                state["processed"] -= 1  # Don't count as processed yet
                            else:
                                state["errors"] += 1
                    except Exception as e:
                        print(f"  Task error: {e}")
                        with state_lock:
                            state["processed"] += 1
                            state["errors"] += 1
                else:
                    still_active.append(future)
            active_futures = still_active
            
            # 2. Refill prepared queue if needed
            if len(prepared_queue) < 10 and row_index < len(remaining_rows):
                batch_to_fetch = remaining_rows[row_index:row_index + 20]
                new_prepared, html_errors = prefetch_html_batch(batch_to_fetch, output_file)
                prepared_queue.extend(new_prepared)
                row_index += len(batch_to_fetch)
                # Track HTML fetch errors
                if html_errors > 0:
                    with state_lock:
                        state["processed"] += html_errors
                        state["errors"] += html_errors
            
            # 3. Try to dispatch from retry queue first, then prepared queue
            dispatch_queue = retry_queue if retry_queue else prepared_queue
            
            # Try to dispatch items, skipping ones that are too large for max capacity
            dispatched = False
            items_to_skip = []
            for i, item in enumerate(dispatch_queue):
                max_capacity = rate_limits["limit_tokens"] - TOKEN_BUFFER
                if item.estimated_tokens > max_capacity:
                    # Item will NEVER fit - skip it permanently
                    items_to_skip.append(i)
                    openalex_id = item.row_data["openalex_id"]
                    print(f"  SKIP (too large): {openalex_id} needs {item.estimated_tokens} tokens, max is {max_capacity}")
                    # Write empty result
                    with open(output_file, "a") as f:
                        f.write(f"{openalex_id}\t0\t0\t\n")
                    with state_lock:
                        state["processed"] += 1
                        state["errors"] += 1
                    continue
                
                if can_dispatch(item.estimated_tokens):
                    dispatch_queue.pop(i)
                    reserve_capacity(item.estimated_tokens)
                    future = executor.submit(
                        process_single_item,
                        item.row_data,
                        item.html_content,
                        item.estimated_tokens,
                        api_key,
                        output_file
                    )
                    active_futures.append(future)
                    dispatched = True
                    break
            
            # Remove skipped items (in reverse order to maintain indices)
            for i in reversed(items_to_skip):
                dispatch_queue.pop(i)
            
            # 4. Progress report every 10 seconds
            now = time.time()
            if now - last_progress_time >= 10:
                print_progress(total, already_done, start_time)
                last_progress_time = now
            
            # 5. Poll rate limits to get fresh info, then wait
            poll_rate_limits(api_key)
            time.sleep(DISPATCH_INTERVAL)
    
    finally:
        executor.shutdown(wait=True)
    
    # Final progress
    print_progress(total, already_done, start_time)
    
    elapsed = time.time() - start_time
    with state_lock:
        s = state.copy()
    print(f"\nDone! Processed {s['processed']} rows in {format_time(elapsed)}")
    print(f"Success: {s['success']}, Errors: {s['errors']}")
    print(f"Total tokens: {s['total_tokens_up']:,} up, {s['total_tokens_down']:,} down")

if __name__ == "__main__":
    main()
