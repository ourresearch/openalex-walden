#!/usr/bin/env python3
"""
Validate raw_orcid accuracy on OpenAlex authorships.

Samples authorships with raw_orcid, looks up each ORCID in the ORCID API,
compares names using block keys, and reports pass/fail rates by subpopulation.

Usage:
    python validate_raw_orcid.py [--sample-csv PATH] [--resume] [--report-only]
"""

import argparse
import asyncio
import csv
import json
import os
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import aiohttp
from nameparser import HumanName

# =============================================================================
# Configuration
# =============================================================================

DEFAULT_OUTPUT_DIR = Path(__file__).parent / "orcid_validation_data"
ORCID_TOKEN_URL = "https://orcid.org/oauth/token"
ORCID_API_BASE = "https://pub.orcid.org/v3.0"
MAX_CONCURRENT = 20
PROGRESS_INTERVAL = 10  # seconds


# =============================================================================
# Block Key Logic (from libraries/dlt_utils/openalex/dlt/normalize.py:56-86)
# =============================================================================

def remove_latin_characters(author):
    if author and any("\u0080" <= c <= "\u02AF" for c in author):
        author = (
            unicodedata.normalize("NFKD", author)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
    return author


def remove_author_prefixes(author):
    if not author:
        return ""
    prefixes = ["None ", "Array "]
    for prefix in prefixes:
        if author.startswith(prefix):
            author = author.replace(prefix, "")
    return author


def clean_author_name(author_name):
    if not author_name:
        return ""
    return re.sub(r"[ \-‐.'' ́>]", "", author_name).strip()


def last_name_only(author):
    if not author:
        return ["", "", ""]
    author = remove_latin_characters(author)
    author = remove_author_prefixes(author)
    author_name_obj = HumanName(author)
    first_name = clean_author_name(author_name_obj.first)
    last_name = clean_author_name(author_name_obj.last)
    first_initial = first_name[0] if first_name else ""
    return [f"{last_name};{first_initial}", f"{first_name}", f"{last_name}"]


def make_block_key(name_string):
    """Generate block key from a name string. Returns lowercase '{last_name};{first_initial}'."""
    parts = last_name_only(name_string)
    return parts[0].lower() if parts[0] else ""


# =============================================================================
# ORCID API Client
# =============================================================================

def get_orcid_token():
    """Get OAuth token using client credentials."""
    import urllib.request
    import urllib.parse

    client_id = os.environ.get("ORCID_CLIENT_ID")
    client_secret = os.environ.get("ORCID_API_SECRET")
    if not client_id or not client_secret:
        print("ERROR: ORCID_CLIENT_ID and ORCID_API_SECRET must be set")
        sys.exit(1)

    data = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "/read-public",
    }).encode()

    req = urllib.request.Request(ORCID_TOKEN_URL, data=data)
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    return result["access_token"]


def extract_orcid_id(raw_orcid):
    """Extract bare ORCID ID from URL format."""
    match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])", raw_orcid)
    return match.group(1) if match else None


def extract_orcid_names(person_data):
    """Extract name info from ORCID person API response."""
    result = {
        "given_name": None,
        "family_name": None,
        "credit_name": None,
        "other_names": [],
    }
    name = person_data.get("name")
    if not name:
        return result

    gn = name.get("given-names")
    if gn:
        result["given_name"] = gn.get("value")

    fn = name.get("family-name")
    if fn:
        result["family_name"] = fn.get("value")

    cn = name.get("credit-name")
    if cn:
        result["credit_name"] = cn.get("value")

    other = person_data.get("other-names", {})
    if other:
        for entry in other.get("other-name", []):
            content = entry.get("content")
            if content:
                result["other_names"].append(content)

    return result


async def fetch_orcid_person(session, orcid_id, token, semaphore):
    """Fetch a single ORCID person record with rate limiting and retry."""
    url = f"{ORCID_API_BASE}/{orcid_id}/person"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    async with semaphore:
        for attempt in range(3):
            try:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 404:
                        return {"_status": "not_found"}
                    elif resp.status == 429:
                        wait = (attempt + 1) * 2
                        await asyncio.sleep(wait)
                        continue
                    elif resp.status >= 500:
                        await asyncio.sleep((attempt + 1) * 2)
                        continue
                    else:
                        return {"_status": f"error_{resp.status}"}
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt < 2:
                    await asyncio.sleep((attempt + 1) * 2)
                    continue
                return {"_status": "timeout"}
    return {"_status": "max_retries"}


async def lookup_all_orcids(orcid_ids, token, cache):
    """Look up all ORCID IDs, using cache for already-fetched ones."""
    to_fetch = [oid for oid in orcid_ids if oid not in cache]
    if not to_fetch:
        print(f"  All {len(orcid_ids)} ORCIDs already cached")
        return cache

    print(f"  Fetching {len(to_fetch)} ORCIDs ({len(cache)} cached)...")
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    completed = 0
    start_time = time.time()
    last_progress = start_time

    async with aiohttp.ClientSession() as session:
        tasks = {}
        for oid in to_fetch:
            tasks[oid] = asyncio.create_task(
                fetch_orcid_person(session, oid, token, semaphore)
            )

        for oid in to_fetch:
            result = await tasks[oid]
            if "_status" in result:
                cache[oid] = {"_status": result["_status"]}
            else:
                cache[oid] = extract_orcid_names(result)
            completed += 1

            now = time.time()
            if now - last_progress >= PROGRESS_INTERVAL:
                elapsed = now - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = (len(to_fetch) - completed) / rate if rate > 0 else 0
                print(f"  Progress: {completed}/{len(to_fetch)} ({rate:.1f}/s, ETA {remaining:.0f}s)")
                last_progress = now

    elapsed = time.time() - start_time
    print(f"  Done: {len(to_fetch)} lookups in {elapsed:.1f}s ({len(to_fetch)/elapsed:.1f}/s)")
    return cache


# =============================================================================
# Comparison Logic
# =============================================================================

def compare_names(raw_author_name, orcid_names):
    """Compare raw_author_name against ORCID names using block keys."""
    result = {
        "raw_block_key": "",
        "orcid_given_name": "",
        "orcid_family_name": "",
        "orcid_credit_name": "",
        "orcid_primary_block_key": "",
        "orcid_credit_block_key": "",
        "primary_match": False,
        "credit_match": False,
        "alt_match": False,
        "any_match": False,
        "match_type": "none",
        "orcid_lookup_status": "ok",
    }

    # Handle ORCID lookup failures
    if "_status" in orcid_names:
        result["orcid_lookup_status"] = orcid_names["_status"]
        result["match_type"] = orcid_names["_status"]
        return result

    result["orcid_given_name"] = orcid_names.get("given_name") or ""
    result["orcid_family_name"] = orcid_names.get("family_name") or ""
    result["orcid_credit_name"] = orcid_names.get("credit_name") or ""

    # No ORCID name available (private profile)
    if not result["orcid_given_name"] and not result["orcid_family_name"]:
        result["match_type"] = "orcid_no_name"
        result["orcid_lookup_status"] = "no_name"
        return result

    # Generate block keys
    raw_bk = make_block_key(raw_author_name)
    result["raw_block_key"] = raw_bk

    if not raw_bk:
        result["match_type"] = "unparseable_raw"
        return result

    # Primary name
    primary_name = f"{result['orcid_given_name']} {result['orcid_family_name']}".strip()
    primary_bk = make_block_key(primary_name)
    result["orcid_primary_block_key"] = primary_bk

    if raw_bk and primary_bk and raw_bk == primary_bk:
        result["primary_match"] = True
        result["any_match"] = True
        result["match_type"] = "primary"
        return result

    # Credit name
    if result["orcid_credit_name"]:
        credit_bk = make_block_key(result["orcid_credit_name"])
        result["orcid_credit_block_key"] = credit_bk
        if raw_bk and credit_bk and raw_bk == credit_bk:
            result["credit_match"] = True
            result["any_match"] = True
            result["match_type"] = "credit"
            return result

    # Alternate names
    for alt_name in orcid_names.get("other_names", []):
        alt_bk = make_block_key(alt_name)
        if raw_bk and alt_bk and raw_bk == alt_bk:
            result["alt_match"] = True
            result["any_match"] = True
            result["match_type"] = "alt"
            return result

    return result


# =============================================================================
# Report Generation
# =============================================================================

def generate_report(results, output_dir):
    """Generate markdown report with subpopulation breakdowns."""
    total = len(results)
    if total == 0:
        print("ERROR: No results to report on")
        return

    # Filter to only those with successful lookups
    valid = [r for r in results if r["orcid_lookup_status"] == "ok"]
    errors = [r for r in results if r["orcid_lookup_status"] not in ("ok", "no_name")]
    no_name = [r for r in results if r["orcid_lookup_status"] == "no_name"]

    lines = ["# ORCID Raw Accuracy Validation Report", ""]
    lines.append(f"**Date**: {time.strftime('%Y-%m-%d')}")
    lines.append(f"**Sample size**: {total:,}")
    lines.append("")

    # Overall summary
    lines.append("## Overall Summary")
    lines.append("")
    match_count = sum(1 for r in valid if r["any_match"])
    no_match_count = len(valid) - match_count

    lines.append(f"| Metric | Count | Rate |")
    lines.append(f"|--------|-------|------|")
    lines.append(f"| Total sampled | {total:,} | |")
    lines.append(f"| Successful ORCID lookup | {len(valid):,} | {pct(len(valid), total)} |")
    lines.append(f"| ORCID profile has no name | {len(no_name):,} | {pct(len(no_name), total)} |")
    lines.append(f"| ORCID lookup error | {len(errors):,} | {pct(len(errors), total)} |")
    lines.append(f"| **Match (any name)** | **{match_count:,}** | **{pct(match_count, len(valid))}** |")
    lines.append(f"| No match | {no_match_count:,} | {pct(no_match_count, len(valid))} |")
    lines.append("")

    # Match type breakdown
    type_counts = Counter(r["match_type"] for r in valid)
    lines.append("### Match Type Breakdown (of successful lookups)")
    lines.append("")
    lines.append("| Match Type | Count | Rate |")
    lines.append("|------------|-------|------|")
    for mt in ["primary", "credit", "alt", "none"]:
        c = type_counts.get(mt, 0)
        lines.append(f"| {mt} | {c:,} | {pct(c, len(valid))} |")
    for mt in sorted(type_counts):
        if mt not in ("primary", "credit", "alt", "none"):
            c = type_counts[mt]
            lines.append(f"| {mt} | {c:,} | {pct(c, len(valid))} |")
    lines.append("")

    # Subpopulation breakdowns
    lines.extend(breakdown_table("By Provenance", valid, "provenance"))
    lines.extend(breakdown_table("By Author Position", valid, "author_position"))
    lines.extend(breakdown_table("By Work Type", valid, "type"))
    lines.extend(breakdown_by_year(valid))
    lines.extend(breakdown_by_publisher(valid, top_n=15))

    # Failure sample
    lines.extend(failure_sample(results))

    report = "\n".join(lines)
    report_path = output_dir / "report.md"
    report_path.write_text(report)
    print(f"\nReport written to {report_path}")
    print(f"\n{'='*60}")
    print(report)


def pct(n, total):
    if total == 0:
        return "0%"
    return f"{round(100 * n / total)}%"


def breakdown_table(title, results, field):
    """Generate a breakdown table by a categorical field."""
    groups = defaultdict(list)
    for r in results:
        groups[r.get(field, "unknown")].append(r)

    lines = [f"## {title}", ""]
    lines.append("| Group | N | Match Rate |")
    lines.append("|-------|---|------------|")

    for group in sorted(groups, key=lambda g: -len(groups[g])):
        rows = groups[group]
        matches = sum(1 for r in rows if r["any_match"])
        lines.append(f"| {group} | {len(rows):,} | {pct(matches, len(rows))} |")
    lines.append("")
    return lines


def breakdown_by_year(results):
    """Generate breakdown by publication year buckets."""
    buckets = {"pre-2015": [], "2015-2019": [], "2020-2022": [], "2023-2025": [], "2026+": []}
    for r in results:
        try:
            year = int(r["publication_year"])
        except (ValueError, TypeError):
            continue
        if year < 2015:
            buckets["pre-2015"].append(r)
        elif year <= 2019:
            buckets["2015-2019"].append(r)
        elif year <= 2022:
            buckets["2020-2022"].append(r)
        elif year <= 2025:
            buckets["2023-2025"].append(r)
        else:
            buckets["2026+"].append(r)

    lines = ["## By Publication Year", ""]
    lines.append("| Period | N | Match Rate |")
    lines.append("|--------|---|------------|")
    for bucket in ["pre-2015", "2015-2019", "2020-2022", "2023-2025", "2026+"]:
        rows = buckets[bucket]
        if not rows:
            continue
        matches = sum(1 for r in rows if r["any_match"])
        lines.append(f"| {bucket} | {len(rows):,} | {pct(matches, len(rows))} |")
    lines.append("")
    return lines


def breakdown_by_publisher(results, top_n=15):
    """Generate breakdown by publisher (top N by count)."""
    groups = defaultdict(list)
    for r in results:
        groups[r.get("publisher", "unknown")].append(r)

    sorted_groups = sorted(groups.items(), key=lambda x: -len(x[1]))[:top_n]

    lines = [f"## By Publisher (Top {top_n})", ""]
    lines.append("| Publisher | N | Match Rate |")
    lines.append("|-----------|---|------------|")
    for publisher, rows in sorted_groups:
        matches = sum(1 for r in rows if r["any_match"])
        # Truncate long publisher names
        pub_display = publisher[:60] + "..." if len(publisher) > 60 else publisher
        lines.append(f"| {pub_display} | {len(rows):,} | {pct(matches, len(rows))} |")
    lines.append("")
    return lines


def failure_sample(results, n=30):
    """Show a sample of failure cases for manual inspection."""
    failures = [r for r in results if r["match_type"] == "none"]
    sample = failures[:n]

    lines = [f"## Failure Sample ({len(failures):,} total failures, showing {len(sample)})", ""]
    lines.append("| raw_author_name | raw_block_key | orcid_name | orcid_block_key | provenance | work_id |")
    lines.append("|-----------------|---------------|------------|-----------------|------------|---------|")
    for r in sample:
        orcid_name = f"{r['orcid_given_name']} {r['orcid_family_name']}".strip()
        lines.append(
            f"| {r['raw_author_name'][:40]} | {r['raw_block_key']} | {orcid_name[:40]} | {r['orcid_primary_block_key']} | {r['provenance']} | {r['work_id']} |"
        )
    lines.append("")
    return lines


# =============================================================================
# Main
# =============================================================================

def load_sample(csv_path):
    """Load sample CSV into list of dicts."""
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        return list(reader)


def load_cache(cache_path):
    """Load ORCID name cache from JSON."""
    if cache_path.exists():
        with open(cache_path) as f:
            return json.load(f)
    return {}


def save_cache(cache, cache_path):
    """Save ORCID name cache to JSON."""
    with open(cache_path, "w") as f:
        json.dump(cache, f, indent=2)


def save_results(results, results_path):
    """Save results to CSV."""
    if not results:
        return
    fieldnames = [
        "work_id", "doi", "publication_year", "type", "publisher", "provenance",
        "raw_orcid", "orcid_id", "raw_author_name", "author_position", "author_order_number",
        "raw_block_key", "orcid_given_name", "orcid_family_name", "orcid_credit_name",
        "orcid_primary_block_key", "orcid_credit_block_key",
        "primary_match", "credit_match", "alt_match", "any_match", "match_type",
        "orcid_lookup_status",
    ]
    with open(results_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


def main():
    parser = argparse.ArgumentParser(description="Validate raw_orcid accuracy")
    parser.add_argument("--sample-csv", type=Path, help="Path to sample CSV")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--resume", action="store_true", help="Resume from cache")
    parser.add_argument("--report-only", action="store_true", help="Only generate report from existing results")
    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    sample_path = args.sample_csv or output_dir / "sample.csv"
    cache_path = output_dir / "orcid_names_cache.json"
    results_path = output_dir / "results.csv"

    # Report-only mode
    if args.report_only:
        print("Loading existing results...")
        with open(results_path) as f:
            reader = csv.DictReader(f)
            results = list(reader)
        # Convert string booleans back
        for r in results:
            for key in ("primary_match", "credit_match", "alt_match", "any_match"):
                r[key] = r[key] == "True"
        generate_report(results, output_dir)
        return

    # Phase 1: Load sample
    print(f"Loading sample from {sample_path}...")
    sample = load_sample(sample_path)
    print(f"  Loaded {len(sample):,} authorships")

    # Extract unique ORCID IDs
    orcid_map = {}
    skipped = 0
    for row in sample:
        orcid_id = extract_orcid_id(row["raw_orcid"])
        if orcid_id:
            orcid_map[orcid_id] = True
        else:
            skipped += 1
    unique_orcids = list(orcid_map.keys())
    print(f"  {len(unique_orcids):,} unique ORCID IDs ({skipped} unparseable)")

    # Phase 2: ORCID API lookups
    print("\nPhase 2: ORCID API lookups")
    cache = load_cache(cache_path) if args.resume else {}
    token = get_orcid_token()
    print(f"  Got OAuth token")

    cache = asyncio.run(lookup_all_orcids(unique_orcids, token, cache))
    save_cache(cache, cache_path)
    print(f"  Cache saved ({len(cache):,} entries)")

    # Phase 3: Compare names
    print("\nPhase 3: Comparing names...")
    results = []
    for row in sample:
        orcid_id = extract_orcid_id(row["raw_orcid"])
        orcid_names = cache.get(orcid_id, {"_status": "not_in_cache"})
        comparison = compare_names(row["raw_author_name"], orcid_names)

        result = {
            "work_id": row["work_id"],
            "doi": row["doi"],
            "publication_year": row["publication_year"],
            "type": row["type"],
            "publisher": row["publisher"],
            "provenance": row["provenance"],
            "raw_orcid": row["raw_orcid"],
            "orcid_id": orcid_id or "",
            "raw_author_name": row["raw_author_name"],
            "author_position": row["author_position"],
            "author_order_number": row["author_order_number"],
            **comparison,
        }
        results.append(result)

    save_results(results, results_path)
    print(f"  Results saved to {results_path} ({len(results):,} rows)")

    # Phase 4: Report
    print("\nPhase 4: Generating report...")
    generate_report(results, output_dir)


if __name__ == "__main__":
    main()
