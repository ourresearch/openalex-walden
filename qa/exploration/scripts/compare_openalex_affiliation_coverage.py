#!/usr/bin/env python3
"""
Compare OpenAlex API affiliation string coverage: old vs new.
Analyzes both 10k random sample and nees DOIs.

Usage:
    python compare_openalex_affiliation_coverage.py
"""

import json
import os
import time
import requests
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
OPENALEX_API = "https://api.openalex.org/works"
BATCH_SIZE = 50
MAX_WORKERS = 10

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RANDOM_10K_IDS = os.path.join(BASE_DIR, "10k_random/openalex_random_ids.txt")
RANDOM_10K_OLD_DATA = os.path.join(BASE_DIR, "10k_random/openalex.tsv")
NEES_DOIS = os.path.join(BASE_DIR, "runs/2025-01-03_preloaded_state_fix/nees_test/nees_dois.txt")


def load_old_openalex_data(filepath):
    """Load old OpenAlex data from TSV. Returns dict {openalex_id: {year, doi, data}}."""
    result = {}
    with open(filepath, 'r') as f:
        next(f)  # skip header
        for line in f:
            if line.strip():
                parts = line.strip().split('\t')
                if len(parts) >= 4:
                    openalex_id = parts[0]
                    year = parts[1] if parts[1] else None
                    doi = parts[2] if parts[2] else None
                    try:
                        data = json.loads(parts[3])
                    except json.JSONDecodeError:
                        data = {"authors": [], "abstract": None}
                    result[openalex_id] = {
                        "year": int(year) if year else None,
                        "doi": doi,
                        "data": data
                    }
    return result


def load_ids_from_file(filepath):
    """Load OpenAlex IDs or DOIs from a file."""
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def has_any_affiliation(data):
    """Check if any author has at least one non-empty affiliation string."""
    authors = data.get("authors", [])
    for author in authors:
        affiliations = author.get("affiliations", [])
        for aff in affiliations:
            if aff and str(aff).strip():
                return True
    return False


def count_authors_with_affiliations(data):
    """Count authors with at least one affiliation."""
    authors = data.get("authors", [])
    count = 0
    for author in authors:
        affiliations = author.get("affiliations", [])
        for aff in affiliations:
            if aff and str(aff).strip():
                count += 1
                break
    return count


def get_works_batch(openalex_ids, api_key=None, by_doi=False):
    """Fetch work data for a batch of IDs. Returns dict {id: work_data}."""
    if by_doi:
        # For DOIs, use pipe-separated filter
        filter_str = "|".join(f"https://doi.org/{doi}" for doi in openalex_ids)
        filter_param = f"doi:{filter_str}"
    else:
        work_ids = [id_.split('/')[-1] for id_ in openalex_ids]
        filter_str = "|".join(work_ids)
        filter_param = f"openalex_id:{filter_str}"

    params = {
        "filter": filter_param,
        "select": "id,doi,publication_year,authorships,primary_location",
        "per-page": BATCH_SIZE
    }
    if api_key:
        params["api_key"] = api_key

    headers = {"User-Agent": "OpenAlexEvals/2.0 (mailto:team@ourresearch.org)"}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(OPENALEX_API, params=params, headers=headers, timeout=60)
            if response.status_code == 429:
                wait_time = min(2 ** (attempt + 1), 30)
                time.sleep(wait_time)
                continue
            response.raise_for_status()
            data = response.json()

            result = {}
            for item in data.get("results", []):
                oa_id = item.get("id")
                doi = item.get("doi")

                # Build authors with affiliations
                authors = []
                for authorship in (item.get("authorships") or []):
                    author_obj = {
                        "name": authorship.get("author", {}).get("display_name", ""),
                        "affiliations": authorship.get("raw_affiliation_strings", []) or [],
                        "is_corresponding": authorship.get("is_corresponding", False) or False
                    }
                    authors.append(author_obj)

                # Get publisher from primary_location
                primary_loc = item.get("primary_location") or {}
                source = primary_loc.get("source") or {}
                publisher = source.get("host_organization_name") or source.get("display_name") or "Unknown"

                work_data = {
                    "openalex_id": oa_id,
                    "doi": doi,
                    "year": item.get("publication_year"),
                    "publisher": publisher,
                    "data": {"authors": authors}
                }

                if oa_id:
                    result[oa_id] = work_data
                if doi and by_doi:
                    # For DOI lookups, also index by DOI
                    doi_key = doi.replace("https://doi.org/", "")
                    result[doi_key] = work_data

            return result
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print(f"  Batch request error after {max_retries} attempts: {e}")
                return {}
    return {}


def fetch_new_data_for_ids(ids, by_doi=False, label=""):
    """Fetch new OpenAlex data for a list of IDs."""
    api_key = os.environ.get("OPENALEX_API_KEY")

    all_results = {}
    total = len(ids)

    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch_ids = ids[batch_start:batch_end]

        results = get_works_batch(batch_ids, api_key, by_doi=by_doi)
        all_results.update(results)

        progress = batch_end / total * 100
        print(f"  {label}: {batch_end}/{total} ({progress:.1f}%)", end='\r')

        # Small delay to be polite
        time.sleep(0.1)

    print()
    return all_results


def analyze_coverage(old_data, new_data, label):
    """Analyze affiliation coverage comparison between old and new data."""
    results = {
        "total": 0,
        "old_has_aff": 0,
        "new_has_aff": 0,
        "both_have_aff": 0,
        "neither_have_aff": 0,
        "new_gained_aff": 0,
        "new_lost_aff": 0,
        "by_year": defaultdict(lambda: {"total": 0, "old_has_aff": 0, "new_has_aff": 0}),
        "by_publisher": defaultdict(lambda: {"total": 0, "old_has_aff": 0, "new_has_aff": 0})
    }

    for oa_id, old_entry in old_data.items():
        results["total"] += 1

        old_has = has_any_affiliation(old_entry.get("data", {}))

        new_entry = new_data.get(oa_id, {})
        new_has = has_any_affiliation(new_entry.get("data", {}))

        if old_has:
            results["old_has_aff"] += 1
        if new_has:
            results["new_has_aff"] += 1
        if old_has and new_has:
            results["both_have_aff"] += 1
        if not old_has and not new_has:
            results["neither_have_aff"] += 1
        if not old_has and new_has:
            results["new_gained_aff"] += 1
        if old_has and not new_has:
            results["new_lost_aff"] += 1

        # By year
        year = old_entry.get("year") or new_entry.get("year")
        if year:
            results["by_year"][year]["total"] += 1
            if old_has:
                results["by_year"][year]["old_has_aff"] += 1
            if new_has:
                results["by_year"][year]["new_has_aff"] += 1

        # By publisher (from new data)
        publisher = new_entry.get("publisher", "Unknown")
        results["by_publisher"][publisher]["total"] += 1
        if old_has:
            results["by_publisher"][publisher]["old_has_aff"] += 1
        if new_has:
            results["by_publisher"][publisher]["new_has_aff"] += 1

    return results


def analyze_nees_coverage(nees_dois, new_data):
    """Analyze affiliation coverage for nees DOIs (no old data available)."""
    results = {
        "total": len(nees_dois),
        "found_in_api": 0,
        "has_aff": 0,
        "by_year": defaultdict(lambda: {"total": 0, "has_aff": 0}),
        "by_publisher": defaultdict(lambda: {"total": 0, "has_aff": 0})
    }

    for doi in nees_dois:
        entry = new_data.get(doi, {})
        if entry:
            results["found_in_api"] += 1
            has_aff = has_any_affiliation(entry.get("data", {}))
            if has_aff:
                results["has_aff"] += 1

            year = entry.get("year")
            if year:
                results["by_year"][year]["total"] += 1
                if has_aff:
                    results["by_year"][year]["has_aff"] += 1

            publisher = entry.get("publisher", "Unknown")
            results["by_publisher"][publisher]["total"] += 1
            if has_aff:
                results["by_publisher"][publisher]["has_aff"] += 1

    return results


def print_report(results_10k, results_nees):
    """Print comprehensive coverage report."""

    print("\n" + "="*80)
    print("OPENALEX API AFFILIATION STRING COVERAGE COMPARISON")
    print("="*80)

    # 10k Random Overall
    print("\n## 10K RANDOM SAMPLE - OVERALL COVERAGE")
    print("-"*50)
    total = results_10k["total"]
    old_pct = results_10k["old_has_aff"] / total * 100 if total > 0 else 0
    new_pct = results_10k["new_has_aff"] / total * 100 if total > 0 else 0
    delta = new_pct - old_pct

    print(f"Total works: {total:,}")
    print(f"OLD API - Works with affiliations: {results_10k['old_has_aff']:,} ({old_pct:.2f}%)")
    print(f"NEW API - Works with affiliations: {results_10k['new_has_aff']:,} ({new_pct:.2f}%)")
    print(f"DELTA: {delta:+.2f}% ({results_10k['new_has_aff'] - results_10k['old_has_aff']:+,} works)")
    print(f"\nTransitions:")
    print(f"  Gained affiliations (0 -> 1+): {results_10k['new_gained_aff']:,}")
    print(f"  Lost affiliations (1+ -> 0): {results_10k['new_lost_aff']:,}")
    print(f"  Both have affiliations: {results_10k['both_have_aff']:,}")
    print(f"  Neither have affiliations: {results_10k['neither_have_aff']:,}")

    # 10k by Year
    print("\n## 10K RANDOM SAMPLE - BY YEAR")
    print("-"*50)
    print(f"{'Year':<6} {'Total':>8} {'Old Cov':>10} {'New Cov':>10} {'Delta':>10}")
    print("-"*50)

    years = sorted(results_10k["by_year"].keys())
    for year in years:
        if year is None:
            continue
        stats = results_10k["by_year"][year]
        total = stats["total"]
        old_pct = stats["old_has_aff"] / total * 100 if total > 0 else 0
        new_pct = stats["new_has_aff"] / total * 100 if total > 0 else 0
        delta = new_pct - old_pct
        print(f"{year:<6} {total:>8,} {old_pct:>9.1f}% {new_pct:>9.1f}% {delta:>+9.1f}%")

    # 10k by Publisher (top 30)
    print("\n## 10K RANDOM SAMPLE - BY PUBLISHER (Top 30 by count)")
    print("-"*80)
    print(f"{'Publisher':<45} {'Total':>7} {'Old':>8} {'New':>8} {'Delta':>8}")
    print("-"*80)

    publishers = sorted(results_10k["by_publisher"].items(), key=lambda x: x[1]["total"], reverse=True)[:30]
    for publisher, stats in publishers:
        total = stats["total"]
        old_pct = stats["old_has_aff"] / total * 100 if total > 0 else 0
        new_pct = stats["new_has_aff"] / total * 100 if total > 0 else 0
        delta = new_pct - old_pct
        pub_name = (publisher[:42] + "...") if len(publisher) > 45 else publisher
        print(f"{pub_name:<45} {total:>7,} {old_pct:>7.1f}% {new_pct:>7.1f}% {delta:>+7.1f}%")

    # Nees DOIs
    print("\n" + "="*80)
    print("\n## NEES DOIS - COVERAGE (New API Only)")
    print("-"*50)
    total = results_nees["total"]
    found = results_nees["found_in_api"]
    has_aff = results_nees["has_aff"]
    found_pct = found / total * 100 if total > 0 else 0
    aff_pct = has_aff / found * 100 if found > 0 else 0

    print(f"Total DOIs: {total:,}")
    print(f"Found in API: {found:,} ({found_pct:.1f}%)")
    print(f"Works with affiliations: {has_aff:,} ({aff_pct:.1f}% of found)")

    # Nees by Year
    print("\n## NEES DOIS - BY YEAR")
    print("-"*50)
    print(f"{'Year':<6} {'Total':>8} {'Has Aff':>10} {'Coverage':>10}")
    print("-"*50)

    years = sorted(results_nees["by_year"].keys())
    for year in years:
        if year is None:
            continue
        stats = results_nees["by_year"][year]
        total = stats["total"]
        has_aff = stats["has_aff"]
        pct = has_aff / total * 100 if total > 0 else 0
        print(f"{year:<6} {total:>8,} {has_aff:>10,} {pct:>9.1f}%")

    # Nees by Publisher
    print("\n## NEES DOIS - BY PUBLISHER (Top 20)")
    print("-"*80)
    print(f"{'Publisher':<50} {'Total':>7} {'Has Aff':>10} {'Coverage':>10}")
    print("-"*80)

    publishers = sorted(results_nees["by_publisher"].items(), key=lambda x: x[1]["total"], reverse=True)[:20]
    for publisher, stats in publishers:
        total = stats["total"]
        has_aff = stats["has_aff"]
        pct = has_aff / total * 100 if total > 0 else 0
        pub_name = (publisher[:47] + "...") if len(publisher) > 50 else publisher
        print(f"{pub_name:<50} {total:>7,} {has_aff:>10,} {pct:>9.1f}%")

    print("\n" + "="*80)


def main():
    print("OpenAlex API Affiliation Coverage Comparison")
    print("="*50)

    # Load 10k random sample old data
    print("\n1. Loading old 10k random sample data...")
    old_10k_data = load_old_openalex_data(RANDOM_10K_OLD_DATA)
    print(f"   Loaded {len(old_10k_data):,} works")

    # Get all OpenAlex IDs from the old data
    all_10k_ids = list(old_10k_data.keys())

    # Fetch new data for 10k sample
    print("\n2. Fetching NEW OpenAlex data for 10k random sample...")
    new_10k_data = fetch_new_data_for_ids(all_10k_ids, by_doi=False, label="10k random")
    print(f"   Fetched {len(new_10k_data):,} works")

    # Load nees DOIs
    print("\n3. Loading nees DOIs...")
    nees_dois = load_ids_from_file(NEES_DOIS)
    print(f"   Loaded {len(nees_dois):,} DOIs")

    # Fetch new data for nees DOIs
    print("\n4. Fetching NEW OpenAlex data for nees DOIs...")
    new_nees_data = fetch_new_data_for_ids(nees_dois, by_doi=True, label="nees DOIs")
    print(f"   Fetched {len(new_nees_data):,} works")

    # Analyze coverage
    print("\n5. Analyzing coverage...")
    results_10k = analyze_coverage(old_10k_data, new_10k_data, "10k random")
    results_nees = analyze_nees_coverage(nees_dois, new_nees_data)

    # Print report
    print_report(results_10k, results_nees)


if __name__ == "__main__":
    main()
