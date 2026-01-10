#!/usr/bin/env python3
"""
Analyze Parseland failures by publisher.
Queries OpenAlex API to get publisher info for each DOI.
"""

import csv
import requests
import time
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

OPENALEX_API = "https://api.openalex.org/works"


def get_work_info(doi: str) -> dict:
    """Query OpenAlex API to get work info including publisher."""
    if not doi:
        return None

    # Ensure DOI is in full URL format
    if not doi.startswith('https://doi.org/'):
        doi = f'https://doi.org/{doi}'

    try:
        response = requests.get(
            OPENALEX_API,
            params={'filter': f'doi:{doi}'},
            headers={'User-Agent': 'mailto:team@ourresearch.org'},
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            if data.get('results') and len(data['results']) > 0:
                work = data['results'][0]
                source = work.get('primary_location', {}).get('source', {}) or {}
                return {
                    'publisher': source.get('host_organization_name', 'Unknown'),
                    'source_name': source.get('display_name', 'Unknown'),
                    'publication_year': work.get('publication_year'),
                    'type': work.get('type'),
                }
        return None
    except Exception as e:
        return None


def main():
    evals_dir = Path('/Users/jasonpriem/Documents/evals')
    input_file = evals_dir / 'analysis' / 'parseland_failures.tsv'
    output_file = evals_dir / 'analysis' / 'parseland_failures_by_publisher.md'

    print("Loading failures...")
    failures = []
    with open(input_file, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            failures.append(row)

    print(f"Loaded {len(failures)} failures")

    # Get unique DOIs to query
    unique_dois = list(set(f['doi'] for f in failures if f['doi']))
    print(f"Unique DOIs to query: {len(unique_dois)}")

    # Query OpenAlex API for publisher info
    print("\nQuerying OpenAlex API...")
    doi_info = {}

    # Use threading for faster lookups
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_work_info, doi): doi for doi in unique_dois}
        completed = 0
        for future in as_completed(futures):
            doi = futures[future]
            try:
                result = future.result()
                if result:
                    doi_info[doi] = result
            except Exception:
                pass
            completed += 1
            if completed % 100 == 0:
                print(f"  Processed {completed}/{len(unique_dois)} DOIs...")

    print(f"  Got info for {len(doi_info)} DOIs")

    # Count failures by publisher
    publisher_counts = defaultdict(lambda: {'affiliation': 0, 'corresponding_author': 0, 'total': 0, 'dois': []})

    for failure in failures:
        doi = failure['doi']
        failure_type = failure['failure_type']

        if doi in doi_info:
            publisher = doi_info[doi]['publisher']
        else:
            publisher = 'Unknown'

        publisher_counts[publisher][failure_type] += 1
        publisher_counts[publisher]['total'] += 1
        if len(publisher_counts[publisher]['dois']) < 10:  # Keep sample DOIs
            publisher_counts[publisher]['dois'].append(doi)

    # Sort by total failures
    sorted_publishers = sorted(
        publisher_counts.items(),
        key=lambda x: x[1]['total'],
        reverse=True
    )

    # Write markdown report
    with open(output_file, 'w') as f:
        f.write("# Parseland Failures by Publisher\n\n")
        f.write(f"Total failures analyzed: {len(failures)}\n")
        f.write(f"Unique DOIs: {len(unique_dois)}\n")
        f.write(f"DOIs with publisher info: {len(doi_info)}\n\n")

        f.write("## Top Publishers with Failures\n\n")
        f.write("| Rank | Publisher | Affiliations | Corresponding | Total |\n")
        f.write("|------|-----------|--------------|---------------|-------|\n")

        for i, (publisher, counts) in enumerate(sorted_publishers[:30], 1):
            pub_name = publisher if publisher else 'Unknown'
            f.write(f"| {i} | {pub_name[:50]} | {counts['affiliation']} | {counts['corresponding_author']} | {counts['total']} |\n")

        f.write("\n## Sample DOIs for Top Publishers\n\n")
        for publisher, counts in sorted_publishers[:15]:
            pub_name = publisher if publisher else 'Unknown'
            f.write(f"### {pub_name}\n")
            f.write(f"Total failures: {counts['total']} (affiliations: {counts['affiliation']}, corresponding: {counts['corresponding_author']})\n\n")
            f.write("Sample DOIs:\n")
            for doi in counts['dois'][:5]:
                f.write(f"- `{doi}`\n")
            f.write("\n")

    print(f"\nWrote report to: {output_file}")

    # Print summary to console
    print("\n=== TOP 15 PUBLISHERS ===\n")
    print(f"{'Rank':<5} {'Publisher':<50} {'Aff':<6} {'Corr':<6} {'Total':<6}")
    print("-" * 75)
    for i, (publisher, counts) in enumerate(sorted_publishers[:15], 1):
        pub_name = (publisher if publisher else 'Unknown')[:48]
        print(f"{i:<5} {pub_name:<50} {counts['affiliation']:<6} {counts['corresponding_author']:<6} {counts['total']:<6}")


if __name__ == '__main__':
    main()
