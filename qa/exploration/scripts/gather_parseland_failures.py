#!/usr/bin/env python3
"""
Gather all Parseland failure cases from:
1. Evals project - where gold standard has data but Parseland doesn't
2. Nees project - DOIs with landing pages that may have parsing issues

Outputs a consolidated list for publisher analysis.
"""

import json
import csv
from pathlib import Path
from typing import Optional


def parse_data_json(data_str: str) -> Optional[dict]:
    """Parse the JSON data column, returning None if empty or invalid."""
    if not data_str or data_str.strip() == '':
        return None
    try:
        return json.loads(data_str)
    except json.JSONDecodeError:
        return None


def has_any_affiliation(data: Optional[dict]) -> bool:
    """Check if any author has a non-empty affiliation."""
    if not data or 'authors' not in data or not data['authors']:
        return False
    for author in data['authors']:
        affiliations = author.get('affiliations', [])
        if affiliations:
            for aff in affiliations:
                if aff and str(aff).strip():
                    return True
    return False


def has_corresponding_author(data: Optional[dict]) -> bool:
    """Check if any author is marked as corresponding."""
    if not data or 'authors' not in data or not data['authors']:
        return False
    for author in data['authors']:
        if author.get('is_corresponding', False):
            return True
    return False


def load_tsv(filepath: Path, has_header: bool = True) -> dict:
    """Load TSV file and return dict mapping openalex_id to (data, doi)."""
    records = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        if has_header:
            next(reader)
        for row in reader:
            if len(row) >= 1:
                openalex_id = row[0]
                data_str = row[-1] if len(row) > 1 else ''
                records[openalex_id] = parse_data_json(data_str)
    return records


def load_openalex_with_dois(filepath: Path) -> dict:
    """Load openalex.tsv and return dict mapping openalex_id to doi."""
    dois = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)  # skip header
        for row in reader:
            if len(row) >= 3:
                openalex_id = row[0]
                doi = row[2]  # doi column
                if doi:
                    dois[openalex_id] = doi
    return dois


def main():
    evals_dir = Path('/Users/jasonpriem/Documents/evals')
    nees_dir = Path('/Users/jasonpriem/Documents/nees-dois-missing-raw-affiliation-strings')
    data_dir = evals_dir / '10k_random'

    print("Loading data files...")

    # Load gold standard (no header)
    gold = load_tsv(data_dir / 'openai.tsv', has_header=False)
    print(f"  Gold standard: {len(gold)} records")

    # Load Parseland
    parseland = load_tsv(data_dir / 'parseland.tsv', has_header=True)
    print(f"  Parseland: {len(parseland)} records")

    # Load OpenAlex DOIs
    openalex_dois = load_openalex_with_dois(data_dir / 'openalex.tsv')
    print(f"  OpenAlex DOIs: {len(openalex_dois)} records")

    # Find Parseland failures for affiliations
    print("\nFinding Parseland failures...")
    affiliation_failures = []
    corresponding_failures = []

    for oid in gold.keys():
        gold_data = gold.get(oid)
        parseland_data = parseland.get(oid)
        doi = openalex_dois.get(oid, '')

        # Affiliation failures: gold has affiliations, parseland doesn't
        if has_any_affiliation(gold_data) and not has_any_affiliation(parseland_data):
            affiliation_failures.append({
                'openalex_id': oid,
                'doi': doi,
                'failure_type': 'affiliation',
                'source': 'evals'
            })

        # Corresponding author failures: gold has corr author, parseland doesn't
        if has_corresponding_author(gold_data) and not has_corresponding_author(parseland_data):
            corresponding_failures.append({
                'openalex_id': oid,
                'doi': doi,
                'failure_type': 'corresponding_author',
                'source': 'evals'
            })

    print(f"  Affiliation failures: {len(affiliation_failures)}")
    print(f"  Corresponding author failures: {len(corresponding_failures)}")

    # Load Nees DOIs (harvested_html.txt - ones that have landing pages)
    nees_dois = []
    nees_file = nees_dir / 'harvested_html.txt'
    if nees_file.exists():
        with open(nees_file, 'r') as f:
            for line in f:
                doi = line.strip()
                if doi:
                    nees_dois.append({
                        'openalex_id': '',  # Don't have this
                        'doi': doi,
                        'failure_type': 'affiliation',  # Nees was about missing affiliations
                        'source': 'nees'
                    })
        print(f"  Nees DOIs (harvested): {len(nees_dois)}")

    # Combine and deduplicate
    all_failures = affiliation_failures + corresponding_failures

    # Add Nees DOIs that aren't already in our list
    existing_dois = {f['doi'] for f in all_failures if f['doi']}
    for nees in nees_dois:
        if nees['doi'] and nees['doi'] not in existing_dois:
            all_failures.append(nees)
            existing_dois.add(nees['doi'])

    print(f"\nTotal unique failures: {len(all_failures)}")

    # Write output
    output_file = evals_dir / 'analysis' / 'parseland_failures.tsv'
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(['openalex_id', 'doi', 'failure_type', 'source'])
        for failure in all_failures:
            writer.writerow([
                failure['openalex_id'],
                failure['doi'],
                failure['failure_type'],
                failure['source']
            ])

    print(f"\nWrote failures to: {output_file}")

    # Summary by source
    sources = {}
    for f in all_failures:
        src = f['source']
        sources[src] = sources.get(src, 0) + 1

    print("\nBy source:")
    for src, count in sources.items():
        print(f"  {src}: {count}")

    # Summary by failure type
    types = {}
    for f in all_failures:
        t = f['failure_type']
        types[t] = types.get(t, 0) + 1

    print("\nBy failure type:")
    for t, count in types.items():
        print(f"  {t}: {count}")


if __name__ == '__main__':
    main()
