#!/usr/bin/env python3
"""
Compare Parseland results before and after the 2025-01-03 improvements.
Measures improvement in affiliation extraction and corresponding author detection.
"""

import csv
import json
from pathlib import Path
from collections import defaultdict

def load_tsv(filepath):
    """Load TSV file and return dict mapping openalex_id to parsed data."""
    records = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            oid = row.get('openalex_id', '')
            data_str = row.get('data', '')
            if oid and data_str:
                try:
                    records[oid] = json.loads(data_str)
                except json.JSONDecodeError:
                    records[oid] = None
            elif oid:
                records[oid] = None
    return records

def has_any_affiliation(data):
    """Check if any author has a non-empty affiliation."""
    if not data or 'authors' not in data:
        return False
    for author in data.get('authors', []):
        affiliations = author.get('affiliations', [])
        if affiliations and any(aff and str(aff).strip() for aff in affiliations):
            return True
    return False

def has_corresponding_author(data):
    """Check if any author is marked as corresponding."""
    if not data or 'authors' not in data:
        return False
    for author in data.get('authors', []):
        if author.get('is_corresponding'):
            return True
    return False

def load_failures(filepath):
    """Load the original failures file to categorize by failure type."""
    failures = {}
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            oid = row.get('openalex_id', '')
            if oid:
                failures[oid] = row.get('failure_type', 'unknown')
    return failures

def main():
    base_dir = Path(__file__).parent
    evals_dir = base_dir.parent.parent

    # Load data
    print("Loading data...")

    # Original parseland results (before fix)
    original = load_tsv(evals_dir / '10k_random' / 'parseland.tsv')
    print(f"  Original Parseland: {len(original)} records")

    # New parseland results (after fix)
    new_path = base_dir / 'parseland_rerun.tsv'
    if not new_path.exists():
        print(f"  ERROR: {new_path} not found. Run get_parseland.py first.")
        return
    new = load_tsv(new_path)
    print(f"  New Parseland: {len(new)} records")

    # Load failure types
    failures = load_failures(evals_dir / 'analysis' / 'parseland_failures.tsv')
    print(f"  Failure records: {len(failures)}")

    # Compare
    print("\n=== Comparison Results ===\n")

    # Track improvements
    improvements = {
        'affiliation': {'fixed': 0, 'still_failing': 0, 'regressed': 0},
        'corresponding_author': {'fixed': 0, 'still_failing': 0, 'regressed': 0},
    }

    fixed_examples = {'affiliation': [], 'corresponding_author': []}

    for oid in new.keys():
        if oid not in original:
            continue

        old_data = original.get(oid)
        new_data = new.get(oid)
        failure_type = failures.get(oid, 'unknown')

        if failure_type == 'affiliation':
            old_has = has_any_affiliation(old_data)
            new_has = has_any_affiliation(new_data)

            if not old_has and new_has:
                improvements['affiliation']['fixed'] += 1
                if len(fixed_examples['affiliation']) < 5:
                    fixed_examples['affiliation'].append(oid)
            elif not old_has and not new_has:
                improvements['affiliation']['still_failing'] += 1
            elif old_has and not new_has:
                improvements['affiliation']['regressed'] += 1

        elif failure_type == 'corresponding_author':
            old_has = has_corresponding_author(old_data)
            new_has = has_corresponding_author(new_data)

            if not old_has and new_has:
                improvements['corresponding_author']['fixed'] += 1
                if len(fixed_examples['corresponding_author']) < 5:
                    fixed_examples['corresponding_author'].append(oid)
            elif not old_has and not new_has:
                improvements['corresponding_author']['still_failing'] += 1
            elif old_has and not new_has:
                improvements['corresponding_author']['regressed'] += 1

    # Print results
    print("Affiliation Failures:")
    aff = improvements['affiliation']
    total_aff = aff['fixed'] + aff['still_failing'] + aff['regressed']
    print(f"  Fixed: {aff['fixed']} ({aff['fixed']/total_aff*100:.1f}%)" if total_aff > 0 else "  Fixed: 0")
    print(f"  Still failing: {aff['still_failing']}")
    print(f"  Regressed: {aff['regressed']}")

    print("\nCorresponding Author Failures:")
    corr = improvements['corresponding_author']
    total_corr = corr['fixed'] + corr['still_failing'] + corr['regressed']
    print(f"  Fixed: {corr['fixed']} ({corr['fixed']/total_corr*100:.1f}%)" if total_corr > 0 else "  Fixed: 0")
    print(f"  Still failing: {corr['still_failing']}")
    print(f"  Regressed: {corr['regressed']}")

    # Summary
    total_fixed = aff['fixed'] + corr['fixed']
    total_tested = total_aff + total_corr
    print(f"\n=== Summary ===")
    print(f"Total tested: {total_tested}")
    print(f"Total fixed: {total_fixed} ({total_fixed/total_tested*100:.1f}%)" if total_tested > 0 else "Total fixed: 0")
    print(f"Total regressions: {aff['regressed'] + corr['regressed']}")

    # Examples
    print("\n=== Example Fixes ===")
    print("\nAffiliation fixes:")
    for oid in fixed_examples['affiliation']:
        print(f"  - {oid}")
    print("\nCorresponding author fixes:")
    for oid in fixed_examples['corresponding_author']:
        print(f"  - {oid}")

    # Write results to markdown
    with open(base_dir / 'comparison_results.md', 'w') as f:
        f.write("# Comparison Results: Before vs After 2025-01-03 Fixes\n\n")
        f.write(f"**Date**: 2025-01-03\n")
        f.write(f"**Commit**: 19363d4\n\n")

        f.write("## Summary\n\n")
        f.write(f"| Metric | Before | After | Improvement |\n")
        f.write(f"|--------|--------|-------|-------------|\n")
        f.write(f"| Affiliation failures fixed | 0 | {aff['fixed']} | +{aff['fixed']} |\n")
        f.write(f"| Corresponding author failures fixed | 0 | {corr['fixed']} | +{corr['fixed']} |\n")
        f.write(f"| **Total fixed** | 0 | {total_fixed} | **+{total_fixed}** |\n")
        f.write(f"| Regressions | 0 | {aff['regressed'] + corr['regressed']} | {aff['regressed'] + corr['regressed']} |\n")

        f.write("\n## Detailed Results\n\n")
        f.write("### Affiliation Extraction\n\n")
        f.write(f"- **Fixed**: {aff['fixed']}")
        if total_aff > 0:
            f.write(f" ({aff['fixed']/total_aff*100:.1f}%)")
        f.write(f"\n- **Still failing**: {aff['still_failing']}\n")
        f.write(f"- **Regressed**: {aff['regressed']}\n")

        f.write("\n### Corresponding Author Detection\n\n")
        f.write(f"- **Fixed**: {corr['fixed']}")
        if total_corr > 0:
            f.write(f" ({corr['fixed']/total_corr*100:.1f}%)")
        f.write(f"\n- **Still failing**: {corr['still_failing']}\n")
        f.write(f"- **Regressed**: {corr['regressed']}\n")

        f.write("\n## Example Fixes\n\n")
        f.write("### Affiliation fixes\n\n")
        for oid in fixed_examples['affiliation']:
            f.write(f"- `{oid}`\n")
        f.write("\n### Corresponding author fixes\n\n")
        for oid in fixed_examples['corresponding_author']:
            f.write(f"- `{oid}`\n")

    print(f"\nResults written to {base_dir / 'comparison_results.md'}")

if __name__ == '__main__':
    main()
