#!/usr/bin/env python3
"""
Cascade/Funnel Analysis for Affiliation and Corresponding Author Pipeline.

Analyzes where DOIs "fall out" of the pipeline:
Gold Standard → OpenAlex → Taxicab → Parseland → NEW vs OLD

Runs 3 analyses:
1. 10k Random - Affiliations
2. 10k Random - Corresponding Authors
3. Nees - Affiliations (OpenAlex level = all missing by definition)
"""

import csv
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Callable
from datetime import datetime


# ============================================
# DATA STRUCTURES
# ============================================

@dataclass
class CascadeLevel:
    """Single level in the cascade."""
    name: str
    count: int
    indent: int = 0  # For visual hierarchy


@dataclass
class CascadeResult:
    """Complete cascade analysis."""
    title: str
    levels: list[CascadeLevel]
    gold_total: int


# ============================================
# HELPER FUNCTIONS (from compare_before_after.py)
# ============================================

def has_any_affiliation(data: Optional[dict]) -> bool:
    """Check if any author has a non-empty affiliation."""
    if not data or 'authors' not in data:
        return False
    for author in data.get('authors', []):
        affiliations = author.get('affiliations', [])
        if affiliations and any(aff and str(aff).strip() for aff in affiliations):
            return True
    return False


def has_corresponding_author(data: Optional[dict]) -> bool:
    """Check if any author is marked as corresponding."""
    if not data or 'authors' not in data:
        return False
    for author in data.get('authors', []):
        if author.get('is_corresponding'):
            return True
    return False


# ============================================
# DATA LOADING
# ============================================

def load_tsv_by_id(filepath: Path, has_header: bool = True, id_column: int = 0) -> dict:
    """
    Load TSV and return dict mapping ID to parsed JSON data.
    Also returns html_uuid if present.
    """
    records = {}
    html_uuids = {}

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        if has_header:
            header = next(reader)
            # Find data column (usually last or named 'data')
            data_col = len(header) - 1
            uuid_col = None
            for i, h in enumerate(header):
                if h == 'data':
                    data_col = i
                if h == 'html_uuid':
                    uuid_col = i
        else:
            data_col = -1  # Last column
            uuid_col = None

        for row in reader:
            if not row:
                continue
            record_id = row[id_column]

            # Get data JSON
            data_str = row[data_col] if len(row) > data_col else ''
            try:
                records[record_id] = json.loads(data_str) if data_str else None
            except json.JSONDecodeError:
                records[record_id] = None

            # Get html_uuid if present
            if uuid_col is not None and len(row) > uuid_col:
                uuid = row[uuid_col].strip()
                if uuid:
                    html_uuids[record_id] = uuid

    return records, html_uuids


def load_openalex_with_years(filepath: Path) -> tuple[dict, dict, dict]:
    """
    Load OpenAlex TSV and return:
    - records: dict mapping ID to parsed JSON data
    - html_uuids: dict mapping ID to html_uuid (empty for openalex)
    - years: dict mapping ID to publication year
    """
    records = {}
    years = {}

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        header = next(reader)  # openalex_id, year, doi, data

        for row in reader:
            if not row or len(row) < 4:
                continue
            record_id = row[0]

            # Get year
            try:
                years[record_id] = int(row[1]) if row[1] else None
            except ValueError:
                years[record_id] = None

            # Get data JSON (last column)
            data_str = row[-1]
            try:
                records[record_id] = json.loads(data_str) if data_str else None
            except json.JSONDecodeError:
                records[record_id] = None

    return records, {}, years


def load_dois(filepath: Path) -> set:
    """Load DOIs from text file."""
    dois = set()
    with open(filepath, 'r') as f:
        for line in f:
            doi = line.strip()
            if doi:
                dois.add(doi)
    return dois


# ============================================
# CASCADE ANALYSIS
# ============================================

def analyze_cascade(
    title: str,
    gold_ids: set,
    feature_func: Callable,
    gold_data: dict,
    openalex_data: dict,
    parseland_original: dict,
    parseland_current: dict,
    html_uuids: dict,
    openalex_all_missing: bool = False  # For nees dataset
) -> CascadeResult:
    """
    Run cascade analysis for a given feature (affiliations or CA).

    Args:
        title: Analysis title
        gold_ids: Set of IDs in gold standard with this feature
        feature_func: Function to check if data has the feature
        gold_data: Gold standard data dict
        openalex_data: OpenAlex data dict
        parseland_original: Original parseland data dict
        parseland_current: Current (merged with rerun) parseland data dict
        html_uuids: Dict of ID -> html_uuid
        openalex_all_missing: If True, skip OpenAlex check (all are missing)
    """
    levels = []
    gold_total = len(gold_ids)

    # Level 1: Gold standard
    levels.append(CascadeLevel("Gold standard has feature", gold_total, indent=0))

    if openalex_all_missing:
        # Nees case: all are missing in OpenAlex by definition
        oa_has = set()
        oa_missing = gold_ids
        levels.append(CascadeLevel("OpenAlex has", 0, indent=1))
        levels.append(CascadeLevel("OpenAlex missing", len(oa_missing), indent=1))
    else:
        # Level 2: OpenAlex has/missing
        oa_has = {oid for oid in gold_ids if feature_func(openalex_data.get(oid))}
        oa_missing = gold_ids - oa_has
        levels.append(CascadeLevel("OpenAlex has", len(oa_has), indent=1))
        levels.append(CascadeLevel("OpenAlex missing", len(oa_missing), indent=1))

    # Level 3: Of OA missing, Taxicab has/missing
    taxicab_has = {oid for oid in oa_missing if oid in html_uuids}
    taxicab_missing = oa_missing - taxicab_has
    levels.append(CascadeLevel("Has Taxicab HTML", len(taxicab_has), indent=2))
    levels.append(CascadeLevel("No Taxicab HTML", len(taxicab_missing), indent=2))

    # Level 4: Of Taxicab has, Parseland extracts/fails
    parseland_extracts = {oid for oid in taxicab_has if feature_func(parseland_current.get(oid))}
    parseland_fails = taxicab_has - parseland_extracts
    levels.append(CascadeLevel("Parseland extracts", len(parseland_extracts), indent=3))
    levels.append(CascadeLevel("Parseland fails", len(parseland_fails), indent=3))

    # Level 5: Of Parseland extracts, OLD vs NEW
    old_count = sum(1 for oid in parseland_extracts if feature_func(parseland_original.get(oid)))
    new_count = len(parseland_extracts) - old_count
    levels.append(CascadeLevel("OLD (baseline)", old_count, indent=4))
    levels.append(CascadeLevel("NEW (improved)", new_count, indent=4))

    return CascadeResult(title, levels, gold_total)


# ============================================
# OUTPUT FORMATTING
# ============================================

def format_cascade_table(result: CascadeResult) -> str:
    """Format cascade as markdown table."""
    lines = []
    lines.append(f"| Level | Category | Count | % of Gold |")
    lines.append(f"|-------|----------|------:|----------:|")

    for level in result.levels:
        # Use non-breaking spaces for indentation (invisible but preserves hierarchy)
        indent_str = "\u00A0\u00A0\u00A0" * level.indent
        pct = 100 * level.count / result.gold_total if result.gold_total > 0 else 0
        lines.append(f"| {level.indent + 1} | {indent_str}{level.name} | {level.count:,} | {pct:.1f}% |")

    return "\n".join(lines)


def format_ascii_bar(count: int, total: int, max_width: int = 50) -> str:
    """Create ASCII bar for visual representation."""
    if total == 0:
        return ""
    width = int(max_width * count / total)
    return "█" * width


def format_cascade_visual(result: CascadeResult) -> str:
    """Format cascade as ASCII visual."""
    lines = []
    max_count = result.gold_total

    for level in result.levels:
        bar = format_ascii_bar(level.count, max_count, 40)
        indent_str = "  " * level.indent
        pct = 100 * level.count / result.gold_total if result.gold_total > 0 else 0
        lines.append(f"{bar:40} {level.count:>6,} ({pct:5.1f}%) {indent_str}{level.name}")

    return "\n".join(lines)


def generate_report(results: list[CascadeResult], output_path: Path,
                    year_results: list[tuple[str, list[CascadeResult]]] = None):
    """Generate complete markdown report."""
    lines = []
    lines.append("# Affiliation/CA Pipeline Cascade Analysis")
    lines.append(f"\n**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("\n---\n")

    # Summary section
    lines.append("## Executive Summary\n")
    for i, result in enumerate(results, 1):
        gold = result.gold_total
        oa_has = result.levels[1].count  # OpenAlex has
        oa_missing = result.levels[2].count  # OpenAlex missing
        parseland_extracts = result.levels[5].count if len(result.levels) > 5 else 0
        new_improvements = result.levels[8].count if len(result.levels) > 8 else 0  # Index 8 = NEW

        capture_rate = 100 * oa_has / gold if gold > 0 else 0
        lines.append(f"**{result.title}**:")
        lines.append(f"- Gold standard: {gold:,} DOIs")
        lines.append(f"- OpenAlex capture rate: {capture_rate:.1f}%")
        lines.append(f"- Parser improvements (NEW): +{new_improvements:,} DOIs")
        lines.append("")

    lines.append("---\n")

    # Detailed sections
    for i, result in enumerate(results, 1):
        lines.append(f"## {i}. {result.title}\n")
        lines.append("### Cascade Table\n")
        lines.append(format_cascade_table(result))

        # Add 2024-2025 combined cascade after "10k Random - Affiliations" section
        if result.title == "10k Random - Affiliations" and year_results:
            # Find the combined 2024-2025 cascade
            for year_str, cascades in year_results:
                if year_str == "2024-2025":
                    for cascade in cascades:
                        if cascade.title == "Affiliations":
                            lines.append("\n### Recent Years (2024-2025 Combined)\n")
                            lines.append("*Recent publications have lower OpenAlex coverage - this is where improvements matter most.*\n")
                            lines.append(format_cascade_table(cascade))
                            lines.append("")
                            break

        lines.append("\n---\n")

    # Key insights
    lines.append("## Key Insights\n")

    for result in results:
        lines.append(f"### {result.title}\n")

        gold = result.gold_total
        oa_has = result.levels[1].count
        oa_missing = result.levels[2].count
        taxicab_has = result.levels[3].count
        no_taxicab = result.levels[4].count
        parseland_extracts = result.levels[5].count
        parseland_fails = result.levels[6].count
        old_count = result.levels[7].count
        new_count = result.levels[8].count if len(result.levels) > 8 else 0

        lines.append(f"- **Captured by OpenAlex**: {oa_has:,} ({100*oa_has/gold:.1f}%)")
        lines.append(f"- **Gap (missing in OA)**: {oa_missing:,} ({100*oa_missing/gold:.1f}%)")
        lines.append(f"  - Lost to no Taxicab HTML: {no_taxicab:,}")
        lines.append(f"  - Lost to Parseland failure: {parseland_fails:,}")
        lines.append(f"  - **Parseland can fill**: {parseland_extracts:,}")
        lines.append(f"    - Already working (OLD): {old_count:,}")
        lines.append(f"    - New improvements (NEW): {new_count:,}")
        lines.append("")

    with open(output_path, 'w') as f:
        f.write("\n".join(lines))

    print(f"Report written to: {output_path}")


# ============================================
# MAIN
# ============================================

def main():
    base_dir = Path('/Users/jasonpriem/Documents/evals')
    output_dir = base_dir / 'analysis' / 'overall' / '2025-01-04'

    print("Loading data files...")

    # 10k random dataset
    gold_10k, _ = load_tsv_by_id(base_dir / '10k_random' / 'openai.tsv', has_header=False)
    print(f"  Gold standard (OpenAI): {len(gold_10k)} records")

    openalex_10k, _, years_10k = load_openalex_with_years(base_dir / '10k_random' / 'openalex.tsv')
    print(f"  OpenAlex: {len(openalex_10k)} records, {len(years_10k)} with year data")

    parseland_original_10k, html_uuids_10k = load_tsv_by_id(
        base_dir / '10k_random' / 'parseland.tsv', has_header=True
    )
    print(f"  Parseland original: {len(parseland_original_10k)} records, {len(html_uuids_10k)} with html_uuid")

    parseland_rerun, _ = load_tsv_by_id(
        base_dir / 'runs' / '2025-01-03_preloaded_state_fix' / 'parseland_rerun.tsv',
        has_header=True
    )
    print(f"  Parseland rerun: {len(parseland_rerun)} records")

    # Merge parseland: rerun overrides original
    parseland_current_10k = {**parseland_original_10k, **parseland_rerun}
    print(f"  Parseland current (merged): {len(parseland_current_10k)} records")

    # Nees dataset
    nees_dois = load_dois(base_dir / 'runs' / '2025-01-03_preloaded_state_fix' / 'nees_test' / 'nees_dois.txt')
    print(f"  Nees DOIs: {len(nees_dois)}")

    # Load nees parseland - keyed by DOI (column 0)
    parseland_nees, html_uuids_nees = load_tsv_by_id(
        base_dir / 'runs' / '2025-01-03_preloaded_state_fix' / 'nees_test' / 'parseland_results.tsv',
        has_header=True,
        id_column=0
    )
    print(f"  Nees Parseland: {len(parseland_nees)} records, {len(html_uuids_nees)} with html_uuid")

    print("\nRunning cascade analyses...")
    results = []

    # Analysis 1: 10k Random - Affiliations
    gold_ids_affil = {oid for oid, data in gold_10k.items() if has_any_affiliation(data)}
    print(f"\n1. 10k Random - Affiliations: {len(gold_ids_affil)} in gold standard")

    result_affil = analyze_cascade(
        title="10k Random - Affiliations",
        gold_ids=gold_ids_affil,
        feature_func=has_any_affiliation,
        gold_data=gold_10k,
        openalex_data=openalex_10k,
        parseland_original=parseland_original_10k,
        parseland_current=parseland_current_10k,
        html_uuids=html_uuids_10k
    )
    results.append(result_affil)

    # Analysis 2: 10k Random - Corresponding Authors
    gold_ids_ca = {oid for oid, data in gold_10k.items() if has_corresponding_author(data)}
    print(f"2. 10k Random - Corresponding Authors: {len(gold_ids_ca)} in gold standard")

    result_ca = analyze_cascade(
        title="10k Random - Corresponding Authors",
        gold_ids=gold_ids_ca,
        feature_func=has_corresponding_author,
        gold_data=gold_10k,
        openalex_data=openalex_10k,
        parseland_original=parseland_original_10k,
        parseland_current=parseland_current_10k,
        html_uuids=html_uuids_10k
    )
    results.append(result_ca)

    # Analysis 3: Nees - Affiliations
    # Gold = all 316 DOIs (by definition they have affils on publisher sites)
    # OpenAlex = all missing (that's why they're in nees)
    print(f"3. Nees - Affiliations: {len(nees_dois)} in gold standard (all missing in OA)")

    result_nees = analyze_cascade(
        title="Nees - Affiliations",
        gold_ids=nees_dois,
        feature_func=has_any_affiliation,
        gold_data={},  # Not used since all are in gold by definition
        openalex_data={},  # Not used since all are missing
        parseland_original=parseland_nees,  # Original = current for nees (no separate rerun)
        parseland_current=parseland_nees,
        html_uuids=html_uuids_nees,
        openalex_all_missing=True
    )
    results.append(result_nees)

    # Combined 2024-2025 analysis for affiliations (10k random only)
    print("\nRunning combined 2024-2025 analysis for affiliations...")
    year_results = []

    # Filter gold IDs by years 2024 and 2025
    gold_affil_recent = {oid for oid in gold_ids_affil if years_10k.get(oid) in [2024, 2025]}

    if gold_affil_recent:
        result = analyze_cascade(
            title="Affiliations",
            gold_ids=gold_affil_recent,
            feature_func=has_any_affiliation,
            gold_data=gold_10k,
            openalex_data=openalex_10k,
            parseland_original=parseland_original_10k,
            parseland_current=parseland_current_10k,
            html_uuids=html_uuids_10k
        )
        year_results.append(("2024-2025", [result]))
        print(f"  2024-2025 Combined - Affiliations: {len(gold_affil_recent)} in gold")

    # Generate report
    print("\nGenerating report...")
    generate_report(results, output_dir / 'report.md', year_results=year_results)

    # Also print summary to console
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    for result in results:
        print(f"\n{result.title}:")
        for level in result.levels:
            indent = "  " * level.indent
            pct = 100 * level.count / result.gold_total if result.gold_total > 0 else 0
            print(f"  {indent}{level.name}: {level.count:,} ({pct:.1f}%)")


if __name__ == '__main__':
    main()
