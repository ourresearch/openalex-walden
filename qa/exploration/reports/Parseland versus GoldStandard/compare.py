#!/usr/bin/env python3
"""
Compare Parseland and OpenAlex against OpenAI Gold Standard.

Analyzes two binary features:
1. Has any affiliation? - Does ANY author have a non-empty affiliation string?
2. Has any corresponding author? - Is ANY author marked is_corresponding: true?
"""

import json
import csv
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class ConfusionMatrix:
    tp: int = 0  # True Positive
    fp: int = 0  # False Positive
    fn: int = 0  # False Negative
    tn: int = 0  # True Negative

    @property
    def precision(self) -> float:
        if self.tp + self.fp == 0:
            return 0.0
        return self.tp / (self.tp + self.fp)

    @property
    def recall(self) -> float:
        if self.tp + self.fn == 0:
            return 0.0
        return self.tp / (self.tp + self.fn)

    @property
    def f1(self) -> float:
        if self.precision + self.recall == 0:
            return 0.0
        return 2 * (self.precision * self.recall) / (self.precision + self.recall)

    @property
    def accuracy(self) -> float:
        total = self.tp + self.fp + self.fn + self.tn
        if total == 0:
            return 0.0
        return (self.tp + self.tn) / total

    @property
    def total(self) -> int:
        return self.tp + self.fp + self.fn + self.tn


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
    """Load TSV file and return dict mapping openalex_id to data JSON."""
    records = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        if has_header:
            next(reader)  # Skip header
        for row in reader:
            if len(row) >= 1:
                openalex_id = row[0]
                # Data column is the last column
                data_str = row[-1] if len(row) > 1 else ''
                records[openalex_id] = parse_data_json(data_str)
    return records


def compare_feature(gold: dict, test: dict, feature_func, gold_ids: set) -> tuple[ConfusionMatrix, list, list]:
    """
    Compare a binary feature between gold standard and test dataset.
    Returns confusion matrix and lists of false negative and false positive IDs.
    """
    cm = ConfusionMatrix()
    false_negatives = []  # Gold has it, test doesn't
    false_positives = []  # Test has it, gold doesn't

    for oid in gold_ids:
        gold_data = gold.get(oid)
        test_data = test.get(oid)

        gold_has = feature_func(gold_data)
        test_has = feature_func(test_data)

        if gold_has and test_has:
            cm.tp += 1
        elif gold_has and not test_has:
            cm.fn += 1
            false_negatives.append(oid)
        elif not gold_has and test_has:
            cm.fp += 1
            false_positives.append(oid)
        else:
            cm.tn += 1

    return cm, false_negatives, false_positives


def main():
    data_dir = Path('/Users/jasonpriem/Documents/evals/10k_random')

    print("Loading data files...")

    # Load gold standard (openai.tsv has no header based on inspection)
    gold = load_tsv(data_dir / 'openai.tsv', has_header=False)
    print(f"  Gold standard (OpenAI): {len(gold)} records")

    # Load Parseland and OpenAlex (both have headers)
    parseland = load_tsv(data_dir / 'parseland.tsv', has_header=True)
    print(f"  Parseland: {len(parseland)} records")

    openalex = load_tsv(data_dir / 'openalex.tsv', has_header=True)
    print(f"  OpenAlex: {len(openalex)} records")

    # Only analyze records that exist in gold standard
    gold_ids = set(gold.keys())
    print(f"\nAnalyzing {len(gold_ids)} records from gold standard\n")

    # Check coverage
    parseland_missing = gold_ids - set(parseland.keys())
    openalex_missing = gold_ids - set(openalex.keys())
    print(f"Parseland missing from gold IDs: {len(parseland_missing)}")
    print(f"OpenAlex missing from gold IDs: {len(openalex_missing)}")

    # Count records with valid data
    gold_with_data = sum(1 for oid in gold_ids if gold.get(oid) is not None)
    parseland_with_data = sum(1 for oid in gold_ids if parseland.get(oid) is not None)
    openalex_with_data = sum(1 for oid in gold_ids if openalex.get(oid) is not None)
    print(f"\nRecords with valid JSON data:")
    print(f"  Gold standard: {gold_with_data}")
    print(f"  Parseland: {parseland_with_data}")
    print(f"  OpenAlex: {openalex_with_data}")

    print("\n" + "="*70)
    print("FEATURE 1: Has Any Affiliation?")
    print("="*70)

    # Gold standard stats for affiliations
    gold_has_affil = sum(1 for oid in gold_ids if has_any_affiliation(gold.get(oid)))
    print(f"\nGold standard: {gold_has_affil} records have affiliations ({100*gold_has_affil/len(gold_ids):.1f}%)")

    # Parseland vs Gold
    cm_parseland_affil, fn_parseland_affil, fp_parseland_affil = compare_feature(
        gold, parseland, has_any_affiliation, gold_ids
    )

    print(f"\nParseland vs Gold Standard:")
    print(f"  TP: {cm_parseland_affil.tp} | FP: {cm_parseland_affil.fp}")
    print(f"  FN: {cm_parseland_affil.fn} | TN: {cm_parseland_affil.tn}")
    print(f"  Precision: {cm_parseland_affil.precision:.4f}")
    print(f"  Recall:    {cm_parseland_affil.recall:.4f}")
    print(f"  F1 Score:  {cm_parseland_affil.f1:.4f}")
    print(f"  Accuracy:  {cm_parseland_affil.accuracy:.4f}")

    # OpenAlex vs Gold
    cm_openalex_affil, fn_openalex_affil, fp_openalex_affil = compare_feature(
        gold, openalex, has_any_affiliation, gold_ids
    )

    print(f"\nOpenAlex vs Gold Standard:")
    print(f"  TP: {cm_openalex_affil.tp} | FP: {cm_openalex_affil.fp}")
    print(f"  FN: {cm_openalex_affil.fn} | TN: {cm_openalex_affil.tn}")
    print(f"  Precision: {cm_openalex_affil.precision:.4f}")
    print(f"  Recall:    {cm_openalex_affil.recall:.4f}")
    print(f"  F1 Score:  {cm_openalex_affil.f1:.4f}")
    print(f"  Accuracy:  {cm_openalex_affil.accuracy:.4f}")

    print("\n" + "="*70)
    print("FEATURE 2: Has Any Corresponding Author?")
    print("="*70)

    # Gold standard stats for corresponding authors
    gold_has_corr = sum(1 for oid in gold_ids if has_corresponding_author(gold.get(oid)))
    print(f"\nGold standard: {gold_has_corr} records have corresponding authors ({100*gold_has_corr/len(gold_ids):.1f}%)")

    # Parseland vs Gold
    cm_parseland_corr, fn_parseland_corr, fp_parseland_corr = compare_feature(
        gold, parseland, has_corresponding_author, gold_ids
    )

    print(f"\nParseland vs Gold Standard:")
    print(f"  TP: {cm_parseland_corr.tp} | FP: {cm_parseland_corr.fp}")
    print(f"  FN: {cm_parseland_corr.fn} | TN: {cm_parseland_corr.tn}")
    print(f"  Precision: {cm_parseland_corr.precision:.4f}")
    print(f"  Recall:    {cm_parseland_corr.recall:.4f}")
    print(f"  F1 Score:  {cm_parseland_corr.f1:.4f}")
    print(f"  Accuracy:  {cm_parseland_corr.accuracy:.4f}")

    # OpenAlex vs Gold
    cm_openalex_corr, fn_openalex_corr, fp_openalex_corr = compare_feature(
        gold, openalex, has_corresponding_author, gold_ids
    )

    print(f"\nOpenAlex vs Gold Standard:")
    print(f"  TP: {cm_openalex_corr.tp} | FP: {cm_openalex_corr.fp}")
    print(f"  FN: {cm_openalex_corr.fn} | TN: {cm_openalex_corr.tn}")
    print(f"  Precision: {cm_openalex_corr.precision:.4f}")
    print(f"  Recall:    {cm_openalex_corr.recall:.4f}")
    print(f"  F1 Score:  {cm_openalex_corr.f1:.4f}")
    print(f"  Accuracy:  {cm_openalex_corr.accuracy:.4f}")

    print("\n" + "="*70)
    print("SUMMARY COMPARISON")
    print("="*70)

    print("\n                    | Affiliations          | Corresponding Author")
    print("                    | Precision | Recall    | Precision | Recall")
    print("-"*70)
    print(f"Parseland           | {cm_parseland_affil.precision:.4f}    | {cm_parseland_affil.recall:.4f}    | {cm_parseland_corr.precision:.4f}    | {cm_parseland_corr.recall:.4f}")
    print(f"OpenAlex            | {cm_openalex_affil.precision:.4f}    | {cm_openalex_affil.recall:.4f}    | {cm_openalex_corr.precision:.4f}    | {cm_openalex_corr.recall:.4f}")

    # Calculate the gap
    affil_recall_gap = cm_openalex_affil.recall - cm_parseland_affil.recall
    corr_recall_gap = cm_openalex_corr.recall - cm_parseland_corr.recall

    print(f"\nRecall Gap (OpenAlex - Parseland):")
    print(f"  Affiliations: {affil_recall_gap:+.4f}")
    print(f"  Corresponding: {corr_recall_gap:+.4f}")

    # Sample false negatives for investigation
    print("\n" + "="*70)
    print("SAMPLE FALSE NEGATIVES (Parseland missing data)")
    print("="*70)

    print("\nAffiliations - Sample of records where gold has affiliations but Parseland doesn't:")
    for oid in fn_parseland_affil[:5]:
        gold_data = gold.get(oid)
        parseland_data = parseland.get(oid)
        print(f"\n  {oid}")
        if gold_data and 'authors' in gold_data:
            for auth in gold_data['authors'][:2]:
                affils = auth.get('affiliations', [])
                if affils:
                    print(f"    Gold: {auth.get('name', 'N/A')} -> {affils[0][:60]}...")
        if parseland_data is None:
            print(f"    Parseland: NO DATA")
        else:
            print(f"    Parseland: {parseland_data.get('authors', [])[:1]}")

    print("\nCorresponding Authors - Sample of records where gold has corr. author but Parseland doesn't:")
    for oid in fn_parseland_corr[:5]:
        gold_data = gold.get(oid)
        parseland_data = parseland.get(oid)
        print(f"\n  {oid}")
        if gold_data and 'authors' in gold_data:
            for auth in gold_data['authors']:
                if auth.get('is_corresponding'):
                    print(f"    Gold: {auth.get('name', 'N/A')} is_corresponding=True")
                    break
        if parseland_data is None:
            print(f"    Parseland: NO DATA")
        elif 'authors' in parseland_data:
            corr_authors = [a for a in parseland_data['authors'] if a.get('is_corresponding')]
            print(f"    Parseland: {len(corr_authors)} corresponding authors found")

    print("\n" + "="*70)
    print("INTERVENTION ANALYSIS")
    print("="*70)

    # For affiliations: Of records where OpenAlex is missing data (FN),
    # how many could Parseland provide if improved?
    print("\n--- AFFILIATIONS ---")
    print(f"OpenAlex currently has affiliations for: {cm_openalex_affil.tp} records")
    print(f"OpenAlex is missing (Gold=Yes, OA=No): {cm_openalex_affil.fn} records")

    # Check what Parseland has for those OpenAlex FN cases
    openalex_fn_affil = set(fn_openalex_affil)
    parseland_has_for_oa_fn = 0
    parseland_missing_for_oa_fn = 0

    for oid in openalex_fn_affil:
        if has_any_affiliation(parseland.get(oid)):
            parseland_has_for_oa_fn += 1
        else:
            parseland_missing_for_oa_fn += 1

    print(f"\nOf those {cm_openalex_affil.fn} OpenAlex misses:")
    print(f"  - Parseland ALSO misses: {parseland_missing_for_oa_fn}")
    print(f"  - Parseland HAS the data: {parseland_has_for_oa_fn} (pipeline issue?)")

    print(f"\nIf Parseland achieved 100% recall:")
    print(f"  - Could fill: {parseland_missing_for_oa_fn} additional records for OpenAlex")
    print(f"  - OpenAlex affiliations would go from {cm_openalex_affil.tp} to {cm_openalex_affil.tp + parseland_missing_for_oa_fn}")
    pct_increase_affil = 100 * parseland_missing_for_oa_fn / cm_openalex_affil.tp if cm_openalex_affil.tp > 0 else 0
    print(f"  - That's a {pct_increase_affil:.1f}% increase in OpenAlex affiliation coverage")

    # For corresponding authors: same analysis
    print("\n--- CORRESPONDING AUTHORS ---")
    print(f"OpenAlex currently has corresponding authors for: {cm_openalex_corr.tp} records")
    print(f"OpenAlex is missing (Gold=Yes, OA=No): {cm_openalex_corr.fn} records")

    openalex_fn_corr = set(fn_openalex_corr)
    parseland_has_corr_for_oa_fn = 0
    parseland_missing_corr_for_oa_fn = 0

    for oid in openalex_fn_corr:
        if has_corresponding_author(parseland.get(oid)):
            parseland_has_corr_for_oa_fn += 1
        else:
            parseland_missing_corr_for_oa_fn += 1

    print(f"\nOf those {cm_openalex_corr.fn} OpenAlex misses:")
    print(f"  - Parseland ALSO misses: {parseland_missing_corr_for_oa_fn}")
    print(f"  - Parseland HAS the data: {parseland_has_corr_for_oa_fn} (pipeline issue?)")

    print(f"\nIf Parseland achieved 100% recall:")
    print(f"  - Could fill: {parseland_missing_corr_for_oa_fn} additional records for OpenAlex")
    print(f"  - OpenAlex corresponding authors would go from {cm_openalex_corr.tp} to {cm_openalex_corr.tp + parseland_missing_corr_for_oa_fn}")
    pct_increase_corr = 100 * parseland_missing_corr_for_oa_fn / cm_openalex_corr.tp if cm_openalex_corr.tp > 0 else 0
    print(f"  - That's a {pct_increase_corr:.1f}% increase in OpenAlex corresponding author coverage")

    # Calculate coverage in percentage points
    gold_total = len(gold_ids)

    # Current coverage (percentage points)
    current_affil_coverage = 100 * cm_openalex_affil.tp / gold_has_affil
    current_corr_coverage = 100 * cm_openalex_corr.tp / gold_has_corr

    # Pipeline fix impact
    pipeline_fix_affil = parseland_has_for_oa_fn
    pipeline_fix_corr = parseland_has_corr_for_oa_fn

    after_pipeline_affil = cm_openalex_affil.tp + pipeline_fix_affil
    after_pipeline_corr = cm_openalex_corr.tp + pipeline_fix_corr

    after_pipeline_affil_coverage = 100 * after_pipeline_affil / gold_has_affil
    after_pipeline_corr_coverage = 100 * after_pipeline_corr / gold_has_corr

    pipeline_affil_gain = after_pipeline_affil_coverage - current_affil_coverage
    pipeline_corr_gain = after_pipeline_corr_coverage - current_corr_coverage

    # Perfect Parseland impact (on top of current, without pipeline fix)
    perfect_parseland_affil = parseland_missing_for_oa_fn
    perfect_parseland_corr = parseland_missing_corr_for_oa_fn

    after_parseland_affil = cm_openalex_affil.tp + perfect_parseland_affil
    after_parseland_corr = cm_openalex_corr.tp + perfect_parseland_corr

    after_parseland_affil_coverage = 100 * after_parseland_affil / gold_has_affil
    after_parseland_corr_coverage = 100 * after_parseland_corr / gold_has_corr

    parseland_affil_gain = after_parseland_affil_coverage - current_affil_coverage
    parseland_corr_gain = after_parseland_corr_coverage - current_corr_coverage

    # Both interventions combined
    both_affil = cm_openalex_affil.tp + pipeline_fix_affil + perfect_parseland_affil
    both_corr = cm_openalex_corr.tp + pipeline_fix_corr + perfect_parseland_corr

    # Cap at gold standard (can't exceed 100%)
    both_affil = min(both_affil, gold_has_affil)
    both_corr = min(both_corr, gold_has_corr)

    both_affil_coverage = 100 * both_affil / gold_has_affil
    both_corr_coverage = 100 * both_corr / gold_has_corr

    both_affil_gain = both_affil_coverage - current_affil_coverage
    both_corr_gain = both_corr_coverage - current_corr_coverage

    # Calculate 4 MUTUALLY EXCLUSIVE categories (should sum to 100%):
    # 1. OAX has it now (OpenAlex=Yes, regardless of Gold)
    # 2. Pipeline fix would add (OpenAlex=No, Gold=Yes, Parseland=Yes)
    # 3. Perfect Parseland would add (OpenAlex=No, Gold=Yes, Parseland=No)
    # 4. Unavailable anywhere (OpenAlex=No, Gold=No)

    total_records = len(gold_ids)

    # Category 1: OAX has it now (TP + FP)
    oax_has_affil = cm_openalex_affil.tp + cm_openalex_affil.fp
    oax_has_corr = cm_openalex_corr.tp + cm_openalex_corr.fp
    oax_has_affil_pct = 100 * oax_has_affil / total_records
    oax_has_corr_pct = 100 * oax_has_corr / total_records

    # Category 2: Pipeline fix (OAX=No, Gold=Yes, Parseland=Yes)
    pipeline_affil = pipeline_fix_affil  # already calculated above
    pipeline_corr = pipeline_fix_corr
    pipeline_affil_pct = 100 * pipeline_affil / total_records
    pipeline_corr_pct = 100 * pipeline_corr / total_records

    # Category 3: Perfect Parseland (OAX=No, Gold=Yes, Parseland=No)
    parseland_affil = perfect_parseland_affil  # already calculated above
    parseland_corr = perfect_parseland_corr
    parseland_affil_pct = 100 * parseland_affil / total_records
    parseland_corr_pct = 100 * parseland_corr / total_records

    # Category 4: Unavailable anywhere (OAX=No, Gold=No) = TN
    unavail_affil = cm_openalex_affil.tn
    unavail_corr = cm_openalex_corr.tn
    unavail_affil_pct = 100 * unavail_affil / total_records
    unavail_corr_pct = 100 * unavail_corr / total_records

    # Verify they sum to 100%
    affil_sum = oax_has_affil_pct + pipeline_affil_pct + parseland_affil_pct + unavail_affil_pct
    corr_sum = oax_has_corr_pct + pipeline_corr_pct + parseland_corr_pct + unavail_corr_pct

    print("\n" + "="*70)
    print("COVERAGE BREAKDOWN (% of all records, mutually exclusive)")
    print("="*70)

    print("\n                              | AFFILIATIONS | CORRESPONDING AUTHORS")
    print("-"*60)
    print(f"OAX has it now                |    {oax_has_affil_pct:5.1f}%   |    {oax_has_corr_pct:5.1f}%")
    print(f"+ Pipeline fix                |    {pipeline_affil_pct:5.1f}%   |    {pipeline_corr_pct:5.1f}%")
    print(f"+ Perfect Parseland           |    {parseland_affil_pct:5.1f}%   |    {parseland_corr_pct:5.1f}%")
    print(f"Unavailable anywhere          |    {unavail_affil_pct:5.1f}%   |    {unavail_corr_pct:5.1f}%")
    print("-"*60)
    print(f"TOTAL                         |   {affil_sum:5.1f}%   |   {corr_sum:5.1f}%")

    print("\n" + "="*70)
    print("BREAKDOWN BY YEAR (3-YEAR BINS)")
    print("="*70)

    # Load openalex data with years
    openalex_with_years = {}
    with open(data_dir / 'openalex.tsv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)  # Skip header
        for row in reader:
            if len(row) >= 2:
                oid = row[0]
                try:
                    year = int(row[1]) if row[1] else None
                except ValueError:
                    year = None
                openalex_with_years[oid] = year

    # Define year bins
    year_bins = [
        (None, 1999, "≤1999"),
        (2000, 2005, "2000-05"),
        (2006, 2008, "2006-08"),
        (2009, 2011, "2009-11"),
        (2012, 2014, "2012-14"),
        (2015, 2017, "2015-17"),
        (2018, 2020, "2018-20"),
        (2021, 2023, "2021-23"),
        (2024, 2026, "2024-26"),
    ]

    def get_bin(year):
        if year is None:
            return None
        for start, end, label in year_bins:
            if start is None and year <= end:
                return label
            elif start is not None and start <= year <= end:
                return label
        return None

    # Calculate metrics by year bin using 4 mutually exclusive categories
    print("\n--- AFFILIATIONS BY YEAR (% of all records, mutually exclusive) ---")
    print("Year Bin  |   n   | OAX Has | +Pipeline | +Parseland | Unavail")
    print("-"*65)

    for start, end, label in year_bins:
        bin_ids = [oid for oid in gold_ids if get_bin(openalex_with_years.get(oid)) == label]
        if len(bin_ids) < 10:
            continue

        bin_total = len(bin_ids)

        # 4 mutually exclusive categories
        bin_oax_has = sum(1 for oid in bin_ids if has_any_affiliation(openalex.get(oid)))
        bin_pipeline = sum(1 for oid in bin_ids
                          if not has_any_affiliation(openalex.get(oid))
                          and has_any_affiliation(gold.get(oid))
                          and has_any_affiliation(parseland.get(oid)))
        bin_parseland = sum(1 for oid in bin_ids
                           if not has_any_affiliation(openalex.get(oid))
                           and has_any_affiliation(gold.get(oid))
                           and not has_any_affiliation(parseland.get(oid)))
        bin_unavail = sum(1 for oid in bin_ids
                         if not has_any_affiliation(openalex.get(oid))
                         and not has_any_affiliation(gold.get(oid)))

        bin_oax_pct = 100 * bin_oax_has / bin_total
        bin_pipeline_pct = 100 * bin_pipeline / bin_total
        bin_parseland_pct = 100 * bin_parseland / bin_total
        bin_unavail_pct = 100 * bin_unavail / bin_total

        print(f"{label:9} | {bin_total:5} | {bin_oax_pct:6.1f}% | {bin_pipeline_pct:8.1f}% | {bin_parseland_pct:9.1f}% | {bin_unavail_pct:6.1f}%")

    print("\n--- CORRESPONDING AUTHORS BY YEAR (% of all records, mutually exclusive) ---")
    print("Year Bin  |   n   | OAX Has | +Pipeline | +Parseland | Unavail")
    print("-"*65)

    for start, end, label in year_bins:
        bin_ids = [oid for oid in gold_ids if get_bin(openalex_with_years.get(oid)) == label]
        if len(bin_ids) < 10:
            continue

        bin_total = len(bin_ids)

        # 4 mutually exclusive categories
        bin_oax_has = sum(1 for oid in bin_ids if has_corresponding_author(openalex.get(oid)))
        bin_pipeline = sum(1 for oid in bin_ids
                          if not has_corresponding_author(openalex.get(oid))
                          and has_corresponding_author(gold.get(oid))
                          and has_corresponding_author(parseland.get(oid)))
        bin_parseland = sum(1 for oid in bin_ids
                           if not has_corresponding_author(openalex.get(oid))
                           and has_corresponding_author(gold.get(oid))
                           and not has_corresponding_author(parseland.get(oid)))
        bin_unavail = sum(1 for oid in bin_ids
                         if not has_corresponding_author(openalex.get(oid))
                         and not has_corresponding_author(gold.get(oid)))

        bin_oax_pct = 100 * bin_oax_has / bin_total
        bin_pipeline_pct = 100 * bin_pipeline / bin_total
        bin_parseland_pct = 100 * bin_parseland / bin_total
        bin_unavail_pct = 100 * bin_unavail / bin_total

        print(f"{label:9} | {bin_total:5} | {bin_oax_pct:6.1f}% | {bin_pipeline_pct:8.1f}% | {bin_parseland_pct:9.1f}% | {bin_unavail_pct:6.1f}%")

    # Return results for use in findings
    return {
        'gold_count': len(gold_ids),
        'gold_with_data': gold_with_data,
        'parseland_with_data': parseland_with_data,
        'openalex_with_data': openalex_with_data,
        'gold_has_affil': gold_has_affil,
        'gold_has_corr': gold_has_corr,
        'parseland_affil': cm_parseland_affil,
        'openalex_affil': cm_openalex_affil,
        'parseland_corr': cm_parseland_corr,
        'openalex_corr': cm_openalex_corr,
        'fn_parseland_affil': fn_parseland_affil,
        'fn_parseland_corr': fn_parseland_corr,
        'fn_openalex_affil': fn_openalex_affil,
        'fn_openalex_corr': fn_openalex_corr,
    }


if __name__ == '__main__':
    main()
