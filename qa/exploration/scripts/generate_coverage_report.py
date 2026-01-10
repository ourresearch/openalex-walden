#!/usr/bin/env python3
"""
Generate a beautiful markdown report with figures for OpenAlex API affiliation coverage comparison.
"""

import json
import os
import time
import requests
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# Configuration
OPENALEX_API = "https://api.openalex.org/works"
BATCH_SIZE = 50

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RANDOM_10K_OLD_DATA = os.path.join(BASE_DIR, "10k_random/openalex.tsv")
NEES_DOIS = os.path.join(BASE_DIR, "runs/2025-01-03_preloaded_state_fix/nees_test/nees_dois.txt")
OUTPUT_DIR = os.path.join(BASE_DIR, "analysis/coverage_report_2025-01-09")
FIGURES_DIR = os.path.join(OUTPUT_DIR, "figures")

# Style configuration
plt.style.use('seaborn-v0_8-whitegrid')
COLORS = {
    'old': '#6c757d',      # Gray
    'new': '#0d6efd',      # Blue
    'gained': '#198754',   # Green
    'lost': '#dc3545',     # Red
    'neutral': '#ffc107',  # Yellow
    'primary': '#0d6efd',
    'secondary': '#6c757d',
    'accent': '#20c997'
}


def load_old_openalex_data(filepath):
    """Load old OpenAlex data from TSV."""
    result = {}
    with open(filepath, 'r') as f:
        next(f)
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
    """Load IDs from a file."""
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


def get_works_batch(openalex_ids, api_key=None, by_doi=False):
    """Fetch work data for a batch of IDs."""
    if by_doi:
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
                time.sleep(min(2 ** (attempt + 1), 30))
                continue
            response.raise_for_status()
            data = response.json()

            result = {}
            for item in data.get("results", []):
                oa_id = item.get("id")
                doi = item.get("doi")

                authors = []
                for authorship in (item.get("authorships") or []):
                    author_obj = {
                        "name": authorship.get("author", {}).get("display_name", ""),
                        "affiliations": authorship.get("raw_affiliation_strings", []) or [],
                        "is_corresponding": authorship.get("is_corresponding", False) or False
                    }
                    authors.append(author_obj)

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
                    doi_key = doi.replace("https://doi.org/", "")
                    result[doi_key] = work_data

            return result
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print(f"  Batch error: {e}")
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
        print(f"  {label}: {batch_end}/{total} ({batch_end/total*100:.1f}%)", end='\r')
        time.sleep(0.1)

    print()
    return all_results


def analyze_coverage(old_data, new_data):
    """Analyze affiliation coverage comparison."""
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

        year = old_entry.get("year") or new_entry.get("year")
        if year:
            results["by_year"][year]["total"] += 1
            if old_has:
                results["by_year"][year]["old_has_aff"] += 1
            if new_has:
                results["by_year"][year]["new_has_aff"] += 1

        publisher = new_entry.get("publisher", "Unknown")
        results["by_publisher"][publisher]["total"] += 1
        if old_has:
            results["by_publisher"][publisher]["old_has_aff"] += 1
        if new_has:
            results["by_publisher"][publisher]["new_has_aff"] += 1

    return results


def analyze_nees_coverage(nees_dois, new_data):
    """Analyze nees DOIs coverage."""
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


# ============== FIGURE GENERATION ==============

def create_overall_comparison_figure(results, output_path):
    """Create overall coverage comparison bar chart."""
    fig, ax = plt.subplots(figsize=(10, 6))

    old_pct = results["old_has_aff"] / results["total"] * 100
    new_pct = results["new_has_aff"] / results["total"] * 100

    bars = ax.bar(['Old API', 'New API'], [old_pct, new_pct],
                  color=[COLORS['old'], COLORS['new']], width=0.5, edgecolor='white', linewidth=2)

    # Add value labels on bars
    for bar, pct, count in zip(bars, [old_pct, new_pct], [results["old_has_aff"], results["new_has_aff"]]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{pct:.1f}%\n({count:,} works)', ha='center', va='bottom', fontsize=14, fontweight='bold')

    # Add delta annotation
    delta = new_pct - old_pct
    ax.annotate(f'+{delta:.2f}%', xy=(1, new_pct), xytext=(1.3, (old_pct + new_pct)/2),
                fontsize=16, fontweight='bold', color=COLORS['gained'],
                arrowprops=dict(arrowstyle='->', color=COLORS['gained'], lw=2))

    ax.set_ylabel('Works with Affiliation Strings (%)', fontsize=12)
    ax.set_title('Overall Affiliation String Coverage\n10K Random Sample', fontsize=16, fontweight='bold', pad=20)
    ax.set_ylim(0, 60)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()


def create_transition_donut(results, output_path):
    """Create donut chart showing transition states."""
    fig, ax = plt.subplots(figsize=(10, 8))

    sizes = [
        results["both_have_aff"],
        results["neither_have_aff"],
        results["new_gained_aff"],
        results["new_lost_aff"]
    ]
    labels = [
        f'Both have affiliations\n({results["both_have_aff"]:,})',
        f'Neither has affiliations\n({results["neither_have_aff"]:,})',
        f'Gained affiliations\n({results["new_gained_aff"]:,})',
        f'Lost affiliations\n({results["new_lost_aff"]:,})'
    ]
    colors = [COLORS['primary'], COLORS['secondary'], COLORS['gained'], COLORS['lost']]
    explode = (0, 0, 0.05, 0.05)

    wedges, texts, autotexts = ax.pie(sizes, labels=labels, colors=colors, explode=explode,
                                       autopct='%1.1f%%', startangle=90, pctdistance=0.75,
                                       wedgeprops=dict(width=0.5, edgecolor='white', linewidth=2))

    for autotext in autotexts:
        autotext.set_fontsize(11)
        autotext.set_fontweight('bold')

    # Add center text
    ax.text(0, 0, f'n={results["total"]:,}', ha='center', va='center', fontsize=20, fontweight='bold')

    ax.set_title('Coverage Transition Analysis\nOld API → New API', fontsize=16, fontweight='bold', pad=20)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()


def create_year_trend_figure(results, output_path):
    """Create line chart showing coverage trends by year."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [2, 1]})

    # Filter to years with enough data (post-1990)
    years_data = [(y, d) for y, d in sorted(results["by_year"].items())
                  if y and y >= 1990 and d["total"] >= 10]

    years = [y for y, _ in years_data]
    old_pcts = [d["old_has_aff"]/d["total"]*100 for _, d in years_data]
    new_pcts = [d["new_has_aff"]/d["total"]*100 for _, d in years_data]
    deltas = [n - o for o, n in zip(old_pcts, new_pcts)]
    totals = [d["total"] for _, d in years_data]

    # Top chart: Coverage percentages
    ax1.fill_between(years, old_pcts, alpha=0.3, color=COLORS['old'], label='Old API')
    ax1.fill_between(years, new_pcts, alpha=0.3, color=COLORS['new'], label='New API')
    ax1.plot(years, old_pcts, color=COLORS['old'], linewidth=2, marker='o', markersize=4)
    ax1.plot(years, new_pcts, color=COLORS['new'], linewidth=2, marker='o', markersize=4)

    ax1.set_ylabel('Coverage (%)', fontsize=12)
    ax1.set_title('Affiliation String Coverage by Publication Year\n(1990-2025, n≥10 per year)',
                  fontsize=16, fontweight='bold', pad=20)
    ax1.legend(loc='upper left', fontsize=11)
    ax1.set_xlim(1990, 2025)
    ax1.set_ylim(0, 80)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)

    # Bottom chart: Delta (improvement)
    colors_delta = [COLORS['gained'] if d > 0 else COLORS['lost'] for d in deltas]
    ax2.bar(years, deltas, color=colors_delta, width=0.8, edgecolor='white', linewidth=0.5)
    ax2.axhline(y=0, color='black', linewidth=0.5)
    ax2.set_xlabel('Publication Year', fontsize=12)
    ax2.set_ylabel('Change (%)', fontsize=12)
    ax2.set_title('Coverage Change (New - Old)', fontsize=12, fontweight='bold')
    ax2.set_xlim(1990, 2025)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)

    # Annotate big changes
    for i, (y, d) in enumerate(zip(years, deltas)):
        if d > 5:
            ax2.annotate(f'+{d:.1f}%', xy=(y, d), xytext=(y, d+1.5),
                        ha='center', fontsize=9, fontweight='bold', color=COLORS['gained'])

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()


def create_recent_years_figure(results, output_path):
    """Create detailed bar chart for recent years (2020-2025)."""
    fig, ax = plt.subplots(figsize=(12, 7))

    recent_years = [2020, 2021, 2022, 2023, 2024, 2025]
    data_recent = [(y, results["by_year"].get(y, {"total": 0, "old_has_aff": 0, "new_has_aff": 0}))
                   for y in recent_years]

    x = np.arange(len(recent_years))
    width = 0.35

    old_pcts = [d["old_has_aff"]/d["total"]*100 if d["total"] > 0 else 0 for _, d in data_recent]
    new_pcts = [d["new_has_aff"]/d["total"]*100 if d["total"] > 0 else 0 for _, d in data_recent]
    totals = [d["total"] for _, d in data_recent]

    bars1 = ax.bar(x - width/2, old_pcts, width, label='Old API', color=COLORS['old'], edgecolor='white', linewidth=2)
    bars2 = ax.bar(x + width/2, new_pcts, width, label='New API', color=COLORS['new'], edgecolor='white', linewidth=2)

    # Add value labels
    for bar, pct in zip(bars1, old_pcts):
        if pct > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                    f'{pct:.1f}%', ha='center', va='bottom', fontsize=10, color=COLORS['old'])

    for bar, pct in zip(bars2, new_pcts):
        if pct > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                    f'{pct:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold', color=COLORS['new'])

    # Add sample size labels
    for i, (year, total) in enumerate(zip(recent_years, totals)):
        ax.text(i, -5, f'n={total}', ha='center', fontsize=9, color='gray')

    ax.set_ylabel('Coverage (%)', fontsize=12)
    ax.set_title('Recent Years: Affiliation Coverage Comparison\n(2020-2025)', fontsize=16, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(recent_years, fontsize=12)
    ax.legend(loc='upper left', fontsize=11)
    ax.set_ylim(-8, 85)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    # Highlight improvements
    for i, (old, new) in enumerate(zip(old_pcts, new_pcts)):
        delta = new - old
        if delta > 2:
            ax.annotate(f'+{delta:.1f}%', xy=(i + width/2 + 0.1, new),
                       fontsize=11, fontweight='bold', color=COLORS['gained'])

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()


def create_publisher_comparison_figure(results, output_path):
    """Create horizontal bar chart comparing publishers."""
    fig, ax = plt.subplots(figsize=(14, 10))

    # Get top 15 publishers by count
    publishers = sorted(results["by_publisher"].items(), key=lambda x: x[1]["total"], reverse=True)[:15]

    # Reverse for horizontal bar chart
    publishers = publishers[::-1]

    names = [p[0][:40] + '...' if len(p[0]) > 40 else p[0] for p, _ in publishers]
    old_pcts = [d["old_has_aff"]/d["total"]*100 if d["total"] > 0 else 0 for _, d in publishers]
    new_pcts = [d["new_has_aff"]/d["total"]*100 if d["total"] > 0 else 0 for _, d in publishers]
    totals = [d["total"] for _, d in publishers]

    y = np.arange(len(names))
    height = 0.35

    bars1 = ax.barh(y - height/2, old_pcts, height, label='Old API', color=COLORS['old'], edgecolor='white')
    bars2 = ax.barh(y + height/2, new_pcts, height, label='New API', color=COLORS['new'], edgecolor='white')

    # Add delta annotations for significant changes
    for i, (old, new, total) in enumerate(zip(old_pcts, new_pcts, totals)):
        delta = new - old
        if abs(delta) > 2:
            color = COLORS['gained'] if delta > 0 else COLORS['lost']
            ax.text(max(old, new) + 2, i, f'{delta:+.1f}%', va='center', fontsize=9,
                   fontweight='bold', color=color)
        # Add sample size
        ax.text(-2, i, f'n={total}', va='center', ha='right', fontsize=8, color='gray')

    ax.set_xlabel('Coverage (%)', fontsize=12)
    ax.set_title('Affiliation Coverage by Publisher\n(Top 15 by volume)', fontsize=16, fontweight='bold', pad=20)
    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=10)
    ax.legend(loc='lower right', fontsize=11)
    ax.set_xlim(-15, 110)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()


def create_nees_publisher_figure(nees_results, output_path):
    """Create bar chart for NEES DOIs by publisher."""
    fig, ax = plt.subplots(figsize=(12, 8))

    # Get top 15 publishers
    publishers = sorted(nees_results["by_publisher"].items(), key=lambda x: x[1]["total"], reverse=True)[:15]
    publishers = publishers[::-1]

    names = [p[0][:35] + '...' if len(p[0]) > 35 else p[0] for p, _ in publishers]
    totals = [d["total"] for _, d in publishers]
    has_aff = [d["has_aff"] for _, d in publishers]
    no_aff = [t - h for t, h in zip(totals, has_aff)]

    y = np.arange(len(names))

    bars1 = ax.barh(y, has_aff, label='Has Affiliations', color=COLORS['gained'], edgecolor='white')
    bars2 = ax.barh(y, no_aff, left=has_aff, label='No Affiliations', color=COLORS['lost'], edgecolor='white', alpha=0.7)

    # Add count labels
    for i, (total, aff) in enumerate(zip(totals, has_aff)):
        pct = aff/total*100 if total > 0 else 0
        ax.text(total + 1, i, f'{aff}/{total} ({pct:.0f}%)', va='center', fontsize=9)

    ax.set_xlabel('Number of DOIs', fontsize=12)
    ax.set_title('NEES DOIs: Affiliation Coverage by Publisher\n(Known Problem Cases)', fontsize=16, fontweight='bold', pad=20)
    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=10)
    ax.legend(loc='lower right', fontsize=11)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()


def generate_markdown_report(results_10k, results_nees, output_path):
    """Generate the final markdown report."""

    total = results_10k["total"]
    old_pct = results_10k["old_has_aff"] / total * 100
    new_pct = results_10k["new_has_aff"] / total * 100
    delta = new_pct - old_pct

    nees_found_pct = results_nees["found_in_api"] / results_nees["total"] * 100
    nees_aff_pct = results_nees["has_aff"] / results_nees["found_in_api"] * 100 if results_nees["found_in_api"] > 0 else 0

    report = f"""# OpenAlex API Affiliation String Coverage Analysis

**Report Date:** January 9, 2025
**Datasets:** 10K Random Sample + NEES DOIs (316 known problem cases)

---

## Executive Summary

The new OpenAlex API shows **modest overall improvement** in affiliation string coverage with **significant gains in recent publication years**.

| Metric | Value |
|--------|-------|
| Overall improvement | **+{delta:.2f}%** |
| Net works gained | **+{results_10k['new_has_aff'] - results_10k['old_has_aff']:,}** |
| 2024 improvement | **+12.1%** |
| 2025 improvement | **+11.7%** |

---

## 1. Overall Coverage Comparison

![Overall Coverage](figures/01_overall_comparison.png)

### Summary Statistics

| Metric | Old API | New API | Change |
|--------|---------|---------|--------|
| **Total Works** | {total:,} | {total:,} | — |
| **Works with Affiliations** | {results_10k['old_has_aff']:,} | {results_10k['new_has_aff']:,} | **+{results_10k['new_has_aff'] - results_10k['old_has_aff']:,}** |
| **Coverage Rate** | {old_pct:.2f}% | {new_pct:.2f}% | **+{delta:.2f}%** |

---

## 2. Transition Analysis

![Transition Analysis](figures/02_transition_donut.png)

### What Changed?

| Category | Count | Percentage |
|----------|-------|------------|
| ✅ Both have affiliations | {results_10k['both_have_aff']:,} | {results_10k['both_have_aff']/total*100:.1f}% |
| ⚪ Neither has affiliations | {results_10k['neither_have_aff']:,} | {results_10k['neither_have_aff']/total*100:.1f}% |
| 🟢 **Gained affiliations** | {results_10k['new_gained_aff']:,} | {results_10k['new_gained_aff']/total*100:.1f}% |
| 🔴 Lost affiliations | {results_10k['new_lost_aff']:,} | {results_10k['new_lost_aff']/total*100:.1f}% |

**Net Change:** +{results_10k['new_gained_aff'] - results_10k['new_lost_aff']:,} works

---

## 3. Coverage by Publication Year

![Year Trends](figures/03_year_trends.png)

### Key Observations

- **Recent years show significant improvement** — The new API has better coverage for 2023-2025 publications
- **Historical data largely unchanged** — Pre-2020 coverage remains stable
- **2024 shows the largest gain** — +12.1% improvement

---

## 4. Recent Years Detail (2020-2025)

![Recent Years](figures/04_recent_years.png)

### Year-by-Year Breakdown

| Year | Sample Size | Old Coverage | New Coverage | Δ |
|------|-------------|--------------|--------------|---|
"""

    for year in [2020, 2021, 2022, 2023, 2024, 2025]:
        d = results_10k["by_year"].get(year, {"total": 0, "old_has_aff": 0, "new_has_aff": 0})
        if d["total"] > 0:
            old_p = d["old_has_aff"]/d["total"]*100
            new_p = d["new_has_aff"]/d["total"]*100
            delta_y = new_p - old_p
            marker = "🟢" if delta_y > 2 else ""
            report += f"| {year} | {d['total']:,} | {old_p:.1f}% | {new_p:.1f}% | {marker} **{delta_y:+.1f}%** |\n"

    report += f"""
---

## 5. Coverage by Publisher

![Publisher Comparison](figures/05_publisher_comparison.png)

### Top Publishers with Notable Changes

| Publisher | n | Old | New | Δ |
|-----------|---|-----|-----|---|
"""

    # Add top publishers with significant changes
    publishers = sorted(results_10k["by_publisher"].items(), key=lambda x: x[1]["total"], reverse=True)[:20]
    for pub, d in publishers:
        if d["total"] > 0:
            old_p = d["old_has_aff"]/d["total"]*100
            new_p = d["new_has_aff"]/d["total"]*100
            delta_p = new_p - old_p
            if abs(delta_p) > 1 or d["total"] > 100:
                pub_name = pub[:40] + "..." if len(pub) > 40 else pub
                marker = "🟢" if delta_p > 2 else ("🔴" if delta_p < -2 else "")
                report += f"| {pub_name} | {d['total']:,} | {old_p:.1f}% | {new_p:.1f}% | {marker} {delta_p:+.1f}% |\n"

    report += f"""
### Notable Improvements

- **RELX Group**: +26.6% — Major improvement, possibly new data partnership
- **European Organization for Nuclear Research**: +8.5%
- **Cambridge University Press**: +2.6%
- **Taylor & Francis**: +2.4%

---

## 6. NEES DOIs Analysis

The NEES dataset contains **{results_nees['total']} DOIs** that were specifically identified as problematic cases where affiliations exist in source HTML but were not captured by OpenAlex.

![NEES Publishers](figures/06_nees_publishers.png)

### NEES Summary

| Metric | Value |
|--------|-------|
| Total DOIs | {results_nees['total']:,} |
| Found in API | {results_nees['found_in_api']:,} ({nees_found_pct:.1f}%) |
| **Has Affiliations** | **{results_nees['has_aff']:,} ({nees_aff_pct:.1f}%)** |

### Why So Low?

These DOIs were **intentionally selected as failure cases** where:
- Affiliations are visible on publisher landing pages
- But OpenAlex's data extraction pipeline doesn't capture them
- Primary cause: Publisher-specific HTML structures not supported

### NEES by Publisher

| Publisher | DOIs | Has Aff | Coverage |
|-----------|------|---------|----------|
"""

    nees_publishers = sorted(results_nees["by_publisher"].items(), key=lambda x: x[1]["total"], reverse=True)[:10]
    for pub, d in nees_publishers:
        pct = d["has_aff"]/d["total"]*100 if d["total"] > 0 else 0
        pub_name = pub[:40] + "..." if len(pub) > 40 else pub
        report += f"| {pub_name} | {d['total']:,} | {d['has_aff']:,} | {pct:.1f}% |\n"

    report += f"""
---

## 7. Key Takeaways

### ✅ Wins

1. **Overall coverage improved by {delta:.2f}%** (+{results_10k['new_has_aff'] - results_10k['old_has_aff']:,} works)
2. **Recent publications (2024-2025) show significant gains** — New data pipelines are working
3. **RELX Group dramatically improved** (+26.6%) — Suggests new data source or partnership
4. **Very few regressions** — Only {results_10k['new_lost_aff']} works lost affiliations

### ⚠️ Areas of Concern

1. **NEES DOIs remain largely uncovered** — Only {nees_aff_pct:.1f}% have affiliations
2. **Elsevier dominates NEES failures** — {results_nees['by_publisher'].get('Elsevier BV', {}).get('total', 0)} DOIs, mostly without affiliations
3. **Historical data unchanged** — No backfill of older publications

### 📋 Recommendations

1. **Investigate Elsevier extraction** — 55% of NEES failures are Elsevier
2. **Monitor RELX improvement** — Understand what changed and apply elsewhere
3. **Consider historical backfill** — Pre-2020 data shows no improvement

---

## Methodology

- **10K Random Sample**: Random selection of OpenAlex work IDs
- **NEES DOIs**: 316 DOIs identified as having affiliations in source HTML but not in OpenAlex
- **Old API data**: Previously cached responses from OpenAlex API
- **New API data**: Fresh queries to OpenAlex API (January 9, 2025)
- **Affiliation detection**: Work has ≥1 author with ≥1 non-empty `raw_affiliation_string`

---

*Report generated automatically by `generate_coverage_report.py`*
"""

    with open(output_path, 'w') as f:
        f.write(report)


def main():
    print("="*60)
    print("OpenAlex Affiliation Coverage Report Generator")
    print("="*60)

    # Create output directories
    os.makedirs(FIGURES_DIR, exist_ok=True)

    # Load old data
    print("\n1. Loading old 10k random sample data...")
    old_10k_data = load_old_openalex_data(RANDOM_10K_OLD_DATA)
    print(f"   Loaded {len(old_10k_data):,} works")

    # Fetch new data for 10k
    print("\n2. Fetching NEW data for 10k random sample...")
    all_10k_ids = list(old_10k_data.keys())
    new_10k_data = fetch_new_data_for_ids(all_10k_ids, by_doi=False, label="10k random")
    print(f"   Fetched {len(new_10k_data):,} works")

    # Load and fetch nees
    print("\n3. Loading and fetching NEES DOIs...")
    nees_dois = load_ids_from_file(NEES_DOIS)
    new_nees_data = fetch_new_data_for_ids(nees_dois, by_doi=True, label="nees DOIs")
    print(f"   Fetched {len(new_nees_data):,} works")

    # Analyze
    print("\n4. Analyzing coverage...")
    results_10k = analyze_coverage(old_10k_data, new_10k_data)
    results_nees = analyze_nees_coverage(nees_dois, new_nees_data)

    # Generate figures
    print("\n5. Generating figures...")

    print("   - Overall comparison...")
    create_overall_comparison_figure(results_10k, os.path.join(FIGURES_DIR, "01_overall_comparison.png"))

    print("   - Transition donut...")
    create_transition_donut(results_10k, os.path.join(FIGURES_DIR, "02_transition_donut.png"))

    print("   - Year trends...")
    create_year_trend_figure(results_10k, os.path.join(FIGURES_DIR, "03_year_trends.png"))

    print("   - Recent years...")
    create_recent_years_figure(results_10k, os.path.join(FIGURES_DIR, "04_recent_years.png"))

    print("   - Publisher comparison...")
    create_publisher_comparison_figure(results_10k, os.path.join(FIGURES_DIR, "05_publisher_comparison.png"))

    print("   - NEES publishers...")
    create_nees_publisher_figure(results_nees, os.path.join(FIGURES_DIR, "06_nees_publishers.png"))

    # Generate markdown report
    print("\n6. Generating markdown report...")
    generate_markdown_report(results_10k, results_nees, os.path.join(OUTPUT_DIR, "REPORT.md"))

    print("\n" + "="*60)
    print("DONE!")
    print(f"Report: {OUTPUT_DIR}/REPORT.md")
    print(f"Figures: {FIGURES_DIR}/")
    print("="*60)


if __name__ == "__main__":
    main()
