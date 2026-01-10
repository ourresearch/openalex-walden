# QA Exploration

This directory contains tools and data for exploring OpenAlex data quality.

## Directory Structure

```
exploration/
├── datasets/           # Reusable evaluation datasets
│   ├── 10k-random/    # 10k random sample
│   └── nees/          # Known problem DOIs
├── scripts/           # Data collection & analysis tools
└── reports/           # One-off analysis reports
```

## Datasets

### 10k-random

10,000 randomly sampled OpenAlex work IDs for measuring overall coverage.

Files:
- `openalex.tsv` - OpenAlex API responses
- `parseland.tsv` - Parseland extraction results
- `gold-standard.tsv` - OpenAI extractions (ground truth)

### NEES

~316 DOIs known to have affiliation extraction issues. Useful for:
- Targeted parser testing
- Measuring improvement on hard cases

## Scripts

### Data Collection

| Script | Purpose |
|--------|---------|
| `get_openalex.py` | Fetch OpenAlex API data for work IDs |
| `get_parseland.py` | Fetch Parseland results via Taxicab |
| `get_openai.py` | Extract gold standard using OpenAI |

### Analysis

| Script | Purpose |
|--------|---------|
| `compare_coverage.py` | Compare old vs new API coverage |
| `gather_failures.py` | Consolidate failures from multiple sources |
| `analyze_by_publisher.py` | Group failures by publisher |

### Reports

| Script | Purpose |
|--------|---------|
| `generate_coverage_report.py` | Create visual coverage report |
| `verify_nees.py` | Deep-dive verification of NEES DOIs |

## Running Scripts

Most scripts support checkpointing for resumption:

```bash
# Run full collection
python qa/exploration/scripts/get_parseland.py

# Resume from where you left off (if interrupted)
python qa/exploration/scripts/get_parseland.py  # Automatically resumes

# Get help
python qa/exploration/scripts/get_parseland.py --help
```

## Creating Reports

Reports are stored in timestamped directories:

```bash
# Generate a new coverage report
python qa/exploration/scripts/generate_coverage_report.py

# Output goes to: reports/YYYY-MM-DD-coverage/
```

## Tips

1. **Sample first**: Test on 10-50 records before full runs
2. **Check rate limits**: APIs have rate limits, use delays
3. **Document findings**: If you discover an issue, create it in `../issues/open/`
