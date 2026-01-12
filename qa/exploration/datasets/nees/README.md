# Nees Dataset

Sample DOIs from Nees Jan van Eck (CWTS Leiden) identifying works where OpenAlex is missing affiliation data that exists in Scopus/WoS.

## Background

In December 2025, Nees Jan van Eck from CWTS Leiden contacted OpenAlex about a significant drop in metadata completeness for 2024 publications. His analysis showed:

- **Affiliations for Scopus/WoS works dropped from ~94% (2022) to ~85% (2024)**
- **Elsevier** is a major contributor to the affiliation drop
- **IEEE** is a major contributor to the references drop

See `email_thread_2025-12.md` for the full email exchange.

## Purpose

This dataset contains ~316 DOIs that are known to have problems with affiliation extraction. It's useful for:
- Targeted parser testing
- Measuring improvement on hard cases
- Regression testing after parser changes
- Validating fixes for the landing page regression

## Files

| File | Description |
|------|-------------|
| `dois.txt` | List of problematic DOIs (one per line) |
| `email_thread_2025-12.md` | Full email thread with Nees Jan van Eck |

## Origin

Nees provided a spreadsheet with several tabs of sample DOIs:
- Random 2024 works without affiliations (in Scopus/WoS)
- Random 2024 works from sources with largest affiliation drops (many Elsevier)
- Random 2024 works without references (in Scopus/WoS)
- Sources with largest drops in affiliations and references

The `dois.txt` file contains DOIs extracted from these samples, focusing on affiliation issues.

## Related Issue

**Issue:** `qa/issues/open/landing-page-regression-2026-01/`

The landing page parser regression (Dec 27, 2025 - Jan 3, 2026) is one cause of missing affiliations. The fix involves:
1. Running `RefreshStaleParserResponses.py` notebook to reload Parseland responses
2. Prioritizing Elsevier articles (`10.1016/%`) as they represent 55% of failures

## Key Findings

From our analysis of the NEES DOIs:

| Metric | Value |
|--------|-------|
| Total DOIs | 316 |
| Have affiliations in OpenAlex | 18 (5.7%) |
| Elsevier DOIs | 174 (55%) |

### By Publisher

| Publisher | DOIs | Coverage |
|-----------|------|----------|
| Elsevier BV | 174 | 1.7% |
| IOP Publishing | 19 | 5.3% |
| Emerald | 9 | 22.2% |
| Wiley | 8 | 0.0% |

## Nees's Hypothesis

From the email thread, Nees suggested the drop might be due to:
> "Could it be that scraping landing pages has become harder lately because many publishers and platforms are nowadays using services like Cloudflare?"

This aligns with our investigation findings about landing page parser issues.

## Usage

```bash
# Test Parseland on these DOIs
python qa/exploration/scripts/verify_nees_elsevier.py

# Use as input to other scripts
cat qa/exploration/datasets/nees/dois.txt | head -10
```

## Notes

- Many of these DOIs may not have HTML available (404 from Taxicab)
- Some publishers in this list have structural HTML issues requiring parser updates
- This is an intentionally "hard" dataset - low success rates are expected
- Nees cannot share the full list due to Scopus/WoS license restrictions
