# Affiliation/CA Pipeline Cascade Analysis

**Generated**: 2026-01-04 10:02

---

## Executive Summary

**10k Random - Affiliations**:
- Gold standard: 3,534 DOIs
- OpenAlex capture rate: 89.9%
- Parser improvements (NEW): +23 DOIs

**10k Random - Corresponding Authors**:
- Gold standard: 3,270 DOIs
- OpenAlex capture rate: 69.0%
- Parser improvements (NEW): +153 DOIs

**Nees - Affiliations**:
- Gold standard: 316 DOIs
- OpenAlex capture rate: 0.0%
- Parser improvements (NEW): +0 DOIs

---

## 1. 10k Random - Affiliations

### Cascade Table

| Level | Category | Count | % of Gold |
|-------|----------|------:|----------:|
| 1 | Gold standard has feature | 3,534 | 100.0% |
| 2 |    OpenAlex has | 3,178 | 89.9% |
| 2 |    OpenAlex missing | 356 | 10.1% |
| 3 |       Has Taxicab HTML | 356 | 10.1% |
| 3 |       No Taxicab HTML | 0 | 0.0% |
| 4 |          Parseland extracts | 158 | 4.5% |
| 4 |          Parseland fails | 198 | 5.6% |
| 5 |             OLD (baseline) | 135 | 3.8% |
| 5 |             NEW (improved) | 23 | 0.7% |

### Recent Years (2024-2025 Combined)

*Recent publications have lower OpenAlex coverage - this is where improvements matter most.*

| Level | Category | Count | % of Gold |
|-------|----------|------:|----------:|
| 1 | Gold standard has feature | 318 | 100.0% |
| 2 |    OpenAlex has | 209 | 65.7% |
| 2 |    OpenAlex missing | 109 | 34.3% |
| 3 |       Has Taxicab HTML | 109 | 34.3% |
| 3 |       No Taxicab HTML | 0 | 0.0% |
| 4 |          Parseland extracts | 84 | 26.4% |
| 4 |          Parseland fails | 25 | 7.9% |
| 5 |             OLD (baseline) | 62 | 19.5% |
| 5 |             NEW (improved) | 22 | 6.9% |


---

## 2. 10k Random - Corresponding Authors

### Cascade Table

| Level | Category | Count | % of Gold |
|-------|----------|------:|----------:|
| 1 | Gold standard has feature | 3,270 | 100.0% |
| 2 |    OpenAlex has | 2,256 | 69.0% |
| 2 |    OpenAlex missing | 1,014 | 31.0% |
| 3 |       Has Taxicab HTML | 1,014 | 31.0% |
| 3 |       No Taxicab HTML | 0 | 0.0% |
| 4 |          Parseland extracts | 428 | 13.1% |
| 4 |          Parseland fails | 586 | 17.9% |
| 5 |             OLD (baseline) | 275 | 8.4% |
| 5 |             NEW (improved) | 153 | 4.7% |

---

## 3. Nees - Affiliations

### Cascade Table

| Level | Category | Count | % of Gold |
|-------|----------|------:|----------:|
| 1 | Gold standard has feature | 316 | 100.0% |
| 2 |    OpenAlex has | 0 | 0.0% |
| 2 |    OpenAlex missing | 316 | 100.0% |
| 3 |       Has Taxicab HTML | 292 | 92.4% |
| 3 |       No Taxicab HTML | 24 | 7.6% |
| 4 |          Parseland extracts | 167 | 52.8% |
| 4 |          Parseland fails | 125 | 39.6% |
| 5 |             OLD (baseline) | 167 | 52.8% |
| 5 |             NEW (improved) | 0 | 0.0% |

---

## Key Insights

### 10k Random - Affiliations

- **Captured by OpenAlex**: 3,178 (89.9%)
- **Gap (missing in OA)**: 356 (10.1%)
  - Lost to no Taxicab HTML: 0
  - Lost to Parseland failure: 198
  - **Parseland can fill**: 158
    - Already working (OLD): 135
    - New improvements (NEW): 23

### 10k Random - Corresponding Authors

- **Captured by OpenAlex**: 2,256 (69.0%)
- **Gap (missing in OA)**: 1,014 (31.0%)
  - Lost to no Taxicab HTML: 0
  - Lost to Parseland failure: 586
  - **Parseland can fill**: 428
    - Already working (OLD): 275
    - New improvements (NEW): 153

### Nees - Affiliations

- **Captured by OpenAlex**: 0 (0.0%)
- **Gap (missing in OA)**: 316 (100.0%)
  - Lost to no Taxicab HTML: 24
  - Lost to Parseland failure: 125
  - **Parseland can fill**: 167
    - Already working (OLD): 167
    - New improvements (NEW): 0
