# Context for AI Agents

This document provides essential context for AI agents working on OpenAlex QA tasks.

## Critical Understanding: Data Flow

OpenAlex gets author/affiliation data from **multiple sources**:

```
                    ┌─────────────┐
                    │  Crossref   │──→ Raw metadata (often incomplete)
                    └─────────────┘
                           │
                    ┌─────────────┐
                    │   PubMed    │──→ Author names, some affiliations
                    └─────────────┘
                           │
┌─────────────┐     ┌─────────────┐
│   Taxicab   │────→│  Parseland  │──→ HTML extraction (richest source)
│ (scraper)   │     │  (parser)   │
└─────────────┘     └─────────────┘
                           │
                    ┌─────────────┐
                    │    PDF      │──→ PDF text extraction
                    │  Parsing    │
                    └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │  OpenAlex   │──→ Final merged result
                    │    API      │
                    └─────────────┘
```

### Why This Matters

A "Parseland-only" fix might NOT show improvement in the final OpenAlex API if:
1. OpenAlex already had the data from Crossref or PubMed
2. The data merge prioritizes a different source
3. There's a pipeline issue preventing data flow

**Always compare BOTH:**
- Direct Parseland output (what the parser extracts)
- OpenAlex API output (what users actually see)

## Key Services

| Service | Purpose | Endpoint |
|---------|---------|----------|
| **Taxicab** | HTML scraping & storage | `http://harvester-load-balancer-*.elb.amazonaws.com/taxicab` |
| **Parseland** | HTML → structured data | `http://parseland-load-balancer-*.elb.amazonaws.com/parseland` |
| **OpenAlex API** | Public data API | `https://api.openalex.org/` |

## Databricks Tables

| Table | Purpose |
|-------|---------|
| `openalex.taxicab.taxicab_results` | Scraped HTML metadata |
| `openalex.landing_page.taxicab_enriched_new` | Parser results (cached) |
| `openalex.works.locations_parsed` | Unified location data |
| `openalex.works.openalex_works` | Final works table |

## Common Failure Patterns

### 1. HTML Not Available (404)

```
Symptom: Parseland returns no data
Cause: HTML was purged from Taxicab storage
Fix: Request fresh scrape, not parser changes
```

### 2. Stale Cache

```
Symptom: Parseland works directly but not in pipeline
Cause: DLT streaming tables don't re-process existing records
Fix: Batch UPDATE or delete/reprocess
```

### 3. PDF URL Selection

```
Symptom: 0 authors extracted
Cause: PDF pages don't have structured author data
Fix: Ensure DOI URL is used, not /pdf URL
```

### 4. Publisher-Specific HTML Structure

```
Symptom: Parser fails for specific publisher
Cause: New HTML structure not supported
Fix: Add publisher-specific parser in parseland-lib
```

## Evaluation Datasets

### 10k-random

- 10,000 randomly sampled OpenAlex work IDs
- Good for measuring overall coverage
- Located at: `qa/exploration/datasets/10k-random/`

### NEES (Known Problems)

- ~316 DOIs known to have affiliation issues
- Intentionally hard cases for targeted testing
- Located at: `qa/exploration/datasets/nees/`

## Testing Tips

1. **Always sample first** - Before processing thousands of records, test on 10-50
2. **Check multiple sources** - Compare Parseland, OpenAlex, and the actual webpage
3. **Look for patterns** - Group failures by publisher, year, or DOI prefix
4. **Document everything** - Future agents (and humans) will thank you

## Related Repositories

| Repo | Purpose |
|------|---------|
| `openalex-walden` | Databricks pipelines (you are here) |
| `parseland-lib` | Parser implementations |
| `openalex-overview` | System documentation |

## Questions?

If you're stuck, document what you've tried in the issue's `INVESTIGATION.md` file. This helps the next agent (or human) continue where you left off.
