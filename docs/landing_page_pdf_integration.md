# Landing Page & PDF Integration

How `landing_page_works` and `pdf_works` get merged into crossref/repo records via super authorships and super locations.

## How Records Are Joined

Landing page and PDF records are **NOT** joined to crossref/repo by `native_id = native_id`. Each source has its own `native_id` type:

| Source | native_id is a... |
|--------|------------------|
| crossref | DOI |
| repo / repo_backfill | OAI-PMH ID |
| landing_page | URL |
| pdf | URL / source PDF ID |

The join works by **extracting a DOI or PMH from the `ids` array** of landing_page/pdf records, then matching that to the crossref/repo `native_id`.

### In Super Authorships
All sources come from `locations_parsed`. A computed join key is created per row:
```sql
-- Crossref super authorships
CASE WHEN provenance = 'crossref' THEN native_id
     ELSE get(filter(ids, x -> x.namespace = "doi").id, 0)
END AS doi

-- Repo super authorships
CASE WHEN provenance IN ('repo', 'repo_backfill') THEN native_id
     ELSE get(filter(ids, x -> x.namespace = "pmh").id, 0)
END AS pmh_id
```

### In Super Locations
CTEs extract DOI/PMH from landing_page/pdf `ids` arrays into columns, then join to crossref/repo `native_id`:
```sql
-- e.g., crossref super locations
LEFT JOIN pdf_urls_for_crossref p ON c.native_id = p.doi
LEFT JOIN landing_page_urls_for_crossref l ON c.native_id = l.doi
```

## What These Tables Contain

### `landing_page_works` (from `notebooks/ingest/LandingPage.py`)
HTML scraping via Parseland. Key columns:
- `native_id` — URL or DOI identifier
- `authors` — array of structs: `{given, family, name, orcid, author_key}`
- `abstract` — plain text (capped at 65,535 chars)
- `license` — normalized CC license ("other-oa" → null)
- `is_corresponding` — array of booleans per author position
- `ids` — array of `{namespace, value}` (doi, pmh, etc.)
- `updated_date`, `provenance` ("landing_page")

Only rows where `error_had=False` AND (has authors OR abstract OR license) are kept.

### `pdf_works` (from `notebooks/ingest/PDF.py`)
GROBID TEI-XML parsing. Key columns:
- `native_id` — source PDF identifier
- `authors` — array of structs: `{given, family, name, orcid, author_key, affiliations}`
- `affiliations` — structured: raw string + ROR ID + ORCID
- `abstract` — plain text (capped at 10K chars)
- `fulltext` — full document text (capped at 200K chars)
- `references` — parsed reference list
- `language`, `funders`, `license`
- `version` — PMH: acceptedVersion/submittedVersion; DOI: publishedVersion (except arxiv/zenodo→accepted)
- `ids` — includes `docs.pdf` and `docs.parsed-pdf` namespaces
- `is_oa` — always True

## Super Authorships: How Authors Get Merged

See `CreateCrossrefSuperAuthorships.ipynb` (keyed by DOI) and `CreateRepoSuperAuthorships.ipynb` (keyed by pmh_id).

### Author names (who is the authoritative author list?)
- **Crossref**: crossref(1) / landing_page(2) — PDF is NOT used for names
- **Repo**: repo/repo_backfill(1) / landing_page(2) — PDF is NOT used for names

### Affiliations (merged from multiple sources)
- **Crossref**: crossref > pubmed > pdf (≤2 cap) > landing_page (≤5 cap)
- **Repo**: repo > pdf (≤2 cap) > landing_page (≤5 cap)

Matching strategies:
1. **Key-based** (primary): match by `author_key` (LastName;FirstInitial) between authoritative author list and affiliation source
2. **Positional** (fallback): if zero key matches AND author count matches between sources, use array position — this solves CJK/romanized name mismatches

### Affiliation noise guards
- PDF: ≤2 affiliations per author (GROBID can produce noisy affiliations)
- Landing page: ≤5 affiliations per author

### is_corresponding
- From **landing_page only** (key-based or positional match)
- PDF is_corresponding is not used

## Super Locations: How Fields Get Merged

See `CreateSuperLocations.ipynb`. Only crossref and repo records get landing page/PDF integration. DataCite, PubMed, and MAG do not.

### Crossref DOIs — Field Priority
| Field | Priority Order |
|-------|---------------|
| License | landing_page > pdf > crossref |
| Abstract | crossref > landing_page > pdf |
| References | crossref > pdf |
| Authors | super_authorships, fallback to crossref > landing_page > pdf |

### Repo PMH IDs — Field Priority
| Field | Priority Order |
|-------|---------------|
| License | landing_page > pdf > repo |
| Abstract | repo > landing_page > pdf |
| References | repo > pdf |
| Version | submittedVersion > acceptedVersion > publishedVersion |

### URL Selection
- **Landing page URL**: doi.org highest priority, then known publishers (elsevier, springer, wiley), then .edu, avoid PDF URLs
- **PDF URL**: by version (publishedVersion > acceptedVersion > submittedVersion for crossref; inverted for repo), then has grobid_id, then domain quality
- **is_oa**: True if ANY source says True
