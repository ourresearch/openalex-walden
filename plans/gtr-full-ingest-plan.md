# GTR Full Awards Ingest Plan

## Overview

Ingest all ~171K projects from the Gateway to Research (GtR) API to get complete UK Research Council grant data, replacing the current approach that only captures grants cited by publications.

## Current State

| Source | Count | Issue |
|--------|-------|-------|
| `gtr_awards` table | 1.3M rows (64K unique grants) | Only grants cited by publications |
| `openalex_awards` (GTR provenance) | 36K | ~28K lost to backfill due to low priority |
| **GTR API** | **171K projects** | Full dataset we need |

**Root causes:**
1. Current ingest reads publications parquet, not projects API
2. GTR has lowest priority (3) vs backfill (2), losing duplicates

## GTR Data Model

**Research Councils (actual funders):**
- AHRC, BBSRC, EPSRC, ESRC, MRC, NERC, STFC, INNOVATE UK

**Special Programmes (funding schemes, not funders):**
- UKRI FLF (Future Leaders Fellowship)
- GCRF (Global Challenges Research Fund)
- Newton Fund
- Horizon Europe Guarantee
- And ~20 more...

**Grant Reference Format:** `"GRANT_ID:FUNDER_OR_PROGRAMME"`
- `"MR/X035506/1:UKRI FLF"` → MRC grant via UKRI FLF programme
- `"EP/Y036530/1:Horizon Europe Guarantee"` → EPSRC via Horizon Europe
- `"NE/Y005449/1:NERC"` → Standard NERC grant

**Mapping to OpenAlex:**
| GTR Field | OpenAlex Field | Logic |
|-----------|----------------|-------|
| Grant ID prefix | `funder` | EP/→EPSRC, MR/→MRC, ST/→STFC, BB/→BBSRC, NE/→NERC, ES/→ESRC, AH/→AHRC |
| Suffix after colon | `funder_scheme` | Programme name (UKRI FLF, GCRF, etc.) or NULL if just council |
| grant_category | `funding_type` | Research Grant→research, Fellowship→fellowship, etc. |

## Solution

### Part 1: Local Script - `gtr_to_s3.py`

**Location:** `openalex-walden/scripts/local/gtr_to_s3.py`

**Pattern:** Follow `nih_exporter_to_s3.py`

```
Step 1: Fetch all projects from GtR API
        - Endpoint: https://gtr.ukri.org/gtr/api/projects
        - Pagination: p=1..17132, s=100 (max page size)
        - Format: XML → parse to dict
        - Rate limit: Add 0.5s delay between requests

Step 2: For each project, fetch fund details
        - Endpoint: https://gtr.ukri.org/gtr/api/projects/{id}/funds
        - Get: amount, start/end dates, funder org link

Step 3: Combine into DataFrame with columns:
        - project_id (UUID)
        - grant_reference (RCUK field, e.g., "EP/Y004663/2")
        - title
        - abstract
        - status (Active/Closed)
        - grant_category (Research Grant, Fellowship, etc.)
        - lead_funder (EPSRC, MRC, etc.)
        - amount (GBP)
        - start_date, end_date
        - lead_org_name
        - pi_name (from PI_PER link)

Step 4: Save as parquet, upload to S3
        - Output: s3://openalex-ingest/awards/gtr/gtr_projects.parquet
```

**Estimated runtime:** ~4 hours (171K requests with rate limiting)

### Part 2: Databricks Notebook - `CreateGTRProjectAwards.ipynb`

**Location:** `openalex-walden/notebooks/awards/CreateGTRProjectAwards.ipynb`

**Pattern:** Follow `CreateNIHAwards.ipynb`

```sql
-- Step 1: Create staging table from S3
CREATE OR REPLACE TABLE openalex.awards.gtr_projects_raw
AS SELECT * FROM parquet.`s3a://openalex-ingest/awards/gtr/gtr_projects.parquet`;

-- Step 2: Create GTR awards table with full metadata
CREATE OR REPLACE TABLE openalex.awards.gtr_project_awards
AS
WITH
-- Map grant ID prefix to research council
prefix_to_council AS (
    SELECT * FROM (VALUES
        ('EP/', 'EPSRC'),
        ('MR/', 'MRC'),
        ('ST/', 'STFC'),
        ('BB/', 'BBSRC'),
        ('NE/', 'NERC'),
        ('ES/', 'ESRC'),
        ('AH/', 'AHRC')
    ) AS t(prefix, council_abbrev)
),
-- Get OpenAlex funder records for UK councils
funders AS (
    SELECT DISTINCT funder_id, alternate_title, display_name, ror_id, doi
    FROM openalex.common.funder
    LATERAL VIEW explode(from_json(alternate_titles, 'array<string>')) as alternate_title
    WHERE location = 'United Kingdom'
      AND alternate_title IN ('AHRC','BBSRC','EPSRC','ESRC','MRC','NERC','STFC','Innovate UK')
),
-- Parse grant reference to extract council and programme
parsed AS (
    SELECT
        g.*,
        -- Extract prefix (e.g., "EP/" from "EP/Y036530/1")
        CONCAT(SUBSTRING(g.grant_reference, 1, 2), '/') as grant_prefix,
        -- Extract programme from suffix after colon in leadFunder field
        -- If leadFunder is a council name, programme is NULL; otherwise it's the programme
        CASE
            WHEN g.lead_funder IN ('AHRC','BBSRC','EPSRC','ESRC','MRC','NERC','STFC','Innovate UK')
            THEN NULL
            ELSE g.lead_funder  -- This is actually a programme name
        END as programme
    FROM openalex.awards.gtr_projects_raw g
),
-- Join to get proper funder from prefix
with_funder AS (
    SELECT
        p.*,
        COALESCE(ptc.council_abbrev, p.lead_funder) as council_abbrev
    FROM parsed p
    LEFT JOIN prefix_to_council ptc ON p.grant_prefix = ptc.prefix
)
SELECT
    CONCAT(f.funder_id, ':', LOWER(g.grant_reference)) as id,
    g.title as display_name,
    g.abstract as description,
    f.funder_id,
    g.grant_reference as funder_award_id,
    g.amount,
    'GBP' as currency,
    struct(concat('https://openalex.org/F', f.funder_id), f.display_name, f.ror_id, f.doi) as funder,
    -- Map grant_category to funding_type
    CASE
        WHEN g.grant_category = 'Research Grant' THEN 'research'
        WHEN g.grant_category = 'Fellowship' THEN 'fellowship'
        WHEN g.grant_category = 'Training Grant' THEN 'training'
        WHEN g.grant_category = 'Studentship' THEN 'studentship'
        ELSE LOWER(REPLACE(g.grant_category, ' ', '_'))
    END as funding_type,
    g.programme as funder_scheme,  -- Programme name (UKRI FLF, GCRF, etc.) or NULL
    'gateway_to_research' as provenance,
    g.start_date,
    g.end_date,
    YEAR(g.start_date) as start_year,
    YEAR(g.end_date) as end_year,
    struct(g.pi_given_name, g.pi_family_name, NULL as orcid, NULL as role_start,
           struct(g.lead_org_name, 'United Kingdom', NULL) as affiliation) as lead_investigator,
    NULL as co_lead_investigator,
    NULL as investigators,
    CONCAT('https://gtr.ukri.org/projects?ref=', g.grant_reference) as landing_page_url,
    NULL as doi,
    current_timestamp() as created_date,
    current_timestamp() as updated_date
FROM with_funder g
JOIN funders f ON g.council_abbrev = f.alternate_title;
```

### Part 3: Update `CreateAwards.ipynb`

**Location:** `openalex-walden/notebooks/awards/CreateAwards.ipynb`

**Changes:**

1. **Add GTR project awards as new source** with highest priority (0)
2. **Change priority scheme:**
   - Priority 0: `gtr_project_awards` (authoritative for UK grants)
   - Priority 1: `crossref_awards`
   - Priority 2: `backfill_awards`
   - Priority 3: `gtr_awards` (legacy publication-based, for work linkage)

```sql
-- Add to UNION ALL in CreateAwards.ipynb:
UNION ALL

SELECT
    abs(xxhash64(id)) % 9000000000 as id,
    display_name,
    description,
    funder_id,
    funder_award_id,
    amount,
    currency,
    funder,
    funding_type,
    funder_scheme,
    provenance,
    start_date,
    end_date,
    start_year,
    end_year,
    lead_investigator,
    co_lead_investigator,
    investigators,
    landing_page_url,
    doi,
    works_api_url,
    created_date,
    updated_date,
    0 as priority  -- HIGHEST priority for GTR project data
FROM openalex.awards.gtr_project_awards
```

### Part 4: Keep Work-Award Linkage

The existing `gtr_awards` table (publication-based) is still valuable for linking works to awards. Keep `CreateGatewayToResearchAwards.ipynb` but use it only for `work_awards` linkage, not as a primary award source.

## Files to Create/Modify

| File | Action |
|------|--------|
| `scripts/local/gtr_to_s3.py` | **CREATE** - Local script to fetch GtR API data |
| `notebooks/awards/CreateGTRProjectAwards.ipynb` | **CREATE** - Databricks ETL notebook |
| `notebooks/awards/CreateAwards.ipynb` | **MODIFY** - Add GTR project source, update priorities |
| `plans/gtr-full-ingest-plan.md` | **CREATE** - This plan (in walden repo) |

## Expected Outcomes

| Metric | Before | After |
|--------|--------|-------|
| Unique GTR grants in API | ~36K | **~171K** |
| GTR grants with full metadata | ~0 | **~171K** (title, abstract, amount, dates, PI) |
| GTR grants lost to backfill | ~28K | **0** (GTR now wins) |

## Execution Order

1. Create `scripts/local/gtr_to_s3.py`
2. Run locally: `python gtr_to_s3.py` (~4 hours)
3. Create `notebooks/awards/CreateGTRProjectAwards.ipynb`
4. Run in Databricks: CreateGTRProjectAwards
5. Modify `notebooks/awards/CreateAwards.ipynb`
6. Run in Databricks: CreateAwards (rebuilds openalex_awards)
7. Run in Databricks: sync_awards (syncs to Elasticsearch)

## API Details

**GtR API Reference:**
- Base: `https://gtr.ukri.org/gtr/api`
- Projects: `/projects?p={page}&s={size}` (max size=100)
- Project funds: `/projects/{id}/funds`
- Format: XML (parse with ElementTree)
- No auth required
- Total: 171,314 projects across 17,132 pages

**Key fields:**
- `project/id` - UUID
- `project/identifiers/identifier[@type='RCUK']` - Grant reference (EP/xxx)
- `project/title`
- `project/abstractText`
- `project/status` - Active/Closed
- `project/grantCategory`
- `project/fund/funder/name` - Lead funder
- `project/fund/valuePounds` - Amount in GBP
- `project/fund/start`, `project/fund/end` - Dates
