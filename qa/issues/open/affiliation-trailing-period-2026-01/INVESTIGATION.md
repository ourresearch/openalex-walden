# Investigation: Raw Affiliation Strings Duplicated Due to Trailing Periods

## Timeline

- **Ongoing**: Issue has existed since pipeline inception
- **2026-01-13**: Issue discovered via work_id 4414994979

## Root Cause

Raw affiliation strings from source data (Crossref, PubMed, DataCite, PDF, Landing Page) sometimes include trailing periods and sometimes don't. The current cleanup logic in `CreateWorksBase.ipynb` removes newlines and trims whitespace, but does not strip trailing periods.

**Location of existing cleanup logic:**
`notebooks/end2end/CreateWorksBase.ipynb`, in the `authors_with_corresponding` CTE:

```sql
ARRAY_DISTINCT(
    TRANSFORM(
        ARRAY_COMPACT(
            CONCAT(
                COALESCE(e.raw_affiliation_strings, ARRAY()),
                COALESCE(l.raw_affiliation_strings, ARRAY())
            )
        ),
        s -> TRIM(REPLACE(s, '\\n', ''))  -- No trailing period removal
    )
) AS raw_affiliation_strings,
```

## Example Work

**work_id**: 4414994979

This work has the same affiliation appearing twice:
1. `Department of General Surgery, Sir Run Run Hospital, Nanjing Medical University, Nanjing, Jiangsu, 211112, China.`
2. `Department of General Surgery, Sir Run Run Hospital, Nanjing Medical University, Nanjing, Jiangsu, 211112, China`

## Test Examples (50 total)

### Should Have Trailing Period Removed (25 examples)

| # | Original String | After Fix |
|---|-----------------|-----------|
| 1 | `University of California, San Francisco, CA, USA.` | `University of California, San Francisco, CA, USA` |
| 2 | `Department of Medicine, Beijing, China.` | `Department of Medicine, Beijing, China` |
| 3 | `Max Planck Institute, Munich, Germany.` | `Max Planck Institute, Munich, Germany` |
| 4 | `National Institutes of Health, Bethesda, MD.` | `National Institutes of Health, Bethesda, MD` |
| 5 | `Department of General Surgery, Sir Run Run Hospital, Nanjing Medical University, Nanjing, Jiangsu, 211112, China.` | `Department of General Surgery, Sir Run Run Hospital, Nanjing Medical University, Nanjing, Jiangsu, 211112, China` |
| 6 | `Harvard Medical School, Boston, Massachusetts.` | `Harvard Medical School, Boston, Massachusetts` |
| 7 | `Oxford University, Oxford, United Kingdom.` | `Oxford University, Oxford, United Kingdom` |
| 8 | `Tokyo Institute of Technology, Tokyo, Japan.` | `Tokyo Institute of Technology, Tokyo, Japan` |
| 9 | `ETH Zurich, Switzerland.` | `ETH Zurich, Switzerland` |
| 10 | `Seoul National University, Seoul, South Korea.` | `Seoul National University, Seoul, South Korea` |
| 11 | `CNRS, Paris, France.` | `CNRS, Paris, France` |
| 12 | `MIT, Cambridge, MA 02139.` | `MIT, Cambridge, MA 02139` |
| 13 | `Stanford University, Stanford, CA 94305.` | `Stanford University, Stanford, CA 94305` |
| 14 | `Acme Corp.` | `Acme Corp` |
| 15 | `Company Inc.` | `Company Inc` |
| 16 | `Institute Ltd.` | `Institute Ltd` |
| 17 | `St. Mary's Hospital, London.` | `St. Mary's Hospital, London` |
| 18 | `Dr. Smith's Laboratory, UCLA.` | `Dr. Smith's Laboratory, UCLA` |
| 19 | `Dept. of Physics, Caltech.` | `Dept. of Physics, Caltech` |
| 20 | `Center for Disease Control and Prevention, Atlanta, GA.` | `Center for Disease Control and Prevention, Atlanta, GA` |
| 21 | `Memorial Sloan Kettering Cancer Center, New York, NY.` | `Memorial Sloan Kettering Cancer Center, New York, NY` |
| 22 | `Peking University, Beijing 100871, P.R. China.` | `Peking University, Beijing 100871, P.R. China` |
| 23 | `University of São Paulo, Brazil.` | `University of São Paulo, Brazil` |
| 24 | `Karolinska Institutet, Stockholm, Sweden.` | `Karolinska Institutet, Stockholm, Sweden` |
| 25 | `Tsinghua University, Beijing.` | `Tsinghua University, Beijing` |

### Should NOT Be Changed (25 examples)

| # | Original String | After Fix | Notes |
|---|-----------------|-----------|-------|
| 1 | `University of California, San Francisco, CA, USA` | `University of California, San Francisco, CA, USA` | No trailing period |
| 2 | `Department of Medicine, Beijing, China` | `Department of Medicine, Beijing, China` | No trailing period |
| 3 | `Max Planck Institute, Munich, Germany` | `Max Planck Institute, Munich, Germany` | No trailing period |
| 4 | `National Institutes of Health, Bethesda, MD` | `National Institutes of Health, Bethesda, MD` | No trailing period |
| 5 | `St. Mary's Hospital, London` | `St. Mary's Hospital, London` | Period is abbreviation, not trailing |
| 6 | `Dr. John Smith's Lab, Harvard` | `Dr. John Smith's Lab, Harvard` | Period is abbreviation |
| 7 | `Dept. of Computer Science, MIT` | `Dept. of Computer Science, MIT` | Period is abbreviation |
| 8 | `U.S. Department of Energy` | `U.S. Department of Energy` | Periods are abbreviations |
| 9 | `P.O. Box 12345, Stanford University` | `P.O. Box 12345, Stanford University` | Periods are abbreviations |
| 10 | `MIT, Cambridge, MA 02139` | `MIT, Cambridge, MA 02139` | Ends with number |
| 11 | `Stanford University (School of Medicine)` | `Stanford University (School of Medicine)` | Ends with parenthesis |
| 12 | `contact@university.edu` | `contact@university.edu` | Email address |
| 13 | `Building A, Floor 3` | `Building A, Floor 3` | Ends with number |
| 14 | `Research Center #5` | `Research Center #5` | Ends with number |
| 15 | `University of Tokyo (東京大学)` | `University of Tokyo (東京大学)` | Ends with CJK characters |
| 16 | `Université Paris-Saclay` | `Université Paris-Saclay` | No trailing period |
| 17 | `Instituto de Investigación` | `Instituto de Investigación` | No trailing period |
| 18 | `ETH Zürich` | `ETH Zürich` | No trailing period |
| 19 | `Beijing 100871` | `Beijing 100871` | Ends with number |
| 20 | `Laboratory of Dr. Chen` | `Laboratory of Dr. Chen` | Period is abbreviation |
| 21 | `Prof. Wang's Research Group` | `Prof. Wang's Research Group` | Period is abbreviation |
| 22 | `123 University Ave` | `123 University Ave` | No trailing period |
| 23 | `Room 401, Building B` | `Room 401, Building B` | No trailing period |
| 24 | `c/o Department of Biology` | `c/o Department of Biology` | No trailing period |
| 25 | `University of California - Berkeley` | `University of California - Berkeley` | Ends with hyphenated name |

**Decision**: Remove ALL trailing periods for consistency. The `RTRIM(s, '.')` function only removes periods at the very end of the string, so abbreviations like "St.", "Dr.", "Inc." in the middle are preserved.

## Impact Assessment

### Tables with raw_affiliation_strings

| Table | Description |
|-------|-------------|
| `openalex.institutions.affiliation_strings_lookup` | Lookup table for ML institution matching |
| `openalex.works.work_authors` | Author records with raw affiliations |
| `openalex.works.work_authorships` | Enriched authorship data |
| `openalex.works.openalex_works` | Final works table (API/Elasticsearch) |

### Data Flow

```
Source Data (Crossref, PubMed, etc.)
       ↓
CreateWorksBase.ipynb → openalex_works_base (CLEANUP POINT)
       ↓
UpdateWorkAuthors.ipynb → work_authors
       ↓
UpdateWorkAuthorships.ipynb → work_authorships
       ↓
CreateWorksEnriched.ipynb → openalex_works
       ↓
sync_works → Elasticsearch
```

## Impact Queries

Run these queries to determine the number of affected strings:

```sql
-- Count affiliation strings ending with period in lookup table
SELECT COUNT(*) AS strings_with_trailing_period
FROM openalex.institutions.affiliation_strings_lookup
WHERE raw_affiliation_string LIKE '%.';

-- Count duplicate pairs (both with and without period exist)
SELECT COUNT(*) AS duplicate_pairs
FROM openalex.institutions.affiliation_strings_lookup a
JOIN openalex.institutions.affiliation_strings_lookup b
  ON RTRIM(a.raw_affiliation_string, '.') = b.raw_affiliation_string
WHERE a.raw_affiliation_string LIKE '%.';

-- Count work_authors records with any trailing period string
SELECT COUNT(*) AS work_authors_affected
FROM openalex.works.work_authors
WHERE EXISTS(raw_affiliation_strings, s -> s LIKE '%.');

-- Sample of strings that will be altered
SELECT raw_affiliation_string
FROM openalex.institutions.affiliation_strings_lookup
WHERE raw_affiliation_string LIKE '%.'
LIMIT 20;
```

## Open Questions

None - approach confirmed.
