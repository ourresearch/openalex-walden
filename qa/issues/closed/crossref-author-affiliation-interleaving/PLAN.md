# Plan: Fix Crossref Author/Affiliation Interleaving

## Summary

Modify `notebooks/ingest/Crossref.py` to filter out affiliation-as-author entries (institution names incorrectly appearing as author fields) for publishers NOT on a safe exclusion list.

## Requirements

1. **Safe publishers excluded**: Fix applies to publishers NOT on the exclusion list
2. **Remove affiliation-as-author entries entirely**: Filter out author entries matching institution keywords
3. **Inline configuration**: Publisher list and keywords defined as constants in Crossref.py

## Two Patterns to Detect

Based on real examples, affiliation-as-author entries appear in two forms:

**Pattern 1** - Institution split across given/family (DOI: `10.26907/esd.17.3.14`):
```json
{"given": "Kazan", "family": "University", "name": null}
```

**Pattern 2** - Institution in name field only (DOI: `10.21002/jaki.2024.11`):
```json
{"name": "University of Professional Studies, Accra, Ghana", "given": null, "family": null}
```

The detection logic must check ALL THREE fields.

## Files to Modify

- `notebooks/ingest/Crossref.py` (lines 17-21 for constants, lines 191-215 for transformation)

## Files to Create

- `qa/issues/open/crossref-author-affiliation-interleaving/PLAN.md` - Copy of this fix plan
- `notebooks/maintenance/CleanupAffiliationAsAuthor.py` - Data cleanup notebook (see Data Cleanup section)

## Implementation Steps

### Step 1: Add Constants (after line 21)

Add the safe publisher list and institution detection regex pattern:

```python
# Author/Affiliation Interleaving Fix - Safe publishers excluded from fix
SAFE_PUBLISHERS = [
    "IEEE",
    "Informa UK Limited",
    "American Chemical Society (ACS)",
    "Springer Berlin Heidelberg",
    "Institute of Electrical and Electronics Engineers (IEEE)",
    "Springer International Publishing",
    "Routledge",
    "Walter de Gruyter GmbH",
    "Elsevier",
    "Royal Society of Chemistry (RSC)",
    "AIP Publishing",
    "Georg Thieme Verlag KG",
    "Oxford University Press",
    "SPIE",
    "ENCODE Data Coordination Center",
    "ACM",
    "Emerald",
    "Project MUSE",
    "BRILL",
    "CRC Press",
    "University of Chicago Press",
    "Trans Tech Publications, Ltd.",
    "CAIRN",
    "PERSEE Program",
    "Springer Nature Switzerland",
    "OpenEdition",
    "Springer Nature Singapore",
    "De Gruyter",
    "Egypts Presidential Specialized Council",
    "Japan Society of Mechanical Engineers",
    "American Geophysical Union (AGU)",
    "IUCN",
    "Cambridge University Press",
    "H1 Connect",
    "IGI Global",
    "Inderscience Publishers",
    "The Conversation",
    "Institution of Engineering and Technology (IET)",
    "American Institute of Aeronautics & Astronautics",
    "The Electrochemical Society",
    "Center for Open Science",
    "Research Square Platform LLC",
    "Duke University Press",
    "SAE International",
    "Universidade de Sao Paulo",
    "Bentham Science Publishers Ltd.",
    "Acoustical Society of America (ASA)",
    "transcript Verlag",
    "International Union of Crystallography (IUCr)",
    "Edward Elgar Publishing",
    "Association for Computing Machinery (ACM)",
    "Atlantis Press",
    "American Society of Civil Engineers (ASCE)",
    "VS Verlag fur Sozialwissenschaften",
    "World Scientific Pub Co Pte Lt",
    "Scientific Research Publishing, Inc.",
    "ASTM International",
    "Hans Publishers",
    "Egyptian Knowledge Bank",
    "American Society of Mechanical Engineers",
    "The Royal Society",
    "Nomos Verlagsgesellschaft mbH & Co. KG",
    "Sciencedomain International",
]

# Institution keywords regex for detecting affiliation-as-author entries (case-insensitive)
INSTITUTION_KEYWORDS_PATTERN = (
    r"(?i)\b("
    # English institution keywords
    r"University|Institute|College|Hospital|Department|School|Center|Centre|"
    r"Laboratory|Faculty|Academy|"
    # Non-English institution keywords
    r"Universiteit|Universidade|Università|Uniwersytet|Üniversitesi|Universite|"
    r"Hochschule|Fakultät|Klinikum|Krankenhaus|Politecnico|Politechnika|"
    # Corporate/organization patterns
    r"Inc|LLC|Ltd|Corp|Corporation|Company|GmbH|Consortium|Association|"
    r"Collaboration|Committee|Council|Organization|Organisation|"
    # Additional keywords
    r"Clinic|Medical|Research|Museum|Library|Foundation|Polytechnic"
    r")\b"
)
```

### Step 2: Add Helper Functions (after constants)

```python
def create_author_struct(author):
    """Transform raw Crossref author to normalized author struct."""
    return F.struct(
        F.substring(author["given"], 0, MAX_AUTHOR_NAME_LENGTH).alias("given"),
        F.substring(author["family"], 0, MAX_AUTHOR_NAME_LENGTH).alias("family"),
        F.substring(author["name"], 0, MAX_AUTHOR_NAME_LENGTH).alias("name"),
        F.regexp_extract(
            author["ORCID"], r"(\d{4}-\d{4}-\d{4}-\d{4})", 1
        ).alias("orcid"),
        F.transform(
            author["affiliation"],
            lambda aff: F.struct(
                F.substring(aff["name"], 0, MAX_AFFILIATION_STRING_LENGTH).alias("name"),
                F.substring(F.get(aff["department"], 0), 0, MAX_AFFILIATION_STRING_LENGTH).alias("department"),
                F.when(
                    F.get(aff["id"]["id-type"], 0) == "ROR",
                    F.get(aff["id"]["id"], 0)
                ).alias("ror_id")
            )
        ).alias("affiliations")
    )

def is_valid_author(author):
    """
    Returns True if the author is NOT an affiliation-as-author entry.

    Detects two patterns:
    1. Institution keywords in given OR family fields (e.g., given="Kazan", family="University")
    2. Institution keywords in name field when given/family are empty
       (e.g., name="University of Professional Studies, Accra, Ghana")
    """
    family = F.coalesce(author["family"], F.lit(""))
    given = F.coalesce(author["given"], F.lit(""))
    name = F.coalesce(author["name"], F.lit(""))

    # Pattern 1: Institution keywords in given or family
    has_institution_in_given_family = (
        family.rlike(INSTITUTION_KEYWORDS_PATTERN) |
        given.rlike(INSTITUTION_KEYWORDS_PATTERN)
    )

    # Pattern 2: Institution in name field when given/family are empty
    has_institution_in_name_only = (
        (F.trim(given) == "") &
        (F.trim(family) == "") &
        name.rlike(INSTITUTION_KEYWORDS_PATTERN)
    )

    return ~(has_institution_in_given_family | has_institution_in_name_only)
```

### Step 3: Modify Authors Transform (replace lines 191-215)

Replace the existing `.withColumn("authors", ...)` with conditional logic:

```python
.withColumn(
    "authors",
    F.when(
        F.col("publisher").isin(SAFE_PUBLISHERS),
        # Safe publishers: keep all authors unchanged
        F.transform("author", create_author_struct)
    ).otherwise(
        # Other publishers: filter out affiliation-as-author entries, then transform
        F.transform(
            F.filter(F.col("author"), is_valid_author),
            create_author_struct
        )
    )
)
```

## Verification

1. **Test with known affected DOIs**:
   - `10.26907/esd.17.3.14` (Pattern 1: given="Kazan", family="University")
   - `10.21002/jaki.2024.11` (Pattern 2: name="University of Professional Studies...")
2. **Test safe publisher records** - verify all authors retained
3. **Run validation queries after deployment** - must verify BOTH tables:

### 3a. Check crossref_works (source table)

```sql
-- Check for remaining affiliation-as-author entries in crossref_works
SELECT publisher, COUNT(*) as remaining_affiliation_as_author
FROM openalex.crossref.crossref_works
WHERE publisher NOT IN ('IEEE', 'Elsevier', 'Springer Berlin Heidelberg', ...)
  AND EXISTS(SELECT 1 FROM explode(authors) a
             WHERE a.family RLIKE '(?i)\\b(University|Institute|College|Hospital)\\b'
                OR a.given RLIKE '(?i)\\b(University|Institute|College|Hospital)\\b'
                OR (a.name RLIKE '(?i)\\b(University|Institute|College|Hospital)\\b'
                    AND COALESCE(a.given, '') = ''
                    AND COALESCE(a.family, '') = ''))
GROUP BY publisher
ORDER BY remaining_affiliation_as_author DESC
LIMIT 20
```

### 3b. Check openalex_works (final destination table)

```sql
-- Verify fix propagated to openalex_works for known affected DOIs
SELECT native_id, authors
FROM openalex.works.openalex_works
WHERE native_id IN ('10.26907/esd.17.3.14', '10.21002/jaki.2024.11')
-- Should show only real authors, no institution names
```

```sql
-- Check for remaining affiliation-as-author entries in openalex_works
SELECT COUNT(*) as remaining_affiliation_as_author
FROM openalex.works.openalex_works
WHERE provenance = 'crossref'
  AND EXISTS(SELECT 1 FROM explode(authors) a
             WHERE a.family RLIKE '(?i)\\b(University|Institute|College|Hospital)\\b'
                OR a.given RLIKE '(?i)\\b(University|Institute|College|Hospital)\\b'
                OR (a.name RLIKE '(?i)\\b(University|Institute|College|Hospital)\\b'
                    AND COALESCE(a.given, '') = ''
                    AND COALESCE(a.family, '') = ''))
```

## Data Cleanup: Existing work_authors Records

After deploying the fix to Crossref.py, existing records in `openalex.works.work_authors` need to be cleaned up.

### Cleanup Notebook: `notebooks/maintenance/CleanupAffiliationAsAuthor.py`

Create a new notebook with the following steps:

#### Step 1: Identify Affected Work IDs

```sql
-- Find works with affiliation-as-author entries from Crossref provenance
-- Store in temp table for reprocessing
CREATE OR REPLACE TEMP VIEW affected_work_ids AS
SELECT DISTINCT w.id as work_id
FROM openalex.works.openalex_works_base w
WHERE w.provenance = 'crossref'
  AND EXISTS(
    SELECT 1 FROM explode(w.authors) a
    WHERE a.family RLIKE '(?i)\\b(University|Institute|College|Hospital|Department|School|Center|Centre|Laboratory|Faculty|Academy|Universiteit|Universidade|Università|Uniwersytet|Üniversitesi|Universite|Hochschule|Fakultät|Klinikum|Krankenhaus|Politecnico|Politechnika|Inc|LLC|Ltd|Corp|Corporation|Company|GmbH|Consortium|Association|Collaboration|Committee|Council|Organization|Organisation|Clinic|Medical|Research|Museum|Library|Foundation|Polytechnic)\\b'
      OR a.given RLIKE '(?i)\\b(University|Institute|...)\\b'
      OR (a.name RLIKE '(?i)\\b(University|Institute|...)\\b'
          AND COALESCE(a.given, '') = ''
          AND COALESCE(a.family, '') = '')
  );

-- Log count
SELECT COUNT(*) as affected_work_count FROM affected_work_ids;
```

#### Step 2: Delete Affected Records from work_authors

```sql
-- Delete existing records for affected works
DELETE FROM openalex.works.work_authors
WHERE work_id IN (SELECT work_id FROM affected_work_ids);

-- Log deleted count
SELECT COUNT(*) as deleted_count
FROM openalex.works.work_authors
WHERE work_id IN (SELECT work_id FROM affected_work_ids);  -- Should be 0
```

#### Step 3: Trigger Reprocessing

Option A: Update the source to trigger incremental reprocessing:
```sql
-- Touch the updated_date on affected works to trigger reprocessing
UPDATE openalex.works.openalex_works_base
SET updated_date = current_date()
WHERE id IN (SELECT work_id FROM affected_work_ids);
```

Option B: Force full reprocessing by resetting max_updated_date:
```sql
-- Set max_updated_date variable to before the earliest affected record
-- Then run UpdateWorkAuthors notebook normally
SET VARIABLE max_updated_date = (
  SELECT MIN(created_date) - INTERVAL 1 DAY
  FROM openalex.works.openalex_works_base
  WHERE id IN (SELECT work_id FROM affected_work_ids)
);
```

#### Step 4: Re-run UpdateWorkAuthors

Run the existing `notebooks/end2end/UpdateWorkAuthors.ipynb` which will:
1. Pick up the affected works (since their updated_date changed)
2. Process them through author matching with the FIXED authors array
3. MERGE them back into work_authors (as NOT MATCHED, so INSERT)

#### Step 5: Refresh Materialized Views

```sql
REFRESH MATERIALIZED VIEW openalex.works.work_author_affiliations_mv;
```

#### Step 6: Continue Pipeline

Run the rest of the end2end pipeline normally (UpdateWorkAuthorships, etc.)

### Execution Order

1. **Deploy Crossref.py fix** - Filter out affiliation-as-author entries
2. **Wait for Crossref DLT pipeline to process** - New/updated records get clean authors
3. **Run cleanup notebook** - Remove and reprocess historical affected records
4. **Verify** - Run validation queries on both tables

## Rollback

Remove the `F.when().otherwise()` wrapper and `is_valid_author` filter, restoring the original `F.transform("author", create_author_struct)` call. The helper functions can remain as they don't affect behavior.
