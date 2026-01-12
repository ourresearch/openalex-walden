# Test Records for Affiliation-as-Author Fix

Use these records to verify the fix works correctly after deployment.

## 1. Records WITH Affiliation-as-Author Issue (should be filtered)

These records contain institution names as author entries that should be REMOVED after the fix.

### DOI: 10.26907/esd.17.3.14
- **Publisher:** Kazan Federal University
- **Issue:** Pattern 1 - Institution split across given/family fields
- **Before fix:** 30 authors (many are institutions like "Kazan University", "Moscow University")
- **After fix:** Should have ~15 real authors (Roza Valeeva, Gulfiya Parfilova, Irina Demakova, etc.)
- **Affected authors to remove:**
  - Author 1: given='Kazan', family='University'
  - Author 4: given='Kazan', family='University'
  - Author 6: given='Moscow', family='University'
  - Author 8: given='Moscow', family='University'
  - (and more...)

### DOI: 10.21002/jaki.2024.11
- **Publisher:** Universitas Indonesia
- **Issue:** Pattern 1 - Institution split across given/family fields
- **Before fix:** 4 authors
- **After fix:** Should have 2 real authors (Emmanuel Dwomor, Emmanuel Mensah)
- **Affected authors to remove:**
  - Author 1: given='Accra', family='UniversityofProfessionalStudies'
  - Author 4: given='Accra', family='UniversityofProfessionalStudies'

### DOI: 10.22394/1726-1139-2018-10-46-63
- **Publisher:** The Russian Presidential Academy of National Economy and Public Administration
- **Before fix:** 4 authors
- **After fix:** Should have 2 real authors (Vladimir Khalin, Galina Chernova)
- **Affected authors to remove:**
  - Author 1: given='SaintPetersburg', family='University'
  - Author 4: given='SaintPetersburg', family='University'

### DOI: 10.17705/1jais.00257
- **Publisher:** Association for Information Systems
- **Before fix:** 8 authors
- **After fix:** Should have 4 real authors (Rajendra Singh, Lars Mathiassen, Max Stachura, Elena Astapova)
- **Affected authors to remove:**
  - Author 1: given='Georgia', family='University'
  - Author 4: given='Georgia', family='University'
  - Author 6: given='Medical', family='CollegeofGeorgia'
  - Author 8: given='Medical', family='CollegeofGeorgia'

### DOI: 10.5840/philtoday20201110368
- **Publisher:** Philosophy Documentation Center
- **Before fix:** 2 authors
- **After fix:** Should have 1 real author (Yuk Hui)
- **Affected authors to remove:**
  - Author 2: given='DePaul', family='University'

---

## 2. Records from EXCLUDED Publishers (should NOT be altered)

These publishers are in `AFFILIATION_AS_AUTHOR_EXCLUDED_PUBLISHERS` and their records should remain unchanged.

### DOI: 10.1093/oso/9780197627112.001.0001
- **Publisher:** Oxford University Press
- **Authors:** Julian Strube
- **Expected:** No change

### DOI: 10.1109/glocom.2014.7037162
- **Publisher:** IEEE
- **Authors:** Hanxu Hou, Kenneth W. Shum, Minghua Chen, Hui Li
- **Expected:** No change (all 4 authors retained)

### DOI: 10.1109/itsc.2011.6082932
- **Publisher:** IEEE
- **Authors:** Matthew Hausknecht, Tsz-Chiu Au, Peter Stone, David Fajardo, Travis Waller
- **Expected:** No change (all 5 authors retained)

---

## 3. Clean Records from Non-Excluded Publishers (should NOT be altered)

These records are from non-excluded publishers but do NOT have the affiliation-as-author issue.

### DOI: 10.14361/9783839442494
- **Publisher:** transcript Verlag
- **Authors:** Miriam Gutekunst
- **Expected:** No change

### DOI: 10.5603/nmr.2016.0019
- **Publisher:** VM Media SP. zo.o VM Group SK
- **Authors:** Ildikó Garai, Sandor Barna, Gabor Nagy, Attila Forgacs
- **Expected:** No change (all 4 authors retained)

### DOI: 10.2979/trancharpeirsoc.57.1.04
- **Publisher:** Indiana University Press
- **Authors:** Aames
- **Expected:** No change

---

## Verification Queries

After deploying the fix, run these queries to verify:

```sql
-- Check specific DOIs that should be filtered
SELECT native_id, size(authors) as author_count,
       transform(authors, a -> struct(a.given, a.family)) as authors
FROM openalex.crossref.crossref_works
WHERE native_id IN (
    '10.26907/esd.17.3.14',    -- Should go from 30 to ~15 authors
    '10.21002/jaki.2024.11',   -- Should go from 4 to 2 authors
    '10.22394/1726-1139-2018-10-46-63',  -- Should go from 4 to 2 authors
    '10.17705/1jais.00257'     -- Should go from 8 to 4 authors
)
```

```sql
-- Check excluded publisher records are unchanged
SELECT native_id, size(authors) as author_count
FROM openalex.crossref.crossref_works
WHERE native_id IN (
    '10.1093/oso/9780197627112.001.0001',  -- Oxford: should stay 1
    '10.1109/glocom.2014.7037162',         -- IEEE: should stay 4
    '10.1109/itsc.2011.6082932'            -- IEEE: should stay 5
)
```

```sql
-- Check clean non-excluded records are unchanged
SELECT native_id, size(authors) as author_count
FROM openalex.crossref.crossref_works
WHERE native_id IN (
    '10.14361/9783839442494',      -- transcript Verlag: should stay 1
    '10.5603/nmr.2016.0019',       -- VM Media: should stay 4
    '10.2979/trancharpeirsoc.57.1.04'  -- Indiana UP: should stay 1
)
```