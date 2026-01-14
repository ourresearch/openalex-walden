# Acceptance Tests: Trailing Period Removal from Affiliation Strings

## Test 1: No Trailing Periods in affiliation_strings_lookup

**Query:**
```sql
SELECT COUNT(*) AS trailing_period_count
FROM openalex.institutions.affiliation_strings_lookup
WHERE raw_affiliation_string LIKE '%.';
```

**Expected Result:** `0`

**Actual Result:**
| trailing_period_count |
|-----------------------|
| PENDING |

---

## Test 2: No Duplicate Pairs in affiliation_strings_lookup

**Query:**
```sql
SELECT COUNT(*) AS duplicate_pairs
FROM openalex.institutions.affiliation_strings_lookup a
JOIN openalex.institutions.affiliation_strings_lookup b
  ON RTRIM(a.raw_affiliation_string, '.') = b.raw_affiliation_string
WHERE a.raw_affiliation_string LIKE '%.';
```

**Expected Result:** `0`

**Actual Result:**
| duplicate_pairs |
|-----------------|
| PENDING |

---

## Test 3: Specific Work Example - work_id 4414994979

**Query:**
```sql
SELECT
    work_id,
    authorship.raw_affiliation_strings
FROM openalex.works.openalex_works
LATERAL VIEW EXPLODE(authorships) AS authorship
WHERE work_id = 4414994979;
```

**Expected Result:** No duplicate affiliations, no trailing periods

**Before Fix:**
| raw_affiliation_strings |
|------------------------|
| `['Department of General Surgery, Sir Run Run Hospital, Nanjing Medical University, Nanjing, Jiangsu, 211112, China.', 'Department of General Surgery, Sir Run Run Hospital, Nanjing Medical University, Nanjing, Jiangsu, 211112, China']` |

**After Fix (Expected):**
| raw_affiliation_strings |
|------------------------|
| `['Department of General Surgery, Sir Run Run Hospital, Nanjing Medical University, Nanjing, Jiangsu, 211112, China']` |

**Actual Result:**
| raw_affiliation_strings |
|------------------------|
| PENDING |

---

## Test 4: No Trailing Periods in work_authors

**Query:**
```sql
SELECT COUNT(*) AS records_with_trailing_period
FROM openalex.works.work_authors
WHERE EXISTS(raw_affiliation_strings, s -> s LIKE '%.');
```

**Expected Result:** `0`

**Actual Result:**
| records_with_trailing_period |
|------------------------------|
| PENDING |

---

## Test 5: Sample Affiliations Look Correct

**Query:**
```sql
SELECT DISTINCT raw_affiliation_string
FROM openalex.institutions.affiliation_strings_lookup
WHERE raw_affiliation_string LIKE '%China'
   OR raw_affiliation_string LIKE '%USA'
   OR raw_affiliation_string LIKE '%Germany'
LIMIT 10;
```

**Expected Result:** No strings ending with periods

**Actual Result:**
| raw_affiliation_string |
|------------------------|
| PENDING |

---

## Verification Log

| Test | Date | Status | Notes |
|------|------|--------|-------|
| Test 1 | | PENDING | |
| Test 2 | | PENDING | |
| Test 3 | | PENDING | |
| Test 4 | | PENDING | |
| Test 5 | | PENDING | |

---

## Schema Reference

**affiliation_strings_lookup:**
- `raw_affiliation_string` - STRING (primary key)
- `institution_ids` - ARRAY<BIGINT>
- `institution_ids_override` - ARRAY<BIGINT>
- `countries` - ARRAY<STRING>
- `created_datetime` - TIMESTAMP

**work_authors:**
- `work_id` - BIGINT
- `author_sequence` - INT
- `raw_affiliation_strings` - ARRAY<STRING>
- `updated_at` - TIMESTAMP

**openalex_works.authorships[]:**
- `raw_affiliation_strings` - ARRAY<STRING>
