# Investigation: Elsevier __PRELOADED_STATE__ and Springer Corresponding Author

**Status**: Complete
**Investigator**: Human + Claude
**Date**: 2025-01-03

## Background

Analysis of 10k random DOIs identified significant parsing failures:
- **Total failures**: 3,187 (affiliations: 546, corresponding author: 2,325, from Nees project: 316)
- **Top publishers by failure count**:
  1. Elsevier BV: 494 failures (276 affiliation, 218 corresponding)
  2. Springer Science+Business Media: 197 failures (6 affiliation, 191 corresponding)
  3. Wiley: 83 failures
  4. Springer Nature: 76 failures
  5. Oxford University Press: 65 failures

---

## Root Cause: Elsevier/ScienceDirect

Modern ScienceDirect pages no longer embed author data in `<script type="application/json">` tags. Instead, they use:

```javascript
window.__PRELOADED_STATE__ = {"authors": [...], "affiliations": [...], ...};
```

The previous parser only looked for the JSON script tag, missing all `__PRELOADED_STATE__` pages.

---

## Root Cause: Springer

Many Springer pages indicate the corresponding author in a separate "Correspondence to" section:

```html
<p>Correspondence to Xiaolei Guo.</p>
```

The parser was extracting authors but not cross-referencing with this section to set `is_corresponding`.

---

## Changes Made

### parseland-lib commit: 19363d4

1. **sciencedirect.py**:
   - `extract_json()`: Now tries both `<script type="application/json">` and `window.__PRELOADED_STATE__`
   - `get_json_authors_affiliations_abstract()`: Returns proper empty result instead of causing KeyError

2. **springer.py**:
   - Added `_get_correspondence_name()`: Extracts name from "Correspondence to" paragraphs
   - Added `_mark_corresponding_author()`: Matches name against author list using fuzzy matching

---

## Verification

Deployment to production Parseland API verified with sample DOIs:

### Elsevier (4/4 success)
| DOI | Authors | Affiliations | Corresponding |
|-----|---------|--------------|---------------|
| 10.1016/j.jde.2019.01.028 | 2 | Yes | Yes |
| 10.1016/j.jobe.2024.110541 | 2 | Yes | Yes |
| 10.1016/j.nhres.2025.01.003 | 9 | Yes | Yes |
| 10.1016/j.micron.2024.103705 | 4 | Yes | Yes |

### Springer (2/2 success)
| DOI | Authors | Corresponding |
|-----|---------|---------------|
| 10.1007/s00170-017-0085-8 | 6 | Yes |
| 10.1007/s00339-016-9973-2 | 2 | Yes |

---

## Known Limitations

- **Character encoding**: Some Springer pages have upstream encoding issues (e.g., `Â` characters) that are not fixed by these changes
- **Wiley**: Not addressed in this fix
- **Other publishers**: Not addressed in this fix
