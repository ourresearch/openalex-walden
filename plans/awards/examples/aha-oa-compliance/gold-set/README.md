# AHA gold-set precision/recall

A reproducible pipeline that measures how well **OpenAlex** links works to the
**American Heart Association** as a funder, scored against AHA's own manually
verified gold set, and diagnoses the misses to drive funding-pipeline improvements.
Mirrors the method in [`../../tula-hakai-funder-impact/13_precision_recall.py`](../../tula-hakai-funder-impact/13_precision_recall.py).

> **Private partner data.** AHA's verification workbook contains their internal
> compliance judgments and PI contact details. It is **not** in this repo, and
> neither are the derived gold set or results (`data/`, `tables/` are gitignored).
> Only the code is committed. Point `AHA_XLSX` at the workbook to reproduce.

## The gold set
AHA's workbook is a **Dimensions export, one tab per month**, that AHA staff
manually QC'd. The verification signal is the **Status** column (its colour is
just conditional formatting on that column):

| Status | Colour | Meaning | Gold class |
|---|---|---|---|
| Compliant | green | verified AHA-funded, policy-compliant | `positive` |
| Out of compliance | red | verified AHA-funded, not compliant | `positive` |
| N/A | grey | not actually AHA's (note says why) | `negative` / `positive_no_grant` / `ambiguous` |
| Pending / Non-responsive / blank | amber / — | not yet verified | `pending` (excluded) |

N/A rows are disambiguated by parsing the **Recommendation / Notes** column
(disclosure, AHA-authored, wrong doc type, "have Dimensions unlink", name-match
on author initials "AHA", "theirs but no grant ID cited", …).

## Pipeline (run in order, `py -3` on Windows, `PYTHONIOENCODING=utf-8`)
```
21_extract_goldset.py   # workbook -> data/goldset.csv (+ gitignored _full w/ notes/PI)
22_match_openalex.py    # DOI -> OpenAlex (funders field); data/matched.csv
23_precision_recall.py  # funder-level recall + false-positive rate; tables/23_*.json
24_diagnose.py          # miss causes + definitional-vs-real false positives; tables/24_*.json
```

## What we measure & why
- **Recall (funder level):** of AHA's verified-theirs works, how many does OpenAlex
  link to AHA. Misses are split into *not in OpenAlex* vs *in OpenAlex but not
  AHA-linked* — the latter being the actionable pipeline gap.
- **False positives:** gold negatives that OpenAlex *does* link to AHA. Split into
  **definitional** (AHA acknowledged/disclosed but not a funder — consistent with
  OpenAlex's funder = "anything in acknowledgements" definition) vs **real errors**
  (name-match on initials, wrong document types). Only the latter is actionable.
- **Grant level:** the public OpenAlex `funders` field carries **no award IDs**, and
  the crossref `grants` path has virtually no AHA awards — because AHA's
  grant↔output links live in **PubMed Central**, which OpenAlex does not yet ingest.
  This is the core motivation for the PMC-linkage work (Phase 2 of the collaboration;
  see the parent README roadmap).

## Gotchas
- DOIs: lowercased, `https://doi.org/` stripped, trailing punctuation removed.
- `openpyxl`, **not** pandas, reads the workbook — pandas coerces the literal Status
  value `"N/A"` to NaN.
- OpenAlex work funding is the `funders` field (`[{id, display_name, ror}]`);
  `grants` is **not** a valid `select` field — request via the funders field.
