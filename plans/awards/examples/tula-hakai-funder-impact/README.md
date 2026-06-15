# Tula Foundation / Hakai Institute — funder impact case study

A reproducible pipeline that assembles, verifies, and analyzes the research footprint of the
**Tula Foundation** and its **Hakai Institute** entirely from OpenAlex, and the analysis behind
the blog post *"The Hakai Institute, as seen by OpenAlex."* Built June 2026.

This is the first entry in a growing collection of **funder-intelligence examples** built on
open OpenAlex metadata.

## Headline results
- **1,684** candidates from 9 search strategies → **170** false positives removed (LLM + manual)
  → **1,514** genuine works; analysis corpus of **1,496** (marine/coastal/environmental,
  excluding clinical/behavioural papers).
- **58,579** citations; median **FWCI 1.69**; **6.1%** in the world top-1% (6.1×), **35.2%** in
  top-10% (3.5×).
- Cited by works from **199 countries** and all **26** fields of science.
- **Search quality:** precision **90%** (full) / **95%** (funder-only); recall **89%** / **49%**
  against Hakai's 356-DOI public list.
- Cross-university coastal co-authorship (equal 15-yr windows): **76 → 462** links; intensity
  **0.06 → 0.13**.
- **537** distinct co-funders (NSERC 388, NSF 127, CIFAR 78, DFO 77, Mitacs 70…).
- BC ~**4×** over-represented in Hakai's focus topics.

## Pipeline (run in order, `py -3` on Windows)
```
01_pull_candidates.py     # 9 strategies -> data/candidates.csv (provenance flags)
02_enrich_works.py        # full work objects + abstracts -> data/works.parquet
03_verify_llm.py          # LLM verification -> data/verification.csv
                          #   --build / --split prep inputs; subagents or --run (API) classify;
                          #   --merge combines verdicts. Marine re-scope verdicts in data/rescope_out/.
04_build_corpus.py        # apply verdicts + re-scope + manual fixes -> data/corpus_*.csv
05_descriptive.py         # works/year, subfield donut (+ tables)
06_citations.py           # FWCI, percentile shares, most-cited table
07_field_did.py           # specialization vs world (associational)
08_collaboration.py       # 5-BC-university co-authorship network, equal 15-yr windows
09_downstream_reach.py    # citing works: world map + fields donut
10_research_lineage.py    # sea-otter & kelp lineage
11_cofunders.py           # co-funder ranking (universities excluded)
12_top_fwci_stories.py    # top-50 FWCI works table (for narrative)
13_precision_recall.py    # precision & recall vs Hakai's public list
```
Set `PYTHONIOENCODING=utf-8` on Windows (cp1252 default mangles accents on print).
Analyses cache OpenAlex API responses to `.cache/` (regenerated; gitignored).

**Authentication:** set your OpenAlex API key via `export OPENALEX_API_KEY=...` or by replacing
`"XXX"` in `config.py` (see <https://docs.openalex.org/how-to-use-the-api/api-keys>).

## Layout
- `config.py` — OpenAlex API key, IDs, search strings, BC universities, Hakai topics, treatment year.
- `lib/openalex.py` — OpenAlex client (API-key auth, cursor paging, on-disk cache, abstract
  reconstruction). Note: the `+` AND-operator is kept **literal** in URLs (OpenAlex ignores `%2B`).
- `lib/plotstyle.py` — shared figure style; `donut()`, `year_axis()`, `save()` (PNG + SVG).
- `data/` — candidates, enriched works, verification verdicts, final corpora, `verify_rubric.md`.
- `figures/` — the 10 figures used in the post (PNG 300 dpi + SVG).
- `tables/` — CSV/JSON summaries behind every figure and number.
- `blog/` — `tula-case-study.md` and the `.docx` export (for Google Docs editing).

## Notes & caveats
- Trend charts run through **2025** (last reliably-indexed year); OpenAlex 2026 publication-year
  counts are inflated by indexing lag, though corpus totals span 2026.
- The field-influence analysis is **associational, not causal** (BC was already strong; Tula's
  grants began ~2001).
- Citing-country/field tallies are citation *instances* (a work citing two corpus papers counts
  twice); distinct-country/field counts are exact.
- A few mega-consortium papers inflate the *mean* FWCI; median and percentile shares are robust.
- The LLM verification was run via in-session agents; `03 --run` reproduces it with an
  `ANTHROPIC_API_KEY`. Verdicts are checked in (`data/verification.csv`, `data/rescope_out/`).
