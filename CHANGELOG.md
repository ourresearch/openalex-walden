# Changelog

We document notable changes to the data in this file; it's in reverse chronological order (recent changes up top).

## [Unreleased]
### Changed
* Works: `related_works` is recomputed for every work: up to 10 works closest in meaning to each one (the nearest neighbours of its title, abstract and venue in OpenAlex's Qwen3 embeddings), never paratext, errata, retractions, reviews or retracted works, and never a work with the same title. 437.5M works have a list (95.8% of titled core works, 92.8% of titled expansion-pack works; before: 64.9% of core works and no expansion-pack work, a one-time 2025 copy of an older topic-based method). Blind-judged on random works, the new lists' items average 2.34 on a 0-3 relatedness scale vs 0.72 for the old lists. New works now get their list the day after they are added (a nightly step; recall of the 10 nearest vs exact search 0.93). The backfill did not move `updated_date` (snapshot users need a full reload of works); a new work's list landing does. (oxjob #1648)
* Works: `topics` and `primary_topic` now come from a new topic classifier (a Qwen3-8B model trained on 2M works labelled by Claude Opus 5.5, reading the title, abstract and venue). The topics themselves do not change: same 4,516 topics, names, IDs and subfield / field / domain hierarchy. Each work gets up to 3 topics; `score` is the model's calibrated probability that the topic is the work's main one, so 2nd and 3rd scores are often small. Works the model cannot classify (mostly records with no abstract and little title text) get no topic. On a fresh test set of random works the primary topic is right 81.7% of the time vs 24.8% for the previous model; about 447M works change topics. `fwci`, `citation_normalized_percentile` and `cited_by_percentile_year` move with the new subfields (their cohort). `updated_date` does not move for this change: snapshot users need a full reload. Code, weights and evaluations: https://github.com/ourresearch/openalex-topic-classification/tree/main/v2 (oxjob #1531)
* Works: `sustainable_development_goals` now comes from a new classifier: a per-goal head on the work's Qwen3 embedding, trained on 200K Jev (TypeSafe) judgments of the 17 goals and calibrated per goal. Same element shape (`id`, `display_name`, `score`); a goal is listed when its `score` (the classifier's confidence, 0-1, higher = surer) is at least 0.4, highest first. Works without an embedding have an empty list. Display names follow the UN wording: "Quality education", "Life on land", "Peace, justice, and strong institutions". Against a judge committee on 2,000 random works the new tags score F1 0.68 vs 0.29 for the previous model, and better on all 17 goals. (oxjob #1300)
### Fixed
* Works: a new arXiv preprint's DOI (10.48550/arXiv.<id>, which arXiv registers with DataCite) now joins the work that holds the preprint's arXiv record. It named its paper only in the DOI, so it could join only by title, and about 1 in 23 arXiv preprints (139,059) became two works: `/works/doi:10.48550/arXiv.<id>` answered with a second, thin copy, and a `doi:` filter or a citation to the arXiv DOI found it instead of the paper. (oxjob #1940)
* Authors: about 25,000 author profiles a week stop getting a new id. A weekly re-check meant to find papers bound to the wrong person read every byline written 'Surname, Given' or 'Organisation, Place' as empty, so it unbound those papers, matched them again and minted a new profile under a new id, deleting the old one (which then answered 404). Most were organisation names from library catalogues ('Hamilton College (Clinton, N.Y.)', '7 Stages Theatre (Atlanta, Ga.)'); some were people written surname first. Ids now stay as they are.
* Works: abstracts and titles are no longer cut short at a "<" that is text. The cleaner read any "<" followed by a letter as the start of an HTML tag and deleted everything up to the next ">" or to the end of the text, so an inequality ("(<I00 days old)", "0<x<1"), a detection limit ("<LOD"), a placeholder ("<script_name.py>") or an angle-quoted title ("Film <The Throne>", "<Hygrophila phlomoides> Nees") cut the rest of the sentence or of the abstract. About 659,000 abstracts and 80,000 titles regain the text (38M words; 135K had lost half or more), from every source: PDFs, landing pages, repositories, DataCite, Crossref, PubMed and the legacy abstract table. Real markup (HTML, JATS, MathML) is still removed, and nothing that could run in a browser is kept. Swapped tags in some repository titles (">i<Pinus>/i<") are repaired. `updated_date` moved for the works whose abstract or title changed. (oxjob #1928)
* Works: about 220,000 Research Square preprints (220,104) show their abstract again. Research Square licenses every preprint CC BY in its own Crossref record, but Crossref names its publisher Springer, so the rule that withholds Springer and Elsevier abstracts hid them. A work now shows a Research Square abstract only when it is the text of that preprint's own Crossref record, under that record's CC BY licence. Where a preprint is merged with its published version, the work shows the preprint's abstract; the published paper's abstract stays hidden, even when the two differ. (oxjob #1926)
* Works: `mesh` matches the work's current PubMed record: each heading once, with NLM's major-topic marks. Before, a work carried one copy of its headings for every PubMed record version we had stored, so 33.4M of the 33.6M works with MeSH listed each heading 2-3 times (1.30B entries for 406M distinct), 284K held copies that disagreed on `is_major_topic`, and about 577K kept headings NLM has since removed. And `is_major_topic` read only the descriptor's mark, so a pair NLM marks on its qualifier ("Neoplasms/drug therapy*") showed false: 72.2M pairs on 24.2M works. Now only the latest record version is read and `is_major_topic` is true when NLM marks the descriptor or that qualifier: 100 of 100 random works match NCBI's current record (0 of 100 before). About 14K works whose current record has no MeSH now have none. `updated_date` moved for the ~33.4M works whose `mesh` changed. (oxjob #1920)
* Authorships: each affiliation is listed once per author. About 16.3M works listed the same affiliation two or more times on one author, the copies differing only in punctuation, quote marks or spacing ("Xi’an" / "Xi'an", "Royal Holloway University of London  Egham UK" / "Royal Holloway University of London, Egham, UK"), most often one copy from the work's record and one from the older affiliation table. `raw_affiliation_strings` and `affiliations` now keep one copy per affiliation: the best-formed one (parts separated, no doubled spaces or stray marks), carrying the institution ids of every copy, so no institution is lost and `institutions` does not change. 50.2M copies leave 46.8M authorships. `updated_date` moved for these works. (oxjob #1911)
* Works: about 7.06M works that had no `publication_year` (6.1M of them in the expansion pack, most of them digitised older material from library and repository collections) now carry the year their own repository or DataCite records give. A model read every record we hold for each of the 14.8M works with no year, and a year is kept only when the model was sure (about half of them). An independent check against other catalogues found about 1.5% of the kept years off by more than a year (the years Crossref gives for old works: 1.4%). Years before 1900 are now kept, which replaces the 2025-11-01 OAI-PMH rule. A DataCite placeholder year of 10000 no longer outranks a real date. `fwci`, `citation_normalized_percentile` and `cited_by_percentile_year` compare a work with others of the same year, subfield and type, so works published before 1950 see their values move where these collections fill their cohorts; for works published since 1950, 99.9% of FWCI values move by less than 10%. `updated_date` moved for the works that gained a year. (oxjob #1644)
### Deprecated
* Works: `sustainable_development_goals_aurora` holds the previous model's (Aurora SDG-BERT) tags, frozen on the switch date and no longer updated. API only, not in the snapshot; removed about a month after the switch. (oxjob #1300)

## [2026-08-18]
### Fixed
* Authorships: raw affiliation strings with double-encoded UTF-8 (mojibake, e.g. `UniversitÃ©`) are repaired to their proper form (`Université`) — ~172K distinct strings on ~189K works, mostly publisher-deposited Crossref metadata; a garbled string and its clean twin on the same author collapse into one entry. Institution matching now keys on the repaired string: where the garbled form and its clean twin had been matched differently, the clean form's match stands (the garbled-text matches were systematically degraded), and garbled strings with no clean twin are re-matched on the clean text. Expect small institution works_count shifts (largest: junk institutions losing works, e.g. "Anna Needs Neuroblastoma Answers" −1K; real universities gaining, e.g. University of Concepción +1.2K). RAS curations keyed on a garbled string follow it to the repaired form. (oxjob #801)

## [2025-11-02]
### Fixed
* Citations: corrected `cited_by_percentile_year` by computing proper per-year distributions and bounds; min/max now reflect true placement across years.

## [2025-11-01]
### Changed
* OAI-PMH: set `publication_date` to the earliest available date; if the earliest parsed date is < 1900, leave `publication_date` null.
* Keywords: restrict derived keywords to concepts with `level > 0` (more changes may be required).

## [2025-10-30]
### Changed
* Sources: minor curation (removed one problematic source), which may shift `primary_location.source` for a small subset of works.

## [2025-10-29]
### Changed
* References: stabilize `referenced_works` by unioning backfilled and parsed sources rather than overwriting.

### Added
* Authorships: populate `author.id` (OpenAlex URL) and `author.orcid`; fill `affiliations.institution_ids`.

## [2025-10-28]
### Added
* Authorships: populate `affiliations.institution_ids` in authorships.

## [2025-10-27]
### Changed
* Topics: for merged works, include topics from `merge_into_id` while keeping the original `work_id` assignment.

## [2025-10-26]
### Fixed
* Added `raw_native_type` field to `superlocations` table for all provenances (crossref, repo, datacite, mag, pubmed) to ensure proper schema alignment in UNION operations

## [2025-10-22]
### Fixed
* 160,083 new PubMed and Arxiv records ingested and linked to existing works; ongoing ingest and mapping fixed
* Adjusted `created_date` so it is stable and does not reset every time a location is added to a work

## [2025-10-21]
### Added
* Added Support for Works Magnet Institutes Moderation

### Fixed
* `issn` field added to `Work.locations.source`, ensuring all issns displayed
* 384,969 bioRxiv works matched to source ID S4306402567 in `Work.locations.source`
* `Work.locations.source` is null if no source ID is found
* PubMed host organization was set to 'PubMed' within `Work.locations.source` but has been changed to National Institutes of Health
* Remove 'mag' from `ids` object if its value is null

## [2025-10-20]
### Added
* Add roles attribute to Funders and Publishers

## [2025-10-17]
### Aded
* Add roles, repositories and associated_institutions attributes to Institutions

### Fixed

* Corrected a data type issue in the `crossref_parsed` table schema.

## [2025-10-07]

### Added

* New notebook `sync_funders.ipynb` for ad-hoc analysis.
* Updated `CreateOpenAlexWorks` to insert more Funder data.

## [2025-10-02]

### Added

* **Publishers:** Added `lineage`, `summary_stats`, and `updated_date` attributes for Elastic Search sync.
* **Funders:** Added `summary_stats`, `grants_count`, `works_count`, `cited_by_count`, `counts_by_year`, and `roles` attributes for Elastic Search sync.

## [2025-10-01]

### Added

* **Authors:** Added `topics` array (top 5) for Elastic Search sync.
* **Sources:** Added `host_organization`, `lineage`, `summary_stats`, `societies`, `alternate_titles`, `topics`, and `topics_share` attributes.

### Removed

* **Sources:** Removed the `country` attribute.

## [2025-09-25]

### Added

* New notebook `analyze_fwci_distribution.ipynb` for ad-hoc analysis.
* Utility function `get_latest_partition()` to simplify reading from time-series data.

### Changed

* The `crossref_deduplicated` table now reads from a temporary table to improve DLT graph analysis.
* Increased the number of partitions for the `work_sdg` ingestion job to 32.

### Fixed

* Corrected `work_id` assignment logic to properly use the `mid.work.MERGE_INTO_work_id` table.

## [2025-09-21]

### Fixed

* Resolved a `NULL` handling bug in the `issued_date` transformation logic.

## [2025-09-15]

### Added

* Implemented language detection for work titles.
* Added documentation explaining the biblio merge normalization algorithm.

### Changed

* Updated logic to use DOI prefixes for connecting ODi-assigned works to sources.

## [2025-08-24]

### Added

* FWCI is now calculated and available in the `works` table.
* Added new keyword generation logic.
* Concepts are now processed and included in the dataset.

### Fixed

* Resolved stability issues with `work_id` and `updated_date` assignments.
* Corrected OpenSearch sync process.

## [2025-08-13]

### Added

* Initial implementation of the `keywords` feature.

## [2025-07-16]

### Added

* Implemented a backfill for `topics` from the production environment.

### Fixed

* Resolved an issue where the abstract inverted index was not appearing in the API.

## [2025-07-07]

### Added

* Sources are now available in the API with basic counts.

## [2025-06-26]

### Fixed

* Implemented logic to merge duplicate ISSN sources.
