# Changelog

We document notable changes to the data in this file; it's in reverse chronological order (recent changes up top).

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
