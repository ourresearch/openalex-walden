# Changelog

We document notable changes to the data in this file; it's in reverse chronological order (recent changes up top).

## [2025-10-20]
### ADDED
Add roles attribute to Funders and Publishers

## [2025-10-17]
### ADDED
Add roles, repositories and associated_institutions attributes to Institutions

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
