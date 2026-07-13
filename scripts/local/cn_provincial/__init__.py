"""Shared scraper framework for Chinese provincial S&T-department award lists.

Each province is a thin config (see configs.py) plus a per-province runner
(e.g. shandong_nsf_to_s3.py) that keeps the standard <provenance>_to_s3.py CLI
contract (--limit / --skip-upload / --output-dir / --allow-shrink). All the
heavy lifting -- listing pagination, article fetch, attachment download,
xls/xlsx/pdf/docx table parsing, GBK/UTF-8 handling, checkpoint/resume, S3
upload with the runbook 1.4 shrink-check -- lives in common.py.

Design goal: a province whose pages don't fit its config fails its OWN config,
not the framework. Adding a province = one ProvinceConfig entry + a ~30-line
runner.
"""
