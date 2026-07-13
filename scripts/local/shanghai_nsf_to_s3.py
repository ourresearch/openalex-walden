#!/usr/bin/env python3
"""Natural Science Foundation of Shanghai awards -> S3.

Thin runner over the shared CN-provincial framework (scripts/local/cn_provincial),
static-HTML variant. All machinery -- topic-page listing, PDF/xls/docx
header-driven parsing, checkpoint/resume, shrink-checked S3 upload -- lives in
cn_provincial/{common,html_listing}.py. This file wires the SHANGHAI_NSF config
to the standard <provenance>_to_s3.py CLI contract.

Funder:     Natural Science Foundation of Shanghai (F4320309612, CN)
Provenance: shanghai_nsf   (priority 451)
Source:     stcsm.sh.gov.cn 上海市自然科学基金 topic page ->
            自然科学基金 立项/拟立项 notices with 立项清单 PDF attachments.
Window:     2017-2023 (per-project 立项 rosters; PDFs persist on the live site).
Amounts:    NOT published in the rosters -> §6.7 amount waiver. Roster columns
            are 项目编号 / 项目名称 / 项目承担单位 / 项目负责人 / 项目实施周期.
PI names:   Chinese, family-first -> full name in lead_family_name, given NULL
            (NSFC precedent).

Companion: shanghai_stcsm_to_s3.py harvests the non-NSF STCSM 基础研究 program
from the same portal into a separate parquet/notebook/priority (§2.3.2 split).

Usage:
    python shanghai_nsf_to_s3.py --limit 2 --skip-upload   # smoke test
    python shanghai_nsf_to_s3.py --skip-upload             # full local build
    python shanghai_nsf_to_s3.py                           # build + upload

Output: s3://openalex-ingest/awards/shanghai_nsf/shanghai_nsf_projects.parquet
Requirements: pip install pandas pyarrow requests openpyxl xlrd pdfplumber boto3
"""

from cn_provincial.configs import SHANGHAI_NSF
from cn_provincial.html_listing import run_province_html

if __name__ == "__main__":
    run_province_html(SHANGHAI_NSF)
