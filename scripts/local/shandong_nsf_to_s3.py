#!/usr/bin/env python3
"""Natural Science Foundation of Shandong Province awards -> S3.

Thin runner over the shared CN-provincial framework (scripts/local/cn_provincial).
All machinery -- Hanweb dataproxy listing, attachment download, xls/xlsx/pdf/docx
header-driven parsing, checkpoint/resume, shrink-checked S3 upload -- lives in
cn_provincial/common.py. This file just wires the Shandong config to the standard
<provenance>_to_s3.py CLI contract.

Funder:     Natural Science Foundation of Shandong Province (F4320324174, CN)
Provenance: shandong_nsf
Source:     http://kjt.shandong.gov.cn/col/col13360/index.html  (通知公告 column)
Window:     2014-2020 public 拟立项/拟推荐 rosters (PDF/xls/xlsx/docx attachments).
            2021+ rosters moved behind the cloud.kjt.shandong.gov.cn login.
Amounts:    NOT published in any roster -> §6.7 amount waiver (grants carry
            implicit standard tiers; the announcements list title/PI/institution).
PI names:   Chinese, family-first -> full name in lead_family_name, given NULL
            (NSFC precedent).

Usage:
    python shandong_nsf_to_s3.py --limit 3 --skip-upload     # smoke test
    python shandong_nsf_to_s3.py --skip-upload               # full local build
    python shandong_nsf_to_s3.py                             # build + upload

Output: s3://openalex-ingest/awards/shandong_nsf/shandong_nsf_projects.parquet

Requirements: pip install pandas pyarrow requests openpyxl xlrd pdfplumber boto3
"""

from cn_provincial.common import run_province
from cn_provincial.configs import SHANDONG_NSF

if __name__ == "__main__":
    run_province(SHANDONG_NSF)
