#!/usr/bin/env python3
"""Science and Technology Commission of Shanghai Municipality awards -> S3.

Thin runner over the shared CN-provincial framework, static-HTML variant.
Wires the SHANGHAI_STCSM config (the STCSM 基础研究 program, i.e. the non-
自然科学基金 basic-research rosters on the same stcsm.sh.gov.cn portal) to the
standard CLI contract. Per how-to §2.3.2 the 自然科学基金 rosters go to the
sibling shanghai_nsf runner; everything else STCSM-basic-research lands here.

Funder:     Science and Technology Commission of Shanghai Municipality
            (F4320321885, CN)
Provenance: shanghai_stcsm   (priority 449)
Source:     stcsm.sh.gov.cn 基础研究科技计划项目 topic page ->
            基础研究(领域) 立项/拟立项 notices (excluding 自然科学基金) with
            立项清单 PDF attachments.
Amounts:    NOT published -> §6.7 amount waiver.
PI names:   Chinese, family-first -> lead_family_name, given NULL.

Usage:
    python shanghai_stcsm_to_s3.py --limit 2 --skip-upload   # smoke test
    python shanghai_stcsm_to_s3.py                           # build + upload

Output: s3://openalex-ingest/awards/shanghai_stcsm/shanghai_stcsm_projects.parquet
Requirements: pip install pandas pyarrow requests openpyxl xlrd pdfplumber boto3
"""

from cn_provincial.configs import SHANGHAI_STCSM
from cn_provincial.html_listing import run_province_html

if __name__ == "__main__":
    run_province_html(SHANGHAI_STCSM)
