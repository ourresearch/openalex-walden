#!/usr/bin/env python3
"""Natural Science Foundation of Jiangxi Province awards -> S3.

Thin runner over the shared CN-provincial framework (scripts/local/cn_provincial).
All machinery -- /queryList JSON listing (newer-Hanweb SPA, W2-B listing_fn
extension), attachment download, xls/xlsx/pdf/docx header-driven parsing,
checkpoint/resume, shrink-checked S3 upload -- lives in cn_provincial/common.py
+ configs.py. This file just wires the Jiangxi config to the standard
<provenance>_to_s3.py CLI contract.

Funder:     Natural Science Foundation of Jiangxi Province (F4320322665, CN)
Provenance: jiangxi_nsf
Source:     https://kjt.jiangxi.gov.cn/jxskxjst/col/col27045/index.html
            (通知公告 column, enumerated via the JSON POST /queryList API)
Window:     2020 / 2022 / 2024 (main + 联合基金) / 2025 / 2026 拟立项公示 rosters
            (docx/xlsx attachments). The 公示'd categories are 重大 / 创新研究群体 /
            重点 / 杰青 / 优青 / 青年直接支持; the general 面上/青年 categories are
            approved inside the SSO egrantweb system with no public roster.
Amounts:    NOT published in the rosters -> §6.7 amount waiver.
PI names:   Chinese, family-first -> full name in lead_family_name, given NULL
            (NSFC precedent).

Usage:
    python jiangxi_nsf_to_s3.py --limit 2 --skip-upload    # smoke test
    python jiangxi_nsf_to_s3.py --skip-upload              # full local build
    python jiangxi_nsf_to_s3.py                            # build + upload

Output: s3://openalex-ingest/awards/jiangxi_nsf/jiangxi_nsf_projects.parquet

Requirements: pip install pandas pyarrow requests openpyxl xlrd pdfplumber boto3
"""

from cn_provincial.common import run_province
from cn_provincial.configs import JIANGXI_NSF

if __name__ == "__main__":
    run_province(JIANGXI_NSF)
