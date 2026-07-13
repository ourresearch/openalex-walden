#!/usr/bin/env python3
"""Natural Science Foundation of Liaoning Province awards -> S3 (via Wayback).

Thin runner over the shared CN-provincial framework (scripts/local/cn_provincial).
kjt.ln.gov.cn REMOVES its 拟立项公示 articles from the live site (they 404 even
weeks after publication and are absent from the column listing), so the listing
is enumerated from the Wayback CDX index of the 工作通知通告 column and article
pages + roster PDF attachments are fetched from web.archive.org replays (W2-B
listing_fn extension; see cn_provincial/configs.py LIAONING_NSF for evidence).

Funder:     Natural Science Foundation of Liaoning Province (F4320323086, CN)
Provenance: liaoning_nsf
Source:     https://kjt.ln.gov.cn/kjt/tztg/gztz/ (工作通知通告 column) via
            http://web.archive.org/cdx/search/cdx?url=kjt.ln.gov.cn/kjt/tztg/gztz*
Window:     Wayback-archived 拟立项公示 editions (2024 / 2025 / 2026 confirmed
            captured; PDF tables 序号|项目名称|承担单位|负责人). 应用基础研究计划
            (2021-2023 successor program) and 联合计划(基金) are EXCLUDED --
            distinct programs that do not map to F4320323086.
Amounts:    NOT published in the rosters -> §6.7 amount waiver.
PI names:   Chinese, family-first -> full name in lead_family_name, given NULL
            (NSFC precedent).

Usage:
    python liaoning_nsf_to_s3.py --limit 1 --skip-upload   # smoke test
    python liaoning_nsf_to_s3.py --skip-upload             # full local build
    python liaoning_nsf_to_s3.py                           # build + upload

Output: s3://openalex-ingest/awards/liaoning_nsf/liaoning_nsf_projects.parquet

Requirements: pip install pandas pyarrow requests openpyxl xlrd pdfplumber boto3
"""

from cn_provincial.common import run_province
from cn_provincial.configs import LIAONING_NSF

if __name__ == "__main__":
    run_province(LIAONING_NSF)
