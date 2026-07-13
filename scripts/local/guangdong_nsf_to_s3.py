#!/usr/bin/env python3
"""Natural Science Foundation of Guangdong Province awards -> S3.

Thin runner over the shared CN-provincial framework, static-HTML variant with
the extras.py Wayback recovery add-on. Wires the GUANGDONG_NSF config to the
standard CLI contract.

Funder:     Natural Science Foundation of Guangdong Province (F4320321921, CN)
Provenance: guangdong_nsf   (priority 445)
Source:     gdstc.gd.gov.cn/zwgk_n/tzgg (通知公告) ->
            广东省基础与应用基础研究基金委员会 ...自然科学基金拟立项项目的公示.
Amounts:    PUBLISHED (拟立项金额, in 万元 / ×10,000 CNY). Roster columns are
            序号|主管部门|项目名称|申报单位|负责人|拟立项金额|拨付金额|项目类型.
            The notebook multiplies amount_raw by 10,000 and sets currency=CNY.
PI names:   Chinese, family-first -> lead_family_name, given NULL.

CAVEAT: gdstc removes the roster PDF from the live server once the 公示期 ends,
leaving the filename as plain text (no href). extras.resolve_via_wayback (wired
into the config) recovers archived copies from the Internet Archive. Wayback
coverage is PARTIAL -- the two biggest 自然科学基金 editions (2025年度 / 2026
年度, ~3,000+ projects each, both WITH amounts) are archived; other editions
may not be. Expect the harvest to cover the archived subset.

Usage:
    python guangdong_nsf_to_s3.py --limit 1 --skip-upload   # smoke test
    python guangdong_nsf_to_s3.py                           # build + upload

Output: s3://openalex-ingest/awards/guangdong_nsf/guangdong_nsf_projects.parquet
Requirements: pip install pandas pyarrow requests openpyxl xlrd pdfplumber boto3
"""

from cn_provincial.configs import GUANGDONG_NSF
from cn_provincial.html_listing import run_province_html

if __name__ == "__main__":
    run_province_html(GUANGDONG_NSF)
