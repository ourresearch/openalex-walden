#!/usr/bin/env python3
"""National Social Science Fund of China (国家社科基金) awards -> S3.

Thin runner over the shared CN-provincial framework (scripts/local/cn_provincial).
NATIONAL funder (not provincial) -- the framework's header-keyword table parser
applies unchanged to the NSSFC roster PDFs. Listing is a custom people.cn-CMS
column crawler (W2-B listing_fn extension) over the 13 subcolumns of the
nopss.gov.cn 通知公告 section, plus a fixed list of roster articles the column
pages omit (see cn_provincial/configs.py NSSFC for evidence).

Funder:     National Social Science Fund of China (F4320335869, CN)
Provenance: nssfc
Source:     http://www.nopss.gov.cn/GB/219469/ (通知公告: 年度项目 / 重大项目 /
            专项工程 / 后期资助 / 中华学术外译 / 单列学科 / 学术通俗读物 columns)
Window:     立项名单/立项结果 rosters, 2022-2026 (annual 年度项目 editions =
            重点/一般/青年/西部 PDFs on download.people.com.cn, ~4,500-5,000
            projects/yr; plus 重大项目, 专项, 后期资助, 艺术学/教育学, 外译,
            通俗读物 editions). PDF tables: 序号|涉及学科|课题名称|申请人|责任单位|
            批准号 -- the 批准号 (e.g. 24AKS001) ships as funder_award_id.
Attribution: everything published here is a 国家社科基金 program -> attributed
            to NSSFC (F4320335869) ONLY. NOPSS (F4320327557) is the administering
            office; do NOT double-ship these projects under NOPSS.
Amounts:    NOT published in the rosters -> §6.7 amount waiver (program tiers).
PI names:   Chinese, family-first -> full name in lead_family_name, given NULL
            (NSFC precedent).

Usage:
    python nssfc_to_s3.py --limit 2 --skip-upload    # smoke test
    python nssfc_to_s3.py --skip-upload              # full local build
    python nssfc_to_s3.py                            # build + upload

Output: s3://openalex-ingest/awards/nssfc/nssfc_projects.parquet

Requirements: pip install pandas pyarrow requests openpyxl xlrd pdfplumber boto3
"""

from cn_provincial.common import run_province
from cn_provincial.configs import NSSFC

if __name__ == "__main__":
    run_province(NSSFC)
