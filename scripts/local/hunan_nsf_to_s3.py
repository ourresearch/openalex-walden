#!/usr/bin/env python3
"""Natural Science Foundation of Hunan Province awards -> S3.

Thin runner over the shared CN-provincial framework, static-HTML variant with
the extras.py legacy-.doc add-on. Wires the HUNAN_NSF config to the standard
CLI contract.

Funder:     Natural Science Foundation of Hunan Province (F4320322843, CN)
Provenance: hunan_nsf   (priority 453)
Source:     kjt.hunan.gov.cn/kjt/xxgk/tzgg/tzgg_1 (通知公告) ->
            湖南省自然科学基金 ...立项的通知 with 立项名单 .doc attachments.
Amounts:    NOT published -> §6.7 amount waiver.
PI names:   Chinese, family-first -> lead_family_name, given NULL.

The rosters are legacy binary Word (.doc, OLE), which common.py does not parse.
This runner switches on extras.enable_legacy_doc_parsing() (Word COM, Windows)
before harvesting, so the .doc tables are extracted. The harvest is therefore a
LOCAL-WINDOWS step (Word must be installed); only the resulting parquet goes to
S3/Databricks.

Usage:
    python hunan_nsf_to_s3.py --limit 1 --skip-upload   # smoke test
    python hunan_nsf_to_s3.py                           # build + upload

Output: s3://openalex-ingest/awards/hunan_nsf/hunan_nsf_projects.parquet
Requirements: pip install pandas pyarrow requests openpyxl xlrd pdfplumber boto3 pywin32
"""

from cn_provincial import extras
from cn_provincial.configs import HUNAN_NSF
from cn_provincial.html_listing import run_province_html

if __name__ == "__main__":
    extras.enable_legacy_doc_parsing()  # Word COM .doc support (Windows)
    run_province_html(HUNAN_NSF)
