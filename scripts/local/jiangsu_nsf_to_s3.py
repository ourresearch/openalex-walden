#!/usr/bin/env python3
"""Natural Science Foundation of Jiangsu Province awards -> S3.

Thin runner over the shared CN-provincial framework (scripts/local/cn_provincial).

STATUS: NO-GO on the static-HTML path (2026-07-12). The Jiangsu S&T dept notice
column (kxjst.jiangsu.gov.cn col82540) publishes only guidance / application /
management notices for the 省基础研究计划(自然科学基金) -- NOT the funded-project
rosters. Since ~2024 the 拟立项公示 rosters are published on the
jsszkj.kxjst.jiangsu.gov.cn "江苏数字科技" SPA, whose project-query API
(/szjs-api/gateway/program-manage/group/project/page) is auth-gated (HTTP 401
-> SSO redirect) and whose /publicity iframe is scoped to achievement
registrations, not NSF awards. No public downloadable roster was found from a
US IP. Running this today reports "0 award-list articles match" against the
notice column, by design -- it does NOT ship rows.

F-id NOTE: the tracker row's F4320322005 is WRONG (that OpenAlex id is
"Ministry of Security and Public Administration", KR). The real Jiangsu NSF is
F4320322769 (CN; ROR 01h0zpd94; DOI 10.13039/501100004608). The config carries
the corrected id.

If a public roster channel is later identified (e.g. an unauthenticated
jsszkj comp-query export, or the dept resumes attaching PDF rosters to col82540
notices), update JIANGSU_NSF.article_pattern / listing_columns and this becomes
a live pilot with no framework changes.

Usage (same CLI contract as every other <provenance>_to_s3.py):
    python jiangsu_nsf_to_s3.py --limit 3 --skip-upload

Output (when live): s3://openalex-ingest/awards/jiangsu_nsf/jiangsu_nsf_projects.parquet
"""

from cn_provincial.common import run_province
from cn_provincial.configs import JIANGSU_NSF

if __name__ == "__main__":
    run_province(JIANGSU_NSF)
