"""Per-province configs for the CN provincial award-list framework.

Each entry is one ProvinceConfig. Adding a province is (usually) just:
  1. Find the S&T dept's Hanweb notice column -- open the col/colNNNNN page,
     grep the HTML for `dataproxy.jsp?...columnid=...&unitid=...&webid=...`,
     url-decode `webname`.
  2. Verify the OpenAlex funder id via api.openalex.org/funders/FXXXX.
  3. Write the article_pattern that selects the province's award-list notices.
The framework's header-keyword detection handles the varied attachment layouts.
"""

from __future__ import annotations

import re

from .common import ListingColumn, ProvinceConfig


def _year_from_title(title: str):
    m = re.search(r"(20\d{2})\s*年", title)
    return int(m.group(1)) if m else None


def _shandong_scheme(title: str):
    # The batch type lives in the announcement title, not always in the table.
    for kw, label in [
        ("联合基金", "联合基金"),
        ("重大基础研究", "重大基础研究项目"),
        ("集中申报", "面上/青年项目"),
        ("转化医学", "转化医学专题"),
        ("优秀青年", "优秀青年人才联合基金"),
    ]:
        if kw in title:
            return label
    return None


# ---------------------------------------------------------------------------
# Shandong (PILOT, LIVE) -- F4320324174, priority 439
# ---------------------------------------------------------------------------
# kjt.shandong.gov.cn col13360 (通知公告) serves 3,500 notices back to 2010 via
# the Hanweb dataproxy. Award-result lists 2014-2020 carry public PDF/xls/docx
# attachments (项目名称 / 申报者 / 依托单位 [/ 项目类别]). 2021+ batches moved the
# roster behind the cloud.kjt.shandong.gov.cn login (no public attachment), so
# the harvestable public window is 2014-2020. No amounts are published in any
# attachment -> §6.7 amount waiver applies (grants have implicit standard tiers).
SHANDONG_NSF = ProvinceConfig(
    provenance="shandong_nsf",
    funder_id=4320324174,
    funder_display_name="Natural Science Foundation of Shandong Province",
    base_url="http://kjt.shandong.gov.cn",
    listing_columns=[
        ListingColumn(
            referer="http://kjt.shandong.gov.cn/col/col13360/index.html",
            columnid="13360", unitid="734277", webid="73",
            webname="山东省科学技术厅",
            path="http://kjt.shandong.gov.cn/",
            perpage=15, stride=45, needs_cookie_prime=False,
        ),
    ],
    # Award ROSTER lists only: 自然科学基金 + one of the "results/list" markers.
    article_pattern=r"自然科学基金.*(拟立项|拟推荐项目|立项资助结果|拟立项目|推荐项目公示|推荐项目目录)",
    # Drop expert-panel / defense-score / application-guide notices (no roster).
    article_exclude=r"(评审专家|答辩评审专家|答辩评审成绩|学科组评审专家|申报通知|申报的通知|申报工作|指南|形式审查|网评结果|网络评审|验收|结题|执行情况|注册|管理办法|经费管理)",
    scheme_from_title=_shandong_scheme,
    year_from_title=_year_from_title,
    request_pause=0.8,
)


# ---------------------------------------------------------------------------
# Jiangsu (PILOT, NO-GO for static-HTML path) -- real F-id F4320322769
# ---------------------------------------------------------------------------
# NOTE: the tracker's F4320322005 is WRONG (that id is a Korean ministry). The
# real OpenAlex funder is F4320322769 (ROR 01h0zpd94, DOI 10.13039/501100004608).
# The Jiangsu S&T dept notice column (kxjst.jiangsu.gov.cn col82540) publishes
# only guidance / application / management notices for the 省基础研究计划
# (自然科学基金) -- NOT the funded-project rosters. Since ~2024 the 拟立项公示
# rosters are published on the jsszkj.kxjst.jiangsu.gov.cn "江苏数字科技" SPA,
# whose project-query API (/szjs-api/gateway/program-manage/group/project/page)
# is auth-gated (HTTP 401 -> SSO redirect) and whose /publicity iframe is for
# achievement registrations, not NSF awards. No public downloadable roster was
# found from a US IP. This config is retained as a stub so the province can be
# revived if a public roster channel is identified; run_province will report
# "0 award-list articles match" against the notice column, by design.
JIANGSU_NSF = ProvinceConfig(
    provenance="jiangsu_nsf",
    funder_id=4320322769,  # VERIFIED via api.openalex.org/funders/F4320322769
    funder_display_name="Natural Science Foundation of Jiangsu Province",
    base_url="https://kxjst.jiangsu.gov.cn",
    listing_columns=[
        ListingColumn(
            referer="https://kxjst.jiangsu.gov.cn/col/col82540/index.html",
            columnid="82540", unitid="345950", webid="89",
            webname="江苏省科技厅",
            path="/", perpage=15, stride=45, needs_cookie_prime=True,
        ),
    ],
    article_pattern=r"(基础研究计划|自然科学基金).*(拟立项|立项的通知|拟立项目|下达)",
    article_exclude=r"(申报|指南|管理办法|中期检查|验收|结题|评审|专家|征集|修订)",
    year_from_title=_year_from_title,
    request_pause=1.0,
)


ALL_CONFIGS = {
    "shandong_nsf": SHANDONG_NSF,
    "jiangsu_nsf": JIANGSU_NSF,
}
