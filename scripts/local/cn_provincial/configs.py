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


# ===========================================================================
# W2-A scale-up provinces (Guangdong / Shanghai / Hunan / Beijing / Hebei /
# Hubei). Appended below the pilot block; ALL_CONFIGS is extended via .update()
# at the very end so this never touches the pilot dict literal (clean merge
# with sibling agent W2-B, who appends its own block + .update()).
#
# These provinces run non-Hanweb CMSes (TRS / wdit-freemarker / gov.cn static
# publishers) whose notice columns are plain paginated static HTML, so they use
# the html_listing extension (HtmlProvinceConfig / HtmlListing) rather than the
# Hanweb dataproxy ListingColumn. Two provinces need the extras.py add-ons:
# Guangdong (Wayback recovery of publicity-expired PDFs) and Hunan (legacy .doc
# via Word COM).
# ===========================================================================

from .html_listing import HtmlListing, HtmlProvinceConfig
from . import extras


def _shanghai_scheme(title: str):
    for kw in ["杰出青年", "优秀青年", "启明星", "扬帆", "探索者", "原创",
               "面上", "青年", "重点", "重大"]:
        if kw in title:
            return kw
    return None


def _guangdong_scheme(title: str):
    # The GD roster carries a 项目类型 column (面上/杰出青年/青年提升/...), so the
    # table's own scheme wins; this title fallback only fires when it's absent.
    for kw in ["杰出青年", "卓越青年团队", "青年提升", "面上", "联合基金", "重大"]:
        if kw in title:
            return kw
    return None


# ---------------------------------------------------------------------------
# Guangdong Natural Science Foundation (GO, PARTIAL) -- F4320321921, priority 445
# ---------------------------------------------------------------------------
# gdstc.gd.gov.cn/zwgk_n/tzgg (通知公告) is a TRS static column (60 pages,
# index_N.html) going back to 2022. The 广东省基础与应用基础研究基金委员会's
# per-project 自然科学基金拟立项项目 rosters are published there as PDFs whose
# table = 序号|主管部门|项目名称|申报单位|负责人|拟立项金额(万元)|拨付金额|项目类型
# -- i.e. PI + institution + AMOUNT (万元, ×10000 CNY) + scheme. RICH.
#
# CAVEAT (documented NO-GO-ish): gdstc DELETES the roster PDF from the live
# server once the notice's 公示期 closes, leaving the filename as plain text
# with no href (confirmed: post_4796434.pdf -> live 404, article link stripped).
# The PDFs are recoverable from the Internet Archive, but Wayback coverage is
# partial: of 17 per-project roster notices, the two biggest 自然科学基金
# editions (2025年度 / 2026年度, ~3,000+ projects each, both WITH amounts) ARE
# archived; the 联合基金 editions mostly are not (see guangdong_babrf NO-GO
# note). So guangdong_nsf harvests the Wayback-archived NSF rosters only.
# extras.resolve_via_wayback handles the live-then-Wayback fallback.
GUANGDONG_NSF = HtmlProvinceConfig(
    provenance="guangdong_nsf",
    funder_id=4320321921,  # VERIFIED api.openalex.org/funders/F4320321921
    funder_display_name="Natural Science Foundation of Guangdong Province",
    base_url="http://gdstc.gd.gov.cn",
    listing_columns=[],
    html_listings=[
        HtmlListing(
            page_url=lambda n: "http://gdstc.gd.gov.cn/zwgk_n/tzgg/index.html" if n == 1
                       else f"http://gdstc.gd.gov.cn/zwgk_n/tzgg/index_{n}.html",
            max_pages=60,
            article_href=r"content/post_\d+\.html",
        ),
    ],
    # 自然科学基金 per-project rosters only (NOT 联合基金, NOT 拟安排资金 aggregate
    # tables, NOT 形式审查结果 form-review lists).
    # NSF per-project rosters, all editions: "...自然科学基金[面上/杰青/青年提升/...]
    # 拟立项项目的公示". Excludes 联合基金 (-> guangdong_babrf, NO-GO) and non-roster
    # notices.
    article_pattern=r"基金委员会.*自然科学基金.*拟立项项目的公示",
    article_exclude=r"(联合基金|形式审查|拟安排资金|资金安排|评审专家|指南|申报)",
    scheme_from_title=_guangdong_scheme,
    year_from_title=_year_from_title,
    request_pause=1.0,
    attachment_resolver=extras.resolve_via_wayback,  # live-then-Wayback recovery
)


# ---------------------------------------------------------------------------
# Shanghai -- ONE portal (stcsm.sh.gov.cn), TWO funders/parquets/notebooks:
#   - shanghai_stcsm : Science & Technology Commission of Shanghai Municipality
#                      (F4320321885, priority 449) -- the 基础研究 program.
#   - shanghai_nsf   : Natural Science Foundation of Shanghai
#                      (F4320309612, priority 451) -- the 自然科学基金 program.
# Per how-to §2.3.2 the split is by title: 自然科学基金 rosters -> shanghai_nsf;
# other STCSM basic-research rosters -> shanghai_stcsm. Two thin runners over
# these two configs keep the framework's one-config-one-parquet contract while
# satisfying "one scraper OK, two parquets/notebooks/priorities".
#
# Listing channel: the STCSM CMS (wdit/freemarker) does NOT serve paginated
# col index pages (they 404 with a freemarker template error). Instead each
# program has a curated TOPIC page under /zwgk/zfxxgkbzml/zdgz/jcyj/ that lists
# every 立项/拟立项 notice for that program. Those topic pages are the reliable
# listing (single page, max_pages=1). The 立项 notices carry PERSISTENT PDF
# attachments (立项清单.pdf) whose table = 序号|项目编号|项目名称|项目承担单位|
# 项目负责人|项目实施周期 -- award_id + title + institution + PI + period dates.
# No amount column -> §6.7 waiver. Confirmed: 2023 NSF roster = 824 projects,
# 81 pages, clean per-project rows.
SHANGHAI_NSF = HtmlProvinceConfig(
    provenance="shanghai_nsf",
    funder_id=4320309612,  # VERIFIED api.openalex.org/funders/F4320309612
    funder_display_name="Natural Science Foundation of Shanghai",
    base_url="https://stcsm.sh.gov.cn",
    listing_columns=[],
    html_listings=[
        HtmlListing(  # 上海市自然科学基金 topic page
            page_url=lambda n: "https://stcsm.sh.gov.cn/zwgk/zfxxgkbzml/zdgz/jcyj/shzrkxjj/",
            max_pages=1,
            article_href=r"/zwgk/[^\"']+\.html",
        ),
    ],
    article_pattern=r"自然科学基金.*(立项|拟立项|拟资助)",
    article_exclude=r"(申报指南|指南|征集|申报的通知|专家|变更|终止|注册|管理办法|经费)",
    year_from_title=_year_from_title,
    scheme_from_title=_shanghai_scheme,
    request_pause=1.0,
)

SHANGHAI_STCSM = HtmlProvinceConfig(
    provenance="shanghai_stcsm",
    funder_id=4320321885,  # VERIFIED api.openalex.org/funders/F4320321885
    funder_display_name="Science and Technology Commission of Shanghai Municipality",
    base_url="https://stcsm.sh.gov.cn",
    listing_columns=[],
    html_listings=[
        HtmlListing(  # 基础研究科技计划项目 topic page (non-NSF STCSM basic research)
            page_url=lambda n: "https://stcsm.sh.gov.cn/zwgk/zfxxgkbzml/zdgz/jcyj/jcyjkj/",
            max_pages=1,
            article_href=r"/zwgk/[^\"']+\.html",
        ),
    ],
    # 基础研究(领域) 立项 rosters, EXCLUDING 自然科学基金 (those go to shanghai_nsf).
    article_pattern=r"基础研究.*(立项|拟立项|拟资助)",
    article_exclude=r"(自然科学基金|申报指南|指南|征集|专家|变更|终止|管理办法|经费)",
    year_from_title=_year_from_title,
    scheme_from_title=_shanghai_scheme,
    request_pause=1.0,
)


# ---------------------------------------------------------------------------
# Hunan Natural Science Foundation (GO) -- F4320322843, priority 453
# ---------------------------------------------------------------------------
# kjt.hunan.gov.cn/kjt/xxgk/tzgg/tzgg_1 (通知公告) is a TRS static column (25
# pages, index_N.html, createPageHTML). The 立项 notices ("...立项的通知")
# carry PERSISTENT .doc attachments ("...实施目标...及立项名单.doc") on the
# article-scoped /{id}/files/{hash}.doc path (confirmed 2025 = 8.4 MB roster,
# 2026 batch 1). These are legacy binary Word (.doc, OLE) which common.py does
# NOT parse -> extras.enable_legacy_doc_parsing() (Word COM) is required and is
# switched on in the runner. Table columns vary by year but include the project
# title / PI / institution / project code. No amount -> §6.7 waiver.
HUNAN_NSF = HtmlProvinceConfig(
    provenance="hunan_nsf",
    funder_id=4320322843,  # VERIFIED api.openalex.org/funders/F4320322843
    funder_display_name="Natural Science Foundation of Hunan Province",
    base_url="https://kjt.hunan.gov.cn",
    listing_columns=[],
    html_listings=[
        HtmlListing(
            page_url=lambda n: "https://kjt.hunan.gov.cn/kjt/xxgk/tzgg/tzgg_1/index.html" if n == 1
                       else f"https://kjt.hunan.gov.cn/kjt/xxgk/tzgg/tzgg_1/index_{n}.html",
            max_pages=25,
            article_href=r"/kjt/xxgk/tzgg/tzgg_1/20\d{4}/t20\d+_\d+\.html",
        ),
    ],
    # Final 立项 rosters only (they carry the 立项名单.doc). Drop 拟立项公示
    # (those articles reference an inline清单 with no downloadable file) and the
    # frequent 变更/项目变更 notices.
    article_pattern=r"湖南省自然科学基金.*立项的通知",
    article_exclude=r"(变更|终止|指南|征集|任务书|申报|验收|拟立项)",
    attachment_ext=r"\.(docx?|xlsx?|pdf|et|wps)([?\"]|$)|/files/",
    year_from_title=_year_from_title,
    request_pause=1.0,
    # Hunan roster files live at an ARTICLE-relative path (33585991/files/<hash>.doc);
    # common.py would join them against the site root and 502. Resolve against the
    # article URL instead.
    attachment_resolver=extras.resolve_relative_to_article,
)


# ---------------------------------------------------------------------------
# NO-GO provinces (documented; retained as stubs so they can be revived if a
# public roster channel appears). Each records the exact probe evidence.
# ---------------------------------------------------------------------------
# Basic and Applied Basic Research Foundation of Guangdong (F4320337111, prio
# 447): the BABRF committee's 联合基金 per-project 拟立项项目 rosters run through
# the SAME gdstc column as guangdong_nsf, but their PDFs are removed after the
# 公示期 (same as NSF) AND -- unlike the two big NSF editions -- are almost all
# NOT archived on the Internet Archive (of the ~11 联合基金 roster notices, a
# Wayback CDX probe found only the 2024 省市联合基金 edition captured). The
# live 拟安排资金 PDFs that DO persist are institution/program-level aggregate
# funding tables (序号|项目名称(基金分类)|基金名称|省财政资金|受托管理单位) with
# NO per-project PI, so they cannot populate the awards schema. NO public
# per-project channel -> NO-GO.
#
# Beijing Natural Science Foundation (F4320322919, prio 455): kw.beijing.gov.cn
# publishes 拟资助项目 announcements, but the roster itself is NOT in them --
# they carry a .docx whose only content is a login URL
# (bjt.beijing.gov.cn/renzheng/open/login -> nsf.kw.beijing.gov.cn SSO). The
# 资助决定 notices say to query the "北京市自然科学基金依托单位工作系统" (SSO).
# Roster is behind provincial SSO -> NO-GO.
#
# Hebei Natural Science Foundation (F4320322163, prio 457): kjt.hebei.gov.cn
# 通知公告/省厅通知 (114-page TRS column) DOES carry 拟立项项目公示 notices, but
# the roster attachment ("...拟立项项目清单.doc/.docx") appears ONLY as plain
# text -- no href, no discoverable file URL (article HTML has only inline PNGs).
# Recent 评审结果 notices direct applicants to query the 河北省科技计划项目综合
# 服务平台 (https://www.hebkjt.cn) SSO. Roster behind platform SSO -> NO-GO.
#
# Hubei Natural Science Foundation (F4320322186, prio 459): kjt.hubei.gov.cn
# /kjdt/tzgg (21-page column) publishes only 申报/指南 (application/guidance)
# notices for the 自然科学基金 -- NO 立项/拟立项 rosters at all. The project
# system redirects to /egrantweb (login). No public roster channel -> NO-GO.

ALL_CONFIGS.update({
    "guangdong_nsf": GUANGDONG_NSF,
    "shanghai_nsf": SHANGHAI_NSF,
    "shanghai_stcsm": SHANGHAI_STCSM,
    "hunan_nsf": HUNAN_NSF,
})


# ===========================================================================
# W2-B group (Anhui / Tianjin / Jiangxi / Shaanxi x2 / Liaoning / Henan /
# NSSFC / NOPSS). Appended below the W2-A block; ALL_CONFIGS is extended via
# .update() at the very end so this never touches the earlier dict literals
# (clean merge with sibling agent W2-A).
#
# W2-B sources use the common.py `listing_fn` extension (additive field on
# ProvinceConfig) for three non-Hanweb listing channels:
#   - Jiangxi:  newer-Hanweb SPA whose column lists are served by a JSON
#               POST /queryList endpoint (static col JSON only carries page 1).
#   - NSSFC:    people.cn CMS (server-rendered indexN.html pagination) across
#               the 13 subcolumns of nopss.gov.cn 通知公告, PLUS a fixed list
#               of roster articles the column pages omit (the annual 年度项目
#               rosters routinely vanish from the column view / the live site;
#               attachments persist on download.people.com.cn, and removed
#               articles are recovered from the Internet Archive).
#   - Liaoning: kjt.ln.gov.cn REMOVES 拟立项公示 articles outright (live 404,
#               confirmed even for a 2026-04 公示 by 2026-07), so the listing
#               is enumerated from the Wayback CDX index of the 工作通知通告
#               column and articles+attachments are fetched from web.archive.org.
# ===========================================================================

import json as _json_w2b
import time as _time_w2b
import urllib.request as _urlreq_w2b

UA_W2B = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


def _queryList_listing(base: str, website: str, channel: str, unitid: str):
    """Listing enumerator for newer-Hanweb portals with a JSON /queryList API.

    Confirmed on kjt.jiangxi.gov.cn: POST {base}/queryList with a JSON body
    (current/unitid/webSiteCode/channelCode/pageSize/sort/order) returns
    {"data": {"total": N, "results": [{"source": {...title, pubDate, urls}}]}}.
    GET returns 404 -- it must be a JSON POST. The static col JSON
    (colNNNNN-articleList.json) only ever carries page 1 (15 records).
    """
    def fn(cfg, sess):
        headers = {"Content-Type": "application/json",
                   "Referer": f"{base}/{website}/col/{channel}/index.html",
                   "X-Requested-With": "XMLHttpRequest"}
        arts, page, total = [], 1, None
        consec_err = 0
        while True:
            body = {"current": page, "unitid": unitid, "webSiteCode": [website],
                    "channelCode": [channel], "pageSize": "50",
                    "sort": "pubDate", "order": "desc"}
            try:
                r = sess.post(base + "/queryList", data=_json_w2b.dumps(body),
                              headers=headers, timeout=40)
                d = r.json()["data"]
            except Exception as exc:
                consec_err += 1
                print(f"  queryList {channel} p{page}: EXC {exc} ({consec_err}/5)")
                if consec_err >= 5:
                    raise
                _time_w2b.sleep(5)
                continue
            consec_err = 0
            total = d["total"]
            for res in d["results"]:
                s = res["source"]
                u = ""
                try:
                    u = _json_w2b.loads(s.get("urls") or "{}").get("pc", "")
                except Exception:
                    pass
                if not u:
                    continue
                arts.append({
                    "url": (base + u) if u.startswith("/") else u,
                    "title": re.sub(r"\s+", " ", (s.get("title") or "")).strip(),
                    "date": (s.get("pubDate") or "")[:10] or None,
                })
            print(f"  queryList {channel} p{page}: cum {len(arts)}/{total}")
            if len(arts) >= total or page > 80:
                break
            page += 1
            _time_w2b.sleep(cfg.request_pause)
        return arts
    return fn


# ---------------------------------------------------------------------------
# Jiangxi (GO) -- F4320322665, priority 465
# ---------------------------------------------------------------------------
# kjt.jiangxi.gov.cn/jxskxjst is a newer-Hanweb SPA; the 通知公告 column
# (col27045, 621 articles) is enumerated via the JSON POST /queryList endpoint
# (see _queryList_listing). The 省自然科学基金 拟立项公示 notices carry docx/xlsx
# attachments (拟立项清单). Public roster window found: 2020 / 2022 / 2024 (main
# batch + 联合基金) / 2025 / 2026 -- the 公示'd categories are 重大, 创新研究群体,
# 重点, 杰青, 优青, 青年直接支持 (a few hundred projects per year); the 面上/青年
# general categories are approved inside the ywgl.kjt.jiangxi.gov.cn egrantweb
# system without a public per-project roster. NOTE: col64265 (政府信息公开公示)
# duplicates the same rosters under DIFFERENT content urls -- enumerate ONLY
# col27045 or every project would be ingested twice.
# No amounts in the rosters -> §6.7 amount waiver (implicit standard tiers).
JIANGXI_NSF = ProvinceConfig(
    provenance="jiangxi_nsf",
    funder_id=4320322665,  # VERIFIED api.openalex.org/funders/F4320322665
    funder_display_name="Natural Science Foundation of Jiangxi Province",
    base_url="https://kjt.jiangxi.gov.cn",
    listing_columns=[],
    listing_fn=_queryList_listing("https://kjt.jiangxi.gov.cn", "jxskxjst",
                                  "col27045", "366600"),
    # NSF roster titles: 自然科学基金... / 省自然科学创新研究群体... + 拟立项.
    article_pattern=r"自然科学.*拟立项",
    article_exclude=r"(受理|申报|指南|结题|验收|评审专家|答辩|征集|管理办法|中期|绩效)",
    allow_inline_table=True,   # some editions may inline the 清单 table
    year_from_title=_year_from_title,
    request_pause=1.0,
)


# ---------------------------------------------------------------------------
# Liaoning (GO via Wayback) -- F4320323086, priority 471
# ---------------------------------------------------------------------------
# kjt.ln.gov.cn (EasySite CMS) publishes annual 辽宁省自然科学基金计划 拟立项
# 公示 notices in the 工作通知通告 column, then REMOVES them: live articles 404
# even weeks after publication (2026-04-23 公示 -> 404 by 2026-07-13; 2024/2025
# editions also 404), and the column listing (31 pages, 618 articles 2016-2026)
# retains only guidance/application notices. The Internet Archive holds the
# articles AND their roster PDF attachments (confirmed: 2025 edition = 5 PDFs,
# tables 序号|项目名称|承担单位|负责人). Listing is therefore enumerated from
# the Wayback CDX index of the column's article URLs; article pages and
# attachments are fetched from web.archive.org replays.
# Rosters carry no amounts -> §6.7 waiver. 应用基础研究计划 (the 2021-2023
# successor program) and 科技计划联合计划(基金) rosters are EXCLUDED -- they are
# distinct programs that do not map to F4320323086.

def _liaoning_wayback_listing(cfg, sess):
    cdx = ("http://web.archive.org/cdx/search/cdx?url="
           "kjt.ln.gov.cn/kjt/tztg/gztz*"
           "&output=json&collapse=urlkey&filter=statuscode:200")
    data = None
    for attempt in range(5):
        try:
            req = _urlreq_w2b.Request(cdx, headers={"User-Agent": UA_W2B})
            data = _json_w2b.load(_urlreq_w2b.urlopen(req, timeout=180))
            break
        except Exception as exc:
            print(f"  wayback CDX attempt {attempt + 1}/5 failed: {exc}")
            _time_w2b.sleep(15)
    if data is None:
        raise RuntimeError("wayback CDX enumeration failed after 5 attempts")
    rows = data[1:] if data else []
    cands = [(r[1], r[2]) for r in rows if r[2].endswith("index.shtml")]
    print(f"  wayback CDX: {len(cands)} archived article captures")
    arts = []
    consec = 0
    for i, (ts, orig) in enumerate(cands, 1):
        # `id_` = raw original bytes, NO Wayback link rewriting: attachment
        # hrefs stay /kjt/attachDir/... and resolve against the LIVE origin
        # (kjt.ln.gov.cn keeps attachment files even after it deletes the
        # article page -- confirmed live 200 for the 2024/2025/2026 roster
        # PDFs while all three article pages 404).
        replay = f"http://web.archive.org/web/{ts}id_/{orig}"
        title = None
        for attempt in range(3):
            try:
                r = sess.get(replay, timeout=60)
                if r.status_code == 200:
                    m = re.search(r"<title>(.*?)</title>",
                                  r.content.decode("utf-8", "replace"), re.S)
                    title = re.sub(r"\s+", " ", m.group(1)).strip() if m else ""
                    break
            except Exception as exc:
                print(f"    [{i}] retry {attempt + 1}: {exc}")
            _time_w2b.sleep(3)
        if title is None:
            consec += 1
            print(f"    [{i}/{len(cands)}] FAILED {replay} ({consec}/8 consecutive)")
            if consec >= 8:
                raise RuntimeError("too many consecutive wayback failures")
            continue
        consec = 0
        dm = re.match(r"(\d{8})", orig.rsplit("/", 2)[-2])
        date = (f"{dm.group(1)[:4]}-{dm.group(1)[4:6]}-{dm.group(1)[6:8]}"
                if dm else None)
        arts.append({"url": replay, "title": title, "date": date})
        if i % 20 == 0:
            print(f"    [{i}/{len(cands)}] titles fetched")
        _time_w2b.sleep(cfg.request_pause)
    return arts


def _liaoning_scheme(text: str):
    for kw, label in [
        ("青年科学基金A", "青年科学基金A类（原省杰青）"),
        ("青年科学基金B", "青年科学基金B类（原省优青）"),
        ("面上", "面上项目"),
        ("博士科研启动", "博士科研启动项目"),
        ("援疆援藏", "援疆援藏医疗专项"),
        ("杰出青年", "杰出青年基金"),
        ("优秀青年", "优秀青年基金"),
        ("青年", "青年项目"),
    ]:
        if kw in text:
            return label
    return None


LIAONING_NSF = ProvinceConfig(
    provenance="liaoning_nsf",
    funder_id=4320323086,  # VERIFIED api.openalex.org/funders/F4320323086
    funder_display_name="Natural Science Foundation of Liaoning Province",
    # Articles come from Wayback `id_` replays (raw bytes, unrewritten hrefs),
    # so their relative attachment hrefs resolve against the LIVE origin --
    # kjt.ln.gov.cn keeps /kjt/attachDir/ files after deleting the articles.
    base_url="https://kjt.ln.gov.cn",
    listing_columns=[],
    listing_fn=_liaoning_wayback_listing,
    article_pattern=r"自然科学基金.*拟立项.*公示",
    article_exclude=r"(申报|指南|结题|验收|征集|应用基础研究|联合计划|软科学)",
    scheme_from_attachment=_liaoning_scheme,
    year_from_title=_year_from_title,
    request_pause=1.2,
)


# ---------------------------------------------------------------------------
# NSSFC -- National Social Science Fund of China (GO) -- F4320335869, prio 475
# ---------------------------------------------------------------------------
# NATIONAL fund (国家社科基金), NOT provincial; administered by NOPSS (the
# office). Rosters (立项名单/立项结果) are published on www.nopss.gov.cn under
# the 通知公告 subcolumns (年度项目/重大项目/专项工程/后期资助/中华学术外译/
# 单列学科/学术通俗读物), server-rendered people.cn CMS. Roster attachments are
# PDFs hosted on download.people.com.cn (tables: 序号|涉及学科|课题名称|申请人|
# 责任单位|批准号 -- award id 批准号 like 24AKS001 included); some specials are
# inline HTML tables. GOTCHAS handled by _nssfc_listing:
#   - the column pages omit some rosters entirely (2022/2023 annual editions are
#     live at direct URLs but not in any column page);
#   - the 2025 annual roster article was REMOVED from the live site (404), but
#     its Wayback capture works and its 4 PDFs are still LIVE on
#     download.people.com.cn -> fixed EXTRA list below carries these.
# No amounts published -> §6.7 amount waiver (NSSFC tiers are set per program).
# PI names: Chinese, full name in family_name, given NULL (NSFC precedent).

_NSSFC_BASE = "http://www.nopss.gov.cn"
_NSSFC_COLUMNS = ["431027", "431028", "431029", "431030", "431031", "431033",
                  "431034", "431035", "431036", "431037", "431038", "431039",
                  "459559"]
# Rosters absent from the column listings. ALL of these have been REMOVED from
# the live site (404) even though the column-listed 2024 edition survives --
# nopss purges older annual rosters. Fetched as Wayback `id_` replays (raw
# original bytes, NO link rewriting), so their attachment hrefs stay the
# ORIGINAL download.people.com.cn URLs, which are still live (verified: the
# 2025 PDFs 200 with %PDF). A rewritten (non-id_) replay serves a Wayback
# "capture not found" HTML for un-archived attachments -- confirmed failure
# mode, do not switch these back.
_NSSFC_EXTRA_ARTICLES = [
    {"url": ("http://web.archive.org/web/20240224152751id_/"
             "http://www.nopss.gov.cn/n1/2022/0930/c431027-32538160.html"),
     "title": "2022年国家社会科学基金年度项目和青年项目立项结果公布",
     "date": "2022-09-30"},
    {"url": ("http://web.archive.org/web/20250430071604id_/"
             "http://www.nopss.gov.cn/n1/2022/0930/c431027-32538158.html"),
     "title": "2022年国家社会科学基金西部项目立项结果公布",
     "date": "2022-09-30"},
    {"url": ("http://web.archive.org/web/20251214192734id_/"
             "http://www.nopss.gov.cn/n1/2023/0922/c431027-40083454.html"),
     "title": "2023年国家社会科学基金年度项目和青年项目立项结果公布",
     "date": "2023-09-22"},
    {"url": ("http://web.archive.org/web/20240917151750id_/"
             "http://www.nopss.gov.cn/n1/2023/1228/c431028-40148385.html"),
     "title": "2023年度国家社科基金重大项目立项名单公布",
     "date": "2023-12-28"},
    {"url": ("http://web.archive.org/web/20250929174944id_/"
             "http://www.nopss.gov.cn/n1/2025/0929/c431027-40574700.html"),
     "title": "2025年国家社会科学基金年度项目立项结果公布",
     "date": "2025-09-29"},
]


def _nssfc_listing(cfg, sess):
    arts = []
    for col in _NSSFC_COLUMNS:
        page = 1
        while page <= 40:
            url = (f"{_NSSFC_BASE}/GB/219469/{col}/index.html" if page == 1
                   else f"{_NSSFC_BASE}/GB/219469/{col}/index{page}.html")
            html = None
            for attempt in range(3):
                try:
                    r = sess.get(url, timeout=40)
                    if r.status_code == 200:
                        html = r.content.decode("utf-8", "replace")
                        break
                    if r.status_code == 404:
                        break
                except Exception as exc:
                    print(f"  nssfc col{col} p{page}: EXC {exc}")
                _time_w2b.sleep(3)
            if html is None:
                break
            n0 = len(arts)
            for m in re.finditer(
                    r"<li><a href='([^']+)'[^>]*>(.*?)</a>\s*<em>\[([\d\- :]+)\]</em>",
                    html):
                arts.append({
                    "url": (_NSSFC_BASE + m.group(1)
                            if m.group(1).startswith("/") else m.group(1)),
                    "title": re.sub(r"\s+", " ",
                                    re.sub(r"<[^>]+>", "", m.group(2))).strip(),
                    "date": m.group(3)[:10],
                })
            print(f"  nssfc col{col} p{page}: +{len(arts) - n0} (cum {len(arts)})")
            nm = re.search(r"href='index(\d+)\.html'\s*>下一页", html)
            if not nm:
                break
            page = int(nm.group(1))
            _time_w2b.sleep(cfg.request_pause)
    arts += _NSSFC_EXTRA_ARTICLES
    return arts


def _nssfc_scheme(text: str):
    """Program name from the attachment link text or article title."""
    for kw in ["重点项目", "一般项目", "青年项目", "西部项目", "重大项目",
               "后期资助", "中华学术外译", "冷门绝学", "文化遗产保护传承",
               "高校思想政治理论课", "艺术学", "教育学", "学术通俗读物",
               "重大历史问题研究专项", "重大专项", "重大课题",
               # LAST on purpose: only fires when no specific program matched
               # (the 2022/2023 annual 立项名单 attachments say just 年度项目).
               "年度项目"]:
        if kw in text:
            return kw
    return None


NSSFC = ProvinceConfig(
    provenance="nssfc",
    funder_id=4320335869,  # VERIFIED api.openalex.org/funders/F4320335869
    funder_display_name="National Social Science Fund of China",
    base_url=_NSSFC_BASE,
    listing_columns=[],
    listing_fn=_nssfc_listing,
    # 立项名单/立项结果 rosters only; NOT 结项 (completions), NOT 申报/招标.
    article_pattern=r"立项(名单|结果|课题)",
    article_exclude=(r"(结项|申报|招标|评审|鉴定|中标|期刊|考核|成果文库|入选"
                     r"|申请|违规|通报|撤销)"),
    allow_inline_table=True,   # some 专项 rosters are inline HTML tables
    scheme_from_attachment=_nssfc_scheme,
    scheme_from_title=_nssfc_scheme,
    year_from_title=_year_from_title,
    request_pause=1.0,
)


# ---------------------------------------------------------------------------
# W2-B NO-GO sources (documented probe evidence, 2026-07-12/13)
# ---------------------------------------------------------------------------
# Anhui NSF (F4320334897, prio 461): kjt.ah.gov.cn sits behind the Knownsec
# 创宇盾 (Jiasule) WAF with an explicit IP-REGION block -- a real browser from a
# North-American IP renders "您的IP所在区域不允许访问此网站" (your IP region is
# not allowed); curl gets the same 403 challenge page on HTTP and HTTPS. The
# 拟立项公示 articles exist (e.g. kjt.ah.gov.cn/kjzx/tzgg/123043051.html, 2025
# 第一批次) but are NOT in the Wayback Machine (availability API: no snapshots
# for the 2022/2023/2025 roster URLs -- the WAF blocks IA crawlers too). No
# accessible channel from US infrastructure -> NO-GO (needs a CN-region fetch,
# same class as the data.gov.in blocker).
#
# Tianjin NSF (F4320323993, prio 463): kxjs.tj.gov.cn hard-403s ("<html></html>",
# 13 bytes) from US IPs on both schemes, browser included. The site HAS a public
# 项目立项 aggregation column (BSFW9672/XMGL4742/XMLX1772, Wayback-archived
# 2026-01) listing "2024年天津市自然科学基金拟立项项目的公示（已过期）" -- note
# 已过期 (expired): Tianjin also expires its 公示 pages. Wayback coverage of the
# column exists but per-article/attachment coverage is unverified. NO-GO for
# this wave; candidate for a Wayback-CDX follow-up like liaoning_nsf.
#
# Shaanxi NSF (F4320324173, prio 467) + Shaanxi Key R&D (F4320336350, prio 469):
# kjt.shaanxi.gov.cn IS reachable, but a full sweep of its 通知公告 column
# (index.html + index_1..69, 1,000 notices, 2021-10 -> 2026-07) contains ZERO
# 自然科学基础研究计划 or 省重点研发计划 立项/拟立项/下达 rosters -- only
# 申报/征集/答辩/指南 notices (the only 拟立项公示 items are for the separate
# 中央引导地方科技发展资金). The dedicated 结果公示 columns (/zwfw/jg/jgcx,
# /zwfw/jg/fugs) carry lab-animal licences and 高新技术企业 lists, not program
# rosters. 拟立项 notifications go to applicants inside the SSO 陕西省科技业务
# 综合服务信息系统 (ywgl.sstrc.com/egrantweb; its public 立项情况 tab is an
# empty ajax stub). Rosters behind provincial SSO -> NO-GO (both rows).
#
# Henan NSF (F4320323845, prio 473): kjt.henan.gov.cn hard-403s from US IPs
# (edge block "PS-ATL-..." on HTTP+HTTPS, browser included). 拟立项支持项目公示
# articles exist (e.g. 2025年度 roster = 1,818 projects, 2025-01-24, mirrored by
# third-party aggregators keceyun.com / kjzch.com) and Wayback holds a Jan-2025
# crawl of kjt.henan.gov.cn article pages. NO-GO for this wave; candidate for a
# Wayback-CDX follow-up like liaoning_nsf (funder's own archived pages beat the
# aggregator mirrors).
#
# NOPSS (F4320327557, prio 477): nopss.gov.cn is the ADMINISTERING OFFICE of the
# NSSFC -- every roster on the site is a 国家社科基金 (NSSFC) program roster.
# Per §2.3.2 these projects are attributed to NSSFC (F4320335869) ONLY; shipping
# them again under NOPSS would double-ingest every project. The NOPSS tracker
# row should be marked covered-by/duplicate-of the nssfc provenance, not given
# its own parquet/notebook/priority.

ALL_CONFIGS.update({
    "jiangxi_nsf": JIANGXI_NSF,
    "liaoning_nsf": LIAONING_NSF,
    "nssfc": NSSFC,
})
