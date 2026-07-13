#!/usr/bin/env python3
"""
Natural Science Foundation of Fujian Province -> S3
===================================================

Funder:      Natural Science Foundation of Fujian Province
             OpenAlex F4320321878 (ROR null, DOI 10.13039/501100003392, CN)
Provenance:  fujian_nsf
Priority:    405
Output:      s3://openalex-ingest/awards/fujian_nsf/fujian_nsf_projects.parquet

Source
------
The Fujian Provincial Department of Science and Technology publishes an
annual "关于下达XXXX年度省自然科学基金计划项目和经费的通知" under its annual-plan
column (xxgk/ghjhlm/ndjh). Each notice carries a bulk Excel workbook with
several sheets — 省级 (provincial), 市级 (municipal), 厦门自筹 (Xiamen
self-funded), 省创新实验室 (provincial innovation labs) — of that year's newly
started NSF projects. Columns:

    序号 | 项目编号 | 项目名称 | 项目类型 | [起止年限] | 主管部门 | 承担单位 | 负责人 |
    [资助经费（万元）  or  经费（万元）→计划总数] | 备注(科目编码)

项目编号 (e.g. 2026J001083) is the native grant number cited in papers.
项目类型 (面上项目 / 重点项目 / 创青项目 / ...) is the programme (funder_scheme).
资助经费 is in 万元 -> *10000 CNY. Older years (2021/2022) put the amount under
经费（万元）with a 计划总数 sub-column. 起止年限 is "YYYY/YYYY" (start/end year).

2023 is published as a single .doc (no xlsx) and is skipped here (see report);
all other years 2021-2026 are clean xlsx reachable with a browser UA.

PI names: Chinese personal names -> whole name in family_name, given_name NULL.

Usage:
    python fujian_nsf_to_s3.py [--skip-upload] [--limit N] [--output-dir DIR]
"""
import argparse
import re
from pathlib import Path

import pandas as pd

import _cn_province_common as C

FUNDER_ID = 4320321878
PROVENANCE = "fujian_nsf"
S3_KEY = f"awards/{PROVENANCE}/{PROVENANCE}_projects.parquet"

NDJH = "https://kjt.fujian.gov.cn/xxgk/ghjhlm/ndjh"
# year -> (landing_page_url, [attachment_urls...])
NOTICES = {
    "2021": (f"{NDJH}/202110/t20211008_5701311.htm", [
        f"{NDJH}/202110/P020211008630877368367.xlsx",   # 附件1 省属
        f"{NDJH}/202110/P020211008630877794723.xlsx",   # 附件2 市属
        f"{NDJH}/202110/P020211008630878358343.xlsx",   # 附件3 厦门自筹/省创
    ]),
    "2022": (f"{NDJH}/202208/t20220803_5967582.htm", [
        f"{NDJH}/202208/P020220803405169250630.xlsx",   # 附件1 省级
        f"{NDJH}/202208/P020220803405170172952.xlsx",   # 附件2 市级
        f"{NDJH}/202208/P020220803405169761834.xlsx",   # 附件3 厦门自筹/省创
    ]),
    "2024": (f"{NDJH}/202410/t20241012_6541986.htm", [
        f"{NDJH}/202410/P020241012427994358912.xlsx",   # 附件1-4 combined
    ]),
    "2025": (f"{NDJH}/202508/t20250801_6986879.htm", [
        f"{NDJH}/202508/P020250801420299066239.xlsx",   # 附件1-4 combined
    ]),
    "2026": ("https://kjt.fujian.gov.cn/xxgk/tzgg/202605/t20260508_7145681.htm", [
        "https://kjt.fujian.gov.cn/xxgk/tzgg/202605/P020260508623119968519.xlsx",
    ]),
}


def _find_header(df):
    """Locate the header row (contains 项目名称 + 负责人). Return (idx, combined
    header where each col = header-row token + the next row's token, to catch
    the 经费（万元）→计划总数 two-row header)."""
    for i in range(min(len(df), 8)):
        row = [C.clean(x) or "" for x in df.iloc[i].tolist()]
        if "项目名称" in "".join(row) and "负责人" in "".join(row):
            nxt = [C.clean(x) or "" for x in df.iloc[i + 1].tolist()] if i + 1 < len(df) else []
            combined = []
            for j in range(len(row)):
                sub = nxt[j] if j < len(nxt) else ""
                combined.append((row[j] + " " + sub).strip())
            return i, row, combined
    return None, None, None


def _col(header, *names):
    for j, h in enumerate(header):
        for n in names:
            if n in (h or ""):
                return j
    return None


def _amount_col(header, combined):
    # prefer an explicit 资助经费 column
    j = _col(header, "资助经费")
    if j is not None:
        return j
    # else the 经费（万元） block's 计划总数 sub-column (total planned funding)
    for k, h in enumerate(combined):
        if "经费" in (h or "") and "计划总数" in (h or ""):
            return k
    # else any 经费（万元） not being 总投资/已拨/当年
    for k, h in enumerate(header):
        if h and "经费" in h and "总投资" not in h:
            return k
    return None


def _parse_year_range(v):
    s = C.clean(v)
    if not s:
        return (None, None, None, None)
    yrs = re.findall(r"(19|20)\d{2}", s)
    yrs = [int((m + rest)) for m, rest in re.findall(r"((?:19|20))(\d{2})", s)]
    if not yrs:
        return (None, None, None, None)
    sy = yrs[0]
    ey = yrs[1] if len(yrs) > 1 else None
    sd = f"{sy}-01-01" if sy else None
    ed = f"{ey}-12-31" if ey else None
    return (sd, ed, sy, ey)


def parse_workbook(path, year, landing):
    xl = pd.ExcelFile(path)
    recs = []
    for sheet in xl.sheet_names:
        df = xl.parse(sheet, header=None)
        if df.empty:
            continue
        hi, header, combined = _find_header(df)
        if hi is None:
            continue
        c_id = _col(header, "项目编号", "批准号")
        c_title = _col(header, "项目名称")
        c_type = _col(header, "项目类型")
        c_inst = _col(header, "承担单位", "依托单位", "申报单位")
        c_pi = _col(header, "负责人")
        c_date = _col(header, "起止年限", "起止时间", "执行期限")
        c_amt = _amount_col(header, combined)
        if c_title is None or c_pi is None:
            continue
        # data starts after header; skip the sub-header row if it's the 计划总数 row
        start = hi + 1
        sub = [C.clean(x) or "" for x in df.iloc[start].tolist()] if start < len(df) else []
        if any(x in ("计划总数", "已拨累计", "当年") for x in sub):
            start += 1
        for i in range(start, len(df)):
            row = df.iloc[i].tolist()
            title = C.clean(row[c_title]) if c_title < len(row) else None
            pi = C.clean(row[c_pi]) if c_pi < len(row) else None
            first = C.clean(row[0]) if len(row) else None
            if not title or (first and first in ("合计", "总计", "小计")):
                continue
            award_id = C.clean(row[c_id]) if c_id is not None and c_id < len(row) else None
            scheme = C.clean(row[c_type]) if c_type is not None and c_type < len(row) else None
            inst = C.clean(row[c_inst]) if c_inst is not None and c_inst < len(row) else None
            given, family = C.split_name(pi)
            amt = C.parse_amount_wan(row[c_amt]) if c_amt is not None and c_amt < len(row) else None
            sd = ed = sy = ey = None
            if c_date is not None and c_date < len(row):
                sd, ed, sy, ey = _parse_year_range(row[c_date])
            if not sy:
                sy = int(year)
            recs.append({
                "funder_award_id": award_id,
                "display_name": title,
                "funder_scheme": scheme,
                "institution": inst,
                "given_name": given,
                "family_name": family,
                "amount": amt if amt else None,
                "currency": "CNY" if amt else None,
                "start_date": sd,
                "end_date": ed,
                "start_year": sy,
                "end_year": ey,
                "landing_page_url": landing,
                "source_year": str(year),
            })
    print(f"    {year} [{Path(path).name}]: {len(recs):,} rows")
    return recs


def main():
    ap = argparse.ArgumentParser(description="Fujian NSF -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("./fujian_nsf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    dl = args.output_dir / "downloads"
    dl.mkdir(exist_ok=True)

    print("=" * 60 + f"\nFujian NSF -> S3  (F{FUNDER_ID}, provenance {PROVENANCE})\n" + "=" * 60)
    years = sorted(NOTICES)
    if args.limit:
        years = years[:args.limit]
    all_recs = []
    for year in years:
        landing, atts = NOTICES[year]
        for n, att in enumerate(atts):
            dest = dl / f"fujian_{year}_{n}.xlsx"
            if not dest.exists() or dest.stat().st_size < 1024:
                print(f"  downloading {year}#{n}: {att}")
                if not C.http_get(att, dest, referer=landing):
                    print(f"    [skip] {year}#{n} failed")
                    continue
            try:
                all_recs.extend(parse_workbook(dest, year, landing))
            except Exception as e:
                print(f"    [parse-fail] {year}#{n}: {e}")

    if not all_recs:
        print("  [FATAL] no rows parsed"); raise SystemExit(1)
    out = C.finalize_df(all_recs, PROVENANCE, args.output_dir,
                        f"{PROVENANCE}_projects.parquet")
    if not args.skip_upload:
        C.upload_to_s3(out, S3_KEY)
    print(f"\nDone. Next: notebooks/awards/CreateFujianNSFAwards.ipynb")


if __name__ == "__main__":
    main()
