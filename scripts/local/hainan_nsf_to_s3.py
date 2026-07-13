#!/usr/bin/env python3
"""
Natural Science Foundation of Hainan Province -> S3
===================================================

Funder:      Natural Science Foundation of Hainan Province
             OpenAlex F4320322866 (ROR https://ror.org/01h0zpd94,
             DOI 10.13039/501100004761, CN)
Provenance:  hainan_nsf
Priority:    413
Output:      s3://openalex-ingest/awards/hainan_nsf/hainan_nsf_projects.parquet

Source
------
The Hainan Provincial Department of Science and Technology publishes an
annual "关于XXXX年海南省自然科学基金项目立项的通知" (award-establishment notice)
under its information-disclosure column (xxgk/xxgkzl/xxgkml). Each notice
carries one bulk Excel attachment ``立项项目表`` (or the ``拟立项项目表``
publicity table for the year not yet finalised) with the columns:

    序号 | 批准号 | 项目名称 | 申报单位 | 负责人 | 起止时间 | 资助经费（万元）

Some years split funding across two fiscal-year columns (e.g. 2026 has
``2026年经费（万元）`` + ``2027年经费（万元）``) — we sum them. Section header rows
(``一、面上项目...``) give the programme/scheme (funder_scheme). Amounts are
万元 -> *10000 CNY. 批准号 is the native grant number cited in papers.

These attachments are reachable with a plain browser UA over HTTPS (no WAF).
The per-year attachment URLs are pinned in NOTICES below (discovered via the
portal's search + Wayback for purged publicity pages).

PI names: Chinese personal names -> whole name in family_name, given_name NULL
(NSFC/MOST convention).

Usage:
    python hainan_nsf_to_s3.py                 # download + parse + upload
    python hainan_nsf_to_s3.py --skip-upload   # local only
    python hainan_nsf_to_s3.py --limit 1       # smoke: first year only
    python hainan_nsf_to_s3.py --output-dir DIR
"""
import argparse
import re
from pathlib import Path

import pandas as pd

import _cn_province_common as C

FUNDER_ID = 4320322866
PROVENANCE = "hainan_nsf"
S3_KEY = f"awards/{PROVENANCE}/{PROVENANCE}_projects.parquet"

# year -> (landing_page_url, attachment_url).
# Final 立项通知 tables where available; 拟立项 publicity table for 2025
# (final notice carried no persistent attachment). All reachable via browser UA.
BASE = "https://dost.hainan.gov.cn/xxgk/xxgkzl/xxgkml"
NOTICES = {
    "2022": (f"{BASE}/202204/t20220421_3178515.html",
             f"{BASE}/202204/P020220421401506081488.xls"),
    "2023": (f"{BASE}/202303/t20230301_3369271.html",
             f"{BASE}/202303/P020230301555051824000.xls"),
    "2024": ("https://dost.hainan.gov.cn/xxgk/xxgkzl/xxgkml/202402/t20240208_3592950.html",
             "https://dost.hainan.gov.cn/xxgk/xxgkzl/xxgkml/202402/P020240220354878232650.xls"),
    "2025": (f"{BASE}/202503/t20250313_3832084.html",   # 拟立项 publicity (受理编号)
             f"{BASE}/202503/P020250314356040468052.xls"),
    "2026": (f"{BASE}/202603/t20260316_4043712.html",
             f"{BASE}/202603/P020260316709481869098.xls"),
}


def _find_header_row(df):
    """Locate the row that contains 项目名称 / 负责人 (the column header)."""
    for i in range(min(len(df), 12)):
        row = [C.clean(x) or "" for x in df.iloc[i].tolist()]
        joined = "".join(row)
        if "项目名称" in joined and "负责人" in joined:
            return i, row
    return None, None


def _col_index(header, *names):
    for j, h in enumerate(header):
        for n in names:
            if n in (h or ""):
                return j
    return None


def parse_year(path, year, landing):
    xl = pd.ExcelFile(path)
    recs = []
    for sheet in xl.sheet_names:
        df = xl.parse(sheet, header=None)
        if df.empty:
            continue
        hi, header = _find_header_row(df)
        if hi is None:
            continue
        c_id = _col_index(header, "批准号", "项目编号", "受理编号", "项目批准号")
        c_title = _col_index(header, "项目名称")
        c_inst = _col_index(header, "申报单位", "依托单位", "承担单位", "单位")
        c_pi = _col_index(header, "负责人", "项目负责人")
        c_date = _col_index(header, "起止时间", "起止年限", "执行期")
        # amount columns: any header containing 经费/资助 and 万元/金额
        amt_cols = [j for j, h in enumerate(header)
                    if h and re.search(r"经费|资助|金额", h)]
        if c_title is None or c_pi is None:
            continue
        scheme = None
        for i in range(hi + 1, len(df)):
            row = df.iloc[i].tolist()
            cells = [C.clean(x) for x in row]
            rest_empty = all(c is None for j, c in enumerate(cells) if j != 0)
            if C.is_section_header(cells[0], rest_empty):
                scheme = C.scheme_from_heading(cells[0])
                continue
            title = C.clean(row[c_title]) if c_title < len(row) else None
            pi = C.clean(row[c_pi]) if c_pi is not None and c_pi < len(row) else None
            # skip 合计 / total / blank rows
            first = C.clean(row[0]) if len(row) else None
            if not title or (first and first in ("合计", "总计", "小计")):
                continue
            if not pi and not title:
                continue
            award_id = C.clean(row[c_id]) if c_id is not None and c_id < len(row) else None
            inst = C.clean(row[c_inst]) if c_inst is not None and c_inst < len(row) else None
            given, family = C.split_name(pi)
            # amount: sum the (万元) columns present on the row
            amt = None
            for j in amt_cols:
                if j < len(row):
                    a = C.parse_amount_wan(row[j])
                    if a:
                        amt = (amt or 0.0) + a
            sdate = edate = None
            if c_date is not None and c_date < len(row):
                sdate, edate = C.parse_date_range(row[c_date])
            syear = C.year_from_date(sdate) or int(year)
            recs.append({
                "funder_award_id": award_id,
                "display_name": title,
                "funder_scheme": scheme,
                "institution": inst,
                "given_name": given,
                "family_name": family,
                "amount": amt if amt else None,
                "currency": "CNY" if amt else None,
                "start_date": sdate,
                "end_date": edate,
                "start_year": syear,
                "end_year": C.year_from_date(edate),
                "landing_page_url": landing,
                "source_year": str(year),
            })
    print(f"    {year}: {len(recs):,} rows")
    return recs


def main():
    ap = argparse.ArgumentParser(description="Hainan NSF -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("./hainan_nsf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="cap number of years (smoke)")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    dl = args.output_dir / "downloads"
    dl.mkdir(exist_ok=True)

    print("=" * 60 + f"\nHainan NSF -> S3  (F{FUNDER_ID}, provenance {PROVENANCE})\n" + "=" * 60)
    years = sorted(NOTICES)
    if args.limit:
        years = years[:args.limit]
    all_recs = []
    for year in years:
        landing, att = NOTICES[year]
        ext = att.rsplit(".", 1)[-1].split("?")[0]
        dest = dl / f"hainan_{year}.{ext}"
        if not dest.exists() or dest.stat().st_size < 1024:
            print(f"  downloading {year}: {att}")
            if not C.http_get(att, dest, referer=landing):
                print(f"    [skip] {year} download failed")
                continue
        try:
            all_recs.extend(parse_year(dest, year, landing))
        except Exception as e:
            print(f"    [parse-fail] {year}: {e}")

    if not all_recs:
        print("  [FATAL] no rows parsed"); raise SystemExit(1)
    out = C.finalize_df(all_recs, PROVENANCE, args.output_dir,
                        f"{PROVENANCE}_projects.parquet")
    if not args.skip_upload:
        C.upload_to_s3(out, S3_KEY)
    print(f"\nDone. Next: notebooks/awards/CreateHainanNSFAwards.ipynb")


if __name__ == "__main__":
    main()
