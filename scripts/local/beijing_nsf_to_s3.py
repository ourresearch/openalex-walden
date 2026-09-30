#!/usr/bin/env python3
"""Beijing Municipal Natural Science Foundation (北京市自然科学基金) awards -> S3.

Funder:     Beijing Municipal Natural Science Foundation (F4320334977, CN)
            TWIN: F4320322919 "Natural Science Foundation of Beijing Municipality"
            (same body; coordinator assigned F4320334977 for #1451 -- flagged).
Provenance: beijing_nsf   (priority 523)
Portal:     kw.beijing.gov.cn (shared with BMSTC -> see beijing_bmstc_to_s3.py;
            only 北京市自然科学基金委员会办公室 (基金办) rosters are used here).

Why only 2017-2023: since 2024 the 基金办 publishes 拟资助/资助决定 notices whose
roster attachment is only a login URL (bjt.beijing.gov.cn SSO ->
nsf.kw.beijing.gov.cn). The rosters below are every public per-project list found
(live site + Internet Archive raw captures):

  * 2022 资助项目 xlsx (政务服务/办事结果, all 2022 schemes, no 资助编号/amount)
  * 2022 资助决定 docx: 杰出青年 (JQ22xxx), 重点研究专题 (Z22xxxx), 小米联合 (L223xxx)
  * 2023 资助决定 .doc: 面上/青年 (+ M23xxx), with 资助编号 + amount
  * 2023 拟资助 docx: 杰出青年 / 重点研究专题 / 小米联合 (final 资助决定 has no list)
  * 2023 拟资助 .doc (Wayback): 海淀原始创新 / 丰台轨道交通 / 昌平创新 联合基金
  * 2023 京津冀基础研究合作专项 拟资助 docx (Wayback; 基金办-issued J23xxxx ids)
  * 2020 第二批验收项目清单 xlsx (Wayback): completed 2017-2018 面上/青年 projects

funder_award_id: the published 资助编号 (e.g. 7202106, JQ22001, L233001) -- exactly
the form citing works use (works?filter=grants.funder:F4320334977 shows 7164309,
L243009, 4232058 ...). Rows with no published number (most of the 2022 xlsx) get a
stable synthetic key "BJNSF-<sha1(title|institution)[:12]>".
Amounts:    万元 -> CNY (×10,000) where published.
PI names:   Chinese, family-first -> lead_family_name (full name), given NULL.
Filter:     all rows are research grants (no subsidies on these rosters).

Usage:
    py -3.13 beijing_nsf_to_s3.py --limit 2 --skip-upload   # smoke (first 2 sources)
    py -3.13 beijing_nsf_to_s3.py                           # build + upload

Output: s3://openalex-ingest/awards/beijing_nsf/beijing_nsf_projects.parquet
Requirements: pip install pandas pyarrow requests python-docx openpyxl lxml boto3 pywin32
"""

import argparse
import re
from pathlib import Path

import pandas as pd

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22; TWCF variant) ---
# (grep marker for §4.0: sys.stdout.reconfigure)
import sys as _sys_utf8
try:
    _sys_utf8.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    _sys_utf8.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if _sys_utf8.platform == "win32":
    import builtins as _builtins_utf8
    import pathlib as _pathlib_utf8

    _orig_wt = _pathlib_utf8.Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_wt(self, data, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.write_text = _wt

    _orig_rt = _pathlib_utf8.Path.read_text
    def _rt(self, encoding=None, errors=None, newline=None):
        return _orig_rt(self, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.read_text = _rt

    _orig_open = _builtins_utf8.open
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---

_sys_utf8.path.insert(0, str(Path(__file__).resolve().parent))
import beijing_kw_common as B  # noqa: E402

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/beijing_nsf/beijing_nsf_projects.parquet"
TZGG = "https://kw.beijing.gov.cn/zwgk/tzgg/"
ATTACH = "https://kw.beijing.gov.cn/attach/0/"

# (url, kind, wayback_ts, year, stage, scheme_hint, notice_url)
# Stage rank: 资助决定/资助项目 (final) > 验收 (completed) > 拟资助 (proposed).
SOURCES = [
    ("https://kw.beijing.gov.cn/zwfw/bsjg/202307/P020240909007898263058.xlsx", "xlsx", None, 2022,
     "资助项目", None, "https://kw.beijing.gov.cn/zwfw/bsjg/202307/t20230711_3796602.html"),
    (TZGG + "202209/P020240919615247116004.docx", "docx", None, 2022, "资助决定", "杰出青年科学基金",
     TZGG + "202209/t20220923_3900456.html"),
    (TZGG + "202209/P020240919615247193153.docx", "docx", None, 2022, "资助决定", "重点研究专题",
     TZGG + "202209/t20220923_3900456.html"),
    (TZGG + "202209/P020240919615247268190.docx", "docx", None, 2022, "资助决定", "小米创新联合基金",
     TZGG + "202209/t20220923_3900456.html"),
    (TZGG + "202304/P020240919618520394745.doc", "doc", None, 2023, "资助决定", "面上及青年科学基金",
     TZGG + "202304/t20230407_3900655.html"),
    (TZGG + "202308/P020240919618915061151.docx", "docx", None, 2023, "拟资助", "杰出青年科学基金",
     TZGG + "202308/t20230821_3900828.html"),
    (TZGG + "202308/P020240919618915134063.docx", "docx", None, 2023, "拟资助", "重点研究专题",
     TZGG + "202308/t20230821_3900828.html"),
    (TZGG + "202308/P020240919618915172931.docx", "docx", None, 2023, "拟资助", "小米创新联合基金",
     TZGG + "202308/t20230821_3900828.html"),
    (ATTACH + "1.2023%E5%B9%B4%E5%BA%A6%E5%8C%97%E4%BA%AC%E5%B8%82%E8%87%AA%E7%84%B6%E7%A7%91%E5%AD%A6%E5%9F%BA%E9%87%91-%E6%B5%B7%E6%B7%80%E5%8E%9F%E5%A7%8B%E5%88%9B%E6%96%B0%E8%81%94%E5%90%88%E5%9F%BA%E9%87%91%E6%8B%9F%E8%B5%84%E5%8A%A9%E9%A1%B9%E7%9B%AE%E5%90%8D%E5%8D%95.doc",
     "doc", "20231112094313", 2023, "拟资助", "海淀原始创新联合基金", None),
    (ATTACH + "2.2023%E5%B9%B4%E5%BA%A6%E5%8C%97%E4%BA%AC%E5%B8%82%E8%87%AA%E7%84%B6%E7%A7%91%E5%AD%A6%E5%9F%BA%E9%87%91-%E4%B8%B0%E5%8F%B0%E8%BD%A8%E9%81%93%E4%BA%A4%E9%80%9A%E5%89%8D%E6%B2%BF%E7%A0%94%E7%A9%B6%E8%81%94%E5%90%88%E5%9F%BA%E9%87%91%E6%8B%9F%E8%B5%84%E5%8A%A9%E9%A1%B9%E7%9B%AE%E5%90%8D%E5%8D%95.doc",
     "doc", "20231112044122", 2023, "拟资助", "丰台轨道交通前沿研究联合基金", None),
    (ATTACH + "3.2023%E5%B9%B4%E5%BA%A6%E5%8C%97%E4%BA%AC%E5%B8%82%E8%87%AA%E7%84%B6%E7%A7%91%E5%AD%A6%E5%9F%BA%E9%87%91-%E6%98%8C%E5%B9%B3%E5%88%9B%E6%96%B0%E8%81%94%E5%90%88%E5%9F%BA%E9%87%91%E6%8B%9F%E8%B5%84%E5%8A%A9%E9%A1%B9%E7%9B%AE%E5%90%8D%E5%8D%95.doc",
     "doc", "20231112042948", 2023, "拟资助", "昌平创新联合基金", None),
    (ATTACH + "1.2023%E5%B9%B4%E5%BA%A6%E4%BA%AC%E6%B4%A5%E5%86%80%E5%9F%BA%E7%A1%80%E7%A0%94%E7%A9%B6%E5%90%88%E4%BD%9C%E4%B8%93%E9%A1%B9%E6%8B%9F%E8%B5%84%E5%8A%A9%E9%A1%B9%E7%9B%AE%E5%90%8D%E5%8D%95.docx",
     "docx", "20231119075059", 2023, "拟资助", "京津冀基础研究合作专项", None),
    (ATTACH + "1.%E5%8C%97%E4%BA%AC%E8%87%AA%E7%84%B6%E7%A7%91%E5%AD%A6%E5%9F%BA%E9%87%912020%E5%B9%B4%E7%AC%AC%E4%BA%8C%E6%89%B9%E9%AA%8C%E6%94%B6%E9%A1%B9%E7%9B%AE%E6%B8%85%E5%8D%95.xlsx",
     "xlsx", "20240609212842", None, "验收", None, None),
]
STAGE_RANK = {"资助决定": 3, "资助项目": 3, "验收": 2, "拟资助": 1}

# 资助编号 prefix -> scheme (fallback when neither the table nor a section row says)
PREFIX_SCHEME = [(r"^JQ", "杰出青年科学基金"), (r"^Z\d", "重点研究专题"), (r"^J\d", "京津冀基础研究合作专项"),
                 (r"^L\d", "联合基金"), (r"^M\d", "专项项目")]


def award_year(award_id: str, fallback):
    """7-digit 资助编号 = discipline(1) + yy(2) + type(1) + serial(3); JQ22/Z22/L22/J23/M23 carry yy too."""
    if award_id and re.fullmatch(r"\d{7}", award_id):
        return 2000 + int(award_id[1:3])
    m = re.match(r"^(?:JQ|[ZLJM])(\d{2})", award_id or "")
    if m:
        return 2000 + int(m.group(1))
    return fallback


def build(limit=None, work_dir=Path("/tmp/beijing_nsf")) -> pd.DataFrame:
    recs = []
    sources = SOURCES[:limit] if limit else SOURCES
    for url, kind, ts, year, stage, hint, notice in sources:
        B.log(f"[{stage} {year or ''}] {kind} {'wayback ' + ts if ts else 'live'} {url[-70:]}")
        content = B.fetch(url, wayback_ts=ts, pause=3.0 if ts else 1.0)
        _, tables = B.load_tables(kind, content, work_dir)
        rows = B.rows_from_tables(tables)
        if not rows:
            raise RuntimeError(f"no roster rows parsed from {url} -- layout changed?")
        B.log(f"    {len(rows)} rows")
        for r in rows:
            title = r.get("title", "").strip()
            if not title:
                continue
            aid = re.sub(r"\s+", "", r.get("funder_award_id", "") or "")
            aid = aid if re.fullmatch(r"[A-Z]{0,2}\d{5,8}", aid) else ""
            scheme = r.get("scheme") or ""
            section = r.get("section") or ""
            if not scheme and section and section not in ("企业联合资助项目",):
                scheme = section
            if not scheme and hint and hint != "面上及青年科学基金":
                scheme = hint
            if not scheme:
                scheme = next((s for p, s in PREFIX_SCHEME if re.match(p, aid)), None) or hint or ""
            fy = r.get("funding_year", "")
            y = award_year(aid, int(fy) if fy.isdigit() else year)
            start, end = B.parse_period(r.get("period", ""))
            recs.append({
                "funder_award_id": aid or None,
                "display_name": title,
                "institution": B.first_org(r.get("institution", "")) or None,
                "lead_family_name": B.first_person(r.get("pi", "")) or None,
                "lead_title": r.get("pi_title") or None,
                "discipline": r.get("discipline") or None,
                "funder_scheme": scheme or None,
                "amount_raw_wan": r.get("amount_raw") or None,
                "amount": B.wan_to_cny(r.get("amount_raw", "")),
                "currency": "CNY" if B.wan_to_cny(r.get("amount_raw", "")) else None,
                "start_year": str(y) if y else None,
                "start_date": start or (f"{y}-01-01" if y else None),
                "end_date": end,
                "stage": stage,
                "source_url": url if not ts else f"https://web.archive.org/web/{ts}/{url}",
                "landing_page_url": notice or (f"https://web.archive.org/web/{ts}/{url}" if ts else url),
            })
    df = pd.DataFrame(recs)
    B.log(f"raw rows {len(df)}")

    # Attach published 资助编号/amount to id-less rows of the same project (title+institution),
    # then dedupe: one row per award, best stage first, rows with id/amount preferred.
    df["_k"] = (df["display_name"].str.replace(r"\s+", "", regex=True) + "|" +
                df["institution"].fillna("").str.replace(r"\s+", "", regex=True))
    ids = df.dropna(subset=["funder_award_id"]).drop_duplicates("_k").set_index("_k")
    for col in ("funder_award_id", "amount", "amount_raw_wan", "currency", "funder_scheme"):
        fill = df["_k"].map(ids[col])
        df[col] = df[col].where(df[col].notna(), fill)
    df["_rank"] = df["stage"].map(STAGE_RANK).fillna(0)
    df["_has"] = df["funder_award_id"].notna().astype(int) * 2 + df["amount"].notna().astype(int)
    df = df.sort_values(["_rank", "_has"], ascending=False)
    df["_dk"] = df["funder_award_id"].fillna("K:" + df["_k"])
    df = df.drop_duplicates("_dk").drop_duplicates("_k")
    df["funder_award_id"] = df["funder_award_id"].where(
        df["funder_award_id"].notna(),
        [B.synthetic_key("BJNSF", t, i or "") for t, i in zip(df["display_name"], df["institution"])])
    df = df.drop(columns=["_k", "_rank", "_has", "_dk"]).reset_index(drop=True)
    return df


def main() -> None:
    p = argparse.ArgumentParser(description="Beijing NSF rosters -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N source documents (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    B.CACHE_DIR = args.output_dir / "_cache_beijing_nsf"

    df = build(args.limit, args.output_dir / "beijing_nsf_work")
    B.log(f"final awards {len(df)}")
    B.log(f"  real 资助编号: {(~df['funder_award_id'].str.startswith('BJNSF-')).sum()}")
    B.log(f"  with amount:  {df['amount'].notna().sum()}   with PI: {df['lead_family_name'].notna().sum()}")
    B.log(f"  by year: {df['start_year'].value_counts().sort_index().to_dict()}")
    B.log(f"  by scheme: {df['funder_scheme'].value_counts().head(15).to_dict()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    df = df.astype("string")
    parquet_path = args.output_dir / "beijing_nsf_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    B.log(f"Wrote {len(df)} rows to {parquet_path}")
    if args.skip_upload:
        return
    B.upload_with_shrink_guard(df, parquet_path, S3_BUCKET, S3_KEY, args.allow_shrink)


if __name__ == "__main__":
    main()
