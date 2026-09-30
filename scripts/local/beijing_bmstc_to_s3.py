#!/usr/bin/env python3
"""Beijing Municipal Science and Technology Commission (BMSTC, 北京市科学技术委员会)
plan-project awards -> S3.

Funder:     Beijing Municipal Science and Technology Commission (F4320325902, CN)
            (related programme row: F4320335843 "Beijing Science and Technology
            Planning Project" -- flagged, not used)
Provenance: beijing_bmstc   (priority 510)
Portal:     kw.beijing.gov.cn (shared with the Beijing NSF -> beijing_nsf_to_s3.py).
            Per how-to §2.3.2 this ingest takes ONLY the 市科委/中关村管委会
            "项目(课题)立项(项目)公开清单" -- the commission's own quarterly
            disclosure of newly approved plan-project 课题. 北京市自然科学基金 rosters
            (基金办) are NOT here; they go to beijing_nsf.

Source lists (inline HTML tables, one row per 课题):
    序号 | 项目名称 (programme) | 课题名称 | 课题承担单位 | 课题负责人 | 主管处室 |
    市财政经费(万元) | 课题实施周期
  * live:    zwgk/tzgg "…季度项目(课题)立项公开清单", discovered by crawling the
             通知公告 column (2022 Q2 - 2023 Q1; the series stopped after 2023 Q1)
  * archive: old Hanweb site art_736_* pages (2019-2021 quarters) via raw Internet
             Archive captures (NSSFC 475 precedent), listed in WAYBACK below.

funder_award_id: BMSTC publishes no 课题 number on these lists (papers cite
Z-numbers such as Z181100001918023 that appear nowhere public), so each 课题 gets a
stable synthetic key "BMSTC-<sha1(课题名称|承担单位)[:12]>". It collapses onto 0
existing citation stubs (reported in the #1451 result).
Amounts:    市财政经费 万元 -> CNY (×10,000).
Dates:      课题实施周期 "2023年03月至2025年03月" -> start/end dates.
PI names:   Chinese, family-first -> lead_family_name (full name), given NULL.
Filter:     research 课题 only -- programmes that are financial-service / subsidy
            lines (科技金融, 补贴, 保险) are excluded (NON_RESEARCH below).

Usage:
    py -3.13 beijing_bmstc_to_s3.py --limit 1 --skip-upload   # smoke (first list only)
    py -3.13 beijing_bmstc_to_s3.py                           # build + upload

Output: s3://openalex-ingest/awards/beijing_bmstc/beijing_bmstc_projects.parquet
Requirements: pip install pandas pyarrow requests lxml boto3
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
S3_KEY = "awards/beijing_bmstc/beijing_bmstc_projects.parquet"
TZGG = "https://kw.beijing.gov.cn/zwgk/tzgg/"
TZGG_MAX_PAGES = 60
TITLE_RE = re.compile(r"季度项目[(（]课题[)）]立项(项目)?公开清单")
NON_RESEARCH = re.compile(r"科技金融|补贴|保险")

# Old-site quarterly lists that survive only on the Internet Archive:
# (capture timestamp, original URL, title)
WAYBACK = [
    ("20200119025351", "http://kw.beijing.gov.cn:80/art/2020/1/16/art_736_472080.html",
     "2019年度市科委第四季度项目(课题)立项项目公开清单"),
    ("20200407153707", "http://kw.beijing.gov.cn:80/art/2020/3/31/art_736_513582.html",
     "2020年度市科委第一季度项目(课题)立项项目公开清单"),
    ("20200807031548", "http://kw.beijing.gov.cn:80/art/2020/7/30/art_736_526834.html",
     "2020年度市科委第二季度项目(课题)立项项目公开清单"),
]


def discover_live() -> list[tuple[str, str]]:
    """Crawl the 通知公告 column; return [(article_url, title)] of quarterly 立项 lists."""
    found, empty_streak = [], 0
    for n in range(TZGG_MAX_PAGES):
        url = TZGG + ("index.html" if n == 0 else f"index_{n}.html")
        try:
            html = B.fetch(url, pause=0.5, cache=False).decode("utf-8", errors="replace")
        except RuntimeError:
            # past the last page the column 404s; tolerate a gap, stop after two misses
            empty_streak += 1
            if empty_streak >= 2:
                break
            continue
        empty_streak = 0
        for href, t1, t2 in re.findall(
                r'href="([^"]*t\d{8}_\d+\.html)"[^>]*?(?:title="([^"]+)")?[^>]*>\s*([^<]*)<', html):
            title = (t1 or t2).strip()
            if TITLE_RE.search(title):
                art = href if href.startswith("http") else TZGG + href.lstrip("./")
                found.append((art, title))
    return list(dict.fromkeys(found))


def build(limit=None) -> pd.DataFrame:
    sources = [(u, None, t) for u, t in discover_live()] + [(u, ts, t) for ts, u, t in WAYBACK]
    B.log(f"{len(sources)} quarterly 立项公开清单 lists ({len(WAYBACK)} from Wayback)")
    if not sources:
        raise RuntimeError("no quarterly lists discovered -- column layout changed?")
    if limit:
        sources = sources[:limit]
    recs, dropped = [], 0
    for url, ts, title in sources:
        m = re.search(r"(20\d{2})年度", title)
        list_year = int(m.group(1)) if m else None
        content = B.fetch(url, wayback_ts=ts, pause=3.0 if ts else 1.0)
        _, tables = B.tables_from_html(content)
        rows = B.rows_from_tables(tables)
        B.log(f"  {title}: {len(rows)} rows ({'wayback ' + ts if ts else 'live'})")
        if not rows:
            raise RuntimeError(f"no rows parsed from {url}")
        for r in rows:
            prog = r.get("programme", "")
            if NON_RESEARCH.search(prog):
                dropped += 1
                continue
            start, end = B.parse_period(r.get("period", ""))
            y = int(start[:4]) if start else list_year
            amt = B.wan_to_cny(r.get("amount_raw", ""))
            recs.append({
                "display_name": r["title"].strip(),
                "programme": prog or None,
                "institution": B.first_org(r.get("institution", "")) or None,
                "institutions_all": r.get("institution") or None,
                "lead_family_name": B.first_person(r.get("pi", "")) or None,
                "office": r.get("office") or None,
                "amount_raw_wan": r.get("amount_raw") or None,
                "amount": amt,
                "currency": "CNY" if amt else None,
                "start_date": start,
                "end_date": end,
                "start_year": str(y) if y else None,
                "end_year": end[:4] if end else None,
                "list_title": title,
                "landing_page_url": f"https://web.archive.org/web/{ts}/{url}" if ts else url,
            })
    B.log(f"excluded {dropped} non-research (金融/补贴/保险) rows")
    df = pd.DataFrame(recs)
    df["funder_award_id"] = [B.synthetic_key("BMSTC", t, i or "")
                             for t, i in zip(df["display_name"], df["institution"])]
    # The same 课题 can re-appear in a later quarter (re-approval / batch rollover): keep one.
    df = df.drop_duplicates("funder_award_id").reset_index(drop=True)
    return df


def main() -> None:
    p = argparse.ArgumentParser(description="BMSTC quarterly 立项公开清单 -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N lists (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    B.CACHE_DIR = args.output_dir / "_cache_beijing_bmstc"

    df = build(args.limit)
    B.log(f"final 课题 {len(df)}; with amount {df['amount'].notna().sum()}; "
          f"with PI {df['lead_family_name'].notna().sum()}")
    B.log(f"  by start_year: {df['start_year'].value_counts().sort_index().to_dict()}")
    B.log(f"  top programmes: {df['programme'].value_counts().head(10).to_dict()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    df = df.astype("string")
    parquet_path = args.output_dir / "beijing_bmstc_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    B.log(f"Wrote {len(df)} rows to {parquet_path}")
    if args.skip_upload:
        return
    B.upload_with_shrink_guard(df, parquet_path, S3_BUCKET, S3_KEY, args.allow_shrink)


if __name__ == "__main__":
    main()
