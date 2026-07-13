#!/usr/bin/env python3
"""
Natural Science Foundation of Heilongjiang Province -> S3
=========================================================

Funder:      Natural Science Foundation of Heilongjiang Province
             OpenAlex F4320323085 (ROR null, DOI 10.13039/501100005046, CN)
Provenance:  heilongjiang_nsf
Priority:    409
Output:      s3://openalex-ingest/awards/heilongjiang_nsf/heilongjiang_nsf_projects.parquet

Source
------
The Heilongjiang Provincial Department of Science and Technology publishes an
annual "关于XXXX年度黑龙江省自然科学基金拟资助项目的公示" (proposed-award publicity)
under its 公示公告 column, carrying a bulk attachment (xlsx or PDF) listing the
funded projects, split by programme:

    序号 | 项目名称 | 负责人/申报人 | 申报单位/依托单位

Programmes (each an xlsx sheet or a PDF section 附件N): 研究团队项目, 杰出青年项目,
重点项目, 优秀青年项目, 联合引导项目, 联合基金重点项目, 联合基金培育项目, 青年项目,
非共识项目, 博士生项目, 校（院）企合作项目.

**Live-site caveat:** kjt.hlj.gov.cn purges publicity posts after the comment
window (the original t20241128 / t20251222 shtml pages now 404), and the site
serves an aliyun WAF that 404s the attachment file route from off-China IPs.
The durable source is therefore the **Internet Archive Wayback Machine**, which
captured both the 2024 xlsx and the 2025 PDF. The pinned ``id_`` Wayback URLs in
FILES below fetch the raw attachment bytes. When the province republishes a live
notice, add its direct URL; the Wayback fallback keeps the ingest reproducible.

No grant number and no amount are published in these lists (amount NULL ->
Step 6.7 waiver; funder_award_id NULL -> content-hash row_key gives each row a
distinct OpenAlex award id in the notebook).

PI names: Chinese personal names -> whole name in family_name, given_name NULL.

Usage:
    python heilongjiang_nsf_to_s3.py [--skip-upload] [--limit N] [--output-dir DIR]
"""
import argparse
import re
from pathlib import Path

import _cn_province_common as C

FUNDER_ID = 4320323085
PROVENANCE = "heilongjiang_nsf"
S3_KEY = f"awards/{PROVENANCE}/{PROVENANCE}_projects.parquet"

# year -> (kind, landing_page, attachment_url via Wayback id_ raw fetch)
WB = "http://web.archive.org/web"
FILES = {
    "2024": ("xlsx",
             "https://kjt.hlj.gov.cn/kjt/c113908/202411/c00_31787486.shtml",
             f"{WB}/20250420004744id_/https://kjt.hlj.gov.cn/kjt/c113908/202411/"
             "31787486/files/%E9%99%84%E4%BB%B6%EF%BC%9A2024%E5%B9%B4%E5%BA%A6%E9%BB%91"
             "%E9%BE%99%E6%B1%9F%E7%9C%81%E8%87%AA%E7%84%B6%E7%A7%91%E5%AD%A6%E5%9F%BA"
             "%E9%87%91%E6%8B%9F%E8%B5%84%E5%8A%A9%E9%A1%B9%E7%9B%AE%E5%90%8D%E5%8D%95.xlsx"),
    "2025": ("pdf",
             "https://kjt.hlj.gov.cn/kjt/c113908/202512/c00_31898731.shtml",
             f"{WB}/20251222163224id_/https://kjt.hlj.gov.cn/kjt/c113908/202512/"
             "31898731/files/%E9%99%84%E4%BB%B6%EF%BC%9A2025%E5%B9%B4%E5%BA%A6%E7%9C%81"
             "%E8%87%AA%E7%84%B6%E7%A7%91%E5%AD%A6%E5%9F%BA%E9%87%91%E6%8B%9F%E8%B5%84"
             "%E5%8A%A9%E9%A1%B9%E7%9B%AE.pdf"),
}

# section/scheme heading in the PDF, e.g. "2025年度省自然科学基金研究团队项目"
SEC_RE = re.compile(r"20\d\d年(?:度)?省自然科学基金([^\n]{2,20}?项目)")


def _scheme_from_heading(txt):
    m = SEC_RE.search(txt or "")
    return m.group(1).strip() if m else None


def _col(header, *names):
    for j, h in enumerate(header):
        for n in names:
            if n in (h or ""):
                return j
    return None


def parse_xlsx(path, year, landing):
    import pandas as pd
    xl = pd.ExcelFile(path)
    recs = []
    for sheet in xl.sheet_names:
        df = xl.parse(sheet, header=None)
        if df.empty:
            continue
        scheme = C.clean(sheet)  # sheet name IS the programme
        hi = None
        for i in range(min(6, len(df))):
            row = [C.clean(x) or "" for x in df.iloc[i].tolist()]
            if "项目名称" in "".join(row) and ("负责人" in "".join(row) or "申报人" in "".join(row)):
                hi, header = i, row
                break
        if hi is None:
            continue
        c_title = _col(header, "项目名称")
        c_pi = _col(header, "负责人", "申报人")
        c_inst = _col(header, "申报单位", "依托单位", "承担单位")
        for i in range(hi + 1, len(df)):
            row = df.iloc[i].tolist()
            title = C.clean(row[c_title]) if c_title is not None and c_title < len(row) else None
            pi = C.clean(row[c_pi]) if c_pi is not None and c_pi < len(row) else None
            first = C.clean(row[0]) if len(row) else None
            if not title or (first and first in ("合计", "序号", "总计")):
                continue
            inst = C.clean(row[c_inst]) if c_inst is not None and c_inst < len(row) else None
            given, family = C.split_name(pi)
            recs.append(_rec(title, scheme, inst, given, family, year, landing))
    print(f"    {year} [xlsx]: {len(recs):,} rows")
    return recs


def parse_pdf(path, year, landing):
    import fitz
    d = fitz.open(str(path))
    recs = []
    scheme = None
    for page in d:
        events = []
        for blk in page.get_text("dict")["blocks"]:
            for ln in blk.get("lines", []):
                t = "".join(sp["text"] for sp in ln["spans"]).strip()
                s = _scheme_from_heading(t)
                if s:
                    events.append((ln["bbox"][1], "sec", s))
        for t in page.find_tables().tables:
            events.append((t.bbox[1], "tbl", t))
        events.sort(key=lambda e: e[0])
        for y, kind, payload in events:
            if kind == "sec":
                scheme = payload
                continue
            ext = payload.extract()
            hdr_i = None
            for i, row in enumerate(ext[:2]):
                cells = [C.clean(c) or "" for c in row]
                if "项目名称" in "".join(cells):
                    hdr_i = i
                    header = cells
                    break
            start = (hdr_i + 1) if hdr_i is not None else 0
            if hdr_i is not None:
                c_title = _col(header, "项目名称")
                c_pi = _col(header, "负责人", "申报人")
                c_inst = _col(header, "申报单位", "依托单位")
            else:
                c_title, c_pi, c_inst = 1, 2, 3
            for row in ext[start:]:
                cells = [C.clean(c) for c in row]
                if not any(cells):
                    continue
                title = cells[c_title] if c_title is not None and c_title < len(cells) else None
                pi = cells[c_pi] if c_pi is not None and c_pi < len(cells) else None
                first = cells[0] if cells else None
                if not title or (first and first in ("合计", "序号")):
                    continue
                inst = cells[c_inst] if c_inst is not None and c_inst < len(cells) else None
                given, family = C.split_name(pi)
                recs.append(_rec(title.replace("\n", ""), scheme, inst, given, family, year, landing))
    print(f"    {year} [pdf]: {len(recs):,} rows")
    return recs


def _rec(title, scheme, inst, given, family, year, landing):
    return {
        "funder_award_id": None,
        "display_name": title,
        "funder_scheme": scheme,
        "institution": inst,
        "given_name": given,
        "family_name": family,
        "amount": None,
        "currency": None,
        "start_date": f"{year}-01-01",
        "end_date": None,
        "start_year": int(year),
        "end_year": None,
        "landing_page_url": landing,
        "source_year": year,
    }


def main():
    ap = argparse.ArgumentParser(description="Heilongjiang NSF -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("./heilongjiang_nsf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    dl = args.output_dir / "downloads"
    dl.mkdir(exist_ok=True)

    print("=" * 60 + f"\nHeilongjiang NSF -> S3  (F{FUNDER_ID}, provenance {PROVENANCE})\n" + "=" * 60)
    years = sorted(FILES)
    if args.limit:
        years = years[:args.limit]
    all_recs = []
    for year in years:
        kind, landing, att = FILES[year]
        dest = dl / f"heilongjiang_{year}.{kind}"
        if not dest.exists() or dest.stat().st_size < 1024:
            print(f"  downloading {year} ({kind}) via Wayback")
            if not C.http_get(att, dest, timeout=240):
                print(f"    [skip] {year} download failed")
                continue
        try:
            if kind == "xlsx":
                all_recs.extend(parse_xlsx(dest, year, landing))
            else:
                all_recs.extend(parse_pdf(dest, year, landing))
        except Exception as e:
            print(f"    [parse-fail] {year}: {e}")

    if not all_recs:
        print("  [FATAL] no rows parsed"); raise SystemExit(1)
    out = C.finalize_df(all_recs, PROVENANCE, args.output_dir,
                        f"{PROVENANCE}_projects.parquet")
    if not args.skip_upload:
        C.upload_to_s3(out, S3_KEY)
    print(f"\nDone. Next: notebooks/awards/CreateHeilongjiangNSFAwards.ipynb")


if __name__ == "__main__":
    main()
