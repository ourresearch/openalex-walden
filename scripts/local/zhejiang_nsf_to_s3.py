#!/usr/bin/env python3
"""
Natural Science Foundation of Zhejiang Province -> S3
=====================================================

Funder:      Natural Science Foundation of Zhejiang Province
             OpenAlex F4320338464 (DOI 10.13039/501100004731, CN)
Provenance:  zhejiang_nsf
Priority:    403
Output:      s3://openalex-ingest/awards/zhejiang_nsf/zhejiang_nsf_projects.parquet

Source
------
The Zhejiang NSF committee portal (zjnsf.kjt.zj.gov.cn) publishes the annual
"关于下达XX年度浙江省自然科学基金项目的通知" (award-establishment notices) with a
bulk attachment listing every funded project. The portal has a clean JSON API
(``/insiis7/api/site/post/list?categoryId=2018515`` for the notice list,
``/insiis7/api/site/post/detail?postId=...`` for full text + attachment links)
and its ueditor/staticfiles attachments download with a plain browser UA.

Programme history: 2019 and 2025+ lists are pure 自然科学基金; 2020-2024 the
fund ran inside the bundled 基础公益研究计划, whose 2022 notice shipped a
separate NSF-only table and whose 2023/2024 clean-list PDFs contain ONLY
自然科学基金 sections (公益技术应用研究 is a separate attachment/notice we do not
ingest — §2.3.2 split verified per file). The 2018/2020/2021 attachments are
dead on the funder's servers (legacy ``/base/load.ashx`` + ``/files`` handlers
removed; kjt.zj.gov.cn mirrors WAF-block the file route; not in Wayback) —
those years are noted as a gap.

Attachment table shape (docx tables and PDF implicit tables, parsed via
python-docx / PyMuPDF find_tables):

    序号 | 项目名称 | 立项编号 | 负责人 | 依托单位     (2023+; 2022 has 立项编号 2nd)

Section headings (一、重大项目 ...) carry the programme -> funder_scheme.
立项编号 (LD26C200001 / LTGS23... / ZCLZ26... for self-funded) is the native
grant number cited in papers. **No per-project amounts are published** in any
year's list -> amount NULL (Step 6.7 waiver). 自筹经费 (self-funded) lists are
included: the committee formally establishes those projects and papers cite
their ZCLZ grant numbers, but the money is institution-raised -> amount NULL.

PI names: Chinese personal names -> whole name in family_name, given_name NULL.

Usage:
    python zhejiang_nsf_to_s3.py [--skip-upload] [--limit N] [--output-dir DIR]
"""
import argparse
import re
from pathlib import Path

import _cn_province_common as C

FUNDER_ID = 4320338464
PROVENANCE = "zhejiang_nsf"
S3_KEY = f"awards/{PROVENANCE}/{PROVENANCE}_projects.parquet"

PORTAL = "https://zjnsf.kjt.zj.gov.cn"
DETAIL = PORTAL + "/portal/detail.html?typeid=2018515&postid={pid}"
UE = PORTAL + "/isp/staticfiles/ueditor"

# label -> (year, postid, attachment_url, is_self_funded)
NOTICES = {
    "2019":      ("2019", "1008249042",
                  PORTAL + "/files/h/01/1008249042/基金项目附件.docx", False),
    "2022":      ("2022", "917436473936969728",
                  UE + "/202112/2022年度浙江省自然科学基金资助项目表.pdf", False),
    "2023":      ("2023", "1050375499009753088",
                  UE + "/202212/附件：2023年度浙江省基础公益研究计划资助项目清单.pdf", False),
    "2024":      ("2024", "1187793206591356928",
                  UE + "/202312/xmqd.pdf", False),
    "2024self":  ("2024", "1197588358281297920",
                  UE + "/202401/附件2024年度浙江省基础公益研究计划自筹经费项目立项清单 (1).pdf", True),
    "2025":      ("2025", "1328652320979812352",
                  UE + "/202501/2025年度浙江省自然科学基金资助项目清单.docx", False),
    "2025self":  ("2025", "1328652883570196480",
                  UE + "/202501/2025年度浙江省自然科学基金自筹经费立项项目清单.docx", True),
    "2026":      ("2026", "1454052273054285824",
                  UE + "/202512/2026年度浙江省自然科学基金资助项目清单.pdf", False),
    "2026self":  ("2026", "1454053398365077504",
                  UE + "/202512/133305d986924a6bb61f1a05c17f7af7.pdf", True),
}

# grant 立项编号, e.g. LD26C200001, LRG25H090001, LDT23F05011F05, LQ19A010001,
# ZCLZ24H0901 (self-funded). Alnum starting L or ZC, >=8 chars, >=4 digits.
CODE_RE = re.compile(r"^(?:L|ZC)[A-Z0-9]{6,16}$")
SEQ_RE = re.compile(r"^\d{1,4}(?:-\d{1,3})?$")
_CN_SEC = re.compile(r"^[一二三四五六七八九十]+、")


def _clean_cell(v):
    if v is None:
        return None
    s = str(v).replace("\n", "")
    # WPS PDFs insert spaces at wrap points incl. inside CJK — safe to drop
    # spaces that sit between CJK chars; keep single spaces in latin runs
    s = re.sub(r"(?<=[一-鿿（）、：])\s+(?=[一-鿿（）、：])", "", s)
    return C.clean(s)


def _is_code(s):
    return bool(s) and bool(CODE_RE.match(s))


def _emit(recs, seq, cells_by_col, scheme, year, landing, is_self):
    """cells_by_col: dict col->text for one logical row (code col detected)."""
    vals = [v for v in cells_by_col if v]
    code_idx = next((i for i, v in enumerate(cells_by_col) if _is_code(v or "")), None)
    if code_idx is None:
        return
    code = cells_by_col[code_idx]
    after = [v for v in cells_by_col[code_idx + 1:] if v]
    before = [v for v in cells_by_col[1:code_idx] if v]
    if code_idx <= 1:
        # layout: 序号 | 立项编号 | 项目名称 | 负责人 | 依托单位   (2022)
        title = after[0] if len(after) > 0 else None
        pi = after[1] if len(after) > 1 else None
        inst = after[2] if len(after) > 2 else None
    else:
        # layout: 序号 | 项目名称 | 立项编号 | 负责人 | 依托单位   (2023+)
        title = " ".join(before) if before else None
        pi = after[0] if len(after) > 0 else None
        inst = after[1] if len(after) > 1 else None
    if not title:
        return
    given, family = C.split_name(pi)
    scheme_out = scheme
    if is_self:
        scheme_out = (scheme + "（自筹经费）") if scheme else "自筹经费项目"
    recs.append({
        "funder_award_id": code,
        "display_name": title,
        "funder_scheme": scheme_out,
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
    })


def parse_pdf(path, year, landing, is_self):
    import fitz
    d = fitz.open(str(path))
    recs = []
    scheme = None
    for page in d:
        # events: section headings (from text lines) + table rows, merged by y
        events = []
        for blk in page.get_text("dict")["blocks"]:
            for ln in blk.get("lines", []):
                txt = "".join(sp["text"] for sp in ln["spans"]).strip()
                if _CN_SEC.match(txt) and len(txt) < 60:
                    events.append((ln["bbox"][1], "sec", txt))
        tabs = page.find_tables()
        for t in tabs.tables:
            ext = t.extract()
            for ri, row in enumerate(ext):
                y = t.rows[ri].bbox[1] if ri < len(t.rows) else 0
                events.append((y, "row", row))
        events.sort(key=lambda e: e[0])
        for y, kind, payload in events:
            if kind == "sec":
                scheme = C.scheme_from_heading(payload)
                continue
            cells = [_clean_cell(c) for c in payload]
            nonempty = [c for c in cells if c]
            if not nonempty:
                continue
            joined = "".join(nonempty)
            if "项目名称" in joined or "立项编号" in joined or "依托单位" in joined:
                continue  # header row
            # section heading rendered as a table row
            if _CN_SEC.match(nonempty[0]) and len(nonempty) <= 2:
                scheme = C.scheme_from_heading(nonempty[0])
                continue
            seq = cells[0] if cells and cells[0] and SEQ_RE.match(cells[0]) else None
            if not any(_is_code(c or "") for c in cells):
                # continuation fragment of the previous row -> append per column
                if recs and seq is None:
                    frag_map = {1: "display_name", 2: "display_name",
                                3: "family_name", 4: "institution"}
                    for j, c in enumerate(cells):
                        if c and j in frag_map and frag_map[j] in ("display_name", "institution"):
                            k = frag_map[j]
                            recs[-1][k] = ((recs[-1][k] or "") + c) if recs[-1][k] else c
                continue
            _emit(recs, seq, cells, scheme, year, landing, is_self)
    print(f"    {year}{'(self)' if is_self else ''} [{Path(path).name}]: {len(recs):,} rows")
    return recs


def parse_docx(path, year, landing, is_self):
    import docx
    from docx.table import Table
    from docx.text.paragraph import Paragraph

    doc = docx.Document(str(path))
    recs = []
    scheme = None
    body = doc.element.body
    for child in body.iterchildren():
        if child.tag.endswith("}p"):
            txt = Paragraph(child, doc).text.strip()
            if _CN_SEC.match(txt) and len(txt) < 60:
                scheme = C.scheme_from_heading(txt)
        elif child.tag.endswith("}tbl"):
            t = Table(child, doc)
            for row in t.rows:
                cells = [_clean_cell(c.text) for c in row.cells]
                nonempty = [c for c in cells if c]
                if not nonempty:
                    continue
                joined = "".join(nonempty)
                if "项目名称" in joined or "立项编号" in joined or "项目编号" in joined:
                    continue
                # merged section row (all cells identical heading)
                if _CN_SEC.match(nonempty[0]) and len(set(nonempty)) == 1:
                    scheme = C.scheme_from_heading(nonempty[0])
                    continue
                if not any(_is_code(c or "") for c in cells):
                    continue
                seq = cells[0] if cells[0] and SEQ_RE.match(cells[0]) else None
                _emit(recs, seq, cells, scheme, year, landing, is_self)
    print(f"    {year}{'(self)' if is_self else ''} [{Path(path).name}]: {len(recs):,} rows")
    return recs


def main():
    ap = argparse.ArgumentParser(description="Zhejiang NSF -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("./zhejiang_nsf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="cap number of notices (smoke)")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    dl = args.output_dir / "downloads"
    dl.mkdir(exist_ok=True)

    print("=" * 60 + f"\nZhejiang NSF -> S3  (F{FUNDER_ID}, provenance {PROVENANCE})\n" + "=" * 60)
    labels = sorted(NOTICES)
    if args.limit:
        labels = labels[:args.limit]
    all_recs = []
    for label in labels:
        year, pid, att, is_self = NOTICES[label]
        landing = DETAIL.format(pid=pid)
        ext = att.rsplit(".", 1)[-1].split("?")[0].split(" ")[0]
        dest = dl / f"zhejiang_{label}.{ext}"
        if not dest.exists() or dest.stat().st_size < 1024:
            print(f"  downloading {label}: {att[:100]}")
            if not C.http_get(att, dest, referer=landing):
                print(f"    [skip] {label} download failed")
                continue
        try:
            if str(dest).endswith(".docx"):
                all_recs.extend(parse_docx(dest, year, landing, is_self))
            else:
                all_recs.extend(parse_pdf(dest, year, landing, is_self))
        except Exception as e:
            print(f"    [parse-fail] {label}: {e}")

    if not all_recs:
        print("  [FATAL] no rows parsed"); raise SystemExit(1)
    # dedup by grant code (2023 bundle republishes a handful of rows)
    seen, uniq = set(), []
    for r in all_recs:
        k = r["funder_award_id"]
        if k in seen:
            continue
        seen.add(k)
        uniq.append(r)
    if len(uniq) != len(all_recs):
        print(f"  dedup by 立项编号: {len(all_recs):,} -> {len(uniq):,}")
    out = C.finalize_df(uniq, PROVENANCE, args.output_dir,
                        f"{PROVENANCE}_projects.parquet")
    if not args.skip_upload:
        C.upload_to_s3(out, S3_KEY)
    print(f"\nDone. Next: notebooks/awards/CreateZhejiangNSFAwards.ipynb")


if __name__ == "__main__":
    main()
