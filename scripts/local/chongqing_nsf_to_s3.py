#!/usr/bin/env python3
"""
Natural Science Foundation of Chongqing -> S3
=============================================

Funder:      Natural Science Foundation of Chongqing
             OpenAlex F4320323172 (ROR https://ror.org/01h0zpd94,
             DOI 10.13039/501100005230, CN)
Provenance:  chongqing_nsf
Priority:    407
Output:      s3://openalex-ingest/awards/chongqing_nsf/chongqing_nsf_projects.parquet

Source
------
The Chongqing Municipal Science & Technology Bureau publishes, per programme
and year, a "XXX拟立项项目清单公示" (proposed-award publicity list) under its
notice column (zwxx_176/tzgg), each carrying one or more PDF attachments
(附件1/2/3...). Programmes: 面上项目 (general), 博士"直通车" (postdoc),
杰出青年科学基金, 创新发展联合基金 (many industry co-funds: 长安汽车, 市教委,
万州区, 气象, 电力, 星网应用, 智慧医保 ...), plus the 科技发展基金会 special.

PDF table shape (digital text, PyMuPDF find_tables):

    序号 | 项目名称 | (项目)申报单位 | (项目)负责人 | [管理处室] | 备注

These are **proposed-award (拟立项) publicity lists**: they carry the project
title, host institution, and PI, but **no grant number and no amount**
(amount NULL -> Step 6.7 waiver; funder_award_id NULL -> a content-hash
``row_key`` gives each row a distinct OpenAlex award id in the notebook). The
programme name (funder_scheme) is read from each PDF's own heading. The site
is reachable with a plain browser UA. Older years' publicity pages are purged
after the comment window, so the pinned NOTICES manifest below is the durable
set discovered from the live notice column (2023-2026).

Note: several 联合基金 co-funds carry a "备注: 联合资助" flag — the Chongqing NSF
is a co-funder on those, which is exactly the funder edge we want.

PI names: Chinese personal names -> whole name in family_name, given_name NULL.

Usage:
    python chongqing_nsf_to_s3.py [--skip-upload] [--limit N] [--output-dir DIR]
"""
import argparse
import re
from pathlib import Path

import _cn_province_common as C

FUNDER_ID = 4320323172
PROVENANCE = "chongqing_nsf"
S3_KEY = f"awards/{PROVENANCE}/{PROVENANCE}_projects.parquet"

TZGG = "http://kjj.cq.gov.cn/zwxx_176/tzgg"
# label -> (year, notice_url, [attachment_urls])
NOTICES = {
    "2023_lhjj":   ("2023", f"{TZGG}/202311/t20231127_12616340.html", [
        f"{TZGG}/202311/P020231127688608856947.pdf",
        f"{TZGG}/202311/P020231127688610869402.pdf",
        f"{TZGG}/202311/P020231127688612182850.pdf",
        f"{TZGG}/202311/P020231127692866023883.pdf"]),
    "2024_jw":     ("2024", f"{TZGG}/202405/t20240529_13248045.html", [
        f"{TZGG}/202405/P020240529625592277302.pdf"]),
    "2024_zx":     ("2024", f"{TZGG}/202406/t20240617_13300956.html", [
        f"{TZGG}/202406/P020240617766400461060.pdf",   # 面上
        f"{TZGG}/202406/P020240617766401307465.pdf",   # 杰出青年
        f"{TZGG}/202406/P020240617766401902030.pdf"]), # 博士直通车
    "2024_lhjj":   ("2024", f"{TZGG}/202409/t20240913_13630247.html", [
        f"{TZGG}/202409/P020240913656119446937.pdf",
        f"{TZGG}/202409/P020240913656120938485.pdf",
        f"{TZGG}/202409/P020240913656121418098.pdf"]),
    # §2.3.2 split: this notice bundles 自然科学基金 (附件1, NSF) AND
    # 技术创新与应用发展专项 (附件2, applied-development — NOT natural-science
    # basic research). We ingest ONLY 附件1 under chongqing_nsf; 附件2 is a
    # different programme and is deliberately excluded.
    "2024_jjh":    ("2024", f"{TZGG}/202412/t20241227_14027114.html", [
        f"{TZGG}/202412/P020241227416556795303.pdf"]),   # 附件1 自然科学基金 only
    "2025_jw":     ("2025", f"{TZGG}/202506/t20250619_14728870.html", [
        f"{TZGG}/202506/P020250619740396871301.pdf"]),
    "2025_bs":     ("2025", f"{TZGG}/202506/t20250619_14728871.html", [
        f"{TZGG}/202506/P020250619741720839287.pdf"]),
    "2025_ms":     ("2025", f"{TZGG}/202507/t20250704_14779658.html", [
        f"{TZGG}/202507/P020250704589850020750.pdf"]),   # 面上 (~2000)
    "2025_ca":     ("2025", f"{TZGG}/202508/t20250821_14924139.html", [
        f"{TZGG}/202508/P020250821642966301018.pdf"]),
    "2025_lhjj3":  ("2025", f"{TZGG}/202510/t20251010_15066664.html", [
        f"{TZGG}/202510/P020251010432242133820.pdf",
        f"{TZGG}/202510/P020251010432242568156.pdf",
        f"{TZGG}/202510/P020251010432242959737.pdf"]),
    "2025_xw":     ("2025", f"{TZGG}/202511/t20251120_15177813.html", [
        f"{TZGG}/202511/P020251120712915147547.pdf"]),
    "2025_yb":     ("2025", f"{TZGG}/202601/t20260120_15338576.html", [
        f"{TZGG}/202601/P020260120562773557113.pdf"]),
}

SEQ_RE = re.compile(r"^\d{1,4}$")


def _scheme_from_title(txt):
    """Pull the programme name from a PDF heading like
    '2024年度重庆市自然科学基金创新发展联合基金（长安汽车）项目拟立项清单'."""
    t = C.clean(txt) or ""
    m = re.search(r"自然科学基金([^\n]*?)(?:项目)?(?:拟立项|立项)", t)
    if m and m.group(1).strip():
        return m.group(1).strip()
    for kw in ("面上项目", "博士", "杰出青年", "创新发展联合基金", "青年", "重点项目", "专项"):
        if kw in t:
            return kw
    return None


def _col(header, *names):
    for j, h in enumerate(header):
        for n in names:
            if n in (h or ""):
                return j
    return None


def parse_pdf(path, year, landing):
    import fitz
    d = fitz.open(str(path))
    scheme = None
    # scheme from first page heading text
    head_txt = d[0].get_text()[:300]
    scheme = _scheme_from_title(head_txt)
    # §2.3.2 safety net: never ingest a non-NSF programme list even if it slips
    # into the manifest (e.g. 技术创新与应用发展 applied-development specials).
    if "技术创新" in head_txt or ("自然科学基金" not in head_txt and "拟立项" in head_txt):
        print(f"    {Path(path).name}: SKIP (non-NSF programme: {head_txt[:40]!r})")
        return []
    recs = []
    cols = None
    for page in d:
        for t in page.find_tables().tables:
            ext = t.extract()
            if not ext:
                continue
            # detect header row
            hdr_i = None
            for i, row in enumerate(ext[:2]):
                cells = [C.clean(c) or "" for c in row]
                if "项目名称" in "".join(cells) and ("负责人" in "".join(cells)):
                    hdr_i = i
                    header = cells
                    cols = {
                        "title": _col(header, "项目名称"),
                        "inst": _col(header, "申报单位", "依托单位", "承担单位"),
                        "pi": _col(header, "负责人"),
                    }
                    break
            start = (hdr_i + 1) if hdr_i is not None else 0
            if cols is None:
                # fall back to positional 序号|项目名称|单位|负责人
                cols = {"title": 1, "inst": 2, "pi": 3}
            for row in ext[start:]:
                cells = [C.clean(c) for c in row]
                if not any(cells):
                    continue
                ti, ii, pi_i = cols["title"], cols["inst"], cols["pi"]
                title = cells[ti] if ti is not None and ti < len(cells) else None
                inst = cells[ii] if ii is not None and ii < len(cells) else None
                pi = cells[pi_i] if pi_i is not None and pi_i < len(cells) else None
                first = cells[0] if cells else None
                if not title or (first and first in ("合计", "序号", "总计")):
                    continue
                title = title.replace("\n", "")
                given, family = C.split_name(pi)
                recs.append({
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
                })
    print(f"    {Path(path).name}: scheme={scheme} rows={len(recs):,}")
    return recs


def main():
    ap = argparse.ArgumentParser(description="Chongqing NSF -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("./chongqing_nsf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="cap number of notices (smoke)")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    dl = args.output_dir / "downloads"
    dl.mkdir(exist_ok=True)

    print("=" * 60 + f"\nChongqing NSF -> S3  (F{FUNDER_ID}, provenance {PROVENANCE})\n" + "=" * 60)
    labels = list(NOTICES)
    if args.limit:
        labels = labels[:args.limit]
    all_recs = []
    for label in labels:
        year, landing, atts = NOTICES[label]
        for n, att in enumerate(atts):
            dest = dl / f"{label}_{n}.pdf"
            if not dest.exists() or dest.stat().st_size < 1024:
                if not C.http_get(att, dest, referer=landing):
                    print(f"    [skip] {label}#{n} failed")
                    continue
            try:
                all_recs.extend(parse_pdf(dest, year, landing))
            except Exception as e:
                print(f"    [parse-fail] {label}#{n}: {e}")

    if not all_recs:
        print("  [FATAL] no rows parsed"); raise SystemExit(1)
    out = C.finalize_df(all_recs, PROVENANCE, args.output_dir,
                        f"{PROVENANCE}_projects.parquet")
    if not args.skip_upload:
        C.upload_to_s3(out, S3_KEY)
    print(f"\nDone. Next: notebooks/awards/CreateChongqingNSFAwards.ipynb")


if __name__ == "__main__":
    main()
