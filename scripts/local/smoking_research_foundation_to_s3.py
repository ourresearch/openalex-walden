#!/usr/bin/env python3
"""
Smoking Research Foundation (公益財団法人 喫煙科学研究財団, Japan) to S3 Data Pipeline
======================================================================================

SRF (Tokyo; OpenAlex F4320322555) publishes the titles of every completed study it
funded since 1986 through the "Former studies" lists linked from
https://www.srf.or.jp/en/studies.html. The lists are server-rendered by a POST form
(https://www.srf.or.jp/app_list2/, fields cat/area/nendoS/nendoE/lang/mode) and give,
per study: list number, title, period (start-end FY) and, for most studies, a link to
the study's results-summary PDF whose file name carries the study number
(fp00001001.pdf, eFP01812176.pdf, en_2023G004.pdf). Method 5 (static HTML) on the
runbook ladder; no export or API exists (WordPress REST API is disabled, 401).

Lists ingested (English lists are authoritative; the Japanese lists are fetched only
to attach title_ja by study number, because they are not row-aligned with the English):
  - 一般研究 General Research, 11 field lists (Cancer ... Epidemiology and Others)
  - 特定研究 Project Research (grouped by project theme)
  - 若手研究 Early-Career Scientists Research
  - Heated Tobacco Product Related Research (English only; a cross-listing whose
    studies also appear in the field lists, so it only tags existing rows)

Not ingested: the "Current studies" list (FY2026, 179 ongoing studies) carries neither
period nor study number; those studies enter the Former lists, with their number,
when they finish. SRF publishes no per-study PI or amount (standard rates only), so
those fields stay NULL.

funder_award_id: the study number from the PDF name (FP + 8 digits to ~FY2018-start
studies, YYYY{G|Y|T}NNN after), which is the form citing works use (FP01606073,
2022G002); studies with no PDF (mostly 1986-1995) ship a stable synthetic key
SRF-{start}-{end}-{md5(english title)[:8]}.

Output: s3://openalex-ingest/awards/smoking_research_foundation/smoking_research_foundation_projects.parquet
"""

import argparse
import hashlib
import html
import json
import re
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (runbook §1.2 grep marker: sys.stdout.reconfigure)
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

BASE = "https://www.srf.or.jp"
LIST_URL = BASE + "/app_list2/"
LANDING = BASE + "/en/studies.html"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/smoking_research_foundation/smoking_research_foundation_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)",
           "Content-Type": "application/x-www-form-urlencoded"}
REQUEST_DELAY = 1.0
END_FY = "2025"  # last completed fiscal year shown in the Former lists (studies.html uses nendoE=2025)

# (english area, japanese area) for the 一般研究 field lists
AREAS = [
    ("Cancer", "喫煙とがん"), ("Physiology and Pharmacology", "喫煙の生理・薬理"),
    ("Cardiovascular System", "喫煙と循環器系"), ("Respiratory System", "喫煙と呼吸器系"),
    ("Digestive System", "喫煙と消化器系"), ("Endocrine and Metabolic Systems", "喫煙と内分泌・代謝"),
    ("Nervous System", "喫煙と神経系"), ("Obstetrics", "喫煙と妊婦・胎児"),
    ("Mental and Behavioral Science", "喫煙と精神機能・行動"), ("Passive Smoking", "受動喫煙"),
    ("Epidemiology and Others", "疫学等"),
]


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def queries() -> list[dict]:
    q = []
    for en, ja in AREAS:
        q.append({"list": "general", "scheme": f"General Research: {en}",
                  "en": dict(cat="一般研究", area=en, mode="LIST"), "ja": dict(cat="一般研究", area=ja, mode="LIST")})
    q.append({"list": "project", "scheme": "Project Research",
              "en": dict(cat="Project Research", area="", mode="LIST2"), "ja": dict(cat="特定研究", area="", mode="LIST2")})
    q.append({"list": "early_career", "scheme": "Early-Career Scientists Research",
              "en": dict(cat="Early-Career Scientists Research", area="", mode="LIST2"),
              "ja": dict(cat="若手研究", area="", mode="LIST2")})
    q.append({"list": "heated_tobacco", "scheme": "Heated Tobacco Product Related Research",
              "en": dict(cat="Heated Tobacco Product Related Research", area="Heated Tobacco Product Related Research", mode="LIST"),
              "ja": None})
    return q


def post(fields: dict, lang: str, cache_dir: Path | None) -> str:
    form = dict(fields, nendo="", nendoE=END_FY, nendoS="0", lang=lang)
    key = hashlib.md5(json.dumps(form, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:12]
    cache = cache_dir / f"{lang}_{key}.html" if cache_dir else None
    if cache and cache.exists():
        return cache.read_text()
    body = "&".join(f"{k}={urllib.parse.quote(v.encode('utf-8'))}" for k, v in form.items())
    last = None
    for attempt in range(4):
        try:
            r = requests.post(LIST_URL, data=body, headers=HEADERS, timeout=90)
            if r.status_code == 200 and "report_sublist" in r.text:
                text = r.content.decode("utf-8", errors="replace")
                if cache:
                    cache.parent.mkdir(parents=True, exist_ok=True)
                    cache.write_text(text)
                time.sleep(REQUEST_DELAY)
                return text
            last = f"HTTP {r.status_code}, {len(r.content)} bytes, no list table"
        except requests.RequestException as e:  # noqa: PERF203
            last = str(e)
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"POST {fields} ({lang}) failed: {last}")  # never silently drop a list


def clean(x: str) -> str:
    x = re.sub(r"<[^>]+>", "", x)
    return re.sub(r"\s+", " ", html.unescape(x).replace("　", " ")).strip()


def parse_list(page: str) -> list[dict]:
    i, j = page.find("<h2"), page.find("pagetop")
    body = page[i:j]
    rows, group = [], None
    for m in re.finditer(r"<h5>(.*?)</h5>|<tr>\s*<td>(\d+)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*</tr>", body, re.S):
        if m.group(1) is not None:
            group = clean(re.sub(r"<br\s*/?>", " | ", m.group(1)))
            continue
        pdf = re.search(r"href='([^']+\.pdf)'", m.group(3))
        rows.append({"no": int(m.group(2)), "group": group, "title": clean(m.group(4)),
                     "period": clean(m.group(5)), "pdf": BASE + pdf.group(1) if pdf else None})
    return rows


def study_number(pdf_url: str | None) -> str | None:
    if not pdf_url:
        return None
    fn = pdf_url.rsplit("/", 1)[-1]
    m = re.search(r"(?<![0-9])(\d{4}[GYT]\d{3})(?![0-9])", fn)
    if m:
        return m.group(1)
    m = re.search(r"fp(?:ar)?(\d+)", fn, re.I)
    if m and len(m.group(1)) == 8:  # one link has 9 digits (typo): no reliable number
        return "FP" + m.group(1)
    return None


def main() -> None:
    p = argparse.ArgumentParser(description="Smoking Research Foundation former-studies lists -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only fetch the first N lists (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    todo = queries()
    if args.limit:
        todo = todo[: args.limit]
    out = []
    for q in todo:
        en = parse_list(post(q["en"], "en", args.cache_dir))
        if not en:
            raise RuntimeError(f"{q['scheme']}: empty English list")
        # The Japanese list has the same row count but is NOT row-aligned with the English
        # one (Cardiovascular row 43 is a different study in each), and periods can differ
        # for the same study (FP01502034: en 2013-2015 / ja 2012-2015). So the English list
        # is authoritative and a Japanese title is attached only by study number.
        ja = parse_list(post(q["ja"], "ja", args.cache_dir)) if q["ja"] else []
        ja_by_num = {study_number(b["pdf"]): b for b in ja if study_number(b["pdf"])}
        log(f"{q['scheme']}: {len(en)} studies, {sum(1 for r in en if r['pdf'])} with report PDF "
            f"(ja list {len(ja)} rows, {len(ja_by_num)} numbered)")
        for r in en:
            per = re.fullmatch(r"(\d{4})-(\d{4})?", r["period"] or "")
            num = study_number(r["pdf"])
            scheme = q["scheme"]
            if q["list"] == "project" and r["group"]:
                scheme = f"Project Research: {r['group']}"
            out.append({
                "list": q["list"],
                "funder_scheme": scheme,
                "list_no": str(r["no"]),
                "group_heading": r["group"],
                "title_en": r["title"] or None,
                "title_ja": (ja_by_num[num]["title"] or None) if num in ja_by_num else None,
                "period": r["period"],
                "period_ja": ja_by_num[num]["period"] if num in ja_by_num else None,
                "start_year": per.group(1) if per else None,
                "end_year": per.group(2) if per and per.group(2) else None,
                "study_number": num,
                "report_pdf_en": r["pdf"],
                "report_pdf_ja": ja_by_num[num]["pdf"] if num in ja_by_num else None,
                "synthetic_key": f"SRF-{r['period']}-{hashlib.md5((r['title'] or '').lower().encode('utf-8')).hexdigest()[:8]}",
                "landing_page_url": LANDING,
            })

    df = pd.DataFrame(out)
    # Heated-tobacco list is a cross-listing of studies already in the field lists:
    # tag those rows and drop the duplicate; keep any study found only there.
    ht = df[df["list"] == "heated_tobacco"]
    rest = df[df["list"] != "heated_tobacco"]
    ht_nums = set(ht["study_number"].dropna())
    ht_titles = set(zip(ht["title_en"].str.lower(), ht["period"]))
    rest = rest.assign(also_heated_tobacco=[
        (n in ht_nums) or ((t.lower(), pr) in ht_titles) for n, t, pr in zip(rest["study_number"], rest["title_en"], rest["period"])])
    seen_n, seen_t = set(rest["study_number"].dropna()), set(zip(rest["title_en"].str.lower(), rest["period"]))
    ht_only = ht[[not ((n is not None and n in seen_n) or (t.lower(), pr) in seen_t)
                  for n, t, pr in zip(ht["study_number"], ht["title_en"], ht["period"])]].assign(also_heated_tobacco=True)
    log(f"heated-tobacco cross-listing: {len(ht)} rows, {len(ht) - len(ht_only)} already in other lists, {len(ht_only)} kept")
    df = pd.concat([rest, ht_only], ignore_index=True)

    # the same study listed under two fields (same title, period and number) is one award
    key = list(zip(df["study_number"].fillna(""), df["title_en"].str.lower(), df["period"]))
    df = df.assign(_k=key)
    also = df.groupby("_k")["funder_scheme"].agg(lambda s: " ; ".join(s.iloc[1:]) or None)
    before = len(df)
    df = df.drop_duplicates("_k", keep="first").copy()
    df["also_listed_in"] = df["_k"].map(also)
    df = df.drop(columns="_k")
    log(f"  {before - len(df)} studies listed twice (same title/period/number) merged")

    # a study number linked from several different studies is a group-level report
    # (Project Research FP01112200 covers 7 sub-studies): keep it on none of them
    dup_num = df["study_number"].notna() & df.duplicated("study_number", keep=False)
    for n, g in df[dup_num].groupby("study_number"):
        log(f"  study number {n} linked from {len(g)} rows ({g.title_en.tolist()}); dropped to synthetic keys")
    df.loc[dup_num, "study_number"] = None
    df["funder_award_id"] = df["study_number"].fillna(df["synthetic_key"])
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, ['funder_award_id', 'title_en', 'period']].to_dict('records')}")

    log(f"Total {len(df)} studies, {df.start_year.min()}-{df.end_year.max()}")
    for c in ["title_en", "title_ja", "start_year", "end_year", "study_number"]:
        log(f"  {c:14s} {df[c].notna().mean():6.1%}")
    log(f"  by list: {df['list'].value_counts().to_dict()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    df = df.astype("string")
    parquet_path = args.output_dir / "smoking_research_foundation_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_smoking_research_foundation_projects.parquet"
    try:  # runbook §1.4: never shrink the corpus on re-ingest
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
