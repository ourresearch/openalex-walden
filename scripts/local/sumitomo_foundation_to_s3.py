#!/usr/bin/env python3
"""
Sumitomo Foundation (公益財団法人住友財団) to S3 Data Pipeline
=============================================================

The Sumitomo Foundation (Tokyo; OpenAlex F4320310657) publishes one static
HTML grantee list per fiscal year for each grant programme on
https://www.sumitomo.or.jp/ (Shift-JIS FrontPage tables, no export/API).
Method 5 (static HTML) on the runbook ladder.

Research programmes ingested (one row per grant):
  - kiso    基礎科学研究助成 Basic Science Research          FY2014-FY2025  (amounts in 万円)
  - kankyo  環境研究助成   Environmental Research           FY2014-FY2025  (万円; 課題研究 / 一般研究)
  - jare    アジア諸国における日本関連研究助成
            Japan-Related Research Projects in Asian Countries FY2010-FY2024 (千円 or USD; FY2024 円)

Excluded (filter, see notebook header): the three cultural-property programmes
(文化財維持・修復事業助成 domestic restoration, 海外の文化財維持・修復事業助成
overseas restoration, 修復文化財展示事業助成 restored-property exhibitions).
Their lists are restoration/repair jobs on named objects paid to temples,
municipalities and museums, with no researcher, research question or output.
Jare FY2025 is published only as a PDF whose amount column is not row-aligned;
it is not parsed (known gap, ~30 grants).

Grant numbers: Sumitomo grant numbers are what grantees cite (e.g. "180853",
"2200735"; 6 digits to FY2021, 7 digits from FY2022). The site exposes them only
in the file names of the results-report PDFs linked from the FY2014-FY2022 lists
(kiso "001-180853.pdf", kankyo "Aikawa_Masahide_Seika_173025.pdf", some jare
"jare19-04_198035.pdf"). Those rows ship the published number as funder_award_id;
rows without one ship a stable synthetic key SF-{PROG}-{FY}-{md5(name|title)[:8]}.

Output: s3://openalex-ingest/awards/sumitomo_foundation/sumitomo_foundation_projects.parquet
"""

import argparse
import hashlib
import html
import json
import re
import time
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

BASE = "https://www.sumitomo.or.jp/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/sumitomo_foundation/sumitomo_foundation_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 0.5

SCHEMES = {
    "kiso": ("Basic Science Research (基礎科学研究助成)", "research"),
    "kankyo": ("Environmental Research (環境研究助成)", "research"),
    "jare": ("Japan-Related Research Projects in Asian Countries (アジア諸国における日本関連研究助成)", "research"),
}


def pages() -> list[tuple[str, int, str]]:
    out = []
    for fy in range(2014, 2026):
        out.append(("kiso", fy, f"html/kiso/kisotai{fy}.htm" if fy >= 2023 else f"html/kiso/kisotaisyo+Seika%20{fy}.htm"))
        out.append(("kankyo", fy, f"html/kankyo/kantaisyo{fy}.htm"))
    special = {2010: "jareli2010.htm", 2011: "jareli2011j.htm", 2014: "jareli2014j.htm"}
    for fy in range(2010, 2025):
        out.append(("jare", fy, "html/jare/" + special.get(fy, f"jareli{fy}.htm")))
    return out


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def decode(b: bytes) -> str:
    for enc in ("utf-8", "cp932", "euc-jp"):
        try:
            return b.decode(enc)
        except UnicodeDecodeError:
            pass
    return b.decode("cp932", errors="replace")


def fetch(path: str, cache_dir: Path | None) -> str:
    cache = cache_dir / (re.sub(r"[^A-Za-z0-9]+", "_", path) + ".html") if cache_dir else None
    if cache and cache.exists():
        return decode(cache.read_bytes())
    last = None
    for attempt in range(4):
        try:
            r = requests.get(BASE + path, headers=HEADERS, timeout=60)
            if r.status_code == 200:
                if cache:
                    cache.parent.mkdir(parents=True, exist_ok=True)
                    cache.write_bytes(r.content)
                time.sleep(REQUEST_DELAY)
                return decode(r.content)
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:  # noqa: PERF203
            last = str(e)
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {path} failed: {last}")  # a missing year must not silently shrink the corpus


def clean(x: str) -> str:
    x = re.sub(r"<br[^>]*>", "\n", x, flags=re.I)
    x = re.sub(r"<[^>]+>", "", x)
    x = html.unescape(x).replace("　", " ").replace("\xa0", " ")
    return "\n".join(re.sub(r"\s+", " ", l).strip() for l in x.split("\n") if l.strip())


def table_rows(t: str) -> list[list[dict]]:
    t = re.sub(r"<script.*?</script>|<style.*?</style>|<!--.*?-->", "", t, flags=re.S | re.I)
    rows = []
    for tr in re.findall(r"<tr[^>]*>(.*?)(?=<tr[^>]*>|</table>)", t, re.S | re.I):
        cells = []
        for _tag, attrs, c in re.findall(r"<(t[dh])([^>]*)>(.*?)(?=<t[dh][^>]*>|</tr>|$)", tr, re.S | re.I):
            rs = re.search(r"rowspan=\"?(\d+)", attrs, re.I)
            cs = re.search(r"colspan=\"?(\d+)", attrs, re.I)
            cells.append({"rs": int(rs.group(1)) if rs else 1, "cs": int(cs.group(1)) if cs else 1,
                          "text": clean(c), "hrefs": re.findall(r"href=\"([^\"]+)\"", c, re.I)})
        rows.append(cells)
    return rows


def grid(rows: list[list[dict]]) -> list[dict[int, tuple[dict, bool]]]:
    """HTML table model: expand rowspan/colspan so every row maps column -> (cell, is_origin)."""
    out, pending = [], {}
    for cells in rows:
        row, col, ci = {}, 0, 0
        while ci < len(cells) or any(c >= col for c in pending):
            if col in pending:
                cell, rem = pending[col]
                row[col] = (cell, False)
                if rem > 1:
                    pending[col] = (cell, rem - 1)
                else:
                    del pending[col]
                col += 1
                continue
            if ci >= len(cells):
                col += 1
                continue
            cell = cells[ci]
            ci += 1
            for k in range(cell["cs"]):
                row[col + k] = (cell, k == 0)
                if cell["rs"] > 1:
                    pending[col + k] = (cell, cell["rs"] - 1)
            col += cell["cs"]
        out.append(row)
    return out


def nospace(s: str) -> str:
    # header cells spell the long vowel several ways ("テ ― マ", "テ－マ")
    return re.sub(r"\s+", "", s).translate(str.maketrans("―—－‐-ｰ─", "ーーーーーーー"))


def parse_page(prog: str, fy: int, page: str) -> list[dict]:
    plain = re.sub(r"<[^>]+>", "", page)
    unit_txt = re.search(r"単位[：:\s]*([^）)、,]{1,4})", plain)
    unit = {"万円": 10000, "千円": 1000, "円": 1}.get(unit_txt.group(1).strip() if unit_txt else "", None)
    if unit is None:
        raise RuntimeError(f"{prog} FY{fy}: amount unit caption not found")
    recs, cols, section, cur = [], None, None, None
    for row in grid(table_rows(page)):
        texts = {c: cell["text"] for c, (cell, origin) in row.items() if origin}
        joined = nospace(" ".join(texts.values()))
        if any("テーマ" in nospace(v) for v in texts.values()):
            cols = {}
            for c, v in texts.items():
                v = nospace(v)
                if "テーマ" in v:
                    cols["theme"] = c
                elif "研究者" in v or "代表者" in v:
                    cols["res"] = c
                elif "金額" in v or v == "助成":  # "助成金額", or "助成" over a "金額" row (jare FY2020)
                    cols["amt"] = c
                elif "成果" in v or "研究結果" in v:
                    cols["rep"] = c
                elif "分野" in v:
                    cols["field"] = c
            cur = None
            continue
        if re.search(r"合計|小計", joined):  # totals row ("合計 82 件 | 41,850千円 + 166,200米ドル")
            cur = None
            continue
        if prog == "kankyo" and len(texts) == 1:  # section captions: 課題研究 (themed) / 一般研究 (general)
            if "一般研究" in joined:
                section = "一般研究"
            elif "課題研究" in joined:
                section = "課題研究"
        if not cols or "theme" not in cols:
            continue
        tcell = row.get(cols["theme"])
        if tcell is None:
            continue
        cell, origin = tcell
        # a grant starts on a row that has its own theme cell AND its own amount cell
        # (the amount cell spans the grant's rows); theme-column text on a row whose
        # amount column is still spanned is a status note on the current grant
        amt_cell = row.get(cols["amt"]) if "amt" in cols else None
        starts = bool(origin and cell["text"].strip() and ("amt" not in cols or (amt_cell is not None and amt_cell[1])))
        if starts:
            cur = {"prog": prog, "fy": fy, "section": section, "theme": cell["text"], "res": [], "amt": [],
                   "hrefs": [], "other": [], "field": None, "note": None}
            recs.append(cur)
            if "field" in cols and cols["field"] in row:
                cur["field"] = row[cols["field"]][0]["text"]
        elif cur is None:
            continue
        elif origin and cell["text"].strip():
            # "（継続）" = renewal of the previous FY's grant, "（中止）" = discontinued
            if re.fullmatch(r"[（(][^）)]{1,8}[)）]", nospace(cell["text"])):
                cur["note"] = nospace(cell["text"]).strip("（）()")
            else:
                continue  # caption/footnote row, not part of a grant
        for c, (cl, org) in row.items():
            if not org or c == cols["theme"]:
                continue
            if c == cols.get("res"):
                cur["res"] += cl["text"].split("\n") if cl["text"] else []
            elif c == cols.get("amt"):
                if cl["text"]:
                    cur["amt"].append(cl["text"])
            elif c == cols.get("field"):
                pass
            else:
                if cl["text"]:
                    cur["other"].append(cl["text"])
            cur["hrefs"] += cl["hrefs"]
    out = []
    # drop non-grant rows that sit in the theme column: totals ("合 計 102 件"),
    # footnotes ("※..."), copyright lines, section captions without a researcher
    recs = [r for r in recs if r["res"] and not re.match(r"^(合\s*計|小\s*計|※|©)", r["theme"].strip())]
    for k, r in enumerate(recs, 1):
        r["ordinal"] = k
        amt_raw = " ".join(r["amt"]).strip()
        m = re.search(r"([$＄])?\s*[\\¥￥]?\s*([\d,，]+(?:\.\d+)?)", amt_raw)
        amount = currency = None
        if m:
            num = float(m.group(2).replace(",", "").replace("，", ""))
            if m.group(1):
                amount, currency = num, "USD"
            else:
                amount, currency = num * unit, "JPY"
        r["amount_text"], r["amount"], r["currency"], r["unit_jpy"] = amt_raw or None, amount, currency, unit
        out.append(r)
    return out


ORG_HINT = re.compile(r"大学|研究所|研究科|学部|学院|機構|センター|病院|研究院|研究機構|法人|財団|協会|館|会社|株式|省|庁|県|市|研究室|科学院|校|Univ|Institute|College|学校")
ROLE_HINT = re.compile(r"教授|准教授|助教|講師|研究員|教師|教員|リーダー|主任|主幹|部長|室長|所長|センター長|学長|院長|フェロー|助手|学生|課程|ポスドク|特任|研究者|専門員|技師|医長|医師|科長|上席|主席|理事|代表|Professor|Lecturer|Researcher|Fellow|Director|研究生|学芸員|准研究員")
COUNTRY_RE = re.compile(r"[（(]([^（）()]{1,12})[)）]\s*$")
CO_RE = re.compile(r"[（(]?\s*他\s*(\d+)\s*名\s*[)）]?")
CJK = re.compile(r"[一-鿿㐀-䶿豈-﫿]")
KANA = re.compile(r"[゠-ヿ]")


COUNTRY_ISO = {
    "中国": "CN", "台湾": "TW", "韓国": "KR", "香港": "HK", "マカオ": "MO", "モンゴル": "MN",
    "タイ": "TH", "ベトナム": "VN", "インドネシア": "ID", "マレーシア": "MY", "フィリピン": "PH",
    "シンガポール": "SG", "ミャンマー": "MM", "カンボジア": "KH", "ラオス": "LA", "ブルネイ": "BN",
    "東ティモール": "TL", "バングラデシュ": "BD", "バングラディシュ": "BD", "インド": "IN",
    "スリランカ": "LK", "ネパール": "NP", "パキスタン": "PK", "イラン": "IR", "ブータン": "BT",
    "オーストラリア": "AU", "豪州": "AU", "米国": "US", "アメリカ": "US", "英国": "GB", "イギリス": "GB",
    "カナダ": "CA", "ドイツ": "DE", "フランス": "FR", "日本": "JP", "ニュージーランド": "NZ",
    "オランダ": "NL", "スイス": "CH", "スウェーデン": "SE", "イタリア": "IT", "ベルギー": "BE",
}


def split_researcher(lines: list[str], other: list[str], prog: str) -> dict:
    lines = [l for l in (x.strip() for x in lines) if l]
    if not lines:
        return {}
    # a romanisation bracket can wrap onto the next line: "暴 図亜 〔Tuya" / "BAO〕"
    while "〔" in lines[0] and "〕" not in lines[0] and len(lines) > 1:
        lines = [lines[0] + " " + lines[1]] + lines[2:]
    name_line = lines[0]
    co = CO_RE.search(" ".join(lines))
    n_co = int(co.group(1)) if co else 0
    roman = re.search(r"〔([^〕]+)〕", name_line)
    name = CO_RE.sub("", re.sub(r"〔[^〕]*〕|[（(]所属なし[)）]", "", name_line)).strip()
    rest = [CO_RE.sub("", l).strip() for l in lines[1:] if nospace(l) not in {"所属なし", "（所属なし）"}]
    rest = [l for l in rest if l]
    country = None
    if prog == "jare":
        # researcher's country/region: trailing "(中国)" on the last line, or a bare
        # country cell beside the researcher cell (jare FY2021/FY2024 layouts)
        for i in range(len(rest) - 1, -1, -1):
            m = COUNTRY_RE.search(rest[i])
            if m and nospace(m.group(1)) in COUNTRY_ISO:
                country = nospace(m.group(1))
                rest[i] = rest[i][: m.start()].strip()
                break
        if country is None:
            for o in other:
                o = nospace(o).strip("（）()")
                if o in COUNTRY_ISO:
                    country = o
                    break
    rest = [l for l in rest if l]
    position = None
    if len(rest) >= 2 or (len(rest) == 1 and ROLE_HINT.search(rest[-1]) and not ORG_HINT.search(rest[-1])):
        if ROLE_HINT.search(rest[-1]) or not ORG_HINT.search(rest[-1]):
            position = rest.pop()
    inst = " ".join(rest).strip() or None
    # institution's own country when printed after it: "エディスコーワン大学＜オーストラリア＞"
    inst_cc = None
    if inst:
        m = re.search(r"[＜<]([^＞>]{1,10})[＞>]", inst)
        if m and nospace(m.group(1)) in COUNTRY_ISO:
            inst_cc = COUNTRY_ISO[nospace(m.group(1))]
    # names: kanji names are printed family-first ("浅香 猛"), and so are Latin and
    # space-separated katakana names in these Japanese-ordered lists ("Hobro Alison Jane");
    # dot-joined katakana transliterations keep the source's given-first order
    # ("モニール・ホッサン・モニ" -> family "モニ").
    toks = [t for t in re.split(r"\s+", name.strip()) if t]  # family-first order, so not split_name (§2.4.1)
    given = family = None
    if len(toks) >= 2:
        family, given = toks[0], " ".join(toks[1:])
    elif re.search(r"[・･]", name):
        parts = [x for x in re.split(r"[・･]", name) if x]
        family, given = parts[-1], "・".join(parts[:-1]) or None
    elif toks:
        family = toks[0]
    return {"lead_name": name or None, "lead_given_name": given, "lead_family_name": family,
            "lead_name_romanised": roman.group(1).strip() if roman else None,
            "lead_institution": inst, "lead_position": position,
            "lead_country": country,
            "lead_country_code": inst_cc or (COUNTRY_ISO.get(country) if country else ("JP" if prog != "jare" else None)),
            "n_co_investigators": n_co}


def grant_number(fy: int, hrefs: list[str]) -> tuple[str | None, int | None]:
    """(grant number, list ordinal) from a results-report PDF name such as
    "001-180853.pdf", "001-2200735_name_....pdf", "Aikawa_Masahide_Seika_173025.pdf"
    or "jare19-04_198035.pdf". The ordinal is the leading "NNN-" list position (kiso)."""
    yy = f"{fy % 100:02d}"
    for h in hrefs:
        fn = requests.utils.unquote(h.rsplit("/", 1)[-1])
        ordm = re.match(r"(\d{3})-", fn)
        for d in re.findall(r"(?<!\d)(\d{6,7})(?!\d)", fn):
            if d.startswith(yy):
                return d, int(ordm.group(1)) if ordm else None
    return None, None


def main() -> None:
    p = argparse.ArgumentParser(description="Sumitomo Foundation research grant lists -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only parse the first N pages (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    todo = pages()
    if args.limit:
        todo = todo[: args.limit]
    rows = []
    for i, (prog, fy, path) in enumerate(todo, 1):
        page = fetch(path, args.cache_dir)
        recs = parse_page(prog, fy, page)
        log(f"[{i}/{len(todo)}] {prog} FY{fy}: {len(recs)} grants  ({path})")
        if not recs:
            raise RuntimeError(f"{prog} FY{fy}: 0 grants parsed from {path}")  # never silently skip a year
        for r in recs:
            person = split_researcher(r["res"], r["other"], prog)
            num, num_ord = grant_number(fy, r["hrefs"])
            title = re.sub(r"\s*\n\s*", "", r["theme"]).strip()
            key_src = f"{person.get('lead_name') or ''}|{title}"
            synth = f"SF-{prog.upper()}-{fy}-{hashlib.md5(key_src.encode('utf-8')).hexdigest()[:8]}"
            rows.append({
                "programme": prog,
                "funder_scheme": SCHEMES[prog][0],
                "funding_type": SCHEMES[prog][1],
                "fiscal_year": str(fy),
                "section": r["section"],
                "status_note": r["note"],
                "field": r["field"],
                "title": title or None,
                "grant_number": num,
                "grant_number_ordinal": num_ord,
                "list_ordinal": r["ordinal"],
                "funder_award_id": None,
                "synthetic_key": synth,
                "amount_text": r["amount_text"],
                "amount": r["amount"],
                "currency": r["currency"],
                "amount_unit_jpy": r["unit_jpy"],
                **person,
                "report_pdf_url": next((requests.compat.urljoin(BASE + path, h) for h in r["hrefs"] if h.lower().endswith(".pdf")), None),
                "researcher_raw": "\n".join(r["res"]),
                "landing_page_url": BASE + path,
            })

    df = pd.DataFrame(rows)
    # The site occasionally links the same report number from two rows (copy-paste:
    # kiso FY2017 "019-170502.pdf" on rows 19 and 20; kiso FY2015 "027-150743" and
    # "096-150743"; jare FY2019 "198401-0xx" on three rows). Keep a shared number only
    # on the single row whose list position matches the file's "NNN-" prefix; otherwise
    # it is ambiguous and every sharing row falls back to its synthetic key.
    shared = df["grant_number"].notna() & df.duplicated(["programme", "fiscal_year", "grant_number"], keep=False)
    for (_, _, num), g in df[shared].groupby(["programme", "fiscal_year", "grant_number"]):
        own = g[g["grant_number_ordinal"] == g["list_ordinal"]]
        drop = g.index.difference(own.index) if len(own) == 1 else g.index
        log(f"  grant number {num} linked from {len(g)} rows; kept on {len(g) - len(drop)}")
        df.loc[drop, "grant_number"] = None
    df["funder_award_id"] = df["grant_number"].fillna(df["synthetic_key"])
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, ['funder_award_id', 'programme', 'fiscal_year', 'lead_name']].to_dict('records')}")
    log(f"Parsed {len(df)} grants from {len(todo)} pages")
    for prog, g in df.groupby("programme"):
        log(f"  {prog:7s} {len(g):5d} grants FY{g.fiscal_year.min()}-{g.fiscal_year.max()}, "
            f"{g.grant_number.notna().sum()} with published grant number")
    for c in ["title", "amount", "lead_name", "lead_family_name", "lead_institution", "lead_country", "grant_number"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  currencies {df.currency.value_counts(dropna=False).to_dict()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    df = df.astype("string")
    parquet_path = args.output_dir / "sumitomo_foundation_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_sumitomo_foundation_projects.parquet"
    try:
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
