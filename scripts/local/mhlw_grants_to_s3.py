#!/usr/bin/env python3
"""
MHLW Grants System (Health, Labour and Welfare Sciences Research Grants) to S3
===============================================================================

The Ministry of Health, Labour and Welfare (MHLW, OpenAlex F4320321945)
publishes every project funded by its 厚生労働科学研究費補助金 (Health, Labour
and Welfare Sciences Research Grants) in the MHLW GRANTS SYSTEM run by the
National Institute of Public Health: https://mhlw-grants.niph.go.jp/

Ladder item 0 (bulk export): the site's search has a "CSV出力" button
(/search/csv?<search params>) backed by a Drupal views_data_export batch. We
run one export per research fiscal year (years=YYYY), 1997..current. Each CSV
row is ONE REPORT (a 総括 annual report, or a 総合 multi-year summary report),
59 columns: report no. (文献番号), project no. (課題番号), grant category
(研究区分), fiscal year, start / planned end FY, annual research funding
(研究費, JPY), title, PI + affiliation, co-investigators, abstract sections,
and the finance report.

The script aggregates reports to ONE ROW PER PROJECT (課題番号):
  - title / PI / affiliation / category: from the latest annual report
  - amount: sum of 研究費 over the project's annual (総括) reports, one per FY, JPY
  - start / end FY: 開始年度 / 終了予定年度 (Japanese FY: Apr 1 .. Mar 31)
  - description: 研究目的 (research aim) of the latest annual report
  - landing page: /project/<nid> of the latest report (from the paged result HTML)

Scope filter: only 研究区分 whose grant name is an MHLW research grant
(厚生労働科学研究費補助金, its pre-2001 name 厚生科学研究費補助金, and
厚生労働行政推進調査事業費補助金, and the late-1990s Ministry of Health and Welfare
特定疾患調査研究補助金 / 心身障害研究費補助金). The DB also hosts 2023+ grants of the
Children and Families Agency (こども家庭科学研究費補助金) and 2024+ grants of
the Consumer Affairs Agency (食品衛生基準科学研究費補助金); those are other
funders and are dropped.

Output: s3://openalex-ingest/awards/mhlw_grants/mhlw_grants_projects.parquet
"""

import argparse
import hashlib
import html
import json
import re
import unicodedata
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Same block as twcf_to_s3.py (runbook §1.2 item 7); the §4.0 grep looks for
# sys.stdout.reconfigure, which this shim calls as _sys_utf8.stdout.reconfigure.
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

BASE = "https://mhlw-grants.niph.go.jp"
FIRST_YEAR = 1997
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mhlw_grants/mhlw_grants_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 4
PER_PAGE = 100

# Grant names (first token of 研究区分) that are MHLW research money.
MHLW_GRANTS = ("厚生労働科学研究費", "厚生科学研究費", "厚生労働行政推進調査事業費",
               "特定疾患調査研究補助金", "心身障害研究費")

COLS = {
    "文献番号（総括・総合）": "report_no",
    "課題番号": "project_no",
    "研究区分": "category",
    "研究年度": "fiscal_year",
    "報告書区分（総括・総合）": "report_type",
    "開始年度": "start_fy",
    "終了予定年度": "end_fy",
    "研究費": "research_funding",
    "研究者交替、所属機関変更": "pi_change",
    "研究課題名": "title",
    "研究代表者名（所属機関）": "pi",
    "研究分担者名（所属機関）": "co_investigators",
    "研究目的": "aim",
    "概要": "summary",
    "（１）補助金交付額": "grant_awarded",
    "（２）補助金確定額": "grant_confirmed",
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(s: requests.Session, url: str, **kw) -> requests.Response:
    last = None
    for attempt in range(RETRIES):
        try:
            r = s.get(url, timeout=300, **kw)
            if r.status_code >= 500:
                raise RuntimeError(f"HTTP {r.status_code}")
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            log(f"  GET {url} {kw.get('params')} failed ({e}); retry {attempt + 1}/{RETRIES}")
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def export_year_csv(year: int) -> bytes:
    """Drive the views_data_export batch for one research FY and return the CSV bytes."""
    s = requests.Session()
    s.headers.update(HEADERS)
    r = get(s, f"{BASE}/search/csv", params={"years": str(year), "items_per_page": str(PER_PAGE)},
            allow_redirects=False)
    m = re.search(r"batch\?id=(\d+)", r.headers.get("Location", ""))
    if not m:
        raise RuntimeError(f"{year}: no batch redirect (HTTP {r.status_code})")
    bid = m.group(1)
    get(s, f"{BASE}/batch", params={"id": bid, "op": "start"})
    for _ in range(2000):
        r = get(s, f"{BASE}/batch", params={"id": bid, "op": "do_nojs"})
        if re.search(r'http-equiv="Refresh"[^>]*op=finished', r.text):
            break
        time.sleep(REQUEST_DELAY)
    else:
        raise RuntimeError(f"{year}: batch {bid} never finished")
    r = get(s, f"{BASE}/batch", params={"id": bid, "op": "finished"})
    m = re.search(r'href="([^"]+views_data_export[^"]+\.csv)"', r.text)
    if not m:
        raise RuntimeError(f"{year}: batch {bid} finished without a download link")
    r = get(s, html.unescape(m.group(1)))
    if not r.content.startswith(b"\xef\xbb\xbf") and b"," not in r.content[:200]:
        raise RuntimeError(f"{year}: download is not a CSV")
    return r.content


def result_page_nids(year: int) -> tuple[int, dict[str, str]]:
    """Paged search HTML for one FY -> (hit count, {report_no: project nid})."""
    s = requests.Session()
    s.headers.update(HEADERS)
    out, hits, page, pages = {}, None, 0, None
    consecutive_bad = 0
    while pages is None or page < pages:
        r = get(s, f"{BASE}/search", params={"years": str(year), "items_per_page": str(PER_PAGE), "page": str(page)})
        if r.status_code != 200:
            consecutive_bad += 1
            log(f"  {year} page {page}: HTTP {r.status_code} ({consecutive_bad}/5); continuing")
            if consecutive_bad >= 5:
                raise RuntimeError(f"{year}: 5 consecutive non-200 result pages")
            page += 1
            continue
        consecutive_bad = 0
        if hits is None:
            m = re.search(r"ヒット件数：([\d,]+)", r.text)
            hits = int(m.group(1).replace(",", "")) if m else 0
            pages = -(-hits // PER_PAGE)
        for chunk in r.text.split('search-page-row__number-report">')[1:]:
            nos = re.findall(r"\d{9}[A-Z]", chunk[:200])
            nid = re.search(r'href="/project/(\d+)', chunk)
            for no in nos:
                if nid:
                    out[no] = nid.group(1)
        page += 1
        time.sleep(REQUEST_DELAY)
    return hits or 0, out


def western_year(s) -> int | None:
    """'令和4（2022）' / '平成22（2010）' -> 2022 / 2010."""
    if not isinstance(s, str):
        return None
    m = re.search(r"[（(]\s*(\d{4})\s*[）)]", s) or re.search(r"\b(19|20)\d{2}\b", s)
    return int(m.group(1)) if m else None


def yen(s) -> float | None:
    if not isinstance(s, str):
        return None
    d = re.sub(r"[^\d]", "", s)
    return float(d) if d else None


def trailing_group(s: str) -> tuple[str, str | None]:
    """Split 'NAME（AFFIL）' at the LAST balanced top-level paren group:
    '津下　一代（丹羽　一代）(女子栄養大学 栄養学部)' -> ('津下　一代（丹羽　一代）', '女子栄養大学 栄養学部')."""
    s = s.strip()
    if not s or s[-1] not in "）)":
        return s, None
    depth = 0
    for i in range(len(s) - 1, -1, -1):
        c = s[i]
        if c in "）)":
            depth += 1
        elif c in "（(":
            depth -= 1
            if depth == 0:
                return s[:i].strip(), s[i + 1:-1].strip() or None
    return s, None


def split_name_ja(name: str) -> tuple[str | None, str | None, str | None]:
    """Japanese names in this DB are FAMILY<space>GIVEN (full- or half-width space),
    optionally followed by a former/maiden name in parens. Returns (given, family, alt).
    Names without a separating space are left unsplit (family=None, given=None) rather
    than guessed (runbook §2.4.1: NULL rather than guess). Latin-script names
    ('John Smith') go through the canonical given-first split."""
    if not name:
        return None, None, None
    alt = None
    m = re.search(r"[（(]([^（()）]*)[）)]\s*$", name)
    if m:
        alt, name = m.group(1).strip() or None, name[: m.start()]
    tokens = re.split(r"[\s　]+", name.strip())
    tokens = [t for t in tokens if t]
    if not tokens:
        return None, None, alt
    if all(re.fullmatch(r"[A-Za-z.\-']+", t) for t in tokens):
        suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
        while tokens and tokens[-1].lower().strip(",.") in suffixes:
            tokens.pop()
        if len(tokens) < 2:
            return None, (tokens[0] if tokens else None), alt
        return " ".join(tokens[:-1]), tokens[-1], alt
    if len(tokens) < 2:
        return None, None, alt
    return " ".join(tokens[1:]), tokens[0], alt


def parse_person(raw: str) -> dict | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    name, affil = trailing_group(raw)
    given, family, alt = split_name_ja(name)
    return {"name": re.sub(r"[\s　]+", " ", name).strip(), "given_name": given,
            "family_name": family, "former_name": alt, "affiliation": affil}


def parse_people(raw) -> list[dict]:
    """'今井　健(東京大学　…)、今村　知明(…)' -> list; split only on 、 after a closing paren."""
    if not isinstance(raw, str) or not raw.strip():
        return []
    parts = re.split(r"(?<=[）)])\s*[、,]\s*", raw.strip())
    return [p for p in (parse_person(x) for x in parts) if p]


def load_reports(cache_dir: Path, years: list[int]) -> tuple[pd.DataFrame, dict[str, str], dict[int, int]]:
    frames, nids, hits = [], {}, {}
    for y in years:
        csv_path = cache_dir / f"mhlw_{y}.csv"
        nid_path = cache_dir / f"mhlw_{y}_nids.json"
        if not csv_path.exists():
            t0 = time.time()
            b = export_year_csv(y)
            csv_path.write_bytes(b)
            log(f"{y}: exported CSV {len(b):,} bytes in {time.time() - t0:.0f}s")
        if not nid_path.exists():
            h, m = result_page_nids(y)
            nid_path.write_text(json.dumps({"hits": h, "nids": m}))
            log(f"{y}: {h} search hits, {len(m)} report->nid links")
        meta = json.loads(nid_path.read_text())
        hits[y] = meta["hits"]
        nids.update(meta["nids"])
        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig", keep_default_na=False, na_values=[""])
        missing = [c for c in COLS if c not in df.columns]
        if missing:
            raise RuntimeError(f"{y}: CSV lacks expected columns {missing}")
        df = df[list(COLS)].rename(columns=COLS)
        df["export_year"] = y
        log(f"{y}: {len(df)} report rows, {df['project_no'].nunique()} project numbers")
        frames.append(df)
    return pd.concat(frames, ignore_index=True), nids, hits


def award_key(s: str) -> str:
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", s or "")).lower()


def build_projects(rep: pd.DataFrame, nids: dict[str, str]) -> pd.DataFrame:
    rep = rep.copy()
    for c in rep.columns:
        if rep[c].dtype == object:
            rep[c] = rep[c].map(lambda v: v.strip() if isinstance(v, str) else v)
    rep["grant_name"] = rep["category"].fillna("").str.split(r"[\s　]+").str[0]
    keep = rep["grant_name"].str.startswith(MHLW_GRANTS)
    dropped = rep.loc[~keep, "grant_name"].value_counts()
    for g, n in dropped.items():
        log(f"  scope filter: dropped {n} report rows of {g!r}")
    rep = rep[keep].copy()
    rep["fy"] = rep["fiscal_year"].map(western_year)
    rep["report_no"] = rep["report_no"].fillna("")
    # Reports before ~FY2002 carry no 課題番号 (the column is "-"). A multi-year project
    # then has one report per FY with the same title, PI and start FY, so we key those
    # on a synthetic MHLW-{startFY}-{sha1(title|PI name)[:10]} (prefixed so it can never
    # collide with a real project number).
    pno = rep["project_no"].fillna("")
    no_pno = ~pno.map(award_key).str.contains(r"[0-9a-z]", regex=True)
    def synth(r) -> str:
        pi_name, _ = trailing_group(r["pi"] if isinstance(r["pi"], str) else "")
        title = r["title"] if isinstance(r["title"], str) else ""
        basis = re.sub(r"[\s　]+", "", f"{title}|{pi_name}")
        sfy = western_year(r["start_fy"]) or r["fy"] or 0
        return f"MHLW-{sfy}-{hashlib.sha1(basis.encode('utf-8')).hexdigest()[:10]}"
    if no_pno.any():
        rep.loc[no_pno, "project_no"] = rep.loc[no_pno].apply(synth, axis=1)
    rep["project_no_synthetic"] = no_pno
    log(f"  {int(no_pno.sum())} report rows without 課題番号 keyed on synthetic MHLW-{{startFY}}-{{hash}}")
    # NFKC so full-/half-width variants of the same number across years (Ｈ21 / H21, ･ / ・) group together
    rep["key"] = rep["project_no"].map(award_key)
    # Projects that straddle the introduction of 課題番号 (~FY2004/05) have early reports
    # without a number and later ones with it: fold the unnumbered reports into the
    # numbered project when (title, PI name, start FY) identifies exactly one of them.
    def ident(r) -> str:
        pi_name, _ = trailing_group(r["pi"] if isinstance(r["pi"], str) else "")
        title = r["title"] if isinstance(r["title"], str) else ""
        sfy = western_year(r["start_fy"]) or 0
        return re.sub(r"[\s　]+", "", unicodedata.normalize("NFKC", f"{title}|{pi_name}|{sfy}"))
    rep["ident"] = rep.apply(ident, axis=1)
    numbered = rep[~rep["project_no_synthetic"]].groupby("ident")["key"].agg(lambda s: sorted(set(s)))
    unique_num = {i: ks[0] for i, ks in numbered.items() if len(ks) == 1}
    fold = rep["project_no_synthetic"] & rep["ident"].isin(unique_num)
    rep.loc[fold, "key"] = rep.loc[fold, "ident"].map(unique_num)
    rep.loc[fold, "project_no_synthetic"] = False
    log(f"  {int(fold.sum())} unnumbered report rows folded into their numbered project")

    rows = []
    for key, g in rep.groupby("key", sort=True):
        g = g.sort_values(["fy", "report_type"], ascending=[True, True])
        annual = g[g["report_type"].fillna("").str.contains("総括")]
        latest = (annual if len(annual) else g).iloc[-1]
        # one 研究費 per FY (dedupe re-exports of the same annual report)
        per_fy = annual.dropna(subset=["research_funding"]).drop_duplicates(subset=["fy"], keep="last")
        amounts = [yen(v) for v in per_fy["research_funding"]]
        amounts = [a for a in amounts if a is not None]
        start_fy = pd.Series([western_year(v) for v in g["start_fy"]]).dropna()
        end_fy = pd.Series([western_year(v) for v in g["end_fy"]]).dropna()
        pi = parse_person(latest["pi"]) or {}
        co = parse_people(latest["co_investigators"])
        latest_no = latest["report_no"]
        nid = nids.get(latest_no) or next((nids[n] for n in reversed(g["report_no"].tolist()) if n in nids), None)
        category = latest["category"]
        rows.append({
            "project_no": latest["project_no"],
            "project_no_synthetic": str(bool(latest["project_no_synthetic"])).lower(),
            "title": latest["title"],
            "category": category,
            "grant_name": latest["grant_name"],
            "scheme": re.sub(r"^\S+[\s　]+", "", category or "").replace("　", " ").strip() or None,
            "start_fy": int(start_fy.min()) if len(start_fy) else (int(g["fy"].min()) if g["fy"].notna().any() else None),
            "end_fy": int(end_fy.max()) if len(end_fy) else (int(g["fy"].max()) if g["fy"].notna().any() else None),
            "report_fys": ",".join(str(int(v)) for v in sorted(set(g["fy"].dropna()))),
            "n_reports": len(g),
            "amount_jpy": sum(amounts) if amounts else None,
            "amount_fys": len(amounts),
            "annual_funding_json": json.dumps({str(int(f)): yen(v) for f, v in zip(per_fy["fy"], per_fy["research_funding"]) if pd.notna(f)}),
            "grant_awarded_latest": latest["grant_awarded"],
            "pi_raw": latest["pi"],
            "pi_name": pi.get("name"),
            "lead_given_name": pi.get("given_name"),
            "lead_family_name": pi.get("family_name"),
            "lead_former_name": pi.get("former_name"),
            "lead_affiliation": pi.get("affiliation"),
            "co_investigators_json": json.dumps(co, ensure_ascii=False) if co else None,
            "pi_change": latest["pi_change"],
            # latest report that has an aim (the newest FY's summary is often not yet posted)
            "aim": next((v for v in reversed(g["aim"].tolist()) if isinstance(v, str)),
                        next((v for v in reversed(g["summary"].tolist()) if isinstance(v, str)), None)),
            "report_nos": ",".join(sorted(set(n for n in g["report_no"] if n))),
            "latest_report_no": latest_no,
            "landing_page_url": f"{BASE}/project/{nid}" if nid else f"{BASE}/search?report_no={latest_no}",
        })
    return pd.DataFrame(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="MHLW Grants System CSV exports -> projects parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the N most recent fiscal years (smoke test)")
    p.add_argument("--first-year", type=int, default=FIRST_YEAR)
    p.add_argument("--last-year", type=int, default=datetime.now().year)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache per-year CSV + nid maps (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    years = list(range(args.first_year, args.last_year + 1))
    if args.limit:
        years = years[-args.limit:]
    cache = args.cache_dir or (args.output_dir / "mhlw_cache")
    cache.mkdir(parents=True, exist_ok=True)

    rep, nids, hits = load_reports(cache, years)
    log(f"Reports: {len(rep)} rows over FY{years[0]}-{years[-1]}; search hits {sum(hits.values())}")
    df = build_projects(rep, nids)
    dupes = df["project_no"].map(award_key).duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate project numbers after aggregation: {df.loc[dupes, 'project_no'].tolist()[:20]}")
    log(f"Projects: {len(df)}")
    for c in ["title", "amount_jpy", "start_fy", "end_fy", "pi_name", "lead_family_name",
              "lead_affiliation", "co_investigators_json", "aim"]:
        log(f"  {c:24s} {df[c].notna().mean():6.1%}")
    log(f"  landing /project/ nid     {df['landing_page_url'].str.contains('/project/').mean():6.1%}")
    log(f"  total JPY {df['amount_jpy'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "mhlw_grants_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_mhlw_grants_projects.parquet"
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
