#!/usr/bin/env python3
"""
Russian Foundation for Basic Research (RFBR / РФФИ) -> S3  (GRANT PATTERN)
=========================================================================

Harvests the RFBR legacy project archive from the foundation's public
portal at `www.rfbr.ru/project_search` and uploads a parquet of funded
projects to S3.

Funder
------
Russian Foundation for Basic Research (Российский фонд фундаментальных
исследований, RFBR/РФФИ) — merged into the Russian Science Foundation in
2022; the legacy portal is still maintained by РЦНИ (RCSI).
  OpenAlex funder_id : 4320321079  (F4320321079)
  ROR                : https://ror.org/02mh1ke95
  DOI                : 10.13039/501100002261
  country            : RU

Archive status (probed 2026-07-12)
----------------------------------
- kias.rfbr.ru        : reachable, but it is the КИАС РЦНИ login portal —
                        no anonymous project catalog.
- search.rfbr.ru      : DEAD (connection failure). rfbr.ru still links to
                        it as the "2012+" search; those links are broken.
- www.rfbr.ru/project_search : WORKS. Public archive of RFBR
  applications and grants. Coverage by "Год проведения": 1993–2018 is
  dense (~700–1,200 listing pages/year × 20 rows); 2019 has only 14
  rows; 2020+ is EMPTY. RFBR grants from 2019–2021 are therefore NOT
  harvestable from any public RFBR source found (documented gap).

What the portal exposes
-----------------------
Listing rows (`?GRANT_YEAR={y}&page={p}`, 20 rows/page) carry:
  native grant number, title (Russian), year, research area, contest
  type, and application status (поддержана / не поддержана) — the
  archive MIXES funded grants and rejected applications; ~61% of a
  random page sample is "поддержана". Rejected applications are dropped
  at build time (they are not awards).
Detail pages (`/project_search/{internal_id}/`) additionally carry the
PI (Руководитель, Russian order Family Given Patronymic) and the
application abstract. NO per-project amounts and NO host organization
anywhere.

Two-phase harvest
-----------------
Phase 1 — enumerate ALL listing pages per year (1993–2019). The page-1
  pagination block gives the authoritative last-page number per year;
  empty/failed pages mid-run are retried and never treated as
  end-of-corpus (runbook §1). ~21k pages, ~4s each server-side.
Phase 2 — fetch detail pages (fast, ~0.5s) to enrich with PI + abstract
  for the grants OpenAlex already cites (funder F4320321079 /awards
  funder_award_ids, ~17.8k unique well-formed numbers, of which ~13k
  fall in the portal's covered years). Detail-fetching all ~250k
  supported rows would double-to-triple the runtime for rows no work
  cites yet; a later run can extend enrichment with --details-all.

Amount
------
The portal publishes NO per-project amounts -> amount/currency NULL on
every row; Step 6.7 amount check WAIVED (documented). start_year =
"Год проведения" from the listing row.

Output
------
s3://openalex-ingest/awards/rfbr/rfbr_projects.parquet

Usage
-----
    python rfbr_to_s3.py                       # full harvest (resumable)
    python rfbr_to_s3.py --limit 40            # smoke: cap listing pages + details
    python rfbr_to_s3.py --skip-upload
    python rfbr_to_s3.py --output-dir DIR
    python rfbr_to_s3.py --allow-shrink
    python rfbr_to_s3.py --skip-download       # build parquet from checkpoints only
    python rfbr_to_s3.py --workers 4
    python rfbr_to_s3.py --details-all         # enrich EVERY supported row (slow)

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

import argparse
import json
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import unescape
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (runbook §1.2) ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass
if sys.platform == "win32":
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

# =============================================================================
# Configuration
# =============================================================================

FUNDER_ID = 4320321079
FUNDER_DISPLAY_NAME = "Russian Foundation for Basic Research"
PROVENANCE = "rfbr"
OPENALEX_FUNDER_ID = "F4320321079"

BASE = "https://www.rfbr.ru"
SEARCH_URL = BASE + "/project_search"
YEARS = [str(y) for y in range(1993, 2020)]  # 2020+ empty (probed 2026-07-12)

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rfbr/rfbr_projects.parquet"
PARQUET_NAME = "rfbr_projects.parquet"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 "
              "openalex-walden-awards-ingest/1.0 (+https://openalex.org)")

LIST_INTERVAL_S = 0.3    # per worker politeness for listing pages
DETAIL_INTERVAL_S = 0.3  # per worker politeness for detail pages
MAX_CONSECUTIVE_FAILURES = 8

GRANT_NUM_BASE_RE = re.compile(r"^(\d{2}-\d{2}-\d{4,6})")
SEED_NUM_RE = re.compile(r"^\d{2}-\d{2}-\d{4,6}$")

_tls = threading.local()
_ck_lock = threading.Lock()


def _http_get(url: str, interval: float, timeout: int = 120) -> requests.Response:
    sess = getattr(_tls, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
            "Accept-Language": "ru,en;q=0.8",
        })
        _tls.session = sess
        _tls.last_t = 0.0
    elapsed = time.monotonic() - _tls.last_t
    if elapsed < interval:
        time.sleep(interval - elapsed)
    resp = sess.get(url, timeout=timeout, allow_redirects=True)
    _tls.last_t = time.monotonic()
    return resp


def _listing_url(year: str, page: int) -> str:
    q = urllib.parse.urlencode({
        "query": "", "GRANT_TYPE_ALL": "1", "GRANT_TYPE_ID": "1",
        "GRANT_CONTEST_TYPE": "-1", "FILTER_ID": "", "GRANT_YEAR": year,
        "page": str(page),
    })
    return f"{SEARCH_URL}?{q}"


# =============================================================================
# Seed list (grant numbers OpenAlex already cites) — for Phase 2 enrichment
# =============================================================================

def load_seed_ids(output_dir: Path) -> set[str]:
    cache = output_dir / "rfbr_seed_ids.txt"
    if cache.exists():
        ids = {ln.strip() for ln in cache.read_text().splitlines() if ln.strip()}
        print(f"  seed: loaded {len(ids)} grant numbers from {cache}")
        return ids
    print("  seed: pulling RFBR grant numbers from OpenAlex /awards ...")
    api_key = ""
    env = Path(__file__).resolve().parents[2] / ".env"
    if env.exists():
        for ln in env.read_text().splitlines():
            if ln.startswith("OPENALEX_API_KEY"):
                api_key = ln.split("=", 1)[1].strip()
    ak = f"&api_key={api_key}" if api_key else ""
    cur = "*"
    ids: set[str] = set()
    while True:
        url = (f"https://api.openalex.org/awards?filter=funder.id:{OPENALEX_FUNDER_ID}"
               f"&per-page=200&cursor={cur}{ak}")
        with urllib.request.urlopen(url, timeout=120) as r:
            d = json.load(r)
        for a in d["results"]:
            fid = (a.get("funder_award_id") or "").strip()
            if SEED_NUM_RE.match(fid):
                ids.add(fid)
        cur = d["meta"].get("next_cursor")
        if not cur:
            break
    cache.write_text("\n".join(sorted(ids)))
    print(f"  seed: {len(ids)} unique grant numbers cached to {cache}")
    return ids


# =============================================================================
# Parsing
# =============================================================================

_LIST_ROW_RE = re.compile(
    r'<a href="/project_search/(\d+)/" class="link">(.*?)</a>'
    r'<div class="proj_more">(.*?)</div></td><td>([^<]*)</td>',
    re.DOTALL,
)
_MORE_FIELD_RE = re.compile(r'([^:<>]+):\s*<b>(.*?)</b>', re.DOTALL)
_LAST_PAGE_RE = re.compile(r'sel_page\((\d+)\)')

_DETAIL_FIELD_RE = re.compile(r'<td class="projects_th">([^<]+):</th><td>(.*?)</td>', re.DOTALL)
_DETAIL_TITLE_RE = re.compile(r'<h1 class="page_title"[^>]*>(.*?)</h1>', re.DOTALL)
_DETAIL_ABSTRACT_RE = re.compile(
    r'<h2 class="small_title">Аннотация к заявке:</h2>(.*?)(?:<h2|<div class="col-lg-3|<footer|</div>)',
    re.DOTALL,
)


def _clean(s: str) -> str:
    s = re.sub(r"<br\s*/?>", " ", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = unescape(s)
    s = s.replace("\xa0", " ")
    return re.sub(r"\s+", " ", s).strip()


def parse_listing(html: str) -> tuple[list[dict], int]:
    """Return ([row dicts], last_page_number_seen)."""
    norm = re.sub(r"\s+", " ", html)
    rows = []
    for pid, title, more, gn in _LIST_ROW_RE.findall(norm):
        fields = {}
        for label, val in _MORE_FIELD_RE.findall(more):
            fields[_clean(label)] = _clean(val)
        year = None
        ym = re.search(r"(\d{4})", fields.get("Год проведения", ""))
        if ym:
            year = ym.group(1)
        gn = _clean(gn)
        if not gn:
            continue
        rows.append({
            "id": pid,
            "gn": gn,
            "title": _clean(title),
            "year": year,
            "area": fields.get("Область научного знания") or None,
            "contest": fields.get("Тип конкурса") or None,
            "status": fields.get("Статус заявки") or None,
        })
    pages = [int(m.group(1)) for m in _LAST_PAGE_RE.finditer(html)]
    return rows, (max(pages) if pages else 0)


def parse_detail(html: str) -> Optional[dict]:
    tm = _DETAIL_TITLE_RE.search(html)
    if not tm:
        return None
    fields = {}
    for label, body in _DETAIL_FIELD_RE.findall(html):
        fields[_clean(label)] = _clean(body)
    number = fields.get("Номер гранта", "")
    if not number:
        return None
    am = _DETAIL_ABSTRACT_RE.search(html)
    return {
        "gn": number,
        "pi_raw": fields.get("Руководитель") or None,
        "status": fields.get("Статус заявки") or None,
        "abstract": _clean(am.group(1)) if am else None,
    }


# =============================================================================
# Russian PI name splitting (Family Given Patronymic)
# =============================================================================

_RU_DEGREE_RE = re.compile(
    r",\s*(?:Академик|Член-корреспондент|Доктор|Кандидат|Профессор|Доцент|"
    r"без\s+ученой\s+степени|PhD|Ph\.D\.?).*$",
    re.I | re.DOTALL,
)


def split_pi_ru(pi_raw: str) -> tuple[Optional[str], Optional[str]]:
    """Russian order: Family Given Patronymic. Family = first token."""
    if not pi_raw:
        return None, None
    nm = _RU_DEGREE_RE.sub("", pi_raw).strip().strip(",").strip()
    if not nm:
        return None, None
    toks = nm.split()
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[1:]), toks[0]


# =============================================================================
# Phase 1: listing enumeration (checkpointed)
# =============================================================================

def phase1_listings(pages_ck: Path, workers: int, page_budget: Optional[int]) -> None:
    done: set[str] = set()
    known_last: dict[str, int] = {}
    if pages_ck.exists():
        with open(pages_ck, encoding="utf-8") as f:
            for ln in f:
                try:
                    rec = json.loads(ln)
                except Exception:
                    continue
                if "p" in rec:
                    done.add(f"{rec['y']}:{rec['p']}")
                if rec.get("last_page") is not None:
                    known_last[rec["y"]] = rec["last_page"]
    print(f"  phase1: {len(done)} listing pages already fetched")

    consecutive_failures = 0

    def fetch_page(year: str, page: int) -> dict:
        last_err = None
        for attempt in range(4):
            try:
                r = _http_get(_listing_url(year, page), LIST_INTERVAL_S)
                if r.status_code != 200:
                    last_err = f"HTTP {r.status_code}"
                    time.sleep(2.0 * (attempt + 1))
                    continue
                rows, last_page = parse_listing(r.text)
                return {"y": year, "p": page, "last_page": last_page or None,
                        "rows": rows, "_ok": True}
            except Exception as e:
                last_err = str(e)
                time.sleep(2.0 * (attempt + 1))
        return {"y": year, "p": page, "_ok": False, "_err": last_err}

    budget = page_budget if page_budget is not None else float("inf")
    fetched_this_run = 0
    t0 = time.monotonic()

    with open(pages_ck, "a", encoding="utf-8") as ck:
        for year in YEARS:
            if fetched_this_run >= budget:
                break
            # Page 1 first (gives authoritative last page for the year).
            last = known_last.get(year)
            if f"{year}:1" not in done:
                rec = fetch_page(year, 1)
                if not rec.get("_ok"):
                    print(f"  [WARN] {year} page 1 failed ({rec.get('_err')}); skipping year this run")
                    continue
                with _ck_lock:
                    ck.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    ck.flush()
                done.add(f"{year}:1")
                last = rec.get("last_page") or 1
                known_last[year] = last
                fetched_this_run += 1
                print(f"  {year}: last page = {last}, rows on p1 = {len(rec['rows'])}")
            elif last is None:
                # Page 1 fetched in an earlier run but carried no pagination
                # (single-page year like 2019).
                last = 1
                known_last[year] = 1

            todo = [p for p in range(2, (last or 1) + 1) if f"{year}:{p}" not in done]
            remaining_budget = budget - fetched_this_run
            if remaining_budget <= 0:
                break
            if len(todo) > remaining_budget:
                todo = todo[: int(remaining_budget)]
            if not todo:
                continue
            print(f"  {year}: fetching {len(todo)} listing pages (2..{last}) ...")
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(fetch_page, year, p): p for p in todo}
                completed = 0
                for fut in as_completed(futures):
                    rec = fut.result()
                    completed += 1
                    fetched_this_run += 1
                    if rec.get("_ok"):
                        consecutive_failures = 0
                        with _ck_lock:
                            ck.write(json.dumps(rec, ensure_ascii=False) + "\n")
                            ck.flush()
                        if not rec["rows"]:
                            # Empty page mid-corpus: recorded, NOT a terminator
                            # (runbook §1 — empty page != end of corpus).
                            print(f"    [NOTE] {year} p{rec['p']}: 0 rows (kept; not a terminator)")
                    else:
                        consecutive_failures += 1
                        print(f"    [RETRY-NEXT-RUN] {year} p{rec['p']}: {rec.get('_err')} "
                              f"({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")
                        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                            raise RuntimeError(
                                f"{MAX_CONSECUTIVE_FAILURES} consecutive listing failures — "
                                "aborting rather than silently truncating (runbook §1)")
                    if completed % 50 == 0 or completed == len(todo):
                        dt = time.monotonic() - t0
                        rate = fetched_this_run / dt if dt else 0
                        print(f"    [{year}] {completed}/{len(todo)} pages "
                              f"(run total {fetched_this_run}, {rate:.2f} pages/s)")
    print(f"  phase1 run complete: {fetched_this_run} pages fetched this run")


def load_listing_rows(pages_ck: Path) -> dict[str, dict]:
    """Read phase-1 checkpoint -> {grant_number: best row}. Keeps the
    earliest year occurrence of each grant number."""
    best: dict[str, dict] = {}
    if not pages_ck.exists():
        return best
    with open(pages_ck, encoding="utf-8") as f:
        for ln in f:
            try:
                rec = json.loads(ln)
            except Exception:
                continue
            for row in rec.get("rows", []):
                gn = row["gn"]
                prev = best.get(gn)
                if prev is None or (row.get("year") or "9999") < (prev.get("year") or "9999"):
                    best[gn] = row
    return best


# =============================================================================
# Phase 2: detail enrichment (checkpointed)
# =============================================================================

def phase2_details(pages_ck: Path, details_ck: Path, seed: set[str],
                   workers: int, limit: Optional[int], details_all: bool) -> None:
    rows = load_listing_rows(pages_ck)
    print(f"  phase2: {len(rows)} unique grant numbers in listing checkpoint")

    def base_num(gn: str) -> Optional[str]:
        m = GRANT_NUM_BASE_RE.match(gn)
        return m.group(1) if m else None

    supported = {gn: r for gn, r in rows.items()
                 if (r.get("status") or "").strip() == "поддержана"}
    print(f"  phase2: {len(supported)} supported (funded) rows")

    if details_all:
        targets = supported
    else:
        targets = {gn: r for gn, r in supported.items() if base_num(gn) in seed}
    print(f"  phase2: {len(targets)} rows targeted for PI/abstract enrichment")

    done: set[str] = set()
    if details_ck.exists():
        with open(details_ck, encoding="utf-8") as f:
            for ln in f:
                try:
                    rec = json.loads(ln)
                    if not rec.get("_retry"):
                        done.add(rec["_gn"])
                except Exception:
                    continue
    todo = [(gn, r["id"]) for gn, r in targets.items() if gn not in done]
    if limit is not None:
        todo = todo[:limit]
    print(f"  phase2: {len(todo)} detail pages to fetch this run (workers={workers})")

    def fetch_detail(gn: str, pid: str) -> dict:
        last_err = None
        for attempt in range(3):
            try:
                r = _http_get(f"{BASE}/project_search/{pid}/", DETAIL_INTERVAL_S)
                if r.status_code != 200:
                    last_err = f"HTTP {r.status_code}"
                    time.sleep(1.5 * (attempt + 1))
                    continue
                d = parse_detail(r.text)
                if d is None:
                    return {"_gn": gn, "_ok": False, "_reason": "unparseable"}
                d["_gn"] = gn
                d["_ok"] = True
                return d
            except Exception as e:
                last_err = str(e)
                time.sleep(1.5 * (attempt + 1))
        return {"_gn": gn, "_ok": False, "_reason": f"error:{last_err}", "_retry": True}

    hits = fails = 0
    t0 = time.monotonic()
    processed = 0
    with open(details_ck, "a", encoding="utf-8") as ck, \
         ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_detail, gn, pid): gn for gn, pid in todo}
        for fut in as_completed(futures):
            rec = fut.result()
            with _ck_lock:
                ck.write(json.dumps(rec, ensure_ascii=False) + "\n")
                ck.flush()
            processed += 1
            if rec.get("_ok"):
                hits += 1
            else:
                fails += 1
            if processed % 200 == 0 or processed == len(todo):
                dt = time.monotonic() - t0
                rate = processed / dt if dt else 0
                eta = (len(todo) - processed) / rate if rate else 0
                print(f"  [{processed}/{len(todo)}] enriched={hits} failed={fails} "
                      f"rate={rate:.1f}/s ETA={eta/60:.0f}m")
    print(f"  phase2 run complete: {hits} enriched, {fails} failed")


# =============================================================================
# Build DataFrame
# =============================================================================

def build_dataframe(pages_ck: Path, details_ck: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Build DataFrame from checkpoints")
    print("=" * 60)
    rows = load_listing_rows(pages_ck)
    details: dict[str, dict] = {}
    if details_ck.exists():
        with open(details_ck, encoding="utf-8") as f:
            for ln in f:
                try:
                    rec = json.loads(ln)
                except Exception:
                    continue
                if rec.get("_ok"):
                    details[rec["_gn"]] = rec

    out = []
    n_rejected = 0
    for gn, r in rows.items():
        status = (r.get("status") or "").strip()
        if status != "поддержана":
            n_rejected += 1
            continue  # rejected/unknown applications are not awards
        det = details.get(gn, {})
        given, family = split_pi_ru(det.get("pi_raw") or "")
        out.append({
            "funder_award_id": gn,
            "display_name": r.get("title"),
            "description": det.get("abstract") or None,
            "lead_pi_raw": det.get("pi_raw") or None,
            "lead_given_name": given,
            "lead_family_name": family,
            "research_area": r.get("area"),
            "funder_scheme": r.get("contest"),
            "status": status,
            "start_year": r.get("year"),
            "internal_id": r.get("id"),
            "landing_page_url": f"{BASE}/project_search/{r.get('id')}/",
        })
    df = pd.DataFrame.from_records(out)
    print(f"  dropped {n_rejected} non-supported application rows")
    if df.empty:
        print("  [WARN] no rows")
        return df
    df = df.drop_duplicates(subset=["funder_award_id"], keep="first")
    n = len(df)
    print(f"  rows (funded projects): {n}")
    for col in ["display_name", "lead_family_name", "funder_scheme",
                "research_area", "start_year", "description"]:
        cov = df[col].notna().sum()
        print(f"  {col:20s} coverage: {cov}/{n} ({cov*100/n:.1f}%)")
    print("  start_year distribution (top 30):")
    print(df["start_year"].value_counts().sort_index().head(30).to_string())
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (§1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    path = output_dir / PARQUET_NAME
    df.to_parquet(path, index=False, engine="pyarrow")
    print(f"  [OK] wrote {len(df)} rows ({path.stat().st_size/1024:.1f} KB) to {path}")
    return path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError("boto3 required for §1.4 shrink-check; use --skip-upload") from exc
    client = boto3.client("s3")
    print(f"  §1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet — first ingest.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev = output_dir / "_prev_rfbr.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev))
        prev_count = len(pd.read_parquet(prev))
    except Exception as e:
        print(f"    [ERROR] couldn't read existing parquet ({e}); aborting upload.")
        return False
    finally:
        prev.unlink(missing_ok=True)
    print(f"    previous: {prev_count}   new: {new_count}")
    if new_count < prev_count and not allow_shrink:
        print(f"\n[ERROR] §1.4 violation: refusing to shrink {prev_count} -> {new_count}.")
        return False
    print("    [OK] safe to overwrite.")
    return True


def upload_s3(path: Path) -> None:
    import boto3
    client = boto3.client("s3")
    client.upload_file(str(path), S3_BUCKET, S3_KEY)
    print(f"  [OK] uploaded to s3://{S3_BUCKET}/{S3_KEY}")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="RFBR (РФФИ) awards -> S3")
    ap.add_argument("--limit", type=int, default=None,
                    help="smoke mode: cap listing pages AND detail fetches this run")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--skip-download", action="store_true")
    ap.add_argument("--output-dir", default=None)
    ap.add_argument("--allow-shrink", action="store_true")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--details-all", action="store_true",
                    help="enrich every supported row, not just OpenAlex-cited ones")
    args = ap.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else Path(__file__).resolve().parent
    output_dir.mkdir(parents=True, exist_ok=True)
    pages_ck = output_dir / "rfbr_pages_checkpoint.jsonl"
    details_ck = output_dir / "rfbr_details_checkpoint.jsonl"

    print("=" * 60)
    print("RFBR (Russian Foundation for Basic Research / РФФИ) awards ingest")
    print(f"  funder_id={FUNDER_ID}  provenance={PROVENANCE}")
    print(f"  output_dir={output_dir}")
    print("=" * 60)

    if not args.skip_download:
        workers = max(1, min(args.workers, 5))
        seed = load_seed_ids(output_dir)
        phase1_listings(pages_ck, workers, args.limit)
        phase2_details(pages_ck, details_ck, seed, workers, args.limit, args.details_all)

    df = build_dataframe(pages_ck, details_ck)
    if df.empty:
        print("[ERROR] no rows to write.")
        sys.exit(3)
    path = write_parquet(df, output_dir)

    if args.skip_upload:
        print("  --skip-upload set; done (no S3).")
        return
    if not check_no_shrink(len(df), args.allow_shrink, output_dir):
        sys.exit(4)
    upload_s3(path)
    print("\nDone.")


if __name__ == "__main__":
    main()
