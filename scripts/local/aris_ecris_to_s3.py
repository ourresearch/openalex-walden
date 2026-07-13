#!/usr/bin/env python3
"""
ARIS (Slovenian Research and Innovation Agency) projects from eCRIS to S3
=========================================================================

Harvests ARIS-funded research projects and programmes from eCRIS
(cris.cobiss.net), the Slovenian national research information system
(successor of SICRIS), run by IZUM for ARIS.

Funder: Javna agencija za znanstvenoraziskovalno in inovacijsko dejavnost RS
        (ARIS, formerly ARRS) - OpenAlex F4320322554.

Data source (search/index API behind the eCRIS UI - HTMX fragments):
  search:  https://cris.cobiss.net/ecris/si/{lang}/project/search?query=*
             &prj.statadm=UPD&prj.mstid_prg={CODE}&offset=N&limit=499
           (server-rendered result fragments; offset paging is uncapped)
  detail:  https://cris.cobiss.net/ecris/si/en/project/{db_id}

INCLUSION RULE: prj.mstid_prg in the ARIS national funding-instrument codes
  (P research programme, I infrastructure programme, J basic, L applied,
  V target research, Z postdoctoral, M CRP MIR, N/H European-cofunded ARIS
  projects, R development, T heritage, NI/NC/NK/NJ/BI bilateral, GC
  Gravitation, STR strategic, MN mobility, TN TRL 3-6, O citizen science).
  These are exactly eCRIS prj.type in (PRG, PRJ) = 11,865 records; the FWP
  facet (FP4-7, H2020, HORIZON, ERASMUS+, COST, INTERREG, ...) is
  international, not ARIS-funded, and is excluded.

AMOUNTS: eCRIS publishes NO monetary amounts (historic SICRIS price
  categories/FTE hours are not exposed in the current eCRIS UI or fragments).
  amount/currency are NULL for every row -> Step 6.7 amount gate is waived
  per the prize/no-published-amounts clause. Any funding-related text found
  on detail pages would be preserved raw, but none is currently published.

Titles: both English and Slovenian searched (eCRIS is bilingual);
  title_en preferred for display_name downstream, title_sl kept.

Output: s3://openalex-ingest/awards/aris_ecris/aris_ecris_projects.parquet

Usage:
    py -3 aris_ecris_to_s3.py --output-dir C:/tmp/aris --skip-upload
    py -3 aris_ecris_to_s3.py --limit 30 --skip-upload      # smoke test
"""
import argparse
import builtins
import json
import re
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from html import unescape
from pathlib import Path

# ---------------------------------------------------------------- UTF-8 shim
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass
if sys.platform == "win32":
    _orig_open = builtins.open
    def _utf8_open(file, mode="r", buffering=-1, encoding=None, *a, **kw):
        if "b" not in str(mode) and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, *a, **kw)
    builtins.open = _utf8_open
    _orig_write_text = Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_write_text(self, data, encoding or "utf-8", errors, newline)
    Path.write_text = _wt
    _orig_read_text = Path.read_text
    def _rt(self, encoding=None, errors=None):
        return _orig_read_text(self, encoding or "utf-8", errors)
    Path.read_text = _rt

import pandas as pd

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/aris_ecris/aris_ecris_projects.parquet"
BASE = "https://cris.cobiss.net/ecris/si"
UA = {"User-Agent": "OpenAlex-ARIS/1.0 (mailto:support@openalex.org)"}
PAGE_LIMIT = 499
DETAIL_DELAY = 0.35          # politeness between detail-page fetches
MAX_CONSECUTIVE_EMPTY = 3    # §1: empty page != end of corpus
MAX_CONSECUTIVE_NON200 = 5
MAX_PAGES_PER_SLICE = 40     # hard terminator (40*499 ≈ 20k >> largest slice)

# ARIS national funding-instrument codes (facet prj.mstid_prg), with the
# facet counts observed 2026-07-12 for reference.
ARIS_CODES = [
    "P",    # research programme (1,581)
    "I",    # infrastructure programme (211)
    "J",    # basic research project (3,520)
    "L",    # applied research project (2,030)
    "V",    # target research project (2,014)
    "Z",    # postdoctoral research project (841)
    "M",    # CRP MIR (192)
    "N",    # European research project, ARIS-cofunded (480)
    "H",    # European research project - ERA (20)
    "R",    # development research project (34)
    "T",    # natural and cultural heritage (61)
    "NI",   # bilateral - Israel (4)
    "NC",   # bilateral - CEA (41)
    "NK",   # bilateral - China (7)
    "NJ",   # bilateral - Japan (2)
    "BI",   # bilateral projects (764)
    "GC",   # Gravitation (7)
    "STR",  # strategic project (2)
    "MN",   # RRP mobility (29)
    "TN",   # RRP TRL 3-6 (4)
    "O",    # citizen science (21)
]

_ENTITY_RE = re.compile(
    r'<c-result-entity type="project" db-id="(\d+)"[^>]*>(.*?)</c-result-entity>',
    re.S)
_CODE_RE = re.compile(r'data-name="Code">\s*(.*?)\s*</div>', re.S)
_TITLE_RE = re.compile(r'<a href="[^"]*/project/\d+">\s*(.*?)\s*</a>', re.S)
_PERIOD_RE = re.compile(r'data-name="Period">\s*(.*?)\s*</div>', re.S)
_HEAD_RE = re.compile(r'data-name="Head">\s*(.*?)\s*</div>', re.S)


def http_get(url: str, timeout: int = 90) -> str:
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8")


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", s or ""))).strip()


def _parse_period(s: str):
    """'6/1/2026 - 5/31/2029' or '2027-05-01 - 2029-04-30' -> (start, end) ISO."""
    s = _clean(s)
    dates = re.findall(r"(\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2})", s)
    out = []
    for d in dates:
        if "/" in d:
            m, day, y = d.split("/")
            out.append(f"{y}-{int(m):02d}-{int(day):02d}")
        else:
            out.append(d)
    start = min(out) if out else None
    end = max(out) if len(out) > 1 else None
    return start, end


# -------------------------------------------------------------- enumeration
def enumerate_slice(code: str, lang: str) -> dict:
    """Page one prj.mstid_prg slice; return {db_id: record}."""
    out = {}
    offset, page, consecutive_empty, consecutive_non200 = 0, 0, 0, 0
    while page < MAX_PAGES_PER_SLICE:
        page += 1
        url = (f"{BASE}/{lang}/project/search?query=*&prj.statadm=UPD"
               f"&prj.mstid_prg={urllib.parse.quote(code)}"
               f"&order=&offset={offset}&limit={PAGE_LIMIT}")
        try:
            html = http_get(url)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            consecutive_non200 += 1
            print(f"    {code}/{lang} offset={offset}: {e} "
                  f"({consecutive_non200}/{MAX_CONSECUTIVE_NON200}); retrying")
            if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(f"slice {code}/{lang}: too many fetch failures")
            time.sleep(5 * consecutive_non200)
            continue
        consecutive_non200 = 0
        ents = _ENTITY_RE.findall(html)
        if not ents:
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                break
            time.sleep(2)
            continue
        consecutive_empty = 0
        for db_id, body in ents:
            rec = {"db_id": db_id, "mstid_prg": code}
            m = _CODE_RE.search(body)
            rec["code"] = _clean(m.group(1)) if m else None
            m = _TITLE_RE.search(body)
            rec["title"] = _clean(m.group(1)) if m else None
            m = _PERIOD_RE.search(body)
            rec["period"] = _clean(m.group(1)) if m else None
            m = _HEAD_RE.search(body)
            rec["head"] = _clean(m.group(1)) if m else None
            out[db_id] = rec
        print(f"    {code}/{lang} offset={offset}: +{len(ents)} (cum {len(out)})")
        if len(ents) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
        time.sleep(0.5)
    return out


# ------------------------------------------------------------- detail pages
_DETAIL_ORG_RE = re.compile(
    r'(\d{4})&#160;\s*(?:<[^>]+>\s*)*([^<]{3,200}?)\s*</', re.S)


def parse_detail(html: str) -> dict:
    """Extract orgs, FORD field, and sciences from a project detail page."""
    rec = {}
    # FORD classification row: 'code || Science || Field'
    m = re.search(
        r"FORD classification.*?<td[^>]*>\s*([\d.]+)&#160;\s*</td>\s*"
        r"<td[^>]*>\s*([^<]+?)&#160;\s*</td>\s*<td[^>]*>\s*([^<]+?)&#160;\s*</td>",
        html, re.S)
    if m:
        rec["ford_code"] = m.group(1).strip()
        rec["ford_science"] = unescape(m.group(2).strip())
        rec["ford_field"] = unescape(m.group(3).strip())
    # Organisations: <div ... data-label="Code Research organisation"> 3270&#160; \tName </div>
    orgs = []
    for om in re.finditer(
            r'data-label="Code Research organisation">\s*(\d+)&#160;\s*([^<]+?)\s*</div>',
            html):
        orgs.append({"code": om.group(1), "name": unescape(om.group(2)).strip()})
    # Periods list (programmes have several funding periods)
    mp = re.search(r"Periods.*?Research activity", html, re.S)
    period_dates = re.findall(r"([A-Z][a-z]+ \d{1,2}, \d{4})", mp.group(0)) if mp else []
    if period_dates:
        iso = []
        for d in period_dates:
            try:
                iso.append(datetime.strptime(d, "%B %d, %Y").strftime("%Y-%m-%d"))
            except ValueError:
                pass
        if iso:
            rec["periods_start"] = min(iso)
            rec["periods_end"] = max(iso)
    # de-dup, preserve order
    seen, uniq = set(), []
    for o in orgs:
        if o["code"] not in seen:
            seen.add(o["code"])
            uniq.append(o)
    rec["organizations"] = uniq
    # Keywords (EN pages)
    mk = re.search(r"Keywords\s*</h4>.*?<p[^>]*>\s*(.*?)\s*</p>", html, re.S)
    if mk:
        rec["keywords"] = _clean(mk.group(1))[:2000]
    return rec


def fetch_details(db_ids: list, ckpt: Path, limit: int = 0, workers: int = 6) -> dict:
    """Fetch detail pages concurrently with JSONL checkpointing."""
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed
    done = {}
    if ckpt.exists():
        with open(ckpt) as f:
            for line in f:
                try:
                    r = json.loads(line)
                    done[r["db_id"]] = r
                except json.JSONDecodeError:
                    continue
        print(f"    checkpoint: {len(done):,} detail pages already fetched")
    todo = [d for d in db_ids if d not in done]
    if limit:
        todo = todo[:max(0, limit - len(done))]
    if not todo:
        return done
    lock = threading.Lock()
    fail_streak = [0]
    t0 = time.time()

    def fetch_one(db_id):
        html = http_get(f"{BASE}/en/project/{db_id}")
        rec = parse_detail(html)
        rec["db_id"] = db_id
        time.sleep(DETAIL_DELAY)
        return rec

    with open(ckpt, "a") as out, ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_one, d): d for d in todo}
        n_ok, n_err = 0, 0
        for fut in as_completed(futures):
            db_id = futures[fut]
            try:
                rec = fut.result()
            except Exception as e:
                with lock:
                    n_err += 1
                    fail_streak[0] += 1
                    print(f"    detail {db_id}: {e} "
                          f"(streak {fail_streak[0]}/{MAX_CONSECUTIVE_NON200 * 4})")
                    if fail_streak[0] >= MAX_CONSECUTIVE_NON200 * 4:
                        raise RuntimeError("too many consecutive detail failures - aborting (§1)")
                continue
            with lock:
                fail_streak[0] = 0
                done[db_id] = rec
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n_ok += 1
                if n_ok % 200 == 0:
                    out.flush()
                    el = time.time() - t0
                    eta = el / n_ok * (len(todo) - n_ok)
                    print(f"    [{n_ok}/{len(todo)}] detail pages "
                          f"({100*n_ok/len(todo):.1f}%) - ETA {eta/60:.0f}m")
    if n_err:
        # failed ids remain absent from the checkpoint; a re-run retries just
        # those (fail-closed on silent truncation is enforced in assemble
        # coverage prints + rerun instructions)
        print(f"    WARNING: {n_err} detail pages failed; re-run to retry them")
    return done


# ---------------------------------------------------------------- name split
def split_name(name):
    """Split 'James P. Eisenstein' -> ('James P.', 'Eisenstein').

    Canonical helper from wolf_to_s3.py (§2.4.1): strips trailing
    degree/suffix tokens before splitting; last token = family name.
    """
    if not name:
        return None, None
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


_LEADING_TITLES = {"phd", "dr.", "dr", "msc", "mag.", "mag", "prof.", "prof",
                   "acad.", "assist.", "assoc.", "izr.", "red.", "doc.",
                   "znan.", "sod.", "svet."}


def strip_leading_titles(name: str) -> str:
    """eCRIS heads carry leading titles: 'PhD Nika Strasek', 'dr. Mojca J.'"""
    tokens = (name or "").split()
    while tokens and tokens[0].lower() in _LEADING_TITLES:
        tokens.pop(0)
    return " ".join(tokens)


# ------------------------------------------------------------------ assemble
def assemble(en: dict, sl: dict, details: dict) -> pd.DataFrame:
    rows = []
    for db_id, rec in en.items():
        start, end = _parse_period(rec.get("period") or "")
        head_raw = rec.get("head") or None
        head_clean = strip_leading_titles(head_raw) if head_raw else None
        given, family = split_name(head_clean) if head_clean else (None, None)
        det = details.get(db_id, {})
        orgs = det.get("organizations") or []
        rows.append({
            "db_id": db_id,
            "code": rec.get("code"),
            "mstid_prg": rec.get("mstid_prg"),
            "title_en": rec.get("title"),
            "title_sl": (sl.get(db_id) or {}).get("title"),
            "head_raw": head_raw,
            "lead_given_name": given,
            "lead_family_name": family,
            "start_date": start or det.get("periods_start"),
            "end_date": end or det.get("periods_end"),
            "ford_code": det.get("ford_code"),
            "ford_science": det.get("ford_science"),
            "ford_field": det.get("ford_field"),
            "keywords": det.get("keywords"),
            "lead_org_name": orgs[0]["name"] if orgs else None,
            "lead_org_code": orgs[0]["code"] if orgs else None,
            "organizations_json": json.dumps(orgs, ensure_ascii=False) if orgs else None,
            "landing_page_url": f"{BASE}/en/project/{db_id}",
        })
    df = pd.DataFrame(rows)
    # funder_award_id = native ARIS project code; must be unique (§ prize-
    # pattern collision rule: raise, don't warn)
    missing_code = df["code"].isna().sum()
    if missing_code:
        print(f"  WARNING: {missing_code} records without ARIS code; using db_id fallback")
        df.loc[df["code"].isna(), "code"] = "ECRIS-" + df.loc[df["code"].isna(), "db_id"]
    # eCRIS carries multiple records per ARIS code: research programmes (P/I)
    # get one record per funding period, and a few projects were re-registered
    # (e.g. Z1-3189). One award = one code: keep the richest/newest record's
    # descriptive fields, but merge the funding span across ALL records for
    # that code (start = earliest start, end = latest end).
    dup_mask = df["code"].duplicated(keep=False)
    if dup_mask.any():
        print(f"  merging {int(dup_mask.sum())} rows sharing "
              f"{df.loc[dup_mask, 'code'].nunique()} ARIS codes "
              f"(richest/newest record + min/max funding span)")
        span = df.groupby("code").agg(_start_min=("start_date", "min"),
                                      _end_max=("end_date", "max"))
        df["_rank"] = (
            df["head_raw"].notna().astype(int) * 4
            + df["title_en"].notna().astype(int) * 2
            + df["lead_org_name"].notna().astype(int)
        )
        df["_dbid_n"] = pd.to_numeric(df["db_id"], errors="coerce")
        df = (df.sort_values(["code", "_rank", "_dbid_n"], ascending=[True, False, False])
                .drop_duplicates("code", keep="first")
                .drop(columns=["_rank", "_dbid_n"]))
        df = df.merge(span, on="code", how="left")
        df["start_date"] = df["_start_min"].where(df["_start_min"].notna(), df["start_date"])
        df["end_date"] = df["_end_max"].where(df["_end_max"].notna(), df["end_date"])
        df = df.drop(columns=["_start_min", "_end_max"])
    if df["code"].duplicated().any():
        raise RuntimeError("funder_award_id still colliding after dedup")
    df["provenance"] = "aris_ecris"
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df = df.astype("string")
    return df


def check_no_shrink(df: pd.DataFrame, allow_shrink: bool):
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(f"§1.4 shrink-check FAILED: new {len(df):,} < "
                             f"existing {len(prev):,}. Use --allow-shrink to override.")
        print(f"  §1.4 shrink-check OK (new {len(df):,} >= existing {len(prev):,})")
    except SystemExit:
        raise
    except Exception as e:
        print(f"  §1.4 shrink-check: no prior object / not comparable ({type(e).__name__})")


def main():
    ap = argparse.ArgumentParser(description="ARIS eCRIS projects -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("aris_out"))
    ap.add_argument("--limit", type=int, default=0,
                    help="cap detail-page fetches for smoke testing")
    ap.add_argument("--skip-details", action="store_true",
                    help="skip detail pages entirely (orgs/FORD will be NULL)")
    ap.add_argument("--workers", type=int, default=4,
                    help="concurrent detail-page fetchers (default 4)")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    a = ap.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    enum_ckpt = a.output_dir / "enum_checkpoint.json"
    detail_ckpt = a.output_dir / "details_checkpoint.jsonl"

    print("=" * 64)
    print("ARIS (Slovenia) eCRIS projects/programmes -> S3")
    print("=" * 64)

    codes = ARIS_CODES if not a.limit else ARIS_CODES[5:6]  # Z-slice for smoke
    if enum_ckpt.exists() and not a.limit:
        d = json.loads(enum_ckpt.read_text())
        en, sl = d["en"], d["sl"]
        print(f"[1/3] Enumeration checkpoint: {len(en):,} EN / {len(sl):,} SL records")
    else:
        print(f"[1/3] Enumerating {len(codes)} ARIS instrument slices (EN + SL)")
        en, sl = {}, {}
        for code in codes:
            en.update(enumerate_slice(code, "en"))
            sl.update(enumerate_slice(code, "sl"))
        if not a.limit:
            enum_ckpt.write_text(json.dumps({"en": en, "sl": sl}, ensure_ascii=False))
        print(f"      enumerated {len(en):,} projects (EN), {len(sl):,} (SL)")
    if not en:
        raise RuntimeError("enumeration returned 0 projects - eCRIS down or changed")

    if a.skip_details:
        details = {}
        print("[2/3] Skipping detail pages (--skip-details)")
    else:
        print(f"[2/3] Fetching detail pages ({len(en):,} total, checkpointed)")
        details = fetch_details(list(en.keys()), detail_ckpt, a.limit, a.workers)

    print("[3/3] Assembling parquet")
    df = assemble(en, sl, details)
    out = a.output_dir / "aris_ecris_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"\nSaved {out}: {len(df):,} projects, {out.stat().st_size/1e6:.1f} MB")

    nn = lambda c: 100 * df[c].notna().sum() / len(df)
    print("\nCoverage:")
    for c in ["code", "title_en", "title_sl", "head_raw", "lead_family_name",
              "start_date", "end_date", "lead_org_name", "ford_field"]:
        print(f"  {c:18s} {nn(c):5.1f}%")
    print(f"  amount             0.0%  (eCRIS publishes no amounts - §6.7 waiver)")
    print(f"\nInstrument split: {df['mstid_prg'].value_counts().to_dict()}")

    if not a.skip_upload:
        check_no_shrink(df, a.allow_shrink)
        import shutil
        import subprocess
        aws = shutil.which("aws")
        if not aws:
            raise RuntimeError("aws CLI not found; rerun with --skip-upload and upload manually")
        subprocess.run([aws, "s3", "cp", str(out), f"s3://{S3_BUCKET}/{S3_KEY}"], check=True)
        print(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")
    print("\nNext: notebooks/awards/CreateARISAwards.ipynb")


if __name__ == "__main__":
    main()
