#!/usr/bin/env python3
"""
Russian Science Foundation (RSF / РНФ) -> S3 Data Pipeline  (GRANT PATTERN)
==========================================================================

Downloads RSF project cards from the foundation's public project-card
database at `rscf.ru/project/{grant_number}/` and uploads a parquet of
awards to S3.

Funder
------
Russian Science Foundation (Российский научный фонд, RSF/РНФ)
  OpenAlex funder_id : 4320324099  (F4320324099)
  ROR                : https://ror.org/03y2gwe85
  DOI                : 10.13039/501100006769
  country            : RU

Source authority + why cards, not the search index
---------------------------------------------------
RSF publishes a public project card per grant at
`https://rscf.ru/project/{number}/` (e.g. rscf.ru/project/22-29-01013/).
Each card is server-side-rendered HTML with labelled fields
(`<span class="fld_title">LABEL</span>VALUE`):

  - Номер проекта  (project number)
  - Название       (title, Russian)
  - Руководитель   (principal investigator; Russian order Family Given
                    Patronymic, optionally ", <academic degree>")
  - Организация финансирования, регион  (funding organization + region)
  - Конкурс        (competition/scheme; text encodes the competition year)
  - Область знания, основной код классификатора (research area + code)
  - Ключевые слова (keywords)
  - Код ГРНТИ      (GRNTI classifier code)
  - Аннотация      (abstract)

The site ALSO ships a project search UI at `rscf.ru/project/` backed by a
Bitrix iframe (`/extfilterapi/`) whose XHR target
(`/ais/rsf/templates/_ext_filter.php`) **currently 404s** — the public
search backend is broken (the internal analytics system `grant.rscf.ru`
is login-gated and returns empty for anonymous callers). There is also no
project-card entry in any rscf.ru sitemap. Reverse-engineering the search
therefore yields no usable enumeration.

Seed strategy
-------------
Because there is no working public enumeration, we seed the grant-number
list from the grant numbers OpenAlex already associates with RSF
(funder F4320324099, ~26k /awards rows), cleaned to the canonical
`YY-NN-NNNNN` shape and de-duplicated (~15.7k unique). For each we fetch
the authoritative public card, which massively enriches the existing
award stubs (adds Russian title, PI, organization, region, competition,
research area, keywords, GRNTI, abstract). A bogus/non-existent number
returns a card whose fields are empty; we detect that and skip it. The
`funder_award_id` stored is the native RSF grant number.

This is the best feasible harvest while the public search is down; it is
seed-limited (it cannot discover RSF grants that funded zero
OpenAlex-indexed works), which is documented here and in the notebook
header. When RSF restores its search backend, re-run with a full
enumeration seed.

Amount
------
RSF project cards do **not** publish a per-project funding amount (nor an
explicit end date). Amount/currency are NULL for every row -> the Step
6.7 amount-coverage check is WAIVED with this documented reason (source
publishes no amounts). `start_year` is derived from the grant-number
year prefix (`YY` -> 20YY); `start_date`/`end_date` are left NULL rather
than fabricated.

Output
------
s3://openalex-ingest/awards/rscf/rscf_projects.parquet

Usage
-----
    python rscf_to_s3.py                       # full harvest (~15.7k cards, resumable)
    python rscf_to_s3.py --limit 20            # smoke test (first 20 seed ids)
    python rscf_to_s3.py --skip-upload         # local dev, no S3
    python rscf_to_s3.py --output-dir DIR
    python rscf_to_s3.py --allow-shrink
    python rscf_to_s3.py --skip-download       # build parquet from checkpoint only

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
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

FUNDER_ID = 4320324099
FUNDER_DISPLAY_NAME = "Russian Science Foundation"
PROVENANCE = "rscf"

CARD_URL = "https://rscf.ru/project/{num}/"
OPENALEX_FUNDER_ID = "F4320324099"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rscf/rscf_projects.parquet"
PARQUET_NAME = "rscf_projects.parquet"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 "
              "openalex-walden-awards-ingest/1.0 (+https://openalex.org)")

MIN_REQUEST_INTERVAL_S = 0.35
GRANT_NUM_RE = re.compile(r"^\d{2}-\d{2}-\d{4,5}$")

# =============================================================================
# HTTP helper (rate-limited)
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 40) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
            "Accept-Language": "ru,en;q=0.8",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout, allow_redirects=True)
    _last_request_t = time.monotonic()
    return resp


# =============================================================================
# Seed list (grant numbers from OpenAlex, cleaned to YY-NN-NNNNN)
# =============================================================================

def load_seed_ids(output_dir: Path) -> list[str]:
    """Return the sorted unique list of RSF grant numbers to fetch.

    Cached at {output_dir}/rscf_seed_ids.txt. If absent, pulls all RSF
    /awards funder_award_ids from the OpenAlex API and keeps the ones
    matching YY-NN-NNNNN.
    """
    cache = output_dir / "rscf_seed_ids.txt"
    if cache.exists():
        ids = [ln.strip() for ln in cache.read_text().splitlines() if ln.strip()]
        print(f"  seed: loaded {len(ids)} grant numbers from {cache}")
        return ids

    print("  seed: pulling RSF grant numbers from OpenAlex /awards ...")
    api_key = ""
    env = Path(__file__).resolve().parents[2] / ".env"
    if env.exists():
        for ln in env.read_text().splitlines():
            if ln.startswith("OPENALEX_API_KEY"):
                api_key = ln.split("=", 1)[1].strip()
    ak = f"&api_key={api_key}" if api_key else ""
    cur = "*"
    ids: set[str] = set()
    pages = 0
    while True:
        url = (f"https://api.openalex.org/awards?filter=funder.id:{OPENALEX_FUNDER_ID}"
               f"&per-page=200&cursor={cur}{ak}")
        with urllib.request.urlopen(url, timeout=120) as r:
            d = json.load(r)
        for a in d["results"]:
            fid = (a.get("funder_award_id") or "").strip()
            if GRANT_NUM_RE.match(fid):
                ids.add(fid)
        cur = d["meta"].get("next_cursor")
        pages += 1
        if pages % 10 == 0:
            print(f"    ... {pages} pages, {len(ids)} unique grant numbers so far")
        if not cur:
            break
    out = sorted(ids)
    cache.write_text("\n".join(out))
    print(f"  seed: {len(out)} unique grant numbers cached to {cache}")
    return out


# =============================================================================
# Card parsing
# =============================================================================

_FIELD_RE = re.compile(
    r'<span class="fld_title">([^<]+)</span>(.*?)</p>',
    re.DOTALL,
)


def _clean(s: str) -> str:
    s = re.sub(r"<br\s*/?>", " ", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = unescape(s)
    s = s.replace("\xa0", " ")
    return re.sub(r"\s+", " ", s).strip()


def parse_card(html: str, grant_num: str) -> Optional[dict]:
    """Parse an RSF project card. Returns None if the card carries no
    project (non-existent grant number -> empty fields)."""
    fields: dict[str, str] = {}
    for label, body in _FIELD_RE.findall(html):
        fields[_clean(label)] = _clean(body)

    number = fields.get("Номер проекта", "")
    title = fields.get("Название", "")
    # A real card has a non-empty number matching the requested one and a title.
    if not number or not title:
        return None

    pi_raw = fields.get("Руководитель", "")
    org_region = fields.get("Организация финансирования, регион", "")
    competition = fields.get("Конкурс", "")
    area = fields.get("Область знания, основной код классификатора", "")
    keywords = fields.get("Ключевые слова", "")
    grnti = fields.get("Код ГРНТИ", "")
    annotation = fields.get("Аннотация", "")

    # Split "<organization> , <region>" — region is the last comma-group and
    # usually starts with a locality marker (г / обл / край / respublika).
    org, region = org_region, ""
    m = re.search(r"^(.*?),\s*(г\s|город|обл|край|респ|Респ|г\.).*$", org_region)
    # Prefer splitting on the LAST comma that introduces a locality token.
    parts = org_region.rsplit(",", 1)
    if len(parts) == 2 and re.search(r"(^|\s)(г|город|обл|край|респ|ао|пос)\b", parts[1], re.I):
        org, region = parts[0].strip(), parts[1].strip()

    # Competition text like "№64 - Конкурс 2021 года ..." — pull the year.
    comp_year = None
    ym = re.search(r"Конкурс\s+(\d{4})\s+года", competition)
    if ym:
        comp_year = ym.group(1)

    return {
        "funder_award_id": number,
        "title_ru": title,
        "pi_raw": pi_raw,
        "organization": org,
        "region": region,
        "org_region_raw": org_region,
        "competition": competition,
        "competition_year": comp_year,
        "research_area": area,
        "keywords": keywords,
        "grnti_code": grnti,
        "annotation": annotation,
        "landing_page_url": CARD_URL.format(num=number),
    }


# =============================================================================
# Russian PI name splitting (Family Given Patronymic order)
# =============================================================================

# Academic-degree / rank tokens that trail the name after a comma.
_RU_DEGREE_RE = re.compile(
    r",\s*(?:Академик|Член-корреспондент|Доктор|Кандидат|Профессор|Доцент|"
    r"без\s+ученой\s+степени|PhD|Ph\.D\.?).*$",
    re.I | re.DOTALL,
)


def split_pi_ru(pi_raw: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (given_name, family_name, degree) for a Russian PI string.

    Russian order is Family Given Patronymic, optionally followed by a
    comma + academic degree. Family = first token; given = the remaining
    name tokens (given + patronymic). NOT the Western split_name.
    """
    if not pi_raw:
        return None, None, None
    degree = None
    dm = _RU_DEGREE_RE.search(pi_raw)
    if dm:
        degree = pi_raw[dm.start():].lstrip(", ").strip() or None
    nm = _RU_DEGREE_RE.sub("", pi_raw).strip().strip(",").strip()
    if not nm:
        return None, None, degree
    toks = nm.split()
    if len(toks) == 1:
        return None, toks[0], degree
    family = toks[0]
    given = " ".join(toks[1:])
    return given, family, degree


# =============================================================================
# Harvest with checkpoint (resumable)
# =============================================================================

def harvest(seed_ids: list[str], checkpoint: Path, limit: Optional[int]) -> None:
    done: set[str] = set()
    if checkpoint.exists():
        with open(checkpoint, encoding="utf-8") as f:
            for ln in f:
                try:
                    rec = json.loads(ln)
                    done.add(rec["_num"])
                except Exception:
                    continue
    print(f"  checkpoint: {len(done)} grant numbers already processed")

    todo = [n for n in seed_ids if n not in done]
    if limit is not None:
        todo = todo[:limit]
    print(f"  to fetch this run: {len(todo)}")

    fetched = 0
    hits = 0
    misses = 0
    t0 = time.monotonic()
    with open(checkpoint, "a", encoding="utf-8") as ck:
        for i, num in enumerate(todo, 1):
            url = CARD_URL.format(num=num)
            found = False
            row = None
            for attempt in range(3):
                try:
                    resp = _http_get(url)
                    if resp.status_code == 200:
                        row = parse_card(resp.text, num)
                        found = True
                        break
                    if resp.status_code in (404, 410):
                        found = True  # definitively absent
                        break
                    print(f"    [{num}] HTTP {resp.status_code} (attempt {attempt+1}/3)")
                except Exception as e:
                    print(f"    [{num}] error {e} (attempt {attempt+1}/3)")
                time.sleep(1.5 * (attempt + 1))
            if not found:
                # Persistent failure — record as unresolved so we retry next run.
                print(f"    [{num}] giving up after 3 attempts; will retry next run")
                continue
            rec = {"_num": num, "_ok": row is not None}
            if row is not None:
                rec.update(row)
                hits += 1
            else:
                misses += 1
            ck.write(json.dumps(rec, ensure_ascii=False) + "\n")
            ck.flush()
            fetched += 1
            if i % 100 == 0 or i == len(todo):
                dt = time.monotonic() - t0
                rate = fetched / dt if dt else 0
                eta = (len(todo) - i) / rate if rate else 0
                print(f"  [{i}/{len(todo)}] hits={hits} misses={misses} "
                      f"rate={rate:.1f}/s ETA={eta/60:.0f}m")
    print(f"  harvest run complete: {fetched} fetched ({hits} hits, {misses} misses)")


# =============================================================================
# Build DataFrame
# =============================================================================

def build_dataframe(checkpoint: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Build DataFrame from checkpoint")
    print("=" * 60)
    rows = []
    with open(checkpoint, encoding="utf-8") as f:
        for ln in f:
            try:
                rec = json.loads(ln)
            except Exception:
                continue
            if not rec.get("_ok"):
                continue
            given, family, degree = split_pi_ru(rec.get("pi_raw", ""))
            yy = rec["funder_award_id"][:2]
            start_year = None
            try:
                iyy = int(yy)
                # RSF grants: 14->2014 .. 25->2025 (foundation founded 2013).
                if 13 <= iyy <= 30:
                    start_year = 2000 + iyy
            except ValueError:
                pass
            rows.append({
                "funder_award_id": rec["funder_award_id"],
                "display_name": rec["title_ru"],
                "description": rec.get("annotation") or None,
                "lead_pi_raw": rec.get("pi_raw") or None,
                "lead_given_name": given,
                "lead_family_name": family,
                "lead_degree": degree,
                "organization": rec.get("organization") or None,
                "region": rec.get("region") or None,
                "funder_scheme": rec.get("competition") or None,
                "competition_year": rec.get("competition_year"),
                "research_area": rec.get("research_area") or None,
                "keywords": rec.get("keywords") or None,
                "grnti_code": rec.get("grnti_code") or None,
                "start_year": start_year,
                "landing_page_url": rec.get("landing_page_url"),
            })
    df = pd.DataFrame.from_records(rows)
    if df.empty:
        print("  [WARN] no rows parsed")
        return df
    df = df.drop_duplicates(subset=["funder_award_id"], keep="last")
    n = len(df)
    print(f"  rows: {n}")
    for col in ["display_name", "lead_family_name", "organization", "start_year", "description"]:
        cov = df[col].notna().sum()
        print(f"  {col:20s} coverage: {cov}/{n} ({cov*100/n:.1f}%)")
    if df["start_year"].notna().any():
        print("  start_year distribution:")
        print(df["start_year"].value_counts().sort_index().to_string())
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
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
    prev = output_dir / "_prev_rscf.parquet"
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
    ap = argparse.ArgumentParser(description="RSF (РНФ) awards -> S3")
    ap.add_argument("--limit", type=int, default=None, help="only fetch first N seed ids (smoke)")
    ap.add_argument("--skip-upload", action="store_true", help="build parquet, no S3 upload")
    ap.add_argument("--skip-download", action="store_true", help="build from checkpoint only")
    ap.add_argument("--output-dir", default=None, help="output directory (default: script dir)")
    ap.add_argument("--allow-shrink", action="store_true", help="override §1.4 shrink guard")
    args = ap.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else Path(__file__).resolve().parent
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = output_dir / "rscf_checkpoint.jsonl"

    print("=" * 60)
    print("RSF (Russian Science Foundation / РНФ) awards ingest")
    print(f"  funder_id={FUNDER_ID}  provenance={PROVENANCE}")
    print(f"  output_dir={output_dir}")
    print("=" * 60)

    if not args.skip_download:
        seed = load_seed_ids(output_dir)
        harvest(seed, checkpoint, args.limit)

    df = build_dataframe(checkpoint)
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
