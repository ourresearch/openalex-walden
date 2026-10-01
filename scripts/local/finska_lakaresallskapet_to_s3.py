#!/usr/bin/env python3
"""
Finska Läkaresällskapet (FLS, Finnish Medical Society) to S3 Data Pipeline
=========================================================================

FLS publishes its research-grant decisions ("Beviljade forskningsanslag") on
one WordPress page, https://fls.fi/beviljade-forskningsanslag/ , replaced each
February by the new cohort: a list per category (Forskare som ej doktorerat,
Doktorandstipendium as "Supervisor / Doctoral student", Äldre forskare,
Yngre forskare, (Postdoc-)forskning utomlands, and grants from funds FLS
administers: Linda Gadds fond, Einar och Karin Stroems stiftelse, Elli Maria
och Anna Sofia Ruuths fond), each line a name followed by the euro amount
(two amounts for the two years of a 2-year grant). No project titles,
institutions or grant numbers are published.

The live page holds only the current cohort (2026-2027). Earlier cohorts are
read from Internet Archive captures of the same page (raw id_ captures):
2021 (5.2.2021), 2022 (4.2.2022), 2023-2024 (3.2.2023), 2025-2026
(7.2.2025). The 2024-2025 cohort (Feb 2024) was never captured, and pre-2021
lists were PDFs on the old site (mostly not archived): not covered.

research.fi (ladder item 0) does not carry FLS (funder facet checked
2026-10-01: 23 funders, no Läkaresällskapet).

funder_award_id: synthetic FLS-<cohort year>-<category code>-<name slug>
(citing works write no FLS grant number in a consistent form).

Output: s3://openalex-ingest/awards/finska_lakaresallskapet/finska_lakaresallskapet_projects.parquet
"""

import argparse
import html
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (runbook 4.0 grep marker: sys.stdout.reconfigure)
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

PAGE = "https://fls.fi/beviljade-forskningsanslag/"
WAYBACK = "http://web.archive.org/web/{ts}id_/https://fls.fi/beviljade-forskningsanslag/"
# one capture per cohort (the page is replaced every February)
SOURCES = [
    ("2021", WAYBACK.format(ts="20211208025729")),
    ("2022", WAYBACK.format(ts="20220302154011")),
    ("2023", WAYBACK.format(ts="20230331120257")),
    ("2025", WAYBACK.format(ts="20250806234756")),
    ("live", PAGE),
]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/finska_lakaresallskapet/finska_lakaresallskapet_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
RETRIES = 4

CATEGORIES = [  # (regex on the category heading, code, scheme name, funding_type)
    (r"ej doktorerat", "ED", "Forskare som ej doktorerat", "studentship"),
    (r"doktorandstipendi", "DS", "Doktorandstipendium", "studentship"),
    (r"äldre forskare", "AF", "Äldre forskare", "research"),
    (r"yngre forskare", "YF", "Yngre forskare", "research"),
    (r"utomlands", "PU", "Postdoc-forskning utomlands", "fellowship"),
    (r"linda gadd", "LG", "Linda Gadds fond", "research"),
    (r"stroem", "ES", "Einar och Karin Stroems stiftelse", "research"),
    (r"ruuth", "ER", "Elli Maria och Anna Sofia Ruuths fond", "research"),
    (r"palander", "KP", "Kurt och Doris Palanders stiftelse", "research"),
]
AMOUNT_RE = re.compile(r"^\d{1,3}(?:[  ]?\d{3})*\s*(?:€|euro)?(?:\s*/\s*\d{1,3}(?:[  ]?\d{3})*\s*(?:€|euro)?)?$")
LOOSE_AMOUNT_RE = re.compile(r"^\d[\d  ]*\s*(?:€|euro)?$")  # e.g. '27 00' (typo on the 2022 page)
YEAR_RE = re.compile(r"^(19|20)\d\d$")  # '2023' / '2024' column headers of 2-year lists
STOP_RE = re.compile(r"^(Snabblänkar|Stipendierna delas|Läkarnas etiska forum|Kontaktuppgifter)")


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def page_lines(page: str) -> list[str]:
    page = re.sub(r"<script.*?</script>|<style.*?</style>|<head.*?</head>", "", page, flags=re.S)
    page = re.sub(r"(?i)</?(p|h[1-6]|li|div|br|tr|td|th|table)[^>]*>", "\n", page)
    text = html.unescape(re.sub(r"<[^>]+>", "", page)).replace("\xa0", " ").replace("​", "")
    return [re.sub(r"\s+", " ", x).strip() for x in text.split("\n") if x.strip()]


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|docent)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py): 'Given ... Family'."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def split_family_first(name: str) -> tuple[str | None, str | None]:
    """2025+ lists print 'Family Given' ('Jansson Sigfrids Fanny', 'Gahmberg
    Carl G.', 'von Bahr Joar'): given = last token (two when the last is an
    initial), family = the rest."""
    toks = re.split(r"\s+", name.strip())  # family-first layout, not the naive given-first split
    if len(toks) < 2:
        return None, name or None
    k = 2 if len(toks) >= 3 and re.fullmatch(r"[A-ZÅÄÖ]\.", toks[-1]) else 1
    return " ".join(toks[-k:]), " ".join(toks[:-k])


def parse_amount(s: str) -> tuple[float | None, int]:
    parts = [re.sub(r"[^\d]", "", p) for p in s.split("/")]
    nums = [float(p) for p in parts if p]
    return (sum(nums) if nums else None), len(nums)


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def parse_cohort(page: str, label: str, url: str) -> list[dict]:
    L = page_lines(page)
    start = next(i for i, x in enumerate(L) if re.match(r"(?i)^beviljade (stipendier|forskningsanslag) \d{4}", x))
    m = re.search(r"(\d{4})(?:\s*-\s*(\d{4}))?", L[start])
    y0, y1 = int(m.group(1)), int(m.group(2)) if m.group(2) else None
    decided = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", " ".join(L[start:start + 4]))
    decision_date = f"{decided.group(3)}-{int(decided.group(2)):02d}-{int(decided.group(1)):02d}" if decided else None
    family_first = y0 >= 2025
    rows, cat, pending = [], None, None
    i = start + 1
    while i < len(L):
        x = L[i]
        if STOP_RE.match(x):
            break
        nxt = L[i + 1] if i + 1 < len(L) else ""
        if YEAR_RE.match(x):
            pending = None
            i += 1
            continue
        if AMOUNT_RE.match(x):
            if pending is not None:  # second-year amount on its own line
                amt, n = parse_amount(x)
                pending["amount_parts"].append(x)
                pending["amount"] = (pending["amount"] or 0) + (amt or 0)
            i += 1
            continue
        if re.match(r"(?i)^(euro|\d{4})$", x) or re.match(r"(?i)^(finska läkaresällskapet|den totala|\d{1,2}\.\d{1,2}\.\d{4})", x):
            pending = None
            i += 1
            continue
        if (AMOUNT_RE.match(nxt) or LOOSE_AMOUNT_RE.match(nxt)) and not YEAR_RE.match(nxt):
            if cat is None:
                i += 1
                continue
            # malformed amounts ('27 00') ship NULL, raw text kept in amount_parts
            amt = parse_amount(nxt)[0] if AMOUNT_RE.match(nxt) else None
            people = [p.strip() for p in re.split(r"\s*/\s*", x) if p.strip()]
            split = split_family_first if family_first else split_name
            names = [split(p) for p in people]
            pending = {
                "cohort": label, "cohort_start_year": y0, "cohort_end_year": y1, "decision_date": decision_date,
                "category_code": cat[1], "funder_scheme": cat[2], "funding_type": cat[3],
                "recipient_raw": x,
                "lead_given_name": names[0][0], "lead_family_name": names[0][1],
                "second_given_name": names[1][0] if len(names) > 1 else None,
                "second_family_name": names[1][1] if len(names) > 1 else None,
                "amount_parts": [nxt], "amount": amt, "currency": "EUR" if amt is not None else None,
                "source_url": url,
            }
            rows.append(pending)
            i += 2
            continue
        # a heading line (not followed by an amount)
        hit = next((c for c in CATEGORIES if re.search(c[0], x, re.I)), None)
        if hit:
            cat = hit
        elif len(x) < 90 and not re.search(r"\d", x):
            cat = ("XX", "XX", x, "research")
            log(f"  {label}: unknown category heading '{x}'")
        pending = None
        i += 1
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="Finska Läkaresällskapet beviljade forskningsanslag -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="keep only the first N grants (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache the HTML pages here")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    rows = []
    for label, url in SOURCES:
        cache = args.cache_dir / f"{label}.html" if args.cache_dir else None
        if cache and cache.exists():
            page = cache.read_text()
        else:
            page = get(url)
            if cache:
                args.cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(page)
            time.sleep(4)  # Internet Archive: sequential, polite
        cohort = parse_cohort(page, label, url)
        tot = sum(r["amount"] or 0 for r in cohort)
        log(f"{label}: {len(cohort)} grants, EUR {tot:,.0f} ({url})")
        rows += cohort

    df = pd.DataFrame(rows)
    df["amount_parts"] = df["amount_parts"].map(lambda xs: " / ".join(xs))
    keys, used = [], {}
    for r in df.itertuples():
        k = f"FLS-{r.cohort_start_year}-{r.category_code}-{slug(r.recipient_raw)}"
        used[k] = used.get(k, 0) + 1
        keys.append(k if used[k] == 1 else f"{k}-{used[k]}")
    df["funder_award_id"] = keys
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    if args.limit:
        df = df.head(args.limit)
    log(f"Total {len(df)} grants; by cohort {df.groupby('cohort_start_year').size().to_dict()}")
    log(f"  by category {df.groupby('category_code').size().to_dict()}")
    log(f"  amount {df['amount'].notna().mean():.1%}, EUR {df['amount'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "finska_lakaresallskapet_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_finska_lakaresallskapet_projects.parquet"
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
