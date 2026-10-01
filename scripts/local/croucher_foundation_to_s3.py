#!/usr/bin/env python3
"""
Croucher Foundation (Hong Kong) to S3 Data Pipeline
===================================================

The Croucher Foundation (OpenAlex F4320320904, HK) lists every Croucher scholar and
fellow since 1981 on its main site, https://croucher.org.hk/en/fellows-and-scholars
(Next.js; 100 people per page, ?page=N). Each page embeds the people as JSON in the
React-server payload: name, Chinese name, page slug and an `awards` list such as
"2026 Fellowship at the University of Bologna". One row is written per award
(person x award), so a scholar who later won a fellowship has two rows.
Method 5 (static HTML) on the runbook ladder; no export exists.

robots: croucher.org.hk/robots.txt allows all agents (except /page-templates/ and
/en/memo/). The separate directory host scholars.croucher.org.hk disallows all
non-Google agents, so it is NOT used (nor any backend of it).

Award types (all kept; talent/training pathways of a research funder are in scope):
Croucher Scholarship, Fellowship, Studentship, Senior Research Fellowship, Innovation
Award, Cambridge International Scholarship, Oxford Croucher Scholarship, Senior Medical
Research Fellowship, MBBS/PhD Scholarship, Science Communication Studentship, Clinical /
Non-Clinical Assistant Professorship, Max Planck Croucher Postdoctoral Fellowship,
Todd-Croucher Fellowship, Chinese Visitorship.

No amounts or grant numbers are published per award, so funder_award_id is a stable
synthetic key CROUCHER-{year}-{award code}-{person slug}.

Output: s3://openalex-ingest/awards/croucher_foundation/croucher_foundation_projects.parquet
"""

import argparse
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

LIST_URL = "https://croucher.org.hk/en/fellows-and-scholars"
PERSON_URL = "https://croucher.org.hk/en/fellows-and-scholars/{slug}"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/croucher_foundation/croucher_foundation_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 1.0
MAX_CONSECUTIVE_EMPTY = 3

# award label (as printed in the awards strings) -> (code, funding_type)
AWARD_TYPES = {
    "Scholarship": ("sch", "fellowship"),
    "Fellowship": ("fll", "fellowship"),
    "Studentship": ("stu", "fellowship"),
    "Senior Research Fellowship": ("srf", "fellowship"),
    "Innovation Award": ("cia", "research"),
    "Cambridge International Scholarship": ("cis", "fellowship"),
    "Oxford Croucher Scholarship": ("ocs", "fellowship"),
    "Senior Medical Research Fellowship": ("smrf", "fellowship"),
    "MBBS/PhD Scholarship": ("mbbs", "fellowship"),
    "Science Communication Studentship": ("scs", "fellowship"),
    "Clinical Assistant Professorship": ("cap", "fellowship"),
    "Non-Clinical Assistant Professorship": ("ncap", "fellowship"),
    "Max Planck Croucher Postdoctoral Fellowship": ("mpcpf", "fellowship"),
    "Todd-Croucher Fellowship": ("tcf", "fellowship"),
    "Chinese Visitorship": ("vis", "fellowship"),
    "Butterfield Croucher Studentship": ("bcs", "fellowship"),
    "Croucher Research Studentship": ("crs", "fellowship"),
}
# spelling variants seen in the awards strings -> canonical label above
LABEL_ALIASES = {
    "Croucher Scholarship": "Scholarship",
    "Croucher Cambridge International Scholarship": "Cambridge International Scholarship",
    "University of Oxford Croucher Scholarship": "Oxford Croucher Scholarship",
    "Oxford Croucher Scholarships": "Oxford Croucher Scholarship",
    "MBBS/PhD": "MBBS/PhD Scholarship",
    "MBBS/PhD Student": "MBBS/PhD Scholarship",
    "Visitorship": "Chinese Visitorship",
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_page(page: int, cache_dir: Path | None) -> dict:
    cache = cache_dir / f"page_{page:03d}.json" if cache_dir else None
    if cache and cache.exists():
        return json.loads(cache.read_text())
    last = None
    for attempt in range(4):
        try:
            r = requests.get(LIST_URL, params={"page": page}, headers=HEADERS, timeout=90)
            if r.status_code == 200:
                # the listing data sits in the React-server payload pushed via self.__next_f
                chunks = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)</script>', r.text, re.S)
                payload = "".join(json.loads('"' + c + '"') for c in chunks)
                i = payload.find('{"data":{"data":[')
                if i >= 0:
                    data, _ = json.JSONDecoder().raw_decode(payload[i:])
                    data = data["data"]
                    if cache:
                        cache.parent.mkdir(parents=True, exist_ok=True)
                        cache.write_text(json.dumps(data, ensure_ascii=False))
                    time.sleep(REQUEST_DELAY)
                    return data
                last = "no listing payload"
            else:
                last = f"HTTP {r.status_code}"
        except (requests.RequestException, ValueError) as e:  # noqa: PERF203
            last = str(e)
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"page {page}: {last}")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py): last token = family name,
    after stripping degree/suffix tokens. Croucher prints names given-first
    ("Ho Wan Cheng", "Kathryn S.E. Cheah")."""
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


AWARD_RE = re.compile(r"^\s*(\d{4})\s+(.+?)(?:\s+at\s+(?:the\s+)?(.+?))?\s*$")


def parse_award(text: str) -> tuple[str | None, str | None, str | None]:
    m = AWARD_RE.match(text or "")
    if not m:
        return None, None, None
    year, inst = m.group(1), (m.group(3) or "").strip() or None
    label = re.sub(r"\s+at$", "", re.sub(r"\s+", " ", m.group(2)).strip())  # "2019 Scholarship at " (no institution)
    return year, LABEL_ALIASES.get(label, label), inst


def main() -> None:
    p = argparse.ArgumentParser(description="Croucher Foundation fellows & scholars -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only fetch the first N listing pages (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache listing JSON here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    first = fetch_page(1, args.cache_dir)
    total_pages, total = first["meta"]["total_pages"], first["meta"]["total_scholars"]
    log(f"listing: {total} people on {total_pages} pages")
    people, empty = {x["id"]: x for x in first["data"]}, 0
    last_page = min(total_pages, args.limit) if args.limit else total_pages
    for page in range(2, last_page + 1):
        d = fetch_page(page, args.cache_dir)
        if not d["data"]:
            empty += 1
            log(f"  page {page}: empty ({empty}/{MAX_CONSECUTIVE_EMPTY}); continuing")
            if empty >= MAX_CONSECUTIVE_EMPTY:
                raise RuntimeError("listing pages empty before total_pages; refusing a partial corpus")
            continue
        empty = 0
        for x in d["data"]:
            people[x["id"]] = x
        log(f"  page {page}/{total_pages}: {len(people)}/{total} people")
    if not args.limit and len(people) != total:
        raise RuntimeError(f"collected {len(people)} people, listing says {total}")

    rows, unparsed = [], []
    no_awards = [x["attributes"].get("name") for x in people.values() if not x["attributes"].get("awards")]
    if no_awards:
        log(f"  {len(no_awards)} people listed with no award entry (skipped): {no_awards}")
    for pid, x in people.items():
        a = x["attributes"]
        name = re.sub(r"\s+", " ", a.get("name") or "").strip()
        given, family = split_name(name)
        for k, text in enumerate(a.get("awards") or []):
            year, label, inst = parse_award(text)
            if label not in AWARD_TYPES:
                unparsed.append(text)
                continue
            code, ftype = AWARD_TYPES[label]
            rows.append({
                "person_id": pid,
                "slug": a["slug"],
                "name": name or None,
                "chinese_name": (a.get("chinese_name") or "").strip() or None,
                "given_name": given,
                "family_name": family,
                "award_text": text,
                "award_year": year,
                "award_label": label,
                "award_code": code,
                "funder_scheme": label if label.startswith(("Croucher", "Oxford", "Max Planck", "Todd", "Butterfield")) else f"Croucher {label}",
                "funding_type": ftype,
                "institution": inst,
                "award_index": str(k),
                "funder_award_id": f"CROUCHER-{year}-{code}-{a['slug']}",
                "landing_page_url": PERSON_URL.format(slug=a["slug"]),
            })
    if unparsed:
        raise SystemExit(f"{len(unparsed)} award strings with an unknown award type, e.g. {unparsed[:10]}")
    df = pd.DataFrame(rows)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")
    log(f"{len(people)} people -> {len(df)} awards, {df.award_year.min()}-{df.award_year.max()}")
    log(f"  by type: {df.award_label.value_counts().to_dict()}")
    for c in ["name", "family_name", "chinese_name", "institution", "award_year"]:
        log(f"  {c:14s} {df[c].notna().mean():6.1%}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    df = df.astype("string")
    parquet_path = args.output_dir / "croucher_foundation_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload or args.limit:
        log("Upload skipped")
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_croucher_foundation_projects.parquet"
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
