#!/usr/bin/env python3
"""HAL deposits whose own text names the European grant they declare, to S3 (oxjob #2594, ZD 28397).

HAL lists the projects each deposit declares (europeanProjectReference_s, the grant agreement number). Declared links alone
graded 66/100 right on 25 Nov (two blind graders, 100 random links), so, as #362 rules for ANR (hal_anr_work_links), a link
counts only when the deposited full text prints the number. HAL's public search API answers that per number in one query:
    europeanProjectReference_s:"<n>" AND fulltext_t:"<n>"
Output: s3://openalex-ingest/awards/hal_eu/hal_eu_links_<YYYY-MM-DD>.parquet, one row per (hal_id, grant_number), read by
notebooks/awards/CreateFunderReportedLinks.sql (sources cordis and erc: the number must be a CORDIS or ERC award in raw).

Usage:
    python scripts/local/hal_eu_links_to_s3.py --output-dir /tmp/hal_eu [--skip-upload] [--workers 3]
Resumable within a day: answers are cached per number in <output-dir>/answers_<day>.jsonl; a re-run the same day asks only the
numbers not answered yet, and a run on a new day asks every number again (a refresh never reuses an older day's answers).
Only European grant agreement numbers are asked (6 digits, or 101 + 6 digits: nih_fold.EU_NUMBER); a short or odd value could
match an unrelated token in a text.
"""
import argparse
import datetime
import json
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests

API = "https://api.archives-ouvertes.fr/search/"
HEADERS = {"User-Agent": "openalex-grant-links"}
S3_PREFIX = "s3://openalex-ingest/awards/hal_eu/"
MIN_NUMBERS = 8000          # 25 Nov 2026: 13,651 numbers declared; fewer means a broken facet answer, not HAL shrinking
MIN_PAIRS = 30000           # 25 Nov 2026: 39,968 verified pairs; fewer means the full-text index answered short
EU_NUMBER = re.compile(r"^([0-9]{6}|101[0-9]{6})$")   # as notebooks/awards/nightly_grants/lib/nih_fold.py


def get(params, tries=5):
    for i in range(tries):
        try:
            r = requests.get(API, params={**params, "wt": "json"}, headers=HEADERS, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(2 * (i + 1))


def declared_numbers():
    facet = get({"q": "europeanProjectReference_s:*", "rows": 0, "facet": "true", "facet.field": "europeanProjectReference_s",
                 "facet.limit": -1, "facet.mincount": 1})["facet_counts"]["facet_fields"]["europeanProjectReference_s"]
    return {facet[i].strip(): facet[i + 1] for i in range(0, len(facet), 2) if facet[i].strip()}


def verified(number):
    """The deposits that declare the number and whose indexed full text prints it."""
    q = f'europeanProjectReference_s:"{number}" AND fulltext_t:"{number}"'
    answer = get({"q": q, "fl": "halId_s", "rows": 10000})
    r = answer["response"]
    if answer.get("responseHeader", {}).get("partialResults") or len(r["docs"]) != r["numFound"]:
        raise RuntimeError(f"HAL answered {number} partially: {len(r['docs'])} of {r['numFound']} deposits")
    return sorted({d["halId_s"] for d in r["docs"]})


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--workers", type=int, default=3)
    args = ap.parse_args()
    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    declared = declared_numbers()
    numbers = {n: k for n, k in declared.items() if EU_NUMBER.match(n)}
    print(f"{len(declared)} values declared; {len(declared) - len(numbers)} are not European grant agreement numbers, skipped")
    if len(numbers) < MIN_NUMBERS:
        print(f"STOP: HAL's facet gave {len(numbers)} declared European project numbers, fewer than {MIN_NUMBERS}"); return 1
    day = datetime.date.today().isoformat()
    cache = out / f"answers_{day}.jsonl"
    done = {}
    if cache.exists():
        for line in cache.read_text().splitlines():
            a = json.loads(line); done[a["number"]] = a["hal_ids"]
    todo = [n for n in sorted(numbers) if n not in done]
    print(f"{len(numbers)} numbers declared; {len(done)} answered before; asking {len(todo)}", flush=True)
    lock = threading.Lock()
    def ask(n):
        ids = verified(n)
        with lock, cache.open("a") as fh:
            fh.write(json.dumps({"number": n, "hal_ids": ids}) + "\n"); done[n] = ids
        if len(done) % 500 == 0:
            print(f"  {len(done)} answered", flush=True)
    with ThreadPoolExecutor(args.workers) as pool:
        list(pool.map(ask, todo))
    rows = [(h, n, day) for n in sorted(done) if n in numbers for h in done[n]]
    df = pd.DataFrame(rows, columns=["hal_id", "grant_number", "checked_on"]).drop_duplicates()
    if len(df) < MIN_PAIRS:
        print(f"STOP, nothing written: {len(df)} verified pairs, fewer than {MIN_PAIRS}"); return 1
    path = out / f"hal_eu_links_{day}.parquet"
    df.to_parquet(path, index=False)
    print(f"{len(df)} (deposit, grant) pairs whose text names the number, {df.hal_id.nunique()} deposits, {df.grant_number.nunique()} grants -> {path}")
    if args.skip_upload:
        print(f"--skip-upload: would copy {path} to {S3_PREFIX}{path.name}"); return 0
    subprocess.run(["aws", "s3", "cp", str(path), S3_PREFIX + path.name], check=True)
    print(f"UPLOADED {S3_PREFIX}{path.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
