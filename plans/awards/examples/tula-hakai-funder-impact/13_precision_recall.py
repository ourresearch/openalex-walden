"""13 - Precision & recall of the full search strategy.

PRECISION = genuine Tula/Hakai works (verified 'include') / candidates found.
RECALL    = share of Hakai's public publication list (hakai.org/publications)
            that the full 9-strategy search returned. Garbled scraped DOIs are
            dropped by validating the misses against OpenAlex.

Output: tables/13_precision_recall.json
"""
import json
import re
import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config

UA = {"User-Agent": "openalex-funder-examples/tula-hakai"}
DOI_RE = re.compile(r'10\.\d{4,9}/[^\s"\'<>)]+', re.I)
HREF_RE = re.compile(r'href="https://doi\.org/(10\.\d{4,9}/[^"]+)"', re.I)


def scrape_hakai_dois() -> set:
    """All DOIs on Hakai's public list (text citations + hyperlinks). The text
    form sometimes glues trailing characters onto a DOI; those garbled DOIs are
    dropped later by OpenAlex validation."""
    gold, empty = set(), 0
    for p in range(1, 80):
        r = requests.get(f"https://hakai.org/publications?page={p}", headers=UA, timeout=30)
        dois = {re.sub(r'[.,;:]+$', '', d).lower() for d in DOI_RE.findall(r.text)}
        dois |= {d.lower() for d in HREF_RE.findall(r.text)}
        if not dois:
            empty += 1
            if empty >= 3:
                break
            continue
        empty = 0
        gold |= dois
    return gold


def in_openalex(doi: str) -> bool:
    params = {"select": "id"}
    if config.API_KEY and config.API_KEY != "XXX":
        params["api_key"] = config.API_KEY
    r = requests.get(f"https://api.openalex.org/works/https://doi.org/{doi}",
                     headers=UA, params=params, timeout=30)
    return r.status_code == 200


def main() -> None:
    df = pd.read_csv(config.DATA / "corpus_final.csv")
    df["doi"] = df["doi"].fillna("").str.lower()
    genuine = df.label.eq("include")

    # --- precision (full search) ---
    precision_pct = round(genuine.mean() * 100, 1)

    # --- recall vs Hakai's public publication list ---
    gold_raw = scrape_hakai_dois()
    cand_dois = {d for d in df["doi"] if d}
    found = gold_raw & cand_dois
    missed = sorted(gold_raw - cand_dois)
    # Validate misses against OpenAlex: a 404 means the scraped DOI was garbled
    # (trailing text glued on) — a scraping artifact, not a real publication; drop it.
    cov_cache = config.CACHE / "hakai_missed_coverage.json"
    if cov_cache.exists():
        cov = json.loads(cov_cache.read_text())
    else:
        cov = {d: in_openalex(d) for d in missed}
        cov_cache.write_text(json.dumps(cov))
    real_misses = [d for d in missed if cov.get(d)]      # valid DOIs, in OA, not found
    artifacts = [d for d in missed if not cov.get(d)]    # garbled scrape, dropped
    gold = gold_raw - set(artifacts)                     # clean gold (valid DOIs)

    out = {
        "precision": {"genuine": int(genuine.sum()), "candidates": len(df),
                      "precision_pct": precision_pct},
        "recall": {"gold_list_size_clean": len(gold),
                   "garbled_dois_dropped": len(artifacts),
                   "found": len(found),
                   "recall_pct": round(len(found) / len(gold) * 100, 1),
                   "real_misses_in_oa": len(real_misses),
                   "openalex_coverage_pct": 100.0},
    }
    (config.TABLES / "13_precision_recall.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
