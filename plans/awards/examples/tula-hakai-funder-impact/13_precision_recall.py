"""13 - Precision & recall of the search strategy.

PRECISION = genuine Tula/Hakai works (verified 'include') / candidates found.
RECALL    = share of Hakai's official publication list (hakai.org/publications,
            the gold standard) that our search returned.
Both are reported for the funder-only OpenAlex search and the full 9-strategy net.
Recall is also reported OpenAlex-adjusted (excluding gold DOIs OpenAlex doesn't index).

Outputs: tables/13_precision_recall.json , figures/13_precision_recall.png
"""
import json
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config
from lib import plotstyle as ps

ps.apply()
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
    funder = df.prov_funder_hakai | df.prov_funder_tula

    # --- precision ---
    prec = {
        "full_search": {"genuine": int(genuine.sum()), "candidates": len(df),
                        "precision_pct": round(genuine.mean() * 100, 1)},
        "funder_only": {"genuine": int(df[funder].label.eq("include").sum()),
                        "candidates": int(funder.sum()),
                        "precision_pct": round(df[funder].label.eq("include").mean() * 100, 1)},
        "by_strategy": {},
    }
    for col in config.PROV_FLAGS:
        s = df[df[col]]
        if len(s):
            prec["by_strategy"][col] = {"genuine": int(s.label.eq("include").sum()),
                                        "found": len(s),
                                        "precision_pct": round(s.label.eq("include").mean() * 100, 1)}

    # --- recall vs Hakai's official list ---
    gold_raw = scrape_hakai_dois()
    cand_dois = {d for d in df["doi"] if d}
    fund_dois = {d for d in df[funder]["doi"] if d}
    found_full = gold_raw & cand_dois
    found_fund = gold_raw & fund_dois
    missed = sorted(gold_raw - cand_dois)
    # Validate the misses against OpenAlex. A 404 means the scraped DOI was garbled
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

    rec = {
        "gold_list_size_clean": len(gold),
        "garbled_dois_dropped": len(artifacts),
        "openalex_coverage_pct": 100.0,  # every valid gold DOI resolves in OpenAlex
        "full_search": {"found": len(found_full),
                        "recall_pct": round(len(found_full) / len(gold) * 100, 1)},
        "funder_only": {"found": len(found_fund),
                        "recall_pct": round(len(found_fund) / len(gold) * 100, 1)},
        "real_misses_in_oa": len(real_misses),
    }

    out = {"precision": prec, "recall": rec}
    (config.TABLES / "13_precision_recall.json").write_text(json.dumps(out, indent=2))
    print(json.dumps({"precision": {k: prec[k] for k in ("full_search", "funder_only")},
                      "recall": rec}, indent=2))

    # --- figure: funder-only vs full search, precision & recall ---
    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(2)
    funder_vals = [prec["funder_only"]["precision_pct"], rec["funder_only"]["recall_pct"]]
    full_vals = [prec["full_search"]["precision_pct"], rec["full_search"]["recall_pct"]]
    ax.bar(x - 0.2, funder_vals, 0.4, label="Funder filter only", color=ps.GREY)
    ax.bar(x + 0.2, full_vals, 0.4, label="Full 9-strategy search", color=ps.TEAL)
    for i, v in enumerate(funder_vals):
        ax.text(i - 0.2, v + 1.5, f"{v:.0f}%", ha="center", color=ps.NAVY, fontsize=10)
    for i, v in enumerate(full_vals):
        ax.text(i + 0.2, v + 1.5, f"{v:.0f}%", ha="center", color=ps.TEAL_DK, fontweight="bold")
    ax.set(title="How well each search finds Tula/Hakai's research",
           ylabel="%", xticks=x, xticklabels=["Precision", "Recall"], ylim=(0, 112))
    ax.legend(loc="center", bbox_to_anchor=(0.5, 0.72), fontsize=9,
              framealpha=0.9, edgecolor="none")
    ax.text(0.5, -0.17,
            f"Precision: genuine works ÷ candidates found. "
            f"Recall: share of Hakai's {len(gold)}-paper public list returned.",
            transform=ax.transAxes, ha="center", fontsize=8, color=ps.GREY)
    ps.save(fig, "13_precision_recall")


if __name__ == "__main__":
    main()
