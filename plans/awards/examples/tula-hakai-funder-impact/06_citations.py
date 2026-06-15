"""06 - Citation impact of the coastal corpus.

Total/mean/median citations, FWCI, top-1%/top-10% percentile shares vs the world
baseline, FWCI trend over time, most-cited works.
"""
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config
from lib import plotstyle as ps

ps.apply()


def main() -> None:
    c = pd.read_csv(config.DATA / "corpus_coastal.csv")
    has_f = c[c.fwci.notna()]

    metrics = {
        "n_works": len(c),
        "n_with_fwci": len(has_f),
        "total_citations": int(c.cited_by_count.sum()),
        "mean_citations": round(c.cited_by_count.mean(), 1),
        "median_citations": int(c.cited_by_count.median()),
        "mean_fwci": round(has_f.fwci.mean(), 2),
        "median_fwci": round(has_f.fwci.median(), 2),
        "pct_fwci_above_1": round((has_f.fwci > 1).mean() * 100, 1),
        "n_top_1pct": int(c.is_top_1pct.sum()),
        "n_top_10pct": int(c.is_top_10pct.sum()),
        "share_top_1pct": round(c.is_top_1pct.mean() * 100, 1),
        "share_top_10pct": round(c.is_top_10pct.mean() * 100, 1),
        # how many times the world baseline (1% / 10%)
        "top_1pct_vs_world": round(c.is_top_1pct.mean() / 0.01, 1),
        "top_10pct_vs_world": round(c.is_top_10pct.mean() / 0.10, 1),
    }
    (config.TABLES / "06_metrics.json").write_text(
        json.dumps(metrics, indent=2), encoding="utf-8")
    print(json.dumps(metrics, indent=2))

    # --- percentile share vs world baseline ---
    fig, ax = plt.subplots(figsize=(7, 4.5))
    cats = ["Top 1%", "Top 10%"]
    world = [1, 10]
    corpus = [metrics["share_top_1pct"], metrics["share_top_10pct"]]
    x = np.arange(len(cats))
    ax.bar(x - 0.2, world, 0.4, label="World baseline", color=ps.GREY)
    ax.bar(x + 0.2, corpus, 0.4, label="Hakai/Tula coastal", color=ps.TEAL)
    for i, val in enumerate(corpus):
        ax.text(i + 0.2, val + 0.3, f"{val}%", ha="center", color=ps.TEAL_DK,
                fontweight="bold")
    ax.set(title="Share of works in the most-cited percentiles",
           ylabel="% of works", xticks=x, xticklabels=cats)
    ax.legend()
    ps.save(fig, "06_percentile_share")

    # --- most-cited works ---
    cols = ["work_id", "title", "year", "venue", "cited_by_count", "fwci",
            "is_top_1pct"]
    top = c.sort_values("cited_by_count", ascending=False)[cols].head(20)
    top.to_csv(config.TABLES / "06_most_cited.csv", index=False, encoding="utf-8")
    print("\nTop 8 most-cited:")
    for _, r in top.head(8).iterrows():
        f = "" if pd.isna(r.fwci) else f"FWCI {r.fwci:.1f}"
        print(f"  {int(r.year)} | {int(r.cited_by_count):>5} cites | {f:>9} | "
              f"{str(r.title)[:62]}")


if __name__ == "__main__":
    main()
