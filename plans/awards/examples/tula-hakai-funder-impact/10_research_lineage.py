"""10 - The sea-otter & kelp research lineage on the central coast.

(A full trainee-career analysis was scoped out — doing it well means tracing every
Hakai trainee, which is a project of its own. We keep only the research-lineage view.)

Outputs: tables/10_lineage.json , figures/10_otter_kelp_lineage.png
"""
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config
from lib import plotstyle as ps

ps.apply()


def main() -> None:
    c = pd.read_csv(config.DATA / "corpus_coastal.csv")
    text = (c["title"].fillna("") + " " + c["abstract"].fillna("")).str.lower()
    otter = c[text.str.contains("sea otter") | text.str.contains("enhydra")]
    kelp = c[text.str.contains("kelp") | text.str.contains("macrocystis")
             | text.str.contains("nereocystis")]
    lineage = {
        "otter_works": len(otter), "kelp_works": len(kelp),
        "otter_citations": int(otter.cited_by_count.sum()),
        "kelp_citations": int(kelp.cited_by_count.sum()),
    }
    (config.TABLES / "10_lineage.json").write_text(
        json.dumps(lineage, indent=2), encoding="utf-8")

    # cap trend at 2025 (2026 publication years are
    # inflated by OpenAlex indexing lag / misdated records)
    oy = otter[(otter.year >= 2008) & (otter.year <= 2025)].groupby("year").size()
    ky = kelp[(kelp.year >= 2008) & (kelp.year <= 2025)].groupby("year").size()
    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    ax.bar(ky.index - 0.2, ky.values, 0.4, label=f"Kelp ({len(kelp)} works)", color=ps.TEAL)
    ax.bar(oy.index + 0.2, oy.values, 0.4, label=f"Sea otter ({len(otter)} works)", color=ps.ORANGE)
    ax.set(title="The sea-otter & kelp research lineage on the Central Coast",
           xlabel="Year", ylabel="Works/year")
    ps.year_axis(ax, list(ky.index) + list(oy.index))
    ax.legend()
    ps.save(fig, "10_otter_kelp_lineage")
    print("lineage:", json.dumps(lineage))


if __name__ == "__main__":
    main()
