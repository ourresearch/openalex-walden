"""08 - Did Hakai break BC's institutional silos?

Thesis: place-based research at the Hakai field stations pulled scientists from
different BC universities into joint work. We measure co-authorship among the five
BC universities (UBC, SFU, UVic, UNBC, VIU) INSIDE Hakai's focus coastal topics,
before vs after the 2009 field station.

Metric clarity (Kyle's note):
- The analysis is restricted to Hakai's coastal topics (listed in the figure).
- "Co-authorship links" = number of joint works summed over all 10 university pairs.
- To show this isn't just "more papers", we also track collaboration INTENSITY =
  links / total BC coastal-topic output.

Outputs: tables/08_*.csv , figures/08_links_over_time.png ,
         figures/08_network_before_after.png
"""
import json
import sys
from itertools import combinations
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config
from lib import openalex as oa
from lib import plotstyle as ps

ps.apply()

TOPICS = "|".join(config.HAKAI_TOPICS)
TOPIC_NAMES = list(config.HAKAI_TOPICS.values())
UNIS = config.BC_CORE          # name -> id (5 universities)
# Equal-length windows around the 2009 field station: 15 years before (1994-2008)
# vs 15 years after (2010-2024), with 2009 as the transition year.
Y0, Y1 = 1994, 2024
PRE = (1994, 2008)
POST = (2010, 2024)


def yc(filter_expr: str) -> pd.Series:
    out = {}
    for g in oa.group_by("works", filter_expr, "publication_year"):
        try:
            out[int(g["key"])] = g["count"]
        except (ValueError, TypeError):
            pass
    return pd.Series(out)


def main() -> None:
    yrs = list(range(Y0, Y1 + 1))
    pairs = list(combinations(UNIS.items(), 2))  # 10 pairs
    edges = {}
    for (na, ia), (nb, ib) in pairs:
        edges[f"{na}_{nb}"] = yc(
            f"authorships.institutions.id:{ia}+{ib},primary_topic.id:{TOPICS}")
    any_bc = yc("authorships.institutions.id:" + "|".join(UNIS.values())
                + f",primary_topic.id:{TOPICS}")

    df = pd.DataFrame(edges).reindex(yrs).fillna(0)
    df["any_BC"] = any_bc.reindex(yrs).fillna(0)
    df["links"] = df[list(edges)].sum(axis=1)
    df["intensity"] = (df["links"] / df["any_BC"]).replace([float("inf")], 0).fillna(0)
    df.to_csv(config.TABLES / "08_collab_series.csv")

    pre, post = df.loc[PRE[0]:PRE[1]], df.loc[POST[0]:POST[1]]
    summary = {
        "topics": TOPIC_NAMES,
        "links_pre_2009": int(pre["links"].sum()),
        "links_post_2009": int(post["links"].sum()),
        "bc_output_pre": int(pre["any_BC"].sum()),
        "bc_output_post": int(post["any_BC"].sum()),
        "intensity_pre": round(pre["links"].sum() / pre["any_BC"].sum(), 3),
        "intensity_post": round(post["links"].sum() / post["any_BC"].sum(), 3),
        "edges_pre": {k: int(pre[k].sum()) for k in edges},
        "edges_post": {k: int(post[k].sum()) for k in edges},
    }
    (config.TABLES / "08_collab_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "topics"}, indent=2))

    # --- network before vs after (5 universities, no Hakai node) ---
    import numpy as np
    names = list(UNIS)
    ang = np.linspace(90, 90 + 360, len(names), endpoint=False) * np.pi / 180
    pos = {n: (np.cos(a), np.sin(a)) for n, a in zip(names, ang)}
    node_color = {"UBC": ps.TEAL, "SFU": ps.NAVY, "UVic": ps.GOLD,
                  "UNBC": "#5a9e6f", "VIU": "#8c6bb1"}
    fig, axes = plt.subplots(1, 2, figsize=(12, 6))
    mx = max(post[k].sum() for k in edges) or 1
    for ax, (label, agg) in zip(axes, [(f"Before field station ({PRE[0]}–{PRE[1]})", pre),
                                       (f"After field station ({POST[0]}–{POST[1]})", post)]):
        for (na, _), (nb, _) in pairs:
            w = agg[f"{na}_{nb}"].sum()
            if w == 0:
                continue
            (x1, y1), (x2, y2) = pos[na], pos[nb]
            ax.plot([x1, x2], [y1, y2], color=ps.GREY, alpha=0.55,
                    lw=0.5 + 8 * w / mx, zorder=1, solid_capstyle="round")
            ax.text((x1+x2)/2, (y1+y2)/2, str(int(w)), fontsize=8, color=ps.NAVY,
                    ha="center", va="center",
                    bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.85))
        for n, (x, y) in pos.items():
            ax.scatter([x], [y], s=1100, color=node_color[n], zorder=2,
                       edgecolors="white", lw=2)
            ax.text(x, y, n, ha="center", va="center", color="white",
                    fontweight="bold", fontsize=9, zorder=3)
        ax.set_title(label, fontsize=12)
        ax.set_xlim(-1.5, 1.5); ax.set_ylim(-1.5, 1.5); ax.axis("off")
        ax.set_aspect("equal")
    fig.suptitle("Co-authorship ties among BC universities in Hakai's coastal topics",
                 fontweight="bold", y=1.02)
    fig.text(0.5, 0.04,
             "Topics: " + "; ".join(TOPIC_NAMES) +
             "\nEdge label = joint works; width scaled across both panels",
             ha="center", va="bottom", fontsize=10, color=ps.NAVY, wrap=True)
    fig.subplots_adjust(top=0.86, bottom=0.18)
    ps.save(fig, "08_network_before_after")

    print(f"\nLinks {summary['links_pre_2009']} -> {summary['links_post_2009']}; "
          f"intensity {summary['intensity_pre']} -> {summary['intensity_post']}")


if __name__ == "__main__":
    main()
