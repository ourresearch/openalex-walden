"""07 - Field-level influence: did BC pull ahead in Hakai's focus topics?

Design (difference-in-differences on global SHARE, indexed to a treatment year):
  treatment series = BC's share of GLOBAL output in Hakai-focus marine/coastal
                     topics, by year  (BC_in_topics / world_in_topics)
  baseline series  = BC's share of GLOBAL output across ALL science, by year
                     (BC_all / world_all)
A widening gap after the treatment year (Calvert station, 2009) is the effect:
BC's footprint in exactly the topics Hakai funded grows faster than BC's footprint
in science overall. This normalizes out (a) the global growth of marine science and
(b) BC's general research growth. Honest about limitations: associational, topic
assignment is OpenAlex's, BC includes non-Hakai marine labs.
"""
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config
from lib import openalex as oa
from lib import plotstyle as ps

ps.apply()

Y0, Y1 = 2000, 2025
BC = "|".join(config.BC_UNIVERSITIES.values())
TOPICS = "|".join(config.HAKAI_TOPICS)


def year_counts(filter_expr: str) -> dict[int, int]:
    out = {}
    for g in oa.group_by("works", filter_expr, "publication_year"):
        try:
            out[int(g["key"])] = g["count"]
        except (ValueError, TypeError):
            pass
    return out


def main() -> None:
    yrs = list(range(Y0, Y1 + 1))
    series = {
        "bc_topics":   year_counts(f"authorships.institutions.id:{BC},primary_topic.id:{TOPICS}"),
        "world_topics": year_counts(f"primary_topic.id:{TOPICS}"),
        "bc_all":      year_counts(f"authorships.institutions.id:{BC}"),
        "world_all":   year_counts("type:article"),
    }
    df = pd.DataFrame({k: pd.Series(v) for k, v in series.items()}).reindex(yrs).fillna(0)
    df["share_topics"] = df["bc_topics"] / df["world_topics"] * 100
    df["share_all"] = df["bc_all"] / df["world_all"] * 100
    # Specialization: how many times more represented BC is in Hakai's topics than
    # in science overall. Robust to global/BC growth trends (it is a ratio of shares).
    df["specialization"] = df["share_topics"] / df["share_all"]
    df.to_csv(config.TABLES / "07_did_series.csv")

    # --- DiD sensitivity across plausible treatment years (honest reporting) ---
    def avg(col, rng): return df.loc[df.index.isin(rng), col].mean()
    did_rows = []
    for ty in (2002, 2005, 2009):
        pre, post = range(ty - 4, ty), range(ty + 1, ty + 6)
        dt = avg("share_topics", post) - avg("share_topics", pre)
        da = avg("share_all", post) - avg("share_all", pre)
        did_rows.append({"treatment_year": ty,
                         "delta_topics_pp": round(dt, 3),
                         "delta_baseline_pp": round(da, 3),
                         "did_pp": round(dt - da, 3)})
    headline = {
        "specialization_2018_2022": round(df.loc[2018:2022, "specialization"].mean(), 1),
        "bc_share_topics_recent": round(df.loc[2018:2022, "share_topics"].mean(), 2),
        "bc_share_all_recent": round(df.loc[2018:2022, "share_all"].mean(), 2),
        "bc_topics_output_2000": int(df.loc[2000, "bc_topics"]),
        "bc_topics_output_2018_2022_avg": int(df.loc[2018:2022, "bc_topics"].mean()),
        "did_sensitivity": did_rows,
        "note": ("BC was already a marine-science leader and Tula's grants began in the "
                 "early 2000s, so a clean causal DiD around the 2009 field station is "
                 "weak; the robust finding is specialization, not a 2009 discontinuity."),
    }
    (config.TABLES / "07_field_influence.json").write_text(
        json.dumps(headline, indent=2), encoding="utf-8")
    print(json.dumps(headline, indent=2))

    d = df.loc[2000:2025]
    # --- figure 1: specialization ratio over time ---
    fig, ax = plt.subplots(figsize=(9.5, 5))
    ax.fill_between(d.index, d["specialization"], 1, color=ps.TEAL, alpha=0.18)
    ax.plot(d.index, d["specialization"], color=ps.TEAL_DK, lw=2.8, marker="o", ms=4)
    ax.axhline(1.0, color=ps.GREY, ls="--", lw=1.2)
    ax.text(2000.2, 1.25, "parity (no specialization)", color=ps.GREY, fontsize=9)
    ax.set(title="BC is far over-represented in the topics Hakai funds\n"
                 "(BC's share of world output in Hakai topics ÷ BC's share of all science)",
           xlabel="Year", ylabel="Specialization ratio (×)", ylim=(0, None))
    ps.year_axis(ax, d.index)
    ps.save(fig, "07_specialization")

    print(f"\nHeadline: BC ~{headline['specialization_2018_2022']}x over-represented in "
          f"Hakai topics; output grew {headline['bc_topics_output_2000']} -> "
          f"{headline['bc_topics_output_2018_2022_avg']} works/yr.")


if __name__ == "__main__":
    main()
