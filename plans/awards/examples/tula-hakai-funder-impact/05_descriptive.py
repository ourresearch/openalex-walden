"""05 - Descriptive profile of the coastal corpus.

Tables -> tables/ , figures -> figures/
"""
import json
import sys
from collections import Counter
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config
from lib import openalex as oa
from lib import plotstyle as ps

ps.apply()


def main() -> None:
    c = pd.read_csv(config.DATA / "corpus_coastal.csv")
    print(f"Coastal corpus: {len(c)} works")

    # --- works per year (exclude lone 1985 dataset artifact from the trend view) ---
    # Trend shown through 2025, the last reliable year: OpenAlex 2026
    # publication-year counts are inflated by indexing lag / misdated records, so
    # recent bars would be a misleading artifact. (Corpus totals still span 2026.)
    yr = c[(c.year >= 2007) & (c.year <= 2025)].groupby("year").size()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(yr.index, yr.values, color=ps.TEAL, width=0.8)
    ax.set(title="Tula/Hakai research outputs per year (through 2025)",
           xlabel="Year", ylabel="Works")
    ax.text(0.01, 0.97, f"n = {len(c):,} verified works (corpus spans 2026)",
            transform=ax.transAxes, va="top", color=ps.GREY)
    ps.year_axis(ax, yr.index)
    top = ax.get_ylim()[1]
    ax.axvline(2009, color=ps.ORANGE, ls=":", lw=1.6)
    ax.text(2009.15, top * 0.80, "Calvert Island\nstation (2009)", color=ps.ORANGE, fontsize=9)
    ax.axvline(2014, color=ps.ORANGE, ls=":", lw=1.6)
    ax.text(2014.15, top * 0.62, "Quadra Island\nstation (2014)", color=ps.ORANGE, fontsize=9)
    ps.save(fig, "05_works_per_year")

    # --- subfield donut (subfields fetched via API group_by) ---
    sub, subname = Counter(), {}
    ids = c["work_id"].tolist()
    for i in range(0, len(ids), 50):
        f = "ids.openalex:" + "|".join(ids[i:i + 50])
        for g in oa.group_by("works", f, "primary_topic.subfield.id"):
            if g["key"]:
                sid = oa.short_id(g["key"])
                sub[sid] += g["count"]
                subname[sid] = g.get("key_display_name", sid)
    pd.DataFrame([(subname[k], v) for k, v in sub.most_common()],
                 columns=["subfield", "works"]).to_csv(
        config.TABLES / "05_subfields.csv", index=False)
    fig, ax = plt.subplots(figsize=(9.5, 5.5))
    ps.donut(ax, [subname[k] for k in sub], list(sub.values()), top=10,
             title="What the research is about (subfields)")
    ps.save(fig, "05_subfields_donut")
    fld = c["primary_field"].replace("", pd.NA).dropna().value_counts()

    # --- top topics ---
    top = c["primary_topic"].replace("", pd.NA).dropna().value_counts().head(15)
    top.to_csv(config.TABLES / "05_top_topics.csv", header=["works"])

    # --- top venues ---
    # Full list (incl. repositories) for transparency...
    ven_all = c["venue"].replace("", pd.NA).dropna().value_counts().head(20)
    ven_all.to_csv(config.TABLES / "05_top_venues_all.csv", header=["works"])
    # ...but the figure/headline shows peer-reviewed JOURNALS only. Repositories,
    # preprint servers and thesis archives (Zenodo, bioRxiv, Open MIND, Hakai
    # Institute data, Summit/SFU theses, Technical Report) are reported separately.
    journals = c[c["venue_type"].eq("journal")
                 & c["type"].isin(["article", "review", "letter"])]
    ven = journals["venue"].replace("", pd.NA).dropna().value_counts().head(15)
    ven.to_csv(config.TABLES / "05_top_journals.csv", header=["works"])

    # --- contributing countries (author-level) ---
    cc = Counter()
    for s in c["countries"].dropna():
        for code in str(s).split("|"):
            if code:
                cc[code] += 1
    cc_df = pd.DataFrame(cc.most_common(), columns=["country", "works"])
    cc_df.to_csv(config.TABLES / "05_countries.csv", index=False)

    # --- people / institutions counts ---
    authors, insts = set(), set()
    for s in c["author_ids"].dropna():
        authors.update(x for x in str(s).split("|") if x)
    for s in c["institution_ids"].dropna():
        insts.update(x for x in str(s).split("|") if x)

    summary = {
        "n_works": len(c),
        "year_min": int(c.year.min()), "year_max": int(c.year.max()),
        "total_citations": int(c.cited_by_count.sum()),
        "unique_authors": len(authors),
        "unique_institutions": len(insts),
        "contributing_countries": len(cc),
        "pct_open_access": round(c.is_oa.mean() * 100, 1),
        "n_datasets": int((c.type == "dataset").sum()),
        "top_field": fld.index[0],
        "top_topic": top.index[0],
        "top_journal": ven.index[0],
    }
    (config.TABLES / "05_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
