"""11 - Co-funding: who else funded the same work?

Many Tula/Hakai works acknowledge other funders too. Aggregating those shows which
agencies and foundations Tula/Hakai has effectively helped advance — a useful signal
for prospective co-funders. We group the coastal corpus by funder (batched
`ids.openalex` + group_by funders.id), then strip Tula/Hakai themselves.

Outputs: tables/11_cofunders.csv , figures/11_cofunders.png
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

BATCH = 50
SELF = {config.FUNDER_HAKAI, config.FUNDER_TULA}
# Universities show up as "funders" via internal grants, which is confusing in a
# co-funder list. Exclude funders whose name marks them as a university/college.
UNI_MARKERS = ("university", "université", "universidad", "universität",
               "college", "polytechnique", "institute of technology")


def is_university(name: str) -> bool:
    n = (name or "").lower()
    return any(m in n for m in UNI_MARKERS)


def main() -> None:
    c = pd.read_csv(config.DATA / "corpus_coastal.csv")
    ids = c["work_id"].tolist()
    funders, name = Counter(), {}
    works_with_funder = 0

    for i in range(0, len(ids), BATCH):
        f = "ids.openalex:" + "|".join(ids[i:i + BATCH])
        for g in oa.group_by("works", f, "funders.id"):
            if not g["key"]:
                continue
            fid = oa.short_id(g["key"])
            nm = g.get("key_display_name", fid)
            name[fid] = nm
            if fid in SELF or is_university(nm):
                continue
            funders[fid] += g["count"]
        print(f"  {min(i+BATCH,len(ids))}/{len(ids)}")

    # works that acknowledge at least one funder (Hakai/Tula included)
    n_with = int((c["grant_funders"].fillna("").str.len() > 0).sum()) \
        if "grant_funders" in c.columns else None

    rows = [{"funder_id": k, "funder": name[k], "works": v}
            for k, v in funders.most_common()]
    pd.DataFrame(rows).to_csv(config.TABLES / "11_cofunders.csv", index=False,
                             encoding="utf-8")
    out = {
        "distinct_cofunders": len(funders),
        "top_cofunders": rows[:25],
    }
    (config.TABLES / "11_cofunders.json").write_text(
        json.dumps(out, indent=2), encoding="utf-8")
    print(f"\ndistinct co-funders: {len(funders)}")
    for r in rows[:15]:
        print(f"  {r['works']:4d}  {r['funder']}")

    top = rows[:12]
    fig, ax = plt.subplots(figsize=(9, 6))
    labels = [r["funder"] for r in top][::-1]
    vals = [r["works"] for r in top][::-1]
    ax.barh(labels, vals, color=ps.TEAL)
    ax.set(title="Top co-funders of Hakai/Tula coastal research\n"
                 "(other funders acknowledged on the same works)",
           xlabel="Shared works")
    ax.grid(axis="y", visible=False)
    ps.save(fig, "11_cofunders")


if __name__ == "__main__":
    main()
