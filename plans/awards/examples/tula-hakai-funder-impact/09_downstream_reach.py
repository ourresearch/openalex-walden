"""09 - Downstream reach: who cites Hakai/Tula coastal research?

For the whole coastal corpus we aggregate the CITING works by country and by field
(batched `cites:` group_by queries). Country/field tallies are citation *instances*
(a citing work that cites two corpus works is counted twice) — fine for showing the
shape of reach; the headline volume number is the corpus's total citation count.
Distinct-country and distinct-field counts (union of keys) are exact.

Outputs: tables/09_*.csv , figures/09_citing_countries.png , figures/09_citing_fields.png
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

# ISO2 -> display name for the top countries (kept small; others shown by code)
CC_NAME = {
    "US": "United States", "CN": "China", "GB": "United Kingdom", "DE": "Germany",
    "CA": "Canada", "AU": "Australia", "FR": "France", "JP": "Japan", "IT": "Italy",
    "ES": "Spain", "NL": "Netherlands", "BR": "Brazil", "SE": "Sweden",
    "IN": "India", "NO": "Norway", "KR": "South Korea", "CH": "Switzerland",
    "DK": "Denmark", "PT": "Portugal", "NZ": "New Zealand",
}


def main() -> None:
    c = pd.read_csv(config.DATA / "corpus_coastal.csv")
    ids = c["work_id"].tolist()
    countries, fields = Counter(), Counter()
    field_name = {}

    for i in range(0, len(ids), BATCH):
        f = "cites:" + "|".join(ids[i:i + BATCH])
        for g in oa.group_by("works", f, "authorships.countries"):
            if g["key"] and g["key"] != "unknown":
                countries[oa.short_id(g["key"])] += g["count"]
        for g in oa.group_by("works", f, "primary_topic.field.id"):
            if g["key"]:
                fid = oa.short_id(g["key"])
                fields[fid] += g["count"]
                field_name[fid] = g.get("key_display_name", fid)
        print(f"  {min(i+BATCH,len(ids))}/{len(ids)} batches done")

    reach = {
        "corpus_works": len(c),
        "total_citations": int(c.cited_by_count.sum()),
        "distinct_citing_countries": len(countries),
        "distinct_citing_fields": len(fields),
        "top_countries": [(k, CC_NAME.get(k, k), v) for k, v in countries.most_common(20)],
        "top_fields": [(field_name[k], v) for k, v in fields.most_common(12)],
    }
    (config.TABLES / "09_reach.json").write_text(
        json.dumps(reach, indent=2), encoding="utf-8")
    pd.DataFrame([(k, CC_NAME.get(k, k), v) for k, v in countries.most_common()],
                 columns=["code", "country", "citation_instances"]
                 ).to_csv(config.TABLES / "09_citing_countries.csv", index=False)
    pd.DataFrame(reach["top_fields"], columns=["field", "citation_instances"]
                 ).to_csv(config.TABLES / "09_citing_fields.csv", index=False)
    print(json.dumps({k: v for k, v in reach.items()
                      if k not in ("top_countries", "top_fields")}, indent=2))

    # --- world map of citing countries (all of them) ---
    import geopandas as gpd
    import matplotlib.colors as mcolors
    from matplotlib.cm import ScalarMappable

    geo = config.CACHE / "world.geojson"
    if not geo.exists():
        import requests
        url = ("https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
               "master/geojson/ne_110m_admin_0_countries.geojson")
        geo.write_bytes(requests.get(url, timeout=60).content)
    world = gpd.read_file(geo)
    # OpenAlex country codes are ISO-2; Natural Earth ISO_A2_EH is the corrected field
    world["iso2"] = world["ISO_A2_EH"].where(world["ISO_A2_EH"] != "-99", world["ISO_A2"])
    cc = pd.DataFrame({"iso2": list(countries.keys()),
                       "cites": list(countries.values())})
    world = world.merge(cc, on="iso2", how="left")
    world = world[world["NAME"] != "Antarctica"].to_crs("EPSG:8857")  # Equal Earth

    teal_cmap = mcolors.LinearSegmentedColormap.from_list(
        "teal", ["#d9ecee", ps.TEAL, ps.TEAL_DK])
    norm = mcolors.LogNorm(vmin=1, vmax=max(countries.values()))
    fig, ax = plt.subplots(figsize=(12, 6.2))
    world.plot(ax=ax, color="#eef2f3", edgecolor="white", linewidth=0.3)
    world[world["cites"].notna()].plot(
        ax=ax, column="cites", cmap=teal_cmap, norm=norm,
        edgecolor="white", linewidth=0.3)
    ax.axis("off")
    ax.set_title("Where Hakai/Tula coastal research is cited\n"
                 f"(citing works span {len(countries)} countries)",
                 fontweight="bold")
    sm = ScalarMappable(norm=norm, cmap=teal_cmap); sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, fraction=0.022, pad=0.01)
    cbar.set_label("Citing works (log scale)")
    ps.save(fig, "09_citing_map")

    # --- citing fields: donut + clock view ---
    labs = [field_name[k] for k, _ in fields.most_common()]
    vals = [v for _, v in fields.most_common()]
    fig, ax = plt.subplots(figsize=(9.5, 5.5))
    ps.donut(ax, labs, vals, top=8, title="Fields that cite Hakai/Tula research")
    ps.save(fig, "09_citing_fields_donut")

    print(f"\nReach: cited by works from {len(countries)} countries across "
          f"{len(fields)} fields.")


if __name__ == "__main__":
    main()
