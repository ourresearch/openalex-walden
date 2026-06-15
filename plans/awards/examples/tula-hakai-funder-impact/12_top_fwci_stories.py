"""12 - The science behind the numbers: top works by FWCI.

Pulls the highest field-weighted-impact works so we can tell the story of what the
research actually was. Flags likely mega-consortium papers (huge author lists) where
Hakai is one of many contributors, vs. Hakai-central breakthroughs.

Outputs: tables/12_top_fwci.csv (with abstracts for curation)
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config

N = 50


def main() -> None:
    c = pd.read_csv(config.DATA / "corpus_coastal.csv")
    c = c[c.fwci.notna()].copy()
    top = c.sort_values("fwci", ascending=False).head(N)
    cols = ["work_id", "doi", "title", "year", "venue", "fwci", "cited_by_count",
            "n_authors", "primary_topic", "is_top_1pct", "abstract"]
    out = top[cols].copy()
    out["mega_consortium"] = out["n_authors"] >= 50
    out.to_csv(config.TABLES / "12_top_fwci.csv", index=False, encoding="utf-8")
    print(f"Top {N} by FWCI -> tables/12_top_fwci.csv")
    print(f"likely mega-consortium (>=50 authors): {int(out.mega_consortium.sum())}/{N}\n")
    for _, r in out.iterrows():
        flag = "  [consortium]" if r.mega_consortium else ""
        print(f"FWCI {r.fwci:6.1f} | {int(r.year)} | {int(r.cited_by_count):>5} cites | "
              f"{int(r.n_authors):>3} auth{flag} | {str(r.title)[:72]}")


if __name__ == "__main__":
    main()
