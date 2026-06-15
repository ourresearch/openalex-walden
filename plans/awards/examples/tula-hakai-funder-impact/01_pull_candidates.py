"""01 - Assemble Tula/Hakai candidate works from every search strategy.

Unions all strategies, dedups by OpenAlex work ID, and records a boolean
provenance flag per strategy so downstream steps can audit precision-by-strategy
and the methods box can report exactly how each work was found.

Output: data/candidates.csv
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config
from lib import openalex as oa

SELECT = "id,display_name,publication_year,type,primary_location"


def venue_of(rec: dict) -> str:
    loc = rec.get("primary_location") or {}
    src = loc.get("source") or {}
    return src.get("display_name") or ""


def main() -> None:
    records: dict[str, dict] = {}  # work_id -> row

    for flag, filter_expr in config.STRATEGIES:
        n = oa.count("works", filter_expr)
        print(f"[{flag}] {filter_expr}  -> {n} works")
        got = 0
        for rec in oa.paginate("works", filter_expr, select=SELECT):
            wid = oa.short_id(rec["id"])
            row = records.get(wid)
            if row is None:
                row = {
                    "work_id": wid,
                    "title": rec.get("display_name") or "",
                    "year": rec.get("publication_year"),
                    "type": rec.get("type") or "",
                    "venue": venue_of(rec),
                    **{f: False for f in config.PROV_FLAGS},
                }
                records[wid] = row
            row[flag] = True
            got += 1
        print(f"    collected {got} (running unique total: {len(records)})")

    df = pd.DataFrame(list(records.values()))

    # prov_dataset: any candidate whose type is a dataset (Hakai-published data)
    df["prov_dataset"] = df["type"].eq("dataset")

    # convenience columns
    prov_cols = config.PROV_FLAGS + ["prov_dataset"]
    df["provenance_list"] = df[prov_cols].apply(
        lambda r: "|".join(c.replace("prov_", "") for c in prov_cols if r[c]), axis=1
    )
    df["n_strategies"] = df[config.PROV_FLAGS].sum(axis=1)
    df["has_strong_prov"] = df[config.STRONG_PROV].any(axis=1)

    df = df.sort_values(["has_strong_prov", "n_strategies", "year"],
                        ascending=[False, False, True])
    out = config.DATA / "candidates.csv"
    df.to_csv(out, index=False, encoding="utf-8")

    print(f"\nWrote {len(df)} unique candidate works -> {out}")
    print("\nProvenance counts:")
    for c in prov_cols:
        print(f"  {c:22s} {int(df[c].sum()):5d}")
    print(f"\n  strong-provenance works: {int(df['has_strong_prov'].sum())}")
    print(f"  weak-provenance only:    {int((~df['has_strong_prov']).sum())}")
    print("\n  works by type:")
    print(df["type"].value_counts().head(12).to_string())


if __name__ == "__main__":
    main()
