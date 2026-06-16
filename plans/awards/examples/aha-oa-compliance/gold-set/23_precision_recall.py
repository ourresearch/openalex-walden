"""23 - Precision / recall of OpenAlex AHA-funder linkage vs the gold set.

Gold classes (from 21):
  positive / positive_no_grant : AHA verified these ARE theirs  -> OA should link
  negative                     : AHA verified these are NOT theirs -> OA should NOT link
  ambiguous / pending          : excluded from scoring

RECALL (funder)  = AHA-linked in OA / gold positives.
FALSE-POSITIVE   = gold negatives that OA *does* link to AHA (precision proxy on the
                   Dimensions candidate pool — note these are not random works).
GRANT level      = the public OA `funders` field carries no award IDs, so grant-level
                   linkage is structurally ~absent; quantified separately.

Writes tables/23_precision_recall.json and data/misses.csv (de-identified).

Run:  PYTHONIOENCODING=utf-8 py -3 23_precision_recall.py
"""
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C

POS = {"positive", "positive_no_grant"}


def pct(n, d):
    return round(100 * n / d, 1) if d else 0.0


def main() -> None:
    gold = pd.read_csv(C.DATA / "goldset.csv", dtype=str, keep_default_na=False)
    m = pd.read_csv(C.DATA / "matched.csv", keep_default_na=False)
    # normalize match bools
    for col in ("found_oa", "oa_aha_funder"):
        m[col] = m[col].astype(str).str.lower().eq("true")
    df = gold.merge(m, on="doi", how="left").drop_duplicates("doi")
    df["found_oa"] = df["found_oa"].fillna(False)
    df["oa_aha_funder"] = df["oa_aha_funder"].fillna(False)

    pos = df[df.gold_class.isin(POS)]
    neg = df[df.gold_class == "negative"]

    # --- recall (funder level) ---
    n_pos = len(pos)
    in_oa = pos.found_oa.sum()
    linked = pos.oa_aha_funder.sum()
    miss_linked = pos[pos.found_oa & ~pos.oa_aha_funder]      # in OA, not AHA-linked
    not_in_oa = pos[~pos.found_oa]

    # --- false positives (gold negatives that OA links to AHA) ---
    fp = neg[neg.oa_aha_funder]

    # --- grant level (structural) ---
    # gold positives that cite an AHA grant ID but OA has no award linkage at all
    pos_with_grant = pos[pos.n_aha_grants.astype(int) > 0] if "n_aha_grants" in pos else pos

    out = {
        "gold_set": {
            "total_rows": len(df),
            "by_class": df.gold_class.value_counts().to_dict(),
            "scored_positives": n_pos,
            "scored_negatives": len(neg),
        },
        "recall_funder_level": {
            "positives": n_pos,
            "found_in_openalex": int(in_oa),
            "openalex_coverage_pct": pct(in_oa, n_pos),
            "aha_funder_linked": int(linked),
            "recall_vs_all_positives_pct": pct(linked, n_pos),
            "recall_vs_in_openalex_pct": pct(linked, in_oa),
            "misses_in_oa_not_linked": len(miss_linked),
            "misses_not_in_oa": len(not_in_oa),
        },
        "false_positives": {
            "gold_negatives": len(neg),
            "openalex_links_to_aha": len(fp),
            "false_positive_rate_pct": pct(len(fp), len(neg)),
        },
        "grant_level": {
            "note": "Public OpenAlex `funders` field has no award IDs; `grants` "
                    "(crossref) carries virtually no AHA awards. Grant-level linkage "
                    "is effectively absent — the core motivation for PMC ingestion.",
            "positives_citing_an_aha_grant": int((pos.n_aha_grants.astype(int) > 0).sum()),
        },
    }
    (C.TABLES / "23_precision_recall.json").write_text(json.dumps(out, indent=2))

    # de-identified misses for diagnosis (positives OA has, but not AHA-linked)
    cols = ["doi", "month", "status", "gold_class", "n_aha_grants",
            "aha_grant_ids", "pmcid", "oa_id", "oa_n_funders", "oa_is_oa"]
    miss_linked[[c for c in cols if c in miss_linked]].to_csv(
        C.DATA / "misses.csv", index=False, encoding="utf-8")

    print(json.dumps(out, indent=2))
    print(f"\nwrote tables/23_precision_recall.json + data/misses.csv "
          f"({len(miss_linked)} funder-level misses)")


if __name__ == "__main__":
    main()
