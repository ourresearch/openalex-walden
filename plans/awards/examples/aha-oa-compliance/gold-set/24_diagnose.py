"""24 - Diagnose the misses and false positives -> pipeline-improvement signal.

Two questions:
  A. Why does OpenAlex miss 180 verified-AHA works (in OA, not AHA-linked)?
     Key lever: do they cite an AHA grant ID? If so, PMC/PubMed grant ingestion
     (Phase 2) would recover them — OpenAlex's acknowledgement text-mining missed
     them but the grant linkage exists in PMC.
  B. The 'false positives' (gold negatives OA links to AHA) split into:
       - DEFINITIONAL: AHA acknowledged / disclosed but not a funder. OpenAlex's
         funder = anything in acknowledgements, so these are correct *by OA's
         definition* but 'wrong' by AHA's stricter funded-grant definition.
       - REAL ERROR: name-match ('initials AHA'), erroneous links, wrong doc types.

Writes tables/24_diagnosis.json.
Run:  PYTHONIOENCODING=utf-8 py -3 24_diagnose.py
"""
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C

POS = {"positive", "positive_no_grant"}
# how each negative note-category should be read when OA links it to AHA
DEFINITIONAL = {"not_funded_disclosure", "aha_authored"}
REAL_ERROR = {"remove_erroneous", "doc_type_excluded"}


def main() -> None:
    gold = pd.read_csv(C.DATA / "goldset.csv", dtype=str, keep_default_na=False)
    m = pd.read_csv(C.DATA / "matched.csv", keep_default_na=False)
    for c in ("found_oa", "oa_aha_funder"):
        m[c] = m[c].astype(str).str.lower().eq("true")
    df = gold.merge(m, on="doi", how="left").drop_duplicates("doi")
    df["found_oa"] = df["found_oa"].fillna(False)
    df["oa_aha_funder"] = df["oa_aha_funder"].fillna(False)
    df["n_aha_grants"] = df["n_aha_grants"].astype(int)
    df["has_pmcid"] = df["pmcid"].str.strip().ne("")

    # ---- A. funder-level misses ----
    pos = df[df.gold_class.isin(POS)]
    miss = pos[pos.found_oa & ~pos.oa_aha_funder]
    A = {
        "total_misses": len(miss),
        "cite_an_aha_grant_id": int((miss.n_aha_grants > 0).sum()),
        "recoverable_via_pmc_grant_link_pct": round(
            100 * (miss.n_aha_grants > 0).mean(), 1),
        "have_pmcid": int(miss.has_pmcid.sum()),
        "by_month": miss.month.value_counts().to_dict(),
    }
    # for comparison: among HITS, how often is there a grant id too
    hit = pos[pos.oa_aha_funder]
    A["context_hits_cite_aha_grant_pct"] = round(100 * (hit.n_aha_grants > 0).mean(), 1)

    # ---- B. false positives by category ----
    neg = df[df.gold_class == "negative"].copy()
    fp = neg[neg.oa_aha_funder]
    cat = fp.na_note_category.value_counts().to_dict()
    definitional = int(fp.na_note_category.isin(DEFINITIONAL).sum())
    real_error = int(fp.na_note_category.isin(REAL_ERROR).sum())
    B = {
        "total_false_positives": len(fp),
        "by_note_category": cat,
        "definitional_ack_or_authored": definitional,
        "real_error_namematch_or_doctype": real_error,
        "other": len(fp) - definitional - real_error,
        "interpretation": "Definitional FPs are consistent with OpenAlex's funder = "
                          "'anything in acknowledgements' definition; real errors are the "
                          "actionable subset.",
    }

    out = {"A_funder_misses": A, "B_false_positives": B}
    (C.TABLES / "24_diagnosis.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
