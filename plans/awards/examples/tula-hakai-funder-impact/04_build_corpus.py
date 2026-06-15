"""04 - Join LLM verdicts onto enriched works -> the final analysis corpus.

Output:
  data/corpus_final.csv   - ALL candidates with label/scope (audit trail)
  data/corpus_coastal.csv - the primary analysis corpus (label=include, scope=coastal)
  data/corpus_other.csv   - genuine-Tula-but-non-coastal (sidebar)
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config

# Manual post-verification corrections (documented, reproducible). Each entry is a
# work the LLM pass got wrong on hand review; (label, scope, reason).
OVERRIDES = {
    # 1985 oceanography paper with only a spurious Hakai *funder* tag; the Hakai
    # Institute did not exist until the late 2000s. Kyle flagged this one directly.
    "W1992442399": ("exclude", "na",
                    "OVERRIDE: 1985 Queen Charlotte Sound paper predates Hakai; funder mis-assignment"),
}


def main() -> None:
    works = pd.read_parquet(config.DATA / "works.parquet")
    v = pd.read_csv(config.DATA / "verification.csv")[
        ["work_id", "label", "scope", "confidence", "reason"]]
    df = works.merge(v, on="work_id", how="left")

    for wid, (lab, sc, why) in OVERRIDES.items():
        mask = df.work_id == wid
        if mask.any():
            df.loc[mask, ["label", "scope", "reason"]] = [lab, sc, why]
            df.loc[mask, "confidence"] = "override"

    # Re-scope pass: marine protist / microbial-eukaryote work that the first pass
    # had filed under "other_tula" is, per project scope, genuinely marine. A focused
    # LLM re-pass (data/rescope_out/*.json) promotes those records to coastal.
    import json as _json
    rdir = config.DATA / "rescope_out"
    if rdir.exists():
        promoted = 0
        verdicts = {}
        for f in sorted(rdir.glob("chunk_*.json")):
            for v in _json.loads(f.read_text(encoding="utf-8")):
                verdicts[v["work_id"]] = v["scope"]
        for wid, sc in verdicts.items():
            if sc == "coastal":
                m = (df.work_id == wid) & (df.label == "include")
                if m.any() and (df.loc[m, "scope"] == "other_tula").any():
                    df.loc[m, "scope"] = "coastal"
                    df.loc[m, "reason"] = "RE-SCOPED: marine protist/microbial work"
                    promoted += int(m.sum())
        print(f"re-scope: promoted {promoted} marine works other_tula -> coastal")

    # Per Kyle: keep ALL remaining genuine Tula/Hakai research output as part of the
    # in-scope corpus, EXCEPT clinical/behavioural fields. Only Medicine, Psychology,
    # and Neuroscience stay out (these are TulaSalud-adjacent / off-topic).
    EXCLUDE_FIELDS = {"Medicine", "Psychology", "Neuroscience", "Health Professions"}
    keep = ((df.label == "include") & (df.scope == "other_tula")
            & (~df.primary_field.isin(EXCLUDE_FIELDS)))
    df.loc[keep, "scope"] = "coastal"
    df.loc[keep, "reason"] = df.loc[keep, "reason"].astype(str) + " | KEPT: Tula research output (non-clinical)"
    print(f"kept {int(keep.sum())} non-clinical Tula works into the corpus; "
          f"excluded fields kept out: {sorted(EXCLUDE_FIELDS)}")

    # Specific off-topic works Kyle flagged to remove by hand.
    MANUAL_EXCLUDE = {
        "W2334449386",  # "A meeting report: 16th International HLA and Immunogenetics Workshop"
        # (the TulaSalud m-health paper W2145858499 is already out via Health Professions)
    }
    mx = df.work_id.isin(MANUAL_EXCLUDE) & (df.scope == "coastal")
    df.loc[mx, "scope"] = "other_tula"
    df.loc[mx, "reason"] = "MANUAL EXCLUDE: off-topic (Kyle)"
    print(f"manual excludes applied: {int(mx.sum())}")

    # any work the verifier somehow missed -> mark for review, keep out of corpora
    df["label"] = df["label"].fillna("review")
    df["scope"] = df["scope"].fillna("na")

    df.to_csv(config.DATA / "corpus_final.csv", index=False, encoding="utf-8")

    coastal = df[(df.label == "include") & (df.scope == "coastal")].copy()
    other = df[(df.label == "include") & (df.scope == "other_tula")].copy()
    coastal.to_csv(config.DATA / "corpus_coastal.csv", index=False, encoding="utf-8")
    other.to_csv(config.DATA / "corpus_other.csv", index=False, encoding="utf-8")

    print("=== corpus build ===")
    print(df["label"].value_counts().to_string())
    print("\ninclude by scope:")
    print(df[df.label == "include"]["scope"].value_counts().to_string())
    print(f"\nCOASTAL corpus: {len(coastal)} works, "
          f"years {int(coastal.year.min())}-{int(coastal.year.max())}")
    print(f"  total citations: {int(coastal.cited_by_count.sum()):,}")
    print(f"  with FWCI: {coastal.fwci.notna().sum()}, "
          f"top-1%: {int(coastal.is_top_1pct.sum())}, "
          f"top-10%: {int(coastal.is_top_10pct.sum())}")
    print(f"OTHER (sidebar) corpus: {len(other)} works")
    print("\nexcludes by confidence:")
    print(df[df.label == "exclude"]["confidence"].value_counts().to_string())


if __name__ == "__main__":
    main()
