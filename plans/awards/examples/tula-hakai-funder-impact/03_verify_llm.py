"""03 - Full LLM verification pass over candidate works.

Classifies every candidate into:
  label : include | exclude          (exclude = false positive / not really Tula-Hakai)
  scope : coastal | other_tula | na  (coastal = marine/coastal science program;
                                      other_tula = genuine Tula but non-coastal,
                                      e.g. TulaSalud/Guatemala health; na if excluded)
  confidence : high | medium | low
  reason : one short line

Two-tier for cost: cheap model first pass, strong model re-checks every `exclude`
and every low-confidence verdict.

Run modes
---------
  py 03_verify_llm.py --build      # build data/verify_inputs.jsonl (no API calls)
  py 03_verify_llm.py --run        # call Anthropic API (needs ANTHROPIC_API_KEY)
The key is read from env or from a local .env in this folder (KEY=VALUE lines).

Output: data/verification.csv
"""
import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config

FAST_MODEL = "claude-haiku-4-5"
STRONG_MODEL = "claude-opus-4-8"
BATCH = 25
ABSTRACT_CHARS = 700

RUBRIC = """You verify whether each research output was genuinely supported by, \
produced at, or affiliated with the Tula Foundation or its Hakai Institute \
(a British Columbia philanthropic funder of MARINE & COASTAL science; founders \
Eric Peterson & Christina Munck; field stations on Calvert Island and Quadra \
Island; historically called the Hakai Network / Hakai Beach Institute). Tula also \
runs unrelated programs, notably TulaSalud (maternal/child health in Guatemala).

For each record decide:
- label: "include" if there is credible evidence of a real Tula/Hakai tie \
(funder acknowledgement, Hakai/Tula author affiliation, work done at a Hakai \
field station, Hakai-published dataset, or clear methods/text mention). \
"exclude" if it is a false positive: e.g. "Tula" actually refers to a Russian \
university (Tula State University, etc.), "Calvert Island" or "Hakai" appears \
coincidentally with no real institute tie, or the work predates any plausible \
Tula/Hakai involvement with no connection.
- scope (only if include): "coastal" for marine/coastal/nearshore/ocean/ \
freshwater/coastal-archaeology/coastal-earth-science/coastal-ecology research \
tied to the Hakai program; "other_tula" for genuine Tula work that is NOT \
coastal (e.g. TulaSalud health, Guatemala maternal health, unrelated domains). \
Use "na" if label is exclude.
- confidence: high | medium | low
- reason: one short clause.

Return ONLY a JSON array, one object per record, in the same order:
[{"work_id":"W..","label":"include","scope":"coastal","confidence":"high","reason":".."}]"""


def compact(row: pd.Series) -> dict:
    ab = (row.get("abstract") or "")[:ABSTRACT_CHARS]
    insts = (row.get("institution_names") or "")[:400]
    raws = (row.get("raw_affiliations") or "")[:400]
    return {
        "work_id": row["work_id"],
        "title": row.get("title") or "",
        "year": None if pd.isna(row.get("year")) else int(row["year"]),
        "type": row.get("type") or "",
        "venue": row.get("venue") or "",
        "primary_field": row.get("primary_field") or "",
        "primary_topic": row.get("primary_topic") or "",
        "provenance": row.get("provenance_list") or "",
        "institutions": insts,
        "raw_affiliations": raws,
        "countries": row.get("countries") or "",
        "abstract": ab,
    }


def build_inputs() -> Path:
    works = pd.read_parquet(config.DATA / "works.parquet")
    recs = [compact(r) for _, r in works.iterrows()]
    out = config.DATA / "verify_inputs.jsonl"
    with out.open("w", encoding="utf-8") as fh:
        for r in recs:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"Built {len(recs)} verification inputs -> {out}")
    print(f"Batches of {BATCH}: {-(-len(recs)//BATCH)} API calls per tier.")
    return out


def load_key() -> str | None:
    if os.environ.get("ANTHROPIC_API_KEY"):
        return os.environ["ANTHROPIC_API_KEY"]
    envf = config.ROOT / ".env"
    if envf.exists():
        for line in envf.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("ANTHROPIC_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def classify(client, model: str, batch: list[dict]) -> list[dict]:
    payload = "Records:\n" + "\n".join(json.dumps(b, ensure_ascii=False) for b in batch)
    msg = client.messages.create(
        model=model, max_tokens=4000,
        system=RUBRIC,
        messages=[{"role": "user", "content": payload}],
    )
    text = msg.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("```")[1].lstrip("json").strip()
    return json.loads(text)


def run() -> None:
    key = load_key()
    if not key:
        sys.exit("No ANTHROPIC_API_KEY found (env or ./.env). "
                 "Run with --build to prepare inputs, or set the key.")
    import anthropic
    client = anthropic.Anthropic(api_key=key)

    recs = [json.loads(l) for l in
            (config.DATA / "verify_inputs.jsonl").read_text(encoding="utf-8").splitlines()]
    verdicts: dict[str, dict] = {}

    # tier 1
    for i in range(0, len(recs), BATCH):
        batch = recs[i:i + BATCH]
        for v in classify(client, FAST_MODEL, batch):
            verdicts[v["work_id"]] = {**v, "tier": FAST_MODEL}
        print(f"  tier1 {min(i+BATCH,len(recs))}/{len(recs)}")

    # tier 2 re-check
    by_id = {r["work_id"]: r for r in recs}
    recheck = [by_id[w] for w, v in verdicts.items()
               if v.get("label") == "exclude" or v.get("confidence") == "low"]
    print(f"Re-checking {len(recheck)} with {STRONG_MODEL}")
    for i in range(0, len(recheck), BATCH):
        batch = recheck[i:i + BATCH]
        for v in classify(client, STRONG_MODEL, batch):
            verdicts[v["work_id"]] = {**v, "tier": STRONG_MODEL}

    df = pd.DataFrame(list(verdicts.values()))
    out = config.DATA / "verification.csv"
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"\nWrote {len(df)} verdicts -> {out}")
    print(df["label"].value_counts().to_string())
    print(df[df.label == "include"]["scope"].value_counts().to_string())


CHUNK = 70


def split() -> None:
    """Split verify_inputs.jsonl into chunk files for in-session subagents."""
    recs = (config.DATA / "verify_inputs.jsonl").read_text(encoding="utf-8").splitlines()
    cdir = config.DATA / "verify_chunks"
    cdir.mkdir(exist_ok=True)
    odir = config.DATA / "verify_out"
    odir.mkdir(exist_ok=True)
    n = 0
    for i in range(0, len(recs), CHUNK):
        n += 1
        (cdir / f"chunk_{n:02d}.jsonl").write_text(
            "\n".join(recs[i:i + CHUNK]) + "\n", encoding="utf-8")
    print(f"Wrote {n} chunks of <= {CHUNK} to {cdir}")
    print(f"Subagent verdicts expected in {odir}/chunk_NN.json")


def merge() -> None:
    """Merge subagent verdict files into data/verification.csv."""
    odir = config.DATA / "verify_out"
    verdicts: dict[str, dict] = {}
    files = sorted(odir.glob("chunk_*.json"))
    for f in files:
        for v in json.loads(f.read_text(encoding="utf-8")):
            verdicts[v["work_id"]] = v
    df = pd.DataFrame(list(verdicts.values()))
    works = pd.read_parquet(config.DATA / "works.parquet")
    missing = set(works["work_id"]) - set(df["work_id"])
    out = config.DATA / "verification.csv"
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"Merged {len(files)} files -> {len(df)} verdicts -> {out}")
    if missing:
        print(f"WARNING: {len(missing)} works unverified: {list(missing)[:10]}")
    print(df["label"].value_counts().to_string())
    print(df[df.label == "include"]["scope"].value_counts().to_string())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--split", action="store_true")
    ap.add_argument("--merge", action="store_true")
    a = ap.parse_args()
    if a.build:
        build_inputs()
    elif a.run:
        run()
    elif a.split:
        split()
    elif a.merge:
        merge()
    else:
        build_inputs()
        print("\nNow run with --run once ANTHROPIC_API_KEY is available.")
