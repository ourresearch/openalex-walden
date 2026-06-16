"""22 - Match the gold set against OpenAlex by DOI.

For every gold DOI: is it in OpenAlex? Does OpenAlex link it to AHA as a funder
(grants[].funder == AHA)? Which AHA award IDs does OpenAlex carry for it?

Batches DOIs through the works endpoint (OR filter), caches the raw responses.

Writes data/matched.csv  (de-identified — safe to check in).

Run:  PYTHONIOENCODING=utf-8 py -3 22_match_openalex.py
"""
import hashlib
import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C

UA = {"User-Agent": "openalex-funder-examples/aha (mailto:dashboard@ourresearch.org)"}
AHA_URL = f"https://openalex.org/{C.AHA_FUNDER_ID}"
BATCH = 50
# Note: the public work object exposes funding only as `funders` [{id,display_name,ror}]
# — there are NO award IDs here (and `grants` is not select-able). Grant-level award
# matching can't be done from the API; see 23_precision_recall.py for that finding.
SELECT = "id,doi,ids,open_access,funders"


def _params(extra):
    p = {"per_page": BATCH, "select": SELECT,
         "mailto": "dashboard@ourresearch.org", **extra}
    if C.API_KEY and C.API_KEY != "XXX":
        p["api_key"] = C.API_KEY
    return p


def fetch_batch(dois):
    """Return {normalized_doi: work_obj} for a batch, via the OR filter."""
    filt = "doi:" + "|".join(dois)
    cache = C.CACHE / f"b_{hashlib.md5(filt.encode()).hexdigest()}.json"
    if cache.exists():
        results = json.loads(cache.read_text())
    else:
        results = []
        for _ in range(3):
            try:
                r = requests.get(f"{C.API_BASE}/works", headers=UA,
                                 params=_params({"filter": filt}), timeout=60)
                if r.ok:
                    results = r.json().get("results", [])
                    break
            except requests.RequestException:
                pass
            time.sleep(2)
        cache.write_text(json.dumps(results))
    out = {}
    for w in results:
        d = (w.get("doi") or "").lower().replace("https://doi.org/", "")
        if d:
            out[d] = w
    return out


def main() -> None:
    gold = pd.read_csv(C.DATA / "goldset.csv", dtype=str).fillna("")
    dois = [d for d in gold["doi"].unique() if d]
    print(f"matching {len(dois)} unique DOIs in batches of {BATCH} ...")

    found = {}
    for i in range(0, len(dois), BATCH):
        found.update(fetch_batch(dois[i:i + BATCH]))
        if (i // BATCH) % 10 == 0:
            print(f"  {i+BATCH}/{len(dois)}  (cumulative found: {len(found)})")

    recs = []
    for d in dois:
        w = found.get(d)
        if not w:
            recs.append({"doi": d, "found_oa": False, "oa_id": "",
                         "oa_aha_funder": False, "oa_n_funders": 0,
                         "oa_is_oa": "", "oa_oa_status": "", "oa_has_pmcid": ""})
            continue
        funders = w.get("funders") or []
        funder_ids = {f.get("id") for f in funders if f.get("id")}
        oa = w.get("open_access") or {}
        recs.append({
            "doi": d,
            "found_oa": True,
            "oa_id": w.get("id", ""),
            "oa_aha_funder": AHA_URL in funder_ids,
            "oa_n_funders": len(funder_ids),
            "oa_is_oa": oa.get("is_oa", ""),
            "oa_oa_status": oa.get("oa_status", ""),
            "oa_has_pmcid": bool((w.get("ids") or {}).get("pmcid")),
        })

    out = pd.DataFrame(recs)
    out.to_csv(C.DATA / "matched.csv", index=False, encoding="utf-8")
    print(f"\nfound in OpenAlex: {out.found_oa.sum()}/{len(out)} "
          f"({out.found_oa.mean()*100:.1f}%)")
    print(f"AHA-funder-linked in OpenAlex: {out.oa_aha_funder.sum()}")
    print("wrote data/matched.csv")


if __name__ == "__main__":
    main()
