#!/usr/bin/env python3
"""DOE OSTI facility-proposal awards -> parquet for the awards pipeline.

Source: OSTI's Crossref grant-DOI registrations (member 960, type:grant).
Award numbers are FACILITY-SCOPED — 703 of 17,067 numbers recur at more than
one facility as different grants — so the grain is (award, facility), with the
facility code taken from the first token of the DOI suffix
(brcr.proj.2020.51767 -> brcr; aps-188720 -> aps). funder_award_id keeps the
bare cited number so the fold's generic normalized key matches deposited ids;
the notebook salts the entity-id hash with the facility code (scheme).

OSTI publishes no amounts (§6.7 waiver). Award year exists only where the DOI
suffix carries a proj.YYYY token. PI given/family from the lead-investigator
block where registered.

Output: osti_facility_grants.parquet ->
  s3://openalex-ingest/awards/osti_facility/osti_facility_grants.parquet
"""
import re
import time

import pandas as pd
import requests

OUT = "osti_facility_grants.parquet"

rows, cursor, seen = [], "*", 0
S = requests.Session()
S.headers["User-Agent"] = "openalex-awards-ingest (mailto:rohan.mantena@gmail.com)"
while True:
    r = S.get("https://api.crossref.org/members/960/works",
              params={"filter": "type:grant", "rows": 1000, "cursor": cursor}, timeout=120)
    r.raise_for_status()
    msg = r.json()["message"]
    items = msg.get("items", [])
    if not items:
        break
    for it in items:
        doi = (it.get("DOI") or "").strip()
        proj = (it.get("project") or [{}])[0]
        li = proj.get("lead-investigator") or [{}]
        li = li[0] if li else {}
        suffix = doi.split("/", 1)[1] if "/" in doi else ""
        fac = re.split(r"[-./]", suffix)[0] if suffix else ""
        titles = proj.get("project-title") or []
        yr = re.search(r"proj\.((?:19|20)\d{2})\.", doi)
        rows.append({
            "funder_award_id": str(it.get("award", "")).strip(),
            "title": (titles[0].get("title", "") if titles else "").strip(),
            "description": None,
            "amount": None,
            "currency": None,
            "scheme": fac or None,
            "start_date_raw": yr.group(1) if yr else None,
            "end_date_raw": None,
            "pi_given": (li.get("given") or "").strip() or None,
            "pi_family": (li.get("family") or "").strip() or None,
            "institution": None,
            "landing_page_url": f"https://doi.org/{doi}",
            "doi": doi,
        })
    seen += len(items)
    print(f"{seen} records", flush=True)
    cursor = msg.get("next-cursor")
    if not cursor:
        break
    time.sleep(1)

df = pd.DataFrame(rows).astype(object)
df = df[df["funder_award_id"].str.len() > 0]
assert (df["title"].str.len() > 0).all(), "blank titles"
assert not df.duplicated(["funder_award_id", "scheme"]).any(), "(award, facility) grain violated"
assert len(df) > 15000, f"shrink guard: only {len(df)} rows (had 17,770 on 2026-08-05)"
df.to_parquet(OUT, index=False)
print(f"wrote {OUT}: {len(df)} rows, {df['funder_award_id'].nunique()} distinct awards, "
      f"{df['scheme'].nunique()} facilities")
