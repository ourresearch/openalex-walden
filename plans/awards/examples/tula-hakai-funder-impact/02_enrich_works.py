"""02 - Fetch full work objects for every candidate ID.

Batches IDs through the OpenAlex `ids.openalex` filter, reconstructs abstracts,
flattens the fields the analyses need, and caches to data/works.parquet.

Output: data/works.parquet  (+ data/works_sample.json for eyeballing)
"""
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config
from lib import openalex as oa

SELECT = ",".join([
    "id", "doi", "display_name", "publication_year", "publication_date",
    "type", "language", "primary_location", "open_access",
    "authorships", "topics", "primary_topic",
    "fwci", "citation_normalized_percentile", "cited_by_count",
    "referenced_works_count", "abstract_inverted_index",
])
BATCH = 50


def flatten(rec: dict) -> dict:
    wid = oa.short_id(rec["id"])
    loc = rec.get("primary_location") or {}
    src = loc.get("source") or {}
    oa_info = rec.get("open_access") or {}
    pt = rec.get("primary_topic") or {}
    cnp = rec.get("citation_normalized_percentile") or {}

    authorships = rec.get("authorships") or []
    inst_ids, inst_names, countries, raw_affs, author_ids, author_names = (
        set(), set(), set(), [], [], [])
    for a in authorships:
        au = a.get("author") or {}
        if au.get("id"):
            author_ids.append(oa.short_id(au["id"]))
        if au.get("display_name"):
            author_names.append(au["display_name"])
        for inst in a.get("institutions") or []:
            if inst.get("id"):
                inst_ids.add(oa.short_id(inst["id"]))
            if inst.get("display_name"):
                inst_names.add(inst["display_name"])
        for c in a.get("countries") or []:
            countries.add(c)
        for ra in a.get("raw_affiliation_strings") or []:
            raw_affs.append(ra)

    grants = rec.get("grants") or []
    grant_funders = [g.get("funder") and oa.short_id(g["funder"]) for g in grants]
    grant_funders = [g for g in grant_funders if g]

    topics = rec.get("topics") or []

    return {
        "work_id": wid,
        "doi": (rec.get("doi") or "").replace("https://doi.org/", ""),
        "title": rec.get("display_name") or "",
        "abstract": oa.reconstruct_abstract(rec.get("abstract_inverted_index")),
        "year": rec.get("publication_year"),
        "date": rec.get("publication_date"),
        "type": rec.get("type") or "",
        "language": rec.get("language") or "",
        "venue": src.get("display_name") or "",
        "venue_type": src.get("type") or "",
        "is_oa": bool(oa_info.get("is_oa")),
        "oa_status": oa_info.get("oa_status") or "",
        "cited_by_count": rec.get("cited_by_count") or 0,
        "fwci": rec.get("fwci"),
        "cnp_value": cnp.get("value"),
        "is_top_1pct": bool(cnp.get("is_in_top_1_percent")),
        "is_top_10pct": bool(cnp.get("is_in_top_10_percent")),
        "referenced_works_count": rec.get("referenced_works_count") or 0,
        "primary_topic": pt.get("display_name") or "",
        "primary_field": ((pt.get("field") or {}).get("display_name")) or "",
        "primary_domain": ((pt.get("domain") or {}).get("display_name")) or "",
        "topics_json": json.dumps([
            {"id": oa.short_id(t["id"]), "name": t.get("display_name"),
             "field": (t.get("field") or {}).get("display_name")}
            for t in topics]),
        "n_authors": len(authorships),
        "author_ids": "|".join(author_ids),
        "author_names": "|".join(author_names),
        "institution_ids": "|".join(sorted(inst_ids)),
        "institution_names": "|".join(sorted(inst_names)),
        "countries": "|".join(sorted(countries)),
        "raw_affiliations": " || ".join(raw_affs[:30]),
        "grant_funders": "|".join(grant_funders),
    }


def main() -> None:
    cand = pd.read_csv(config.DATA / "candidates.csv")
    ids = cand["work_id"].tolist()
    print(f"Enriching {len(ids)} works in batches of {BATCH}...")

    rows = []
    for i in range(0, len(ids), BATCH):
        chunk = ids[i:i + BATCH]
        filter_expr = "ids.openalex:" + "|".join(chunk)
        for rec in oa.paginate("works", filter_expr, select=SELECT, per_page=BATCH):
            rows.append(flatten(rec))
        print(f"  {min(i + BATCH, len(ids))}/{len(ids)}  (rows: {len(rows)})")

    works = pd.DataFrame(rows)
    # carry provenance flags over from candidates
    prov_cols = config.PROV_FLAGS + ["prov_dataset", "provenance_list",
                                     "n_strategies", "has_strong_prov"]
    works = works.merge(cand[["work_id"] + prov_cols], on="work_id", how="left")

    out = config.DATA / "works.parquet"
    works.to_parquet(out, index=False)
    works.head(20).to_json(config.DATA / "works_sample.json",
                           orient="records", indent=2, force_ascii=False)

    missing = set(ids) - set(works["work_id"])
    print(f"\nWrote {len(works)} enriched works -> {out}")
    if missing:
        print(f"WARNING: {len(missing)} candidate IDs returned no record: "
              f"{list(missing)[:10]}")
    print(f"  abstracts present: {(works['abstract'].str.len() > 0).sum()}")
    print(f"  fwci present:      {works['fwci'].notna().sum()}")
    print(f"  has DOI:           {(works['doi'].str.len() > 0).sum()}")


if __name__ == "__main__":
    main()
