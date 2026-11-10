"""Pack assembly for the work-type nightly (prodpath piece 2): SQL rows -> devproof pack -> s2r3 text -> input_hash.

The row formats are exactly what the pack-input statements return (sql/02_pack_*.sql; the same statements TEST-3 and
TEST-4 were built with, student/out/v3/code/c_packs.py + b_pull.py): works rows, records rows (one JSON column `j`),
tag rows (k, ns, page_title, tx_meta_json), B1/B2 rows (first/last page, reference count) and B3 rows (MEDLINE types,
DataCite type). Pure functions, no I/O, so the notebook can run them per partition (applyInPandas grouped by work_id)
for the backfill or on the driver for the nightly. Today's type is never put into a pack (blinding: evidence_packs.gate).
"""
from __future__ import annotations

import json

import evidence_packs as bp
import stage2_text as rv

NS_OF = {"crossref": "doi", "datacite": "doi", "pubmed": "pmid", "repo": "pmh", "mag": "mag"}


def _txt(x):
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def tag_map(tag_rows) -> dict:
    out = {}
    for r in tag_rows:
        tm = r.get("tx_meta_json")
        tm = json.loads(tm) if isinstance(tm, str) and tm else None
        pt = r.get("page_title")
        out[(r["k"], r["ns"])] = {"page_title": pt if isinstance(pt, str) else None, "tx_meta": tm}
    return out


def records_by_work(record_rows, tags: dict) -> dict[int, list[dict]]:
    recs: dict[int, list[dict]] = {}
    for row in record_rows:
        j = json.loads(row["j"]) if isinstance(row["j"], str) else row["j"]
        r = j.get("r")
        if r is None:
            raise ValueError(f"record {j.get('provenance')}:{j.get('native_id')} came back without fields")
        prov, nid = j["provenance"], j["native_id"]
        t = tags.get((nid.lower() if prov in ("crossref", "datacite") else nid, NS_OF.get(prov)))
        recs.setdefault(int(j["work_id"]), []).append({
            "pos": j["pos"], "provenance": prov, "native_id": nid, "title": r.get("title"),
            "raw_type": None if prov == "pubmed" else r.get("raw_type"), "cr_subtype": j.get("cr_subtype"),
            "src_type": j.get("src_type"), "source_name": r.get("source_name"), "publisher": r.get("publisher"),
            "version": r.get("version"), "published_date": r.get("published_date"), "issue": r.get("issue"),
            "volume": r.get("volume"), "first_page": r.get("first_page"), "last_page": r.get("last_page"),
            "n_references": r.get("n_references"), "landing_page_url": r.get("landing_page_url"), "ids": r.get("ids"),
            "tx_page_title": t.get("page_title") if t else None, "tx_meta": t.get("tx_meta") if t else None})
    return recs


def work_fields(w: dict) -> dict:
    out = {k: (None if not isinstance(w.get(k), (list, dict, str)) and bp.missing(w.get(k)) else w.get(k))
           for k in bp.WORK_COLS if k not in ("split", "fulltext_head")}
    out["fulltext_head"] = None                        # no excerpt in any pack (hub 11-02 / 11-03)
    return out


def packs(works_rows, record_rows, tag_rows) -> list[dict]:
    recs = records_by_work(record_rows, tag_map(tag_rows))
    return [bp.shape(work_fields(w), recs.get(int(w["work_id"]), []))
            for w in sorted(works_rows, key=lambda w: int(w["work_id"]))]


def bmap(b12_rows, b3_rows) -> dict:
    """render_v3.load_b's map, from rows instead of files: {(provenance, native_id): fields}."""
    out: dict = {}
    for r in b12_rows:
        d = out.setdefault(rv.key(r["provenance"], r["native_id"]), {})
        d.update({"first_page": r.get("first_page"), "last_page": r.get("last_page"), "n_references": r.get("n_references")})
    for r in b3_rows:
        d = out.setdefault(rv.key(r["provenance"], r["native_id"]), {})
        mt = r.get("medline_types")
        if mt:
            try:
                v = json.loads(mt)
            except (TypeError, ValueError):
                v = [mt]
            d["medline_types"] = [str(x).strip() for x in (v if isinstance(v, list) else [v]) if str(x).strip()]
        for k in ("dc_type_general", "dc_resource_type"):
            if _txt(r.get(k)):
                d[k] = _txt(r.get(k))
    return out


class GateError(ValueError):
    pass


def gate_one(p: dict) -> None:
    """build_packs.gate()'s checks on one in-memory pack (the blinding gate): whitelisted keys only, PubMed raw types
    null, tag lists of strings. A pack can't carry today's type, a rule or a label: those keys aren't whitelisted."""
    w = p.get("work_id")
    if list(p) != ["work_id", "pack_version", "signals"]:
        raise GateError(f"W{w}: top-level keys {list(p)}")
    s = p["signals"]
    if list(s) != bp.SIGNAL_KEYS:
        raise GateError(f"W{w}: signal keys {list(s)}")
    if s["biblio"] is not None and list(s["biblio"]) != bp.BIBLIO_KEYS:
        raise GateError(f"W{w}: biblio keys")
    for r in s["records"]:
        if list(r) != bp.RECORD_KEYS:
            raise GateError(f"W{w}: record keys {list(r)}")
        if r["provenance"] == "pubmed" and r["raw_type"] is not None:
            raise GateError(f"W{w}: PubMed publication type leaked")
        for k in ("page_tags", "relations"):
            if r[k] is not None and not all(isinstance(x, str) for x in r[k]):
                raise GateError(f"W{w}: {k} not strings")


def bmap_from_records(record_rows, b3_rows) -> dict:
    """The nightly's bmap: B1/B2 are the records statement's own pages and reference counts (the same max_by row as
    b_pull.py's LWT statement), B3 comes from its own rows."""
    b12 = []
    for row in record_rows:
        j = json.loads(row["j"]) if isinstance(row["j"], str) else row["j"]
        r = j.get("r") or {}
        b12.append({"provenance": j["provenance"], "native_id": j["native_id"], "first_page": r.get("first_page"),
                    "last_page": r.get("last_page"), "n_references": r.get("n_references")})
    return bmap(b12, b3_rows)


def stage0_signals(p: dict) -> dict:
    """What a stage-0 source list reads from a pack, exactly as student/prod/stage0.SourceList.key reads it: the DOI
    (stripped, lowercased), the names (work publisher + each record's source name and publisher; lowercased, whitespace
    collapsed, empties dropped) and the records' landing URLs (lowercased). Stored with the work's current pack so the
    decide SQL can match the policy's lists without re-reading the corpus, and a policy with new lists needs no re-pack."""
    s = p["signals"]
    recs = s.get("records") or []
    names = [s.get("publisher")] + [r.get("source_name") for r in recs] + [r.get("publisher") for r in recs]
    return {"s0_doi": (s.get("doi") or "").strip().lower(),
            "s0_names": [" ".join(str(n).lower().split()) for n in names if n],
            "s0_urls": [(r.get("landing_page_url") or "").lower() for r in recs]}


def assemble_row(work_j: str, recs, tags, b3) -> dict:
    """One wt_pack_rows row (sql/02_pack_rows.sql) -> {work_id, input_hash, render_version, pack_version, text, s0_doi,
    s0_names, s0_urls}.
    work_j: JSON of the work fields; recs: JSON records rows (`j`); tags: JSON tag rows; b3: JSON B3 rows."""
    w = json.loads(work_j)
    rr = [{"j": x} for x in (recs if recs is not None else [])]
    tr = [json.loads(x) for x in (tags if tags is not None else [])]
    br = [json.loads(x) for x in (b3 if b3 is not None else [])]
    p = packs([w], rr, tr)[0]
    gate_one(p)
    t = rv.render_v3(p, bmap_from_records(rr, br))
    return {"work_id": int(w["work_id"]), "input_hash": rv.text_hash_v3(t), "render_version": rv.RENDER_VERSION_V3,
            "pack_version": bp.PACK_VERSION, "text": t, **stage0_signals(p)}


def render(packs_: list[dict], bm: dict) -> list[dict]:
    """-> [{work_id, text, input_hash, render_version}] (stage 2's s2r3 text and its hash, exactly)."""
    out = []
    for p in packs_:
        t = rv.render_v3(p, bm)
        out.append({"work_id": int(p["work_id"]), "text": t, "input_hash": rv.text_hash_v3(t), "render_version": rv.RENDER_VERSION_V3})
    return out
