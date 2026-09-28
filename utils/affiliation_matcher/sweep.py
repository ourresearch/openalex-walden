"""New-card sweep (oxjob #1393): when OpenAlex mints institutions from a new ROR dump, re-match the existing strings
that could name them, so old works (and new works reusing an old string) get them too.

1. Candidates (`search`): the matcher's lexical test in reverse on ES raw-affiliation-strings-v3. For each name variant
   of a new institution (retrieve.Index token tuples), a string qualifies when the IDF weight of the variant's tokens
   it contains is >= max(5, th * total) (lex2 needs >= 0.5 per segment and >= 5). Tokens heavier than the slack are
   required (exact, and ES starts from the rare token). Each variant walks a ladder of thresholds until it returns
   <= `broad` strings; the last rung adds the card's city | region | country; a variant still over `cap` is dropped
   (a generic name: the nightly's live cards remain its path). Plus exact phrase search of every name (all scripts).
2. Match: AffiliationMatcherNightly with queue_table / target_table / sweep_ids_table (skips strings no swept
   institution reaches in lex2 / dense).
3. Apply (`APPLY_SQL`): keep a new answer only if it names a swept institution and differs from the current one;
   log it, MERGE it into the answers table.

Measured on ROR's 4,130 records created 1-21 Sep 2026 simulated as new (oxjobs #1393 EXPLORE § 8): exact phrase search
alone finds 80.9% of the strings the corpus run gave them; this ladder (broad 2,000, cap 50,000) 95.3% of strings and
95.5% of works; broad 5,000 would add 0.9 pt of works for +35% candidates.
"""
import collections
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from .retrieve import ABBR, CJK

INDEX = "raw-affiliation-strings-v3"
INV = collections.defaultdict(set)
for _k, _v in ABBR.items():
    if re.fullmatch(r"[a-z]+", _k):
        INV[_v].add(_k)


def _clause(w):
    if CJK.search(w):
        return {"match_phrase": {"raw_affiliation_string": w}}
    return {"terms": {"raw_affiliation_string": sorted({w, w + "s"} | INV.get(w, set()))}}


def body(tt, idf, th, size, after=None, loc=None):
    tot = sum(idf.get(w, 0) for w in tt)
    ms = max(5.0, th * tot)
    slack = max(0.0, tot - ms)
    flt = [{"range": {"works_count": {"gt": 0}}}] + [_clause(w) for w in tt if idf.get(w, 0) > slack + 1e-9]
    if loc:
        flt.append({"bool": {"should": [{"match_phrase": {"raw_affiliation_string": t}} for t in loc],
                             "minimum_should_match": 1}})
    b = {"size": size, "track_total_hits": True, "min_score": ms - 1e-6,
         "_source": ["raw_affiliation_string"] if size else False,
         "query": {"bool": {"should": [{"constant_score": {"filter": _clause(w), "boost": round(idf[w], 4)}}
                                       for w in tt if idf.get(w, 0) > 0], "filter": flt}}}
    if size:
        b["sort"] = [{"works_count": "desc"}, {"raw_affiliation_string.keyword": "asc"}]
        if after:
            b["search_after"] = after
    return b


def phrase_body(text, size, after=None):
    b = {"size": size, "track_total_hits": True, "_source": ["raw_affiliation_string"] if size else False,
         "query": {"bool": {"must": [{"match_phrase": {"raw_affiliation_string": text}}],
                            "filter": [{"range": {"works_count": {"gt": 0}}}]}}}
    if size:
        b["sort"] = [{"works_count": "desc"}, {"raw_affiliation_string.keyword": "asc"}]
        if after:
            b["search_after"] = after
    return b


def phrase_names(ix, iid):
    """Every name of the card as written (all scripts; Latin >= 6 chars, others >= 3), country suffix stripped."""
    d = ix.inst[iid]
    out = []
    for n in [d["name"]] + d["alts"]:
        n = " ".join((re.sub(r"\s*\([^()]*\)\s*$", "", n).strip() or n).split())
        latin = all(ord(c) < 0x250 or not c.isalpha() for c in n)
        if any(c.isalpha() for c in n) and len(n) >= (6 if latin else 3):
            out.append(n)
    return list(dict.fromkeys(out))


def search(ix, ids, es_url, ladder=(0.7, 0.85, 0.999), broad=2000, cap=50000, page=10000, threads=8, log=print):
    """Candidate strings for institutions `ids` (must have cards in ix). Returns ({id: set(strings)}, stats)."""
    url = es_url.rstrip("/") + f"/{INDEX}/_search"
    S = requests.Session()

    def es(b):
        for a in range(6):
            try:
                r = S.post(url, json=b, timeout=300)
                if r.status_code == 200:
                    return r.json()
            except requests.RequestException:
                pass
            time.sleep(2 ** a)
        raise RuntimeError(f"ES search failed: {json.dumps(b)[:300]}")

    def fetch(make):
        got, after = set(), None
        while True:
            hs = es(make(after))["hits"]["hits"]
            got.update(h["_source"]["raw_affiliation_string"] for h in hs)
            if len(hs) < page:
                return got
            after = hs[-1]["sort"]

    V = collections.defaultdict(list)
    for iid, tt in ix.variants:
        if iid in ids and sum(ix.idf.get(w, 0) for w in tt) >= 5.0:
            V[iid].append(tt)

    def loc_of(iid):
        d = ix.inst[iid]
        return [x for x in (d["city"], d["region"], d["country"]) if x and len(x) >= 3]

    def one(iid):
        st = collections.Counter()
        got = set()
        for tt in dict.fromkeys(V.get(iid, [])):
            rungs = [(th, None) for th in ladder] + ([(ladder[-1], loc_of(iid))] if loc_of(iid) else [])
            chosen = None
            for th, loc in rungs:
                n = es(body(tt, ix.idf, th, 0, loc=loc))["hits"]["total"]["value"]
                if n <= broad:
                    chosen = (th, loc, n)
                    break
            if chosen is None and n <= cap:
                chosen = (th, loc, n)
            if chosen is None:
                st["variants_dropped"] += 1
                continue
            th, loc, n = chosen
            st[f"rung_{th}{'_loc' if loc else ''}"] += 1
            if n:
                got |= fetch(lambda after: body(tt, ix.idf, th, page, after, loc))
        for name in phrase_names(ix, iid):
            n = es(phrase_body(name, 0))["hits"]["total"]["value"]
            if n <= cap:
                got |= fetch(lambda after: phrase_body(name, page, after))
            else:
                st["phrases_dropped"] += 1
        return iid, got, st

    out, stats = {}, collections.Counter()
    t0 = time.time()
    with ThreadPoolExecutor(threads) as ex:
        for k, (iid, got, st) in enumerate(ex.map(one, sorted(ids))):
            out[iid] = got
            stats.update(st)
            if (k + 1) % 500 == 0:
                log(f"  sweep search: {k + 1:,}/{len(ids):,} institutions, {time.time() - t0:.0f}s")
    stats["strings"] = len(set().union(*out.values())) if out else 0
    return out, dict(stats)
