"""The new affiliation matcher on one night's new strings (oxjob #1386).

Same generators, pool, Jev question and chooser as the benchmark (oxjobs #1363 `work/`: rstudy_gen.py,
modal_dense.py, decider_pilot.py, decider.py), so a string gets the answer the scoreboard scored:

    retrieve:  lex2 (lexical + acronyms) top 10 ∪ ES neighbour top 5 ∪ dense-chunks top 10 ∪ stored TF top 5
    decide:    the no-Jev chooser first (no model calls); then Jev names_this_institution per (string, candidate)
               for the strings it is unsure about, most unsure first, until a deadline
    choose:    frozen chooser (GBT over 15 features) keeps ids with p >= t

This is #1363's hybrid (EXPLORE § 15, `hybrid.py`): a string is unsure when some candidate's no-Jev p is in
(b, 1 - b). Test v1 high-confidence, exact set: no Jev 82.4%; b = 0.05 sends 62.9% of strings to Jev and gets 89.9%;
b = 0.10, 49.8% -> 89.6%; Jev on everything 89.8%; production 73.2%. Strings the deadline cuts keep the no-Jev answer
(`decider` column says which).
"""
import gzip
import json
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import requests

from .decider import POOL, Decider, Features, pool
from .retrieve import Index

# ---------------------------------------------------------------------------
# Inputs built from walden tables (same fields as #1363 pull.py)
# ---------------------------------------------------------------------------

CARDS_SQL = """
SELECT i.id, i.ror, i.display_name, i.type, i.status, i.works_count,
       to_json(i.display_name_acronyms) AS display_name_acronyms,
       to_json(i.display_name_alternatives) AS display_name_alternatives, to_json(i.geo) AS geo,
       to_json(transform(r.names, n -> named_struct('v', n.value, 'lang', n.lang, 't', n.types))) AS ror_names
FROM openalex.institutions.institutions_api i
LEFT JOIN (SELECT * FROM openalex.institutions.ror_parsed
           QUALIFY row_number() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1) r ON r.id = i.ror
"""


def write_cards(spark, path):
    n = 0
    with gzip.open(path, "wt") as f:
        for r in spark.sql(CARDS_SQL).toLocalIterator():
            d = r.asDict()
            d["id"] = str(d["id"])
            f.write(json.dumps(d) + "\n")
            n += 1
    return n


def write_lineage(spark, path):
    """{id, lineage} for institutions with an ancestor, like #1363 data/lineage.jsonl.gz (22,545 rows then; the
    table has 23,114 on 2026-09-27). institution_ancestors.lineage_ids holds only the ancestors; Features drops the
    id itself either way."""
    n = 0
    with gzip.open(path, "wt") as f:
        for r in spark.sql("""SELECT institution_id AS id, lineage_ids AS lineage
                              FROM openalex.institutions.institution_ancestors
                              WHERE size(lineage_ids) > 0""").toLocalIterator():
            f.write(json.dumps({"id": int(r.id), "lineage": [int(r.id)] + [int(x) for x in r.lineage]}) + "\n")
            n += 1
    return n


def names_for_dense(ix):
    """#1363 rstudy_prep.py: every name variant as "name, city, country"."""
    out = []
    for iid, d in ix.inst.items():
        place = ", ".join(x for x in (d["city"], d["country"]) if x)
        for n in dict.fromkeys([d["name"]] + d["alts"]):
            out.append((iid, f"{n}, {place}" if place else n))
    return out


# ---------------------------------------------------------------------------
# Generators (each returns ids in rank order, as in #1363 cands_<gen>.jsonl)
# ---------------------------------------------------------------------------

_IX = None


def _init_worker(cards_path):
    global _IX
    _IX = Index(cards_path)


def lex2(s):
    """#1363 rstudy_gen.py lex2: lexical top 100 + acronyms (per 10) at 0.4."""
    seen = dict(_IX.lexical(s, k=100))
    for iid in _IX.acronyms(s, per=10):
        seen.setdefault(iid, 0.4)
    return [i for i, _ in sorted(seen.items(), key=lambda x: -x[1])][:100]


def neighbour_all(strings, es_url, threads=32):
    """#1363 rstudy_gen.py neighbour: ES raw-affiliation-strings-v3, 30 nearest other strings with works,
    each hit's institution_ids_final voted with weight (score/top)^4. None = ES failed after retries."""
    url = es_url.rstrip("/") + "/raw-affiliation-strings-v3/_search"
    S = requests.Session()

    def one(s):
        body = {"size": 30, "_source": ["institution_ids_final", "works_count"],
                "query": {"bool": {"must": {"match": {"raw_affiliation_string": s[:1000]}},
                                   "must_not": {"term": {"raw_affiliation_string.keyword": s}},
                                   "filter": {"range": {"works_count": {"gt": 0}}}}}}
        for a in range(5):
            try:
                r = S.post(url, json=body, timeout=60)
                r.raise_for_status()
                hits = r.json()["hits"]["hits"]
                break
            except Exception:
                time.sleep(2 ** a)
        else:
            return None
        if not hits:
            return []
        top = hits[0]["_score"]
        sc = defaultdict(float)
        for h in hits:
            w = (h["_score"] / top) ** 4
            for i in h["_source"].get("institution_ids_final") or []:
                i = int(str(i).lstrip("I"))
                if i > 0:
                    sc[i] += w
        return [i for i, _ in sorted(sc.items(), key=lambda x: -x[1])][:100]

    with ThreadPoolExecutor(threads) as ex:
        return list(ex.map(one, strings))


def pieces(s):
    """#1363 modal_dense.py: 1-2 consecutive ; , ( ) segments, at most 16."""
    parts = [p.strip() for p in re.split(r"[;,()\n|]|\s-\s", s) if len(p.strip()) >= 3]
    wins = parts + [parts[i] + ", " + parts[i + 1] for i in range(len(parts) - 1)]
    return list(dict.fromkeys(w for w in wins if w != s))[:16]


def dense_chunks_all(strings, names, model_name="intfloat/multilingual-e5-base", prefix="query: ", maxlen=128,
                     device=None, name_emb=None, batch_size=256):
    """#1363 modal_dense.py, chunks variant: whole string + pieces, exact inner product against every name
    variant, institution score = max over its variants and the string's pieces; top 100 ids per string.
    Returns (ranks, name_emb) so the caller can cache the name embeddings."""
    import numpy as np
    import torch
    from sentence_transformers import SentenceTransformer
    device = device or ("cuda" if torch.cuda.is_available() else "cpu")
    m = SentenceTransformer(model_name, device=device)
    m.max_seq_length = maxlen
    if device == "cuda":
        m.half()
    enc = lambda xs: m.encode([prefix + x for x in xs], batch_size=batch_size, normalize_embeddings=True,
                              convert_to_tensor=True, show_progress_bar=False)
    nid = np.array([i for i, _ in names])
    N = name_emb if name_emb is not None else enc([t for _, t in names])
    N = N.to(device)
    texts, owner = [], []
    for qi, s in enumerate(strings):
        for p in [s[:2000]] + pieces(s):
            texts.append(p)
            owner.append(qi)
    E = enc(texts)
    if device != "cuda":
        E, N = E.float(), N.float()
    best = [dict() for _ in strings]
    K = 200
    for st in range(0, E.shape[0], 4096):
        v, ix = (E[st:st + 4096] @ N.T).topk(K, dim=1)
        v, ix = v.float().cpu().numpy(), ix.cpu().numpy()
        for r in range(v.shape[0]):
            b = best[owner[st + r]]
            for sc, k in zip(v[r], ix[r]):
                iid = int(nid[k])
                if sc > b.get(iid, -9):
                    b[iid] = float(sc)
    return [sorted(b, key=lambda i: -b[i])[:100] for b in best], N.cpu()


def top5_from_model_response(mr):
    """#1363 rstudy_prep.py: the 2023 model's stored top 5 (ids > 0)."""
    if not mr:
        return []
    if isinstance(mr, str):
        mr = json.loads(mr)
    return [int(x["id"]) for x in mr if int(x["id"]) > 0]


# ---------------------------------------------------------------------------
# Decider: Jev per (string, candidate), #1363 decider_pilot.py
# ---------------------------------------------------------------------------

NAMES_IT = ("Does this affiliation string say the author is affiliated with the institution shown? Answer yes if the string names the institution "
    "(by any of its names, other names, acronyms, former names or obvious typos) or a unit that is part of it (campus, school, college, department, "
    "institute, lab, center, hospital). Answer no if the string names only a different organization (including one with a similar name or the same "
    "acronym elsewhere), only a place, only a person, or no organization at all. Several organizations may be listed; answer yes if any of them is this "
    "institution or a unit of it. A unit name counts only when the string does not present it as belonging to a different organization, and an acronym "
    "counts only when the string does not spell it out as, or attach it to, a different organization. An organization merely located on the institution's "
    "campus, or a fellowship, prize or degree named after it, is not an affiliation.")
QUESTIONS = {"names_this_institution": {"type": "noul", "instructions": NAMES_IT}}


def jev_card(ix, i):
    d = ix.inst[i]
    return {"name": d["name"], "other_names": d["alts"][:6], "acronyms": d["acr"][:4],
            "type": d["type"], "city": d["city"], "country": d["country"]}


def uncertainty(probs):
    """#1363 hybrid.py: the string is unsure at margin b when any candidate's p is in (b, 1 - b); this is the largest
    b at which it is unsure (0 = sure of everything)."""
    return max((min(p, 1.0 - p) for p in probs.values()), default=0.0)


def jev_strings(client, ix, batch, threads=64):
    """batch: [(key, string, [candidate ids])]. {key: {inst: p}} for strings whose every candidate came back."""
    items = [(k, s, i) for k, s, ids in batch for i in ids]

    def one(x):
        k, s, i = x
        r = client.decide({"affiliation_string": s, "institution": jev_card(ix, i)}, QUESTIONS)
        if r.get("ok"):
            try:
                return (k, i, float(r["answers"]["names_this_institution"]["noul"]))
            except (KeyError, TypeError, ValueError):
                return None
        return None

    got = defaultdict(dict)
    with ThreadPoolExecutor(threads) as ex:
        for res in ex.map(one, items):
            if res:
                got[res[0]][res[1]] = res[2]
    return {k: got[k] for k, _, ids in batch if len(got.get(k, {})) == len(ids)}


def candidates(ix, ranks):
    """The pool (#1363 decider.pool) restricted to institutions with a card."""
    return [i for i in pool(ranks) if i in ix.inst]
