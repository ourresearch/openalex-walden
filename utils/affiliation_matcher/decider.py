# Vendored from oxjobs #1363 work/decider.py (sha256 9833296f2976…) for the walden nightly (oxjob #1386).
# Keep the logic byte-identical to the benchmarked copy. Only change: package-relative import.
"""The decider's shared code (#1363 ship-plan step 1): the chooser's features and its apply step.

chooser.py (benchmark) and the corpus run (#1385) both call this, so the corpus runs the code the scoreboard scored.

Pool per string = lex2 top 10 ∪ neighbour top 5 ∪ dense_me5b_chunks top 10 ∪ model top 5 (POOL, first occurrence
wins), each candidate scored by p = the student's (or Jev's) names_this_institution probability for (string, card).

    from decider import Decider
    d = Decider("data/decider/<name>/chooser.pkl")        # loads Index + lineage (~1 min)
    ids, probs = d.decide(string, p_by_inst, ranks_by_gen) # ranks_by_gen = {gen: [ids in rank order]}
"""
import gzip, json, math, pickle, re
from .retrieve import Index, fold

POOL = [("lex2", 10), ("neighbour", 5), ("dense_me5b_chunks", 10), ("top5", 5)]
FEATURES = ["p", "rank_lex2", "rank_neighbour", "rank_dense", "rank_top5", "name_hit", "name_len", "acronym_only",
            "n_liked_ancestors", "n_liked_descendants", "max_p_liked_descendant", "inside_liked_name", "log_works",
            "n_liked", "string_len"]
NO_P = [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14]  # the no-model ablation drops p (0) and the max p of a liked child (10)


def pool(ranks):
    """Candidate ids for a string from its generator lists, in POOL order."""
    ids = []
    for g, k in POOL:
        ids += list(ranks.get(g, []))[:k]
    return list(dict.fromkeys(ids))


class Features:
    def __init__(self, ix=None, lineage="data/lineage.jsonl.gz"):
        self.ix = ix or Index()
        self.anc = {}
        for l in gzip.open(lineage, "rt"):
            d = json.loads(l)
            self.anc[d["id"]] = set(d["lineage"]) - {d["id"]}

    @staticmethod
    def norm(x):
        return " " + re.sub(r"[^\w]+", " ", fold(x).lower()).strip() + " "

    def best_name(self, iid, low, raw):
        """Longest name of the institution found in the string: (length in chars, start, end) or None."""
        d = self.ix.inst[iid]
        best = None
        for n in [d["name"]] + d["alts"]:
            nn = self.norm(n)
            if len(nn) < 6:
                continue
            j = low.find(nn)
            if j >= 0 and (best is None or len(nn) > best[0]):
                best = (len(nn), j, j + len(nn))
        for ac in d["acr"]:
            m = re.search(rf"(?<![A-Za-z]){re.escape(ac)}(?![A-Za-z])", raw) if len(ac) >= 2 else None
            if m and best is None:
                best = (len(ac), -1, -1)  # acronym: no span in the folded string
        return best

    def rows(self, s, js, ranks, no_p=False):
        """[(inst, feature vector)] for every scored candidate of string s. js = {inst: p}; ranks = {gen: [ids]}."""
        ix, ANC = self.ix, self.anc
        low, out, ids = self.norm(s), [], list(js)
        liked = set(js) if no_p else {i for i, p in js.items() if p >= 0.5}
        hit = {i: self.best_name(i, low, s) for i in ids if i in ix.inst}
        for i in ids:
            if i not in ix.inst:
                continue
            rk = [ranks.get(g, [])[:kk].index(i) if i in ranks.get(g, [])[:kk] else 99 for g, kk in POOL]
            h = hit[i]
            anc_liked = [j for j in liked if j != i and j in ANC.get(i, ())]  # a liked parent of i
            desc_liked = [j for j in liked if j != i and i in ANC.get(j, ())]  # a liked child of i
            inside = 0
            if h and h[1] >= 0:
                for j in liked:
                    hj = hit.get(j)
                    if j != i and hj and hj[1] >= 0 and hj[1] <= h[1] and hj[2] >= h[2] and hj[0] > h[0]:
                        inside = 1
            f = [js[i], *rk, h is not None, h[0] if h else 0, h is not None and h[1] < 0,
                 len(anc_liked), len(desc_liked), max([js[j] for j in desc_liked], default=0), inside,
                 math.log10(1 + ix.inst[i]["works"]), len(liked), len(s)]
            if no_p:
                f = [f[j] for j in NO_P]
            out.append((i, f))
        return out


class Decider:
    """A frozen chooser (chooser.py --save): features -> GBT p -> ids with p >= t."""

    def __init__(self, path, feats=None):
        self.m = pickle.load(open(path, "rb"))
        self.F = feats or Features()

    def decide(self, s, js, ranks):
        R = self.F.rows(s, js, ranks, self.m["no_p"])
        if not R:
            return [], {}
        P = self.m["clf"].predict_proba([f for _, f in R])[:, 1]
        probs = {i: float(p) for (i, _), p in zip(R, P)}
        return sorted(i for i, p in probs.items() if p >= self.m["t"]), probs
