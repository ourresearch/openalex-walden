# Vendored from oxjobs #1363 work/retrieve.py (sha256 64c01eaf77f8…) for the walden nightly (oxjob #1386).
# Keep the logic byte-identical to the benchmarked copy: the chooser was trained on these features.
# Only change: the institutions file is an argument (the nightly builds it from walden tables).
"""Candidate institutions for an affiliation string, for the labeller (#1363 step 2).

Union of:
  - lexical: IDF-weighted coverage of each institution name variant by the tokens of a window of
    1-2 consecutive comma/semicolon segments of the string (catches abbreviations after expansion,
    word-order variants, CJK via character bigrams);
  - acronyms: case-sensitive whole-token match in the raw string;
  - the current pipeline's ids for the string (final, model, override, model top 5, curated adds),
    so the gold can never miss what production says. Sources are not shown to the labeller.

    from retrieve import Index
    ix = Index()                      # ~1 min
    ix.candidates(string, frozen_ids) # -> list of (inst_id, score)
    ix.card(inst_id)                  # -> one-line card
"""
import gzip, json, math, re, unicodedata
from collections import defaultdict

DATA = "data/institutions.jsonl.gz"

ABBR = {
    "univ": "university", "universidad": "university", "universidade": "university",
    "universite": "university", "universita": "university", "universitat": "university",
    "universitaet": "university", "universiteit": "university", "uniwersytet": "university",
    "inst": "institute", "institut": "institute", "instituto": "institute", "istituto": "institute",
    "dept": "department", "dep": "department", "depto": "department",
    "hosp": "hospital", "hop": "hospital", "hopital": "hospital",
    "natl": "national", "nat": "national", "nacional": "national", "nationale": "national", "nazionale": "national",
    "sci": "science", "sciences": "science", "technol": "technology", "tech": "technology",
    "lab": "laboratory", "labs": "laboratory", "laboratories": "laboratory", "lab.": "laboratory",
    "ctr": "center", "cent": "center", "centre": "center", "centro": "center", "zentrum": "center",
    "res": "research", "med": "medical", "coll": "college", "sch": "school", "acad": "academy",
    "fac": "faculty", "engn": "engineering", "eng": "engineering", "agr": "agricultural",
    "st": "saint", "ste": "saint", "mt": "mount", "&": "and", "und": "and", "et": "and", "y": "and",
}
STOP = {"of", "the", "and", "de", "la", "le", "les", "des", "du", "di", "del", "della", "der", "die",
        "das", "für", "fur", "for", "in", "at", "da", "do", "dos", "das", "e", "i", "a", "an", "en", "van", "von"}
CJK = re.compile(r"[぀-ヿ㐀-鿿가-힯]")


FOLD = str.maketrans({"ı": "i", "ł": "l", "Ł": "L", "ø": "o", "Ø": "O", "đ": "d", "Đ": "D", "ß": "ss",
                      "æ": "ae", "œ": "oe", "’": "", "'": "", "`": ""})


def fold(s):
    s = unicodedata.normalize("NFKD", s.translate(FOLD))
    return "".join(c for c in s if not unicodedata.combining(c))


def tokens(s):
    s = fold(s).lower().replace("&", " and ")
    out = []
    words = re.findall(r"[^\W_]+", s)
    merged = []  # "m d anderson" -> "md anderson"
    for w in words:
        if len(w) == 1 and not CJK.search(w) and merged and merged[-1][1]:
            merged[-1] = (merged[-1][0] + w, True)
        else:
            merged.append((w, len(w) == 1 and not CJK.search(w)))
    for w, _ in merged:
        if CJK.search(w):
            chars = [c for c in w]
            out += [a + b for a, b in zip(chars, chars[1:])] or chars
            continue
        w = ABBR.get(w, w)
        if w in STOP:
            continue
        if len(w) > 4 and w.endswith("s") and not w.endswith(("ss", "us", "is")):
            w = w[:-1]
        out.append(w)
    return out


def segments(s):
    parts = [p for p in re.split(r"[;,\n|]|\s-\s", s) if p.strip()]
    wins = list(parts)
    wins += [parts[i] + " " + parts[i + 1] for i in range(len(parts) - 1)]
    return wins or [s]


class Index:
    def __init__(self, path=DATA):
        self.inst = {}
        self.variants = []          # (inst_id, token tuple)
        self.acr = defaultdict(set) # acronym -> ids
        post = defaultdict(set)
        for line in gzip.open(path, "rt"):
            r = json.loads(line)
            if r["status"] == "withdrawn":
                continue
            iid = int(r["id"])
            alts = json.loads(r["display_name_alternatives"] or "[]")
            acrs = json.loads(r["display_name_acronyms"] or "[]")
            rn = json.loads(r["ror_names"] or "[]")
            for n in rn:
                if "acronym" in (n.get("t") or []):
                    acrs.append(n["v"])
                else:
                    alts.append(n["v"])
            geo = json.loads(r["geo"] or "{}")
            names = list(dict.fromkeys([r["display_name"]] + alts))
            index_names = names + [re.sub(r"\s*\([^)]*\)", "", n) for n in names if "(" in n]
            self.inst[iid] = dict(name=r["display_name"], alts=[a for a in names[1:]],
                                  acr=sorted(set(acrs)), city=geo.get("city"), region=geo.get("region"),
                                  country=geo.get("country"), cc=geo.get("country_code"),
                                  type=r["type"], status=r["status"], works=int(r["works_count"] or 0))
            for n in index_names:
                t = tuple(dict.fromkeys(tokens(n)))
                if t:
                    vi = len(self.variants)
                    self.variants.append((iid, t))
                    for w in t:
                        post[w].add(vi)
            for a in set(acrs):
                if len(a) >= 2:
                    self.acr[a].add(iid)
        n = len(self.variants)
        self.idf = {w: math.log(n / len(v)) for w, v in post.items()}
        self.post = post
        self.place = defaultdict(set)  # folded lowercase place token -> ids (city / country)
        for iid, d in self.inst.items():
            for p in (d["city"], d["country"]):
                if p:
                    self.place[fold(p).lower()].add(iid)

    def expand(self, T):
        """Add vocabulary tokens that contain, or nearly equal, a distinctive token of T.
        Used only for the labeller's proposed names ("Basell" -> "lyondellbasell", "myongi" -> "myongji")."""
        import difflib
        if not hasattr(self, "_vocab"):
            self._vocab = [w for w in self.post if len(w) >= 5]
        out = set(T)
        for t in T:
            if len(t) < 5 or CJK.search(t) or len(self.post.get(t, ())) > 50:
                continue  # only rare or unseen tokens get expanded
            out |= {w for w in self._vocab if t in w and len(self.post[w]) <= 200}
            out |= set(difflib.get_close_matches(t, self._vocab, n=3, cutoff=0.85))
        return out

    def lexical(self, s, k=25, gather_df=20000, fuzzy=False):
        scores = defaultdict(float)
        for win in segments(s):
            T = set(tokens(win))
            if fuzzy:
                T = self.expand(T)
            if not T:
                continue
            cand = set()
            for w in T:
                p = self.post.get(w)
                if p and len(p) <= gather_df:
                    cand |= p
            for vi in cand:
                iid, t = self.variants[vi]
                tot = sum(self.idf.get(w, 0) for w in t)
                if tot <= 0:
                    continue
                got = sum(self.idf.get(w, 0) for w in t if w in T)
                if got < 5.0:  # generic-only overlap ("hospital", "university of")
                    continue
                cov = got / tot + 0.25 * (got >= tot - 1e-9)
                if cov > scores[iid]:
                    scores[iid] = cov
        low = fold(s).lower()
        ranked = []
        for iid, c in scores.items():
            if c < 0.5:
                continue
            d = self.inst[iid]
            here = any(p and fold(p).lower() in low for p in (d["city"], d["country"]))
            ranked.append((c + 0.15 * here + 0.02 * math.log10(1 + d["works"]), iid))
        ranked.sort(reverse=True)
        return [(iid, round(sc, 3)) for sc, iid in ranked[:k]]

    def acronyms(self, s, per=6):
        out = []
        low = fold(s).lower()
        toks = set(re.findall(r"[A-Za-z][A-Za-z0-9&\-]{1,11}", s))
        toks |= {p for t in list(toks) if "-" in t for p in t.split("-") if len(p) >= 2}  # "IPCF-CNR"
        toks |= {d.replace(".", "") for d in re.findall(r"\b(?:[A-Z]\.){2,8}", s)}  # "U.L.B." -> "ULB"
        for w in toks:
            ids = self.acr.get(w)
            if not ids or not any(c.isupper() for c in w):
                continue
            ranked = sorted(ids, key=lambda i: (
                -any(p and fold(p).lower() in low for p in (self.inst[i]["city"], self.inst[i]["country"])),
                -self.inst[i]["works"]))
            out += ranked[:per]
        return out

    def candidates(self, s, frozen=(), k=25, cap=45):
        seen = {}
        for iid, sc in self.lexical(s, k=k):
            seen[iid] = sc
        for iid in self.acronyms(s):
            seen.setdefault(iid, 0.4)
        for iid in frozen:
            if iid in self.inst:
                seen.setdefault(iid, 0.3)
        return sorted(seen.items(), key=lambda x: -x[1])[:cap]

    def card(self, iid):
        d = self.inst[iid]
        other = [a for a in d["alts"] if a != d["name"]][:6]
        parts = [d["name"]]
        parts.append("; ".join(other + d["acr"][:4]) or "-")
        parts.append(", ".join(x for x in (d["city"], d["region"], d["country"]) if x) or "-")
        parts.append(d["type"] or "-")
        if d["status"] != "active":
            parts.append(d["status"])
        return " | ".join(parts)
