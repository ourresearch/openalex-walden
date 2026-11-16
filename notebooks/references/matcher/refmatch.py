"""Reference matcher (oxjob #2000, nightly in #2296): the clue features, decision rule, type tie-break and reader prompts.

Copied function by function from the code #2000's corpus run used (oxjobs working/reference-matcher/work: train_curve.py,
fixes_eval.py with flags "cgn", bench_links.py, reader_rows.py with COMPACT = 5, refmatch_full_dbx.py), with the research
paths left out. Any change here changes which references link: `check_bench()` must still reproduce #2000's review bench.

A reference (one distinct printed text, key qi = its text_hash) has up to ~50 candidate works (search + lookup). For each pair
`build()` computes clues (title words found, authors, year, volume, pages, journal, the search score); the checklist model
scores every pair; references whose best checklist score is unsure (LO <= p < HI) go to the reader (a fine-tuned 2B model on
Modal) and the combiner model rescores them with the reader's answer. A reference links when its pick scores >= LINE_HI, or
>= THR with first author + volume + page found; then the type tie-break may move the link to a copy of the pick that has the
type the reference cites (a book, a chapter, a thesis...).
"""
import functools
import json
import re
import unicodedata

import numpy as np
import pandas as pd

LINE_HI = 0.97

# ---------- text clues (train_curve.py) ----------


def fold(s):
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


TOK = re.compile(r"[a-z0-9]+")
STOP = set("a an the of and in on for to with by from at as or is are be its into via de la le des du et der die das und".split())
WORD = re.compile(r"[^\W_]+")
BIGRAM_SCRIPTS = ("CJK", "HIRAGANA", "KATAKANA", "HANGUL", "THAI")
PARTICLES = set("van von der den de da del della di du dos das la le ter ten".split())


@functools.lru_cache(maxsize=65536)
def char_class(ch):
    if ch.isascii():
        return "a"
    name = unicodedata.name(ch, "")
    if name.startswith("LATIN"):
        return "x"
    if name.startswith(BIGRAM_SCRIPTS):
        return "c"
    return "w"


def toks(s):
    """Latin text: [a-z0-9]+ runs of the folded text. Other letters: words, or character bigrams for unspaced scripts."""
    f = fold(s)
    if f.isascii():
        return TOK.findall(f)
    f = unicodedata.normalize("NFC", f)
    out = []
    for run in WORD.findall(f):
        seg, cls = "", None
        for ch in run + " ":
            k = char_class(ch) if ch != " " else None
            if k != cls and seg:
                if cls == "a":
                    out.extend(TOK.findall(seg))
                elif cls == "c":
                    out.extend([seg] if len(seg) == 1 else [seg[i:i + 2] for i in range(len(seg) - 1)])
                elif cls == "w":
                    out.append(seg)
                seg = ""
            cls = k
            seg += ch if k else ""
    return out


def is_unspaced(tok):
    return bool(tok) and char_class(tok[0]) == "c"


@functools.lru_cache(maxsize=4096)
def unspaced_text(s):
    return "".join(ch for ch in unicodedata.normalize("NFC", fold(s)) if char_class(ch) == "c")


def surname_keys(name):
    """Forms of an author's surname a reference may print, primary first."""
    name = name or ""
    if "," in name and toks(name.split(",", 1)[1])[:1] in (["jr"], ["sr"], ["ii"], ["iii"], ["iv"], ["phd"], ["md"]):
        name = name.split(",", 1)[0]
    if "," in name:
        before = [x for x in toks(name.split(",", 1)[0]) if len(x) > 1 and not is_unspaced(x)]
        keys = [x for x in before if x not in PARTICLES] or before
        if keys:
            return [keys[-1]] + keys[:-1]
    t = toks(name)
    cjk = unspaced_text(name)
    words = [x for x in t if not is_unspaced(x)]
    if cjk:
        latin = [x for x in words if len(x) > 1]
        return [cjk] + latin[:1]
    if any(char_class(ch) == "w" for x in words for ch in x):
        w2 = [x for x in words if len(x) > 1] or words
        return list(dict.fromkeys([w2[0], w2[-1]]))
    t = [x for x in t if len(x) > 1] or t
    return [t[-1]] if t else []


def surname(name):
    k = surname_keys(name)
    return k[0] if k else ""


def years_in(s):
    return [int(y) for y in re.findall(r"(?<!\d)(1[5-9]\d\d|20[0-3]\d)(?!\d)", s or "")]


def num(x):
    try:
        return int(x)
    except ValueError:
        return None


@functools.lru_cache(maxsize=65536)
def script_of(t):
    letters = [ch for ch in t or "" if unicodedata.category(ch).startswith("L")]
    latin = sum(1 for ch in letters if unicodedata.name(ch, "").startswith("LATIN"))
    return "nonlatin" if letters and latin / len(letters) < 0.5 else "latin"


def pair_features(ref_text, ref_toks, ref_set, ref_years, c):
    tt = [x for x in toks(c["title"]) if x not in STOP]
    tset = set(tt)
    t_in = sum(1 for x in tt if x in ref_set) / len(tt) if tt else 0.0
    r_cov = len(tset & ref_set) / max(1, len(ref_set))
    au = c["au"]
    au = json.loads(au) if isinstance(au, str) else list(au) if au is not None else []
    sn = [surname_keys(a) for a in au if a]
    found = lambda keys: any(len(k) > 1 and (k in unspaced_text(ref_text) if is_unspaced(k) else k in ref_set) for k in keys)
    fa = sn[0][0] if sn and sn[0] else ""
    fa_in = float(bool(sn) and found(sn[0]))
    au_in = sum(1 for keys in sn if found(keys)) / len(sn) if sn else 0.0
    y = c["year"]
    if ref_years and y == y and y is not None:
        dy = min(abs(int(y) - ry) for ry in ref_years)
    else:
        dy = -1
    vol = toks(c["vol"] or "")
    fp_ = toks(c["fp"] or "")
    vol_in = float(bool(vol) and vol[0] in ref_set) if vol else -1.0
    fp_in = float(bool(fp_) and fp_[0] in ref_set) if fp_ else -1.0
    vt = [x for x in toks(c["venue"]) if x not in STOP]
    v_in = sum(1 for x in vt if x in ref_set) / len(vt) if vt else -1.0
    run = best = 0
    joined = " " + " ".join(ref_toks) + " "
    allt = toks(c["title"])
    for i in range(len(allt)):
        for j in range(i + best + 1, len(allt) + 1):
            if " " + " ".join(allt[i:j]) + " " in joined:
                best = j - i
            else:
                break
    out = {"t_in": t_in, "r_cov": r_cov, "t_len": len(tt), "t_run": best, "t_run_frac": best / max(1, len(allt)),
           "fa_in": fa_in, "au_in": au_in, "n_au": c["n_au"] or 0, "dy": dy, "no_ref_year": float(not ref_years),
           "vol_in": vol_in, "fp_in": fp_in, "v_in": v_in,
           "fa_len": len(fa), "fa_vol_fp": float(fa_in == 1 and vol_in == 1 and fp_in == 1)}
    if script_of(ref_text) != script_of(c["title"]):
        out.update(t_in=-1.0, r_cov=-1.0, t_run=-1, t_run_frac=-1.0)
    if vt and script_of(ref_text) != script_of(c["venue"]):
        out["v_in"] = -1.0
    if sn and script_of(ref_text) != script_of(" ".join(a for a in au if a)):
        if not fa_in:
            out["fa_in"] = -1.0
        if not au_in:
            out["au_in"] = -1.0
    nums = {n for n in (num(x) for x in ref_toks if x.isdigit() and len(x) <= 6) if n is not None}
    f, l = (num(fp_[0]) if fp_ and fp_[0].isdigit() else None), None
    lp_ = toks(c.get("lp") or "")
    if lp_ and lp_[0].isdigit():
        l = num(lp_[0])
    out["lp_in"] = float(l is not None and l != f and l in nums) if l is not None else -1.0
    out["pg_in_range"] = float(any(f < n <= l for n in nums)) if f is not None and l is not None and 0 < l - f < 500 else -1.0
    out["fp_near"] = float(any(0 < abs(n - f) <= 2 for n in nums)) if f is not None and f > 2 else -1.0
    out["fa_vol_pg"] = float(fa_in == 1 and vol_in == 1 and (fp_in == 1 or out["pg_in_range"] == 1 or out["fp_near"] == 1))
    return out


TYPES = ["article", "book", "book-chapter", "book-review", "preprint", "dissertation", "review", "report", "dataset", "other",
         "paratext", "letter", "editorial", "reference-entry", "peer-review", "erratum", "libguides"]


def build(refs, pairs):
    """refs: qi, text. pairs: qi, cand_canon, score, rank, title, year, type, au, n_au, venue, vol, fp, lp."""
    rows = []
    rt = {}
    for r in refs.itertuples():
        text = r.text or ""
        t = toks(text)
        rt[r.qi] = (text, t, set(t), years_in(text), len(t))
    for c in pairs.to_dict("records"):
        text, t, s, ys, n = rt[c["qi"]]
        f = pair_features(text, t, s, ys, c)
        f.update(qi=c["qi"], cand=c["cand_canon"], score=c["score"], rank=c["rank"], ref_len=n,
                 type_i=TYPES.index(c["type"]) if c["type"] in TYPES else len(TYPES))
        rows.append(f)
    X = pd.DataFrame(rows)
    g = X.groupby("qi")
    X["score_gap"] = X.score - g.score.transform("max")
    for k in ("t_in", "t_run", "au_in", "r_cov"):
        X[f"{k}_gap"] = X[k] - g[k].transform("max")
        X[f"{k}_rank"] = g[k].rank(ascending=False, method="min")
    return X


# ---------- container records, glued references, type tie-break (fixes_eval.py, bench_links.py) ----------

COVER = re.compile(r"^\s*(issue information|cover picture|cover image|cover|front cover|inside front cover|inside back cover|"
                   r"back cover|inside cover|masthead|table of contents|contents|front matter|back matter|frontispiece|"
                   r"editorial board|issue highlights?|graphical contents list|contents list)\s*(?:[:.\-–—]|$)", re.I)
ISSUE_TAG = re.compile(r"\(((?:[A-Z][A-Za-z]*\.?\s){1,6})\d{1,2}\s*/\s*(?:19|20)\d{2}\)\s*$")
PLAIN_TAGS = {"Small", "Small Methods", "Small Structures", "Small Science"}


def is_container(typ, title):
    """An issue cover / contents record: never linked."""
    m = ISSUE_TAG.search(title or "")
    return bool(title and (COVER.match(title) or (m and ("." in m.group(1) or m.group(1).strip() in PLAIN_TAGS))))


YP = r"\(\s*(?:1[5-9]\d\d|20[0-3]\d)[a-z]?\s*\)"
PERSON = re.compile(r"(?<=[.;]\s)(?<![\s.][A-Z]\.\s)[A-Z][\w'’\-]+(?:[ -][A-Z][\w'’\-]+)?,\s(?:[A-Z]\.\s?-?){1,3}(?=.{0,200}?" + YP + ")")
ORG = re.compile(r"[A-Z][\w&’' ]{3,80}?,\s[A-Z]{2,}\.?\s*" + YP)
LETTER = re.compile(r"[^\W\d_]")


def first_ref(t):
    """The first of two references run together (each opens with authors and a year in parentheses), else t."""
    t = t or ""
    y1 = re.search(YP, t)
    if not y1:
        return t
    for m in sorted([*PERSON.finditer(t, y1.end()), *ORG.finditer(t, y1.end())], key=lambda m: m.start()):
        if len(LETTER.findall(t[y1.end():m.start()])) >= 25:
            return t[:m.start()].rstrip()
    return t


def cited_form(t):
    t = t or ""
    if re.search(r"\b(thesis|dissertation|tesis|tese|doctoral|phd|master'?s)\b", t, re.I): return "thesis"
    if re.search(r"\barxiv\b|biorxiv|medrxiv|ssrn|preprint|research square", t, re.I): return "preprint"
    if re.search(r"\bIn[:\s]+[^.]{0,80}\((?:ed|eds|hrsg|coord)\.?\)|\bIn:\s|\bed(?:s|ited)?\.? by\b", t): return "chapter"
    if re.search(r"\b\d{1,4}\s*(?:\(\s*[\w\-–]+\s*\))?\s*[:,]\s*[A-Za-z]?\d{1,6}\b", t) and not re.search(r"\b(press|verlag|publisher|editora|ed\.)\b", t, re.I):
        return "article"
    if re.search(r"\b(press|verlag|publisher|publishing|editora|editorial|isbn|edition|ed\.)\b", t, re.I): return "book"
    return "unclear"


def type_family(t):
    return {"article": "article", "review": "article", "letter": "article", "editorial": "article", "book": "book",
            "book-chapter": "chapter", "reference-entry": "chapter", "dissertation": "thesis", "preprint": "preprint"}.get(t or "", t or "?")


def copy_of(a, b):
    """Same work, any type: one title's words open the other's, or both have the same words; first author and year agree."""
    ta, tb = toks(a["title"] or ""), toks(b["title"] or "")
    if not ta or not tb:
        return False
    short, long_ = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    if not ((set(ta) == set(tb) and len(" ".join(ta)) > 10) or (len(short) >= 3 and long_[:len(short)] == short)):
        return False
    fa = lambda r: surname(list(r["au"])[0]) if r["au"] is not None and len(r["au"]) else ""
    if fa(a) and fa(b) and fa(a) != fa(b):
        return False
    ya, yb = a["year"], b["year"]
    return not (ya == ya and yb == yb and ya is not None and yb is not None and abs(int(ya) - int(yb)) > 1)


REPO = re.compile(r"zenodo|repositor|escholarship|commons|figshare|\bosf\b|scholarworks|research online|e-?prints|"
                  r"institutional|digital library|\bhal\b|pubmed central|europe pmc|dspace", re.I)


def less_complete(pick, alt):
    pv, av = pick["venue"], alt["venue"]
    return bool(pv) and not REPO.search(pv) and (not av or bool(REPO.search(av)))


FITS = {"article": {"article", "conference-paper"}, "book": {"book"}, "chapter": {"chapter"}, "thesis": {"thesis"},
        "preprint": {"preprint"}}


def tie_break(dec, pairs, texts, score_col):
    """dec: one row per reference (qi, cand, link). Moves a link to a copy of the pick whose type matches the cited form."""
    P = pairs.drop_duplicates(["qi", "cand_canon"]).set_index("qi")
    dec = dec.copy()
    dec["switched"] = False
    for i, r in dec[dec.link].iterrows():
        form = cited_form(first_ref(texts.get(r.qi)))
        if form not in FITS:
            continue
        g = P.loc[[r.qi]] if r.qi in P.index else None
        if g is None:
            continue
        g = g.reset_index()
        pick = g[g.cand_canon == r.cand]
        if pick.empty or type_family(pick.type.iloc[0]) in FITS[form]:
            continue
        pk = pick.iloc[0]
        alts = [c for c in g.itertuples() if c.cand_canon != r.cand and type_family(c.type) in FITS[form] and not is_container(c.type, c.title)
                and copy_of(pk, c._asdict()) and not less_complete(pk, c._asdict())]
        if alts:
            best = max(alts, key=lambda c: score_col.get((r.qi, c.cand_canon), -1))
            dec.loc[i, "cand"] = int(best.cand_canon)
            dec.loc[i, "switched"] = True
    return dec


# ---------- reader prompts (reader_rows.py, COMPACT = 5) ----------

READER_HEAD = ("A reference from a paper's reference list and a shortlist of records from a scholarly index. For each candidate "
               "answer yes only if it is a record of the very item the reference cites (a duplicate or copy of it counts). Answer no "
               "for another version the reference does not describe (other edition, preprint vs published, translation), a different "
               "work, a review of the cited book, a report when an article is cited, a chapter when the book is cited or the reverse.\n")
COMPACT = 5


def card_compact(c):
    au = [a.strip() for a in (c.get("authors") or "").split(",") if a.strip() and not a.strip().startswith("+")]
    bits = [", ".join(surname(a).title() for a in au[:3]) + (" et al" if len(au) > 3 else ""), f"({c['year']})" if c.get("year") else "",
            (c.get("title") or "")[:200], c.get("venue") or "", c.get("type") or "",
            " ".join(x for x in (f"v{c['volume']}" if c.get("volume") else "", f"p{c['pages']}" if c.get("pages") else "") if x)]
    return " | ".join(b for b in bits if b)


def reader_row(key, citation, cards, ids):
    cards, ids = cards[:COMPACT], ids[:COMPACT]
    lines = [f"REFERENCE: {citation[:600]}", "CANDIDATES:"]
    for k, c in enumerate(cards, 1):
        lines.append(f"c{k}: {card_compact(c)}")
    return {"seed_id": key, "prefix": READER_HEAD + "\n".join(lines) + "\nANSWERS:\n",
            "seats": [{"line": f"c{k}", "y": None, "key": str(cid)} for k, cid in enumerate(ids, 1)]}


# ---------- the per-bucket steps the nightly runs with applyInPandas (refmatch_full_dbx.py) ----------

# fe = the training run's feats.json: feats0 (checklist inputs), feats (combiner inputs), LO / HI (the reader's band), THR


def x_cols(fe):
    return ["qi", "cand", "ord"] + fe["feats0"] + ["p"]


def features_bucket(pdf, chk, fe):
    """Pairs of one bucket of references -> checklist features + checklist score p per (qi, cand)."""
    feats0 = fe["feats0"]
    pdf = pdf.sort_values(["qi", "ord"], kind="stable")
    pdf = pdf[pdf.title.notna()]
    keep = ~np.array([is_container(t, ti) for t, ti in zip(pdf.type, pdf.title)], dtype=bool)
    pairs = pdf[keep].reset_index(drop=True)
    if pairs.empty:
        return pd.DataFrame({c: pd.Series(dtype="float64") for c in x_cols(fe)})
    refs = pairs.drop_duplicates("qi")[["qi", "rtext"]].rename(columns={"rtext": "text"})
    refs["text"] = refs.text.map(first_ref)
    X = build(refs, pairs)
    lk = pairs.groupby(["qi", "cand_canon"]).from_lookup.max()
    X["from_lookup"] = [float(lk.get((q, c), False)) for q, c in zip(X.qi, X.cand)]
    X["ord"] = pairs.ord.values
    X = X.sort_values(["qi", "rank"], kind="stable").drop_duplicates(["qi", "cand"])
    X["p"] = chk.predict_proba(X[feats0])[:, 1]
    out = X[x_cols(fe)].copy()
    for c in feats0 + ["p"]:
        out[c] = out[c].astype("float64")
    return out


def reader_rows_bucket(X, P, fe):
    """References in the unsure band -> one reader prompt each (the checklist's best 5 candidates, compact cards)."""
    LO, HI = fe["LO"], fe["HI"]
    if X.empty:
        return pd.DataFrame({"qi": [], "seed_id": [], "prefix": [], "seats": []})
    X["qi"], X["cand"] = X.qi.astype("int64"), X.cand.astype("int64")
    P["qi"], P["cand_canon"] = P.qi.astype("int64"), P.cand_canon.astype("int64")
    X = X.sort_values(["qi", "rank", "ord"], kind="stable")
    top = X.sort_values(["qi", "p"], ascending=[True, False]).groupby("qi").head(1).set_index("qi")
    band = set(top[(top.p >= LO) & (top.p < HI)].index)
    P = P.sort_values(["qi", "ord"], kind="stable").drop_duplicates(["qi", "cand_canon"]).set_index(["qi", "cand_canon"])
    out = []
    for q, d in X[X.qi.isin(band)].sort_values(["qi", "p"], ascending=[True, False]).groupby("qi"):
        cards, ids = [], []
        for k, c in enumerate(d.cand.head(10), 1):
            r = P.loc[(q, c)]
            au = list(r.au) if r.au is not None else []
            cards.append({"key": f"c{k}", "title": r.title, "authors": ", ".join(au) + (f" +{int(r.n_au) - 5}" if (r.n_au or 0) > 5 else ""),
                          "year": None if pd.isna(r.year) else int(r.year), "venue": r.venue, "type": r.type, "volume": r.vol,
                          "issue": r.iss, "pages": "-".join(x for x in (r.fp, r.lp) if x) or None})
            ids.append(int(c))
        text = P.loc[q].rtext.iloc[0]
        row = reader_row(f"t:{q}", text, cards, ids)
        out.append({"qi": q, "seed_id": row["seed_id"], "prefix": row["prefix"], "seats": json.dumps(row["seats"])})
    return pd.DataFrame(out, columns=["qi", "seed_id", "prefix", "seats"])


DECIDE_COLS = ("qi", "cand", "s", "in_band", "checklist_p", "reader_p", "link", "switched")


def decide_bucket(X, P, comb, fe, line_hi=LINE_HI):
    """Checklist features + reader scores (column rp) -> one decision per reference, then the type tie-break."""
    feats, LO, HI, THR = fe["feats"], fe["LO"], fe["HI"], fe["THR"]
    if X.empty:
        return pd.DataFrame({c: [] for c in DECIDE_COLS})
    X["qi"], X["cand"] = X.qi.astype("int64"), X.cand.astype("int64")
    P["qi"], P["cand_canon"] = P.qi.astype("int64"), P.cand_canon.astype("int64")
    X = X.sort_values(["qi", "rank", "ord"], kind="stable").reset_index(drop=True)
    X["rp_f"] = X.rp.fillna(-1.0)
    g = X.groupby("qi")
    X["rp_max_other"] = g.rp_f.transform("max")
    X["rp_gap"] = X.rp_f - X.rp_max_other
    X["rp_top"] = (X.rp_f == X.rp_max_other).astype(float)
    X["p2"] = comb.predict_proba(X[feats])[:, 1]
    top_c = X.sort_values(["qi", "p"], ascending=[True, False]).groupby("qi").head(1).set_index("qi")
    top_r = X.sort_values(["qi", "p2"], ascending=[True, False]).groupby("qi").head(1).set_index("qi")
    band = (top_c.p >= LO) & (top_c.p < HI)
    d = top_c[["cand", "p"]].rename(columns={"p": "s"}).copy()
    d.loc[band, ["cand", "s"]] = top_r.loc[band[band].index, ["cand", "p2"]].values
    d["in_band"] = band
    d["checklist_p"] = top_c.p
    rp = {(q, c): v for q, c, v in zip(X.qi, X.cand, X.rp)}
    d["reader_p"] = [rp.get((q, int(c))) if b else None for q, c, b in zip(d.index, d.cand, d.in_band)]
    fvp = {(q, c): v for q, c, v in zip(X.qi, X.cand, X.fa_vol_pg)}
    d["link"] = (d.s >= line_hi) | ((d.s >= THR) & np.array([fvp.get((q, int(c)), 0) == 1 for q, c in zip(d.index, d.cand)]))
    d = d.reset_index()
    d["cand"] = d.cand.astype(np.int64)
    P = P.sort_values(["qi", "ord"], kind="stable")
    texts = dict(zip(P.qi, P.rtext))
    sc = {(q, c): s_ for q, c, s_ in zip(X.qi, X.cand, X.p2)}
    d = tie_break(d, P[P.title.notna()], texts, sc)
    d["reader_p"] = d.reader_p.astype("float64")
    return d[list(DECIDE_COLS)]


CLF = dict(max_iter=300, learning_rate=0.06, max_leaf_nodes=31, l2_regularization=1.0, random_state=0)


def fit_models(train_dir):
    """The checklist and combiner, refit from #2000's training rows (scikit-learn 1.8.0, random_state 0: the same models #2000's
    corpus run used). Fails if they don't reproduce the scores stored with the bench. Returns (checklist, combiner, fe)."""
    from sklearn.ensemble import HistGradientBoostingClassifier as HGB
    fe = json.load(open(f"{train_dir}/feats.json"))
    A = pd.read_parquet(f"{train_dir}/train_checklist.parquet")
    C = pd.read_parquet(f"{train_dir}/train_combiner.parquet")
    chk = HGB(**CLF).fit(A[fe["feats0"]], A.y)
    comb = HGB(**CLF).fit(C[fe["feats"]], C.y)
    B = pd.read_parquet(f"{train_dir}/bench_X.parquet")
    d1 = float(np.abs(chk.predict_proba(B[fe["feats0"]])[:, 1] - B.p).max())
    d2 = float(np.abs(comb.predict_proba(B[fe["feats"]])[:, 1] - B.p2).max())
    if not (d1 < 1e-9 and d2 < 1e-9):
        raise RuntimeError(f"refit models differ from #2000's: max |p diff| {d1:.2e}, |p2 diff| {d2:.2e}")
    return chk, comb, fe


def check_bench(train_dir, chk, comb, fe):
    """#2000's review bench (3,695 real unlinked references) through these functions: features, checklist scores and
    decisions must equal the stored ones. The bench was stored under the earlier link line (score >= THR); the corpus run's
    line (>= LINE_HI, or >= THR with author + volume + page; Casey 13 Nov) only drops links, so the check uses THR.
    Returns (n, same)."""
    bp = pd.read_parquet(f"{train_dir}/bench_pairs.parquet").reset_index(drop=True)
    br = pd.read_parquet(f"{train_dir}/bench_refs.parquet")
    bp["ord"] = np.arange(len(bp), dtype=np.int64)
    bp = bp.merge(br[["qi", "raw_text"]].rename(columns={"raw_text": "rtext"}), on="qi", how="left").sort_values("ord")
    bp["has_doi"] = bp.has_doi.astype(str)
    bp["au"] = bp.au.map(lambda a: list(a) if a is not None else None)
    X = features_bucket(bp, chk, fe)
    L = pd.read_parquet(f"{train_dir}/bench_X.parquet").set_index(["qi", "cand"])
    G = X.set_index(["qi", "cand"])
    if len(L) != len(G) or not L.index.equals(L.index.intersection(G.index)):
        raise RuntimeError(f"bench pairs differ: stored {len(L)}, now {len(G)}")
    for c in fe["feats0"]:
        if not np.array_equal(L.loc[G.index, c].to_numpy(float), G[c].to_numpy(float), equal_nan=True):
            raise RuntimeError(f"bench feature {c} differs from #2000's")
    # the checklist score may differ in the last bits on another machine's math library (fit_models allows 1e-9 too)
    dp = float(np.abs(L.loc[G.index, "p"].to_numpy(float) - G["p"].to_numpy(float)).max())
    if dp >= 1e-9:
        raise RuntimeError(f"bench checklist scores differ from #2000's by up to {dp:.2e}")
    scores = pd.read_parquet(f"{train_dir}/bench_reader.parquet")
    Xr = X.merge(scores.rename(columns={"p": "rp"})[["qi", "cand", "rp"]], on=["qi", "cand"], how="left")
    got = decide_bucket(Xr, bp[["qi", "cand_canon", "type", "title", "au", "year", "venue", "rtext", "ord"]].copy(), comb, fe,
                        line_hi=fe["THR"]).set_index("qi")
    loc = pd.read_parquet(f"{train_dir}/bench_decisions.parquet").set_index("qi")
    j = loc.join(got, rsuffix="_s", how="outer")
    same = int(((j.link == j.link_s) & (~j.link | (j.cand == j.cand_s))).sum())
    return len(j), same
