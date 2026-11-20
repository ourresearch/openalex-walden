"""Read per-author CRediT roles out of an "Author contributions" statement in a paper's full text (oxjob #2450).

Only statements written in CRediT's own terms count: "Conceptualization, J.Z. and Y.H.; Software, Y.H." (term first:
MDPI, PLOS, Springer) or "Jialin Zhou: Conceptualization, Investigation." (name first: Elsevier's CRediT authorship
contribution statement, ACS, Wiley, Frontiers). Free-form sentences ("J.Z. designed the study") are skipped, and so
are roles outside CRediT ("Data collection").

Every name in the statement is matched to the work's byline (full names, or initials such as "J.Z.", "JZ", "J.-H.L.",
"RS-H"), and a name is used only when exactly one author on the byline fits it. A statement whose names mostly do not
fit the byline is dropped whole: the stored full text may belong to another paper (oxjob #2281).

Pure Python, standard library only, so the same file runs on desk and inside a Databricks job.

    parse(text, byline) -> Result(status, roles, layout, stats, statement)
        text:   full text, or a window of it that holds the statement
        byline: the work's raw author names, in authorship order
        roles:  {seat index: [CRediT slugs in CRediT's order]} when status == "ok"
"""
import re
import unicodedata
from dataclasses import dataclass, field

PARSER_VERSION = "credit-fulltext-3"

CREDIT_ROLES = (
    "conceptualization", "data-curation", "formal-analysis", "funding-acquisition", "investigation", "methodology",
    "project-administration", "resources", "software", "supervision", "validation", "visualization",
    "writing-original-draft", "writing-review-editing",
)

# --- CRediT terms ------------------------------------------------------------------------------------------------
# One pattern per role, with the spellings GROBID leaves: hyphens and spaces lost ("Writingoriginal draft"), the "ft"
# ligature dropped ("original dra", "soware"), "Writing (original draft)".
_W = r"(?:writing[\s\-‐‑‒–—―:,./()]*(?:and\s+|of\s+)?(?:the\s+)?)?"
_TERM_PATTERNS = {
    "conceptualization": r"conceptuali[sz]ation",
    "data-curation": r"data\s*curation",
    "formal-analysis": r"formal\s*analys[ie]s",
    "funding-acquisition": r"funding\s*acquisitions?",
    "investigation": r"investigations?",
    "methodology": r"methodolog(?:y|ies)",
    "project-administration": r"project\s*administration",
    "resources": r"resources?",
    "software": r"so(?:ft)?ware",
    "supervision": r"supervision",
    "validation": r"validation",
    "visualization": r"visuali[sz]ations?",
    "writing-original-draft": _W + r"original\s*dra(?:ft)?(?:\s*(?:preparation|writing))?",
    "writing-review-editing": _W + r"(?:review(?:ing)?|revision)[\s,]*(?:&|and)?[\s,]*editing",
}
_TERM_RE = re.compile(
    r"(?<![A-Za-z])(?:" + "|".join(f"(?P<t{i}>{p})" for i, p in enumerate(_TERM_PATTERNS.values())) + r")(?![A-Za-z])",
    re.I)
_SLUG_BY_GROUP = {f"t{i}": s for i, s in enumerate(_TERM_PATTERNS)}
# A role written with a degree, as CRediT allows: "Methodology (equal)", "Writing – original draft (lead)", "-lead".
_DEGREE = re.compile(r"[(\[]\s*(?:equal|lead|leading|supporting|support|equally)\s*[)\]]"
                     r"|[-–]\s?(?:lead|equal|supporting)(?![A-Za-z])", re.I)

# --- where the statement is ---------------------------------------------------------------------------------------
_HEADING = re.compile(
    r"(?:CRediT\s*(?:authorship\s*)?(?:contributions?\s*)?(?:author\s*)?statement|CRediT\s*:"
    r"|Author(?:s|'s|s'|’s|s’)?\s+contributions?(?:\s+statement)?"
    r"|Authorship\s+contributions?(?:\s+statement)?"
    r"|Contributions?\s+of\s+(?:the\s+)?authors?"
    r"|Contributor(?:s|ship)?\s+statement"
    r"|(?<![a-z])Contributors(?![a-z])"
    r"|(?<![a-z])Contributions(?![a-z]))\s*[:.]?"
    r"|A\s?U\s?T\s?H\s?O\s?R\s?S?\s?C\s?O\s?N\s?T\s?R\s?I\s?B\s?U\s?T\s?I\s?O\s?N\s?S?(?![a-z])\s*[:.]?",
    re.I)
# What ends it: the next back-matter heading, or a closing sentence about all authors.
_STOP = re.compile(
    r"(?:Funding(?!\s*acquisition)|Conflicts?\s+of\s+interests?|Competing\s+interests?|Declaration\s+of|Declarations"
    r"|Acknowledge?ments?|Data\s+availability|Availability\s+of\s+data|Data\s+sharing|Institutional\s+Review"
    r"|Informed\s+consent|Ethic(?:s|al)\s|Supplementa(?:ry|l)\s|Supporting\s+information|Abbreviations|References"
    r"|Disclosures?|Financial\s+(?:support|disclosure)|Consent\s+for|Publisher['’]?s\s+note|Peer\s+review"
    r"|Open\s+access|Sources?\s+of\s+funding|Generative\s+AI|Use\s+of\s+AI|Copyright|ORCID|Appendix"
    r"|Associated\s+content|Author\s+information|Corresponding\s+author|Code\s+availability"
    r"|All\s+(?:the\s+)?(?:co-?)?authors\s+(?:have\s+|had\s+)?(?:read|approved|agreed|reviewed|contributed|critically"
    r"|discussed|commented|revised|edited|gave|participated|made|take|are|were|did|listed|accepted|consented"
    r"|declare|confirm|agree)"
    r"|The\s+authors?\s+(?:have\s+)?(?:read|approved|declare|agreed|thank|confirm|contributed))",
    re.I)
_MAX_STATEMENT = 6000
_MAX_NAMES_RUN = 1500

# --- names --------------------------------------------------------------------------------------------------------
_TITLES = re.compile(r"(?<![A-Za-z])(?:Prof\.?|Professor|Dr\.?|Mr\.|Mrs\.?|Ms\.|Miss|Ph\.?\s?D\.?)(?![A-Za-z])")
_ALL = re.compile(r"^(?:all|each)\s+(?:of\s+)?(?:the\s+)?(?:co-?)?authors?$", re.I)
_SEP = re.compile(r"\s*(?:[,;，；、&+]|\band\b|\bund\b|\bas\s+well\s+as\b)\s*", re.I)
_PARTICLES = {"de", "da", "do", "dos", "das", "del", "della", "di", "du", "van", "von", "der", "den", "la", "le", "el",
              "al", "bin", "ibn", "ben", "y", "e", "ter", "te", "zu", "st"}
_TRANS = str.maketrans({"ı": "i", "ø": "o", "Ø": "O", "ł": "l", "Ł": "L", "đ": "d", "Đ": "D", "ß": "ss", "æ": "ae",
                        "Æ": "AE", "œ": "oe", "Œ": "OE", " ": " ", "’": "'", "‐": "-", "‑": "-", "–": "-"})
# GROBID sometimes splits a letter from its accent: "Ta ¨nzler", "Hu ¨ttelmaier".
_LOOSE_ACCENT = re.compile(r"(?<=[A-Za-z])\s?[¨´ˆ˜¸˘ˇ˙˚˝`]\s?(?=[A-Za-z])")


def _fold(s):
    s = unicodedata.normalize("NFKD", _LOOSE_ACCENT.sub("", s or "").translate(_TRANS))
    return "".join(c for c in s if not unicodedata.combining(c))


def _words(name):
    """A byline name as lowercase words, hyphens kept ("Jia-Hui Ling" -> ["jia-hui", "ling"]); "Ling, Jia-Hui"
    is turned round to given-first."""
    n = _fold(name).strip()
    if n.count(",") == 1:
        fam, giv = n.split(",")
        n = f"{giv} {fam}"
    n = re.sub(r"[^A-Za-z\-' ]+", " ", n.replace(".", " "))
    return [w for w in (x.strip("-'").lower() for x in n.split()) if w]


# a byline entry that is a group, not a person: "all authors" never reaches it and no name matches it
_GROUP = re.compile(r"(?i)\b(?:group|consortium|investigators|collaborat\w*|team|network|committee|study|"
                    r"society|association|initiative|working|members|on behalf)\b")


def _split_h(words):
    return [p for w in words for p in w.split("-") if p]


@dataclass
class _Seat:
    idx: int
    words: list            # lowercase words, hyphens kept
    flat: set = field(default_factory=set)   # every word, hyphen parts and joined form

    def __post_init__(self):
        for w in self.words:
            self.flat.add(w.replace("-", "").replace("'", ""))
            self.flat.update(p for p in re.split(r"[-']", w) if p)

    def word_lists(self, tier):
        """Ways the name may be abbreviated, most literal first. Tier 0: every word, hyphenated names as one or as
        parts. Tier 1: middle names or particles left out, the family name whole or by its parts ("RS-H" = Ramón
        Soto-Hernández). Tier 2: tiers 0-1 family name first."""
        w = self.words
        lists = [w, _split_h(w)]
        if tier >= 1 and len(w) > 1:
            lists += [[w[0], w[-1]], [w[0], *w[-1].split("-")], [*w[0].split("-"), w[-1]]]
            core = [x for x in w if x not in _PARTICLES]
            if core != w:
                lists += [core, _split_h(core)]
        if tier == 2:
            lists = [[l[-1], *l[:-1]] for l in lists if len(l) > 1]
        out = []
        for l in lists:
            if l not in out:
                out.append(l)
        return out


def _initials(chunk):
    """'L.Y.' -> ['l','y'];  'J.-H.L.' -> ['j','h','l'];  'DNK' -> ['d','n','k'];  'M.Sa.' -> ['m','sa'];
    'RMo' -> ['r','mo'];  'RS-H' -> ['r','s','h'];  'F.d.l.T.' -> ['f','d','l','t'].  None when the chunk is not
    written as initials."""
    c = _fold(chunk).strip()
    if not c or len(c) > 16:
        return None
    pieces = [p for p in re.split(r"(?<=\.)|[\s\-]+", c) if p]
    out = []
    for p in pieces:
        q = p.rstrip(".")
        if not q:
            continue
        if re.fullmatch(r"[A-Z]{1,4}", q):
            out += [ch.lower() for ch in q]
        elif re.fullmatch(r"(?:[A-Z][a-z]?){2,4}", q) and sum(ch.isupper() for ch in q) >= 2:
            out += [x.lower() for x in re.findall(r"[A-Z][a-z]?", q)]
        elif re.fullmatch(r"[A-Z][a-z]{2,}[A-Z]{1,2}", q):          # "XiaoW": given name, family initial
            out += [re.match(r"[A-Z][a-z]+", q).group(0).lower()] + [ch.lower() for ch in q if ch.isupper()][1:]
        elif p.endswith(".") and re.fullmatch(r"[A-Z][a-z]{0,2}|[a-z]", q):
            out.append(q.lower())
        else:
            return None
    return out if 1 < len(out) <= 6 else None


def _fits_initials(parts, words):
    return len(parts) == len(words) and all(w.startswith(p) for p, w in zip(parts, words))


def _fits_full(chunk, seat):
    """Score how well a written-out name fits a seat; 0 = it doesn't. Each whole word of the chunk must be one of the
    seat's words (or start like one of the seat's initials: "Diogo Leonai Marques de Souza" fits "Diogo L. M. Souza");
    each initial must start a seat word not used yet. Particles (de, van, ...) may be left out on either side."""
    c = re.sub(r"[^A-Za-z\-' .]+", " ", _fold(chunk)).replace(".", ". ")
    c = re.sub(r"(?<![A-Za-z])([A-Z]{2,3})(?=\s+[A-Z][a-z])", lambda m: ". ".join(m.group(1)) + ".", c)  # "JD Smith"
    c = re.sub(r"(?<=[a-z]\s)([A-Z]{2,3})(?![A-Za-z])", lambda m: ". ".join(m.group(1)) + ".", c)  # "Vuyyuru SK"
    toks = [t.strip("-'").lower() for t in c.replace("-", " ").split()]
    toks = [t for t in toks if t.strip(".")]
    full = [t.rstrip(".") for t in toks if len(t.rstrip(".")) >= 2 and not (t.endswith(".") and len(t) <= 3)]
    inits = [t.rstrip(".") for t in toks if t.rstrip(".") not in full]
    seat_parts = _split_h(seat.words)
    seat_initials = {w for w in seat_parts if len(w) == 1}
    used, exact = set(), 0
    for t in full:
        if t in seat.flat or t.replace("'", "") in seat.flat:
            exact += 1
            used.update(i for i, w in enumerate(seat_parts) if w == t)
            used.update(i for i, w in enumerate(seat.words) if w.replace("-", "") == t)
        elif t[0] in seat_initials:
            used.update(i for i, w in enumerate(seat_parts) if w == t[0])
        elif t in _PARTICLES:
            continue
        else:
            return 0
    # at least one written-out word must be the seat's own, and not only a particle ("de"), unless it is the family
    # name itself ("Das P" for Prasenjit Das)
    if exact == 0 or not any(t in seat.flat and (t not in _PARTICLES or t == seat_parts[-1]) for t in full):
        return 0
    for i in inits:
        free = [k for k, w in enumerate(seat_parts) if k not in used and w.startswith(i[0])]
        if not free:
            return 0
        used.add(free[0])
    return exact * 2 + len(inits)


class Byline:
    def __init__(self, names):
        self.names = list(names or [])
        self.n = len(self.names)
        self.seats = [s for s in (_Seat(i, _words(n)) for i, n in enumerate(self.names))
                      if s.words and not _GROUP.search(self.names[s.idx])]
        # the same name twice on a byline is one person listed twice: a fit to one is a fit to both
        self.seats_by_idx = {s.idx: s for s in self.seats}
        self.same = {}
        for s in self.seats:
            self.same.setdefault(tuple(s.words), []).append(s.idx)

    def _one(self, idxs):
        """idxs fit; one person (possibly listed twice) -> their seats, else None."""
        groups = {tuple(self.seats_by_idx[i].words) for i in idxs}
        return self.same[groups.pop()] if len(groups) == 1 else None

    def match(self, chunk):
        """(seats, how): the byline seats a name chunk names ('ALL' for "all authors"), or (None, why not)."""
        c = _DEGREE.sub(" ", chunk)
        paren = re.search(r"\(([^()]{3,60})\)", c)
        if paren:  # "G.C. (Giuliano Costa)": the full name settles which G.C.
            s, why = self.match(paren.group(1))
            if s is not None and s != "ALL":
                return s, why
            c = c[:paren.start()] + c[paren.end():]
        c = _TITLES.sub(" ", c).strip(" .,:;()[]*")
        c = re.sub(r"\s+", " ", c)
        if re.fullmatch(r"[A-Z][a-z]+(?:[A-Z][a-z]+)+", c):  # "JieLi" (space lost) -> "Jie Li"
            c = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", c)
        if not c:
            return None, "empty"
        if _ALL.match(c):
            return "ALL", "all"
        parts = _initials(c)
        if parts:
            tries = [parts]
            if re.fullmatch(r"[A-Z][a-z]{1,3}[A-Z]", c):   # "PvdS": particles as lower-case initials, letter by letter
                tries.append([ch.lower() for ch in c])
            for p in tries:
                for tier in (0, 1, 2):
                    hits = {s.idx for s in self.seats if any(_fits_initials(p, wl) for wl in s.word_lists(tier))}
                    if hits:
                        one = self._one(hits)
                        return (one, f"initials{tier}") if one else (None, "ambiguous")
            return None, "unmatched"
        if not re.search(r"[A-Za-z]{2}", _fold(c)) or len(c) > 60 or re.search(r"\d", c):
            return None, "not_a_name"
        scores = {s.idx: _fits_full(c, s) for s in self.seats}
        best = max(scores.values(), default=0)
        if best == 0:
            return None, "unmatched"
        one = self._one([i for i, v in scores.items() if v == best])
        return (one, "full") if one else (None, "ambiguous")


def _strip_name(chunk):
    return _TITLES.sub(" ", _DEGREE.sub(" ", chunk)).strip(" .,:;()[]*")


def _looks_like_a_name(chunk):
    c = _strip_name(chunk)
    if not c or re.search(r"\d", c) or len(c) > 45:
        return False
    if _initials(c):
        return True
    ws = c.split()
    return 1 <= len(ws) <= 5 and all(w[0].isupper() or w.lower() in _PARTICLES for w in ws)


def _names(text, byline):
    """Read a run of names ("L.Y., J.L. and G.C.") until the first thing that is not a name. Returns
    (seat lists, n name-like chunks, n matched, problems, stopped early).

    A chunk that is not a name as a whole may begin with one ("Tikam Singh Rana. Supporting information ..."): that
    name is kept and the run ends. Names in a later ';' clause that runs on into other text are the subject of a
    sentence, not of the role ("supervision A.V. and S.B.; Z.G. and M.D. performed the analysis"): they are dropped."""
    seats, n, ok, problems = [], 0, 0, []
    clause, clause_start = 0, 0
    parts = re.split("(" + _SEP.pattern + ")", text[:_MAX_NAMES_RUN], flags=re.I)
    sep = ""
    for k in range(0, len(parts), 2):
        ch = parts[k]
        if k > 0:
            sep = parts[k - 1]
        if re.search(r"[;；]", sep):
            clause, clause_start = clause + 1, len(seats)
        if not ch or not ch.strip(" .:"):
            continue

        def stop():
            if clause > 0:
                del seats[clause_start:]
            return seats, n, ok, problems, True

        s, why = byline.match(ch)
        if s is not None:
            n += 1
            ok += 1
            seats.append(s)
            continue
        # several names with no separator ("P.A. I.V.A"), or a name then other text ("MK FR contributed to ...")
        words = ch.split()
        got, i = [], 0
        while i < len(words):
            for j in range(min(len(words), i + 6), i, -1):
                s2, _ = byline.match(" ".join(words[i:j]))
                if s2 is not None and s2 != "ALL":   # "All authors have accepted ..." is a sentence, not a name
                    got.append(s2)
                    i = j
                    break
            else:
                break
        if got and i == len(words):
            n += 1
            ok += 1
            seats += got
            continue
        if got:   # names, then text: only the first name is the run's; the rest may start the sentence
            n += 1
            ok += 1
            seats.append(got[0])
            return stop()
        if why == "ambiguous" or (_looks_like_a_name(ch) and _initials(_strip_name(ch))):
            # initials that fit nobody ("S.Z") or two people: counted, and the list goes on
            n += 1
            problems.append((ch.strip()[:60], why))
            continue
        # anything else ends the run: a role outside CRediT ("Experiment, Y.K., ..."), a sentence, a reference
        return stop()
    return seats, n, ok, problems, False


@dataclass
class Result:
    status: str
    roles: dict = field(default_factory=dict)
    layout: str = ""
    stats: dict = field(default_factory=dict)
    statement: str = ""


def find_statement(text):
    """(start, end) of the contribution statement: after the last heading that comes before the first CRediT
    writing term, up to the next back-matter heading. None when there is no heading close enough."""
    m = re.search(r"original\s*dra(?:ft)?(?![a-z])|review(?:ing)?[\s,]*(?:&|and)?[\s,]*editing", text, re.I)
    if not m:
        return None
    heads = [h for h in _HEADING.finditer(text, max(0, m.start() - 3000), m.start())]
    if not heads:
        return None
    start = heads[-1].end()
    stop = _STOP.search(text, start, min(len(text), start + _MAX_STATEMENT))
    end = stop.start() if stop else min(len(text), start + _MAX_STATEMENT)
    return (start, end) if end > start else None


def _clean(st):
    st = _LOOSE_ACCENT.sub("", st).replace("�", ":")
    st = re.sub(r"\.(?=[A-Z][a-z])", ". ", st)   # "Writing - original draft.Hongbo Liu:" -> ". Hongbo Liu:"
    return st


def _terms(st):
    """CRediT terms in the statement. "Original draft" and "review and editing" count only as writing roles: with
    "Writing" in the term or just before it ("Writing (original draft, review and editing)"), never alone ("critical
    review and editing of the manuscript")."""
    out = []
    for m in _TERM_RE.finditer(st):
        slug = _SLUG_BY_GROUP[m.lastgroup]
        if slug.startswith("writing-") and not m.group(0).lower().startswith("writ") \
                and "writ" not in st[max(0, m.start() - 40):m.start()].lower():
            continue
        out.append((m.start(), m.end(), slug))
    return out


_SEP_ONLY = re.compile(r"[\s,;:.&/+\-–—()\[\]•·*]*(?:\b(?:and|und|as well as)\b[\s,;:.&/\-–—()]*)*", re.I)


def _is_separator(s):
    return _SEP_ONLY.fullmatch(_DEGREE.sub(" ", s)) is not None


def _assign(roles, seats, slugs, byline):
    for s in seats:
        for idx in ([x.idx for x in byline.seats] if s == "ALL" else s):
            roles.setdefault(idx, set()).update(slugs)


def _parse_term_first(st, terms, byline):
    """'Conceptualization, L.Y. and J.L.; data curation, G.C.; ...' and 'Conceptualization: Yao Wang. Data ...'."""
    roles, pend = {}, []
    stats = {"chunks": 0, "matched": 0, "problems": [], "skipped_lead": 0, "dropped_empty": 0}
    lead = st[:terms[0][0]]
    if not _is_separator(lead):
        # an opening clause in a role outside CRediT ("Concept, N.O.;") is skipped
        if not re.search(r"[;.]\s*$", lead) or len(lead) > 200:
            return None, stats
        stats["skipped_lead"] = 1
    for i, (a, b, slug) in enumerate(terms):
        pend.append(slug)
        nxt = terms[i + 1][0] if i + 1 < len(terms) else len(st)
        seg = _DEGREE.sub(" ", st[b:nxt])
        if _is_separator(seg) and i + 1 < len(terms):
            if ":" in seg or "." in seg:
                # "Methodology: Visualization: X" is a table row with its names lost, not a shared list
                stats["dropped_empty"] += len(pend)
                pend = []
            continue  # "Conceptualization, methodology, J.Z." : the next names cover both
        seg = seg.strip()
        if not re.match(r"^[),:–—\-]|^\(?[A-Z]|^all\b|^each\b", seg, re.I):
            break
        seg = re.sub(r"^[),:–—\-\s]+", "", seg)
        seats, n, ok, problems, stopped = _names(seg, byline)
        stats["chunks"] += n
        stats["matched"] += ok
        stats["problems"] += problems
        if not seats and n == 0:
            break
        _assign(roles, seats, pend, byline)
        pend = []
    return roles, stats


_NAME_END = re.compile(r":|\s[-–—]\s?(?=[A-Za-z])")


def _parse_name_first(st, terms, byline):
    """'Dawei Li: Data curation (equal); Writing – original draft. Hongbo Liu: Funding acquisition ...' and
    'Oto Hanuš -Conceptualization, Funding acquisition'."""
    roles = {}
    stats = {"chunks": 0, "matched": 0, "problems": [], "unknown_items": 0, "entry_cut": 0}
    spans = [(a, b) for a, b, _ in terms]
    # an entry starts where a name group ends in ':' (or ' - ') and a CRediT term follows
    ends = []
    for m in _NAME_END.finditer(st):
        if any(a <= m.start() < b for a, b in spans):
            continue
        after = st[m.end():]
        first = next((t for t in terms if t[0] >= m.end()), None)
        if first and _is_separator(st[m.end():first[0]]) and first[0] - m.end() < 20:
            ends.append(m)
    if not ends:
        return None, stats
    prev = 0
    entries = []
    for m in ends:
        last_term_end = max([b for a, b in spans if prev <= a and b <= m.start()], default=prev)
        entries.append((st[last_term_end:m.start()], m.end(), last_term_end))
        prev = m.end()
    for k, (group, rstart, _) in enumerate(entries):
        rend = entries[k + 1][2] if k + 1 < len(entries) else len(st)
        seg_terms = [t for t in terms if rstart <= t[0] and t[1] <= rend]
        if not seg_terms:
            break
        # Text between two terms that is not a separator is either a role outside CRediT ("Revision": dropped, the
        # list goes on) or the next person written without a colon ("MS Data collection, Methodology": the list
        # ends there, since the terms after it are that person's).
        kept = []
        for (a, b, sl), nxt in zip(seg_terms, seg_terms[1:] + [None]):
            kept.append(sl)
            gap = st[b:nxt[0]] if nxt else ""
            g = _DEGREE.sub(" ", gap)
            if "." in g:  # a sentence or another person's entry starts: the list has ended
                stats["entry_cut"] += 1
                break
            if gap and not _is_separator(gap):
                if _holds_a_name(gap, byline) or len(g.strip()) > 40 or re.search(r"\d", g):
                    stats["entry_cut"] += 1
                    break
                stats["unknown_items"] += 1
        group = _DEGREE.sub(" ", group).strip(" .;,\t")
        if k == 0 and len(group) > 300:
            return None, stats
        seats, n, ok, problems, stopped = _names(group, byline)
        if stopped:   # text before the names, e.g. a sub-heading "Name(s): role(s)": take the names at its end
            seats, n, ok, problems = _names_from_end(group, byline)
        stats["chunks"] += n
        stats["matched"] += ok
        stats["problems"] += problems
        _assign(roles, seats, kept, byline)
    return roles, stats


def _names_from_end(group, byline):
    """The longest run of names that ends the text (for a name group with something written before it)."""
    chunks = [c for c in _SEP.split(group) if c and c.strip(" .:")]
    best = ([], 0, 0, [])
    for k in range(len(chunks)):
        seats, n, ok, problems, stopped = _names(", ".join(chunks[k:]), byline)
        if not stopped and n and ok == n:
            return seats, n, ok, problems
    # the last chunk may itself hold text then a name ("role(s) Oto Hanuš")
    if chunks:
        words = chunks[-1].split()
        for i in range(len(words)):
            s, _ = byline.match(" ".join(words[i:]))
            if s is not None and s != "ALL":
                return [s], 1, 1, []
    return best


def _holds_a_name(text, byline):
    """Does any run of 1-4 words in the text name exactly one author on the byline?"""
    words = [w for w in re.split(r"[\s,;:]+", _DEGREE.sub(" ", text)) if w and re.search(r"[A-Za-z]", w)]
    for i in range(len(words)):
        for j in range(i + 1, min(len(words), i + 4) + 1):
            s, _ = byline.match(" ".join(words[i:j]))
            if s is not None and s != "ALL":
                return True
    return False


def parse(text, byline_names, min_match=0.8):
    byline = Byline(byline_names)
    if not byline.seats:
        return Result("no_byline")
    span = find_statement(text or "")
    if not span:
        return Result("no_statement")
    st = _clean(text[span[0]:span[1]])
    terms = _terms(st)
    if not terms:
        return Result("no_terms", statement=st)
    best = None
    for layout, fn in (("term_first", _parse_term_first), ("name_first", _parse_name_first)):
        roles, stats = fn(st, terms, byline)
        if roles:
            n_pairs = sum(len(v) for v in roles.values())
            if best is None or n_pairs > best[3]:
                best = (layout, roles, stats, n_pairs)
    if best is None:
        return Result("no_layout", statement=st)
    layout, roles, stats, _ = best
    # names that fit two authors ("J.Z." for Jun Zhang and Jie Zhao) are left out, but they do fit this byline, so they
    # count as fitting for the check that the statement belongs to this paper
    chunks = stats["chunks"]
    fits = stats["matched"] + sum(1 for _, why in stats["problems"] if why == "ambiguous")
    if chunks == 0 or fits / chunks < min_match:
        return Result("names_off_byline", layout=layout, stats=stats, statement=st)
    out = {i: [s for s in CREDIT_ROLES if s in v] for i, v in sorted(roles.items()) if v}
    return Result("ok", out, layout, stats, st)
