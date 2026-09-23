"""Study design tagger: the shared logic for notebooks/study_design/* (oxjob #1312).

Pure Python (requests only, no Spark) so the notebook and the local
certification check (oxjobs #1312 scratch/prod_check.py) run byte-identical
request builders, derivations, gates, thresholds and PubMed mappings.

What ships (certified 2026-09-22 on 5,069 Opus-judged works, oxjob #1312
EXPLORE § 5b): one Jev request per work (13-option Choice + two one-clause
Nouls) over title + venue + abstract, eight derived class scores, three
code-side text gates on RCT, per-class thresholds fixed on the dev split.
RCT: 0 false positives in 555 at 0.83 recall; every class over its bar.

Provenance rule (served table): where a MEDLINE-indexed PubMed record carries
a study-characteristics tag, PubMed's values are served; otherwise the
tagger's. Both are stored so disagreement is measurable. The tagger is never
named in the API or docs ("automated tagging").

Never batch several works into one request (7% of answers flip at N = 8).
Never add Nouls to the request without re-certifying: four times an extra
gating Noul perturbed every other answer.
"""
from __future__ import annotations

import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

import requests

# ---------------------------------------------------------------------------
# Versions and prices
# ---------------------------------------------------------------------------

JEV_URL = "https://api.typesafe.ai/v1/systemone"
JEV_MODEL = "jev-1.13.0"                     # pinned; jev-latest moves and shifts calibration (#1305)
RUBRIC_VERSION = "rubric-v2/2026-09-22"
CONFIG_NAME = "r2_gate4"                     # oxjobs #1312 scratch/harness/configs/r2_gate4.py
TAGGER_VERSION = f"{CONFIG_NAME}/{JEV_MODEL}"  # checkpoint key: a row counts only if it carries this
JEV_USD_PER_TOKEN = 0.042 / 1e6              # input only; output free
RETRYABLE = {429, 500, 502, 503, 529}

# ---------------------------------------------------------------------------
# The request (== configs/r2_shortnoul.py: rubric v2 texts + v1's short Nouls)
# ---------------------------------------------------------------------------

RULE = ("Classify the study design of this paper from its title and abstract. Judge only what this paper itself reports and "
        "states; ignore designs it cites, discusses or builds on. Answer unknown unless the design is stated or unambiguous "
        "from the described methods. A paper that comments on, summarises or appraises another study or review is "
        "editorial_letter or narrative_review, not the design it discusses.")

DESIGN = {
    "rct": ("randomized controlled trial: this paper reports results (primary, updated, follow-up or extension) of a trial "
            "that explicitly randomized human participants, clusters or treatment periods to compared interventions; "
            "randomized crossover trials count"),
    "nonrandomized_trial": ("interventional trial in which investigators prospectively assigned an intervention to human "
                            "participants without stated randomization (single-arm, phase I/II, controlled before-after, or "
                            "blinded / placebo-controlled with no mention of randomization) and report its results"),
    "secondary_analysis": ("secondary, post-hoc, subgroup, exploratory or pooled analysis of data from an earlier trial or "
                           "cohort; not the primary or updated results report of that study"),
    "observational": ("observational study of human participants with no investigator-assigned intervention: cohort, "
                      "case-control, cross-sectional, registry, survey, genetic association, or a descriptive series of "
                      "more than about ten patients who received routine care"),
    "case_report": ("case report or small case series: one to about ten patients described individually, no comparison "
                    "group, no cohort statistics"),
    "systematic_review": ("systematic review: this paper itself reports a systematic literature search and study selection "
                          "in any field, without pooled quantitative meta-analysis"),
    "meta_analysis": ("meta-analysis: this paper itself pools quantitative results across published studies in any field; "
                      "pooling participant-level or summary data across cohorts (e.g. GWAS) is observational instead"),
    "narrative_review": ("narrative, expert or 'comprehensive' review without a reported systematic search; also summaries, "
                         "evidence updates or appraisals of others' reviews or trials"),
    "editorial_letter": "editorial, commentary, letter, opinion, correspondence, or a commentary appraising another paper",
    "protocol": ("protocol for a planned study with human or animal participants: planned methods, no results yet "
                 "(not a technical, monitoring or mission plan)"),
    "guideline": "clinical practice guideline or consensus recommendations",
    "other_primary_research": ("primary research whose units are not human patients or participants: laboratory, in vitro, "
                               "animal, agricultural, computational, methods, qualitative fieldwork, tool or program "
                               "development, analyses of firms, documents, texts or ecosystems; also lab experiments on "
                               "volunteers that only randomize the order of stimuli"),
    "unknown": "cannot tell from title and abstract",
}

# v1's one-clause Nouls. Long Nouls with caveats cost 10-16 points of RCT recall (three times).
NOULS = {
    "is_rct": "Is this paper itself a randomized controlled trial (not a protocol, not a secondary analysis of one)?",
    "human_subjects": "Does this paper report data collected from human participants or patients?",
}

ABSTRACT_CHARS = 6000


def questions() -> dict:
    q = {"design": {"type": "choice", "instructions": "Which study design best describes this paper?", "criteria": DESIGN}}
    for k, v in NOULS.items():
        q[k] = {"type": "noul", "instructions": v}
    return q


def state_for(w: dict) -> dict:
    """w: {title, venue, abstract}. Key order matters (it is what was certified)."""
    s = {"rule_design": RULE, "title": w.get("title") or ""}
    if w.get("venue"):
        s["venue"] = w["venue"]
    if w.get("abstract"):
        s["abstract"] = w["abstract"][:ABSTRACT_CHARS]
    return s


# ---------------------------------------------------------------------------
# Derivation (== configs/v1.derive -> r2_base.derive -> r2_gate4 gates)
# ---------------------------------------------------------------------------

CLASSES = ["rct", "clinical_trial", "observational", "case_report", "systematic_review", "meta_analysis", "protocol",
           "other_primary_research"]

# Dev-fixed thresholds (EXPLORE § 5b). Protocol: dev picked 0.74 with zero protocol FPs; the pooled curve clears
# the 0.97 bar at 0.90, which is what ships.
THRESHOLDS = {"rct": 0.90, "meta_analysis": 0.97, "systematic_review": 0.88, "protocol": 0.90, "clinical_trial": 0.90,
              "case_report": 0.97, "observational": 0.82, "other_primary_research": 0.30}

# Served value ids (API slugs) and the implied parents.
VALUE_ID = {
    "rct": "randomized-controlled-trial",
    "clinical_trial": "clinical-trial",
    "observational": "observational-study",
    "case_report": "case-report",
    "systematic_review": "systematic-review",
    "meta_analysis": "meta-analysis",
    "protocol": "study-protocol",
    "other_primary_research": "other-primary-research",
}
PARENT = {"rct": "clinical_trial", "meta_analysis": "systematic_review"}

# Code-side text gates (== harness/textsig.py). Under rubric v2 an RCT must state random allocation, so the
# stated-random regex is a hard gate, not a hint.
RAND = re.compile(r"randomi[sz]|randomly|at random|random(?:ized|ised|isation|ization)?\b|aleatori[sz]|aleat[oó]ri|randomisiert|randomisé|"
                  r"gerandomiseerd|randomiser|随机|ランダム|無作為|무작위|рандомиз|случайн|losow", re.I)
SIM = re.compile(r"\b(simulat(?:ion|or|ed)|manikin|mannequin|phantom|cadaver(?:ic|s)?|bench(?:top)?\s+(?:study|model)|in vitro)\b", re.I)
SECONDARY = re.compile(r"\b(secondary|post[- ]?hoc|exploratory|subgroup|sub-study|substudy|ancillary|pooled|mediation|moderat(?:or|ion))\s+analys|"
                       r"\b(safety|secondary|data|lessons|insights|analysis)\s+from\s+(?:a|the|two|three|\d+)\s+randomi|"
                       r"\bparticipants\s+(?:of|in|from)\s+(?:a|the)\s+randomi|\bin\s+the\s+[A-Z][A-Za-z0-9-]+\s+trial\b", re.I)


def stated_random(w: dict) -> bool:
    return bool(RAND.search((w.get("title") or "") + " " + (w.get("abstract") or "")))


def simulation_title(w: dict) -> bool:
    return bool(SIM.search(w.get("title") or ""))


def secondary_title(w: dict) -> bool:
    return bool(SECONDARY.search(w.get("title") or ""))


def derive(a: dict, w: dict) -> dict:
    """a = Jev answers. Returns {class: score in [0, 1]} for the eight shipped classes, gates applied."""
    pr = a["design"]["probabilities"]
    rct = min(a["is_rct"]["noul"], 1.0) if a["human_subjects"]["noul"] >= 0.5 else 0.0
    d = {
        "rct": rct,
        "clinical_trial": max(pr.get("rct", 0), pr.get("nonrandomized_trial", 0)),
        "observational": pr.get("observational", 0),
        "case_report": pr.get("case_report", 0),
        "systematic_review": max(pr.get("systematic_review", 0), pr.get("meta_analysis", 0)),
        "meta_analysis": pr.get("meta_analysis", 0),
        "protocol": pr.get("protocol", 0),
        "other_primary_research": pr.get("other_primary_research", 0),
    }
    sec = pr.get("secondary_analysis", 0)
    d["rct"] = d["rct"] * (1 - sec)
    d["clinical_trial"] = d["clinical_trial"] * (1 - sec)
    if not stated_random(w) or simulation_title(w) or secondary_title(w):
        d["rct"] = 0.0
    return d


def classes_at_thresholds(scores: dict) -> list[str]:
    """Internal class names that clear their threshold, parents added, in CLASSES order."""
    hit = {c for c in CLASSES if scores.get(c, 0.0) >= THRESHOLDS[c]}
    for c in list(hit):
        if c in PARENT:
            hit.add(PARENT[c])
    return [c for c in CLASSES if c in hit]


def tagger_values(scores: dict) -> list[str]:
    return [VALUE_ID[c] for c in classes_at_thresholds(scores)]


# ---------------------------------------------------------------------------
# PubMed provenance: MeSH V03 (Study Characteristics) tags -> served values
# ---------------------------------------------------------------------------

# README schema table. Formats (Editorial, Letter, Review, Guideline ...) are `type`'s business; Scoping Review and
# the modifier tags (Comparative, Multicenter, Evaluation, Validation Study) are out.
PUBMED_MAP = {
    "Randomized Controlled Trial": "rct",
    "Randomized Controlled Trial, Veterinary": "rct",
    "Pragmatic Clinical Trial": "rct",
    "Equivalence Trial": "rct",
    "Adaptive Clinical Trial": "rct",
    "Clinical Trial": "clinical_trial",
    "Controlled Clinical Trial": "clinical_trial",
    "Clinical Trial, Phase I": "clinical_trial",
    "Clinical Trial, Phase II": "clinical_trial",
    "Clinical Trial, Phase III": "clinical_trial",
    "Clinical Trial, Phase IV": "clinical_trial",
    "Clinical Study": "clinical_trial",
    "Clinical Trial, Veterinary": "clinical_trial",
    "Observational Study": "observational",
    "Observational Study, Veterinary": "observational",
    "Twin Study": "observational",
    "Case Reports": "case_report",
    "Systematic Review": "systematic_review",
    "Meta-Analysis": "meta_analysis",
    "Network Meta-Analysis": "meta_analysis",
    "Clinical Trial Protocol": "protocol",
}


def pubmed_classes(pub_types: list[str] | None) -> list[str]:
    """Internal class names implied by a record's PublicationType list (parents added), or [] if none map."""
    hit = {PUBMED_MAP[t] for t in (pub_types or []) if t in PUBMED_MAP}
    for c in list(hit):
        if c in PARENT:
            hit.add(PARENT[c])
    return [c for c in CLASSES if c in hit]


def pubmed_values(pub_types: list[str] | None) -> list[str]:
    return [VALUE_ID[c] for c in pubmed_classes(pub_types)]


def sql_pubmed_case() -> str:
    """The same mapping as a SQL CASE over one exploded tag, for the served-table rebuild."""
    lines = []
    for t, c in PUBMED_MAP.items():
        lines.append(f"WHEN '{t}' THEN '{VALUE_ID[c]}'")
    return "CASE t " + " ".join(lines) + " END"


# ---------------------------------------------------------------------------
# Jev client: threads + requests, paced on requests/s (the driver runs this)
# ---------------------------------------------------------------------------

def _backoff(attempt: int) -> float:
    return min(30.0, 0.5 * 2 ** attempt) * (0.5 + random.random())


class Pacer:
    """Token bucket on requests/s shared by all threads. Jev's caps: 400 req/s, 1.2M tok/s (2026-09-21)."""

    def __init__(self, rps: float):
        self._interval = 1.0 / max(rps, 0.001)
        self._next = time.perf_counter()
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.perf_counter()
            self._next = max(self._next, now)
            delay = self._next - now
            self._next += self._interval
        if delay > 0:
            time.sleep(delay)


class JevClient:
    """Native TypeSafe API. One request per work. Retries 429/5xx/transport with jittered backoff."""

    def __init__(self, api_key: str, concurrency: int = 64, rps: float = 250.0, timeout: float = 60.0,
                 max_attempts: int = 6, model: str = JEV_MODEL):
        if not api_key:
            raise RuntimeError("Jev api_key missing")
        self._headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        self._concurrency = concurrency
        self._pacer = Pacer(rps)
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._model = model
        self._local = threading.local()
        self._lock = threading.Lock()
        self.total_tokens = 0
        self.n_ok = 0
        self.n_fail = 0
        self.n_retry = 0

    @property
    def usd(self) -> float:
        return self.total_tokens * JEV_USD_PER_TOKEN

    def _session(self) -> requests.Session:
        s = getattr(self._local, "s", None)
        if s is None:
            s = requests.Session()
            self._local.s = s
        return s

    def decide(self, state: Any, qs: dict) -> dict:
        body = {"model": self._model, "state": state, "questions": qs}
        attempt = 0
        while True:
            attempt += 1
            self._pacer.wait()
            t0 = time.perf_counter()
            try:
                resp = self._session().post(JEV_URL, headers=self._headers, json=body, timeout=self._timeout)
                ms = (time.perf_counter() - t0) * 1000
                if resp.status_code == 200:
                    data = resp.json()
                    tok = int((data.get("usage") or {}).get("input_tokens") or 0)
                    with self._lock:
                        self.total_tokens += tok
                        self.n_ok += 1
                    return {"ok": True, "answers": data.get("answers", {}), "model": data.get("model"),
                            "input_tokens": tok, "latency_ms": round(ms), "attempts": attempt}
                if resp.status_code in RETRYABLE and attempt < self._max_attempts:
                    with self._lock:
                        self.n_retry += 1
                    ra = resp.headers.get("retry-after")
                    time.sleep(float(ra) if ra else _backoff(attempt))
                    continue
                with self._lock:
                    self.n_fail += 1
                return {"ok": False, "status": resp.status_code, "error": resp.text[:300], "attempts": attempt}
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt < self._max_attempts:
                    with self._lock:
                        self.n_retry += 1
                    time.sleep(_backoff(attempt))
                    continue
                with self._lock:
                    self.n_fail += 1
                return {"ok": False, "status": 0, "error": repr(e)[:300], "attempts": attempt}

    def tag_many(self, works: list[dict], on_result: Callable[[dict, dict], None] | None = None) -> list[tuple[dict, dict]]:
        """works: dicts with work_id, title, venue, abstract. Returns [(work, result)] in completion order."""
        qs = questions()
        out = []
        with ThreadPoolExecutor(self._concurrency) as ex:
            futs = {ex.submit(self.decide, state_for(w), qs): w for w in works}
            for f in as_completed(futs):
                w = futs[f]
                try:
                    r = f.result()
                except Exception as e:  # never lose the chunk to one bad thread
                    r = {"ok": False, "status": -1, "error": repr(e)[:300], "attempts": 0}
                    with self._lock:
                        self.n_fail += 1
                out.append((w, r))
                if on_result:
                    on_result(w, r)
        return out


# ---------------------------------------------------------------------------
# Rows for the tagger table
# ---------------------------------------------------------------------------

def answer_row(w: dict, r: dict) -> dict | None:
    """One works_study_design_tagger row from a successful Jev result, or None if the answer is malformed."""
    a = r.get("answers") or {}
    try:
        probs = {k: float(v) for k, v in a["design"]["probabilities"].items()}
        is_rct = float(a["is_rct"]["noul"])
        human = float(a["human_subjects"]["noul"])
    except (KeyError, TypeError, ValueError):
        return None
    scores = derive(a, w)
    return {
        "work_id": int(w["work_id"]),
        "tagger_values": tagger_values(scores),
        "scores": {k: float(v) for k, v in scores.items()},
        "probabilities": probs,
        "is_rct": is_rct,
        "human_subjects": human,
        "stated_random": stated_random(w),
        "abstract_chars": len(w.get("abstract") or ""),
        "input_tokens": int(r.get("input_tokens") or 0),
        "tagger_version": TAGGER_VERSION,
        "jev_model": r.get("model") or JEV_MODEL,
    }

# ---------------------------------------------------------------------------
# Student (oxjob #1335): a fine-tuned encoder that reproduces Jev's outputs; owns the works it is sure about
# ---------------------------------------------------------------------------

# The student emits the same 13 Choice probabilities + 2 Nouls as Jev, so derive() and the gates run unchanged.
# Per class: STUDENT_TAU_POS = its threshold certified on the judged dev split at the class bar (oxjob #1335 EXPLORE § 3);
# STUDENT_TAU_NEG = the score under which its negatives lose <= 2% of Jev's positives (RCT 0.5%) on 78K held-out Jev labels
# (§ 5). A work is the student's when every class score is outside (TAU_NEG, TAU_POS); the rest go to Jev. Classes the
# student does not own (RCT; Observational by the recall rule) have no TAU_POS: any score >= TAU_NEG routes the work to Jev.
STUDENT_ARM = "e5s"                                   # multilingual-e5-small, 512 tokens, 1 epoch over 1.5M Jev labels
STUDENT_VERSION = f"student-{STUDENT_ARM}-v1+{CONFIG_NAME}"   # checkpoint key for student-tagged rows (jev_model NULL)
STUDENT_MODEL_DIR = f"/Volumes/openalex/works/models/study_design/{STUDENT_ARM}"
STUDENT_TAU_POS = {"clinical_trial": 0.86, "observational": 0.87, "meta_analysis": 0.99, "systematic_review": 0.94, "case_report": 0.98, "other_primary_research": 0.3, "protocol": 0.65}
STUDENT_TAU_NEG = {"rct": 0.629, "meta_analysis": 0.931, "systematic_review": 0.719, "protocol": 0.643, "clinical_trial": 0.643, "case_report": 0.875, "observational": 0.425, "other_primary_research": 0.115}
TAGGER_VERSIONS = (TAGGER_VERSION, STUDENT_VERSION)   # a work is tagged if it has a row at either


def student_route(scores: dict) -> str:
    """'student' when no class score sits in its residual band, else 'jev'. scores = derive() output (gates applied)."""
    for c in CLASSES:
        s = scores.get(c, 0.0)
        tp = STUDENT_TAU_POS.get(c)
        if tp is None:
            if s >= STUDENT_TAU_NEG[c]:
                return "jev"
        elif STUDENT_TAU_NEG[c] <= s < tp:
            return "jev"
    return "student"


def student_classes(scores: dict) -> list[str]:
    """Owned classes clearing their certified threshold, parents added, in CLASSES order."""
    hit = {c for c, tp in STUDENT_TAU_POS.items() if scores.get(c, 0.0) >= tp}
    for c in list(hit):
        if c in PARENT:
            hit.add(PARENT[c])
    return [c for c in CLASSES if c in hit]


def student_values(scores: dict) -> list[str]:
    return [VALUE_ID[c] for c in student_classes(scores)]


def student_answer_row(w: dict, probs: dict, is_rct: float, human: float) -> dict:
    """One works_study_design_tagger row from the student's outputs (only meaningful when student_route == 'student')."""
    a = {"design": {"probabilities": probs}, "is_rct": {"noul": float(is_rct)}, "human_subjects": {"noul": float(human)}}
    scores = derive(a, w)
    return {
        "work_id": int(w["work_id"]),
        "tagger_values": student_values(scores),
        "scores": {k: float(v) for k, v in scores.items()},
        "probabilities": {k: float(v) for k, v in probs.items()},
        "is_rct": float(is_rct),
        "human_subjects": float(human),
        "stated_random": stated_random(w),
        "abstract_chars": len(w.get("abstract") or ""),
        "input_tokens": 0,
        "tagger_version": STUDENT_VERSION,
        "jev_model": None,
    }


def sql_versions() -> str:
    return ", ".join(f"'{v}'" for v in TAGGER_VERSIONS)
