"""Jev quality sample: shared logic for notebooks/metrics/JevQualitySample.py (oxjob #1301).

Pure Python (requests only, no Spark) so the notebook and the local dry run
(oxjobs #1301 scratch/dryrun_notebook.py) run the same SQL builders, question
shapes, state builders, p_wrong readers, tail selection and aggregation.

Five strata, one night each, judged by Jev (jev-1.13.0, native API) with a
capped Opus 5 tail on the highest-confidence Jev flags. Thresholds and the
per-stratum p_wrong reading come from the 2026-09-21 probe (EXPLORE.md in the
job): Jev's flags are 0.75-1.00 precise at these cuts, its recall against Opus
is 35-85%, so the numbers are a drift signal, not an error rate.
"""
from __future__ import annotations

import json
import random
import re
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

import requests

STRATA = ["a_type", "b_affil", "c_merge", "d_repo", "e_auth"]

# p_wrong cut per stratum (EXPLORE.md § Thresholds): highest cut that keeps the
# best recall with false-alarm share <= 0.3.
JEV_CUT = {"a_type": 0.5, "b_affil": 0.7, "c_merge": 0.6, "d_repo": 0.5, "e_auth": 0.6}

# Strata whose flags go to the Opus tail. (b) is excluded by default: Jev is
# 0.97-1.00 precise there and its 35% flag rate would eat the whole cap.
TAIL_STRATA_DEFAULT = ["a_type", "c_merge", "d_repo", "e_auth"]

JUNK_KINDS = {"periodical_issue_or_newspaper", "image_or_object", "supplementary_component",
              "index_or_toc_page", "announcement_or_call"}

JEV_URL = "https://api.typesafe.ai/v1/systemone"
JEV_MODEL = "jev-1.13.0"                 # pinned; jev-latest moves
JEV_USD_PER_TOKEN = 0.042 / 1e6           # input only; output free
JEV_CHARS_PER_TOKEN = 1.65                # measured; char/4 estimates are 2x low
RETRYABLE = {429, 500, 502, 503, 529}

OPUS_MODEL = "databricks-claude-opus-5"   # FMAPI pay-per-token; the probe judged with claude-opus-5
OPUS_IN_USD, OPUS_OUT_USD = 5.0, 25.0     # list price per 1M tokens (Anthropic list; FMAPI bills the same)
OPUS_CHARS_PER_TOKEN = 4.0
OPUS_ASSUMED_OUT_TOKENS = 120

# ---------------------------------------------------------------------------
# SQL builders. Each returns one SELECT over one night; columns are the
# contract build_state() reads. Sampling is deterministic (xxhash64 of the id).
# ---------------------------------------------------------------------------

LWT = "openalex.works.locations_w_types"
LWI = "openalex.works.location_work_ids"
SOURCES = "openalex.sources.sources"
LOOKUP = "openalex.institutions.affiliation_strings_lookup"
INST = "openalex.institutions.institutions"
REPO_ENRICHED = "openalex.repo.repo_enriched"
PENDING = "openalex.authors.pending_author_assignments"
BATCH = "openalex.authors.author_matching_batch"
AFM = "openalex.authors.authors_for_matching"
WB = "openalex.works.openalex_works_base"


def sql_night_counts(table: str, date_expr: str, snapshot: str, days_back: int = 4) -> str:
    """Rows per night for the last few nights; the caller picks the latest full one."""
    return f"""
SELECT {date_expr} AS night, COUNT(*) AS n
FROM {table}
WHERE {date_expr} BETWEEN DATE '{snapshot}' - INTERVAL {days_back} DAYS AND DATE '{snapshot}'
GROUP BY 1 ORDER BY 1 DESC"""


def sql_a_type(night: str, per_rule: int = 30, cap: int = 3000) -> str:
    return f"""
WITH keys AS (
  SELECT provenance, native_id_namespace, native_id, classified_rule,
         ROW_NUMBER() OVER (PARTITION BY classified_rule
                            ORDER BY xxhash64(CONCAT(provenance, ':', native_id_namespace, ':', native_id))) AS rn,
         COUNT(*) OVER (PARTITION BY classified_rule) AS n_rule
  FROM {LWT}
  WHERE updated_date = DATE '{night}' AND classified_rule IS NOT NULL
),
picked AS (
  SELECT * FROM keys WHERE rn <= {per_rule}
  ORDER BY rn, xxhash64(CONCAT(provenance, ':', native_id_namespace, ':', native_id))
  LIMIT {cap}
)
SELECT l.provenance, l.native_id_namespace, l.native_id, l.title, SUBSTR(l.abstract, 1, 1400) AS abstract,
       COALESCE(l.source_name, s.display_name) AS venue, l.publisher, l.raw_type, l.type AS assigned_type,
       l.classified_rule, l.language, YEAR(l.published_date) AS year, l.issue, l.first_page, l.last_page,
       p.n_rule
FROM picked p
JOIN {LWT} l ON l.provenance = p.provenance AND l.native_id_namespace = p.native_id_namespace
             AND l.native_id = p.native_id AND l.updated_date = DATE '{night}'
LEFT JOIN {SOURCES} s ON s.id = l.source_id"""


def sql_b_affil(night: str, cap: int = 3000) -> str:
    return f"""
WITH pairs AS (
  SELECT a.raw_affiliation_string, t.inst_id, SIZE(a.institution_ids) AS n_inst,
         SIZE(a.institution_ids_override) > 0 AS has_override,
         TRY_ELEMENT_AT(a.model_response, 1).score AS top_score,
         TRY_ELEMENT_AT(FILTER(a.model_response, m -> m.id = CAST(t.inst_id AS STRING)), 1).score AS inst_score
  FROM {LOOKUP} a
  LATERAL VIEW EXPLODE(a.institution_ids) t AS inst_id
  WHERE DATE(a.created_datetime) = DATE '{night}' AND t.inst_id > 0
)
SELECT p.raw_affiliation_string, p.inst_id, p.n_inst, p.has_override, p.top_score, p.inst_score,
       i.display_name, i.display_name_acronyms, i.display_name_alternatives, i.country, i.city, i.type AS inst_type
FROM pairs p JOIN {INST} i ON i.id = p.inst_id
ORDER BY xxhash64(CONCAT(p.raw_affiliation_string, '|', CAST(p.inst_id AS STRING)))
LIMIT {cap}"""


def sql_c_merge(night: str, cap: int = 2000) -> str:
    return f"""
WITH new_ta AS (
  SELECT work_id, provenance, native_id_namespace, native_id
  FROM {LWI}
  WHERE openalex_created_dt = DATE '{night}' AND work_id_source = 'title_author'
),
other AS (
  SELECT n.work_id, n.provenance p1, n.native_id_namespace ns1, n.native_id id1,
         o.provenance p2, o.native_id_namespace ns2, o.native_id id2, o.openalex_created_dt dt2, o.work_id_source src2,
         ROW_NUMBER() OVER (PARTITION BY n.provenance, n.native_id_namespace, n.native_id
                            ORDER BY o.openalex_created_dt, xxhash64(o.native_id)) AS rn
  FROM new_ta n
  JOIN {LWI} o ON o.work_id = n.work_id
   AND NOT (o.provenance = n.provenance AND o.native_id_namespace = n.native_id_namespace AND o.native_id = n.native_id)
  WHERE o.openalex_created_dt < DATE '{night}'
),
picked AS (
  SELECT * FROM other WHERE rn = 1
  ORDER BY xxhash64(CONCAT(p1, ':', ns1, ':', id1)) LIMIT {cap * 2}
)
SELECT x.work_id, x.p1, x.ns1, x.id1, x.p2, x.ns2, x.id2, CAST(x.dt2 AS STRING) AS dt2, x.src2,
       a.title AS title_a, SLICE(TRANSFORM(a.authors, z -> z.name), 1, 6) AS authors_a, SIZE(a.authors) AS n_authors_a,
       COALESCE(a.source_name, sa.display_name) AS venue_a, YEAR(a.published_date) AS year_a,
       a.type AS type_a, a.best_doi AS doi_a, a.merge_key.title_author AS key_a,
       b.title AS title_b, SLICE(TRANSFORM(b.authors, z -> z.name), 1, 6) AS authors_b, SIZE(b.authors) AS n_authors_b,
       COALESCE(b.source_name, sb.display_name) AS venue_b, YEAR(b.published_date) AS year_b,
       b.type AS type_b, b.best_doi AS doi_b, b.merge_key.title_author AS key_b
FROM picked x
JOIN {LWT} a ON a.provenance = x.p1 AND a.native_id_namespace = x.ns1 AND a.native_id = x.id1
JOIN {LWT} b ON b.provenance = x.p2 AND b.native_id_namespace = x.ns2 AND b.native_id = x.id2
LEFT JOIN {SOURCES} sa ON sa.id = a.source_id
LEFT JOIN {SOURCES} sb ON sb.id = b.source_id
ORDER BY xxhash64(CONCAT(x.p1, ':', x.ns1, ':', x.id1))
LIMIT {cap}"""


def sql_d_repo(night: str, per_endpoint: int = 30, cap: int = 3000) -> str:
    """New repo locations, round-robin over endpoints so one bulk load cannot own the sample."""
    return f"""
WITH keys AS (
  SELECT w.provenance, w.native_id_namespace, w.native_id, w.work_id, w.work_id_source, l.endpoint_id,
         ROW_NUMBER() OVER (PARTITION BY l.endpoint_id ORDER BY xxhash64(w.native_id)) AS rn,
         COUNT(*) OVER (PARTITION BY l.endpoint_id) AS n_endpoint
  FROM {LWI} w
  JOIN {LWT} l ON l.provenance = w.provenance AND l.native_id_namespace = w.native_id_namespace AND l.native_id = w.native_id
  WHERE w.openalex_created_dt = DATE '{night}' AND w.provenance = 'repo'
),
picked AS (
  SELECT * FROM keys WHERE rn <= {per_endpoint}
  ORDER BY rn, xxhash64(native_id) LIMIT {cap}
)
SELECT p.work_id, p.work_id_source, p.native_id, p.native_id_namespace, p.endpoint_id, p.n_endpoint,
       l.title, SUBSTR(l.abstract, 1, 300) AS abstract, s.display_name AS venue, l.raw_type,
       r.raw_native_type, SLICE(r.set_spec, 1, 5) AS set_spec, l.type AS assigned_type, l.classified_rule,
       l.language, YEAR(l.published_date) AS year, TRANSFORM(SLICE(l.urls, 1, 3), u -> u.url) AS urls
FROM picked p
JOIN {LWT} l ON l.provenance = p.provenance AND l.native_id_namespace = p.native_id_namespace AND l.native_id = p.native_id
LEFT JOIN {SOURCES} s ON s.id = l.source_id
LEFT JOIN {REPO_ENRICHED} r ON r.native_id = p.native_id AND r.native_id_namespace = p.native_id_namespace
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.native_id_namespace, p.native_id ORDER BY r.updated_date DESC) = 1"""


def sql_e_auth(per_tier: int = 1250) -> str:
    """Arm A prompt SQL from notebooks/metrics/AuthorshipQualityJudge.py with a bigger per-tier
    quota (that notebook materialises 800/night; this stratum wants ~20K). Keep the two in sync:
    the prompt body is the Jev state and the Opus prompt."""
    return f"""
WITH sample AS (
  SELECT p.work_id, p.author_sequence, p.raw_author_name, p.institution_ids,
         p.work_source_ids, p.existing_author_id,
         CASE WHEN p.match_method = 'orcid' AND p.orcid_blind_match THEN 'orcid_blind'
              WHEN p.match_method = 'orcid' THEN 'orcid'
              ELSE p.name_match_tier END AS tier
  FROM {PENDING} p
  WHERE p.match_outcome = 'MATCHED' AND p.existing_author_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CASE WHEN p.match_method = 'orcid' AND p.orcid_blind_match THEN 'orcid_blind'
                      WHEN p.match_method = 'orcid' THEN 'orcid'
                      ELSE p.name_match_tier END
    ORDER BY xxhash64(CONCAT(CAST(p.work_id AS STRING), ':', CAST(p.author_sequence AS STRING)))
  ) <= {per_tier}
),
cand_ids AS (SELECT DISTINCT existing_author_id AS author_id FROM sample),
coauth AS (
  SELECT s.work_id, s.author_sequence,
         CONCAT_WS('; ', SLICE(COLLECT_LIST(b.raw_author_name), 1, 8)) AS coauthors
  FROM sample s
  JOIN {BATCH} b ON b.work_id = s.work_id AND b.author_sequence <> s.author_sequence
  GROUP BY s.work_id, s.author_sequence
),
inc_inst AS (
  SELECT x.work_id, x.author_sequence,
         CONCAT_WS('; ', SLICE(COLLECT_LIST(i.display_name), 1, 5)) AS inst_names
  FROM (
    SELECT s.work_id, s.author_sequence, iid
    FROM sample s LATERAL VIEW EXPLODE(s.institution_ids) t AS iid
  ) x
  JOIN {INST} i ON CAST(SUBSTRING(x.iid, 23) AS BIGINT) = i.id
  GROUP BY x.work_id, x.author_sequence
),
wk AS (
  SELECT id, title, publication_year
  FROM {WB} WHERE id IN (SELECT work_id FROM sample)
),
prof_inst AS (
  SELECT x.author_id,
         CONCAT_WS('; ', SLICE(COLLECT_LIST(i.display_name), 1, 5)) AS inst_names
  FROM (
    SELECT a.author_id, iid
    FROM {AFM} a LATERAL VIEW EXPLODE(a.institution_ids) t AS iid
    WHERE a.author_id IN (SELECT author_id FROM cand_ids)
  ) x
  JOIN {INST} i ON CAST(SUBSTRING(x.iid, 23) AS BIGINT) = i.id
  GROUP BY x.author_id
)
SELECT s.work_id, s.author_sequence, s.tier, s.existing_author_id, s.raw_author_name,
  TRY_ELEMENT_AT(s.work_source_ids, 1) AS primary_source_id,
  CONCAT(
    'INCOMING AUTHORSHIP\\n',
    'name: ', s.raw_author_name, '\\n',
    'coauthors on this work: ', COALESCE(ca.coauthors, '(none listed)'), '\\n',
    'institutions: ', COALESCE(ii.inst_names, '(none listed)'), '\\n',
    'work: "', COALESCE(w.title, '(untitled)'), '" (', COALESCE(CAST(w.publication_year AS STRING), '?'), ')\\n',
    '\\nASSIGNED AUTHOR PROFILE\\n',
    a.display_name,
    CASE WHEN SIZE(a.name_variants) > 1
         THEN CONCAT(' (variants: ', CONCAT_WS('; ', SLICE(a.name_variants, 1, 4)), ')')
         ELSE '' END,
    '; institutions: ', COALESCE(pi.inst_names, '(none)'),
    '; active ', COALESCE(CAST(a.first_active_year AS STRING), '?'),
    '-', COALESCE(CAST(a.last_active_year AS STRING), '?'),
    '; works: ', CAST(a.works_count AS STRING)
  ) AS prompt
FROM sample s
LEFT JOIN coauth ca ON ca.work_id = s.work_id AND ca.author_sequence = s.author_sequence
LEFT JOIN inc_inst ii ON ii.work_id = s.work_id AND ii.author_sequence = s.author_sequence
LEFT JOIN wk w ON w.id = s.work_id
JOIN {AFM} a ON a.author_id = s.existing_author_id
LEFT JOIN prof_inst pi ON pi.author_id = s.existing_author_id"""


# ---------------------------------------------------------------------------
# Questions (Jev shape). Opus reuses the wording.
# ---------------------------------------------------------------------------

def questions(stratum: str, work_types: dict[str, str] | None = None) -> dict:
    if stratum == "a_type":
        crit = {"assigned_is_fine": "assigned_type is the right type for this record"}
        crit.update(work_types or {})
        crit["unknown"] = "cannot tell from this record"
        return {
            "type_is_right": {"type": "noul", "instructions": "Is assigned_type the right OpenAlex work type for this record? Judge from the title, abstract, venue, raw_type and other fields."},
            "better_type": {"type": "choice", "instructions": "Which OpenAlex work type fits this record best? Choose assigned_is_fine if assigned_type is right.", "criteria": crit},
        }
    if stratum == "b_affil":
        return {
            "names_this_institution": {"type": "noul", "instructions": (
                "Does this affiliation string say the author is affiliated with the institution shown? Answer yes if the string names the institution "
                "(by any of its names, other names, acronyms, former names or obvious typos) or a unit that is part of it (campus, school, college, department, "
                "institute, lab, center, hospital). Answer no if the string names only a different organization (including one with a similar name or the same "
                "acronym elsewhere), only a place, only a person, or no organization at all. Several organizations may be listed; answer yes if any of them is this "
                "institution or a unit of it. A unit name counts only when the string does not present it as belonging to a different organization, and an acronym "
                "counts only when the string does not spell it out as, or attach it to, a different organization. An organization merely located on the institution's "
                "campus, or a fellowship, prize or degree named after it, is not an affiliation.")},
            "verdict": {"type": "choice", "instructions": "What does the affiliation string denote, with respect to the institution shown?", "criteria": {
                "names_it": "the institution itself or a unit or campus that is part of it",
                "different_organization": "some other organization; it may share a word, a unit name, an acronym or a city with the institution, but is not part of it",
                "not_an_organization": "no organization at all: a person, a place, a sentence, a code, junk",
                "unknown": "it could be this institution or something else and the string does not settle it (a bare acronym or a generic unit name shared by several institutions)"}},
        }
    if stratum == "c_merge":
        return {
            "same_work": {"type": "score", "instructions": "record_a and record_b are two bibliographic records. Decide whether they describe the same scholarly work. Author name formatting (order, initials, diacritics) and venue naming differences are not evidence of different works.",
                          "criteria": ["different works", "a version of the same work (preprint, accepted manuscript, reprint, translation, new edition, supplementary version of the same item)", "the same record described twice"]},
            "same_authors": {"type": "noul", "instructions": "Do record_a and record_b list the same authors, allowing for name formatting differences and one list being truncated?"},
            "title_format_only": {"type": "noul", "instructions": "Are the two titles identical apart from formatting (case, punctuation, diacritics, whitespace, HTML)?"},
        }
    if stratum == "d_repo":
        return {
            "record_kind": {"type": "choice", "instructions": "What kind of record is this repository item? Judge from the title, abstract start, raw type, set_spec and repository.", "criteria": {
                "scholarly_work": "a scholarly work: article, paper, thesis, book or chapter, report, dataset, software, preprint, conference contribution, poster, or similar research output",
                "retraction_notice": "a notice that retracts another work",
                "correction_notice": "an erratum or correction to another work",
                "withdrawn": "a record marked withdrawn or removed",
                "periodical_issue_or_newspaper": "a whole issue, volume or number of a periodical, newspaper or magazine, or a newspaper page or clipping",
                "image_or_object": "a photograph, image, map, artwork, museum object or physical item",
                "supplementary_component": "a figure, table, appendix, supplementary file, or part of a larger work with no standalone content",
                "index_or_toc_page": "a table of contents, index, cover, masthead, or front/back matter page",
                "announcement_or_call": "a call for papers, event announcement, news item, course description, or administrative record",
                "unknown": "cannot tell from this record"}},
        }
    if stratum == "e_auth":
        return {
            "same_person": {"type": "noul", "instructions": "Are the incoming authorship and the assigned author profile the same person? Weigh name compatibility, institutions, research era and field."},
            "verdict": {"type": "choice", "instructions": "Are the incoming authorship and the assigned author profile the same person?", "criteria": {
                "same_person": "the same person", "different_person": "a different person", "unknown": "cannot determine from this evidence"}},
        }
    raise ValueError(stratum)


# ---------------------------------------------------------------------------
# State builders: SQL row (dict) -> (item_id, dimension, meta, state)
# ---------------------------------------------------------------------------

def _j(v):
    """Arrays arrive as lists from Spark and as JSON strings from the statement API."""
    if isinstance(v, str) and v[:1] in "[{":
        try:
            return json.loads(v)
        except Exception:
            return v
    return v


def _cut(s, n):
    s = (s or "").strip()
    return s if len(s) <= n else s[:n] + " …"


def _num(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def score_band(score) -> str:
    s = _num(score)
    if s is None:
        return "no_score"
    return "<0.1" if s < 0.1 else "0.1-0.3" if s < 0.3 else "0.3-0.5" if s < 0.5 else "0.5-0.9" if s < 0.9 else ">=0.9"


def _surnames(names):
    out = set()
    for n in names or []:
        if not n:
            continue
        n = unicodedata.normalize("NFKD", str(n)).encode("ascii", "ignore").decode().lower()
        toks = re.findall(r"[a-z]+", n)
        if toks:
            out.add(toks[-1] if "," not in n else toks[0])
    return out


def build_state(stratum: str, r: dict) -> tuple[str, str, dict, Any]:
    if stratum == "a_type":
        item_id = f"{r['provenance']}:{r['native_id_namespace']}:{r['native_id']}"
        meta = {"assigned_type": r["assigned_type"], "language": r["language"], "provenance": r["provenance"],
                "n_rule": r.get("n_rule"), "has_abstract": bool(r.get("abstract"))}
        state = {"title": _cut(r["title"], 500), "abstract": _cut(r["abstract"], 1300) or None,
                 "venue": r["venue"], "publisher": r["publisher"], "year": r["year"], "language": r["language"],
                 "raw_type": r["raw_type"], "source_record": r["provenance"], "issue": r["issue"],
                 "pages": f"{r['first_page']}-{r['last_page']}" if r.get("first_page") else None,
                 "assigned_type": r["assigned_type"]}
        return item_id, r["classified_rule"], meta, state
    if stratum == "b_affil":
        acr = _j(r["display_name_acronyms"]) or []
        alt = _j(r["display_name_alternatives"]) or []
        score = r.get("inst_score") if r.get("inst_score") is not None else r.get("top_score")
        item_id = f"{r['inst_id']}:{_stable_hash(r['raw_affiliation_string'])}"
        meta = {"inst_id": int(r["inst_id"]), "inst_name": r["display_name"], "country": r["country"],
                "n_inst_on_string": r.get("n_inst"), "has_override": bool(r.get("has_override")),
                "inst_type": r["inst_type"], "matcher_score": _num(score), "string": _cut(r["raw_affiliation_string"], 200)}
        state = {"affiliation_string": _cut(r["raw_affiliation_string"], 600),
                 "institution": {"name": r["display_name"], "acronyms": list(acr)[:5], "other_names": list(alt)[:6],
                                 "type": r["inst_type"], "city": r["city"], "country": r["country"]}}
        return item_id, score_band(score), meta, state
    if stratum == "c_merge":
        aa, ab = _j(r["authors_a"]) or [], _j(r["authors_b"]) or []
        ya, yb = r["year_a"], r["year_b"]
        if ya is None or yb is None:
            gap = "unknown"
        else:
            g = abs(int(ya) - int(yb))
            gap = "0" if g == 0 else "1" if g == 1 else "2+"
        shared = sorted(_surnames(aa) & _surnames(ab))

        def rec(t, au, n, v, y, doi, ty, pv):
            return {"title": _cut(t, 400), "authors": list(au), "n_authors": n, "year": y, "venue": v,
                    "doi_prefix": (doi or "").split("/")[0] or None, "type": ty, "source_record": pv}
        item_id = f"{r['p1']}:{r['ns1']}:{r['id1']}"
        meta = {"work_id": r["work_id"], "p1": r["p1"], "p2": r["p2"], "src2": r["src2"], "dt2": r["dt2"],
                "same_key": r["key_a"] == r["key_b"], "year_gap": gap, "title_a": _cut(r["title_a"], 120)}
        state = {"record_a": rec(r["title_a"], aa, r["n_authors_a"], r["venue_a"], ya, r["doi_a"], r["type_a"], r["p1"]),
                 "record_b": rec(r["title_b"], ab, r["n_authors_b"], r["venue_b"], yb, r["doi_b"], r["type_b"], r["p2"]),
                 "year_gap": gap, "shared_author_surnames": shared, "n_shared_author_surnames": len(shared)}
        return item_id, r["p1"], meta, state
    if stratum == "d_repo":
        urls = _j(r.get("urls")) or []
        host = None
        for u in urls:
            m = re.match(r"https?://([^/]+)/", (u or "") + "/")
            if m:
                host = m.group(1)
                break
        item_id = f"{r['native_id_namespace']}:{r['native_id']}"
        meta = {"endpoint_id": r["endpoint_id"], "venue": r["venue"], "assigned_type": r["assigned_type"],
                "classified_rule": r["classified_rule"], "language": r["language"], "work_id_source": r["work_id_source"],
                "work_id": r["work_id"], "n_endpoint": r.get("n_endpoint"), "title": _cut(r["title"], 120)}
        state = {"title": _cut(r["title"], 400), "repository": r["venue"], "host": host,
                 "raw_type": r.get("raw_native_type") or r.get("raw_type"), "set_spec": list(_j(r.get("set_spec")) or [])[:5],
                 "abstract_start": _cut(r["abstract"], 300) or None, "year": r["year"], "language": r["language"]}
        return item_id, str(r["endpoint_id"] or "no_endpoint"), meta, state
    if stratum == "e_auth":
        item_id = f"{r['work_id']}:{r['author_sequence']}"
        meta = {"tier": r["tier"], "existing_author_id": r["existing_author_id"], "raw_author_name": r.get("raw_author_name"),
                "primary_source_id": r.get("primary_source_id")}
        return item_id, r["tier"], meta, r["prompt"]
    raise ValueError(stratum)


def _stable_hash(s: str) -> str:
    import hashlib
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Reading the answers
# ---------------------------------------------------------------------------

def jev_p_wrong(stratum: str, ans: dict, meta: dict | None = None) -> tuple[float, str]:
    """(p_wrong, jev_label). p_wrong = Jev's probability that the pipeline's decision is wrong,
    read from the Choice / Score, never from the Noul (poorly calibrated on (a))."""
    meta = meta or {}
    if stratum == "a_type":
        pr = ans["better_type"]["probabilities"]
        ch = ans["better_type"]["choice"]
        fine = {"assigned_is_fine", "unknown", meta.get("assigned_type")}
        p_fine = sum(pr.get(k, 0.0) for k in fine if k)
        return max(0.0, 1.0 - p_fine), ("wrong" if ch not in fine else "assigned_is_fine" if ch == meta.get("assigned_type") else ch)
    if stratum == "b_affil":
        pr = ans["verdict"]["probabilities"]
        ch = ans["verdict"]["choice"]
        return pr.get("different_organization", 0.0) + pr.get("not_an_organization", 0.0), \
            ("wrong" if ch in ("different_organization", "not_an_organization") else ch)
    if stratum == "c_merge":
        sc = ans["same_work"]
        pr = sc["probabilities"]
        p_diff = pr.get("0", pr.get(0, 0.0)) if isinstance(pr, dict) else 0.0
        s = float(sc.get("score", 2.0))
        return float(p_diff), ("wrong" if s < 0.5 else "version" if s < 1.5 else "same")
    if stratum == "d_repo":
        pr = ans["record_kind"]["probabilities"]
        ch = ans["record_kind"]["choice"]
        return sum(pr.get(k, 0.0) for k in JUNK_KINDS), ("wrong" if ch in JUNK_KINDS else ch)
    if stratum == "e_auth":
        pr = ans["verdict"]["probabilities"]
        ch = ans["verdict"]["choice"]
        return pr.get("different_person", 0.0), ("wrong" if ch == "different_person" else ch)
    raise ValueError(stratum)


def opus_spec(stratum: str, work_types: dict[str, str] | None = None) -> tuple[str, dict]:
    """(system prompt, JSON schema) for the Opus tail; same wording as the Jev questions plus a reason."""
    q = questions(stratum, work_types)

    def enum_schema(name, opts, extra=None):
        props = {name: {"type": "string", "enum": list(opts)}}
        props.update(extra or {})
        props["reason"] = {"type": "string", "description": "one line"}
        return {"type": "object", "properties": props, "required": list(props), "additionalProperties": False}

    if stratum == "a_type":
        crit = q["better_type"]["criteria"]
        sysm = ("You judge OpenAlex work-type assignments. The record is JSON with an assigned_type. Decide whether assigned_type is right, and which type fits best.\n\nTYPES:\n"
                + "\n".join(f"- {k}: {v}" for k, v in crit.items())
                + "\n\nAnswer with type_is_right (true/false), better_type (assigned_is_fine if right; unknown if you cannot tell), and a one-line reason. Answer only via the JSON schema.")
        return sysm, enum_schema("better_type", crit.keys(), {"type_is_right": {"type": "boolean"}})
    if stratum == "b_affil":
        crit = q["verdict"]["criteria"]
        sysm = ("You judge affiliation-string matching. " + q["names_this_institution"]["instructions"]
                + "\n\nAnswer names_this_institution as yes / no / unknown, verdict as one of:\n" + "\n".join(f"- {k}: {v}" for k, v in crit.items())
                + "\nand a one-line reason. Answer only via the JSON schema.")
        return sysm, enum_schema("verdict", crit.keys(), {"names_this_institution": {"type": "string", "enum": ["yes", "no", "unknown"]}})
    if stratum == "c_merge":
        sysm = ("You judge whether two bibliographic records were correctly merged into one OpenAlex work. " + q["same_work"]["instructions"]
                + "\n\nAnswer same_work as one of: different (different works), version (a version of the same work: preprint, accepted manuscript, reprint, translation, new edition, supplementary version of the same item), same (the same record described twice); "
                "same_authors yes/no; and a one-line reason. Answer only via the JSON schema.")
        return sysm, enum_schema("same_work", ["different", "version", "same"], {"same_authors": {"type": "string", "enum": ["yes", "no"]}})
    if stratum == "d_repo":
        crit = q["record_kind"]["criteria"]
        sysm = ("You judge repository records admitted into OpenAlex as works. " + q["record_kind"]["instructions"] + "\n\nKINDS:\n"
                + "\n".join(f"- {k}: {v}" for k, v in crit.items()) + "\n\nAnswer record_kind and a one-line reason. Answer only via the JSON schema.")
        return sysm, enum_schema("record_kind", crit.keys())
    if stratum == "e_auth":
        sysm = ("You judge author disambiguation. " + q["same_person"]["instructions"]
                + " Answer same_person as same_person / different_person / unknown (cannot determine), and a one-line reason. Answer only via the JSON schema.")
        return sysm, enum_schema("same_person", ["same_person", "different_person", "unknown"])
    raise ValueError(stratum)


def opus_wrong(stratum: str, a: dict, meta: dict | None = None) -> tuple[bool | None, str]:
    """(pipeline wrong per Opus: True/False/None=abstained, verdict label)."""
    meta = meta or {}
    if stratum == "a_type":
        if a["better_type"] == "unknown":
            return None, "unknown"
        fine = a["better_type"] in ("assigned_is_fine", meta.get("assigned_type"))
        return (not a["type_is_right"]) and not fine, ("assigned_is_fine" if fine else a["better_type"])
    if stratum == "b_affil":
        if a["names_this_institution"] == "unknown" or a["verdict"] == "unknown":
            return None, "unknown"
        return a["names_this_institution"] == "no", a["verdict"]
    if stratum == "c_merge":
        return a["same_work"] == "different", a["same_work"]
    if stratum == "d_repo":
        if a["record_kind"] == "unknown":
            return None, "unknown"
        return a["record_kind"] in JUNK_KINDS, a["record_kind"]
    if stratum == "e_auth":
        if a["same_person"] == "unknown":
            return None, "unknown"
        return a["same_person"] == "different_person", a["same_person"]
    raise ValueError(stratum)


# ---------------------------------------------------------------------------
# Cost projection (before any call)
# ---------------------------------------------------------------------------

def state_chars(state: Any) -> int:
    return len(state) if isinstance(state, str) else len(json.dumps(state, ensure_ascii=False))


def project_jev_usd(items: list[dict], stratum_questions: dict[str, dict]) -> tuple[float, int]:
    """items: dicts with stratum, state. Options and instructions are billed as input every call."""
    q_chars = {s: len(json.dumps(q, ensure_ascii=False)) for s, q in stratum_questions.items()}
    chars = sum(state_chars(it["state"]) + q_chars[it["stratum"]] for it in items)
    tokens = int(chars / JEV_CHARS_PER_TOKEN)
    return tokens * JEV_USD_PER_TOKEN, tokens


def project_opus_usd(n_items: int, avg_state_chars: float, system_chars: float) -> float:
    in_tok = n_items * (avg_state_chars + system_chars) / OPUS_CHARS_PER_TOKEN
    out_tok = n_items * OPUS_ASSUMED_OUT_TOKENS
    return (in_tok * OPUS_IN_USD + out_tok * OPUS_OUT_USD) / 1e6


# ---------------------------------------------------------------------------
# Clients (threads + requests; the notebook driver runs these)
# ---------------------------------------------------------------------------

def _backoff(attempt: int) -> float:
    return min(30.0, 0.5 * 2 ** attempt) * (0.5 + random.random())


class JevClient:
    """Native TypeSafe API. One request per item. Retries 429/5xx/transport with jittered backoff."""

    def __init__(self, api_key: str, concurrency: int = 16, timeout: float = 30.0, max_attempts: int = 6, model: str = JEV_MODEL):
        if not api_key:
            raise RuntimeError("Jev api_key missing")
        self._headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        self._concurrency = concurrency
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._model = model
        self._local = threading.local()
        self.total_tokens = 0
        self.n_ok = 0
        self.n_fail = 0
        self._lock = threading.Lock()

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
                    return {"ok": True, "answers": data.get("answers", {}), "input_tokens": tok,
                            "latency_ms": round(ms), "attempts": attempt}
                if resp.status_code in RETRYABLE and attempt < self._max_attempts:
                    ra = resp.headers.get("retry-after")
                    time.sleep(float(ra) if ra else _backoff(attempt))
                    continue
                with self._lock:
                    self.n_fail += 1
                return {"ok": False, "status": resp.status_code, "error": resp.text[:300], "attempts": attempt}
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt < self._max_attempts:
                    time.sleep(_backoff(attempt))
                    continue
                with self._lock:
                    self.n_fail += 1
                return {"ok": False, "status": 0, "error": repr(e)[:300], "attempts": attempt}

    def judge_many(self, items: list[dict], qs_by_stratum: dict[str, dict],
                   on_result: Callable[[dict, dict], None] | None = None) -> list[tuple[dict, dict]]:
        """items: dicts with stratum, state. Returns [(item, result)] in completion order."""
        out = []
        with ThreadPoolExecutor(self._concurrency) as ex:
            futs = {ex.submit(self.decide, it["state"], qs_by_stratum[it["stratum"]]): it for it in items}
            for f in as_completed(futs):
                it = futs[f]
                try:
                    r = f.result()
                except Exception as e:  # never lose the chunk to one bad thread
                    r = {"ok": False, "status": -1, "error": repr(e)[:300]}
                    with self._lock:
                        self.n_fail += 1
                out.append((it, r))
                if on_result:
                    on_result(it, r)
        return out


class OpusClient:
    """Databricks Foundation Model API (OpenAI-chat shape) with JSON-schema output.
    Called over REST rather than ai_query so no temperature parameter is injected
    (the DBR 16.4 cluster-side ai_query bug that broke AuthorshipQualityJudge night 1)."""

    def __init__(self, host: str, token: str, model: str = OPUS_MODEL, concurrency: int = 6,
                 timeout: float = 180.0, max_attempts: int = 4, max_tokens: int = 1500):
        self._url = f"{host.rstrip('/')}/serving-endpoints/{model}/invocations"
        self._headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self._concurrency = concurrency
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._max_tokens = max_tokens
        self.model = model
        self.total_in = 0
        self.total_out = 0
        self.n_ok = 0
        self.n_fail = 0
        self._lock = threading.Lock()

    def judge(self, state: Any, system: str, schema: dict) -> dict:
        user = state if isinstance(state, str) else json.dumps(state, ensure_ascii=False)
        body = {"messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "max_tokens": self._max_tokens,
                "response_format": {"type": "json_schema", "json_schema": {"name": "verdict", "schema": schema, "strict": True}}}
        attempt = 0
        while True:
            attempt += 1
            t0 = time.perf_counter()
            try:
                resp = requests.post(self._url, headers=self._headers, json=body, timeout=self._timeout)
                ms = (time.perf_counter() - t0) * 1000
                if resp.status_code == 200:
                    data = resp.json()
                    usage = data.get("usage") or {}
                    tin, tout = int(usage.get("prompt_tokens") or 0), int(usage.get("completion_tokens") or 0)
                    text = ((data.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
                    if isinstance(text, list):  # some endpoints return content blocks
                        text = "".join(b.get("text", "") for b in text if isinstance(b, dict))
                    rec = {"ok": False, "model": data.get("model"), "in": tin, "out": tout, "latency_ms": round(ms),
                           "cost": (tin * OPUS_IN_USD + tout * OPUS_OUT_USD) / 1e6, "attempts": attempt}
                    with self._lock:
                        self.total_in += tin
                        self.total_out += tout
                    try:
                        m = re.search(r"\{.*\}", text, re.S)
                        rec["answer"] = json.loads(m.group(0) if m else text)
                        rec["ok"] = True
                        with self._lock:
                            self.n_ok += 1
                    except Exception:
                        rec["error"] = "bad_json"
                        rec["raw"] = text[:300]
                        with self._lock:
                            self.n_fail += 1
                    return rec
                if resp.status_code in RETRYABLE and attempt < self._max_attempts:
                    time.sleep(_backoff(attempt) * 2)
                    continue
                with self._lock:
                    self.n_fail += 1
                return {"ok": False, "status": resp.status_code, "error": resp.text[:300], "cost": 0.0, "attempts": attempt}
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt < self._max_attempts:
                    time.sleep(_backoff(attempt) * 2)
                    continue
                with self._lock:
                    self.n_fail += 1
                return {"ok": False, "status": 0, "error": repr(e)[:300], "cost": 0.0, "attempts": attempt}

    def judge_many(self, items: list[dict], spec_by_stratum: dict[str, tuple[str, dict]],
                   on_result: Callable[[dict, dict], None] | None = None) -> list[tuple[dict, dict]]:
        out = []
        with ThreadPoolExecutor(self._concurrency) as ex:
            futs = {ex.submit(self.judge, it["state"], *spec_by_stratum[it["stratum"]]): it for it in items}
            for f in as_completed(futs):
                it = futs[f]
                try:
                    r = f.result()
                except Exception as e:
                    r = {"ok": False, "status": -1, "error": repr(e)[:300], "cost": 0.0}
                out.append((it, r))
                if on_result:
                    on_result(it, r)
        return out


# ---------------------------------------------------------------------------
# Tail selection and aggregation
# ---------------------------------------------------------------------------

def select_tail(rows: list[dict], cap: int, strata: list[str]) -> list[dict]:
    """Highest-confidence Jev flags, round-robin across strata so one stratum cannot own the cap.
    rows: dicts with stratum, item_id, jev_flag, jev_p_wrong."""
    by_s: dict[str, list[dict]] = {}
    for r in rows:
        if r["stratum"] in strata and r.get("jev_flag"):
            by_s.setdefault(r["stratum"], []).append(r)
    ranked = []
    for s, rs in by_s.items():
        rs.sort(key=lambda r: (-float(r["jev_p_wrong"]), r["item_id"]))
        for i, r in enumerate(rs):
            ranked.append((i, -float(r["jev_p_wrong"]), r))
    ranked.sort(key=lambda t: (t[0], t[1], t[2]["stratum"], t[2]["item_id"]))
    return [t[2] for t in ranked[:cap]]


def aggregate(rows: list[dict]) -> list[dict]:
    """Per (stratum, dimension) and per stratum (dimension None). rows: sample rows with
    stratum, dimension, jev_flag, opus_wrong (True/False/None), jev_cost_usd, opus_cost_usd."""
    groups: dict[tuple[str, str | None], list[dict]] = {}
    for r in rows:
        groups.setdefault((r["stratum"], r["dimension"]), []).append(r)
        groups.setdefault((r["stratum"], None), []).append(r)
    out = []
    for (s, d), rs in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1] or "")):
        n = len(rs)
        n_flag = sum(1 for r in rs if r.get("jev_flag"))
        n_op = sum(1 for r in rs if r.get("opus_wrong") is not None)
        n_conf = sum(1 for r in rs if r.get("opus_wrong") is True)
        flag_prec = (n_conf / n_op) if n_op else None
        out.append({
            "stratum": s, "dimension": d, "n": n, "n_flagged": n_flag,
            "precision_jev": (1.0 - n_flag / n) if n else None,
            "n_opus": n_op, "n_opus_confirmed": n_conf,
            "opus_flag_precision": flag_prec,
            # Opus-adjusted precision: scale the flag count by the share Opus confirms (stratum-level tail)
            "precision_opus_adj": (1.0 - (n_flag * flag_prec) / n) if (n and flag_prec is not None) else None,
            "jev_cost_usd": sum(float(r.get("jev_cost_usd") or 0) for r in rs),
            "opus_cost_usd": sum(float(r.get("opus_cost_usd") or 0) for r in rs),
        })
    return out


def metrics_rows(daily: list[dict], jev_errors: dict[str, int]) -> list[tuple[str, str | None, float]]:
    """Tall rows (metric, dimension, value) for openalex.monitoring.metrics, component jev_quality.
    Dimension for the per-dimension metrics is 'stratum|dimension' so checks can capture with
    jev_judged[a_type|*]."""
    rows: list[tuple[str, str | None, float]] = []
    for d in daily:
        if d["dimension"] is None:
            rows.append(("jev_stratum_judged", d["stratum"], float(d["n"])))
            rows.append(("jev_stratum_flagged", d["stratum"], float(d["n_flagged"])))
            rows.append(("opus_judged", d["stratum"], float(d["n_opus"])))
            rows.append(("opus_confirmed", d["stratum"], float(d["n_opus_confirmed"])))
        else:
            key = f"{d['stratum']}|{d['dimension']}"
            rows.append(("jev_judged", key, float(d["n"])))
            rows.append(("jev_flagged", key, float(d["n_flagged"])))
    for s, e in jev_errors.items():
        rows.append(("jev_errors", s, float(e)))
    return rows
