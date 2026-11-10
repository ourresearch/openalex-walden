"""SQL templates for the work-type nightly: rendering, the versioned policy, and its identity checks.

Every SQL file under ../sql is a template. `${name}` is a parameter; render() fills it and refuses on an unknown or
leftover one. The decide parameters (policy id, model id, tau, the 26 classes) come only from the versioned policy file
policy_<id>.json, and load_policy() recomputes both content hashes before it returns them: pol-<sha12> over the
policy's content (student/prod/policy.py Policy.id) and s2-<sha16> over the manifest's content (models.py Manifest.id).
So a template can't be run with a policy or class list that isn't the one the id names.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
SQL_DIR = HERE.parent / "sql"
PARAM = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
SAFE = re.compile(r"^[A-Za-z0-9_.:\-]*$")            # identifiers, ids, dates, numbers: nothing that can end a literal
OUTCOMES = ("student-s2", "fallback:today", "no_today_type", "stage0-list")
GENERATED = ("classes", "stage0_lists", "s2_cuts")      # built by this module from the hashed policy file, never typed in
ALIAS = {"libguide": "libguides", "conference-proceedings": "conference-paper", "thesis": "dissertation"}  # prod/vocab.py


class TemplateError(ValueError):
    pass


def _canon(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


LIST_TUPLES = ("doi_prefixes", "names_exact", "url_substrings", "exclude_title_flags")
LIST_FIELDS = ("name", "type", "doi_prefixes", "doi_regex", "names_exact", "names_regex", "url_substrings", "exclude_title_flags")


def list_id(content: dict) -> str:
    """student/prod/stage0.SourceList.id, recomputed from a list's content (sorted tuple fields, no `source`)."""
    c = {k: (sorted(content.get(k) or []) if k in LIST_TUPLES else content.get(k)) for k in LIST_FIELDS}
    return f"s0-{c['name']}-" + hashlib.sha256(json.dumps(c, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:8]


def decision_id(base_policy_id: str, list_ids) -> str:
    """student/prod/stage0.decision_id: the base policy id plus the enabled lists' ids."""
    return "pol4-" + hashlib.sha256((base_policy_id + "|" + "|".join(sorted(list_ids))).encode()).hexdigest()[:12]


def cuts_id(base_tau, type_cuts: dict, model: str, lists: list) -> str:
    """worktype-stage2's id for a per-type cut version (v6/cert/cuts_*.json): 's2cuts-' + 16 hex of the canonical JSON
    {base_tau, type_cuts, model, lists} (sorted keys, no spaces)."""
    c = {"base_tau": base_tau, "type_cuts": type_cuts, "model": model, "lists": lists}
    return "s2cuts-" + hashlib.sha256(json.dumps(c, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()[:16]


def cuts_decision_id(base_policy_id: str, s2cuts_id: str, list_ids) -> str:
    """student/prod/writer_lists.cuts_decision_id: 'pol5-' + 12 hex over the base policy, the cuts id and the lists."""
    c = {"base_policy_id": base_policy_id, "cuts_id": s2cuts_id, "lists": sorted(list_ids)}
    return "pol5-" + hashlib.sha256(json.dumps(c, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:12]


def check_cuts(block: dict, tau: float, classes, model_id: str, lists: list, model_tag: str | None = None) -> dict:
    """A per-type cut block: the cuts id recomputed from its inputs (which must name this model, this tau and these lists),
    every cut type a model class, every cut null (never) or in [tau, 1]."""
    tc = block["type_cuts"]
    inp = block["cuts_id_inputs"]
    if cuts_id(inp["base_tau"], tc, inp["model"], inp["lists"]) != block["cuts_id"]:
        raise TemplateError(f"cut block hashes to {cuts_id(inp['base_tau'], tc, inp['model'], inp['lists'])}, the file says {block['cuts_id']}")
    if round(float(inp["base_tau"]), 4) != round(float(tau), 4):
        raise TemplateError("the cut version was made for another tau")
    if inp["model"] not in {model_id} | ({f"{model_id} ({model_tag})"} if model_tag else set()):   # exact (Codex 11-09 #3)
        raise TemplateError(f"the cut version names model {inp['model']!r}; the policy's model is {model_id}")
    if list(inp["lists"]) != [L["name"] for L in lists]:
        raise TemplateError(f"the cut version was made with lists {inp['lists']}, the policy has {[L['name'] for L in lists]}")
    for t, c in tc.items():
        if t not in classes:
            raise TemplateError(f"cut for {t!r}, which the model can't predict")
        if c is not None and not (float(c) == round(float(c), 4) and float(tau) <= float(c) <= 1.0):   # Codex #5
            raise TemplateError(f"cut {t}: {c}: a per-type cut must be a 4-dp value in [tau, 1], or null (never emitted)")
    return tc


def check_lists(lists: list) -> list:
    """Each list's id must be its content's hash; title-flag guards aren't implemented in SQL, so such a list is refused."""
    for L in lists:
        if list_id(L) != L["id"]:
            raise TemplateError(f"list {L.get('name')}: content hashes to {list_id(L)}, the file says {L['id']}")
        if L.get("exclude_title_flags"):
            raise TemplateError(f"list {L['id']}: title-flag guards aren't supported in the nightly SQL")
        if L.get("doi_regex") is None and L.get("url_substrings"):
            raise TemplateError(f"list {L['id']}: URL substrings without a DOI regex aren't a stage-0 key")
    return lists


def load_policy(policy_id: str = "pol-9b8e9d427e64", policy_doc: dict | None = None) -> dict:
    """policy_doc: the policy file's content (tests); else lib/policy_<id>.json. With `stage0_lists` the file's policy_id
    is the combined decision id (stage0.decision_id) over the base policy (`base_policy_id`) and the lists, in order."""
    p = policy_doc if policy_doc is not None else json.loads((HERE / f"policy_{policy_id}.json").read_text())
    pid = "pol-" + hashlib.sha256(json.dumps(p["policy_content"], sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:12]
    lists = check_lists(p.get("stage0_lists") or [])
    cut_block = p.get("s2_type_cuts")
    if cut_block:
        if p.get("base_policy_id") != pid:
            raise TemplateError(f"base policy hashes to {pid}, the file says {p.get('base_policy_id')}")
        did = cuts_decision_id(pid, cut_block["cuts_id"], [L["id"] for L in lists])
        if did != policy_id or p["policy_id"] != policy_id:
            raise TemplateError(f"policy + cuts + lists hash to {did}, not {policy_id}")
        pid = did
    elif lists:
        if p.get("base_policy_id") != pid:
            raise TemplateError(f"base policy hashes to {pid}, the file says {p.get('base_policy_id')}")
        did = decision_id(pid, [L["id"] for L in lists])
        if did != policy_id or p["policy_id"] != policy_id:
            raise TemplateError(f"policy + lists hash to {did}, not {policy_id}")
        pid = did
    m = p["model"]
    mid = f"{m['content']['kind']}-" + hashlib.sha256(_canon(m["content"]).encode()).hexdigest()[:16]
    c = p["policy_content"]
    if not lists and not cut_block and (pid != policy_id or p["policy_id"] != policy_id):
        raise TemplateError(f"policy file hashes to {pid}, not {policy_id}")
    if mid != m["id"] or c["s2_model"] != mid:
        raise TemplateError(f"model content hashes to {mid}; the policy names {c['s2_model']}")
    if c["base"] != "C4" or c["fallback"] != "today" or c["cuts"] is not None:
        raise TemplateError("the SQL decide implements stage 2 alone (C4, fallback today, no stage-1 cuts) only")
    if m["content"]["render_version"] != "s2r3":
        raise TemplateError("the nightly packs render s2r3 only")
    type_cuts = check_cuts(cut_block, c["tau"], m["content"]["classes"], mid, lists, m.get("tag")) if cut_block else {}
    kind = "C4" + ("+cuts" if cut_block else "") + ("+stage0" if lists else "")
    return {"policy_id": pid, "model_id": mid, "tau": f"{float(c['tau']):.4f}", "classes": list(m["content"]["classes"]),
            "render_version": m["content"]["render_version"], "policy_kind": kind, "stage0_lists": lists, "type_cuts": type_cuts}


def cuts_sql(type_cuts: dict) -> str:
    """The per-type cuts as a one-line relation (type, cut DECIMAL(5,4); a null cut = stage 2 never emits the type)."""
    if not type_cuts:
        return "SELECT CAST(NULL AS STRING) AS type, CAST(NULL AS DECIMAL(5,4)) AS cut WHERE 1 = 0"
    for t in type_cuts:
        if not re.fullmatch(r"[a-z][a-z-]*", t):
            raise TemplateError(f"cut type {t!r}")
    return " UNION ALL ".join(f"SELECT '{t}' AS type, CAST({'NULL' if c is None else f'{float(c):.4f}'} AS DECIMAL(5,4)) AS cut"
                              for t, c in sorted(type_cuts.items()))


def _lit(x: str, dialect: str) -> str:
    """A SQL string literal. Spark unescapes backslashes inside literals, so they are doubled there; DuckDB doesn't."""
    x = x.replace("'", "''")
    return "'" + (x.replace("\\", "\\\\") if dialect == "spark" else x) + "'"


def _arr(xs, dialect: str) -> str:
    if dialect == "spark":
        return "array(" + ", ".join(_lit(x, dialect) for x in xs) + ")" if xs else "CAST(array() AS ARRAY<STRING>)"
    return "[" + ", ".join(_lit(x, dialect) for x in xs) + "]" if xs else "CAST([] AS VARCHAR[])"


def stage0_sql(lists: list, dialect: str = "spark") -> str:
    """The policy's lists as a relation (ord, list_id, type, doi_prefixes, doi_regex, names_exact, names_regex,
    url_substrings), in the policy's order (the first matching list names the override, as stage0.apply keeps it)."""
    null = "CAST(NULL AS STRING)"
    if not lists:
        return (f"SELECT CAST(NULL AS INT) AS ord, {null} AS list_id, {null} AS type, {_arr([], dialect)} AS doi_prefixes, "
                f"{null} AS doi_regex, {_arr([], dialect)} AS names_exact, {null} AS names_regex, {_arr([], dialect)} AS url_substrings WHERE 1 = 0")
    rows = []
    for i, L in enumerate(lists):
        rx = lambda v: _lit(v, dialect) if v else null  # noqa: E731
        rows.append(f"SELECT {i} AS ord, {_lit(L['id'], dialect)} AS list_id, {_lit(L['type'], dialect)} AS type, "
                    f"{_arr(L.get('doi_prefixes') or [], dialect)} AS doi_prefixes, {rx(L.get('doi_regex'))} AS doi_regex, "
                    f"{_arr(L.get('names_exact') or [], dialect)} AS names_exact, {rx(L.get('names_regex'))} AS names_regex, "
                    f"{_arr(L.get('url_substrings') or [], dialect)} AS url_substrings")
    return " UNION ALL ".join(rows)          # one line: a generated parameter never spans lines


def classes_sql(classes) -> str:
    for c in classes:
        if not re.fullmatch(r"[a-z][a-z-]*", c):
            raise TemplateError(f"class {c!r}")
    return "array(" + ", ".join(f"'{c}'" for c in classes) + ")"


def render(name_or_text: str, **params) -> str:
    """name: a file in ../sql (e.g. '04_decide.sql') or SQL text. Params must be identifier-safe strings."""
    text = (SQL_DIR / name_or_text).read_text() if name_or_text.endswith(".sql") and "\n" not in name_or_text else name_or_text
    need = set(PARAM.findall(text))
    if need - set(params):
        raise TemplateError(f"missing parameters {sorted(need - set(params))}")
    for k in need:
        v = str(params[k])
        if k not in GENERATED and not SAFE.match(v):
            raise TemplateError(f"parameter {k}={v!r} is not identifier-safe")
    for k in need & set(GENERATED):
        if "\n" in str(params[k]):
            raise TemplateError(f"generated parameter {k} spans lines")
    out = PARAM.sub(lambda m: str(params[m.group(1)]), text)
    if "${" in out:
        raise TemplateError("unrendered parameter left")
    return out


def decide_params(policy_id: str = "pol-9b8e9d427e64", policy_doc: dict | None = None, dialect: str = "spark", **more) -> dict:
    p = load_policy(policy_id, policy_doc)
    return dict(more, policy_id=p["policy_id"], model_id=p["model_id"], tau=p["tau"], classes=classes_sql(p["classes"]),
                n_classes=str(len(p["classes"])), render_version=p["render_version"], policy_kind=p["policy_kind"].replace("+", "_"),
                stage0_lists=stage0_sql(p["stage0_lists"], dialect), s2_cuts=cuts_sql(p["type_cuts"]))


def _code(line: str) -> str:
    """The line without a trailing -- comment (a '--' inside a '...' literal is kept)."""
    q = False
    for i, ch in enumerate(line):
        if ch == "'":
            q = not q
        elif not q and line.startswith("--", i):
            return line[:i]
    return line


def statements(sql: str) -> list[str]:
    """Split a rendered multi-statement file on ';' at the end of a line's code (a trailing -- comment may follow; no ';'
    inside literals in these templates)."""
    parts, cur = [], []
    for line in sql.splitlines():
        cur.append(line)
        if _code(line).rstrip().endswith(";"):
            cur[-1] = _code(line).rstrip()[:-1]          # the closing ';' and any trailing comment after it
            s = "\n".join(cur).strip()
            if s and not all(l.strip().startswith("--") or not l.strip() for l in s.splitlines()):
                parts.append(s)
            cur = []
    s = "\n".join(cur).strip()
    if s and not all(l.strip().startswith("--") or not l.strip() for l in s.splitlines()):
        parts.append(s)
    return parts


def code_of(statement: str) -> str:
    """The statement without its comments (whole-line and trailing)."""
    return "\n".join(c for c in (_code(l).rstrip() for l in statement.splitlines()) if c.strip())


def creates(statement: str, table: str) -> bool:
    """True if the statement's code (comments ignored) creates or replaces `table` (any schema prefix)."""
    return re.search(r"\bCREATE\s+(OR\s+REPLACE\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?[\w.${}]*\b" + re.escape(table) + r"\b",
                     code_of(statement), re.I) is not None
