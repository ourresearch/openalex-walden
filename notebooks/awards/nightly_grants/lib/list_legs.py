"""Pinned link lists (ce1014): one generic way to ship frozen, gated link sets through Nightly Grants.

  ADD      rows (work_id, award_id)          -> a direct link to that award (resolved through the registry, so a pre-fold id lands
                                                on its fold survivor), unioned into work_links like any other leg
           rows (work_id, funder_id, number) -> the `list_add_number` WorkAwards leg (registry mode, same as `relink`), so the
                                                number is matched and elected exactly like every other observation
  SUPPRESS rows (work_id, award_id, reason)  -> that served (work, award) link is removed from work_awards after every leg and the
                                                final dedupe. The award document and the work's funder are untouched (the removed
                                                award struct is kept in list_audit, which CreateWorksEnriched reads for .funders)

Every list is a config-pinned Delta VERSION (config "list_legs"), never "current": the night reads exactly what was reviewed.
Each list has a size fuse (expected_rows +- tolerance) and a resolution fuse (max_unresolved / max_unmatched); any breach stops
the night before the first state change (fail closed). Rebuilt from the pinned inputs every night, so re-running is idempotent.
Rollback = pin an empty version with expected_rows 0 (or drop the entry): the next night serves without the list.
Everything a list did tonight is one row per list row in the `list_audit` output (action, status, award)."""
import json
import re

from stable_award_ids import ident, literal

NAME = re.compile(r"[a-z][a-z0-9_]{0,40}")
COLUMN = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")
NUMBER_LEG = "list_add_number"            # the WorkAwards leg view (work_awards.LEGS) fed by number-kind ADD lists
KEYS = {"name", "relation", "version", "expected_rows", "tolerance", "kind", "source", "columns", "filter", "priority",
        "max_unresolved", "max_unmatched", "note"}
COLUMNS = {("add", "award_id"): ("work_id", "award_id"),
           ("add", "number"): ("work_id", "funder_id", "number"),
           ("suppress", "award_id"): ("work_id", "award_id", "reason")}
G = "concat('https://openalex.org/G',{})"
EMPTY = {
    "add_award": "SELECT CAST(NULL AS STRING) list_name,CAST(NULL AS BIGINT) list_version,CAST(NULL AS STRING) source,"
                 "CAST(NULL AS INT) priority,CAST(NULL AS BIGINT) work_id,CAST(NULL AS BIGINT) award_id WHERE false",
    "add_number": "SELECT CAST(NULL AS STRING) list_name,CAST(NULL AS BIGINT) list_version,CAST(NULL AS STRING) source,"
                  "CAST(NULL AS BIGINT) work_id,CAST(NULL AS BIGINT) funder_id,CAST(NULL AS STRING) number WHERE false",
    "suppress": "SELECT CAST(NULL AS STRING) list_name,CAST(NULL AS BIGINT) list_version,CAST(NULL AS STRING) source,"
                "CAST(NULL AS BIGINT) work_id,CAST(NULL AS BIGINT) award_id,CAST(NULL AS STRING) reason WHERE false",
}


def _int(value, name, low=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < low:
        raise ValueError(f"LIST_CONFIG: {name} must be an integer >= {low}")
    return value


def specs(config, bound=None):
    """Validated, normalised list specs from config["list_legs"] = {"add": [...], "suppress": [...]} (both optional) plus the
    rows bound tonight in the control table (`bound`, same shape; see bindings()). A name in both is refused (duplicate)."""
    lists = config.get("list_legs") or {}
    if bound:
        if not isinstance(lists, dict):
            raise ValueError("LIST_CONFIG: list_legs takes only 'add' and 'suppress'")
        lists = {a: list(lists.get(a) or []) + list(bound.get(a) or []) for a in ("add", "suppress")}
    if not isinstance(lists, dict) or set(lists) - {"add", "suppress"}:
        raise ValueError("LIST_CONFIG: list_legs takes only 'add' and 'suppress'")
    out, names = [], set()
    for action in ("add", "suppress"):
        for raw in lists.get(action) or []:
            if not isinstance(raw, dict) or set(raw) - KEYS:
                raise ValueError("LIST_CONFIG: unknown keys " + repr(sorted(set(raw) - KEYS) if isinstance(raw, dict) else raw))
            name = raw.get("name")
            if not isinstance(name, str) or not NAME.fullmatch(name) or name in names:
                raise ValueError("LIST_CONFIG: bad or duplicate list name " + repr(name))
            names.add(name)
            kind = raw.get("kind", "award_id")
            if (action, kind) not in COLUMNS:
                raise ValueError(f"LIST_CONFIG: {name}: kind {kind!r} is not allowed for {action}")
            if "version" not in raw:
                raise ValueError(f"LIST_CONFIG: {name}: version must be pinned")
            if "expected_rows" not in raw:
                raise ValueError(f"LIST_CONFIG: {name}: expected_rows is required (size fuse)")
            columns = dict(raw.get("columns") or {})
            if set(columns) - set(COLUMNS[(action, kind)]) or not all(isinstance(v, str) and COLUMN.fullmatch(v) for v in columns.values()):
                raise ValueError(f"LIST_CONFIG: {name}: bad columns mapping")
            filt = dict(raw.get("filter") or {})
            if not all(COLUMN.fullmatch(k) and isinstance(v, (str, int)) and not isinstance(v, bool) for k, v in filt.items()):
                raise ValueError(f"LIST_CONFIG: {name}: filter is {{column: string|int}} equality only")
            source = raw.get("source", name)
            if not isinstance(source, str) or not source:
                raise ValueError(f"LIST_CONFIG: {name}: bad source")
            priority = _int(raw.get("priority", 3), name + ".priority", 1)
            out.append(dict(action=action, kind=kind, name=name, relation=ident(raw.get("relation")),
                            version=_int(raw["version"], name + ".version"),
                            expected_rows=_int(raw["expected_rows"], name + ".expected_rows"),
                            tolerance=_int(raw.get("tolerance", 0), name + ".tolerance"),
                            columns=columns, filter=filt, source=source, priority=priority,
                            max_unresolved=_int(raw.get("max_unresolved", 0), name + ".max_unresolved"),
                            max_unmatched=_int(raw.get("max_unmatched", 0), name + ".max_unmatched")))
    return out


def _bucket(s):
    return "suppress" if s["action"] == "suppress" else ("add_number" if s["kind"] == "number" else "add_award")


def select_sql(s):
    """The pinned read of one list, projected onto the standard columns (extra columns in the table are ignored)."""
    col = lambda k: s["columns"].get(k, k)
    award = lambda k: f"CAST(regexp_replace(CAST({col(k)} AS STRING),'^https://openalex.org/G','') AS BIGINT)"
    where = " AND ".join(f"{k} = {literal(v) if isinstance(v, str) else int(v)}" for k, v in sorted(s["filter"].items())) or "true"
    head = f"SELECT {literal(s['name'])} list_name,CAST({s['version']} AS BIGINT) list_version,{literal(s['source'])} source,"
    body = {"add_award": f"CAST({s['priority']} AS INT) priority,CAST({col('work_id')} AS BIGINT) work_id,{award('award_id')} award_id",
            "add_number": f"CAST({col('work_id')} AS BIGINT) work_id,CAST({col('funder_id')} AS BIGINT) funder_id,CAST({col('number')} AS STRING) number",
            "suppress": f"CAST({col('work_id')} AS BIGINT) work_id,{award('award_id')} award_id,CAST({col('reason')} AS STRING) reason"}[_bucket(s)]
    return f"{head}{body} FROM {s['relation']} VERSION AS OF {s['version']} WHERE {where}"


def fuse(s, rows):
    if abs(int(rows) - s["expected_rows"]) > s["tolerance"]:
        raise RuntimeError(f"LIST_SIZE_FUSE: {s['name']} rows={rows} expected={s['expected_rows']}+-{s['tolerance']}")


BINDING_COLUMNS = ("list_name", "action", "kind", "relation", "version", "expected_rows", "tolerance", "source", "columns_json",
                   "filter_json", "priority", "max_unresolved", "max_unmatched", "enabled", "active_from_night", "bound_at", "bound_by", "note")


def night_of(started):
    """The ship night a run belongs to: its start date in Pacific time (the 20:00 PT run of Mon 10-19 is night 2026-10-19)."""
    from zoneinfo import ZoneInfo
    return started.astimezone(ZoneInfo("America/Los_Angeles")).date().isoformat()


def bindings(c):
    """Data-only activation: config "list_bindings" names an append-only control table. It is bound once at run start like every
    input (so a row written mid-run waits for the next night). Per list_name the newest row with active_from_night <= tonight wins;
    enabled=false on that row turns the list off (rollback without a push). Two rows for one list on its winning night stop the
    run (LIST_BINDING_AMBIGUOUS). Returns {"add": [...], "suppress": [...]} in list_legs shape."""
    relation = c.config.get("list_bindings")
    if not relation:
        return {}
    c.bind("list_bindings", relation)
    night = night_of(c.started)
    c.counts["list_bindings_night"] = night
    active = f"SELECT * FROM list_bindings_v WHERE active_from_night <= DATE {literal(night)}"
    c.zero("LIST_BINDING_AMBIGUOUS", f"""SELECT list_name,active_from_night,count(*) n FROM ({active}) b
      WHERE (list_name,active_from_night) IN (SELECT list_name,max(active_from_night) FROM ({active}) x GROUP BY list_name)
      GROUP BY 1,2 HAVING count(*)>1""")
    rows = c.sql(f"""SELECT * FROM ({active}) b QUALIFY row_number() OVER (PARTITION BY list_name ORDER BY active_from_night DESC,bound_at DESC)=1
      ORDER BY list_name""").collect()
    out = {"add": [], "suppress": []}
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        c.versions.setdefault("list_bindings_rows", []).append({k: str(r.get(k)) for k in ("list_name", "relation", "version", "enabled", "active_from_night")})
        if not r.get("enabled"):
            continue
        if r.get("action") not in ("add", "suppress"):
            raise ValueError(f"LIST_CONFIG: binding {r.get('list_name')!r}: action must be add or suppress")
        raw = dict(name=r["list_name"], relation=r["relation"], version=r["version"], expected_rows=r["expected_rows"],
                   kind=r.get("kind"), tolerance=r.get("tolerance"), source=r.get("source"), priority=r.get("priority"),
                   max_unresolved=r.get("max_unresolved"), max_unmatched=r.get("max_unmatched"),
                   columns=json.loads(r["columns_json"]) if r.get("columns_json") else None,
                   filter=json.loads(r["filter_json"]) if r.get("filter_json") else None)
        raw = {k: (int(v) if k in ("version", "expected_rows", "tolerance", "priority", "max_unresolved", "max_unmatched") else v)
               for k, v in raw.items() if v is not None}
        out[r["action"]].append(raw)
    return out


def bind(c):
    """Called from bind_inputs before derive_inputs (the number leg feeds work_observations_in). Creates the three views
    list_add_award_v, list_add_number_v, list_suppress_v (empty and typed when no list is configured or bound)."""
    lists = specs(c.config, bindings(c))
    c.require(not lists or "list_audit" in (getattr(c, "outputs", None) or {"list_audit": None}), "LIST_AUDIT_OUTPUT_REQUIRED")
    c.list_specs = {s["name"]: s for s in lists}
    parts = {"add_award": [], "add_number": [], "suppress": []}
    for s in lists:
        rel = c.artifact("list_in_" + s["name"], select_sql(s))
        n = c.count("list_" + s["name"] + "_rows", rel)
        c.versions["list_" + s["name"]] = dict(relation=s["relation"], version=s["version"], rows=n, action=s["action"], kind=s["kind"],
                                               filter=s["filter"])
        fuse(s, n)
        keys = COLUMNS[(s["action"], s["kind"])]
        c.zero("LIST_NULL_KEYS_" + s["name"].upper(), f"SELECT * FROM {rel} WHERE " + " OR ".join(f"{k} IS NULL" for k in keys if k != "reason"))
        parts[_bucket(s)].append(f"SELECT * FROM {rel}")
    for bucket, selects in parts.items():
        view = {"add_award": "list_add_award", "add_number": NUMBER_LEG, "suppress": "list_suppress"}[bucket]
        c.view(view, c.artifact(view, " UNION ALL ".join(selects) if selects else EMPTY[bucket]))


def resolve_adds(c):
    """award_id ADD rows -> tonight's ACTIVE stable id (a redirected/folded id resolves to its survivor). Returns the relation."""
    r = c.r
    rel = c.artifact("list_add_resolved", f"""SELECT l.*,x.stable_id,
        CASE WHEN x.stable_id IS NULL THEN 'UNRESOLVED' WHEN a.id IS NULL THEN 'NO_DOCUMENT'
             WHEN x.stable_id<>l.award_id THEN 'REDIRECTED' ELSE 'OK' END resolution
      FROM list_add_award_v l LEFT JOIN {r}resolve_candidate x ON x.owner_id=l.award_id AND x.status='ACTIVE'
      LEFT JOIN (SELECT DISTINCT id FROM {r}awards_candidate) a ON a.id=x.stable_id""")
    bad = {row.list_name: int(row.n) for row in c.sql(
        f"SELECT list_name,count(*) n FROM {rel} WHERE resolution IN ('UNRESOLVED','NO_DOCUMENT') GROUP BY 1").collect()}
    for name, s in getattr(c, "list_specs", {}).items():
        if s["action"] == "add" and s["kind"] == "award_id":
            c.counts["list_" + name + "_unresolved"] = bad.get(name, 0)
            if bad.get(name, 0) > s["max_unresolved"]:
                raise RuntimeError(f"LIST_ADD_UNRESOLVED_FUSE: {name} unresolved={bad[name]} max={s['max_unresolved']}")
    return rel


def links_sql(c):
    """Direct ADD links for work_links (same columns: work_id, stable_id, priority, from_list)."""
    return f"SELECT work_id,stable_id,priority,true from_list FROM {c.r}list_add_resolved WHERE resolution IN ('OK','REDIRECTED')"


def apply(c, survivors, move, unsuppressed):
    """unsuppressed = work_awards rows after every leg + dedupe. Builds work_awards_candidate (minus SUPPRESS rows) and list_audit."""
    r = c.r
    on = "true" if move else "false"
    c.artifact("list_suppress_keyed", f"""SELECT s.*,coalesce(m.survivor_work_id,s.work_id) served_work_id,{G.format('s.award_id')} award_url
      FROM list_suppress_v s LEFT JOIN {survivors} m ON {on} AND m.loser_work_id=s.work_id""")
    # every ADD row of both kinds, on the served work id (merged works moved like every other link) with its resolved award
    c.artifact("list_adds", f"""SELECT a.list_name,a.list_version,a.source,a.work_id,coalesce(m.survivor_work_id,a.work_id) served_work_id,
        CAST(a.award_id AS STRING) list_award,a.stable_id,a.resolution detail FROM {r}list_add_resolved a
        LEFT JOIN {survivors} m ON {on} AND m.loser_work_id=a.work_id
      UNION ALL SELECT n.list_name,n.list_version,n.source,n.work_id,coalesce(m.survivor_work_id,n.work_id),concat(n.funder_id,':',n.number),
        x.stable_id,CASE WHEN x.stable_id IS NULL THEN 'UNMATCHED' ELSE 'number_leg' END FROM {NUMBER_LEG}_v n
        LEFT JOIN {survivors} m ON {on} AND m.loser_work_id=n.work_id
        LEFT JOIN (SELECT DISTINCT work_id,source_funder_id,award_key,stable_id FROM {r}work_matches
                   WHERE source='{NUMBER_LEG}' AND stable_id IS NOT NULL) x
          ON x.work_id=n.work_id AND x.source_funder_id=n.funder_id AND x.award_key=lower(n.number)""")
    c.zero("LIST_ADD_SUPPRESS_CONFLICT", f"""SELECT a.list_name add_list,s.list_name suppress_list,s.served_work_id,s.award_id
      FROM {r}list_adds a JOIN {r}list_suppress_keyed s ON s.served_work_id=a.served_work_id AND s.award_id=a.stable_id""")
    c.artifact("work_awards_candidate", f"""SELECT w.* FROM {unsuppressed} w
      LEFT ANTI JOIN {r}list_suppress_keyed s ON s.served_work_id=w.work_id AND s.award_url=w.award.id""")
    # ADD status: ADDED = no other leg produces the pair tonight (after merged-work moves); ALREADY_LINKED = served anyway;
    # NO_WORK = the (survivor) work is not a served work; DROPPED = resolved but missing from tonight's rows (gated to zero)
    c.artifact("list_audit", f"""WITH other AS (SELECT DISTINCT work_id,stable_id FROM {r}work_links_moved WHERE NOT from_list),
      served AS (SELECT DISTINCT l.served_work_id work_id FROM {r}list_adds l LEFT SEMI JOIN works_v v ON v.id=l.served_work_id)
      SELECT 'ADD' action,a.list_name,a.list_version,a.source,a.work_id,a.served_work_id,a.list_award,a.stable_id award_stable_id,
        CASE WHEN a.detail IN ('UNRESOLVED','NO_DOCUMENT','UNMATCHED') THEN a.detail
             WHEN sv.work_id IS NULL THEN 'NO_WORK' WHEN w.work_id IS NULL THEN 'DROPPED'
             WHEN o.work_id IS NOT NULL THEN 'ALREADY_LINKED' ELSE 'ADDED' END status,
        a.detail,CAST(NULL AS STRING) reason,w.award,:rid release_id
      FROM {r}list_adds a LEFT JOIN other o ON o.work_id=a.served_work_id AND o.stable_id=a.stable_id
      LEFT JOIN served sv ON sv.work_id=a.served_work_id
      LEFT JOIN {r}work_awards_candidate w ON w.work_id=a.served_work_id AND w.award.id={G.format('a.stable_id')}
      UNION ALL
      SELECT 'SUPPRESS',s.list_name,s.list_version,s.source,s.work_id,s.served_work_id,CAST(s.award_id AS STRING),s.award_id,
        CASE WHEN u.work_id IS NULL THEN 'NOT_SERVED' ELSE 'REMOVED' END,NULL,s.reason,u.award,:rid
      FROM {r}list_suppress_keyed s
      LEFT JOIN {unsuppressed} u ON u.work_id=s.served_work_id AND u.award.id=s.award_url""")
    rows = c.sql(f"SELECT list_name,status,count(*) n FROM {r}list_audit GROUP BY 1,2").collect()
    got = {}
    for row in rows:
        got.setdefault(row.list_name, {})[row.status] = int(row.n)
        c.counts[f"list_{row.list_name}_{row.status.lower()}"] = int(row.n)
    for name, s in getattr(c, "list_specs", {}).items():
        st = got.get(name, {})
        if s["action"] == "suppress" and st.get("NOT_SERVED", 0) > s["max_unmatched"]:
            raise RuntimeError(f"LIST_SUPPRESS_UNMATCHED_FUSE: {name} not_served={st['NOT_SERVED']} max={s['max_unmatched']}")
        bad = sum(st.get(k, 0) for k in ("UNRESOLVED", "NO_DOCUMENT", "UNMATCHED", "NO_WORK"))
        if s["action"] == "add" and bad > s["max_unresolved"]:
            raise RuntimeError(f"LIST_ADD_UNRESOLVED_FUSE: {name} unresolved_or_unmatched_or_no_work={bad} max={s['max_unresolved']}")
    c.zero("LIST_ADD_DROPPED", f"SELECT * FROM {r}list_audit WHERE status='DROPPED'")
    # exact arithmetic: tonight's rows = rows before suppression - distinct removed pairs
    before = c.count("work_awards_before_suppress", unsuppressed)
    removed = int(c.sql(f"SELECT count(DISTINCT served_work_id,award_stable_id) n FROM {r}list_audit WHERE action='SUPPRESS' AND status='REMOVED'").collect()[0].n)
    added = int(c.sql(f"SELECT count(DISTINCT served_work_id,award_stable_id) n FROM {r}list_audit WHERE action='ADD' AND status='ADDED'").collect()[0].n)
    after = c.count("work_awards_after_suppress", r + "work_awards_candidate")
    c.counts["list_suppressed_pairs"], c.counts["list_added_pairs"] = removed, added
    c.require(after == before - removed, f"LIST_SUPPRESS_ARITHMETIC: before={before} removed={removed} after={after}")
    c.zero("LIST_SUPPRESSED_GONE", f"""SELECT w.work_id,w.award.id FROM {r}work_awards_candidate w
      JOIN {r}list_suppress_keyed s ON s.served_work_id=w.work_id AND s.award_url=w.award.id""")
    link_accounting(c, before, added)


def previous(c, query, default=0):
    try:
        return int(c.sql(query).collect()[0].n or 0)
    except Exception as exc:                             # the live audit table does not exist before its DDL / first night
        if "TABLE_OR_VIEW_NOT_FOUND" not in str(exc):
            raise
        return default


def link_accounting(c, before, added):
    """Declared vs accidental link change. Lists are declared: SUPPRESS removals are bounded by their own pinned list (size fuse +
    max_unmatched), ADD additions by theirs. Everything else is the legs' own rows, measured WITHOUT the lists on both nights:
      base_now  = tonight's rows before suppression - pairs only a list added
      base_prev = last night's served rows + pairs a list removed last night - pairs only a list added last night
    so a declared suppression never spends the allowance and a list's additions never hide an accidental loss. With
    "work_links_drop_fuse" set, base_now < base_prev - max(10, fuse*base_prev) stops the night (WORK_LINK_DROP_FUSE)."""
    outputs = getattr(c, "outputs", {}) or {}
    if "work_awards" not in outputs:
        return
    prev_rows = previous(c, f"SELECT count(*) n FROM {outputs['work_awards']}")
    audit = outputs.get("list_audit")
    prev_removed = previous(c, f"SELECT count(DISTINCT served_work_id,award_stable_id) n FROM {audit} WHERE action='SUPPRESS' AND status='REMOVED'") if audit else 0
    prev_added = previous(c, f"SELECT count(DISTINCT served_work_id,award_stable_id) n FROM {audit} WHERE action='ADD' AND status='ADDED'") if audit else 0
    base_now, base_prev = before - added, prev_rows + prev_removed - prev_added
    c.counts.update(work_links_base_now=base_now, work_links_base_prev=base_prev, work_links_base_change=base_now - base_prev)
    fuse = c.config.get("work_links_drop_fuse")
    if fuse is not None:
        c.require(base_now >= base_prev - max(10, float(fuse) * base_prev),
                  f"WORK_LINK_DROP_FUSE: legs-only links {base_prev} -> {base_now} (lists excluded), fuse {fuse}")


def removed_pairs_sql(c):
    """(work_id, award url) pairs taken out by SUPPRESS tonight, on both the list's and the served work id (conservation gates
    compare against pre-move work_matches and post-move candidate edges): treated as explained removals."""
    return (f"SELECT served_work_id work_id,award_url award_id FROM {c.r}list_suppress_keyed "
            f"UNION SELECT work_id,award_url FROM {c.r}list_suppress_keyed")


def added_pairs_sql(c):
    """(work_id, award url) pairs produced by ADD lists of both kinds tonight, on the list's and the survivor work id (a producer for
    link-conservation gates)."""
    return (f"SELECT work_id,{G.format('stable_id')} award_id FROM {c.r}list_adds WHERE stable_id IS NOT NULL "
            f"UNION SELECT served_work_id,{G.format('stable_id')} FROM {c.r}list_adds WHERE stable_id IS NOT NULL")


def audit_superset(c):
    """Publication order that never drops a funder: CreateWorksEnriched keeps a SUPPRESSed link's funder through the audit, so the
    audit published BEFORE work_awards is tonight's audit plus every pair last night's audit removed that tonight's does not (a list
    rolled back or shrunk). Then work_awards, then tonight's exact audit. Every intermediate state has each removed link either in
    work_awards or in the audit. Returns the relation to publish first (None without an audit output)."""
    outputs = getattr(c, "outputs", {}) or {}
    if "list_audit" not in outputs:
        return None
    r, live = c.r, outputs["list_audit"]
    try:
        return c.artifact("list_audit_superset", f"""SELECT * FROM {r}list_audit UNION ALL
          SELECT l.* FROM {live} l LEFT ANTI JOIN {r}list_audit t ON t.action='SUPPRESS' AND t.status='REMOVED'
            AND t.served_work_id=l.served_work_id AND t.award_stable_id=l.award_stable_id
          WHERE l.action='SUPPRESS' AND l.status='REMOVED'""")
    except Exception as exc:                             # live audit not created yet (DDL line runs before the push)
        if "TABLE_OR_VIEW_NOT_FOUND" not in str(exc):
            raise
        return c.r + "list_audit"
