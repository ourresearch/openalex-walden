# Databricks notebook source
# MAGIC %md
# MAGIC # Index rebuild and swap: control notebook (oxjob #1456)
# MAGIC
# MAGIC The standard way to reindex when a change touches most documents: build a fresh copy of the index beside the
# MAGIC live one from the Delta source pinned to one version (no replicas, no refresh, no search traffic), add the
# MAGIC replica, replay the changes since the pinned version, verify, then move the alias in one atomic call. Rollback
# MAGIC is moving the alias back. The loader is the entity's sync notebook run with `target_index` / `source_version`
# MAGIC (works: `notebooks/elastic/sync_works`; catch-up: `sync_works` + `delete_works` with `target_index` and
# MAGIC `since_ts`). This notebook does the rest, one mode per run. The procedure: oxjob #1456 PLAYBOOK.md.
# MAGIC
# MAGIC Every mode takes `old_index`, `new_index`, `alias` and `dry_run`. **`dry_run` defaults to true: print what
# MAGIC would happen and change nothing.** Every Elasticsearch or job write below sits behind `if not DRY`.
# MAGIC
# MAGIC | mode | does | writes when dry_run=false |
# MAGIC |---|---|---|
# MAGIC | `plan` | parameter sheet: old index size and tombstones, cluster disk, projected size and peak disk, pinned Delta version V, its commit time T, count at V; the load chunks' id ranges at V (published as task values `range_k`) | nothing |
# MAGIC | `create_index` | new index body = old index's LIVE mapping verbatim + its settings minus internal keys + build overrides (0 replicas, refresh -1, total_shards_per_node); saves the body to a Volume; reports what the matching templates would add (simulated) and refuses template aliases or extra fields; after the PUT the mapping must equal the old one | PUT new index |
# MAGIC | `watchdog` | runs beside the load chunks; cancels the job run when a data node's search queue or cluster disk breaks the stop rules; logs the indexing rate | cancel the run |
# MAGIC | `close_load` | refresh; count within `close_tolerance` (default 10) of the source at V; `_mget` of random source ids | refresh new index |
# MAGIC | `add_replicas` | raises recovery speed (transient), sets replicas, waits for green, restores the recorded recovery settings | cluster settings, new index settings |
# MAGIC | `live_settings` | refresh_interval back to the old index's value; total_shards_per_node back to the old index's (usually none) | new index settings |
# MAGIC | `verify` | counts; random docs vs **Delta at V** on comparable fields (the FAIL check); old-vs-new `_source` diff (informational: the old index is stale on hash-excluded fields); keyword filter counts vs Delta; PASS/FAIL summary | nothing |
# MAGIC | `swap` | one `_aliases` call moving `alias` from old to new; refuses unless new is green with the old replica count | alias |
# MAGIC | `rollback` | the reverse; lifts the old index's write block first | old index settings, alias |
# MAGIC | `block_old` | `index.blocks.write: true` on the old index; refuses while the alias still points at it | old index settings |

# COMMAND ----------

# MAGIC %pip install elasticsearch==8.19.0

# COMMAND ----------

import datetime as dt
import json
import math
import os
import random
import re
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

MODES = ("plan", "create_index", "watchdog", "close_load", "add_replicas", "live_settings", "verify", "swap",
         "rollback", "block_old")

dbutils.widgets.text("mode", "plan")
dbutils.widgets.text("old_index", "works-v34")
dbutils.widgets.text("new_index", "works-v35")
dbutils.widgets.text("alias", "works")
dbutils.widgets.text("dry_run", "true")                      # anything but "false" = change nothing
dbutils.widgets.text("source_table", "openalex.works.openalex_works")
dbutils.widgets.text("source_version", "")                   # Delta version V; "" = latest (plan only)
dbutils.widgets.text("id_prefix", "https://openalex.org/W")  # ES _id = prefix + source id
dbutils.widgets.text("if_exists", "fail")                    # create_index: "reuse" accepts an existing new index with the old mapping
dbutils.widgets.text("volume_dir", "/Volumes/openalex/works/data/es_rebuild")
dbutils.widgets.text("warehouse_id", "3996dc0a9b183ce3")
dbutils.widgets.text("sample_size", "1000")
dbutils.widgets.text("chunks", "16")                         # plan: id-range load chunks (published as task values)
dbutils.widgets.text("replicas", "")                         # add_replicas: "" = the old index's replica count
dbutils.widgets.text("recovery_max_bytes_per_sec", "250mb")  # add_replicas (ES default 40mb)
dbutils.widgets.text("node_concurrent_recoveries", "4")      # add_replicas (ES default 2)
dbutils.widgets.text("cluster_concurrent_rebalance", "")     # add_replicas: "" = leave alone (PLAN D5 suggests 8)
dbutils.widgets.text("max_wait_hours", "36")
dbutils.widgets.text("job_run_id", "")                       # watchdog: {{job.run_id}}
dbutils.widgets.text("chunk_prefix", "es_chunk_")            # watchdog: task keys of the load chunks
dbutils.widgets.text("queue_limit", "50")                    # watchdog stop rule: search queue on any data node
dbutils.widgets.text("strikes", "3")                         # consecutive 60 s checks over a limit
dbutils.widgets.text("max_disk_pct", "75")                   # watchdog stop rule: cluster disk
dbutils.widgets.text("expect_diff", "")                     # verify: fields allowed to differ old vs new (works: keywords,indexed_timestamp)
dbutils.widgets.text("keywords_table", "")                  # verify: keyword vocabulary; "" skips the keyword checks
dbutils.widgets.text("keyword_field", "keywords.id")
dbutils.widgets.text("keyword_sample", "20")
dbutils.widgets.text("count_tolerance", "1000")              # verify: |new count - source count now|
dbutils.widgets.text("delta_mismatch_pct", "1")              # verify: FAIL if more than this % of sampled docs differ from Delta at V
dbutils.widgets.text("close_tolerance", "10")                # close_load: |new count - source count at V|

MODE = dbutils.widgets.get("mode").strip()
OLD = dbutils.widgets.get("old_index").strip()
NEW = dbutils.widgets.get("new_index").strip()
ALIAS = dbutils.widgets.get("alias").strip()
DRY = dbutils.widgets.get("dry_run").strip().lower() != "false"
SOURCE_TABLE = dbutils.widgets.get("source_table").strip()
SOURCE_VERSION = dbutils.widgets.get("source_version").strip()
ID_PREFIX = dbutils.widgets.get("id_prefix").strip()
IF_EXISTS = dbutils.widgets.get("if_exists").strip()
VOLUME_DIR = dbutils.widgets.get("volume_dir").strip().rstrip("/")
WH = dbutils.widgets.get("warehouse_id").strip()
SAMPLE = int(dbutils.widgets.get("sample_size"))
CHUNKS = int(dbutils.widgets.get("chunks"))
REPLICAS = dbutils.widgets.get("replicas").strip()
RECOVERY_MBPS = dbutils.widgets.get("recovery_max_bytes_per_sec").strip()
NODE_RECOVERIES = dbutils.widgets.get("node_concurrent_recoveries").strip()
REBALANCE = dbutils.widgets.get("cluster_concurrent_rebalance").strip()
DEADLINE = time.time() + float(dbutils.widgets.get("max_wait_hours")) * 3600
JOB_RUN_ID = dbutils.widgets.get("job_run_id").strip()
CHUNK_PREFIX = dbutils.widgets.get("chunk_prefix").strip()
QUEUE_LIMIT = int(dbutils.widgets.get("queue_limit"))
STRIKES = int(dbutils.widgets.get("strikes"))
MAX_DISK_PCT = float(dbutils.widgets.get("max_disk_pct"))
EXPECT_DIFF = {f.strip() for f in dbutils.widgets.get("expect_diff").split(",") if f.strip()}
KEYWORDS_TABLE = dbutils.widgets.get("keywords_table").strip()
KEYWORD_FIELD = dbutils.widgets.get("keyword_field").strip()
KEYWORD_SAMPLE = int(dbutils.widgets.get("keyword_sample"))
COUNT_TOLERANCE = int(dbutils.widgets.get("count_tolerance"))
DELTA_MISMATCH_PCT = float(dbutils.widgets.get("delta_mismatch_pct"))
CLOSE_TOLERANCE = int(dbutils.widgets.get("close_tolerance"))

NAME_RE = r"[a-z0-9][a-z0-9._-]*"
if MODE not in MODES:
    raise ValueError(f"mode must be one of {MODES}, got {MODE!r}")
for label, v in (("old_index", OLD), ("new_index", NEW), ("alias", ALIAS)):
    if not re.fullmatch(NAME_RE, v):
        raise ValueError(f"{label} must be an index/alias name, got {v!r}")
if len({OLD, NEW, ALIAS}) != 3:
    raise ValueError("old_index, new_index and alias must be three different names")
if SOURCE_VERSION and not re.fullmatch(r"\d+", SOURCE_VERSION):
    raise ValueError(f"source_version must be a Delta version number, got {SOURCE_VERSION!r}")
if not re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+", SOURCE_TABLE):
    raise ValueError(f"source_table must be catalog.schema.table, got {SOURCE_TABLE!r}")
if IF_EXISTS not in ("fail", "reuse"):
    raise ValueError(f"if_exists must be fail or reuse, got {IF_EXISTS!r}")

w = WorkspaceClient()


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def banner():
    log(f"mode={MODE} old={OLD} new={NEW} alias={ALIAS} dry_run={DRY}"
        + ("  (DRY RUN: nothing will change)" if DRY else "  (LIVE: writes enabled)"))


def sql(statement):
    """Run SQL on the warehouse (same helper as KeywordsCatchup); rows come back as lists of strings."""
    r = w.statement_execution.execute_statement(warehouse_id=WH, statement=statement, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(10)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL {r.status.state}: {r.status.error.message if r.status.error else ''}")
    return (r.result.data_array or []) if r.result else []


def es(timeout=180):
    from elasticsearch import Elasticsearch
    return Elasticsearch(hosts=[dbutils.secrets.get(scope="elastic", key="elastic_url")], request_timeout=timeout,
                         max_retries=5, retry_on_timeout=True)


def src(version=None):
    return f"{SOURCE_TABLE} VERSION AS OF {int(version)}" if version not in (None, "") else SOURCE_TABLE


# Delta commits that change no rows; T for such a V is later than the data itself (safe for the catch-up).
NON_DATA_OPS = ("OPTIMIZE", "VACUUM START", "VACUUM END", "SET TBLPROPERTIES", "ANALYZE", "COMPUTE STATISTICS")


def pinned():
    """(V, T): source_version (or the latest version) and its commit time as 'YYYY-MM-DD HH:MM:SS' UTC."""
    where = f"WHERE version = {int(SOURCE_VERSION)}" if SOURCE_VERSION else ""
    rows = sql(f"""SELECT version, date_format(to_utc_timestamp(timestamp, current_timezone()), 'yyyy-MM-dd HH:mm:ss'),
                          operation
                   FROM (DESCRIBE HISTORY {SOURCE_TABLE}) {where} ORDER BY version DESC LIMIT 1""")
    if not rows:
        raise ValueError(f"version {SOURCE_VERSION} is not in {SOURCE_TABLE}'s history (vacuumed or not yet written)")
    v, t, op = rows[0]
    return int(v), t, op


def last_data_commit(v):
    ops = ", ".join(f"'{o}'" for o in NON_DATA_OPS)
    rows = sql(f"""SELECT version, date_format(to_utc_timestamp(timestamp, current_timezone()), 'yyyy-MM-dd HH:mm:ss'),
                          operation
                   FROM (DESCRIBE HISTORY {SOURCE_TABLE}) WHERE version <= {v} AND operation NOT IN ({ops})
                   ORDER BY version DESC LIMIT 1""")
    return rows[0] if rows else None


def source_count(version=None):
    return int(sql(f"SELECT count(*) FROM {src(version)}")[0][0])


def random_ids(n, version=None):
    """n uniformly random source ids (pre-filter by rand() so the sort is small, then shuffle-and-take)."""
    total = source_count(version)
    frac = min(1.0, 5.0 * n / max(total, 1))
    rows = sql(f"SELECT id FROM (SELECT id FROM {src(version)} WHERE rand() < {frac}) ORDER BY rand() LIMIT {n}")
    return [r[0] for r in rows]


def one(resp, name):
    """ES responses keyed by index: insist the key is exactly `name` (a concrete index, not an alias)."""
    if list(resp.keys()) != [name]:
        raise ValueError(f"{name!r} must be one concrete index, got {list(resp.keys())}")
    return resp[name]


def flat_settings(c, index):
    return one(c.indices.get_settings(index=index, flat_settings=True), index)["settings"]


def get_mapping(c, index):
    return one(c.indices.get_mapping(index=index), index)["mappings"]


def exists(c, index):
    return bool(c.indices.exists(index=index))


def data_nodes(c):
    # data roles: d data, h hot, s content (w/c/f warm/cold/frozen tiers don't hold works)
    return [n["name"] for n in c.cat.nodes(format="json", h="name,node.role") if set(n["node.role"]) & set("dhs")]


def disk(c):
    rows = [r for r in c.cat.allocation(format="json", bytes="b") if r.get("disk.total")]
    used = sum(int(r["disk.used"]) for r in rows)
    total = sum(int(r["disk.total"]) for r in rows)
    indices = sum(int(r["disk.indices"] or 0) for r in rows)
    return used, total, indices


def aliases_of(c, alias):
    """{concrete index: alias properties} for `alias`; {} if the alias doesn't exist."""
    from elasticsearch import NotFoundError
    try:
        r = c.indices.get_alias(name=alias)
    except NotFoundError:
        return {}
    return {idx: body["aliases"][alias] for idx, body in r.items()}


def health(c, index):
    return c.cluster.health(index=index)["status"]


def tb(n):
    return f"{n / 1e12:,.2f} TB"


def save(name, obj):
    """Write evidence JSON to the Volume (also in dry runs: the operator reviews the saved body)."""
    path = f"{VOLUME_DIR}/{NEW}/{name}"
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(obj, f, indent=1, sort_keys=True)
        log(f"saved {path}")
    except OSError as e:
        if not DRY:
            raise
        log(f"could not save {path} ({e}); continuing because this is a dry run")
    return path


# ---- settings and mapping helpers (pure functions; create_index / live_settings / swap)

# Settings that describe one physical index, not how to build one: never copied to the new index.
INTERNAL_EXACT = {"index.uuid", "index.creation_date", "index.provided_name", "index.history.uuid", "index.frozen",
                  "index.search.throttled", "index.verified_before_close", "index.number_of_replicas",
                  "index.refresh_interval"}
INTERNAL_PREFIXES = ("index.version.", "index.routing.allocation.", "index.blocks.", "index.resize.", "index.shrink.",
                     "index.store.snapshot.", "index.downsample.")


def is_internal(key):
    return key in INTERNAL_EXACT or key.startswith(INTERNAL_PREFIXES)


def build_settings(old, data_node_count):
    """Old index's flat settings minus internal keys, plus build overrides. Returns (settings, dropped, overrides)."""
    kept = {k: v for k, v in old.items() if not is_internal(k)}
    dropped = {k: v for k, v in old.items() if is_internal(k)}
    shards = int(old["index.number_of_shards"])
    overrides = {
        "index.number_of_replicas": "0",
        "index.refresh_interval": "-1",
        # spread primaries evenly while loading; live_settings puts the old index's value (usually none) back
        "index.routing.allocation.total_shards_per_node": str(math.ceil(shards / data_node_count)),
    }
    return {**kept, **overrides}, dropped, overrides


# Mapping parameters ES fills in when absent (8.x): stripped before comparing the verbose template with the compact
# GET _mapping form. A report aid only; the post-create check compares real GET _mapping output, which is exact.
_NUMERIC = {"coerce": True, "doc_values": True, "ignore_malformed": False, "index": True, "store": False}
MAPPING_DEFAULTS = {
    "keyword": {"doc_values": True, "eager_global_ordinals": False, "index": True, "index_options": "docs",
                "norms": False, "split_queries_on_whitespace": False, "store": False},
    "text": {"eager_global_ordinals": False, "fielddata": False, "index": True, "index_options": "positions",
             "index_phrases": False, "norms": True, "store": False},
    "object": {"type": "object", "dynamic": True, "enabled": True},
    "boolean": {"doc_values": True, "index": True, "store": False},
    "search_as_you_type": {"index": True, "index_options": "positions", "norms": True, "store": False},
    "flattened": {"doc_values": True, "eager_global_ordinals": False, "index": True,
                  "split_queries_on_whitespace": False},
    "date": {"doc_values": True, "format": "strict_date_optional_time||epoch_millis", "ignore_malformed": False,
             "index": True, "store": False},
    **{t: _NUMERIC for t in ("long", "integer", "short", "byte", "double", "float", "half_float", "scaled_float")},
}


def normalize_field(leaf, has_children):
    leaf = {k: (v.lower() == "true" if isinstance(v, str) and v.lower() in ("true", "false") else v)
            for k, v in leaf.items()}
    ftype = leaf.get("type", "object" if has_children or not leaf else None)
    for k, v in MAPPING_DEFAULTS.get(ftype, {}).items():
        if leaf.get(k) == v:
            leaf.pop(k)
    if leaf.get("search_analyzer") is not None and leaf.get("search_analyzer") == leaf.get("analyzer"):
        leaf.pop("search_analyzer")
    if leaf.get("search_quote_analyzer") is not None and leaf.get("search_quote_analyzer") in (
            leaf.get("search_analyzer"), leaf.get("analyzer")):
        leaf.pop("search_quote_analyzer")
    return leaf


def flatten_mapping(props, prefix=""):
    """{'a.b.c': normalized field definition} for every field and multi-field."""
    out = {}
    for name, d in (props or {}).items():
        path = prefix + name
        children = d.get("properties")
        out[path] = normalize_field({k: v for k, v in d.items() if k not in ("properties", "fields")}, bool(children))
        out.update(flatten_mapping(children, path + "."))
        for sub, sd in (d.get("fields") or {}).items():
            out[f"{path}.{sub}"] = normalize_field(dict(sd), False)
    return out


def mapping_drift(template_mappings, live_mappings):
    t, l = flatten_mapping(template_mappings.get("properties")), flatten_mapping(live_mappings.get("properties"))
    return {
        "template_only": sorted(set(t) - set(l)),       # would be MERGED into the new index at creation
        "live_only": sorted(set(l) - set(t)),           # drift: added to the live index, never to the template
        "differ": {f: {"template": t[f], "live": l[f]} for f in sorted(set(t) & set(l)) if t[f] != l[f]},
        "template_fields": len(t), "live_fields": len(l),
    }


def flat_template_settings(tsettings, prefix=""):
    out = {}
    for k, v in (tsettings or {}).items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(flat_template_settings(v, key + "."))
        else:
            out[key] = v
    return {(k if k.startswith("index.") else "index." + k): v for k, v in out.items()}


def show(title, obj, limit=40):
    items = list(obj.items()) if isinstance(obj, dict) else list(obj)
    print(f"{title} ({len(items)})")
    for it in items[:limit]:
        print(f"    {it}")
    if len(items) > limit:
        print(f"    ... {len(items) - limit} more (see the saved JSON)")


# COMMAND ----------

banner()

if MODE == "plan":
    c = es()
    st = c.indices.stats(index=OLD, metric="docs,store")["indices"][OLD]
    live, deleted = st["primaries"]["docs"]["count"], st["primaries"]["docs"]["deleted"]
    primary_bytes, total_bytes = st["primaries"]["store"]["size_in_bytes"], st["total"]["store"]["size_in_bytes"]
    old_settings = flat_settings(c, OLD)
    shards, replicas = int(old_settings["index.number_of_shards"]), int(old_settings.get("index.number_of_replicas", 1))
    used, total, on_indices = disk(c)
    nodes = data_nodes(c)
    new_primary = primary_bytes * live / max(live + deleted, 1)
    peak = used + new_primary * (1 + replicas)
    v, t, op = pinned()
    data_commit = last_data_commit(v)
    n_at_v = source_count(v)
    cpu = c.cat.nodes(format="json", h="name,cpu,node.role")
    cpus = [int(n["cpu"]) for n in cpu if set(n["node.role"]) & set("dhs")]
    queues = [int(r["queue"]) for r in c.cat.thread_pool(thread_pool_patterns="search", format="json", h="node_name,queue")]
    since = (dt.datetime.strptime(t, "%Y-%m-%d %H:%M:%S") - dt.timedelta(seconds=60)).strftime("%Y-%m-%d %H:%M:%S")
    # Load chunks by id RANGE at V (a range prunes files on the id-clustered table; pmod scans all of it per chunk).
    # Open first and last ends, so every id at V falls in exactly one chunk. Plus a 1/64 calibration slice.
    qs = [1 / 64] + [k / CHUNKS for k in range(1, CHUNKS)]
    pct = [int(x) for x in json.loads(sql(
        f"SELECT approx_percentile(id, array({', '.join(repr(q) for q in qs)}), 10000) FROM {src(v)}")[0][0])]
    calib, bounds = pct[0], pct[1:]
    if any(a >= b for a, b in zip(bounds, bounds[1:])):
        raise RuntimeError(f"id percentiles are not strictly increasing: {bounds}")
    ranges = [f"{bounds[k - 1] if k else ''}:{bounds[k] if k < CHUNKS - 1 else ''}" for k in range(CHUNKS)]
    try:
        for k, r in enumerate(ranges):
            dbutils.jobs.taskValues.set(key=f"range_{k}", value=r)  # the job's es_chunk_k reads {{tasks.plan.values.range_k}}
    except Exception as e:  # noqa: BLE001  (outside a job run there is nowhere to publish them)
        log(f"task values not set ({e}); fine outside a job run")
    print(f"""
| Parameter | Value |
|---|---|
| Alias / old -> new | `{ALIAS}` -> `{', '.join(aliases_of(c, ALIAS)) or 'none'}` today; `{OLD}` -> `{NEW}` (new exists: {exists(c, NEW)}) |
| Source | `{SOURCE_TABLE} VERSION AS OF {v}` |
| Pinned version V / commit time T (UTC) | {v} ({op}) / {t} |
| Last data-changing commit at or before V | {data_commit[0] + ' ' + data_commit[2] + ' at ' + data_commit[1] if data_commit else 'not in history'} |
| Documents expected (count at V) | {n_at_v:,} |
| Old index: live docs / tombstones | {live:,} / {deleted:,} ({100 * deleted / max(live, 1):.1f}% of live) |
| Old index: primary store / with replicas | {tb(primary_bytes)} / {tb(total_bytes)} |
| Shards / replicas (old) | {shards} / {replicas} |
| Data nodes / total_shards_per_node for the build | {len(nodes)} / {math.ceil(shards / max(len(nodes), 1))} |
| Projected new primary (primary x live/(live+deleted)) | {tb(new_primary)} |
| Cluster disk used / total now | {tb(used)} / {tb(total)} ({100 * used / total:.1f}%; indices {tb(on_indices)}) |
| Peak disk with the new index at {replicas} replica(s) and the old kept | {tb(peak)} ({100 * peak / total:.1f}%; rule: under 70%) |
| Data-node CPU avg / max; busiest search queue | {sum(cpus) / max(len(cpus), 1):.0f}% / {max(cpus, default=0)}%; {max(queues, default=0)} |
| Catch-up since_ts (T minus 60 s) | {since} |
| Old index refresh_interval (restored by live_settings) | {old_settings.get('index.refresh_interval', '1s (default)')} |
| Calibration slice (first 1/64 at V): id_range | `:{calib}` |
| Load chunks (id_range, lo inclusive, hi exclusive) | {CHUNKS} ranges, published as task values range_0..range_{CHUNKS - 1} |
""")
    for k, r in enumerate(ranges):
        print(f"    range_{k}: {r}")
    print("Next: create_index with dry_run=true, read the body and the template drift, then with dry_run=false.")

elif MODE == "create_index":
    c = es()
    if exists(c, NEW) and IF_EXISTS != "reuse":
        raise ValueError(f"{NEW} already exists; use if_exists=reuse to accept it (its mapping must equal {OLD}'s)")
    if SOURCE_VERSION:
        v, t, op = pinned()
        log(f"source pinned at version {v} ({op}, {t} UTC); the load chunks read {src(v)}")
    old_settings = flat_settings(c, OLD)
    old_mapping = get_mapping(c, OLD)
    nodes = data_nodes(c)
    settings, dropped, overrides = build_settings(old_settings, len(nodes))
    body = {"settings": settings, "mappings": old_mapping}
    show("Copied from the old index", {k: v for k, v in settings.items() if k not in overrides})
    show("Dropped (internal to the old index, or replaced by an override)", dropped)
    show("Build overrides", {k: f"{old_settings.get(k, '(unset)')} -> {v}" for k, v in overrides.items()})

    # Whatever templates match the new name are applied at creation: their settings fill gaps, their mappings are
    # MERGED, their aliases are ADDED. simulate_index_template shows exactly that. Report it; never change it here.
    sim = c.indices.simulate_index_template(name=NEW)
    tmpl = sim.get("template") or {}
    sim_aliases = tmpl.get("aliases") or {}
    drift = mapping_drift(tmpl.get("mappings") or {}, old_mapping)
    tsettings = flat_template_settings((tmpl.get("settings") or {}).get("index", {}))
    settings_gap = {k: v for k, v in tsettings.items() if k not in settings}
    settings_overridden = {k: {"template": v, "body": settings[k]} for k, v in tsettings.items()
                           if k in settings and str(v) != str(settings[k])}
    print(f"\nTemplates applied to {NEW} (simulated): {drift['template_fields']} fields; "
          f"{OLD} live mapping: {drift['live_fields']} fields")
    print(f"  aliases the template would add to {NEW}: {sim_aliases or 'none'}")
    print(f"  other templates overlapping {NEW}: {sim.get('overlapping') or 'none'}")
    show("  template-only fields (would be merged into the new mapping)", drift["template_only"])
    show("  live-only fields (drift: never added to the template)", drift["live_only"], limit=20)
    show("  fields defined differently (after default-normalisation)", drift["differ"], limit=20)
    show("  template settings not in the body (would apply)", settings_gap)
    show("  template settings the body overrides", settings_overridden)
    save("create_body.json", body)
    save("template_drift.json", {"simulated_for": NEW, **drift, "settings_gap": settings_gap,
                                 "settings_overridden": settings_overridden, "simulated_aliases": sim_aliases})

    problems = []
    if sim_aliases:
        problems.append(f"the template would add {NEW} to aliases {list(sim_aliases)} at creation")
    if drift["template_only"]:
        problems.append(f"the template would merge {len(drift['template_only'])} fields the old index lacks")
    if problems:
        raise RuntimeError("refusing to create: " + "; ".join(problems))

    if exists(c, NEW):
        new_mapping = get_mapping(c, NEW)
        same = json.dumps(new_mapping, sort_keys=True) == json.dumps(old_mapping, sort_keys=True)
        ns = flat_settings(c, NEW)
        log(f"{NEW} exists (if_exists=reuse): mapping {'equals' if same else 'DIFFERS FROM'} {OLD}'s; "
            f"shards {ns.get('index.number_of_shards')}, replicas {ns.get('index.number_of_replicas')}, "
            f"refresh {ns.get('index.refresh_interval')}")
        if not same or ns.get("index.number_of_shards") != old_settings["index.number_of_shards"]:
            raise RuntimeError(f"{NEW} exists but doesn't match {OLD}; delete it by hand or pick another name")
    elif DRY:
        log(f"DRY: would PUT {NEW} with the body above (saved to the Volume); nothing created")
    else:
        c.options(request_timeout=300).indices.create(index=NEW, settings=settings, mappings=old_mapping)
        log(f"created {NEW}")
        deadline = time.time() + 600
        while health(c, NEW) != "green" and time.time() < deadline:
            time.sleep(10)
        log(f"{NEW} health {health(c, NEW)}")
        new_mapping = get_mapping(c, NEW)
        if json.dumps(new_mapping, sort_keys=True) != json.dumps(old_mapping, sort_keys=True):
            save("created_mapping.json", new_mapping)
            raise RuntimeError(f"{NEW}'s mapping differs from {OLD}'s after creation (saved); do not load it")
        log(f"{NEW} mapping equals {OLD}'s (empty diff)")
        per_node = {}
        for s in c.cat.shards(index=NEW, format="json", h="node,prirep,state"):
            per_node[s["node"]] = per_node.get(s["node"], 0) + 1
        log(f"primaries per node: min {min(per_node.values())}, max {max(per_node.values())} over {len(per_node)} nodes")

elif MODE == "watchdog":
    # Moved from KeywordsCatchup (oxjob #1443): stop the load before it hurts live search. Doesn't touch merge threads.
    if not JOB_RUN_ID:
        raise ValueError("watchdog needs job_run_id ({{job.run_id}})")
    c = es()
    strikes, last = 0, None
    while time.time() < DEADLINE:
        run = w.jobs.get_run(int(JOB_RUN_ID))
        latest = {}
        for t in run.tasks:  # a retried task has one entry per attempt; the latest counts
            if t.task_key.startswith(CHUNK_PREFIX) and (t.task_key not in latest or (t.attempt_number or 0) >= (latest[t.task_key].attempt_number or 0)):
                latest[t.task_key] = t
        states = [t.state.life_cycle_state.value if t.state and t.state.life_cycle_state else None for t in latest.values()]
        if latest and all(s in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR") for s in states):
            log(f"all {len(latest)} chunks finished; watchdog done")
            break
        pools = c.nodes.stats(metric="thread_pool")["nodes"].values()
        worst = max(((n["thread_pool"]["search"]["queue"], n["name"]) for n in pools), default=(0, ""))
        used, total, _ = disk(c)
        pct = 100 * used / total
        indexed = c.indices.stats(index=NEW, metric="indexing")["indices"][NEW]["primaries"]["indexing"]["index_total"]
        now = time.time()
        if last is None or now - last[0] >= 300:
            rate = f"{(indexed - last[1]) / (now - last[0]):,.0f} docs/s" if last else "n/a"
            log(f"{NEW}: {indexed:,} docs indexed on primaries ({rate}); worst search queue {worst[0]} on {worst[1]}; "
                f"disk {pct:.1f}%; chunks running {states.count('RUNNING')}")
            last = (now, indexed)
        over = []
        if worst[0] > QUEUE_LIMIT:
            over.append(f"search queue {worst[0]} on {worst[1]} (> {QUEUE_LIMIT})")
        if pct > MAX_DISK_PCT:
            over.append(f"disk {pct:.1f}% (> {MAX_DISK_PCT}%)")
        strikes = strikes + 1 if over else 0
        if over:
            log(f"{'; '.join(over)}: strike {strikes}/{STRIKES}")
        if strikes >= STRIKES:
            if DRY:
                log(f"DRY: would cancel run {JOB_RUN_ID}")
            else:
                w.jobs.cancel_run(int(JOB_RUN_ID))
                log(f"stop rule: cancelled run {JOB_RUN_ID}; repair the run later to rerun the unfinished chunks")
            break
        time.sleep(60)
    else:
        log("watchdog deadline reached; chunks still running, not cancelling")

elif MODE == "close_load":
    if not SOURCE_VERSION:
        raise ValueError("close_load needs source_version (the pinned V)")
    c = es()
    v = int(SOURCE_VERSION)
    if DRY:
        log(f"DRY: would refresh {NEW}; counts below may lag (refresh_interval -1 during the load)")
    else:
        log(f"refreshing {NEW} (can take minutes after a full load)")
        c.options(request_timeout=3600).indices.refresh(index=NEW)
    n_new, n_src = c.count(index=NEW)["count"], source_count(v)
    log(f"{NEW}: {n_new:,} docs; {src(v)}: {n_src:,} rows; difference {n_new - n_src:+,}")
    ids = random_ids(SAMPLE, v)
    missing = []
    for i in range(0, len(ids), 500):
        docs = c.mget(index=NEW, ids=[f"{ID_PREFIX}{x}" for x in ids[i:i + 500]], source=False)["docs"]
        missing += [d["_id"] for d in docs if not d.get("found")]
    log(f"_mget {len(ids):,} random source ids at V: {len(missing)} missing {missing[:20]}")
    ok = abs(n_new - n_src) <= CLOSE_TOLERANCE and not missing
    print(f"close_load: {'PASS' if ok else 'FAIL'} (count {'equal' if n_new == n_src else f'differs by {n_new - n_src:+,}'}, "
          f"tolerance {CLOSE_TOLERANCE}, "
          f"{len(missing)} of {len(ids)} sampled ids missing)")
    if not ok and not DRY:
        raise RuntimeError("close_load FAIL: rerun the chunks that failed (repair the run) before adding replicas")

elif MODE == "add_replicas":
    c = es()
    new_settings, old_settings = flat_settings(c, NEW), flat_settings(c, OLD)
    target = int(REPLICAS) if REPLICAS else int(old_settings.get("index.number_of_replicas", 1))
    shards = int(new_settings["index.number_of_shards"])
    nodes = len(data_nodes(c))
    need = math.ceil(shards * (1 + target) / nodes)
    tspn = new_settings.get("index.routing.allocation.total_shards_per_node")
    keys = {"indices.recovery.max_bytes_per_sec": RECOVERY_MBPS,
            "cluster.routing.allocation.node_concurrent_recoveries": NODE_RECOVERIES}
    if REBALANCE:
        keys["cluster.routing.allocation.cluster_concurrent_rebalance"] = REBALANCE
    cs = c.cluster.get_settings(flat_settings=True)
    record = {k: {"transient": cs.get("transient", {}).get(k), "persistent": cs.get("persistent", {}).get(k)} for k in keys}
    save("recovery_settings_before.json", record)
    index_change = {"index.number_of_replicas": str(target)}
    if tspn and int(tspn) < need:
        # the build's ceil(shards / nodes) leaves no room for replica copies; the index would never go green
        index_change["index.routing.allocation.total_shards_per_node"] = str(need)
    log(f"recorded cluster settings: {record}")
    log(f"plan: transient {keys}; {NEW} {index_change} (replicas now {new_settings.get('index.number_of_replicas')}, "
        f"total_shards_per_node now {tspn}, need >= {need} for {shards} x {1 + target} copies on {nodes} data nodes)")
    if DRY:
        log("DRY: nothing changed")
    else:
        try:
            c.cluster.put_settings(transient=keys)
            c.indices.put_settings(index=NEW, settings=index_change)
            log("replicas requested; waiting for green")
            while time.time() < DEADLINE:
                h = c.cluster.health(index=NEW)
                if h["status"] == "green":
                    break
                active = c.cat.recovery(index=NEW, active_only=True, format="json", h="shard,stage,bytes_percent")
                log(f"{NEW} {h['status']}: initializing {h['initializing_shards']}, relocating "
                    f"{h['relocating_shards']}, unassigned {h['unassigned_shards']}, active recoveries {len(active)}")
                time.sleep(300)
            else:
                raise TimeoutError(f"{NEW} not green by the deadline")
            log(f"{NEW} green with {target} replica(s)")
        finally:
            restore = {k: record[k]["transient"] for k in keys}  # None removes the transient override
            c.cluster.put_settings(transient=restore)
            log(f"restored transient cluster settings: {restore}")

elif MODE == "live_settings":
    c = es()
    old_settings, new_settings = flat_settings(c, OLD), flat_settings(c, NEW)
    change = {
        "index.refresh_interval": old_settings.get("index.refresh_interval"),  # None = the ES default
        "index.routing.allocation.total_shards_per_node":
            old_settings.get("index.routing.allocation.total_shards_per_node"),  # None removes the build's value
    }
    log(f"{NEW}: {({k: new_settings.get(k) for k in change})} -> {change}")
    after = {**new_settings, **{k: v for k, v in change.items() if v is not None}}
    after = {k: v for k, v in after.items() if not (k in change and change[k] is None)}
    differ = {k: {"old": old_settings.get(k), "new": after.get(k)} for k in sorted(set(old_settings) | set(after))
              if not is_internal(k) and old_settings.get(k) != after.get(k)}
    show("settings that would still differ from the old index (expect none)", differ)
    if new_settings.get("index.number_of_replicas") != old_settings.get("index.number_of_replicas"):
        log(f"note: replicas new {new_settings.get('index.number_of_replicas')} vs old "
            f"{old_settings.get('index.number_of_replicas')}; run add_replicas")
    if DRY:
        log("DRY: nothing changed")
    else:
        c.indices.put_settings(index=NEW, settings=change)
        log(f"{NEW}: live settings applied")

elif MODE == "verify":
    c = es(timeout=300)
    results = []  # (check, PASS/FAIL/SKIP/INFO, detail)
    HAVE_OLD = c.indices.exists(index=OLD)  # after retire the old index is gone: only the Delta checks apply
    n_new = c.count(index=NEW)["count"]
    n_old = c.count(index=OLD)["count"] if HAVE_OLD else None
    n_now = source_count()
    n_v = source_count(SOURCE_VERSION) if SOURCE_VERSION else None
    detail = f"new {n_new:,}; " + (f"old {n_old:,} (old - new {n_old - n_new:+,}); " if HAVE_OLD else f"old {OLD} absent; ") + (
        f"source now {n_now:,}") + (f"; source at V {n_v:,}" if n_v is not None else "")
    results.append(("count: new vs source now", "PASS" if abs(n_new - n_now) <= COUNT_TOLERANCE else "FAIL", detail))

    ids = [f"{ID_PREFIX}{x}" for x in random_ids(SAMPLE)]
    docs_old, docs_new = {}, {}
    for i in range(0, len(ids), 200):
        for idx, store in (((OLD, docs_old),) if HAVE_OLD else ()) + ((NEW, docs_new),):
            for d in c.mget(index=idx, ids=ids[i:i + 200])["docs"]:
                if d.get("found"):
                    store[d["_id"]] = d["_source"]
    lost = [i for i in ids if i in docs_old and i not in docs_new]    # in old, not in new: a rebuild gap
    both_absent = [i for i in ids if i not in docs_old and i not in docs_new]  # newer than both syncs
    field_diffs, unexpected_docs, examples = {}, 0, {}
    for i in ids:
        if i in docs_old and i in docs_new:
            a, b = docs_old[i], docs_new[i]
            fields = [f for f in sorted(set(a) | set(b)) if a.get(f) != b.get(f)]
            for f in fields:
                field_diffs[f] = field_diffs.get(f, 0) + 1
                examples.setdefault(f, i)
            unexpected_docs += bool(set(fields) - EXPECT_DIFF)
    compared = sum(1 for i in ids if i in docs_old and i in docs_new)
    missing_new = [i for i in ids if i not in docs_new]
    results.append(("docs: sampled source ids present in new", "PASS" if not missing_new else "FAIL",
                    f"{len(missing_new)} of {len(ids)} missing {missing_new[:10]}"))
    if HAVE_OLD:
        results.append(("docs: in old but missing from new", "PASS" if not lost else "FAIL",
                        f"{len(lost)} of {len(ids)} {lost[:10]}; {len(both_absent)} in neither (source newer than both)"))
        show(f"fields differing old vs new over {compared} docs (expected: {sorted(EXPECT_DIFF)})",
             {f: f"{n} docs, e.g. {examples[f]}" + ("" if f in EXPECT_DIFF else "  <- UNEXPECTED")
              for f, n in sorted(field_diffs.items(), key=lambda x: -x[1])})
    pct = 100 * unexpected_docs / max(compared, 1)
    # Informational only (oxjob #1456): the old index is stale on every field the content hash excludes (fwci,
    # citation percentiles, institutions_distinct_count, source.listed_in, location `updated`, transient updated_date
    # bumps) and on null-vs-[] representation, so on 2026-10-01 88% of docs differed and every one matched Delta.
    if HAVE_OLD:
        results.append(("docs: old vs new field differences (info)", "INFO",
                        f"{unexpected_docs} of {compared} docs ({pct:.1f}%) differ outside {sorted(EXPECT_DIFF)}; "
                        f"the FAIL check is 'docs: new vs Delta at V' below"))
    # The truth check: the new index vs the source at V on fields that compare directly (no sync-side transforms).
    v_tbl = src(SOURCE_VERSION) if SOURCE_VERSION else src()
    DELTA_FIELDS = {  # es _source path -> SQL expression on the source row; both sides normalised by norm()
        "publication_year": "publication_year", "type": "type", "language": "language",
        "display_name": "title", "updated_date": "updated_date", "cited_by_count": "cited_by_count",
        "fwci": "fwci", "citation_normalized_percentile.value": "citation_normalized_percentile.value",
        "institutions_distinct_count": "institutions_distinct_count", "referenced_works_count": "referenced_works_count",
        "open_access.is_oa": "open_access.is_oa", "open_access.oa_status": "open_access.oa_status",
        "primary_location.source.id": "primary_location.source.id", "authorships_count": "size(authorships)",
        "keywords_ids": "to_json(sort_array(transform(keywords, k -> k.id)))",
        "topics_ids": "to_json(sort_array(transform(topics, t -> t.id)))",
    }
    def dig(doc, path):
        for part in path.split("."):
            doc = doc.get(part) if isinstance(doc, dict) else None
        return doc
    def es_value(doc, key):
        if key == "authorships_count":  # `authorships` is truncated at 100 in ES; `authorships_full` is not
            return len(doc.get("authorships_full") or doc.get("authorships") or [])
        if key in ("keywords_ids", "topics_ids"):
            return json.dumps(sorted(k.get("id") for k in (doc.get(key.split("_")[0]) or []) if k.get("id")))
        return dig(doc, key)
    def norm(v):
        if v is None or v == "" or v == "[]":
            return ""
        if isinstance(v, str) and v.startswith("["):
            try:
                return json.dumps(json.loads(v), separators=(",", ":"), sort_keys=True)  # Spark and Python space JSON differently
            except ValueError:
                pass
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, float) or (isinstance(v, str) and re.fullmatch(r"-?\d+\.\d+(E-?\d+)?", v)):
            return f"{float(v):.4g}"
        v = str(v)
        if re.match(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", v):
            return v[:19].replace("T", " ")  # timestamps: second precision, ignore T/space and zone
        return v
    sample_ids = [i for i in ids if i in docs_new]
    delta_rows = {}
    for i in range(0, len(sample_ids), 500):
        batch = sample_ids[i:i + 500]
        ints = ", ".join(x[len(ID_PREFIX):] for x in batch)
        cols = ", ".join(f"{expr} AS c{n}" for n, expr in enumerate(DELTA_FIELDS.values()))
        for r in sql(f"SELECT id, {cols} FROM {v_tbl} WHERE id IN ({ints})"):
            delta_rows[f"{ID_PREFIX}{r[0]}"] = dict(zip(DELTA_FIELDS.keys(), r[1:]))
    delta_diffs, delta_bad_docs, delta_examples, resynced = {}, 0, {}, 0
    for i in sample_ids:
        row = delta_rows.get(i)
        if row is None:
            if SOURCE_VERSION:
                resynced += 1  # sampled from the source now; created after V, so no row at V
                continue
            delta_diffs["(missing from Delta)"] = delta_diffs.get("(missing from Delta)", 0) + 1
            delta_examples.setdefault("(missing from Delta)", (i, "present in ES", "no row"))
            delta_bad_docs += 1
            continue
        if SOURCE_VERSION and norm(es_value(docs_new[i], "updated_date")) > norm(row["updated_date"]):
            resynced += 1  # re-sent by a sync after V (catch-up or nightly): newer than the pinned source, not comparable
            continue
        bad = [k for k in DELTA_FIELDS if norm(es_value(docs_new[i], k)) != norm(row[k])]
        for k in bad:
            delta_diffs[k] = delta_diffs.get(k, 0) + 1
            delta_examples.setdefault(k, (i, es_value(docs_new[i], k), row[k]))
        delta_bad_docs += bool(bad)
    show(f"fields differing new vs Delta at V over {len(sample_ids)} docs",
         {k: f"{n} docs, e.g. {delta_examples[k][0]} es={str(delta_examples[k][1])[:60]!r} delta={str(delta_examples[k][2])[:60]!r}"
          for k, n in sorted(delta_diffs.items(), key=lambda x: -x[1])} if delta_diffs else {"(none)": "every compared field equal"})
    comparable = len(sample_ids) - resynced
    dpct = 100 * delta_bad_docs / max(comparable, 1)
    results.append(("docs: new vs Delta at V", "PASS" if dpct <= DELTA_MISMATCH_PCT else "FAIL",
                    f"{delta_bad_docs} of {comparable} docs ({dpct:.1f}%; limit {DELTA_MISMATCH_PCT}%) differ from "
                    f"{v_tbl} on {len(DELTA_FIELDS)} comparable fields; {resynced} skipped as re-synced after V"))

    if KEYWORDS_TABLE:
        col = KEYWORD_FIELD.split(".")[0]
        sub = KEYWORD_FIELD.split(".", 1)[1]
        used = [k[sub] for s in docs_new.values() for k in (s.get(col) or []) if k.get(sub)]
        vocab = set()
        uniq = sorted(set(used))
        for i in range(0, len(uniq), 1000):
            quoted = ", ".join("'" + k.replace("'", "''") + "'" for k in uniq[i:i + 1000])
            vocab |= {r[0] for r in sql(f"SELECT id FROM {KEYWORDS_TABLE} WHERE id IN ({quoted})")}
        stale = sorted(set(uniq) - vocab)
        results.append((f"keywords: sampled docs carry only {KEYWORDS_TABLE} ids", "PASS" if not stale else "FAIL",
                        f"{len(stale)} of {len(uniq)} ids not in the vocabulary {stale[:10]}"))
        # half uniform over the vocabulary (mostly small keywords), half weighted by use (mostly large ones)
        pick = [r[0] for r in sql(f"SELECT id FROM {KEYWORDS_TABLE} ORDER BY rand() LIMIT {KEYWORD_SAMPLE // 2}")]
        for k in random.Random(1456).sample(used, len(used)):  # `used` repeats ids by use, so big keywords come first
            if len(pick) >= KEYWORD_SAMPLE:
                break
            if k not in pick:
                pick.append(k)
        quoted = ", ".join("'" + k.replace("'", "''") + "'" for k in pick)
        def delta_counts(version):
            return {r[0]: int(r[1]) for r in sql(
                f"SELECT kw.{sub}, count(DISTINCT wid) FROM (SELECT id AS wid, explode({col}) AS kw FROM {src(version)}) "
                f"WHERE kw.{sub} IN ({quoted}) GROUP BY kw.{sub}")}
        d_now = delta_counts(None)
        d_v = delta_counts(SOURCE_VERSION) if SOURCE_VERSION else {}
        bad = 0
        print(f"{'keyword':70} {'ES new':>9} {'ES old':>9} {'Delta now':>9} {'Delta V':>9}")
        for k in pick:
            e_new = c.count(index=NEW, query={"term": {KEYWORD_FIELD: k}})["count"]
            e_old = c.count(index=OLD, query={"term": {KEYWORD_FIELD: k}})["count"] if HAVE_OLD else 0
            dn, dv = d_now.get(k, 0), d_v.get(k)
            tol = max(2, 0.001 * dn)
            ok = abs(e_new - dn) <= tol or (dv is not None and abs(e_new - dv) <= tol)
            bad += not ok
            print(f"{k[:70]:70} {e_new:>9,} {e_old:>9,} {dn:>9,} {'' if dv is None else f'{dv:,}':>9}{'' if ok else '  <- off'}")
        results.append((f"keywords: {KEYWORD_FIELD} filter counts vs Delta ({len(pick)} keywords)",
                        "PASS" if not bad else "FAIL", f"{bad} off by more than max(2, 0.1%)"))
    else:
        results.append(("keywords", "SKIP", "keywords_table is empty"))

    # TODO(oxjob #1456, PLAN Phase 1): replay the 500 real API queries (search, filters incl. keywords.id, group_by,
    # sorts, cursor paging) against OLD and NEW: hits.total within 0.1% (exact for filters), top-25 ids equal for
    # sorted queries, same group_by buckets, p50/p95 within 10% after warm-up. The query set doesn't exist yet.
    results.append(("query replay (500 real API queries)", "SKIP", "TODO: query set not built yet"))

    print("\nverify summary")
    for check, status, det in results:
        print(f"  {status:4}  {check}: {det}")
    failed = [r for r in results if r[1] == "FAIL"]
    print(f"\nOVERALL: {'FAIL' if failed else 'PASS'} ({len(failed)} failed, "
          f"{sum(r[1] == 'SKIP' for r in results)} skipped)")
    save("verify.json", {"results": results, "field_diffs": field_diffs, "examples": examples,
                         "delta_diffs": delta_diffs, "delta_examples": {k: list(v) for k, v in delta_examples.items()}})
    if failed:
        raise RuntimeError(f"verify FAIL: {[r[0] for r in failed]}")

elif MODE == "swap":
    c = es()
    current = aliases_of(c, ALIAS)
    old_s, new_s = flat_settings(c, OLD), flat_settings(c, NEW)
    problems = []
    if list(current) != [OLD]:
        problems.append(f"alias {ALIAS} points at {list(current) or 'nothing'}, expected only {OLD}")
    if health(c, NEW) != "green":
        problems.append(f"{NEW} is {health(c, NEW)}, not green")
    if new_s.get("index.number_of_replicas") != old_s.get("index.number_of_replicas"):
        problems.append(f"{NEW} has {new_s.get('index.number_of_replicas')} replicas, {OLD} has "
                        f"{old_s.get('index.number_of_replicas')} (run add_replicas)")
    if new_s.get("index.refresh_interval") == "-1":
        problems.append(f"{NEW} still has refresh_interval -1 (run live_settings)")
    if str(new_s.get("index.blocks.write", "false")).lower() == "true":
        problems.append(f"{NEW} is write-blocked")
    props = current.get(OLD, {})  # carry filter / routing / is_write_index over to the new index
    body = {"actions": [{"remove": {"index": OLD, "alias": ALIAS}},
                        {"add": {"index": NEW, "alias": ALIAS, **props}}]}
    print("POST _aliases\n" + json.dumps(body, indent=1))
    print(f"Reminder: every reader and writer must use `{ALIAS}`, not `{OLD}` (PLAN Phase 0), or the nightly keeps "
          f"writing {OLD} by name after this swap.")
    if problems:
        raise RuntimeError("refusing to swap: " + "; ".join(problems))
    if DRY:
        log("DRY: alias unchanged")
    else:
        c.indices.update_aliases(actions=body["actions"])
        log(f"swapped: {ALIAS} -> {list(aliases_of(c, ALIAS))}. Next: watch the proxy 30 min; move the sync "
            f"watermark row if it is keyed by {OLD}; block_old.")

elif MODE == "rollback":
    c = es()
    current = aliases_of(c, ALIAS)
    if list(current) != [NEW]:
        raise RuntimeError(f"refusing to roll back: alias {ALIAS} points at {list(current) or 'nothing'}, expected only {NEW}")
    if health(c, OLD) == "red":
        raise RuntimeError(f"refusing to roll back: {OLD} is red")
    blocked = str(flat_settings(c, OLD).get("index.blocks.write", "false")).lower() == "true"
    props = current.get(NEW, {})
    body = {"actions": [{"remove": {"index": NEW, "alias": ALIAS}},
                        {"add": {"index": OLD, "alias": ALIAS, **props}}]}
    print(("PUT " + OLD + "/_settings {\"index.blocks.write\": null}\n" if blocked else "")
          + "POST _aliases\n" + json.dumps(body, indent=1))
    if DRY:
        log("DRY: nothing changed")
    else:
        if blocked:
            c.indices.put_settings(index=OLD, settings={"index.blocks.write": None})
            log(f"{OLD}: write block lifted")
        c.indices.update_aliases(actions=body["actions"])
        log(f"rolled back: {ALIAS} -> {list(aliases_of(c, ALIAS))}. Next: replay writes made since the swap into "
            f"{OLD} (sync_works since_ts=<swap time>, delete_works since_ts=<swap time>).")

elif MODE == "block_old":
    c = es()
    current = aliases_of(c, ALIAS)
    if OLD in current:
        raise RuntimeError(f"refusing: alias {ALIAS} still points at {OLD}; swap first")
    print(f"PUT {OLD}/_settings {{\"index.blocks.write\": true}}")
    print(f"Reminder: anything still writing {OLD} by name (the nightly before PLAN Phase 0) will fail after this.")
    if DRY:
        log("DRY: nothing changed")
    else:
        c.indices.put_settings(index=OLD, settings={"index.blocks.write": True})
        log(f"{OLD}: writes blocked; keep it 48 hours, then delete it with the owner's OK")
