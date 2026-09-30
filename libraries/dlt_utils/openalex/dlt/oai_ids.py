"""Re-keying of OAI identifiers that carry a platform placeholder host (oxjob #1407).

repo_works is keyed on the OAI header identifier. Installs that never set their repository
identifier emit the platform default, so unrelated journals emit identical ids
(`oai:ojs.pkp.sfu.ca:article/1` from 198 endpoints) and apply_changes keeps one record per id.
Measured 2026-09-28: 335 endpoints, 1,267,040 harvested records collapsing onto 886,136 ids,
~335K distinct records lost.

The fix: for an endpoint listed in ENDPOINT_ID_HOST_TABLE, an id whose host is one of
DEFAULT_OAI_HOSTS gets the endpoint's own host instead (`id_host`, the lowercased pmh_url host,
frozen once assigned):

    oai:ojs.pkp.sfu.ca:article/1  --(endpoint https://et.ippt.pan.pl/index.php/index/oai)-->
    oai:et.ippt.pan.pl:article/1

Every other id, and every id of an endpoint with no row, passes through unchanged. A row must be
written BEFORE the endpoint's first harvest: an endpoint whose placeholder records are already
stored would have them split across the old and new keys (moving stored keys is oxjob #1407 Phase B).

`keep_placeholder_host` (per endpoint, default false) keeps the placeholder inside the new host
part -- oai:<id_host>/<placeholder>:<local> -- for aggregators that re-emit two placeholder hosts
with the same local ids for different articles (LA Referencia: ojs.localhost:article/N and
ojs.pkp.sfu.ca:article/N), where dropping it would merge two records.

ONE implementation, imported by every call site (Repo.py, RepoBackfill.py). A gate that
disagrees between call sites splits one record across two keys (the #801 lesson), so nothing may
re-type this expression inline.

Hosts were chosen from data, not a guess list (oxjob #1407 EXPLORE §3): for every OAI host,
the ids harvested by 2+ endpoints were checked for identical titles. These four sit at
93-100% different titles; genuinely shared hosts (hal, arxiv.org) sit at ~0.3%, and nothing
falls in between. Adding a host here re-keys every record carrying it: size it first.
"""

import re

import pyspark.sql.functions as F

DEFAULT_OAI_HOSTS = (
    "ojs.pkp.sfu.ca",       # OJS 2 default
    "generic.eprints.org",  # EPrints default
    "localhost",            # DSpace (and some OJS) with an unset dspace.hostname
    "ojs.localhost",        # OJS installs configured against localhost
)

ENDPOINT_ID_HOST_TABLE = "openalex.repo.endpoint_id_host"

# Java regex, case-insensitive on the "oai:" scheme and the host. No backslashes on purpose:
# these strings are embedded in Spark SQL literals from Python notebooks, where escape
# handling differs between f-strings, SQL cells and spark.sql().
_HOST_RE = "(?i)^oai:([^:]+):"
_LOCAL_RE = "(?i)^oai:[^:]+:(.*)$"

# What may appear inside the generated SQL literals.
_ENDPOINT_ID_PY = re.compile(r"^[A-Za-z0-9_-]+$")
_ID_HOST_PY = re.compile(r"^[a-z0-9.-]+(/[a-z0-9._~-]+)*$")


def _sql_str_list(values):
    return ", ".join("'" + v + "'" for v in values)


def oai_host_sql(id_sql):
    """SQL: the lowercased host of an OAI identifier ('' when it is not oai:<host>:<local>)."""
    return f"lower(regexp_extract({id_sql}, '{_HOST_RE}', 1))"


def is_placeholder_id_sql(id_sql):
    """SQL predicate: the id's host is one of DEFAULT_OAI_HOSTS."""
    return f"{oai_host_sql(id_sql)} IN ({_sql_str_list(DEFAULT_OAI_HOSTS)})"


def rekey_native_id_sql(id_sql, id_host_sql, keep_placeholder_sql="false"):
    """SQL: the re-keyed native_id. Unchanged unless the host is a placeholder AND id_host is set."""
    return (
        f"CASE WHEN {id_host_sql} IS NOT NULL AND {id_host_sql} <> '' "
        f"AND {is_placeholder_id_sql(id_sql)} "
        f"THEN concat('oai:', {id_host_sql}, "
        f"CASE WHEN coalesce({keep_placeholder_sql}, false) THEN concat('/', {oai_host_sql(id_sql)}) ELSE '' END, "
        f"':', regexp_extract({id_sql}, '{_LOCAL_RE}', 1)) "
        f"ELSE {id_sql} END"
    )


def pmh_url_id_host_sql(pmh_url_sql):
    """SQL: the id_host for an endpoint's pmh_url -- lowercased host, no scheme, port or leading www.

    Host only, not path: journal-scoped and install-wide endpoints of one install
    (id-press.eu /mjms vs /index) share ids that are the same records, and a path key would split
    them. A host running two independent installs at different paths needs a hand-set `host/path`
    id_host instead (oxjob #1407 PLAN A4).
    """
    return (
        f"regexp_replace(lower(regexp_extract(trim({pmh_url_sql}), "
        f"'^[A-Za-z][A-Za-z0-9+.-]*://([^/:?#]+)', 1)), '^www[.]', '')"
    )


def read_endpoint_id_hosts(spark, table=ENDPOINT_ID_HOST_TABLE):
    """The endpoint -> (id_host, keep_placeholder_host) rows, collected (a few hundred at most)."""
    return [(r.endpoint_id, r.id_host, bool(r.keep_placeholder_host))
            for r in spark.read.table(table).select("endpoint_id", "id_host", "keep_placeholder_host").collect()]


def _lookup_sql(endpoint_sql, pairs, default):
    whens = " ".join(f"WHEN '{k}' THEN {v}" for k, v in pairs)
    return f"CASE {endpoint_sql} {whens} ELSE {default} END"


def with_rekeyed_native_id(df, id_hosts, endpoint_col, raw_id_col="oai_identifier"):
    """Set native_id from `raw_id_col`, re-keyed for the endpoints in `id_hosts`
    (read_endpoint_id_hosts). The raw id stays in `raw_id_col` for code that parses the OAI host.

    The endpoint lookup is inlined as a literal CASE, not a join: in a streaming query this is a
    plain projection, so adding it (or a new row, picked up when the next pipeline update starts)
    never changes the query's stateful plan.
    """
    seen = set()
    for endpoint_id, id_host, _ in id_hosts:
        if not _ENDPOINT_ID_PY.match(endpoint_id or "") or not _ID_HOST_PY.match(id_host or ""):
            raise ValueError(f"{ENDPOINT_ID_HOST_TABLE}: bad row {endpoint_id!r} -> {id_host!r}")
        if endpoint_id in seen:
            raise ValueError(f"{ENDPOINT_ID_HOST_TABLE}: endpoint {endpoint_id} has more than one row")
        seen.add(endpoint_id)
    if not id_hosts:
        return df.withColumn("native_id", F.col(raw_id_col))
    ep = f"`{endpoint_col}`"
    host_sql = _lookup_sql(ep, [(e, f"'{h}'") for e, h, _ in id_hosts], "CAST(NULL AS STRING)")
    keep = [(e, "true") for e, _, k in id_hosts if k]
    keep_sql = _lookup_sql(ep, keep, "false") if keep else "false"
    return (
        df.withColumn("_id_host", F.expr(host_sql))
          .withColumn("_keep_placeholder_host", F.expr(keep_sql))
          .withColumn("native_id", F.expr(rekey_native_id_sql(f"`{raw_id_col}`", "_id_host", "_keep_placeholder_host")))
          .drop("_id_host", "_keep_placeholder_host")
    )


# --- pure-Python reference, for tests and for checking the SQL against a warehouse ---------

_HOST_PY = re.compile(r"^oai:([^:]+):", re.IGNORECASE)
_LOCAL_PY = re.compile(r"^oai:[^:]+:(.*)$", re.IGNORECASE | re.DOTALL)


def rekey_native_id_py(native_id, id_host, keep_placeholder_host=False):
    if native_id is None or not id_host:
        return native_id
    m = _HOST_PY.match(native_id)
    if not m or m.group(1).lower() not in DEFAULT_OAI_HOSTS:
        return native_id
    host = f"{id_host}/{m.group(1).lower()}" if keep_placeholder_host else id_host
    return f"oai:{host}:{_LOCAL_PY.match(native_id).group(1)}"


def pmh_url_id_host_py(pmh_url):
    if pmh_url is None:
        return None
    m = re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://([^/:?#]+)", pmh_url.strip())
    host = (m.group(1) if m else "").lower()
    return re.sub(r"^www[.]", "", host)
