"""Re-keying of OAI identifiers that carry a platform placeholder host (oxjob #1407).

repo_works is keyed on the OAI header identifier. Installs that never set their repository
identifier emit the platform default, so unrelated journals emit identical ids
(`oai:ojs.pkp.sfu.ca:article/1` from 198 endpoints) and apply_changes keeps one record per id.
Measured 2026-09-28: 335 endpoints, 1,267,040 harvested records collapsing onto 886,136 ids,
~335K distinct records lost.

The fix: an id whose host is one of DEFAULT_OAI_HOSTS gets the endpoint's own host instead (the
lowercased host of its registered pmh_url, ENDPOINT_REGISTRY_TABLE):

    oai:ojs.pkp.sfu.ca:article/1  --(endpoint https://et.ippt.pan.pl/index.php/index/oai)-->
    oai:et.ippt.pan.pl:article/1

Automatic for every endpoint except oai_ids_excluded.PLACEHOLDER_REKEY_EXCLUDED_ENDPOINTS (TEMPORARY:
the endpoints whose placeholder records were already stored on 2026-09-30; re-keying their new
harvests would split them until Phase B moves their stored keys). Every other id passes through
unchanged. The host follows the pmh_url: an endpoint that moves domain re-keys, exactly as a
correctly configured install's own ids do when it moves.

`keep_placeholder_host` (rekey_native_id_sql, default false) keeps the placeholder inside the new
host part -- oai:<id_host>/<placeholder>:<local> -- for aggregators that re-emit two placeholder
hosts with the same local ids for different articles (LA Referencia); Phase B needs it.

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

from openalex.dlt.oai_ids_excluded import PLACEHOLDER_REKEY_EXCLUDED_ENDPOINTS

ENDPOINT_REGISTRY_TABLE = "openalex_sources.public.oai_pmh_endpoint"

# Java regex, case-insensitive on the "oai:" scheme and the host. No backslashes on purpose:
# these strings are embedded in Spark SQL literals from Python notebooks, where escape
# handling differs between f-strings, SQL cells and spark.sql().
_HOST_RE = "(?i)^oai:([^:]+):"
_LOCAL_RE = "(?i)^oai:[^:]+:(.*)$"

# What may appear inside the endpoint -> host map.
_ENDPOINT_ID_PY = re.compile(r"^[A-Za-z0-9_-]+$")
_ID_HOST_PY = re.compile(r"^[a-z0-9-]+([.][a-z0-9-]+)+$")


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


def endpoint_hosts(pmh_urls, excluded=PLACEHOLDER_REKEY_EXCLUDED_ENDPOINTS):
    """{endpoint_id: host} for re-keying, from (endpoint_id, pmh_url) pairs.

    Left out (their ids stay unchanged): excluded endpoints; any endpoint on the same host as an
    excluded one, since it serves the same install whose records are stored under the placeholder
    keys (a set-scoped endpoint split off an install-wide one, oxjob #1418); and endpoints whose
    pmh_url yields no usable host.
    """
    pairs = [(e, pmh_url_id_host_py(u)) for e, u in pmh_urls]
    held_hosts = {h for e, h in pairs if e in excluded and h}
    return {e: h for e, h in pairs
            if e not in excluded and _ENDPOINT_ID_PY.match(e or "")
            and h and _ID_HOST_PY.match(h) and h not in held_hosts}


def read_endpoint_hosts(spark, table=ENDPOINT_REGISTRY_TABLE):
    """endpoint_hosts() over the endpoint registry, read once (a few thousand rows). Fails loudly if
    the registry can't be read: running without it would store new records under placeholder keys."""
    return endpoint_hosts((r.id, r.pmh_url) for r in spark.read.table(table).select("id", "pmh_url").collect())


def with_rekeyed_native_id(df, hosts, endpoint_col, raw_id_col="oai_identifier"):
    """Set native_id from `raw_id_col`, re-keyed with `hosts` ({endpoint_id: host},
    read_endpoint_hosts). The raw id stays in `raw_id_col` for code that parses the OAI host.

    The map is a literal, looked up only for placeholder ids: in a streaming query this is a plain
    projection, so it never changes the query's stateful plan (checked against an existing
    dropDuplicates checkpoint on Spark 4.2), and registry changes apply from the next update.
    """
    raw = F.col(raw_id_col)
    if not hosts:
        return df.withColumn("native_id", raw)
    keys = sorted(hosts)
    host_map = F.map_from_arrays(F.lit(keys), F.lit([hosts[k] for k in keys]))
    id_host = F.when(F.expr(is_placeholder_id_sql(f"`{raw_id_col}`")),
                     F.try_element_at(host_map, F.col(endpoint_col)))
    return (
        df.withColumn("_id_host", id_host)
          .withColumn("native_id", F.expr(rekey_native_id_sql(f"`{raw_id_col}`", "_id_host")))
          .drop("_id_host")
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
