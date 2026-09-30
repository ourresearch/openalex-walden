"""Re-keying of OAI identifiers that carry a platform placeholder host (oxjob #1407).

repo_works is keyed on the OAI header identifier. Installs that never set their repository
identifier emit the platform default, so unrelated journals emit identical ids
(`oai:ojs.pkp.sfu.ca:article/1` from 198 endpoints) and apply_changes keeps one record per id.
Measured 2026-09-28: 335 endpoints, 1,267,040 harvested records collapsing onto 886,136 ids,
~335K distinct records lost.

The fix: when an id's host is one of DEFAULT_OAI_HOSTS, swap in the endpoint's own host
(`endpoint_id_host.id_host`, the lowercased pmh_url host, frozen once assigned):

    oai:ojs.pkp.sfu.ca:article/1  --(endpoint https://et.ippt.pan.pl/index.php/index/oai)-->
    oai:et.ippt.pan.pl:article/1

Every other id, and any id whose endpoint has no id_host row, passes through unchanged.

`keep_placeholder_host` (per endpoint, default false) keeps the placeholder inside the new host
part -- oai:<id_host>/<placeholder>:<local> -- for endpoints where dropping it would MERGE keys
that are distinct today: an aggregator re-emitting two placeholder hosts with the same local ids
for different articles (LA Referencia: ojs.localhost:article/N and ojs.pkp.sfu.ca:article/N, 3,327
such pairs; RCAAP 847). maintenance/RekeyPlaceholderOaiIds.py sets it for every endpoint whose
plain re-key would be many-to-one over current keys, so the re-key never merges two records.

ONE implementation, imported by every call site (Repo.py, RepoBackfill.py,
maintenance/RekeyPlaceholderOaiIds.py). A gate that disagrees between call sites splits one
record across two keys (the #801 lesson), so nothing may re-type this expression inline.

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
REKEY_MAP_TABLE = "openalex.repo.native_id_rekey_map"
REPLAY_TABLE = "openalex.repo.repo_replay"

# Java regex, case-insensitive on the "oai:" scheme and the host. No backslashes on purpose:
# these strings are embedded in Spark SQL literals from Python notebooks, where escape
# handling differs between f-strings, SQL cells and spark.sql().
_HOST_RE = "(?i)^oai:([^:]+):"
_LOCAL_RE = "(?i)^oai:[^:]+:(.*)$"


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
    (id-press.eu /mjms vs /index) and /perl/ vs /cgi/ EPrints endpoints share ids that are the
    same records, and a path key would split them. A host running two independent installs at
    different paths needs a hand-set `host/path` id_host instead (oxjob #1407 PLAN A4).
    """
    return (
        f"regexp_replace(lower(regexp_extract(trim({pmh_url_sql}), "
        f"'^[A-Za-z][A-Za-z0-9+.-]*://([^/:?#]+)', 1)), '^www[.]', '')"
    )


def rekey_native_id(id_col="oai_identifier", id_host_col="_id_host", keep_col="_keep_placeholder_host"):
    """Column form of rekey_native_id_sql for DataFrame code."""
    return F.expr(rekey_native_id_sql(id_col, id_host_col, keep_col))


def with_rekeyed_native_id(df, id_host_df, endpoint_col, raw_id_col="oai_identifier"):
    """Left-join the frozen endpoint -> id_host table and set native_id from `raw_id_col`.

    `id_host_df` is spark.read.table(ENDPOINT_ID_HOST_TABLE). It is broadcast: a few hundred
    rows. Works on a stream (stream-static join, stateless) and on a batch frame. The raw id
    stays in `raw_id_col` for code that parses the OAI host out of it.
    """
    hosts = F.broadcast(
        id_host_df.select(
            F.col("endpoint_id").alias("_eih_endpoint_id"),
            F.col("id_host").alias("_id_host"),
            F.col("keep_placeholder_host").alias("_keep_placeholder_host"),
        )
    )
    return (
        df.join(hosts, F.col(endpoint_col) == F.col("_eih_endpoint_id"), "left")
          .withColumn("native_id", rekey_native_id(raw_id_col, "_id_host", "_keep_placeholder_host"))
          .drop("_eih_endpoint_id", "_id_host", "_keep_placeholder_host")
    )


REPLAY_CONTROL_COLUMNS = ("replay_job", "replay_op", "replay_provenance", "replay_loaded_at")


def to_replay_rows(df, replay_schema, job, op, provenance):
    """Shape `df` into REPLAY_TABLE rows: its columns in the table's order, typed NULL where
    missing, and the replay control columns set. `op` is 'upsert' or 'delete'; `provenance` is
    'repo' or 'repo_backfill' (the _sequence provenance rank the row is processed with).
    """
    assert op in ("upsert", "delete"), op
    assert provenance in ("repo", "repo_backfill"), provenance
    control = {
        "replay_job": F.lit(job),
        "replay_op": F.lit(op),
        "replay_provenance": F.lit(provenance),
        "replay_loaded_at": F.current_timestamp(),
    }
    have = set(df.columns)
    cols = []
    for f in replay_schema.fields:
        if f.name in control:
            cols.append(control[f.name].cast(f.dataType).alias(f.name))
        elif f.name in have:
            cols.append(F.col(f.name).cast(f.dataType).alias(f.name))
        else:
            cols.append(F.lit(None).cast(f.dataType).alias(f.name))
    return df.select(*cols)


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
