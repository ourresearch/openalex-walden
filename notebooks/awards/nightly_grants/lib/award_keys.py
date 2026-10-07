"""Resolve side-table rows to stable award ids, shared by every side table (sub-award links, recipients, ...).

A side-table row names an award by a NATIVE key (namespace, source record id) when the registry knows one, else by the
producer's STAGING triple (staging id, funder id, award key); never by a G id, so rows follow merges and redirects. Both routes
are looked up in `keys_candidate` and then followed through `resolve_candidate` to the terminal id. Lifted from award_relations.py
(oxjob #1570) so the lookup is written once; the gate names keep each caller's prefix (RELATION_*, RECIPIENT_*).
"""

ENDPOINT_COLUMNS = "namespace,source_record_id,staging_id,funder_id,award_key"


def endpoint_lookup(c, prefix, endpoints, name):
    """`endpoints`: a relation with ENDPOINT_COLUMNS, one row per (namespace, source_record_id). Returns the lookup relation:
    the endpoint columns plus native_owner, staging_owner, native_id, staging_id_resolved, stable_id, status."""
    r = c.r
    c.zero(prefix + "_BRIDGE_UNIQUE", f"SELECT namespace,source_record_id FROM {endpoints} GROUP BY ALL HAVING count(*)<>1")
    # NATIVE (namespace, record id) first, else the producer's STAGING triple; then redirects to the terminal id.
    lookup = c.artifact(name, f"""SELECT o.*,kn.stable_id native_owner,ks.stable_id staging_owner,
        rn.stable_id native_id,rs.stable_id staging_id_resolved,
        coalesce(rn.stable_id,rs.stable_id) stable_id,coalesce(rn.status,rs.status) status
      FROM {endpoints} o
      LEFT JOIN {r}keys_candidate kn ON kn.key_type='NATIVE' AND kn.namespace=o.namespace AND kn.source_record_id=o.source_record_id
      LEFT JOIN {r}keys_candidate ks ON ks.key_type='STAGING' AND ks.staging_id=o.staging_id
        AND ks.funder_id <=> o.funder_id AND ks.award_key <=> o.award_key
      LEFT JOIN {r}resolve_candidate rn ON rn.owner_id=kn.stable_id
      LEFT JOIN {r}resolve_candidate rs ON rs.owner_id=ks.stable_id""")
    c.zero(prefix + "_LOOKUP_UNIQUE", f"SELECT namespace,source_record_id FROM {lookup} GROUP BY ALL HAVING count(*)<>1")
    c.zero(prefix + "_NATIVE_STAGING_DISAGREE", f"""SELECT * FROM {lookup}
      WHERE native_id IS NOT NULL AND staging_id_resolved IS NOT NULL AND native_id<>staging_id_resolved""")
    return lookup


def columns(c, relation):
    return set(c.sql(f"SELECT * FROM {relation} LIMIT 0").columns)


def count_drop_check(c, name, previous_count, now, fuse):
    """Against last night's published count: the night stops when a served quantity falls by more than max(10, fuse*before)."""
    c.checks[name] = now >= previous_count - max(10, fuse * previous_count)
    c.require(c.checks[name], f"{name}: {previous_count} -> {now}")
