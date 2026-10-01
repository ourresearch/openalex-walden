"""Sub-award links: typed award->award relations, resolved to stable ids every night and projected onto the award document.

Input (optional extra input `award_relations_raw`): one row per source assertion, keyed by source-native ids plus the producer
STAGING triple (never a G id), so links follow merges and redirects. Output: `relations_candidate` (one row per child, parent, type)
and extra columns on awards_candidate: parent_awards, sub_awards (first 100), sub_awards_count, and uncapped *_full siblings for
search filters. Relations never create or change work links, ids, or any other award field.

Failure policy (the grants nightly must not stop because one award went away):
- stops the run: malformed or duplicate input rows, a source key bridging to two records, a key that resolves inconsistently,
  a cycle, two relationship types on one pair, or more links dropped tonight than the fuse allows;
- dropped and counted: links whose end is no longer an active served award, or whose two ends were merged into one id.
"""

RELATION_TYPES = ("component", "sub_award")

EMPTY_RAW = """SELECT CAST(NULL AS STRING) source,CAST(NULL AS STRING) relation_type,
  CAST(NULL AS STRING) child_ns,CAST(NULL AS STRING) child_key,CAST(NULL AS STRING) parent_ns,CAST(NULL AS STRING) parent_key,
  CAST(NULL AS BIGINT) child_staging_id,CAST(NULL AS BIGINT) child_funder_id,CAST(NULL AS STRING) child_award_key,
  CAST(NULL AS BIGINT) parent_staging_id,CAST(NULL AS BIGINT) parent_funder_id,CAST(NULL AS STRING) parent_award_key,
  CAST(NULL AS STRING) evidence_uri,CAST(NULL AS STRING) evidence_json,CAST(NULL AS STRING) confidence,
  CAST(NULL AS STRING) asserted_by,CAST(NULL AS STRING) adapter_version,CAST(NULL AS STRING) batch_id,
  CAST(NULL AS TIMESTAMP) asserted_at WHERE false"""

LINK = """named_struct('id',concat('https://openalex.org/G',a.id),'display_name',a.display_name,
  'funder',named_struct('id',a.funder.id,'display_name',a.funder.display_name),
  'relationship',n.relation_type,'asserted_by',n.asserted_by)"""


def columns(c, relation):
    return set(c.sql(f"SELECT * FROM {relation} LIMIT 0").columns)


def raw_input(c):
    if c.has_extra("award_relations_raw"):
        return "award_relations_raw_v"
    # No input configured: allowed only while nothing is linked yet, so links can never vanish silently.
    if "sub_awards_count" in columns(c, "previous_api_v"):
        c.zero("RELATION_INPUT_REQUIRED", "SELECT id FROM previous_api_v WHERE sub_awards_count>0 OR size(parent_awards)>0")
    return c.artifact("relations_raw_empty", EMPTY_RAW)


def run(c):
    c.guard(); r = c.r
    raw = raw_input(c)
    types = ",".join(f"'{t}'" for t in RELATION_TYPES)
    c.zero("RELATION_RAW_SHAPE", f"""SELECT * FROM {raw} WHERE NOT coalesce(relation_type IN ({types})
      AND length(source)>0 AND length(child_ns)>0 AND length(child_key)>0 AND length(parent_ns)>0 AND length(parent_key)>0
      AND length(evidence_uri)>0 AND length(evidence_json)>0 AND confidence IN ('high','medium') AND asserted_by IN ('funder','curated')
      AND length(adapter_version)>0 AND length(batch_id)>0 AND asserted_at IS NOT NULL,false)""")
    c.zero("RELATION_RAW_UNIQUE", f"SELECT source,relation_type,child_ns,child_key,parent_ns,parent_key FROM {raw} GROUP BY ALL HAVING count(*)<>1")
    c.artifact("relation_endpoints", f"""SELECT child_ns namespace,child_key source_record_id,child_staging_id staging_id,
        child_funder_id funder_id,child_award_key award_key FROM {raw}
      UNION SELECT parent_ns,parent_key,parent_staging_id,parent_funder_id,parent_award_key FROM {raw}""")
    c.zero("RELATION_BRIDGE_UNIQUE", f"SELECT namespace,source_record_id FROM {r}relation_endpoints GROUP BY ALL HAVING count(*)<>1")
    # NATIVE (namespace, record id) first, else the producer's STAGING triple; then redirects to the terminal id.
    c.artifact("relation_endpoint_lookup", f"""SELECT o.*,kn.stable_id native_owner,ks.stable_id staging_owner,
        rn.stable_id native_id,rs.stable_id staging_id_resolved,
        coalesce(rn.stable_id,rs.stable_id) stable_id,coalesce(rn.status,rs.status) status
      FROM {r}relation_endpoints o
      LEFT JOIN {r}keys_candidate kn ON kn.key_type='NATIVE' AND kn.namespace=o.namespace AND kn.source_record_id=o.source_record_id
      LEFT JOIN {r}keys_candidate ks ON ks.key_type='STAGING' AND ks.staging_id=o.staging_id
        AND ks.funder_id <=> o.funder_id AND ks.award_key <=> o.award_key
      LEFT JOIN {r}resolve_candidate rn ON rn.owner_id=kn.stable_id
      LEFT JOIN {r}resolve_candidate rs ON rs.owner_id=ks.stable_id""")
    c.zero("RELATION_LOOKUP_UNIQUE", f"SELECT namespace,source_record_id FROM {r}relation_endpoint_lookup GROUP BY ALL HAVING count(*)<>1")
    c.zero("RELATION_NATIVE_STAGING_DISAGREE", f"""SELECT * FROM {r}relation_endpoint_lookup
      WHERE native_id IS NOT NULL AND staging_id_resolved IS NOT NULL AND native_id<>staging_id_resolved""")
    c.artifact("relations_resolved", f"""SELECT a.*,ch.stable_id child_id,pa.stable_id parent_id,
        CASE WHEN ch.stable_id IS NULL OR pa.stable_id IS NULL THEN 'UNRESOLVED'
             WHEN ch.status<>'ACTIVE' OR pa.status<>'ACTIVE' OR dc.id IS NULL OR dp.id IS NULL THEN 'NOT_SERVED'
             WHEN ch.stable_id=pa.stable_id THEN 'MERGED_TO_ONE' END dropped
      FROM {raw} a
      JOIN {r}relation_endpoint_lookup ch ON ch.namespace=a.child_ns AND ch.source_record_id=a.child_key
      JOIN {r}relation_endpoint_lookup pa ON pa.namespace=a.parent_ns AND pa.source_record_id=a.parent_key
      LEFT JOIN {r}awards_candidate dc ON dc.id=ch.stable_id LEFT JOIN {r}awards_candidate dp ON dp.id=pa.stable_id""")
    c.zero("RELATION_NO_LOSS", f"SELECT 1 WHERE (SELECT count(*) FROM {raw})<>(SELECT count(*) FROM {r}relations_resolved)")
    for reason in ("UNRESOLVED", "NOT_SERVED", "MERGED_TO_ONE"):
        c.counts["relations_dropped_" + reason.lower()] = int(c.sql(f"SELECT count(*) n FROM {r}relations_resolved WHERE dropped='{reason}'").collect()[0].n)
    fuse = float(c.config.get("relations_drop_fuse", 0.01))
    c.zero("RELATION_DROP_FUSE", f"""SELECT count_if(dropped IS NOT NULL) dropped,count(*) total FROM {r}relations_resolved
      HAVING count_if(dropped IS NOT NULL) > greatest(10, {fuse}*count(*))""")
    # One edge may carry several source assertions: keep every source, collapse to one row per (child, parent, type).
    c.artifact("relations_candidate", f"""SELECT child_id,parent_id,relation_type,
        concat_ws(',',sort_array(collect_set(asserted_by))) asserted_by,sort_array(collect_set(source)) sources,:rid release_id
      FROM {r}relations_resolved WHERE dropped IS NULL GROUP BY child_id,parent_id,relation_type""")
    c.zero("RELATION_TYPE_CONFLICT", f"SELECT child_id,parent_id FROM {r}relations_candidate GROUP BY ALL HAVING count(*)<>1")
    # Cycle check over the multi-parent graph: repeatedly drop edges whose parent is not itself a child; a cycle never empties.
    remaining = c.artifact("relation_cycle_0", f"SELECT DISTINCT child_id,parent_id FROM {r}relations_candidate")
    for depth in range(101):
        if not c.sql(f"SELECT 1 FROM {remaining} LIMIT 1").collect():
            break
        c.require(depth < 100, "RELATION_DEPTH_FUSE")
        nxt = c.artifact(f"relation_cycle_{depth+1}", f"""SELECT e.* FROM {remaining} e
          LEFT SEMI JOIN (SELECT DISTINCT child_id FROM {remaining}) n ON e.parent_id=n.child_id""")
        c.zero(f"RELATION_CYCLE_{depth}", f"SELECT 1 WHERE (SELECT count(*) FROM {nxt})=(SELECT count(*) FROM {remaining})")
        remaining = nxt
    for row in c.sql(f"""SELECT relation_type,count(*) n FROM {r}relations_candidate GROUP BY 1""").collect():
        c.counts["relations_" + row.relation_type] = int(row.n)
    # Against last night's published links: catches rows deleted from the input, which the drop fuse above cannot see.
    published = c.outputs.get("relations")
    if published:
        try:
            before = {row.relation_type: int(row.n) for row in c.sql(f"SELECT relation_type,count(*) n FROM {published} GROUP BY 1").collect()}
        except Exception as exc:                          # first night: the output table does not exist yet
            if "TABLE_OR_VIEW_NOT_FOUND" not in str(exc):
                raise
            before = {}
        for kind, n in sorted(before.items()):
            now = c.counts.get("relations_" + kind, 0)
            name = "RELATION_COUNT_DROP_" + kind
            c.checks[name] = now >= n - max(10, fuse * n)
            c.require(c.checks[name], f"{name}: {n} -> {now}")
    # Project onto the award document: same rows as before, extra columns only.
    c.artifact("awards_candidate_before_relations", f"SELECT * FROM {r}awards_candidate")
    c.artifact("relation_neighbors", f"""SELECT child_id id,parent_id neighbor_id,'parent' direction,relation_type,asserted_by FROM {r}relations_candidate
      UNION ALL SELECT parent_id,child_id,'sub',relation_type,asserted_by FROM {r}relations_candidate""")
    c.artifact("relation_arrays", f"""SELECT n.id,n.direction,sort_array(collect_list({LINK})) links
      FROM {r}relation_neighbors n JOIN {r}awards_candidate_before_relations a ON a.id=n.neighbor_id GROUP BY n.id,n.direction""")
    c.artifact("awards_candidate", f"""SELECT a.*,
        coalesce(p.links,array()) parent_awards,
        slice(coalesce(s.links,array()),1,100) sub_awards,
        CAST(size(coalesce(s.links,array())) AS BIGINT) sub_awards_count,
        coalesce(p.links,array()) parent_awards_full,
        coalesce(s.links,array()) sub_awards_full
      FROM {r}awards_candidate_before_relations a
      LEFT JOIN {r}relation_arrays p ON p.id=a.id AND p.direction='parent'
      LEFT JOIN {r}relation_arrays s ON s.id=a.id AND s.direction='sub'""")
    c.zero("RELATION_AWARDS_SAME_ROWS", f"""(SELECT id FROM {r}awards_candidate EXCEPT ALL SELECT id FROM {r}awards_candidate_before_relations)
      UNION ALL (SELECT id FROM {r}awards_candidate_before_relations EXCEPT ALL SELECT id FROM {r}awards_candidate)""")
    c.count("awards_with_parent", f"(SELECT id FROM {r}awards_candidate WHERE size(parent_awards)>0) x")
    c.count("awards_with_sub_awards", f"(SELECT id FROM {r}awards_candidate WHERE sub_awards_count>0) x")
