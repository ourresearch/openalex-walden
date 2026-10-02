"""Search publication: changed award documents, redirects (verified), then capped, reverse-authorized deletes (verified).

Kept from deployed lib/sync_awards.py: the worker-side bulk writer (op30), refresh checks, scan-based observation of the live
indexes, REDIRECT_MAPPING_MISMATCH, public reverse gate on deletes, 5% delete fuse, DELETIONS_VERIFIED, EXPECTED_AWARDS_VERIFIED.
Removed: works barrier / receipt / ledger, rev2 revision, snapshot swap, mock backend.
Changed: only new/changed documents are upserted (deployed release 1 re-sent all 17.1M). A document is also resent when the
updated_date stored in the index differs from the candidate's (a generation check on every document, every night). A document is resent when its
updated_date is at or after the start of the last run whose search publication succeeded, or when it is missing from the index;
so a night that fails after its table swap is resent the next night.
Modes: 'none' (skip search entirely), 'observe' (scan the live indexes, stage and check, write nothing), 'elasticsearch'.
"""
import json

from stable_award_ids import literal

PARTITIONS = 96


def check_refresh(result):
    shards = result.get("_shards", {})
    if shards.get("failed", 0) or "failed" not in shards:
        raise RuntimeError("REFRESH_FAILED_OR_INCOMPLETE")


def partition_writer(host, index, mode, deleting=False, es=None):
    """Runs on each worker over its partition (deployed sync_awards.py:107, unchanged in meaning)."""
    def write(rows):
        if es is None:
            from elasticsearch import Elasticsearch as _Client
            from elasticsearch import helpers as _helpers
        else:
            _Client, _helpers = es.Elasticsearch, es.helpers
        import json as _json
        client = _Client(hosts=[host], request_timeout=180, max_retries=3)
        submitted = [0]

        def actions():
            for row in rows:
                if mode == "awards":
                    a = {"_op_type": "index", "_index": index, "_id": row.id, "_source": _json.loads(row.document_json)}
                elif mode == "redirects":
                    old = f"https://openalex.org/G{row.old_id}"
                    a = {"_op_type": "index", "_index": index, "_id": old, "_source": {"id": old, "merge_into_id": f"https://openalex.org/G{row.canonical_id}"}}
                elif mode == "delete":
                    a = {"_op_type": "delete", "_index": index, "_id": row.id}
                else:
                    raise RuntimeError("UNKNOWN_WRITE_MODE: " + str(mode))
                submitted[0] += 1
                yield a
        acknowledged = 0
        for ok, info in _helpers.streaming_bulk(client, actions(), chunk_size=500, raise_on_error=False, raise_on_exception=True):
            op = next(iter(info.values()))
            if not ok and not (deleting and op.get("status") == 404):
                raise RuntimeError("BULK_FAILED: " + repr(info))
            acknowledged += 1
        if acknowledged != submitted[0]:
            raise RuntimeError(f"INCOMPLETE_BULK_RESPONSE: submitted {submitted[0]} acknowledged {acknowledged}")
    return write


# Sub-award fields (award_relations.py) as provisioned on awards-v4 on 10-01. A rebuilt index that inferred other types
# (dynamic mapping turns strings into text) would silently break the API's term filters, so the sync refuses to write.
_LINK_FIELDS = {"id": {"type": "keyword"}, "relationship": {"type": "keyword"}, "asserted_by": {"type": "keyword"},
                "funder.id": {"type": "keyword"},
                "display_name": {"type": "keyword", "index": False, "doc_values": False},
                "funder.display_name": {"type": "keyword", "index": False, "doc_values": False}}
AWARD_FIELD_MAPPINGS = {"sub_awards_count": {"type": "long"},
                        **{f"{arr}.{leaf}": m for arr in ("parent_awards", "parent_awards_full", "sub_awards", "sub_awards_full")
                           for leaf, m in _LINK_FIELDS.items()}}


def mapping_mismatches(field_mapping_response, index):
    """Fields whose live mapping differs from AWARD_FIELD_MAPPINGS (missing counts as different)."""
    live = {}
    for f, m in field_mapping_response.get(index, {}).get("mappings", {}).items():
        leaf = list(m["mapping"].values())[0]
        live[f] = {k: v for k, v in leaf.items() if k in ("type", "index", "doc_values")}
    return sorted(f for f, want in AWARD_FIELD_MAPPINGS.items() if live.get(f) != want)


class Search:
    def __init__(self, c, dbutils):
        from elasticsearch import Elasticsearch, helpers
        self.c, self.helpers = c, helpers
        cfg = c.config["sync"]
        self.mode = cfg["mode"]
        self.awards, self.redirects = cfg["awards_index"], cfg["redirect_index"]
        self.host = dbutils.secrets.get(scope=cfg["secret_scope"], key=cfg["secret_key"])
        self.client = Elasticsearch(hosts=[self.host], request_timeout=180, max_retries=3)
        props = self.client.indices.get_mapping(index=self.redirects)[self.redirects]["mappings"]["properties"]
        c.require(all(props.get(k, {}).get("type") == "keyword" for k in ("id", "merge_into_id")), "REDIRECT_MAPPING_NOT_PROVISIONED")
        bad = mapping_mismatches(self.client.indices.get_field_mapping(index=self.awards, fields=list(AWARD_FIELD_MAPPINGS)), self.awards)
        c.require(not bad, "AWARD_MAPPING_NOT_PROVISIONED: " + ",".join(bad))

    def observed(self, kind, batch=200_000):
        """Full scroll of one index into a scratch table (helpers.scan raises on failed shards; the table is replaced whole)."""
        c = self.c
        name = c.r + "observed_" + kind
        c.require(name.startswith(c.fence), "WRITE_OUTSIDE_FENCE: " + name)
        schema = "id STRING, updated_date STRING" if kind == "awards" else "id STRING, merge_into_id STRING"
        rows, first = [], True

        def flush():
            nonlocal rows, first
            c.spark.createDataFrame(rows, schema).write.mode("overwrite" if first else "append").saveAsTable(name)
            rows, first = [], False
        for hit in self.helpers.scan(self.client, index=self.awards if kind == "awards" else self.redirects,
                                     query={"query": {"match_all": {}}, "_source": ["updated_date"] if kind == "awards" else ["id", "merge_into_id"]},
                                     size=5000, scroll="5m", raise_on_error=True):
            src = hit.get("_source") or {}
            if kind == "awards":
                rows.append((hit["_id"], src.get("updated_date")))
            else:
                c.require(src.get("id") == hit["_id"], "REDIRECT_DOCUMENT_ID_MISMATCH")
                rows.append((hit["_id"], src.get("merge_into_id")))
            if len(rows) >= batch:
                flush()
        flush()                                              # also creates the table when the index is empty
        return name

    def write(self, query, index, mode, deleting=False):
        c = self.c
        c.require(self.mode == "elasticsearch", "SEARCH_WRITE_IN_OBSERVE_MODE")
        c.guard()
        c.sql(query).repartition(PARTITIONS).foreachPartition(partition_writer(self.host, index, mode, deleting))
        c.guard()

    def refresh(self, kind):
        check_refresh(self.client.indices.refresh(index=self.awards if kind == "awards" else self.redirects))


def stage(c, search):
    """Before the swap: decide what to send and prove every delete is authorized. Writes nothing to search."""
    r, p = c.r, c.p
    c.artifact("es_expected", f"SELECT concat('https://openalex.org/G',id) id FROM {r}api_candidate")
    c.zero("ES_INPUT_NONEMPTY", f"SELECT count(*) n FROM {r}es_expected HAVING n=0")
    current = search.observed("awards")
    c.artifact("es_current_before", f"SELECT id,CAST(updated_date AS TIMESTAMP) updated_date FROM {current}")   # frozen: later scans replace observed_awards
    wm = c.sql(f"SELECT max(started_at) w FROM {p}award_nightly_runs WHERE status='SUCCEEDED'").collect()[0].w
    watermark = str(wm) if wm is not None else c.config["sync"]["first_watermark"]
    c.versions["search_watermark"] = watermark
    c.artifact("es_upserts", f"""SELECT concat('https://openalex.org/G',a.id) id,
        to_json(struct(concat('https://openalex.org/G',a.id) AS id,a.* EXCEPT(id,release_id)),map('ignoreNullFields','false')) document_json
      FROM {r}api_candidate a LEFT JOIN {r}es_current_before s ON s.id=concat('https://openalex.org/G',a.id)
      WHERE s.id IS NULL OR a.updated_date >= CAST({literal(watermark)} AS TIMESTAMP)
        OR NOT(date_trunc('SECOND',s.updated_date) <=> date_trunc('SECOND',a.updated_date))""")   # generation check (Codex r1 #8)
    # cumulative public ledger (deployed sync_awards.py:495): REDIRECTED ids, today's GONE, and ids GONE earlier
    c.artifact("public_ledger", f"""SELECT e.stable_id,e.status,e.redirect_to,
        CASE WHEN e.status='GONE' THEN coalesce(d.resolving_observations,0) END resolving_observations
      FROM {r}state_candidate e LEFT JOIN {r}dispositions d USING(stable_id)
      WHERE e.status='REDIRECTED' OR d.disposition='GONE' OR (e.status='GONE' AND EXISTS(SELECT 1 FROM last_state l WHERE l.stable_id=e.stable_id AND l.status='GONE'))""")
    c.artifact("stale_ids", f"SELECT id FROM {r}es_current_before EXCEPT SELECT id FROM {r}es_expected")
    c.artifact("stale_dispositions", f"""SELECT s.id,l.stable_id,
        CASE WHEN l.status='GONE' AND l.resolving_observations=0 THEN 'GONE'
             WHEN l.status='REDIRECTED' AND x.canonical_id=l.redirect_to THEN 'REDIRECTED' ELSE 'UNAUTHORIZED' END disposition
      FROM {r}stale_ids s LEFT JOIN {r}public_ledger l ON s.id=concat('https://openalex.org/G',l.stable_id)
      LEFT JOIN {r}redirects_candidate x ON x.old_id=l.stable_id""")
    c.zero("PUBLIC_REVERSE_GATE", f"SELECT * FROM {r}stale_dispositions WHERE disposition='UNAUTHORIZED'")
    c.zero("FIVE_PERCENT_DELETE_FUSE", f"SELECT count(*) n FROM {r}stale_ids HAVING n>0.05*(SELECT count(*) FROM {r}es_expected)")
    for name in ("es_expected", "es_current_before", "es_upserts", "stale_ids"):
        c.count(name, r + name)
    c.count("redirects", r + "redirects_candidate")


def publish(c, search):
    """After the swap: documents, then redirects (verified exactly), then deletes (verified)."""
    r = c.r
    search.write(f"SELECT id,document_json FROM {r}es_upserts", search.awards, "awards")
    search.refresh("awards")
    after_upsert = search.observed("awards")
    c.zero("ACTIVE_AWARD_DOC_MISSING", f"SELECT id FROM {r}es_expected EXCEPT SELECT id FROM {after_upsert}")
    c.zero("AWARD_DOC_GENERATION", f"""SELECT a.id FROM {r}api_candidate a JOIN {after_upsert} s ON s.id=concat('https://openalex.org/G',a.id)
      WHERE NOT(date_trunc('SECOND',CAST(s.updated_date AS TIMESTAMP)) <=> date_trunc('SECOND',a.updated_date))""")
    search.write(f"SELECT old_id,canonical_id FROM {r}redirects_candidate", search.redirects, "redirects")
    search.refresh("redirects")
    redirects = search.observed("redirects")
    c.zero("REDIRECT_MAPPING_MISMATCH", f"""(SELECT concat('https://openalex.org/G',old_id) id,concat('https://openalex.org/G',canonical_id) merge_into_id
        FROM {r}redirects_candidate EXCEPT SELECT id,merge_into_id FROM {redirects}) UNION ALL
      (SELECT id,merge_into_id FROM {redirects} EXCEPT SELECT concat('https://openalex.org/G',old_id),concat('https://openalex.org/G',canonical_id) FROM {r}redirects_candidate)""")
    c.zero("REDIRECT_MAPPING_DUPLICATE", f"SELECT id FROM {redirects} GROUP BY id HAVING count(*)<>1")
    search.write(f"SELECT id FROM {r}stale_ids", search.awards, "delete", deleting=True)
    search.refresh("awards")
    after = search.observed("awards")
    c.zero("DELETIONS_VERIFIED", f"SELECT id FROM {after} INTERSECT SELECT id FROM {r}stale_ids")
    c.zero("EXPECTED_AWARDS_VERIFIED", f"(SELECT id FROM {r}es_expected EXCEPT SELECT id FROM {after}) UNION ALL (SELECT id FROM {after} EXCEPT SELECT id FROM {r}es_expected)")
