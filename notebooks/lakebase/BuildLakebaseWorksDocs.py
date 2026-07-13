# Databricks notebook source
# MAGIC %md
# MAGIC # Build Lakebase works docs + id map (oxjob #576)
# MAGIC
# MAGIC Maintains the MERGE-updated Delta tables that feed the Lakebase serving layer for
# MAGIC single-work lookups (`GET /works/{id}`):
# MAGIC
# MAGIC - `openalex.works.lakebase_works_docs_0..7` — work_id → API-doc JSON, sharded by
# MAGIC   `pmod(work_id, 8)` (synced tables recommend ≤1 TB per refreshed table)
# MAGIC - `openalex.works.lakebase_works_ids` — external id (DOI/PMID, URL form) → work_id
# MAGIC
# MAGIC `openalex_works` is CREATE-OR-REPLACE'd daily, so these MERGE-maintained intermediates
# MAGIC (CDF enabled, doc_hash-gated) exist so the Lakebase synced tables see only true churn.
# MAGIC
# MAGIC The doc transform is copied from `notebooks/elastic/sync_works.ipynb` (the ES `_source`
# MAGIC shape that the API's WorksSchema consumes), with three deliberate exclusions:
# MAGIC `fulltext` and plaintext `abstract` (not part of API responses; excluded from the #576
# MAGIC storage sizing) and `indexed_timestamp` (nondeterministic — would churn every doc_hash).
# MAGIC Exact-parity serialization (2026-07-13): created/updated_date pre-formatted to mimic
# MAGIC Python isoformat (fraction omitted when zero), and all FLOATs widened to DOUBLE so
# MAGIC to_json prints the same double expansions Python json.dumps sent to ES.
# MAGIC TODO(#576 follow-up): unify this transform with sync_works/export_works into a shared
# MAGIC builder; deliberately not touching the production ES sync in Phase 1.
# MAGIC
# MAGIC Modes:
# MAGIC - `is_full_build=true` — one-time/recovery: rebuild all shards + id map from scratch.
# MAGIC - default (incremental) — same freshness contract as the ES sync
# MAGIC   (`updated_date >= current_date() - 2 days`), hash-gated MERGE per shard + deletes.

# COMMAND ----------

# MAGIC %pip install -U -q databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

SCHEMA = "openalex.works"
WORKS_TABLE = f"{SCHEMA}.openalex_works"
N_SHARDS = 8
DOCS_TABLE = lambda s: f"{SCHEMA}.lakebase_works_docs_{s}"
IDS_TABLE = f"{SCHEMA}.lakebase_works_ids"
BUILD_STAGING = f"{SCHEMA}._lakebase_docs_build"
INCR_STAGING = f"{SCHEMA}._lakebase_docs_incr"

dbutils.widgets.text("is_full_build", "false")
dbutils.widgets.text("guardrails_override", "false")
dbutils.widgets.text("trigger_syncs", "false")  # end2end passes true once synced tables exist
dbutils.widgets.text("run_deletes", "false")    # force the full delete sweep (auto-runs when doc count > works count)

IS_FULL_BUILD = dbutils.widgets.get("is_full_build").lower() == "true"
GUARDRAILS_OVERRIDE = dbutils.widgets.get("guardrails_override").lower() == "true"
TRIGGER_SYNCS = dbutils.widgets.get("trigger_syncs").lower() == "true"
RUN_DELETES = dbutils.widgets.get("run_deletes").lower() == "true"

print(f"IS_FULL_BUILD: {IS_FULL_BUILD}")

# COMMAND ----------

# Guardrails: never build/merge from a broken upstream
total_works = spark.sql(f"SELECT COUNT(*) AS cnt FROM {WORKS_TABLE}").collect()[0].cnt
print(f"{WORKS_TABLE}: {total_works:,} rows")
if total_works < 500_000_000 and not GUARDRAILS_OVERRIDE:
    raise Exception(
        f"GUARDRAIL: {WORKS_TABLE} has {total_works:,} rows (< 500M) — upstream looks broken. "
        "Pass guardrails_override=true to proceed anyway."
    )

if IS_FULL_BUILD:
    SQL_QUERY = f"SELECT * FROM {WORKS_TABLE}"
else:
    SQL_QUERY = f"""SELECT * FROM {WORKS_TABLE}
WHERE updated_date >= current_date() - INTERVAL 2 days
"""
    churn_count = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {WORKS_TABLE} WHERE updated_date >= current_date() - INTERVAL 2 days"
    ).collect()[0].cnt
    print(f"Incremental churn window: {churn_count:,} rows")
    if churn_count == 0 and not GUARDRAILS_OVERRIDE:
        raise Exception(
            "GUARDRAIL: 0 rows in the 2-day churn window — upstream updated_date looks broken. "
            "Pass guardrails_override=true to proceed anyway."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare input (copied from sync_works.ipynb)

# COMMAND ----------

df = (
    spark.sql(SQL_QUERY)
    .withColumn("display_name", F.col("title"))
    # First cast to date/timestamp
    .withColumn("created_date", F.to_timestamp("created_date"))
    .withColumn("updated_date", F.to_timestamp("updated_date"))
    .withColumn("publication_date", F.to_date("publication_date"))
    .withColumn(
        "concepts",
        F.transform(
            F.col("concepts"),
            lambda c: F.struct(
                F.concat(F.lit("https://openalex.org/C"), c.id).alias("id"),
                c.wikidata.alias("wikidata"),
                c.display_name.alias("display_name"),
                c.level.alias("level"),
                c.score.alias("score")
            )
        )
    )
    # Apply range checks using BETWEEN
    .withColumn(
        "created_date",
        F.when(
            F.col("created_date").between(F.lit("1000-01-01"), F.lit("9999-12-31")),
            F.col("created_date")
        ).otherwise(F.lit(None).cast("timestamp"))
    )
    .withColumn(
        "updated_date",
        F.when(
            F.col("updated_date").between(F.lit("1000-01-01"), F.lit("9999-12-31")),
            F.col("updated_date")
        ).otherwise(F.lit(None).cast("timestamp"))
    )
    .withColumn(
        "publication_date",
        F.when(
            F.col("publication_date").between(F.lit("1000-01-01"), F.lit("2050-12-31")),
            F.col("publication_date")
        ).otherwise(F.lit(None).cast("date"))
    )
    .filter(F.col("id").isNotNull())
)

if IS_FULL_BUILD:
    df = df.repartitionByRange(8096, "id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform to the API doc shape (copied from sync_works.ipynb `df_transformed`)

# COMMAND ----------

@udf(StringType())
def truncate_abstract_index_string(raw_json: str, max_bytes: int = 32760) -> str:
    """
    Truncate inverted index JSON by finding a safe cutoff point.
    """
    try:
        if not raw_json:
            return None

        encoded = raw_json.encode('utf-8')
        if len(encoded) <= max_bytes:
            return raw_json

        safe_bytes = max_bytes - 100
        truncated = encoded[:safe_bytes].decode('utf-8', errors='ignore')

        last_complete_array = -1
        for pattern in ['],"', '],']:
            pos = truncated.rfind(pattern)
            if pos > last_complete_array:
                last_complete_array = pos

        if last_complete_array == -1:
            return '{}'

        if truncated[last_complete_array:last_complete_array+3] == '],"':
            result = truncated[:last_complete_array+1] + '}'
        else:
            result = truncated[:last_complete_array+1] + '}'

        if result.count('{') != result.count('}'):
            return '{}'

        return result

    except Exception:
        return None

def sanitize_name(col_name: str):
  """
  Cleans a string column by removing unwanted characters and normalizing whitespace.
  Handles multilingual text by preserving letters, numbers, punctuation, and symbols from all Unicode scripts.
  """
  unwanted_chars_pattern = r"[^\p{L}\p{N}\p{P}\p{S}\p{Z}]"
  multiple_spaces_pattern = r"\s+"

  return F.trim(
      F.regexp_replace(
          F.regexp_replace(F.col(col_name), unwanted_chars_pattern, ""),
          multiple_spaces_pattern, " "
      )
  )

def sanitize_string(col_name: str, max_len: int = 32000):
    return F.when(F.col(col_name).isNotNull(), F.substring(F.col(col_name), 1, max_len)).otherwise(None)

def iso_ts(col_name: str):
    # Mimic Python datetime.isoformat(), which is what the ES bulk sync emits into
    # _source (elasticsearch-py json default): fractional seconds are 6 digits when
    # nonzero and OMITTED entirely when zero. A fixed to_json timestampFormat cannot
    # reproduce the conditional omission, so pre-format to string here (oxjob #576
    # exact-parity fix, 2026-07-13).
    return F.when(F.col(col_name).isNull(), F.lit(None).cast("string")).otherwise(
        F.concat(
            F.date_format(col_name, "yyyy-MM-dd'T'HH:mm:ss"),
            F.when(
                F.date_format(col_name, "SSSSSS") != "000000",
                F.concat(F.lit("."), F.date_format(col_name, "SSSSSS")),
            ).otherwise(F.lit("")),
        )
    )

empty_sdg_array = F.array().cast("array<struct<id:string,display_name:string,score:double>>")

df_transformed = (
    df
    .withColumn("work_id", F.col("id").cast("bigint"))
    .withColumn("id", F.concat(F.lit("https://openalex.org/W"), F.col("id")))
    .withColumn("publication_year", F.coalesce(
        F.col("publication_year"),
        F.year(F.col("publication_date"))
    ))
    .withColumn("publication_year", F.year("publication_date"))
    .withColumn("title", sanitize_name("title"))
    .withColumn("display_name", sanitize_name("display_name"))
    .withColumn("ids",
        F.transform_values("ids",
            lambda k, v: F.when(k == "doi",
                    F.concat(F.lit("https://doi.org/"), v)).otherwise(v)
        )
    )
    .withColumn("doi", sanitize_string("doi"))
    .withColumn("language", sanitize_string("language"))
    .withColumn("type", sanitize_string("type"))
    .withColumn("referenced_works",
                F.expr("transform(referenced_works, x -> 'https://openalex.org/W' || x)"))
    .withColumn("referenced_works_count",
                F.when(F.col("referenced_works").isNotNull(), F.size("referenced_works")).otherwise(0))
    .withColumn("abstract_inverted_index", truncate_abstract_index_string(F.col("abstract_inverted_index")))
    .withColumn("open_access", F.struct(
        F.col("open_access.is_oa"),
        sanitize_string("open_access.oa_status").alias("oa_status"),
        F.col("open_access.any_repository_has_fulltext"),
        F.col("open_access.oa_url")
    ))
    # Build full authorships first, then truncate for the limited version
    .withColumn("authorships_full", F.expr("""
        transform(authorships, x -> named_struct(
            'affiliations', x.affiliations,
            'author', x.author,
            'author_position', substring(x.author_position, 1, 32000),
            'countries', x.countries,
            'raw_author_name', substring(x.raw_author_name, 1, 32000),
            'raw_orcid', x.raw_orcid,
            'is_corresponding', x.is_corresponding,
            'raw_affiliation_strings', transform(x.raw_affiliation_strings, aff -> substring(aff, 1, 32000)),
            'institutions', x.institutions
        ))
    """))
    .withColumn("authorships", F.slice(F.col("authorships_full"), 1, 100))
    .withColumn("locations", F.expr("""
        transform(locations, x -> named_struct(
            'is_oa', x.is_oa,
            'is_published', x.version = 'publishedVersion',
            'landing_page_url', substring(x.landing_page_url, 1, 32000),
            'pdf_url', substring(x.pdf_url, 1, 32000),
            'source', x.source,
            'raw_source_name', x.raw_source_name,
            'raw_type', x.raw_type,
            'native_id', x.native_id,
            'provenance', x.provenance,
            'license', x.license,
            'license_id', x.license_id,
            'version', x.version,
            'is_accepted', x.is_accepted
        ))
    """))
    # limit to a reasonable number (they go up to 130) - mainly for xpac
    .withColumn("concepts", F.slice(F.col("concepts"), 1, 40))
    .withColumn("has_fulltext", F.col("fulltext").isNotNull())
    .withColumn("_doc", F.struct(
        F.col("id"),
        F.col("doi"),
        F.col("title"),
        F.col("display_name"),
        F.col("ids"),
        F.expr("""
            array_sort(
                array_distinct(
                    array_compact(
                        flatten(
                            TRANSFORM(locations, loc ->
                                CASE
                                WHEN loc.provenance IN ('crossref', 'pubmed', 'datacite')
                                    THEN array(loc.provenance, IF(loc.source.is_in_doaj, 'doaj', NULL))
                                WHEN loc.provenance = 'repo' AND lower(loc.native_id) like 'oai:arxiv.org%'
                                    THEN array('arxiv')
                                WHEN loc.provenance = 'repo' AND lower(loc.native_id) like 'oai:doaj.org/%'
                                    THEN array('doaj')
                                WHEN loc.provenance = 'mag' AND lower(loc.source.display_name) = 'pubmed'
                                    THEN array('pubmed')
                                ELSE array()
                                END
                            )
                        )
                    )
                )
            )
        """).alias("indexed_in"),
        F.col("publication_date"),
        F.col("publication_year"),
        F.col("language"),
        F.col("type"),
        F.coalesce(F.col("authorships"), F.lit([])).alias("authorships"),
        F.coalesce(F.col("authorships_full"), F.lit([])).alias("authorships_full"),
        F.col("authors_count"),
        F.coalesce(F.col("corresponding_author_ids"), F.lit([])).alias("corresponding_author_ids"),
        F.coalesce(F.col("corresponding_institution_ids"), F.lit([])).alias("corresponding_institution_ids"),
        F.col("primary_topic"),
        F.col("topics"),
        F.col("keywords"),
        F.col("concepts"),
        F.col("locations"),
        F.col("locations_count"),
        F.col("primary_location"),
        F.col("best_oa_location"),
        F.coalesce(F.col("sustainable_development_goals"), empty_sdg_array).alias("sustainable_development_goals"),
        F.col("awards"),
        F.col("funders"),
        F.col("institutions"),
        F.col("countries_distinct_count"),
        F.col("institutions_distinct_count"),
        F.col("open_access"),
        F.col("is_paratext"),
        F.col("is_retracted"),
        F.col("is_xpac"),
        F.col("biblio"),
        F.col("referenced_works"),
        F.col("referenced_works_count"),
        F.coalesce(F.col("related_works"), F.lit([])).alias("related_works"),
        F.col("abstract_inverted_index"),
        F.col("cited_by_count"),
        F.col("counts_by_year"),
        F.col("apc_list"),
        F.col("apc_paid"),
        F.col("fwci"),
        F.col("citation_normalized_percentile"),
        F.col("cited_by_percentile_year"),
        F.coalesce(F.col("mesh"), F.lit([])).alias("mesh"),
        F.col("abstract_inverted_index").isNotNull().alias("has_abstract"),
        F.col("has_content"),
        F.col("has_fulltext"),
        iso_ts("created_date").alias("created_date"),
        iso_ts("updated_date").alias("updated_date")
    ))
    .select("work_id", "_doc")
)

# Widen every FLOAT to DOUBLE before serializing: the ES sync hands Spark floats to
# Python, where they become doubles, and json.dumps prints the full double expansion
# (e.g. 0.6881897449493408). to_json on a raw float prints the short form (0.68818974),
# which parses to a DIFFERENT double at serve time. float->double cast is exact and
# reproduces the ES bytes. (oxjob #576 exact-parity fix, 2026-07-13)
def _widen_floats(dt):
    if isinstance(dt, FloatType):
        return DoubleType()
    if isinstance(dt, StructType):
        return StructType([StructField(f.name, _widen_floats(f.dataType), f.nullable) for f in dt.fields])
    if isinstance(dt, ArrayType):
        return ArrayType(_widen_floats(dt.elementType), dt.containsNull)
    if isinstance(dt, MapType):
        return MapType(_widen_floats(dt.keyType), _widen_floats(dt.valueType), dt.valueContainsNull)
    return dt

_doc_type = df_transformed.schema["_doc"].dataType
df_transformed = df_transformed.withColumn("_doc", F.col("_doc").cast(_widen_floats(_doc_type)))

# Explicit nulls (ignoreNullFields=false) so the JSON matches the ES _source shape,
# where absent-vs-null matters to the API serializer.
df_docs = (
    df_transformed
    .select(
        "work_id",
        F.to_json(F.col("_doc"), {"ignoreNullFields": "false"}).alias("doc"),
    )
    .withColumn("doc_hash", F.xxhash64(F.col("doc")))
    .dropDuplicates(["work_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage the transformed docs

# COMMAND ----------

STAGING = BUILD_STAGING if IS_FULL_BUILD else INCR_STAGING
df_docs.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGING)
staged = spark.sql(f"SELECT COUNT(*) AS cnt FROM {STAGING}").collect()[0].cnt
print(f"Staged {staged:,} docs into {STAGING}")

# COMMAND ----------

# Shared SQL: current external-id extraction (DOI + PMID, in the URL/term form the API
# queries — DOI gets the https://doi.org/ prefix exactly like the doc transform's ids map;
# pmid is stored in URL form upstream). MIN(work_id) dedupes the rare multi-claim key.
EXT_ID_SOURCE = f"""
SELECT ext_id, MIN(work_id) AS work_id
FROM (
    SELECT CONCAT('https://doi.org/', ids['doi']) AS ext_id, id AS work_id
    FROM {WORKS_TABLE} WHERE id IS NOT NULL AND ids['doi'] IS NOT NULL
    UNION ALL
    SELECT ids['pmid'] AS ext_id, id AS work_id
    FROM {WORKS_TABLE} WHERE id IS NOT NULL AND ids['pmid'] IS NOT NULL
)
GROUP BY ext_id
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full build: create shards + id map from scratch

# COMMAND ----------

if IS_FULL_BUILD:
    for s in range(N_SHARDS):
        t = DOCS_TABLE(s)
        spark.sql(f"""
            CREATE OR REPLACE TABLE {t}
            CLUSTER BY (work_id)
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
            AS SELECT work_id, doc, doc_hash, current_timestamp() AS updated_at
            FROM {BUILD_STAGING}
            WHERE pmod(work_id, {N_SHARDS}) = {s}
        """)
        spark.sql(f"ALTER TABLE {t} ALTER COLUMN work_id SET NOT NULL")
        spark.sql(f"ALTER TABLE {t} ADD CONSTRAINT lakebase_works_docs_{s}_pk PRIMARY KEY (work_id)")
        cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {t}").collect()[0].cnt
        print(f"{t}: {cnt:,} rows")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {IDS_TABLE}
        CLUSTER BY (ext_id)
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        AS SELECT ext_id, work_id, current_timestamp() AS updated_at
        FROM ({EXT_ID_SOURCE})
    """)
    spark.sql(f"ALTER TABLE {IDS_TABLE} ALTER COLUMN ext_id SET NOT NULL")
    spark.sql(f"ALTER TABLE {IDS_TABLE} ADD CONSTRAINT lakebase_works_ids_pk PRIMARY KEY (ext_id)")
    cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {IDS_TABLE}").collect()[0].cnt
    print(f"{IDS_TABLE}: {cnt:,} rows")

    spark.sql(f"DROP TABLE IF EXISTS {BUILD_STAGING}")
    print(f"Dropped {BUILD_STAGING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental: churn-only hash-gated MERGE per shard, then id map
# MAGIC
# MAGIC Daily MERGEs use ONLY the transformed churn as source (file-pruned by work_id
# MAGIC clustering — minutes). The full-table delete sweep (`NOT MATCHED BY SOURCE`) forces a
# MAGIC complete scan of every shard (~90 min observed 2026-07-11) so it does NOT run daily:
# MAGIC it runs when `run_deletes=true` OR automatically when the doc tables hold more rows
# MAGIC than `openalex_works` (i.e. upstream deletions happened). Deletes lagging by days is
# MAGIC still stricter than ES, whose works sync never propagates deletes at all.

# COMMAND ----------

if not IS_FULL_BUILD:
    total_docs = sum(
        spark.sql(f"SELECT COUNT(*) AS cnt FROM {DOCS_TABLE(s)}").collect()[0].cnt
        for s in range(N_SHARDS)
    )
    do_deletes = RUN_DELETES or (total_docs > total_works)
    if do_deletes and not RUN_DELETES:
        print(f"Auto-enabling delete sweep: docs total {total_docs:,} > openalex_works {total_works:,}")
    print(f"docs total: {total_docs:,} | works: {total_works:,} | delete sweep this run: {do_deletes}")

    for s in range(N_SHARDS):
        t = DOCS_TABLE(s)
        result = spark.sql(f"""
            MERGE INTO {t} AS target
            USING (SELECT work_id, doc, doc_hash FROM {INCR_STAGING} WHERE pmod(work_id, {N_SHARDS}) = {s}) AS source
            ON target.work_id = source.work_id
            WHEN MATCHED AND target.doc_hash <> source.doc_hash THEN
                UPDATE SET doc = source.doc, doc_hash = source.doc_hash, updated_at = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (work_id, doc, doc_hash, updated_at)
                VALUES (source.work_id, source.doc, source.doc_hash, current_timestamp())
        """).collect()[0]
        print(f"{t}: updated={result.num_updated_rows:,} inserted={result.num_inserted_rows:,}")

    # Id map, churn-only: keys of just the churned works (update moved keys, insert new ones).
    result = spark.sql(f"""
        MERGE INTO {IDS_TABLE} AS target
        USING (
            SELECT ext_id, MIN(work_id) AS work_id
            FROM (
                SELECT CONCAT('https://doi.org/', w.ids['doi']) AS ext_id, w.id AS work_id
                FROM {WORKS_TABLE} w JOIN {INCR_STAGING} d ON w.id = d.work_id
                WHERE w.ids['doi'] IS NOT NULL
                UNION ALL
                SELECT w.ids['pmid'] AS ext_id, w.id AS work_id
                FROM {WORKS_TABLE} w JOIN {INCR_STAGING} d ON w.id = d.work_id
                WHERE w.ids['pmid'] IS NOT NULL
            )
            GROUP BY ext_id
        ) AS source
        ON target.ext_id = source.ext_id
        WHEN MATCHED AND target.work_id <> source.work_id THEN
            UPDATE SET work_id = source.work_id, updated_at = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (ext_id, work_id, updated_at) VALUES (source.ext_id, source.work_id, current_timestamp())
    """).collect()[0]
    print(f"{IDS_TABLE} (churn): updated={result.num_updated_rows:,} inserted={result.num_inserted_rows:,}")

    if do_deletes:
        for s in range(N_SHARDS):
            t = DOCS_TABLE(s)
            result = spark.sql(f"""
                MERGE INTO {t} AS target
                USING (SELECT id FROM {WORKS_TABLE} WHERE id IS NOT NULL AND pmod(id, {N_SHARDS}) = {s}) AS source
                ON target.work_id = source.id
                WHEN NOT MATCHED BY SOURCE THEN DELETE
            """).collect()[0]
            print(f"{t} (sweep): deleted={result.num_deleted_rows:,}")
        result = spark.sql(f"""
            MERGE INTO {IDS_TABLE} AS target
            USING ({EXT_ID_SOURCE}) AS source
            ON target.ext_id = source.ext_id
            WHEN MATCHED AND target.work_id <> source.work_id THEN
                UPDATE SET work_id = source.work_id, updated_at = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (ext_id, work_id, updated_at) VALUES (source.ext_id, source.work_id, current_timestamp())
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """).collect()[0]
        print(f"{IDS_TABLE} (sweep): updated={result.num_updated_rows:,} inserted={result.num_inserted_rows:,} deleted={result.num_deleted_rows:,}")

    spark.sql(f"DROP TABLE IF EXISTS {INCR_STAGING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Report

# COMMAND ----------

shard_counts = [
    spark.sql(f"SELECT COUNT(*) AS cnt FROM {DOCS_TABLE(s)}").collect()[0].cnt
    for s in range(N_SHARDS)
]
ids_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {IDS_TABLE}").collect()[0].cnt
print(f"Shard counts: {[f'{c:,}' for c in shard_counts]}")
print(f"Total docs: {sum(shard_counts):,} (openalex_works: {total_works:,})")
print(f"Id map: {ids_count:,}")
if sum(shard_counts) != total_works:
    print(f"WARNING: shard total differs from openalex_works by {total_works - sum(shard_counts):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger Lakebase synced-table refreshes (end2end only)
# MAGIC
# MAGIC Triggered-mode synced tables refresh on demand; this starts all 9 pipeline updates and
# MAGIC waits. Requires the synced tables to exist (created in oxjob #576 step 4).

# COMMAND ----------

if TRIGGER_SYNCS:
    import time
    from databricks.sdk import WorkspaceClient

    SYNCED_TABLES = [f"openalex.lakebase.lakebase_works_docs_{s}" for s in range(N_SHARDS)] + [
        "openalex.lakebase.lakebase_works_ids"
    ]

    w = WorkspaceClient()
    pipeline_ids = {}
    for name in SYNCED_TABLES:
        st = w.database.get_synced_database_table(name=name)
        pid = getattr(st.data_synchronization_status, "pipeline_id", None) or getattr(
            getattr(st, "spec", None), "pipeline_id", None
        )
        if not pid:
            raise Exception(f"Could not resolve pipeline id for synced table {name}: {st}")
        pipeline_ids[name] = pid
        w.pipelines.start_update(pipeline_id=pid)
        print(f"Triggered refresh: {name} (pipeline {pid})")

    TIMEOUT_S = 2 * 3600
    start = time.time()
    pending = dict(pipeline_ids)
    failed = {}
    while pending and time.time() - start < TIMEOUT_S:
        time.sleep(60)
        for name, pid in list(pending.items()):
            latest = w.pipelines.list_updates(pipeline_id=pid, max_results=1).updates
            state = latest[0].state.value if latest else "UNKNOWN"
            if state == "COMPLETED":
                print(f"Sync complete: {name}")
                del pending[name]
            elif state in ("FAILED", "CANCELED"):
                failed[name] = state
                del pending[name]
    if failed or pending:
        raise Exception(f"Synced-table refreshes not clean — failed: {failed}, still pending: {list(pending)}")
    print("All synced-table refreshes completed")
