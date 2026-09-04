# Databricks notebook source
# MAGIC %md
# MAGIC # RepointWorkIds — audited fan-out of works that merged only because digits were stripped (oxjob #880)
# MAGIC
# MAGIC A work "fans out" when its pinned anchors carry >= 2 distinct digit-form title keys under ONE
# MAGIC digit-stripped key: records that merged only because `normalize_title` used to drop digits
# MAGIC (#880 `KEY_LEDGER_PLAN.md`, evidence `q75`). Pins never re-resolve, so the only way to split
# MAGIC such a work is the registries' sanctioned correction path: DELETE the pins of the anchors
# MAGIC that should leave, DELETE the alias rows that would pull them straight back, and let the
# MAGIC nightly `MapWorkIds` re-resolve them (one fresh mint per key; anchors that share a key stay
# MAGIC together by construction).
# MAGIC
# MAGIC The **primary group** keeps the work id: the group that carries a DOI/PMID/arXiv, else the
# MAGIC largest, else the earliest pinned. Its anchors and map rows are never touched, so the id,
# MAGIC its citations and its ES doc survive. Works are **held** (never executed) when a non-primary
# MAGIC group carries an identifier (that split needs DOI-row deletes + a citation repoint — wave E)
# MAGIC or when the work is cited >= `hold_cited_over` times (individual sign-off). A non-primary title whose
# MAGIC digit runs are a subset of the primary's (list number, footnote, isotope: a digit INSERTION) is the same
# MAGIC work and stays put; only digit SUBSTITUTIONS (year, part, volume, number) move (`q75 a8/a9`).
# MAGIC
# MAGIC Modes (all keyed on `target_table`, one row per anchor that should leave its work):
# MAGIC - `stage`    build the target from live data for works holding `min_keys`..`max_keys` distinct
# MAGIC              digit titles (`max_keys = 0` = no cap); assign waves of `wave_size` keys, smallest
# MAGIC              works first; write `<target>_cited_review` (every cited work in scope).
# MAGIC - `dry_run`  what `execute` would do for `wave`: pins, alias rows, expected new works, cited works.
# MAGIC - `execute`  `confirm = yes` only, outside 04:00-08:00 UTC: freeze `<target>_wave<N>_audit`,
# MAGIC              then the two DELETEs. Undo: 60-day Delta retention on both registries + the audit table.
# MAGIC - `verify`   the morning after: every audited anchor re-pinned, none back on its old work,
# MAGIC              fan-in of the new works, keys now on > 1 id.
# MAGIC
# MAGIC Wave size is bounded by Guardrails check 1 (7.5M `updated_date` stamps; quiet nights stamp
# MAGIC 1.7-4.4M): every key that leaves becomes a new work = one stamp. Default 3,000,000.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "stage", ["stage", "dry_run", "execute", "verify"])
dbutils.widgets.text("min_keys", "4")
dbutils.widgets.text("max_keys", "0")
dbutils.widgets.text("wave_size", "3000000")
dbutils.widgets.text("wave", "1")
dbutils.widgets.text("hold_cited_over", "100")
dbutils.widgets.text("target_table", "openalex.works.oxjob880_fanout_target")
dbutils.widgets.text("confirm", "no")

MODE = dbutils.widgets.get("mode")
MIN_KEYS = int(dbutils.widgets.get("min_keys"))
MAX_KEYS = int(dbutils.widgets.get("max_keys"))
WAVE_SIZE = int(dbutils.widgets.get("wave_size"))
WAVE = int(dbutils.widgets.get("wave"))
HOLD_CITED_OVER = int(dbutils.widgets.get("hold_cited_over"))
TARGET = dbutils.widgets.get("target_table")
CONFIRM = dbutils.widgets.get("confirm") == "yes"
REVIEW = f"{TARGET}_cited_review"
AUDIT = f"{TARGET}_wave{WAVE}_audit"

REGISTRY = "openalex.works.location_work_ids"
MAP = "openalex.works.work_id_map"
LWT = "openalex.works.locations_w_types"
WORKS = "openalex.works.openalex_works"

import datetime, time
from pyspark.sql import functions as F, Window

print({k: v for k, v in dict(mode=MODE, min_keys=MIN_KEYS, max_keys=MAX_KEYS, wave_size=WAVE_SIZE, wave=WAVE,
                             hold_cited_over=HOLD_CITED_OVER, target=TARGET, confirm=CONFIRM).items()})


def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def one(sql):
    return rows(sql)[0]

# COMMAND ----------

if MODE == "stage":
    t0 = time.time()
    anchor = ["provenance", "native_id_namespace", "native_id"]
    live = (spark.table(LWT)
            .withColumn("rn", F.row_number().over(Window.partitionBy(*anchor).orderBy(F.col("updated_date").desc())))
            .filter("rn = 1")
            .filter("merge_key.title_author RLIKE '[0-9]'")
            .select(*anchor,
                    F.col("merge_key.title_author").alias("new_key"),
                    F.expr("split_part(merge_key.title_author, '_', 1)").alias("title_part"),
                    F.expr("(NULLIF(merge_key.doi, '') IS NOT NULL OR NULLIF(merge_key.pmid, '') IS NOT NULL "
                           "OR NULLIF(merge_key.arxiv, '') IS NOT NULL)").alias("has_id"),
                    F.expr("substr(title, 1, 120)").alias("title"))
            .withColumn("stripped", F.regexp_replace("title_part", "[0-9]", "")))
    pins = (spark.table(REGISTRY).filter("work_id IS NOT NULL")
            .select(*anchor, F.col("work_id").alias("old_work_id"), F.col("work_id_source").alias("old_work_id_source"),
                    F.col("openalex_created_dt").alias("old_created_dt")))
    a = live.join(pins, anchor)

    # one row per (work, stripped, digit title): the fan-out unit is the TITLE part of the key --
    # author-key digit noise must never split a work (q75 a3)
    g = (a.groupBy("old_work_id", "stripped", "title_part")
          .agg(F.count("*").alias("g_anchors"), F.sum(F.col("has_id").cast("int")).alias("g_with_id"),
               F.min("old_created_dt").alias("g_first_pinned"), F.min("title").alias("g_title")))
    w = (g.groupBy("old_work_id", "stripped")
          .agg(F.count("*").alias("n_titles"), F.sum("g_anchors").alias("n_anchors"))
          .filter(F.col("n_titles") >= MIN_KEYS))
    if MAX_KEYS:
        w = w.filter(F.col("n_titles") <= MAX_KEYS)
    rank = Window.partitionBy("old_work_id", "stripped").orderBy(
        F.col("g_with_id").desc(), F.col("g_anchors").desc(), F.col("g_first_pinned").asc(), F.col("title_part"))
    g2 = g.join(w, ["old_work_id", "stripped"]).withColumn("group_rank", F.row_number().over(rank))
    primary = g2.filter("group_rank = 1").select("old_work_id", "stripped", F.col("g_title").alias("primary_title"),
                                                 F.col("title_part").alias("primary_title_part"))
    held_id = (g2.filter("group_rank > 1 AND g_with_id > 0").select("old_work_id", "stripped").distinct()
               .withColumn("hold_id", F.lit(True)))
    cited = spark.table(WORKS).select(F.col("id").alias("old_work_id"), "cited_by_count")

    per_work = (w.join(primary, ["old_work_id", "stripped"])
                 .join(held_id, ["old_work_id", "stripped"], "left")
                 .join(cited, "old_work_id", "left")
                 .withColumn("cited_by_count", F.coalesce("cited_by_count", F.lit(0)))
                 .withColumn("hold_reason", F.when(F.col("hold_id"), F.lit("non_primary_group_has_identifier"))
                             .when(F.col("cited_by_count") >= HOLD_CITED_OVER, F.lit("cited_over_threshold")))
                 .withColumn("keys_to_move", F.col("n_titles") - 1))
    # keys_to_move counts every non-primary title; insertion groups (kept) are removed from the target below,
    # so wave sizes are an upper bound (exact for min_keys >= 4, where insertions are rare)
    # waves: smallest works first, so the low tiers fill wave 1 whatever max_keys is
    order = Window.orderBy(F.col("n_titles").asc(), F.col("old_work_id").asc(), F.col("stripped"))
    per_work = (per_work.withColumn("cum_keys", F.when(F.col("hold_reason").isNull(),
                                                       F.sum(F.when(F.col("hold_reason").isNull(), F.col("keys_to_move")).otherwise(0)).over(order)))
                        .withColumn("wave", F.when(F.col("hold_reason").isNull(), F.ceil(F.col("cum_keys") / F.lit(WAVE_SIZE)).cast("int")))
                        .drop("hold_id", "cum_keys"))

    # a non-primary title whose digit runs are a subset of the primary's (or vice versa) is a digit INSERTION --
    # a list number, footnote digit, isotope notation -- the same work, not a distinct one (q75 a8/a9: 18/20 same);
    # a digit SUBSTITUTION (year, part, volume, bill number) is a distinct object (19/20). Insertions stay put.
    digits = lambda c: F.expr(f"regexp_extract_all({c}, '([0-9]+)', 1)")
    moved_groups = (g2.filter("group_rank > 1")
                      .join(primary.select("old_work_id", "stripped", "primary_title_part"), ["old_work_id", "stripped"])
                      .withColumn("digit_insertion",
                                  (F.size(F.array_except(digits("title_part"), digits("primary_title_part"))) == 0) |
                                  (F.size(F.array_except(digits("primary_title_part"), digits("title_part"))) == 0))
                      .filter("NOT digit_insertion")
                      .select("old_work_id", "stripped", "title_part", "group_rank", "g_anchors", "g_title"))
    other = (spark.table(MAP).filter("title_author IS NOT NULL")
             .select(F.col("title_author").alias("new_key"), F.col("id").alias("map_id")))
    target = (a.join(moved_groups, ["old_work_id", "stripped", "title_part"])
               .join(per_work.select("old_work_id", "stripped", "n_titles", "n_anchors", "cited_by_count",
                                     "hold_reason", "wave", "primary_title"), ["old_work_id", "stripped"])
               .join(other, "new_key", "left")
               .withColumn("joins_existing_work_id", F.when(F.col("map_id") != F.col("old_work_id"), F.col("map_id")))
               .groupBy(*anchor, "old_work_id", "old_work_id_source", "old_created_dt", "new_key", "title_part", "stripped",
                        "group_rank", "n_titles", "n_anchors", "cited_by_count", "hold_reason", "wave", "primary_title", "title")
               .agg(F.min("joins_existing_work_id").alias("joins_existing_work_id"))
               .withColumn("staged_at", F.current_timestamp())
               .withColumn("executed_at", F.lit(None).cast("timestamp")))
    target.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)
    spark.sql(f"ALTER TABLE {TARGET} CLUSTER BY (wave, old_work_id)")

    review = (per_work.filter("cited_by_count > 0")
              .join(moved_groups.groupBy("old_work_id", "stripped")
                    .agg(F.slice(F.array_sort(F.collect_set("g_title")), 1, 5).alias("moved_titles_sample")),
                    ["old_work_id", "stripped"])
              .select("old_work_id", "cited_by_count", "n_titles", "n_anchors", "hold_reason", "wave", "primary_title", "moved_titles_sample")
              .orderBy(F.col("cited_by_count").desc()))
    review.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(REVIEW)

    print(f"staged in {int(time.time() - t0)}s")
    for r in rows(f"""
        SELECT COALESCE(CAST(wave AS STRING), concat('HELD: ', hold_reason)) AS wave,
               COUNT(DISTINCT old_work_id) AS works, COUNT(*) AS anchors,
               COUNT(DISTINCT old_work_id, title_part) AS keys_to_move,
               COUNT(DISTINCT CASE WHEN joins_existing_work_id IS NOT NULL THEN concat(old_work_id, '|', title_part) END) AS keys_joining_existing,
               COUNT(DISTINCT CASE WHEN cited_by_count > 0 THEN old_work_id END) AS cited_works,
               MAX(n_titles) AS max_titles_on_a_work
        FROM {TARGET} GROUP BY 1 ORDER BY 1"""):
        print(r)
    print(f"cited review rows: {one(f'SELECT COUNT(*) AS n FROM {REVIEW}')['n']:,}  ({REVIEW})")

# COMMAND ----------

if MODE in ("dry_run", "execute"):
    scope = f"{TARGET} x WHERE x.wave = {WAVE} AND x.hold_reason IS NULL AND x.executed_at IS NULL"
    plan = one(f"""
        SELECT COUNT(*) AS pins_to_delete, COUNT(DISTINCT x.old_work_id) AS works_touched,
               COUNT(DISTINCT x.old_work_id, x.title_part) AS keys_to_move,
               COUNT(DISTINCT CASE WHEN x.joins_existing_work_id IS NULL THEN concat(x.old_work_id, '|', x.title_part) END) AS expected_new_works,
               COUNT(DISTINCT CASE WHEN x.joins_existing_work_id IS NOT NULL THEN concat(x.old_work_id, '|', x.title_part) END) AS keys_joining_existing_works,
               COUNT(DISTINCT CASE WHEN x.cited_by_count > 0 THEN x.old_work_id END) AS cited_works_touched,
               MAX(x.cited_by_count) AS max_cited
        FROM {scope}""")
    alias_rows = one(f"""
        SELECT COUNT(*) AS alias_rows_to_delete FROM {MAP} m
        WHERE EXISTS (SELECT 1 FROM {scope} AND x.old_work_id = m.id AND x.new_key = m.title_author)""")
    print({**plan, **alias_rows})
    if plan["pins_to_delete"] == 0:
        dbutils.notebook.exit(f"nothing to do for wave {WAVE}")
    print("most-cited works in this wave:")
    for r in rows(f"""SELECT old_work_id, cited_by_count, n_titles, primary_title FROM {TARGET}
                      WHERE wave = {WAVE} AND hold_reason IS NULL AND cited_by_count > 0
                      GROUP BY 1, 2, 3, 4 ORDER BY 2 DESC LIMIT 20"""):
        print("  ", r)

# COMMAND ----------

if MODE == "execute":
    hour = datetime.datetime.utcnow().hour
    if 4 <= hour < 8:
        raise Exception("MapWorkIds writes the registries 05:00-07:00 UTC; run execute outside 04:00-08:00 UTC")
    if not CONFIRM:
        dbutils.notebook.exit("dry run only: pass confirm=yes to execute")
    if spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} exists: wave {WAVE} was already executed (or clean it up first)")

    spark.sql(f"""CREATE TABLE {AUDIT} AS
                  SELECT x.*, current_timestamp() AS audited_at,
                         (SELECT MAX(id) FROM {MAP}) AS max_map_id_at_execute
                  FROM {scope}""")
    n_audit = one(f"SELECT COUNT(*) AS n FROM {AUDIT}")["n"]
    assert n_audit == plan["pins_to_delete"], f"audit rows {n_audit} != planned {plan['pins_to_delete']}"

    pins = spark.sql(f"""DELETE FROM {REGISTRY} r
                         WHERE EXISTS (SELECT 1 FROM {AUDIT} x
                                       WHERE x.provenance = r.provenance AND x.native_id_namespace = r.native_id_namespace
                                         AND x.native_id = r.native_id)""").collect()[0].num_affected_rows
    aliases = spark.sql(f"""DELETE FROM {MAP} m
                            WHERE EXISTS (SELECT 1 FROM {AUDIT} x WHERE x.old_work_id = m.id AND x.new_key = m.title_author)""").collect()[0].num_affected_rows
    spark.sql(f"UPDATE {TARGET} SET executed_at = current_timestamp() WHERE wave = {WAVE} AND hold_reason IS NULL AND executed_at IS NULL")
    out = dict(wave=WAVE, audit_table=AUDIT, audited=n_audit, pins_deleted=pins, alias_rows_deleted=aliases)
    assert pins == n_audit, f"pins deleted {pins} != audited {n_audit}"
    print(out)
    dbutils.notebook.exit(str(out))

# COMMAND ----------

if MODE == "verify":
    if not spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} does not exist: wave {WAVE} was not executed")
    res = one(f"""
        WITH j AS (
          SELECT x.old_work_id, x.title_part, x.joins_existing_work_id, x.max_map_id_at_execute, r.work_id, r.work_id_source
          FROM {AUDIT} x
          LEFT JOIN {REGISTRY} r ON r.provenance = x.provenance AND r.native_id_namespace = x.native_id_namespace AND r.native_id = x.native_id
        )
        SELECT COUNT(*) AS audited,
               SUM(CASE WHEN work_id IS NOT NULL THEN 1 ELSE 0 END) AS repinned,
               SUM(CASE WHEN work_id IS NULL THEN 1 ELSE 0 END) AS still_pending,
               SUM(CASE WHEN work_id = old_work_id THEN 1 ELSE 0 END) AS back_on_old_work_BAD,
               SUM(CASE WHEN work_id = joins_existing_work_id THEN 1 ELSE 0 END) AS joined_existing_as_planned,
               SUM(CASE WHEN work_id > max_map_id_at_execute THEN 1 ELSE 0 END) AS on_a_new_work,
               COUNT(DISTINCT CASE WHEN work_id > max_map_id_at_execute THEN work_id END) AS new_works,
               COUNT(DISTINCT old_work_id, title_part) AS keys_moved,
               SUM(CASE WHEN work_id_source = 'title_author' THEN 1 ELSE 0 END) AS via_title_tier
        FROM j""")
    fanin = one(f"""
        SELECT MAX(n) AS max_anchors_on_a_new_work, percentile_approx(n, 0.5) AS median
        FROM (SELECT r.work_id, COUNT(*) AS n FROM {AUDIT} x
              JOIN {REGISTRY} r ON r.provenance = x.provenance AND r.native_id_namespace = x.native_id_namespace AND r.native_id = x.native_id
              WHERE r.work_id > x.max_map_id_at_execute GROUP BY 1)""")
    multi = one(f"""
        SELECT COUNT(*) AS moved_keys_now_on_multiple_ids FROM (
          SELECT m.title_author FROM {MAP} m
          JOIN (SELECT DISTINCT new_key FROM {AUDIT}) k ON k.new_key = m.title_author
          GROUP BY 1 HAVING COUNT(DISTINCT m.id) > 1)""")
    out = {**res, **fanin, **multi}
    print(out)
    dbutils.notebook.exit(str(out))
