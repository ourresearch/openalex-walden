# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.24-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC # Seed `work_id_map` with digit-bearing title aliases (oxjob #880, problem 1)
# MAGIC
# MAGIC `normalize_title` keeps digits from dlt_utils 0.3.20. `work_id_map.title_author` holds the
# MAGIC old digit-stripped form and cannot be re-keyed in place, so a new record with a digit in
# MAGIC its title would miss the map and mint a duplicate (~2,200-4,500 works/night, #880 q58).
# MAGIC This seeds the new-form key as an **alias** (a map row with an explicit existing `id`).
# MAGIC Per-pair rule (#880 q71/q72, 2026-09-03): every `(old_key, new_key, work_id)` pair is staged with
# MAGIC `n_works_on_new` (works carrying that new key) and `n_new_on_work` (new keys that work holds
# MAGIC under the old key). `insert` aliases pairs with `n_works_on_new <= max_works_per_key` (default 3,
# MAGIC MapWorkIds' own guard -- it resolves a key on <= 3 works to MIN(id)) and
# MAGIC `n_new_on_work <= max_keys_per_work` (default 3). A work holding many new keys under one old key
# MAGIC is a mega-work (`folderscan` -> 176,838 new keys) and gets NO alias, so the sweep can split it.
# MAGIC
# MAGIC Modes: `stage` computes into the stage table (timing trial with `limit`); `insert` appends
# MAGIC the admitted stage rows to `work_id_map` (idempotent, anti-joined on key + id); `verify` runs
# MAGIC the fork probe; `prune` is the post-refresh cleanup and is dry-run unless `confirm = yes`.
# MAGIC Run `insert` outside the 05:00-07:00 UTC MapWorkIds window. Design: #880 PLAN § Problem 1.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "stage", ["stage", "insert", "verify", "prune"])
dbutils.widgets.text("limit", "0")
dbutils.widgets.text("stage_table", "openalex.works.work_id_map_digit_aliases_stage")
dbutils.widgets.text("confirm", "no")
dbutils.widgets.text("max_works_per_key", "3")
dbutils.widgets.text("max_keys_per_work", "3")

MODE = dbutils.widgets.get("mode")
LIMIT = int(dbutils.widgets.get("limit"))
STAGE = dbutils.widgets.get("stage_table")
CONFIRM = dbutils.widgets.get("confirm") == "yes"
MAX_WORKS_PER_KEY = int(dbutils.widgets.get("max_works_per_key"))
MAX_KEYS_PER_WORK = int(dbutils.widgets.get("max_keys_per_work"))

import time
from pyspark.sql import functions as F
from openalex.dlt.normalize import normalize_title_udf

# COMMAND ----------

if MODE == "stage":
    t0 = time.time()
    # one row per anchor, as MapWorkIds reads it; only title-keyed anchors (self-keys never merge)
    t = (spark.table("openalex.works.locations_w_types")
         .withColumn("rwcnt", F.row_number().over(
             __import__("pyspark.sql.window", fromlist=["Window"]).Window
             .partitionBy("provenance", "native_id_namespace", "native_id")
             .orderBy(F.col("updated_date").desc())))
         .filter("rwcnt = 1")
         .filter("merge_key.title_author IS NOT NULL AND merge_key.title_author <> ''")
         .filter("merge_key.title_author <> concat(native_id, provenance)")
         .select("provenance", "native_id_namespace", "native_id", "title", "authors_exist",
                 F.expr("get(authors, 0).author_key").alias("author_key"),  # get(): NULL on empty, ANSI-safe
                 F.col("merge_key.title_author").alias("old_key")))
    r = spark.table("openalex.works.location_work_ids").filter("work_id IS NOT NULL") \
             .select("provenance", "native_id_namespace", "native_id", "work_id")
    rows = t.join(r, ["provenance", "native_id_namespace", "native_id"])
    if LIMIT:
        # trial: a slice of OLD KEYS, so each group is complete and the 1:1 / 1:N test is honest
        keys = rows.select("old_key").distinct().orderBy(F.xxhash64("old_key")).limit(LIMIT)
        rows = rows.join(keys, "old_key")

    # a digit-free title normalizes identically under both rules -- no UDF call needed
    has_digit = F.col("title").rlike("[0-9]")
    new_norm = F.when(has_digit, normalize_title_udf(F.col("title")))
    new_key = F.when(~has_digit, F.col("old_key")).otherwise(
        F.when(F.col("authors_exist"), F.concat_ws("_", new_norm, F.col("author_key"))).otherwise(new_norm))
    keyed = rows.withColumn("new_key", new_key).filter("new_key <> old_key")
    # mega-shape count is taken BEFORE the digit-only filter so a work's full key spread is seen
    per_work = keyed.groupBy("old_key", "work_id").agg(F.countDistinct("new_key").alias("n_new_on_work"))
    # #807 cleans the display title AFTER the key is built, so normalize(current title) can differ
    # from the stored key for reasons other than digits; keep only rows whose sole change is digits
    pairs = (keyed.filter("regexp_replace(new_key, '[0-9]', '') = old_key")
             .groupBy("old_key", "new_key", "work_id").agg(F.count("*").alias("n_rows")))
    per_new = pairs.groupBy("new_key").agg(F.countDistinct("work_id").alias("n_works_on_new"))
    stage = (pairs.join(per_new, "new_key").join(per_work, ["old_key", "work_id"])
             .select("old_key", "new_key", "work_id", "n_rows", "n_works_on_new", "n_new_on_work")
             .withColumn("created_date", F.current_date()))
    stage.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGE)

    st = spark.table(STAGE)
    summary = st.agg(
        F.count("*").alias("pairs"),
        F.countDistinct("old_key").alias("old_keys"),
        F.sum(F.when((F.col("n_works_on_new") == 1) & (F.col("n_new_on_work") == 1), 1).otherwise(0)).alias("one_to_one"),
        F.sum(F.when((F.col("n_works_on_new") <= MAX_WORKS_PER_KEY) & (F.col("n_new_on_work") <= MAX_KEYS_PER_WORK), 1).otherwise(0)).alias("admitted"),
        F.sum(F.when(F.col("n_works_on_new") > MAX_WORKS_PER_KEY, 1).otherwise(0)).alias("skipped_key_on_too_many_works"),
        F.sum(F.when(F.col("n_new_on_work") > MAX_KEYS_PER_WORK, 1).otherwise(0)).alias("skipped_mega_shape")).collect()[0]
    out = {**summary.asDict(), "seconds": int(time.time()-t0)}
    print(out); dbutils.notebook.exit(str(out))

# COMMAND ----------

if MODE == "insert":
    admitted = f"""
      SELECT s.work_id, s.new_key FROM {STAGE} s
      WHERE regexp_replace(s.new_key, '[0-9]', '') = s.old_key
        AND s.n_works_on_new <= {MAX_WORKS_PER_KEY} AND s.n_new_on_work <= {MAX_KEYS_PER_WORK}
    """
    # a new key already in the map under ANOTHER id is a duplicate minted since the flip (#880 q72);
    # the alias still goes in (MapWorkIds takes MIN(id) on <= 3), the duplicate is merged later
    pre = spark.sql(f"""
      SELECT COUNT(*) AS admitted,
             SUM(CASE WHEN m.id IS NULL THEN 1 ELSE 0 END) AS to_insert,
             COUNT(DISTINCT CASE WHEN COALESCE(o.n_ids, 0) > (CASE WHEN m.id IS NULL THEN 0 ELSE 1 END)
                                 THEN a.new_key END) AS keys_already_on_other_id
      FROM ({admitted}) a
      LEFT JOIN openalex.works.work_id_map m ON m.title_author = a.new_key AND m.id = a.work_id
      LEFT JOIN (SELECT title_author, COUNT(DISTINCT id) AS n_ids FROM openalex.works.work_id_map GROUP BY 1) o
             ON o.title_author = a.new_key
    """).collect()[0].asDict()
    print(pre)
    before = spark.sql("SELECT COUNT(*) AS n FROM openalex.works.work_id_map").collect()[0].n
    spark.sql(f"""
      INSERT INTO openalex.works.work_id_map (id, doi, pmid, arxiv, title_author, created_date, updated_date)
      SELECT a.work_id, NULL, NULL, NULL, a.new_key, current_date(), current_timestamp()
      FROM ({admitted}) a
      LEFT ANTI JOIN openalex.works.work_id_map m ON m.title_author = a.new_key AND m.id = a.work_id
    """)
    after = spark.sql("SELECT COUNT(*) AS n FROM openalex.works.work_id_map").collect()[0].n
    out = {**pre, "work_id_map_before": before, "work_id_map_after": after, "inserted": after - before}
    print(out); dbutils.notebook.exit(str(out))

# COMMAND ----------

if MODE == "verify":
    # the fork probe (#880 q58): recent digit-titled title-tier joins must resolve, on their NEW
    # key, to the id they already have -- i.e. the alias exists and points at the right work
    probe = spark.sql("""
      SELECT r.work_id, t.title, t.authors_exist, get(t.authors, 0).author_key AS author_key
      FROM openalex.works.location_work_ids r
      JOIN openalex.works.locations_w_types t
        ON t.provenance = r.provenance AND t.native_id_namespace = r.native_id_namespace
       AND t.native_id = r.native_id
      WHERE r.work_id_source = 'title_author' AND r.openalex_created_dt >= current_date() - 7
        AND t.title RLIKE '[0-9]'
    """)
    nn = normalize_title_udf(F.col("title"))
    probe = probe.withColumn("new_key", F.when(F.col("authors_exist"), F.concat_ws("_", nn, F.col("author_key"))).otherwise(nn))
    m = spark.table("openalex.works.work_id_map").select(F.col("title_author").alias("new_key"), F.col("id").alias("map_id"))
    res = probe.join(m, "new_key", "left").agg(
        F.count("*").alias("probed"),
        F.sum(F.when(F.col("map_id") == F.col("work_id"), 1).otherwise(0)).alias("alias_ok"),
        F.sum(F.when(F.col("map_id").isNull(), 1).otherwise(0)).alias("would_fork")).collect()[0]
    print(res.asDict()); dbutils.notebook.exit(str(res.asDict()))

# COMMAND ----------

if MODE == "prune":
    # post-refresh only: once no pinned row still carries an old-form key, the old rows are dead weight
    still_old = spark.sql(f"""
      SELECT COUNT(*) AS n FROM openalex.works.locations_w_types t
      JOIN {STAGE} s ON s.old_key = t.merge_key.title_author
    """).collect()[0].n
    dead = spark.sql(f"""
      SELECT COUNT(*) AS n FROM openalex.works.work_id_map m JOIN {STAGE} s
        ON m.title_author = s.old_key AND m.id = s.work_id
    """).collect()[0].n
    print(f"rows still carrying an old-form key: {still_old:,}  |  prunable map rows: {dead:,}")
    if CONFIRM and still_old == 0:
        spark.sql(f"""
          DELETE FROM openalex.works.work_id_map m
          WHERE EXISTS (SELECT 1 FROM {STAGE} s WHERE m.title_author = s.old_key AND m.id = s.work_id)
        """)
        print("pruned")
    elif CONFIRM:
        print("NOT pruned: old-form keys are still live in locations_w_types")
