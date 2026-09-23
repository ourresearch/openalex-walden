# Databricks notebook source
# MAGIC %md
# MAGIC # Study-design student tagger (oxjob #1335)
# MAGIC
# MAGIC Runs the fine-tuned encoder (`utils/study_design_student.py`, weights at
# MAGIC `sd.STUDENT_MODEL_DIR`) over the queue chunks in priority order, on the
# MAGIC GPU workers via `mapInPandas`. Every prediction is kept in
# MAGIC `works_study_design_student` (raw probabilities, derived scores, route),
# MAGIC so thresholds can change without another GPU pass. Works the student is
# MAGIC sure about (`sd.student_route == 'student'`) also get a
# MAGIC `works_study_design_tagger` row at `sd.STUDENT_VERSION`; the Jev tagger
# MAGIC that runs next sees only the residual.
# MAGIC
# MAGIC Resumable like the Jev tagger: one `works_study_design_progress` row per
# MAGIC finished chunk, `tagger_version = sd.STUDENT_VERSION`.

# COMMAND ----------

import os
import sys
import time

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "study_design.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "study_design.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import study_design as sd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (ArrayType, BooleanType, FloatType, IntegerType, LongType, MapType,  # noqa: E402
                               StringType, StructField, StructType)

dbutils.widgets.text("schema", "openalex.works", "target schema")
dbutils.widgets.text("max_works", "2000000", "stop after this many works this run")
dbutils.widgets.text("max_minutes", "150", "stop starting new chunks after this long")
dbutils.widgets.text("chunks_per_wave", "8", "queue chunks per Spark wave (8 x 50K works)")
dbutils.widgets.text("batch_size", "128", "encoder batch size per GPU")
dbutils.widgets.text("dry_run", "false", "true = report the queue, run nothing")

SCHEMA = dbutils.widgets.get("schema").strip()
MAX_WORKS = int(dbutils.widgets.get("max_works"))
MAX_MINUTES = float(dbutils.widgets.get("max_minutes"))
WAVE = int(dbutils.widgets.get("chunks_per_wave"))
BATCH = int(dbutils.widgets.get("batch_size"))
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"

QUEUE = f"{SCHEMA}.works_study_design_queue"
TAGGER = f"{SCHEMA}.works_study_design_tagger"
STUDENT = f"{SCHEMA}.works_study_design_student"
PROGRESS = f"{SCHEMA}.works_study_design_progress"
MODEL_DIR = sd.STUDENT_MODEL_DIR
REPO_UTILS = os.path.join(REPO_ROOT, "utils")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {STUDENT} (
  work_id BIGINT NOT NULL,
  probabilities MAP<STRING, FLOAT>,
  is_rct FLOAT,
  human_subjects FLOAT,
  scores MAP<STRING, FLOAT>,
  route STRING,
  student_version STRING,
  build_id STRING,
  updated_at TIMESTAMP
) USING DELTA CLUSTER BY (work_id)
COMMENT 'Every study-design student prediction (oxjob #1335), owned or not; re-threshold from here without a GPU pass'
""")

OUT_SCHEMA = StructType([
    StructField("work_id", LongType(), False),
    StructField("probabilities", MapType(StringType(), FloatType()), True),
    StructField("is_rct", FloatType(), True),
    StructField("human_subjects", FloatType(), True),
    StructField("scores", MapType(StringType(), FloatType()), True),
    StructField("route", StringType(), True),
    StructField("tagger_values", ArrayType(StringType()), True),
    StructField("stated_random", BooleanType(), True),
    StructField("abstract_chars", IntegerType(), True),
])

# COMMAND ----------

build_id = spark.sql(f"SELECT max(build_id) AS b FROM {QUEUE}").collect()[0].b
if not build_id:
    raise RuntimeError(f"{QUEUE} is empty; run BuildStudyDesignQueue first")
done = {int(r.chunk_id) for r in spark.sql(
    f"SELECT chunk_id FROM {PROGRESS} WHERE build_id = '{build_id}' AND tagger_version = '{sd.STUDENT_VERSION}'").collect()}
chunks = [(int(r.chunk_id), int(r.n)) for r in
          spark.sql(f"SELECT chunk_id, count(*) AS n FROM {QUEUE} GROUP BY chunk_id ORDER BY chunk_id").collect()]
todo = [(c, n) for c, n in chunks if c not in done]
print(f"queue build {build_id}: {len(chunks)} chunks / {sum(n for _, n in chunks):,} works; "
      f"{len(done)} chunks done by the student this build; {len(todo)} chunks / {sum(n for _, n in todo):,} to go; "
      f"student {sd.STUDENT_VERSION} from {MODEL_DIR}")
if DRY_RUN:
    dbutils.notebook.exit("dry run")

# COMMAND ----------

def predict_partition(batches):
    """Runs on a GPU worker. Loads the model once per Python worker process; yields the output rows per pandas batch."""
    import pandas as pd
    import sys as _sys
    if REPO_UTILS not in _sys.path:
        _sys.path.insert(0, os.path.dirname(REPO_UTILS))
    from utils import study_design as _sd
    from utils import study_design_student as _st
    global _STUDENT_MODEL
    try:
        _STUDENT_MODEL
    except NameError:
        _STUDENT_MODEL = _st.StudentModel(MODEL_DIR, batch_size=BATCH)
    for pdf in batches:
        works = pdf.where(pdf.notna(), None).to_dict("records")
        preds = _STUDENT_MODEL.predict(works)
        rows = []
        for w, (probs, is_rct, human) in zip(works, preds):
            row = _sd.student_answer_row(w, probs, is_rct, human)
            rows.append({
                "work_id": row["work_id"], "probabilities": row["probabilities"], "is_rct": row["is_rct"],
                "human_subjects": row["human_subjects"], "scores": row["scores"], "route": _sd.student_route(row["scores"]),
                "tagger_values": row["tagger_values"], "stated_random": row["stated_random"],
                "abstract_chars": row["abstract_chars"],
            })
        yield pd.DataFrame(rows)

# COMMAND ----------

run_t0 = time.time()
tot = 0
for i in range(0, len(todo), WAVE):
    if tot >= MAX_WORKS:
        print(f"stop: max_works {MAX_WORKS:,} reached")
        break
    if (time.time() - run_t0) / 60 >= MAX_MINUTES:
        print(f"stop: max_minutes {MAX_MINUTES:.0f} reached")
        break
    wave = todo[i:i + WAVE]
    ids = [c for c, _ in wave]
    n_wave = sum(n for _, n in wave)
    t0 = time.time()
    src = (spark.table(QUEUE).where(F.col("chunk_id").isin(ids))
           .select("work_id", "title", "venue", "abstract"))
    # ~2K works per partition keeps every GPU busy and the pandas batches small
    out = src.repartition(max(8, n_wave // 2000)).mapInPandas(predict_partition, schema=OUT_SCHEMA).cache()
    n_out = out.count()
    (out.select("work_id", "probabilities", "is_rct", "human_subjects", "scores", "route")
        .withColumn("student_version", F.lit(sd.STUDENT_VERSION)).withColumn("build_id", F.lit(build_id))
        .withColumn("updated_at", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(STUDENT))
    owned = out.where(F.col("route") == "student")
    n_owned = owned.count()
    (owned.select("work_id", "tagger_values", "scores", "probabilities", "is_rct", "human_subjects", "stated_random",
                  "abstract_chars")
        .withColumn("input_tokens", F.lit(0)).withColumn("tagger_version", F.lit(sd.STUDENT_VERSION))
        .withColumn("jev_model", F.lit(None).cast(StringType())).withColumn("updated_at", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(TAGGER))
    out.unpersist()
    secs = time.time() - t0
    spark.createDataFrame(
        [(build_id, int(c), int(n), int(n), 0, 0, 0.0, float(secs) * n / max(n_wave, 1), sd.STUDENT_VERSION) for c, n in wave],
        "build_id STRING, chunk_id BIGINT, n_queued INT, n_tagged INT, n_failed INT, input_tokens BIGINT, usd DOUBLE, "
        "seconds DOUBLE, tagger_version STRING",
    ).withColumn("done_at", F.current_timestamp()).write.format("delta").mode("append").saveAsTable(PROGRESS)
    tot += n_out
    print(f"wave {i // WAVE + 1}: chunks {ids[0]}..{ids[-1]} {n_out:,} works ({n_out / secs:.0f}/s), "
          f"student-owned {n_owned:,} ({n_owned / max(n_out, 1):.1%}), total {tot:,} in {(time.time() - run_t0) / 60:.0f} min",
          flush=True)

print(f"done: {tot:,} works this run, {(time.time() - run_t0) / 60:.0f} min")
