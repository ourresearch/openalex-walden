# Databricks notebook source
# MAGIC %md
# MAGIC # Jev Quality Sample (oxjob #1301)
# MAGIC
# MAGIC Nightly drift monitor for five pipeline decisions, judged by Jev (TypeSafe
# MAGIC `jev-1.13.0`) with a capped Opus 5 tail on the highest-confidence flags.
# MAGIC Runs as a task of the Authorship Daily Metrics job in the 22:30 UTC window
# MAGIC (the authorship stratum reads the ephemeral `pending_author_assignments`
# MAGIC that the next end2end replaces at 05:00 UTC).
# MAGIC
# MAGIC **Observation-only**: writes `openalex.monitoring.jev_quality_items` (per-run
# MAGIC scratch), `jev_quality_sample` (tall, one row per judged item),
# MAGIC `jev_quality_daily` (per stratum × dimension) and tall rows in
# MAGIC `openalex.monitoring.metrics` (component `jev_quality`). Never pipeline tables.
# MAGIC
# MAGIC | stratum | decision under test | dimension | night | design n |
# MAGIC |---|---|---|---|---|
# MAGIC | a_type | work type by `classified_rule` | rule | `locations_w_types.updated_date` | 30/rule, cap 3,000 |
# MAGIC | b_affil | new (affiliation string, institution) pair | matcher score band | `affiliation_strings_lookup.created_datetime` | 3,000 |
# MAGIC | c_merge | new `title_author` location joining an older work | new location's provenance | `location_work_ids.openalex_created_dt` | 2,000 |
# MAGIC | d_repo | repository record admitted as a work | `endpoint_id` | same | 30/endpoint, cap 3,000 |
# MAGIC | e_auth | authorship seat bound to a profile (Arm A prompt) | match tier | tonight's run | 1,250/tier (~20,000) |
# MAGIC
# MAGIC **What the numbers mean** (probe 2026-09-21, EXPLORE.md): Jev's flags at the
# MAGIC per-stratum cuts are 0.75-1.00 precise against Opus 5, but its recall is only
# MAGIC 35-85%, so `precision_jev` is a drift signal per rule / band / provenance /
# MAGIC endpoint / tier, not an error rate. The Opus tail (cap 150/night, ~$2) keeps
# MAGIC Jev honest and leaves labelled rows behind.
# MAGIC
# MAGIC **Cost guard** (standing rule): states are materialised FIRST into
# MAGIC `jev_quality_items`; the pass aborts if the projected Jev + Opus spend exceeds
# MAGIC `abort_threshold_usd`. Measured: Jev ~$0.9/night at design volumes, Opus tail
# MAGIC ~$0.015/item uncached via FMAPI.
# MAGIC
# MAGIC **Checkpointing**: items are written once per night; the Jev pass appends
# MAGIC sample rows per chunk and skips item_ids already judged for the night; the Opus
# MAGIC tail only touches rows with `opus_verdict IS NULL`. A rerun on the same
# MAGIC `snapshot_date` resumes; it does not re-pay.

# COMMAND ----------

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType)

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
sys.path.insert(0, REPO_ROOT)
from utils import jev_quality as jq  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("snapshot_date", "", "YYYY-MM-DD (blank = today UTC)")
dbutils.widgets.text("schema", "openalex.monitoring", "target schema (use a scratch schema for dry runs)")
dbutils.widgets.text("type_per_rule", "30", "a_type: items per classified_rule")
dbutils.widgets.text("type_cap", "3000", "a_type: cap")
dbutils.widgets.text("affil_cap", "3000", "b_affil: pairs")
dbutils.widgets.text("merge_cap", "2000", "c_merge: pairs")
dbutils.widgets.text("repo_per_endpoint", "30", "d_repo: items per endpoint")
dbutils.widgets.text("repo_cap", "3000", "d_repo: cap")
dbutils.widgets.text("auth_per_tier", "1250", "e_auth: seats per match tier (16 tiers ~ 20,000)")
dbutils.widgets.text("tail_cap", "150", "Opus tail: items per night across strata")
dbutils.widgets.text("tail_strata", ",".join(jq.TAIL_STRATA_DEFAULT), "Opus tail: strata (b_affil off: Jev is 0.97+ precise there)")
dbutils.widgets.text("opus_model", jq.OPUS_MODEL, "FMAPI endpoint for the tail")
dbutils.widgets.text("abort_threshold_usd", "10", "abort if projected Jev + Opus spend exceeds this")
dbutils.widgets.text("jev_concurrency", "16", "Jev threads (16 x ~1.7K tok / 0.3 s is far under 1.2M tok/s)")
dbutils.widgets.text("mode", "full", "full | materialise_only (no LLM calls: stage items and project cost)")

_sd = dbutils.widgets.get("snapshot_date").strip()
RUN_DATE = datetime.strptime(_sd, "%Y-%m-%d").date() if _sd else datetime.now(timezone.utc).date()
SCHEMA = dbutils.widgets.get("schema").strip()
W = lambda k: dbutils.widgets.get(k).strip()  # noqa: E731
SIZES = dict(type_per_rule=int(W("type_per_rule")), type_cap=int(W("type_cap")), affil_cap=int(W("affil_cap")),
             merge_cap=int(W("merge_cap")), repo_per_endpoint=int(W("repo_per_endpoint")), repo_cap=int(W("repo_cap")),
             auth_per_tier=int(W("auth_per_tier")))
TAIL_CAP = int(W("tail_cap"))
TAIL_STRATA = [s for s in W("tail_strata").split(",") if s]
OPUS_MODEL = W("opus_model")
ABORT_USD = float(W("abort_threshold_usd"))
JEV_CONCURRENCY = int(W("jev_concurrency"))
MODE = W("mode")

ITEMS = f"{SCHEMA}.jev_quality_items"
SAMPLE = f"{SCHEMA}.jev_quality_sample"
DAILY = f"{SCHEMA}.jev_quality_daily"
METRICS = f"{SCHEMA}.metrics"
COMPONENT, SOURCE = "jev_quality", "JevQualitySample"
CHUNK = 500

print(f"RUN_DATE={RUN_DATE} schema={SCHEMA} sizes={SIZES} tail_cap={TAIL_CAP} tail_strata={TAIL_STRATA} "
      f"opus={OPUS_MODEL} abort=${ABORT_USD} mode={MODE}")


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ITEMS} (
  snapshot_date DATE NOT NULL,
  stratum       STRING NOT NULL,   -- a_type | b_affil | c_merge | d_repo | e_auth
  dimension     STRING,            -- classified_rule | score band | provenance | endpoint_id | tier
  item_id       STRING NOT NULL,
  source_night  DATE,              -- the night this item's table was read at
  meta          STRING,            -- JSON: slicing context (assigned_type, inst_id, work_id, ...)
  state         STRING NOT NULL,   -- JSON or prompt text: exactly what the judges see
  state_chars   INT,
  staged_at     TIMESTAMP NOT NULL
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SAMPLE} (
  snapshot_date   DATE NOT NULL,
  stratum         STRING NOT NULL,
  dimension       STRING,
  item_id         STRING NOT NULL,
  source_night    DATE,
  meta            STRING,
  state           STRING,
  jev_model       STRING,
  jev_answers     STRING,          -- JSON: the raw answers (probabilities per option)
  jev_p_wrong     DOUBLE,          -- Jev's probability that the pipeline's decision is wrong
  jev_flag        BOOLEAN,         -- jev_p_wrong >= the stratum cut
  jev_label       STRING,          -- wrong | assigned_is_fine | names_it | version | same | scholarly_work | ...
  jev_cut         DOUBLE,
  jev_tokens      INT,
  jev_cost_usd    DOUBLE,
  judged_at       TIMESTAMP,
  opus_model      STRING,          -- tail only; NULL when not sent
  opus_verdict    STRING,          -- tail: the verdict label (better_type / verdict / same_work / record_kind / same_person)
  opus_wrong      BOOLEAN,         -- tail: pipeline wrong per Opus; NULL = abstained or not sent
  opus_reason     STRING,
  opus_tokens_in  INT,
  opus_tokens_out INT,
  opus_cost_usd   DOUBLE,
  opus_judged_at  TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DAILY} (
  snapshot_date       DATE NOT NULL,
  stratum             STRING NOT NULL,
  dimension           STRING,        -- NULL = whole stratum
  n                   INT,
  n_flagged           INT,
  precision_jev       DOUBLE,        -- 1 - n_flagged / n
  n_opus              INT,           -- tail rows with a decisive Opus verdict
  n_opus_confirmed    INT,           -- of those, Opus agrees the pipeline is wrong
  opus_flag_precision DOUBLE,        -- n_opus_confirmed / n_opus (how much to trust Jev's flags)
  precision_opus_adj  DOUBLE,        -- 1 - n_flagged * opus_flag_precision / n (stratum-level tail)
  jev_cost_usd        DOUBLE,
  opus_cost_usd       DOUBLE,
  computed_at         TIMESTAMP NOT NULL
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METRICS} (
  snapshot_date DATE, component STRING, metric STRING, dimension STRING, value DOUBLE, source STRING, computed_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pick each table's night
# MAGIC `locations_w_types.updated_date` lags `location_work_ids.openalex_created_dt` by a day on
# MAGIC some nights; each stratum reads its own table's latest full night at or before `snapshot_date`.

# COMMAND ----------

def pick_night(table, date_expr, min_rows):
    rows = spark.sql(jq.sql_night_counts(table, date_expr, str(RUN_DATE))).collect()
    for r in rows:
        if r["n"] >= min_rows:
            log(f"{table}: night {r['night']} ({r['n']:,} rows)")
            return str(r["night"])
    log(f"{table}: no night with >= {min_rows} rows in the last 4 days ({[(str(r['night']), r['n']) for r in rows]})")
    return None


NIGHT_LWT = pick_night(jq.LWT, "updated_date", 50_000)
NIGHT_LWI = pick_night(jq.LWI, "openalex_created_dt", 5_000)
NIGHT_LOOKUP = pick_night(jq.LOOKUP, "DATE(created_datetime)", 500)
HAVE_PENDING = spark.catalog.tableExists(jq.PENDING) and \
    "name_match_tier" in [f.name for f in spark.table(jq.PENDING).schema]
if not HAVE_PENDING:
    log("pending_author_assignments missing or pre-match_tier: skipping e_auth")

STRATUM_SQL = {
    "a_type": (NIGHT_LWT, lambda: jq.sql_a_type(NIGHT_LWT, SIZES["type_per_rule"], SIZES["type_cap"])),
    "b_affil": (NIGHT_LOOKUP, lambda: jq.sql_b_affil(NIGHT_LOOKUP, SIZES["affil_cap"])),
    "c_merge": (NIGHT_LWI, lambda: jq.sql_c_merge(NIGHT_LWI, SIZES["merge_cap"])),
    "d_repo": (NIGHT_LWI, lambda: jq.sql_d_repo(NIGHT_LWI, SIZES["repo_per_endpoint"], SIZES["repo_cap"])),
    "e_auth": (str(RUN_DATE) if HAVE_PENDING else None, lambda: jq.sql_e_auth(SIZES["auth_per_tier"])),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialise states (no LLM calls yet; cost is projected from these)

# COMMAND ----------

ITEMS_SCHEMA = StructType([
    StructField("snapshot_date", DateType(), False), StructField("stratum", StringType(), False),
    StructField("dimension", StringType(), True), StructField("item_id", StringType(), False),
    StructField("source_night", DateType(), True), StructField("meta", StringType(), True),
    StructField("state", StringType(), False), StructField("state_chars", IntegerType(), True),
])

staged = {r["stratum"]: r["n"] for r in spark.sql(
    f"SELECT stratum, COUNT(*) n FROM {ITEMS} WHERE snapshot_date = DATE'{RUN_DATE}' GROUP BY 1").collect()}

for stratum in jq.STRATA:
    night, build_sql = STRATUM_SQL[stratum]
    if night is None:
        log(f"{stratum}: no night, skipped")
        continue
    if staged.get(stratum):
        log(f"{stratum}: {staged[stratum]:,} items already staged for {RUN_DATE} (checkpoint), skipping")
        continue
    t0 = time.time()
    rows = [r.asDict(recursive=True) for r in spark.sql(build_sql()).collect()]
    out, seen = [], set()
    for r in rows:
        item_id, dim, meta, state = jq.build_state(stratum, r)
        if item_id in seen:
            continue
        seen.add(item_id)
        state_s = state if isinstance(state, str) else json.dumps(state, ensure_ascii=False)
        out.append((RUN_DATE, stratum, dim, item_id, datetime.strptime(night, "%Y-%m-%d").date(),
                    json.dumps(meta, ensure_ascii=False, default=str), state_s, len(state_s)))
    if out:
        (spark.createDataFrame(out, ITEMS_SCHEMA).withColumn("staged_at", F.current_timestamp())
         .write.format("delta").mode("append").saveAsTable(ITEMS))
    staged[stratum] = len(out)
    log(f"{stratum}: staged {len(out):,} items from night {night} in {time.time() - t0:.0f}s")

# COMMAND ----------

# Cost projection from the ACTUAL staged states; abort above threshold.
work_types = {r["id"]: r["description"] for r in spark.table("openalex.common.work_types").collect()}
QS = {s: jq.questions(s, work_types) for s in jq.STRATA}
OPUS_SPEC = {s: jq.opus_spec(s, work_types) for s in jq.STRATA}

proj = spark.sql(f"""
    SELECT stratum, COUNT(*) n, SUM(state_chars) chars, AVG(state_chars) avg_chars
    FROM {ITEMS} WHERE snapshot_date = DATE'{RUN_DATE}' GROUP BY 1""").collect()
q_chars = {s: len(json.dumps(QS[s], ensure_ascii=False)) for s in jq.STRATA}
jev_tokens = sum((r["chars"] + r["n"] * q_chars[r["stratum"]]) / jq.JEV_CHARS_PER_TOKEN for r in proj)
jev_usd = jev_tokens * jq.JEV_USD_PER_TOKEN
tail_avg_chars = max([r["avg_chars"] for r in proj if r["stratum"] in TAIL_STRATA] or [1500])
sys_chars = max(len(OPUS_SPEC[s][0]) for s in TAIL_STRATA) if TAIL_STRATA else 0
opus_usd = jq.project_opus_usd(TAIL_CAP, tail_avg_chars, sys_chars)
n_items = sum(r["n"] for r in proj)
log(f"staged {n_items:,} items: " + ", ".join(f"{r['stratum']}={r['n']:,}" for r in proj))
log(f"projected: Jev {jev_tokens/1e6:.1f}M tokens = ${jev_usd:.2f}; Opus tail {TAIL_CAP} x ~{(tail_avg_chars + sys_chars)/4:.0f} tok = ${opus_usd:.2f}; total ${jev_usd + opus_usd:.2f}")
if jev_usd + opus_usd >= ABORT_USD:
    raise RuntimeError(f"projected ${jev_usd + opus_usd:.2f} >= ${ABORT_USD} abort threshold; reduce the size widgets or raise the threshold explicitly")
if n_items == 0:
    dbutils.notebook.exit("no items staged; nothing to judge")
if MODE == "materialise_only":
    dbutils.notebook.exit(f"materialise_only: {n_items} items staged, projected ${jev_usd + opus_usd:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Jev pass (chunked, checkpointed per chunk)

# COMMAND ----------

SAMPLE_SCHEMA = StructType([
    StructField("snapshot_date", DateType(), False), StructField("stratum", StringType(), False),
    StructField("dimension", StringType(), True), StructField("item_id", StringType(), False),
    StructField("source_night", DateType(), True), StructField("meta", StringType(), True),
    StructField("state", StringType(), True), StructField("jev_model", StringType(), True),
    StructField("jev_answers", StringType(), True), StructField("jev_p_wrong", DoubleType(), True),
    StructField("jev_flag", BooleanType(), True), StructField("jev_label", StringType(), True),
    StructField("jev_cut", DoubleType(), True), StructField("jev_tokens", IntegerType(), True),
    StructField("jev_cost_usd", DoubleType(), True),
])

jev = jq.JevClient(dbutils.secrets.get(scope="typesafe", key="api_key"), concurrency=JEV_CONCURRENCY)
todo = [r.asDict() for r in spark.sql(f"""
    SELECT i.stratum, i.dimension, i.item_id, i.source_night, i.meta, i.state
    FROM {ITEMS} i
    LEFT ANTI JOIN (SELECT stratum, item_id FROM {SAMPLE} WHERE snapshot_date = DATE'{RUN_DATE}') s
      ON s.stratum = i.stratum AND s.item_id = i.item_id
    WHERE i.snapshot_date = DATE'{RUN_DATE}'
    ORDER BY i.stratum, i.item_id""").collect()]
log(f"Jev: {len(todo):,} items to judge ({n_items - len(todo):,} already judged for {RUN_DATE})")

jev_errors = {s: 0 for s in jq.STRATA}
t0, done, spent = time.time(), 0, 0.0
for start in range(0, len(todo), CHUNK):
    chunk = todo[start:start + CHUNK]
    for it in chunk:
        it["state"] = json.loads(it["state"]) if it["stratum"] != "e_auth" else it["state"]
    results = jev.judge_many(chunk, QS)
    rows = []
    for it, r in results:
        if not r["ok"]:
            jev_errors[it["stratum"]] += 1
            log(f"  jev error {it['stratum']} {it['item_id']}: {r.get('status')} {r.get('error', '')[:120]}")
            continue
        meta = json.loads(it["meta"]) if it["meta"] else {}
        try:
            p, label = jq.jev_p_wrong(it["stratum"], r["answers"], meta)
        except Exception as e:  # unexpected answer shape: log, count, move on
            jev_errors[it["stratum"]] += 1
            log(f"  jev answer unreadable {it['stratum']} {it['item_id']}: {e!r} {json.dumps(r['answers'])[:200]}")
            continue
        cut = jq.JEV_CUT[it["stratum"]]
        cost = r["input_tokens"] * jq.JEV_USD_PER_TOKEN
        spent += cost
        rows.append((RUN_DATE, it["stratum"], it["dimension"], it["item_id"], it["source_night"], it["meta"],
                     it["state"] if isinstance(it["state"], str) else json.dumps(it["state"], ensure_ascii=False),
                     jq.JEV_MODEL, json.dumps(r["answers"], ensure_ascii=False), float(p), bool(p >= cut), label,
                     float(cut), int(r["input_tokens"]), float(cost)))
    if rows:
        (spark.createDataFrame(rows, SAMPLE_SCHEMA).withColumn("judged_at", F.current_timestamp())
         .write.format("delta").mode("append").saveAsTable(SAMPLE))
    done += len(chunk)
    el = time.time() - t0
    rate = done / el if el else 0
    log(f"Jev {done:,}/{len(todo):,} ({chunk[0]['stratum']}): {rate:.1f}/s, ${spent:.3f}, "
        f"errors {sum(jev_errors.values())}, ETA {(len(todo) - done) / rate / 60 if rate else 0:.1f} min")
log(f"Jev done: {jev.n_ok:,} ok, {jev.n_fail:,} failed, {jev.total_tokens:,} tokens, ${spent:.3f}")
if todo and jev.n_fail > 0.2 * len(todo):
    raise RuntimeError(f"Jev failed on {jev.n_fail}/{len(todo)} items; check the typesafe key / API status before rerunning")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opus tail: highest-confidence flags, round-robin across strata, capped

# COMMAND ----------

flagged = [r.asDict() for r in spark.sql(f"""
    SELECT stratum, dimension, item_id, meta, state, jev_p_wrong, jev_flag
    FROM {SAMPLE}
    WHERE snapshot_date = DATE'{RUN_DATE}' AND jev_flag AND opus_verdict IS NULL""").collect()]
already = spark.sql(f"SELECT COUNT(*) n FROM {SAMPLE} WHERE snapshot_date = DATE'{RUN_DATE}' AND opus_verdict IS NOT NULL").collect()[0]["n"]
tail = jq.select_tail(flagged, max(0, TAIL_CAP - already), TAIL_STRATA)
log(f"Opus tail: {len(flagged):,} flagged rows eligible, {already} already judged, sending {len(tail)} "
    + "(" + ", ".join(f"{s}={sum(1 for t in tail if t['stratum'] == s)}" for s in TAIL_STRATA) + ")")

opus_spent = 0.0
if tail:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    opus = jq.OpusClient(ctx.apiUrl().get(), ctx.apiToken().get(), model=OPUS_MODEL, concurrency=6)
    for it in tail:
        it["state"] = json.loads(it["state"]) if it["stratum"] != "e_auth" else it["state"]
    t0 = time.time()
    results = opus.judge_many(tail, OPUS_SPEC)
    ver = []
    for it, r in results:
        opus_spent += r.get("cost", 0.0)
        if not r["ok"]:
            log(f"  opus error {it['stratum']} {it['item_id']}: {r.get('status')} {r.get('error', '')[:120]}")
            continue
        meta = json.loads(it["meta"]) if it["meta"] else {}
        try:
            wrong, label = jq.opus_wrong(it["stratum"], r["answer"], meta)
        except Exception as e:
            log(f"  opus answer unreadable {it['stratum']} {it['item_id']}: {e!r} {json.dumps(r['answer'])[:200]}")
            continue
        ver.append((it["stratum"], it["item_id"], r.get("model") or OPUS_MODEL, label, wrong,
                    (r["answer"].get("reason") or "")[:500], int(r["in"]), int(r["out"]), float(r["cost"])))
    log(f"Opus done: {opus.n_ok} ok, {opus.n_fail} failed in {time.time() - t0:.0f}s; "
        f"{opus.total_in:,} in / {opus.total_out:,} out tokens; ${opus_spent:.2f}")
    if ver:
        vs = StructType([
            StructField("stratum", StringType()), StructField("item_id", StringType()), StructField("opus_model", StringType()),
            StructField("opus_verdict", StringType()), StructField("opus_wrong", BooleanType()), StructField("opus_reason", StringType()),
            StructField("opus_tokens_in", IntegerType()), StructField("opus_tokens_out", IntegerType()), StructField("opus_cost_usd", DoubleType()),
        ])
        spark.createDataFrame(ver, vs).createOrReplaceTempView("opus_verdicts")
        spark.sql(f"""
            MERGE INTO {SAMPLE} s
            USING opus_verdicts v ON s.snapshot_date = DATE'{RUN_DATE}' AND s.stratum = v.stratum AND s.item_id = v.item_id
            WHEN MATCHED THEN UPDATE SET
              s.opus_model = v.opus_model, s.opus_verdict = v.opus_verdict, s.opus_wrong = v.opus_wrong,
              s.opus_reason = v.opus_reason, s.opus_tokens_in = v.opus_tokens_in, s.opus_tokens_out = v.opus_tokens_out,
              s.opus_cost_usd = v.opus_cost_usd, s.opus_judged_at = current_timestamp()""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily aggregate + tall metrics for MonitoringFindings

# COMMAND ----------

sample_rows = [r.asDict() for r in spark.sql(f"""
    SELECT stratum, dimension, jev_flag, opus_wrong, jev_cost_usd, opus_cost_usd
    FROM {SAMPLE} WHERE snapshot_date = DATE'{RUN_DATE}'""").collect()]
daily = jq.aggregate(sample_rows)

DAILY_SCHEMA = StructType([
    StructField("snapshot_date", DateType(), False), StructField("stratum", StringType(), False),
    StructField("dimension", StringType(), True), StructField("n", IntegerType()), StructField("n_flagged", IntegerType()),
    StructField("precision_jev", DoubleType()), StructField("n_opus", IntegerType()), StructField("n_opus_confirmed", IntegerType()),
    StructField("opus_flag_precision", DoubleType()), StructField("precision_opus_adj", DoubleType()),
    StructField("jev_cost_usd", DoubleType()), StructField("opus_cost_usd", DoubleType()),
])
spark.sql(f"DELETE FROM {DAILY} WHERE snapshot_date = DATE'{RUN_DATE}'")
(spark.createDataFrame([(RUN_DATE, d["stratum"], d["dimension"], d["n"], d["n_flagged"], d["precision_jev"], d["n_opus"],
                         d["n_opus_confirmed"], d["opus_flag_precision"], d["precision_opus_adj"], d["jev_cost_usd"], d["opus_cost_usd"])
                        for d in daily], DAILY_SCHEMA)
 .withColumn("computed_at", F.current_timestamp()).write.format("delta").mode("append").saveAsTable(DAILY))

m_rows = jq.metrics_rows(daily, jev_errors)
jev_total = sum(d["jev_cost_usd"] for d in daily if d["dimension"] is None)
opus_total = sum(d["opus_cost_usd"] for d in daily if d["dimension"] is None)
m_rows += [("jev_cost_cents", None, jev_total * 100), ("opus_cost_cents", None, opus_total * 100),
           ("sample_n", None, float(len(sample_rows)))]
MS = StructType([StructField("metric", StringType()), StructField("dimension", StringType()), StructField("value", DoubleType())])
spark.sql(f"DELETE FROM {METRICS} WHERE snapshot_date = DATE'{RUN_DATE}' AND component = '{COMPONENT}' AND source = '{SOURCE}'")
(spark.createDataFrame(m_rows, MS)
 .select(F.lit(RUN_DATE).cast("date").alias("snapshot_date"), F.lit(COMPONENT).alias("component"), "metric", "dimension", "value",
         F.lit(SOURCE).alias("source"), F.current_timestamp().alias("computed_at"))
 .write.format("delta").mode("append").saveAsTable(METRICS))
log(f"wrote {len(daily)} daily rows and {len(m_rows)} metric rows; Jev ${jev_total:.2f} + Opus ${opus_total:.2f} = ${jev_total + opus_total:.2f}")

# COMMAND ----------

display(spark.sql(f"""
SELECT stratum, n, n_flagged, ROUND(precision_jev, 3) precision_jev, n_opus, n_opus_confirmed,
       ROUND(opus_flag_precision, 2) opus_flag_precision, ROUND(precision_opus_adj, 3) precision_opus_adj,
       ROUND(jev_cost_usd, 3) jev_usd, ROUND(opus_cost_usd, 3) opus_usd
FROM {DAILY} WHERE snapshot_date = DATE'{RUN_DATE}' AND dimension IS NULL ORDER BY stratum"""))
