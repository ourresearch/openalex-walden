# Databricks notebook source
# MAGIC %md
# MAGIC # Investigator authors: Judge_With_Jev (oxjob #1847)
# MAGIC
# MAGIC Between score and decide. `judge.sql` lists every entry no other layer serves that has a same-name list of 1-20 profiles, and builds
# MAGIC the night's items: entries never asked, or whose list changed, at most `jev_max_items`, never-asked first. Each is asked of Jev
# MAGIC through walden's `utils.study_design.JevClient` (paced by the shared broker, retrying, pinned jev-1.13.0) with the graded prompt
# MAGIC (`jev_link_prompt.txt`); no call starts after `jev_deadline_min` or once the account is out of credit. `judge_merge.sql` keeps
# MAGIC each batch's answers in `<prefix>jev` as it ends; routes.sql reads it on the next run. Items not asked tonight wait for the next night.

# COMMAND ----------

import os
import sys
import time

sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "..")))   # the repo root, for utils/
import iaa  # noqa: E402
from utils import study_design as sd  # noqa: E402

dbutils.widgets.text("sql", "")
p = iaa.params(dbutils)
PINS = iaa.pins(p)
items_sql, merge_sql = [x.strip() for x in dbutils.widgets.get("sql").split(",")]
iaa.run_sql_file(spark, items_sql, PINS)
# never-asked first (qn, 1..n), so a deadline that stops the night short leaves only re-asks for tomorrow. Each batch is its own short
# query (~19 MB of state at 2,000 items; all 60,000 are ~560 MB, too much to collect at once): no result stream stays open across the
# Jev calls (11-10: with one toLocalIterator held open all night, 58 min passed before a batch's save reached Spark).
items = spark.table(PINS["P"] + "jev_items")
n_items, last_qn = items.selectExpr("count(*)", "coalesce(max(qn), 0)").first()


def fetch(lo, hi):
    rows = (items.where(f"qn > {int(lo)} AND qn <= {int(hi)}").orderBy("qn")
            .select("entry_id", "list_hash", "stratum", "ckeys", "state_json").collect())
    return [r.asDict() for r in rows]


iaa.log(f"{n_items:,} entries to ask tonight (cap {PINS['JEVMAX']:,})")
client = sd.JevClient(dbutils.secrets.get(scope="typesafe", key="api_key"), concurrency=64, rps=150.0, timeout=30.0, max_attempts=4,
                      broker_token=sd.broker_token(dbutils), job="award_investigator_authors")


last_save = [time.monotonic()]


def keep(answers):
    """judge_merge.sql on one batch's answers, as the batch ends: a later failure or timeout loses no answer already paid for. The
    log line splits the batch's time into Jev (since the last save) and the save (11-10: 58 min were lost before a save reached Spark)."""
    t0 = time.monotonic()
    spark.createDataFrame(answers, "entry_id STRING, list_hash STRING, decision STRING, author_id BIGINT, p DOUBLE, model STRING"
                          ).createOrReplaceTempView("jev_out")
    t1 = time.monotonic()
    iaa.run_sql_file(spark, merge_sql, PINS)
    last_save[0], since = time.monotonic(), t0 - last_save[0]
    iaa.log(f"saved {len(answers):,} answers: Jev {since:.0f}s since the last save, dataframe {t1 - t0:.0f}s, merge {last_save[0] - t1:.0f}s")


try:
    answers, asked = iaa.judge_items(client, iaa.items_by_batch(fetch, iaa.JEV_BATCH, last_qn), iaa.jev_questions(),
                                     deadline_s=float(p["jev_deadline_min"]) * 60, batch=iaa.JEV_BATCH, on_answers=keep)
finally:
    credit = client.out_of_credit
    client.out_of_credit = credit or "stopped: this run asks no more"   # an abandoned call makes no further attempt (JevClient fails fast);
    # one already waiting in JevClient's pacer may still send its request: at most `concurrency` (64) more calls, a few cents
    if client.lease:
        client.lease.close()
iaa.log(f"asked {asked:,} of {n_items:,}; {len(answers):,} answered; Jev ${client.usd:.2f}, {client.n_retry} retries, {client.n_fail} failed"
        + (f"; OUT OF CREDIT: {credit}" if credit else ""))
if not answers:   # no answer tonight: the seeded picks still take their lists
    keep([])
