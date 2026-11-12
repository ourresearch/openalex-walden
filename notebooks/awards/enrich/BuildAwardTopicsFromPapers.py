# Databricks notebook source
# MAGIC %md
# MAGIC # Awards enrich: topics for awards with no text, from their papers, into `award_topics_v2` (oxjob #2125 part 2)
# MAGIC
# MAGIC Rule (`lib/papers_topics.py`): an award in `awards_table` with no text-model row in `target` gets the average of its papers'
# MAGIC topics (each paper's 3 work topics scaled to sum to 1) when it has >= 2 papers with topics and the top topic holds >= half of it;
# MAGIC a 1-paper award gets its paper's topics only when 2 of 3 signals agree (the funder's papers, a same-funder text grant on the
# MAGIC paper, the funder's text grants) and no text grant on the paper is in its subfield. Otherwise no row. Rows carry
# MAGIC `source = 'papers_v1'`, `score` = the topic's share of the papers, `text_key` NULL, so Nightly Grants serves them as it serves
# MAGIC text rows and a text row always wins: the topics task's MERGE overwrites a papers row, and this one never touches another source.
# MAGIC
# MAGIC Every run is a full pass (2026-11-10 at full size: 56.3M links, 18.5M papers, 1,802,577 awards served, ~3 min of serverless):
# MAGIC it stages `<state_prefix>papers_links` and `<state_prefix>papers_paper_topics` (the rule reads each several times), builds the
# MAGIC served rows in `<state_prefix>papers_served`, then ONE MERGE inserts new rows, rewrites only rows whose topics changed and deletes
# MAGIC papers rows no longer served. Guards, checked before anything is written to `target`: one row per award, none over a text row,
# MAGIC 1-3 resolved topics, and a delete share (`max_delete_share` of the papers rows) that catches a broken or half-written input.
# MAGIC `dry_run = true`: stages and counts under `state_prefix`; nothing is written to `target`.

# COMMAND ----------

import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "lib"))
import papers_topics as PT  # noqa: E402

for name, default in [("state_prefix", "openalex_dev.rohan_lab.awt_"), ("awards_table", "openalex.awards.awards_api"),
                      ("target", "openalex_dev.rohan_lab.awt_award_topics_v2"), ("work_awards_table", "openalex.awards.work_awards"),
                      ("work_topics_table", "openalex.works.work_topics_v2"), ("max_delete_share", "0.10"), ("dry_run", "true")]:
    dbutils.widgets.text(name, default)
W = {k: dbutils.widgets.get(k).strip() for k in ("state_prefix", "awards_table", "target", "work_awards_table", "work_topics_table")}
LINKS, PAPERS, SERVED = (W["state_prefix"] + x for x in ("papers_links", "papers_paper_topics", "papers_served"))
MAX_DEL = float(dbutils.widgets.get("max_delete_share"))
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
for t in [LINKS, PAPERS, SERVED, W["awards_table"], W["target"], W["work_awards_table"], W["work_topics_table"]]:
    assert t.replace(".", "").replace("_", "").isalnum() and t.count(".") == 2, f"bad table name {t!r}"
# the staged tables are replaced every run: never one of the inputs or the target
assert not {LINKS, PAPERS, SERVED} & {W["awards_table"], W["target"], W["work_awards_table"], W["work_topics_table"]}, "state_prefix names an input"


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def one(sql):
    return spark.sql(sql).collect()[0]

# COMMAND ----------

if not spark.catalog.tableExists(W["target"]):
    raise RuntimeError(f"{W['target']} does not exist: the topics task creates it")
spark.sql(f"""CREATE OR REPLACE TABLE {LINKS}
COMMENT 'Awards enrich: award-paper links with the grants each paper thanks, rebuilt every run by BuildAwardTopicsFromPapers'
AS {PT.links_sql(W['work_awards_table'])}""")
spark.sql(f"""CREATE OR REPLACE TABLE {PAPERS}
COMMENT 'Awards enrich: linked papers topics scaled to sum to 1, rebuilt every run by BuildAwardTopicsFromPapers'
AS {PT.paper_topics_sql(W['work_topics_table'], LINKS)}""")
spark.sql(f"""CREATE OR REPLACE TABLE {SERVED}
COMMENT 'Awards enrich: awards whose topics come from their papers (papers_v1), rebuilt every run by BuildAwardTopicsFromPapers'
AS {PT.served_sql(W['awards_table'], W['target'], LINKS, PAPERS)}""")
# the guards and the MERGE read the same version of the served rows, even if another run replaces the table meanwhile
SERVED_AT = f"{SERVED} VERSION AS OF {int(one(f'DESCRIBE HISTORY {SERVED} LIMIT 1').version)}"
g = one(PT.plan_sql(W["target"], SERVED_AT))
log(f"plan: {json.dumps(g.asDict())}")
bad = []
if g.served != g.served_ids: bad.append(f"{g.served:,} rows for {g.served_ids:,} awards")
if g.over_text_rows: bad.append(f"{g.over_text_rows:,} rows would land on awards with a text row")
if g.bad_rows: bad.append(f"{g.bad_rows:,} rows with 0 or > 3 topics, an unresolved topic, or outside the rule's papers / share bars")
if g.papers_rows and g.would_delete > MAX_DEL * g.papers_rows:
    bad.append(f"would delete {g.would_delete:,} of {g.papers_rows:,} papers rows (> max_delete_share {MAX_DEL}): an input looks broken")
if DRY:
    dbutils.notebook.exit(json.dumps({"dry_run": True, "refusals": bad, **g.asDict()}))
if bad:
    raise RuntimeError("refusing to merge, NOTHING written to the target: " + "; ".join(bad))

spark.sql(PT.merge_sql(W["target"], SERVED_AT))
c = one(f"""SELECT count_if(v.source <=> '{PT.SOURCE}') AS papers_rows,
  count_if(v.source <=> '{PT.SOURCE}' AND r.award_id IS NULL) AS stray,
  count_if(r.award_id IS NOT NULL AND NOT (v.source <=> '{PT.SOURCE}')) AS missing
  FROM {W['target']} v FULL OUTER JOIN {SERVED_AT} r ON r.award_id = v.award_id""")
if c.papers_rows != g.served or c.stray or c.missing:
    raise RuntimeError(f"target check failed after merge: {c.asDict()} (served {g.served:,})")
log(f"{g.served:,} awards served from their papers in {W['target']} ({g.multi:,} by 2+ papers, {g.one_paper:,} by 1 confirmed paper): "
    f"+{g.would_insert:,} new, {g.would_update:,} changed, -{g.would_delete:,} no longer served")
dbutils.jobs.taskValues.set(key="papers_served", value=int(g.served))
