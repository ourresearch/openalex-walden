# Databricks notebook source
# MAGIC %md
# MAGIC # Match References: link tonight's new id-less references (oxjob #2296; matcher from #2000)
# MAGIC
# MAGIC The rule: **each night, every reference text the matcher has never seen is matched once, and every reference row that
# MAGIC prints it gets that answer.** "Seen" = the text has a vector in `openalex.vector_search.reference_embeddings_qwen3`
# MAGIC (every text #2000's corpus run decided, plus every text a night has decided since). A reference row of a list filed
# MAGIC tonight whose text was seen before takes that text's earlier link (`linked_by = 'same_text'`).
# MAGIC
# MAGIC In scope: rows of the lists `parse_referenced_works` filed tonight (`work_reference_lists_changes`, action `file`) that
# MAGIC print no DOI / PMID / arXiv id, are unlinked or linked to a dead id, and are not junk (#2188's classes in SKIP) -
# MAGIC the selection of #2000's corpus run. Stages (job "Match References", one task each; `stage` widget):
# MAGIC 1. `prepare` - tonight's rows and texts; new texts embedded (`ai_query`, the endpoint the search vectors use); same-text
# MAGIC    links; author + volume + first page + year lookup.
# MAGIC 2. `search` - Modal app `reference-matcher` (modal_reference_matcher.py): best 30 works per new text.
# MAGIC 3. `candidates` - candidate pairs with the works' fields, clue features and the checklist score (refmatch.py), reader
# MAGIC    prompts for the unsure band. First re-runs #2000's review bench through refmatch.py: any difference fails the stage.
# MAGIC 4. `reader` - Modal: the fine-tuned 2B reader scores each prompt's candidates.
# MAGIC 5. `decide` - one decision per text (link >= 0.97, or >= 0.9191 with first author + volume + page; type tie-break), links
# MAGIC    into `openalex.works.reference_matcher_links` under run `<mode>-<night>`, then the new texts' vectors are added
# MAGIC    (they are now seen). Live only: the links are applied to `work_references` (as `parse_referenced_works` applies a
# MAGIC    run) and the run is registered in `reference_matcher_runs`, so later nights keep them on re-filed lists.
# MAGIC
# MAGIC **Shadow** (Casey 16 Nov: 3 nights before turning it on): everything is written except step 5's two live writes, so
# MAGIC nothing is served; `parse_referenced_works` applies only runs listed in `reference_matcher_runs`, and shadow runs are
# MAGIC not. A failing stage logs the error and the later stages skip; End 2 End is not affected. Turning on = register the
# MAGIC shadow runs in `reference_matcher_runs` (applied in full by the next `parse_referenced_works`) and set mode `live`.
# MAGIC **Live** (Casey 16 Nov): any failure fails the task, and End 2 End with it.
# MAGIC Every night writes one row per stage to `openalex.works.reference_matcher_nights` (counts, seconds, Modal $ estimate).
# MAGIC **Test** (mode `test`, `test_texts` = a table of text_hash, text, raw_text): those texts run every stage as new texts, each
# MAGIC as one row of a citing work 0; all tables go to `openalex.transient.oxjob2296_test_*` (links included); nothing else is
# MAGIC written (no vectors added, nothing applied).

# COMMAND ----------

import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

sys.path.insert(0, os.getcwd())
import refmatch as RM  # noqa: E402
import cloudpickle  # noqa: E402

cloudpickle.register_pickle_by_value(RM)   # the workers get refmatch's code with each function, not an import

for name, default in [("stage", "prepare"), ("mode", "shadow"), ("night", ""), ("job_run_id", "manual"), ("catchup_from_version", ""),
                      ("max_texts", "8000000"), ("volume_dir", "/Volumes/openalex/works/data/reference_matcher"),
                      ("endpoint", "https://wordslikethis--reference-matcher-api.us-east.modal.run"), ("secret_scope", "modal"),
                      ("secret_key", "location_matcher_token"), ("warehouse_id", "3996dc0a9b183ce3"), ("max_usd", "60"),
                      ("max_minutes", "150"), ("shard_prompts", "20000"), ("scope_table", ""), ("test_texts", "")]:
    dbutils.widgets.text(name, default)
W = {k: dbutils.widgets.get(k).strip() for k in ("stage", "mode", "night", "job_run_id", "catchup_from_version", "max_texts", "volume_dir",
                                                  "endpoint", "secret_scope", "secret_key", "warehouse_id", "max_usd", "max_minutes",
                                                  "shard_prompts", "scope_table", "test_texts")}
STAGE, MODE = W["stage"], W["mode"]
assert MODE in ("shadow", "live", "test"), MODE
TEST = MODE == "test"
assert TEST == bool(W["test_texts"]), "mode test goes with test_texts, and only with it"
NIGHT = W["night"] or dt.datetime.utcnow().strftime("%Y-%m-%d")
assert re.match(r"^\d{4}-\d{2}-\d{2}$", NIGHT), NIGHT
RUN_ID = f"{MODE}-{NIGHT}"
JOB_RUN = W["job_run_id"]
VOL = W["volume_dir"].rstrip("/")
BUILD = f"rm{NIGHT.replace('-', '')}{MODE[0]}{re.sub('[^0-9A-Za-z]', '', JOB_RUN)}"[:40]
OUT = f"{VOL}/{BUILD}"
TRAIN = f"{VOL}/model"
P = "openalex.transient.oxjob2296_test_" if TEST else "openalex.works.reference_matcher_night_"
LINKS = f"{P}links" if TEST else "openalex.works.reference_matcher_links"
RUNS, NIGHTS = "openalex.works.reference_matcher_runs", "openalex.works.reference_matcher_nights"
REFEMB = "openalex.vector_search.reference_embeddings_qwen3"
WR = "openalex.works.work_references"
CHANGES = "openalex.works.work_reference_lists_changes"
ENDPOINT = "databricks-qwen3-embedding-0-6b"
SKIP = ("section_heading", "link_label_placeholder", "toc_dot_leaders", "boilerplate_bio_email", "no_letters", "datacite_url_only", "url_only")
NB = 2048
T0 = time.time()


def sql_part(path, key=None):
    """sql/ref_text_key.sql sections (-- @text, -- @instruction) or the whole file."""
    lines = open(os.path.join(os.getcwd(), path), encoding="utf-8").read().splitlines()
    if key is None:
        return "\n".join(lines)
    out, on = [], False
    for l in lines:
        s = l.strip()
        if s.startswith("-- @"):
            on = s[4:] == key
        elif on and s and not s.startswith("--"):
            out.append(s)
    return " ".join(out)


TEXT_SQL = sql_part("sql/ref_text_key.sql", "text")
INSTRUCTION_SQL = sql_part("sql/ref_text_key.sql", "instruction")
RULES = sql_part("sql/junk_rules.sql")
RULES = RULES[RULES.index("b AS ("):RULES.rindex("SELECT 1")]


def log(msg):
    print(f"{dt.datetime.utcnow():%H:%M:%S} UTC  {msg}", flush=True)


spark.sql(f"""CREATE TABLE IF NOT EXISTS {NIGHTS} (night DATE, run_id STRING, mode STRING, job_run_id STRING, stage STRING,
  status STRING, info STRING, seconds DOUBLE, at TIMESTAMP)
  COMMENT 'Reference matcher nights (oxjob #2296): one row per stage per run of the job Match References. status ok / failed / skipped;
info = JSON counts, Modal cost estimate, error text.'""")


def record(status, info):
    info = json.dumps(info, default=str)[:20000]
    spark.createDataFrame([(NIGHT, RUN_ID, MODE, JOB_RUN, STAGE, status, info, round(time.time() - T0, 1))],
                          "n string, run_id string, mode string, job_run_id string, stage string, status string, info string, seconds double") \
        .selectExpr("CAST(n AS DATE) night", "run_id", "mode", "job_run_id", "stage", "status", "info", "seconds", "current_timestamp() at") \
        .write.mode("append").saveAsTable(NIGHTS)


def earlier_failure():
    if JOB_RUN == "manual":
        return None
    r = spark.sql(f"SELECT stage FROM {NIGHTS} WHERE job_run_id = '{JOB_RUN}' AND status IN ('failed', 'skipped') LIMIT 1").collect()
    return r[0].stage if r else None


def count(t, where="true"):
    return spark.sql(f"SELECT count(*) n FROM {t} WHERE {where}").first().n


log(f"stage {STAGE}, mode {MODE}, night {NIGHT}, run {RUN_ID}, job run {JOB_RUN}, build {BUILD}")

# COMMAND ----------

# ---------- Modal ----------


def modal_call(stage, table_in, out_dir):
    token = dbutils.secrets.get(scope=W["secret_scope"], key=W["secret_key"])

    def post(path, body, tries=5):
        for t in range(tries):
            try:
                req = urllib.request.Request(f"{W['endpoint']}{path}", data=json.dumps(body).encode(), method="POST",
                                             headers={"authorization": f"Bearer {token}", "content-type": "application/json"})
                with urllib.request.urlopen(req, timeout=120) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError as e:
                if e.code < 500 or t == tries - 1:
                    raise RuntimeError(f"{path}: HTTP {e.code} {e.read()[:500]!r}")
            except (urllib.error.URLError, TimeoutError) as e:
                if t == tries - 1:
                    raise
                log(f"{path}: {e!r}; retrying")
            time.sleep(15 * (t + 1))

    dbutils.fs.mkdirs(out_dir)
    r = post("/start", {"stage": stage, "table_in": table_in, "out_dir": out_dir, "build_id": BUILD, "warehouse": W["warehouse_id"]})
    call_id = r["call_id"]
    log(f"Modal {stage} call {call_id} ({r.get('version')})")
    t0, last = time.time(), 0
    while True:
        s = post("/status", {"call_id": call_id})
        if s["state"] == "done":
            return s["result"]
        if s["state"] == "failed":
            raise RuntimeError(f"Modal {stage} call {call_id} failed: {s.get('error')}")
        if time.time() - t0 > float(W["max_minutes"]) * 60:
            raise TimeoutError(f"Modal {stage} call {call_id} not done after {W['max_minutes']} min")
        if time.time() - last > 300:
            log(f"Modal {stage} running ({(time.time() - t0) / 60:.0f} min)")
            last = time.time()
        time.sleep(30)

# COMMAND ----------

# ---------- stages ----------


def stage_prepare():
    info = {}
    if TEST:
        spark.sql(f"""CREATE OR REPLACE TABLE {P}rows AS SELECT CAST(0 AS BIGINT) citing_work_id, concat('test:', text_hash) native_id,
          0 ref_ind, 'test' provenance, 'test' pool, 'unlinked' status, CAST(NULL AS BIGINT) old_cited_work_id, raw_text, text, text_hash
          FROM {W['test_texts']}""")
        spark.sql(f"""CREATE OR REPLACE TABLE {P}texts AS SELECT text_hash, text, min(raw_text) raw_text, count(*) n_rows, false seen
          FROM {P}rows GROUP BY 1, 2""")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {LINKS} LIKE openalex.works.reference_matcher_links")
        return stage_prepare_new(info)
    cur = spark.sql(f"DESCRIBE HISTORY {CHANGES} LIMIT 1").first().version
    versions = list(range(int(W["catchup_from_version"]), cur + 1)) if W["catchup_from_version"] else [cur]
    filed = " UNION ".join(f"SELECT native_id FROM {CHANGES} VERSION AS OF {v} WHERE action = 'file'" for v in versions)
    # a test run can name its own scope (a table of native_id) instead of the change log
    if W["scope_table"]:
        filed = f"SELECT native_id FROM {W['scope_table']}"
    info["change_log_versions"] = [versions[0], versions[-1]] if not W["scope_table"] else W["scope_table"]
    spark.sql(f"""CREATE OR REPLACE TABLE {P}rows CLUSTER BY (text_hash) COMMENT 'Reference matcher (#2296): tonight''s reference rows in scope' AS
WITH filed AS (SELECT DISTINCT native_id FROM ({filed})),
src AS (
  SELECT wr.citing_work_id, wr.native_id, wr.ref_ind, wr.provenance, wr.raw, wr.title, wr.authors, wr.year, wr.cited_work_id,
    coalesce(nullif(trim(wr.raw), ''), concat_ws('. ', wr.authors, wr.year, wr.title)) AS text,
    wr.provenance = 'crossref' AND nullif(trim(wr.raw), '') IS NULL AS from_fields,
    {TEXT_SQL} AS key_text
  FROM {WR} wr LEFT SEMI JOIN filed f ON f.native_id = wr.native_id
  WHERE nullif(trim(wr.doi), '') IS NULL AND wr.parsed_doi IS NULL AND wr.pmid IS NULL AND wr.arxiv IS NULL),
{RULES}
, k AS (SELECT * FROM c WHERE cls NOT IN ({", ".join(f"'{s}'" for s in SKIP)}) AND length(key_text) > 0),
s AS (
  SELECT k.*, CASE WHEN k.cited_work_id IS NULL THEN 'unlinked' WHEN w.id IS NOT NULL THEN 'linked_live'
                   WHEN m.loser_work_id IS NOT NULL THEN 'linked_merged' ELSE 'linked_dead' END AS status
  FROM k LEFT JOIN (SELECT id FROM openalex.works.openalex_works) w ON w.id = k.cited_work_id
  LEFT JOIN (SELECT DISTINCT loser_work_id FROM openalex.works.merged_work_ids) m ON m.loser_work_id = k.cited_work_id)
SELECT citing_work_id, native_id, ref_ind, provenance, CASE WHEN from_fields THEN 'crossref_fields' ELSE provenance END AS pool, status,
  cited_work_id AS old_cited_work_id, text AS raw_text, key_text AS text, xxhash64(key_text) AS text_hash
FROM s WHERE status IN ('unlinked', 'linked_dead')""")
    spark.sql(f"""CREATE OR REPLACE TABLE {P}texts CLUSTER BY (text_hash) AS
SELECT t.*, e.text_hash IS NOT NULL AS seen FROM (SELECT text_hash, text, min(raw_text) raw_text, count(*) n_rows FROM {P}rows GROUP BY 1, 2) t
LEFT JOIN (SELECT text_hash, text FROM {REFEMB}) e ON e.text_hash = t.text_hash AND e.text = t.text""")
    c = spark.sql(f"SELECT count(*) texts, count_if(seen) seen, count_if(NOT seen) new, sum(n_rows) rows FROM {P}texts").first()
    info.update(rows=c.rows or 0, texts=c.texts, seen_texts=c.seen, new_texts=c.new)
    log(f"rows {info['rows']:,}, texts {c.texts:,}: seen {c.seen:,}, new {c.new:,}")
    if c.new > int(W["max_texts"]):
        raise RuntimeError(f"{c.new:,} new texts > max_texts {W['max_texts']}: raise max_texts on purpose to match this night")

    # same-text links: a seen text's latest link from any earlier run
    spark.sql(f"""CREATE OR REPLACE TABLE {P}reuse AS
WITH seen AS (SELECT text_hash FROM {P}texts WHERE seen),
prev AS (SELECT l.text_hash, max_by(l.cited_work_id, l.decided_at) cited, max_by(l.score, l.decided_at) score,
           max_by(l.in_band, l.decided_at) in_band
         FROM {LINKS} l LEFT SEMI JOIN seen s ON s.text_hash = l.text_hash WHERE l.run_id <> '{RUN_ID}' GROUP BY 1)
SELECT r.citing_work_id, r.native_id, r.ref_ind, r.text_hash, p.cited AS cited_work_id, p.score, p.in_band
FROM {P}rows r JOIN {P}texts t ON t.text_hash = r.text_hash AND t.text = r.text AND t.seen
JOIN prev p ON p.text_hash = r.text_hash WHERE p.cited <> r.citing_work_id""")
    info["same_text_links"] = count(f"{P}reuse")
    return stage_prepare_new(info)


def stage_prepare_new(info):
    """Embed the new texts of {P}texts and look them up by author + volume + page."""
    c = spark.sql(f"SELECT count_if(NOT seen) new FROM {P}texts").first()
    if TEST:
        spark.sql(f"CREATE OR REPLACE TABLE {P}reuse (citing_work_id BIGINT, native_id STRING, ref_ind INT, text_hash BIGINT, cited_work_id BIGINT, score DOUBLE, in_band BOOLEAN)")
    # embed the new texts (the instruction is prepended at embed time only), in chunks, 4 at a time
    from concurrent.futures import ThreadPoolExecutor
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("binary")
    def unit_fp16(s: pd.Series) -> pd.Series:
        def one(a):
            if a is None:
                return None
            v = np.asarray(a, dtype=np.float32)
            n = np.linalg.norm(v)
            return (v / n).astype(np.float16).tobytes() if n > 0 else None
        return s.map(one)
    spark.udf.register("refmatch_unit_fp16", unit_fp16)
    chunk = 250_000
    spark.sql(f"""CREATE OR REPLACE TABLE {P}new AS SELECT text_hash, text, raw_text,
      CAST((row_number() OVER (ORDER BY text_hash, text) - 1) / {chunk} AS INT) AS chunk FROM {P}texts WHERE NOT seen""")
    spark.sql(f"CREATE OR REPLACE TABLE {P}q (text_hash BIGINT, text STRING, raw_text STRING, v BINARY, error STRING) CLUSTER BY (text_hash)")
    n_chunks = (c.new + chunk - 1) // chunk

    def embed(k):
        for attempt in range(1, 4):
            try:
                spark.sql(f"""INSERT INTO {P}q SELECT text_hash, text, raw_text, refmatch_unit_fp16(r.result), r.errorMessage
                    FROM (SELECT text_hash, text, raw_text, ai_query('{ENDPOINT}', concat({INSTRUCTION_SQL}, text), failOnError => false) r
                          FROM {P}new WHERE chunk = {k})""")
                return None
            except Exception as e:  # noqa: BLE001
                spark.sql(f"DELETE FROM {P}q WHERE text_hash IN (SELECT text_hash FROM {P}new WHERE chunk = {k})")
                log(f"embed chunk {k} attempt {attempt}: {str(e)[:200]}")
                time.sleep(30 * attempt)
        return k

    with ThreadPoolExecutor(4) as ex:
        failed = [k for k in ex.map(embed, range(n_chunks)) if k is not None]
    if failed:
        raise RuntimeError(f"embedding chunks failed after 3 tries: {failed}")
    bad = count(f"{P}q", "v IS NULL")
    # a text the endpoint could not embed is left out tonight: it stays unseen, so it is matched when its list is filed again
    spark.sql(f"DELETE FROM {P}q WHERE v IS NULL")
    info.update(embedded=count(f"{P}q"), embed_failed=bad, embed_usd_est=round(c.new * 3.4 / 1e6, 2))

    # first author surname + volume + first page + year, against every work with a volume and a first page (#2000 19_rd_lookup)
    spark.sql(f"""CREATE OR REPLACE TABLE {P}lookup CLUSTER BY (text_hash) AS
WITH r AS (SELECT text_hash AS qn, text FROM {P}q),
nums AS (
  SELECT qn, pos, try_cast(n AS BIGINT) AS n
  FROM r LATERAL VIEW posexplode(regexp_extract_all(text, '(\\\\d+)', 1)) t AS pos, n
  WHERE length(n) <= 6),
pairs AS (
  SELECT DISTINCT a.qn, a.n AS vol, b.n AS fp
  FROM nums a JOIN nums b ON a.qn = b.qn AND b.pos > a.pos AND b.pos <= a.pos + 3),
words AS (
  SELECT DISTINCT qn, w FROM r LATERAL VIEW explode(regexp_extract_all(lower(text), '(\\\\p{{L}}{{2,}})', 1)) t AS w),
keys AS (
  SELECT p.qn, x.w AS sn, p.vol, p.fp FROM pairs p JOIN words x ON x.qn = p.qn
  WHERE x.w NOT IN ('of', 'and', 'the', 'in', 'on', 'for', 'an', 'journal', 'international', 'proceedings', 'university',
                    'press', 'review', 'research', 'society', 'et', 'al', 'pp', 'vol', 'no', 'ed', 'eds', 'la', 'le', 'des')),
wk AS (
  SELECT id, lower(element_at(split(trim(authorships[0].author.display_name), '\\\\s+'), -1)) AS sn,
    try_cast(regexp_extract(biblio.volume, '(\\\\d+)', 1) AS BIGINT) AS vol,
    try_cast(regexp_extract(biblio.first_page, '(\\\\d+)', 1) AS BIGINT) AS fp, publication_year AS yr
  FROM openalex.works.openalex_works
  WHERE biblio.volume IS NOT NULL AND biblio.first_page IS NOT NULL AND size(authorships) > 0),
hits AS (
  SELECT DISTINCT k.qn, wk.id AS cand
  FROM keys k JOIN wk ON wk.sn = k.sn AND wk.vol = k.vol AND wk.fp = k.fp
  JOIN nums y ON y.qn = k.qn AND y.n BETWEEN wk.yr - 1 AND wk.yr + 1)
SELECT qn AS text_hash, cand FROM (SELECT *, count(*) OVER (PARTITION BY qn) n FROM hits) WHERE n <= 21""")
    info["lookup_hits"] = count(f"{P}lookup")
    return info


def stage_search():
    n = count(f"{P}q")
    spark.sql(f"CREATE OR REPLACE TABLE {P}knn (text_hash BIGINT, cand BIGINT, score DOUBLE, rank INT, probe_rank INT)")
    if n == 0:
        return {"queries": 0}
    res = modal_call("search", f"{P}q", f"{OUT}/knn")
    if res["queries"] != n:
        raise RuntimeError(f"search covered {res['queries']:,} of {n:,} texts")
    if float(res.get("usd_est", 0)) > float(W["max_usd"]):
        raise RuntimeError(f"search cost estimate ${res['usd_est']} > max_usd {W['max_usd']}")
    (spark.read.parquet(f"{OUT}/knn").select("text_hash", "cand", F.col("score").cast("double"), F.col("rank").cast("int"),
                                             F.col("probe_rank").cast("int"))
     .write.mode("append").saveAsTable(f"{P}knn"))
    got = spark.sql(f"SELECT count(*) n, count(DISTINCT text_hash) t FROM {P}knn").first()
    if got.t != n:
        raise RuntimeError(f"search results for {got.t:,} of {n:,} texts")
    return {"queries": n, "knn_rows": got.n, **{k: v for k, v in res.items() if k not in ("version",)}}


def stage_candidates():
    t = time.time()
    chk, comb, fe = RM.fit_models(TRAIN)
    n, same = RM.check_bench(TRAIN, chk, comb, fe)
    if same != n:
        raise RuntimeError(f"review bench: {same} of {n} decisions as #2000's; refmatch.py or a library changed")
    info = {"bench": f"{same}/{n}", "models_s": round(time.time() - t, 1)}
    # candidates per text: its best 30 from the search + lookup hits (at most 20); merged-away works as their winner; one printed form
    spark.sql(f"""CREATE OR REPLACE TABLE {P}pairs AS
WITH r AS (SELECT text_hash AS qi, raw_text FROM {P}q),
k AS (SELECT text_hash AS qi, cand, score, rank, probe_rank FROM {P}knn),
l AS (SELECT text_hash AS qi, cand FROM (SELECT *, count(*) OVER (PARTITION BY text_hash) n FROM {P}lookup) WHERE n <= 20),
u AS (SELECT coalesce(k.qi, l.qi) qi, coalesce(k.cand, l.cand) cand, k.score, coalesce(k.rank, 101) rank,
        l.cand IS NOT NULL from_lookup, k.probe_rank
      FROM k FULL OUTER JOIN l ON k.qi = l.qi AND k.cand = l.cand),
m AS (SELECT loser_work_id, max(winner_work_id) winner_work_id FROM openalex.works.merged_work_ids GROUP BY 1)
SELECT u.qi, u.cand, u.score, u.rank, u.from_lookup, u.probe_rank, coalesce(m.winner_work_id, u.cand) cand_canon, w.title,
  w.publication_year year, w.type, CAST(w.doi IS NOT NULL AS STRING) has_doi, slice(transform(w.authorships, a -> a.author.display_name), 1, 5) au,
  size(w.authorships) n_au, w.primary_location.source.display_name venue, w.biblio.volume vol, w.biblio.issue iss,
  w.biblio.first_page fp, w.biblio.last_page lp, r.raw_text AS rtext,
  row_number() OVER (PARTITION BY u.qi ORDER BY u.rank, u.cand) AS ord
FROM u JOIN r ON r.qi = u.qi
LEFT JOIN m ON m.loser_work_id = u.cand
LEFT JOIN openalex.works.openalex_works w ON w.id = coalesce(m.winner_work_id, u.cand)""")
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("double")
    def cos(a: pd.Series, b: pd.Series) -> pd.Series:
        out = []
        for x, y in zip(a, b):
            if x is None or y is None:
                out.append(None)
                continue
            q = np.frombuffer(x, dtype=np.float16).astype(np.float32)
            w = np.frombuffer(y, dtype=np.float16).astype(np.float32)
            out.append(float(w @ (q / np.linalg.norm(q)) / np.linalg.norm(w)))
        return pd.Series(out)
    # a lookup-only candidate has no search score: its exact cosine with the same vectors
    (spark.table(f"{P}pairs").where("score IS NULL").select("qi", "cand")
     .join(spark.table(f"{P}q").select(F.col("text_hash").alias("qi"), F.col("v").alias("qv")), "qi")
     .join(spark.table("openalex.vector_search.work_reference_style_embeddings_qwen3").select(F.col("work_id").alias("cand"), F.col("v").alias("wv")), "cand")
     .select("qi", "cand", cos("qv", "wv").alias("cos"))
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{P}lookup_cos"))
    spark.sql(f"""MERGE INTO {P}pairs p USING {P}lookup_cos c ON p.qi = c.qi AND p.cand = c.cand AND p.score IS NULL
                  WHEN MATCHED THEN UPDATE SET p.score = c.cos""")
    spark.sql(f"UPDATE {P}pairs SET score = 0.0 WHERE score IS NULL")
    c = spark.sql(f"SELECT count(*) pairs, count(DISTINCT qi) texts, count_if(from_lookup) lookup, count_if(title IS NULL) no_title FROM {P}pairs").first()
    info.update(pairs=c.pairs, texts_with_pairs=c.texts, lookup_pairs=c.lookup, pairs_no_title=c.no_title)

    x_schema = "qi long, cand long, ord long, " + ", ".join(f"{f} double" for f in fe["feats0"]) + ", p double"

    def feat_fn(pdf):
        return RM.features_bucket(pdf, chk, fe)
    (spark.table(f"{P}pairs").select("qi", "cand", "score", "rank", "from_lookup", "cand_canon", "title", "year", "type", "au", "n_au",
                                     "venue", "vol", "iss", "fp", "lp", "rtext", "ord")
     .withColumn("b", F.pmod(F.col("qi"), F.lit(NB))).groupBy("b").applyInPandas(feat_fn, x_schema)
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{P}x"))
    info["x_rows"] = count(f"{P}x")

    def rows_fn(X, Pp):
        return RM.reader_rows_bucket(X, Pp, fe)
    Xb = spark.table(f"{P}x").withColumn("qi", F.col("qi").cast("long")).withColumn("b", F.pmod(F.col("qi"), F.lit(NB)))
    Pb = (spark.table(f"{P}pairs").select("qi", "cand_canon", "title", "au", "n_au", "year", "venue", "type", "vol", "iss", "fp", "lp",
                                         "rtext", "ord").withColumn("qi", F.col("qi").cast("long")).withColumn("b", F.pmod(F.col("qi"), F.lit(NB))))
    n_shard = max(1, int(np.ceil(c.texts * 0.3 / int(W["shard_prompts"]))))
    (Xb.groupBy("b").cogroup(Pb.groupBy("b")).applyInPandas(rows_fn, "qi long, seed_id string, prefix string, seats string")
     .withColumn("shard", F.pmod(F.col("qi"), F.lit(n_shard)))
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{P}reader_rows"))
    info["reader_prompts"] = count(f"{P}reader_rows")
    return info


def stage_reader():
    n = count(f"{P}reader_rows")
    spark.sql(f"CREATE OR REPLACE TABLE {P}reader_scores (qi BIGINT, cand BIGINT, p DOUBLE)")
    if n == 0:
        return {"prompts": 0}
    res = modal_call("reader", f"{P}reader_rows", f"{OUT}/reader")
    if res["errors"] or res["shards_done"] != res["shards"] or res["prompts"] != n:
        raise RuntimeError(f"reader incomplete: {json.dumps({k: v for k, v in res.items() if k != 'version'})[:1500]}")
    if float(res.get("usd_est", 0)) > float(W["max_usd"]):
        raise RuntimeError(f"reader cost estimate ${res['usd_est']} > max_usd {W['max_usd']}")
    (spark.read.parquet(f"{OUT}/reader").select(F.split("seed_id", ":").getItem(1).cast("long").alias("qi"), F.col("cid").alias("cand"), "p")
     .write.mode("append").saveAsTable(f"{P}reader_scores"))
    missing = spark.sql(f"SELECT count(*) n FROM {P}reader_rows r LEFT ANTI JOIN {P}reader_scores s ON s.qi = r.qi").first().n
    if missing:
        raise RuntimeError(f"{missing:,} prompts have no reader score")
    return {"prompts": n, **{k: v for k, v in res.items() if k not in ("version",)}}


def stage_decide():
    _, comb, fe = RM.fit_models(TRAIN)

    def decide_fn(X, Pp):
        return RM.decide_bucket(X, Pp, comb, fe)
    Xr = (spark.table(f"{P}x").withColumn("qi", F.col("qi").cast("long"))
          .join(spark.table(f"{P}reader_scores").select(F.col("qi").cast("long"), F.col("cand").cast("long"), F.col("p").alias("rp")),
                ["qi", "cand"], "left")
          .withColumn("b", F.pmod(F.col("qi"), F.lit(NB))))
    Pb = (spark.table(f"{P}pairs").select("qi", "cand_canon", "type", "title", "au", "year", "venue", "rtext", "ord")
          .withColumn("qi", F.col("qi").cast("long")).withColumn("b", F.pmod(F.col("qi"), F.lit(NB))))
    (Xr.groupBy("b").cogroup(Pb.groupBy("b"))
     .applyInPandas(decide_fn, "qi long, cand long, s double, in_band boolean, checklist_p double, reader_p double, link boolean, switched boolean")
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{P}decisions"))
    d = spark.sql(f"SELECT count(*) texts, count_if(link) linked, count_if(in_band) band, count_if(switched) switched FROM {P}decisions").first()
    info = {"decided_texts": d.texts, "linked_texts": d.linked, "band_texts": d.band, "switched": d.switched}

    # the night's links: rows of new texts get their text's decision (never the citing work itself); rows of seen texts their text's earlier link
    spark.sql(f"DELETE FROM {LINKS} WHERE run_id = '{RUN_ID}'")
    spark.sql(f"""INSERT INTO {LINKS}
SELECT r.citing_work_id, r.native_id, r.ref_ind, r.text_hash, d.cand, d.s, d.in_band,
  CASE WHEN d.s >= {RM.LINE_HI} THEN 'score' ELSE 'author_volume_page' END, '{RUN_ID}', current_timestamp()
FROM {P}rows r JOIN {P}q q ON q.text_hash = r.text_hash AND q.text = r.text JOIN {P}decisions d ON d.qi = r.text_hash
WHERE d.link AND d.cand <> r.citing_work_id
UNION ALL
SELECT citing_work_id, native_id, ref_ind, text_hash, cited_work_id, score, in_band, 'same_text', '{RUN_ID}', current_timestamp()
FROM {P}reuse""")
    c = spark.sql(f"""SELECT count(*) links, count_if(linked_by = 'same_text') same_text, count(DISTINCT citing_work_id) citing,
        count(DISTINCT cited_work_id) cited FROM {LINKS} WHERE run_id = '{RUN_ID}'""").first()
    info.update(links=c.links, same_text_links=c.same_text, citing_works=c.citing, cited_works=c.cited)

    if TEST:
        return info
    # the new texts are seen from now on (also in shadow: turning on registers the shadow runs, so their links are applied then)
    spark.sql(f"""INSERT INTO {REFEMB} (text_hash, text, v, embedded_at)
SELECT q.text_hash, q.text, q.v, current_timestamp() FROM {P}q q LEFT ANTI JOIN {REFEMB} e ON e.text_hash = q.text_hash AND e.text = q.text""")
    info["texts_marked_seen"] = count(f"{P}q")

    if MODE == "live":
        target_text = re.sub(r"\b(raw|authors|year|title)\b", r"target.\1", TEXT_SQL)
        m = spark.sql(f"""MERGE INTO {WR} AS target
USING (
  WITH redirect AS (SELECT id AS k, merge_into_id AS v FROM openalex.works.merged_work_ids_map),
  links AS (
    SELECT l.native_id, l.ref_ind, l.text_hash, coalesce(rd.v, l.cited_work_id) AS cited_work_id, l.decided_at
    FROM {LINKS} l LEFT JOIN redirect rd ON rd.k = l.cited_work_id WHERE l.run_id = '{RUN_ID}')
  SELECT native_id, ref_ind, text_hash, max_by(cited_work_id, decided_at) AS cited_work_id
  FROM links LEFT SEMI JOIN openalex.works.openalex_works_base b ON b.id = links.cited_work_id
  GROUP BY 1, 2, 3
) AS source
ON target.native_id = source.native_id AND target.ref_ind = source.ref_ind AND xxhash64({target_text}) = source.text_hash
WHEN MATCHED AND target.cited_work_id IS NULL AND source.cited_work_id <> target.citing_work_id THEN UPDATE SET
  target.cited_work_id = source.cited_work_id,
  target.updated_timestamp = current_timestamp()""").first()
        info["applied"] = m.asDict() if m else None
        spark.sql(f"DELETE FROM {RUNS} WHERE run_id = '{RUN_ID}'")
        spark.sql(f"INSERT INTO {RUNS} VALUES ('{RUN_ID}', current_timestamp())")
    return info

# COMMAND ----------

STAGES = {"prepare": stage_prepare, "search": stage_search, "candidates": stage_candidates, "reader": stage_reader, "decide": stage_decide}
before = earlier_failure()
if before:
    record("skipped", {"after_failed_stage": before})
    dbutils.notebook.exit(f"skipped: stage {before} of this run did not finish")
try:
    out = STAGES[STAGE]()
except Exception as e:
    record("failed", {"error": repr(e)[:3000]})
    if MODE == "live":
        raise
    dbutils.notebook.exit(f"shadow: stage {STAGE} failed (logged; End 2 End not affected): {e!r}"[:1000])
record("ok", out)
log(json.dumps(out, default=str)[:3000])
dbutils.notebook.exit(json.dumps(out, default=str)[:1000])
