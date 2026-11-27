"""PubMed markup abstract check (oxjob #1052). Read-only.

A PubMed AbstractText with an inline child element (<i>, <sub>, <sup>, MathML) used to be stored with only the text
after its last child. Since 27 Nov 2026 pubmed_parsed builds such an abstract from the record's raw markup
(notebooks/ingest/PubMed.py: the pubmed_abstract_markup table, and the history table for files before 24 Nov). It falls
back to the old, cut text when no markup row exists when the record is parsed. PubMed.py makes that impossible in a
normal update (the markup reader runs after pubmed_items), so a miss means that guarantee broke: a selective refresh, a
continuous-mode switch, or a runtime change.

This check finds pubmed_parsed rows parsed in the last LOOKBACK_DAYS whose version (PMID + DateRevised) has a markup row
now but whose abstract is not the markup build, i.e. rows that took the fallback. Fails (non-zero exit, so the job fails
and the failure reaches Sentry through the Databricks bridge) when there is any, and prints the repair: the cut rows are
recoverable at any time from the markup tables (oxjob #1052 work/backfill/06_gap_build.sql + 07_gap_merge.sql, which
rewrite pubmed_works for every version whose stored abstract is not its markup build).

Job: jobs/pubmed_markup_abstract_check.yaml. Locally (Databricks Connect):
    DATABRICKS_CONFIG_PROFILE=dbc-ce570f73-0362 python monitoring/checks/pubmed_markup_abstract_check.py
"""
import ast
import os
import sys

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "8"))
# Rows parsed before the forward path shipped took the old path by design (repaired by #1052's gap MERGE).
SHIPPED_AT = "2026-11-27 21:00:00"  # UTC
MARKUP = "openalex.pubmed.pubmed_abstract_markup"
HISTORY = "openalex.pubmed.pubmed_abstract_markup_history"
MAX_ABSTRACT_LENGTH = 10000  # PubMed.py's cap on the abstract before cleaning


def markup_abstract_sql():
    """PubMed.py's pubmed_markup_abstract_sql, read from the notebook so the check and the pipeline never drift."""
    here = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
    for base in (here, os.getcwd()):
        for up in ("", "..", os.path.join("..", "..")):
            path = os.path.join(base, up, "notebooks", "ingest", "PubMed.py")
            if os.path.exists(path):
                tree = ast.parse(open(path, encoding="utf-8").read())
                keep = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "pubmed_markup_abstract_sql"]
                ns = {}
                exec(compile(ast.Module(body=keep, type_ignores=[]), path, "exec"), ns)
                return ns["pubmed_markup_abstract_sql"]
    sys.exit("notebooks/ingest/PubMed.py not found next to this check")


try:
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
except Exception:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile(os.getenv("DATABRICKS_CONFIG_PROFILE", "dbc-ce570f73-0362")).serverless(True).getOrCreate()

build = markup_abstract_sql()("m.abstract_markup")
misses = spark.sql(f"""
WITH m AS (
  SELECT pmid, revised_date, abstract_markup FROM {MARKUP}
  UNION ALL
  SELECT h.pmid, h.revised_date, h.abstract_markup FROM {HISTORY} h
  LEFT ANTI JOIN {MARKUP} s ON s.pmid = h.pmid AND s.revised_date = h.revised_date
),
p AS (
  SELECT native_id, updated_date, abstract, ingested_at FROM openalex.pubmed.pubmed_parsed
  WHERE ingested_at >= greatest(current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS, TIMESTAMP'{SHIPPED_AT}')
)
SELECT p.native_id, p.updated_date, p.ingested_at
FROM p JOIN m ON m.pmid = p.native_id AND m.revised_date = p.updated_date
WHERE NOT (p.abstract <=> substring({build}, 0, {MAX_ABSTRACT_LENGTH}))
""").collect()

checked = spark.sql(f"""
SELECT count(*) FROM openalex.pubmed.pubmed_parsed
WHERE ingested_at >= greatest(current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS, TIMESTAMP'{SHIPPED_AT}')
""").collect()[0][0]

if misses:
    sample = ", ".join(f"{r.native_id} ({r.updated_date}, parsed {r.ingested_at})" for r in misses[:20])
    msg = (f"FAIL: {len(misses):,} PubMed records parsed in the last {LOOKBACK_DAYS} days took the cut struct text although "
           f"their markup exists (of {checked:,} parsed). First: {sample}. The markup reader did not run before "
           f"pubmed_parsed for them (selective refresh? continuous mode?). Repair: oxjob #1052 work/backfill/06_gap_build.sql "
           f"then 07_gap_merge.sql (new receipt table name), and pre-clear the re-stamps.")
    print(msg)
    sys.exit(msg)
print(f"OK: 0 of {checked:,} PubMed records parsed in the last {LOOKBACK_DAYS} days took the cut text while markup exists")
