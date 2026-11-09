# Databricks notebook source
# MAGIC %md
# MAGIC # Investigator authors: prepare (oxjob #1847)
# MAGIC
# MAGIC Loads `excluded_funders.csv` (validated) as the temp view `excluded_funders`, then runs `sql/names.sql` (the name functions and every
# MAGIC live profile's name keys) and `sql/people.sql` (every person on a grant, profile ORCIDs, paper authors). Fails when awards_api gave
# MAGIC under 4M people or under 90% of live profiles got a name key: the inputs look broken.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.getcwd())
import iaa  # noqa: E402

p = iaa.params(dbutils)
P, PINS = p["prefix"], iaa.pins(p)
iaa.log(f"run {p['run_date']}, tables {P}*")
spark.createDataFrame(iaa.read_excluded(), "funder_id BIGINT, country_code STRING, reason STRING, decided STRING").createOrReplaceTempView(
    "excluded_funders")
for f in ("names.sql", "people.sql"):
    iaa.run_sql_file(spark, f, PINS)
n = spark.sql(f"""SELECT (SELECT count(*) FROM {P}people) AS people,
  (SELECT count(DISTINCT author_id) FROM {P}profile_names) AS named,
  (SELECT count(*) FROM openalex.authors.authors_api WHERE works_count > 0) AS live""").first()
iaa.log(f"{n.people:,} people; {n.named:,} of {n.live:,} live profiles with a name key")
if n.people < 4_000_000 or n.named < 0.9 * n.live:
    raise RuntimeError(f"inputs look broken: {n.people:,} people, {n.named:,} of {n.live:,} profiles named")
