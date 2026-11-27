# Databricks notebook source
# MAGIC %md
# MAGIC # OpenAPC (oxjob #1231)
# MAGIC
# MAGIC Loads OpenAPC's article fees into `openalex.mid.work_openapc`, one row per work. `CreateWorksBase` serves the amount as
# MAGIC the work's `apc_paid` (in EUR, with `value_usd`) whenever OpenAPC has the article, ahead of the list-price estimate.
# MAGIC
# MAGIC Until 26 Nov 2026 the table was a one-off snapshot of 18 Sep 2025 that nothing refreshed. OpenAPC changes several times
# MAGIC a week and moves records between its two article files, so this reads both:
# MAGIC - `data/apc_de.csv`: fees institutions paid for single articles;
# MAGIC - `data/transformative_agreements/transformative_agreements.csv`: articles under transformative agreements; only the rows
# MAGIC   with a euro amount are used (most have none, because the agreement's cost is not split per article).
# MAGIC
# MAGIC A DOI appears once across the two files. Euro -> USD uses the ECB's annual average for the article's period (the year's
# MAGIC monthly average for the current year). The standing ruling "Upstream is the source of truth" (#1164) applies: today's
# MAGIC OpenAPC amount replaces the old one, and a record OpenAPC drops stops being served.
# MAGIC
# MAGIC A failed run or a failed guard leaves the table as it was. Revert a bad load with `RESTORE TABLE openalex.mid.work_openapc
# MAGIC TO VERSION AS OF <n>` (DESCRIBE HISTORY shows the versions).

# COMMAND ----------

dbutils.widgets.text("table", "openalex.mid.work_openapc")
dbutils.widgets.text("works_table", "openalex.works.openalex_works")
dbutils.widgets.text("base", "https://raw.githubusercontent.com/OpenAPC/openapc-de/master/data/")
TABLE = dbutils.widgets.get("table")
WORKS = dbutils.widgets.get("works_table")
BASE = dbutils.widgets.get("base")
FILES = {"apc_de": BASE + "apc_de.csv",
         "transformative_agreements": BASE + "transformative_agreements/transformative_agreements.csv"}
ECB = "https://data-api.ecb.europa.eu/service/data/EXR/{freq}.USD.EUR.SP00.A?format=csvdata&startPeriod=1999"
UA = {"User-Agent": "OpenAlex walden (mailto:team@ourresearch.org)"}

# COMMAND ----------

import csv, io, re, statistics, urllib.request
from pyspark.sql import functions as F

DOI = re.compile(r"^10\.\d{4,9}/\S+$")


def fetch(url):
    return urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=600).read().decode("utf-8")


def norm_doi(value):
    d = (value or "").strip().lower()
    d = re.sub(r"^(https?://(dx\.)?doi\.org/|doi:)", "", d)
    return d if DOI.match(d) else None


def parse(name, text):
    reader = csv.DictReader(io.StringIO(text))
    missing = {"period", "euro", "doi"} - set(reader.fieldnames or [])
    assert not missing, f"OpenAPC's {name} file has no {sorted(missing)} column: table left as it was"
    out = []
    for r in reader:
        doi = norm_doi(r["doi"])
        try:
            euro = float(r["euro"])
        except ValueError:      # 'NA': transformative-agreement articles without a per-article cost
            continue
        try:
            year = int(r["period"])
        except ValueError:
            year = None
        if doi and euro > 0:
            out.append((doi, year, euro, name))
    return out


rows = []
for name, url in FILES.items():
    part = parse(name, fetch(url))
    print(f"{name}: {len(part):,} articles with a DOI and a euro amount")
    rows += part
by_doi = {}
for doi, year, euro, name in rows:      # a DOI is in one file once; keep the apc_de row if a record were ever in both
    if doi not in by_doi or name == "apc_de":
        by_doi[doi] = (doi, year, euro, name)
rows = list(by_doi.values())
print(f"{len(rows):,} distinct DOIs")

# COMMAND ----------

# ECB EUR -> USD: annual averages; the current year (no annual value yet) gets the average of its months so far.
def ecb(freq):
    rates = {}
    for r in csv.DictReader(io.StringIO(fetch(ECB.format(freq=freq)))):
        rates[r["TIME_PERIOD"]] = float(r["OBS_VALUE"])
    return rates


annual = {int(k): v for k, v in ecb("A").items()}
monthly = ecb("M")
for y in {y for _, y, _, _ in rows if y and y not in annual}:
    months = [v for k, v in monthly.items() if k.startswith(f"{y}-")]
    if months:
        annual[y] = statistics.mean(months)
assert annual and max(annual) >= 2025, "ECB rates missing or stale: table left as it was"


def usd_rate(year):
    if year in annual:
        return annual[year]
    nearest = min(annual, key=lambda y: abs(y - (year or max(annual))))
    return annual[nearest]


print("EUR->USD used:", {y: round(annual[y], 4) for y in sorted(annual) if y >= 2019})
rows = [(doi, year, euro, round(euro * usd_rate(year), 2), name) for doi, year, euro, name in rows]

# COMMAND ----------

new = spark.createDataFrame(rows, "doi STRING, year INT, apc_in_euro DOUBLE, apc_in_usd DOUBLE, openapc_file STRING")
works = (spark.table(WORKS)
         .where(F.col("doi").isNotNull())
         .select(F.regexp_extract(F.col("id").cast("string"), r"(\d+)$", 1).cast("bigint").alias("paper_id"),
                 F.lower(F.regexp_replace("doi", r"^https?://(dx\.)?doi\.org/", "")).alias("doi")))
matched = works.join(F.broadcast(new), "doi")   # ~300K OpenAPC rows against every work with a DOI

old_exists = spark.catalog.tableExists(TABLE)
if old_exists:
    old = spark.table(TABLE)
    paper_id_type = dict(old.dtypes)["paper_id"]
    matched = matched.withColumn("paper_id", F.col("paper_id").cast(paper_id_type))
    old_rows = old.count()
else:
    old_rows = 0
matched = matched.select("paper_id", "doi", "year", "apc_in_euro", "apc_in_usd", "openapc_file").cache()
n_new = matched.count()
n_works = matched.select("paper_id").distinct().count()
print(f"{n_new:,} OpenAPC articles matched to {n_works:,} works ({len(rows) - n_new:,} DOIs not in {WORKS}); "
      f"{old_rows:,} rows in {TABLE} before this load")

# Guards: nothing is written unless every one passes.
assert n_new >= 0.95 * old_rows, f"only {n_new:,} matched rows vs {old_rows:,} loaded before: table left as it was"
if old_exists:
    same = (old.select(F.lower(F.regexp_replace("doi", r"^https?://(dx\.)?doi\.org/", "")).alias("doi"),
                       F.col("paper_id").alias("old_paper_id"))
            .join(matched.select("doi", "paper_id"), "doi"))
    n_same = same.count()
    agree = same.where("old_paper_id = paper_id").count()
    print(f"work id agrees with the previous load for {agree:,} of {n_same:,} DOIs in both")
    assert n_same == 0 or agree >= 0.97 * n_same, \
        "the DOI -> work id match disagrees with the previous load on over 3% of DOIs: table left as it was"

# COMMAND ----------

(matched.withColumn("loaded_at", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE))

display(spark.sql(f"""
  SELECT openapc_file, COUNT(*) AS articles, COUNT(DISTINCT paper_id) AS works, MIN(year) AS first_year,
         MAX(year) AS last_year, ROUND(MEDIAN(apc_in_euro)) AS median_euro
  FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC"""))
