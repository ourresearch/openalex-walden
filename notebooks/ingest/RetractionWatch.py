# Databricks notebook source
# MAGIC %md
# MAGIC # Retraction Watch (oxjob #737)
# MAGIC
# MAGIC Loads the Retraction Watch database, which Crossref publishes in full every day, into
# MAGIC `openalex.works.retraction_watch`, one row per Retraction Watch record. `CreateWorksBase` flags a work retracted when
# MAGIC Retraction Watch lists one of its DOIs as the original paper of a retraction and never as reinstated.
# MAGIC
# MAGIC Crossref's own records carry a Retraction Watch retraction only when Retraction Watch names the notice's DOI (Crossref,
# MAGIC community thread 16586, 2026-10-15), so ~1.3K retractions never reach a Crossref record; we read the file itself.
# MAGIC
# MAGIC A failed run leaves the table as it was, and the nightly reads the previous day's file.

# COMMAND ----------

dbutils.widgets.text("table", "openalex.works.retraction_watch")
dbutils.widgets.text("url", "https://gitlab.com/crossref/retraction-watch-data/-/raw/main/retraction_watch.csv")
TABLE = dbutils.widgets.get("table")
URL = dbutils.widgets.get("url")

# COMMAND ----------

import csv, datetime, io, re, urllib.parse, urllib.request
from pyspark.sql import functions as F

DOI = re.compile(r"^10\.\d{4,9}/\S+$")


def norm_doi(value):
    # a few are URL-encoded ('...74:3%3C201::AID-JSO8%3E3.0.CO;2-5'); 'Unavailable', 'unavailable' and blank -> NULL
    d = urllib.parse.unquote((value or "").strip()).lower()
    d = re.sub(r"^(https?://(dx\.)?doi\.org/|doi:)", "", d)
    return d if DOI.match(d) else None


def parse_date(value):
    # '7/15/2026 0:00'
    try:
        return datetime.datetime.strptime(value.strip().split(" ")[0], "%m/%d/%Y").date()
    except ValueError:
        return None


req = urllib.request.Request(URL, headers={"User-Agent": "OpenAlex walden (mailto:team@ourresearch.org)"})
reader = csv.DictReader(io.StringIO(urllib.request.urlopen(req, timeout=300).read().decode("utf-8")))
missing = {"Record ID", "Title", "Publisher", "ArticleType", "RetractionDate", "RetractionDOI", "OriginalPaperDOI",
           "RetractionNature"} - set(reader.fieldnames)
assert not missing, f"the Retraction Watch file has no {sorted(missing)} column: table left as it was"
rows = [
    (int(r["Record ID"]), norm_doi(r["OriginalPaperDOI"]), norm_doi(r["RetractionDOI"]), r["RetractionNature"].strip(),
     parse_date(r["RetractionDate"]), r["ArticleType"].strip(), r["Title"].strip(), r["Publisher"].strip())
    for r in reader
]

current = spark.table(TABLE).count() if spark.catalog.tableExists(TABLE) else 0
print(f"{len(rows):,} records today, {current:,} in {TABLE}")
assert len(rows) >= 0.95 * current, "today's file is over 5% shorter than the last one loaded: table left as it was"

# COMMAND ----------

(spark.createDataFrame(rows, "record_id INT, original_paper_doi STRING, retraction_doi STRING, retraction_nature STRING, "
                             "retraction_date DATE, article_type STRING, title STRING, publisher STRING")
 .withColumn("loaded_at", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE))

display(spark.sql(f"""
  SELECT retraction_nature, COUNT(*) AS records, COUNT(original_paper_doi) AS with_doi,
         COUNT_IF(original_paper_doi IS NOT NULL AND retraction_doi IS NULL) AS with_doi_no_notice_doi
  FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC"""))
