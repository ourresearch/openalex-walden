# Databricks notebook source
# MAGIC %md
# MAGIC # Re-derive stored full text with a space between TEI units (oxjob #2655)
# MAGIC
# MAGIC One-off backfill for `notebooks/ingest/PDF.py` `tei_text()`. Until #2655 the full text was
# MAGIC `''.join(<text>.itertext())`, so GROBID's adjacent sentences, paragraphs and headings were glued into one search
# MAGIC token ('pupil.Lower', 'INTRODUCTIONOver') and a phrase starting a sentence could not be found.
# MAGIC
# MAGIC Input `candidates_table` (built by the job's SQL): per `pdf_fulltext_keys` key, every pdf_combined row whose raw
# MAGIC text has the stored raw length, with its GROBID file id and `stored_md5` = MD5 of the stored (cleaned) text.
# MAGIC For each candidate's TEI (`openalex.pdf.grobid_processing_results`, and with `legacy_tei_table` set also the
# MAGIC Feb-2025 backfill files in `openalex.pdf.grobid_xml_backfill`) this notebook runs PDF.py's own functions,
# MAGIC loaded from the file in this checkout so the code is the parser's, byte for byte:
# MAGIC - `old_md5`: MD5 of the OLD derivation after CreateWorkFulltext's cleaning. It equals `stored_md5` only when this
# MAGIC   TEI is the stored text's source; only those keys get a new text (`new_raw` is NULL otherwise).
# MAGIC - `old_md5_ascii`: the same with every non-ASCII character dropped before the whitespace collapse, which is how the
# MAGIC   2025 legacy backfill (`pdf_works_grobid_backfill.fulltext`) made its texts ('Maringá' -> 'Maring', 'µg' -> 'g').
# MAGIC   `how` says which of the two reproduced the stored text.
# MAGIC - `new_raw`: the NEW derivation (what the fixed parser writes to pdf_combined.fulltext), non-ASCII characters
# MAGIC   kept, as the parser keeps them. CreateWorkFulltext's cleaning is applied afterwards in SQL.
# MAGIC
# MAGIC Writes `output_table` only (a scratch table); production tables are written by the job's SQL after checks.

# COMMAND ----------

dbutils.widgets.text("candidates_table", "openalex.transient.oxjob2655_winners")
dbutils.widgets.text("output_table", "openalex.transient.oxjob2655_rederived")
dbutils.widgets.text("tei_table", "openalex.pdf.grobid_processing_results")  # a small copy (id, status, xml_content) for tests
dbutils.widgets.text("legacy_tei_table", "")  # wave 2: openalex.pdf.grobid_xml_backfill (id, xml_content)
dbutils.widgets.text("broadcast_candidates", "false")  # true when the candidates fit in memory: no TEI shuffle
CANDIDATES = dbutils.widgets.get("candidates_table")
TEI_TABLE = dbutils.widgets.get("tei_table")
LEGACY_TEI_TABLE = dbutils.widgets.get("legacy_tei_table").strip()
BROADCAST = dbutils.widgets.get("broadcast_candidates").lower() == "true"
OUTPUT = dbutils.widgets.get("output_table")
assert OUTPUT.startswith("openalex.transient."), "scratch output only"

# COMMAND ----------

import ast
import os

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, StructField, StructType

# PDF.py's own TEI_INLINE, tei_text, clean_xml and MAX_FULLTEXT_LENGTH, as source text for the executors
PDF_PY = os.path.join(os.getcwd(), "..", "ingest", "PDF.py")
_src = open(PDF_PY).read()
_want = {"TEI_INLINE", "tei_text", "clean_xml", "MAX_FULLTEXT_LENGTH"}
_nodes = [n for n in ast.parse(_src).body
          if (isinstance(n, ast.FunctionDef) and n.name in _want)
          or (isinstance(n, ast.Assign) and {getattr(t, "id", None) for t in n.targets} & _want)]
PARSER_CODE = "\n\n".join(ast.get_source_segment(_src, n) for n in _nodes)
assert all(name in PARSER_CODE for name in _want), "PDF.py lost a function this backfill needs"

SCHEMA = StructType([StructField("old_md5", StringType()), StructField("old_md5_ascii", StringType()),
                     StructField("new_raw", StringType())])


@pandas_udf(SCHEMA)
def rederive(xml: pd.Series) -> pd.DataFrame:
    import hashlib
    import re
    import xml.etree.ElementTree as ET

    ns = {"re": re, "ET": ET}
    exec(PARSER_CODE, ns)
    tei_text, clean_xml, cap = ns["tei_text"], ns["clean_xml"], ns["MAX_FULLTEXT_LENGTH"]
    non_ascii = re.compile(r"[^\x00-\x7f]")

    def finish(text, old):
        # PDF.py _fulltext() after the join, old (until #2655) and new
        if not text:
            return None
        text = re.sub(r"<[^>]+>", "", text)
        text = " ".join(text.split())
        if old:
            text = "\n".join([line.strip() for line in text.splitlines() if line.strip()])
        return text[:cap]

    def stored(raw):
        # CreateWorkFulltext cell 5: SUBSTRING 200000, '<[^>]+>' -> ' ', '\\s+' -> ' ' (Java: ASCII whitespace), trim
        s = re.sub(r"<[^>]+>", " ", raw[:200000])
        s = re.sub(r"[ \t\n\x0b\f\r]+", " ", s)
        s = re.sub(r"(^[ \t\n\x0b\f\r]+|[ \t\n\x0b\f\r]+$)", "", s)
        return s.strip(" ")

    def md5_of(text):
        return hashlib.md5(stored(text).encode("utf-8")).hexdigest() if text else None

    olds, olds_ascii, news = [], [], []
    for x in xml:
        o = oa = n = None
        try:
            c = clean_xml(x)
            t = ET.fromstring(c).find(".//{http://www.tei-c.org/ns/1.0}text") if c else None
            if t is not None:
                joined = "".join(t.itertext()).strip()
                o = md5_of(finish(joined, True))
                oa = md5_of(finish(non_ascii.sub("", joined), True))
                if o or oa:
                    n = finish(tei_text(t), False)
        except Exception:
            pass
        olds.append(o)
        olds_ascii.append(oa)
        news.append(n)
    return pd.DataFrame({"old_md5": olds, "old_md5_ascii": olds_ascii, "new_raw": news})

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "200")  # TEIs run to megabytes
spark.conf.set("spark.sql.shuffle.partitions", "6000")

cands = spark.table(CANDIDATES).select("key_type", "key", "raw_length", "stored_md5", "grobid_id")
tei = (spark.table(TEI_TABLE)
       .where("status LIKE 'success%' AND xml_content IS NOT NULL")
       .select(F.col("id").alias("grobid_id"), "xml_content", F.lit("delta").alias("tei_src")))
if LEGACY_TEI_TABLE:
    tei = tei.unionByName(
        spark.table(LEGACY_TEI_TABLE).where("xml_content IS NOT NULL")
        .select(F.col("id").alias("grobid_id"), "xml_content", F.lit("legacy").alias("tei_src")))

out = (tei.join(F.broadcast(cands) if BROADCAST else cands, "grobid_id")
       .withColumn("d", rederive("xml_content"))
       .select("key_type", "key", "raw_length", "stored_md5", "grobid_id", "tei_src",
               F.col("d.old_md5").alias("old_md5"), F.col("d.old_md5_ascii").alias("old_md5_ascii"),
               F.when(F.col("d.old_md5") == F.col("stored_md5"), F.lit("plain"))
                .when(F.col("d.old_md5_ascii") == F.col("stored_md5"), F.lit("ascii_drop")).alias("how"),
               F.col("d.new_raw").alias("new_raw_any"))
       .withColumn("new_raw", F.when(F.col("how").isNotNull(), F.col("new_raw_any")))
       .drop("new_raw_any"))
out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(OUTPUT)

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) candidates, COUNT(DISTINCT key_type, key) keys, COUNT_IF(old_md5 IS NULL) unparsed,
       COUNT(DISTINCT CASE WHEN new_raw IS NOT NULL THEN concat(key_type, '|', key) END) keys_rederived,
       COUNT_IF(how = 'plain') plain, COUNT_IF(how = 'ascii_drop') ascii_drop, COUNT_IF(tei_src = 'legacy') legacy_rows,
       COUNT_IF(new_raw IS NOT NULL AND LENGTH(new_raw) < raw_length) new_shorter
FROM {OUTPUT}"""))
