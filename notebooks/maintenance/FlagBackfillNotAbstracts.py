# Databricks notebook source
# MAGIC %md
# MAGIC # Flag legacy backfill abstracts that are not abstracts (oxjob #1800)
# MAGIC
# MAGIC `CreateWorksBase` falls back to `openalex.abstracts.abstracts_backfill` (priority 997) when no
# MAGIC other source has an abstract. That static legacy snapshot holds the same junk Parseland used to
# MAGIC emit: site blurbs ("Established in 1964, the IUCN Red List ..."), ChemInform notices, "No abstract
# MAGIC available.", SEO lines ('"Title." Journal, 12(3), pp. 4-5'), CABI's "This datasheet on X covers ...".
# MAGIC On 25 Oct 2026, 642,455 of the 1.88M works whose landing-page junk #1800's re-parse removes kept the
# MAGIC identical text through this fallback.
# MAGIC
# MAGIC This notebook builds `openalex.abstracts.abstracts_backfill_not_abstract`: the work_ids whose
# MAGIC backfill text (after the #807 repair overlay, i.e. exactly what CreateWorksBase would show) fails
# MAGIC parseland-lib's `is_not_abstract()`. `CreateWorksBase` anti-joins it in the `abstracts_backfill` CTE,
# MAGIC so those works fall through to no abstract instead of the junk.
# MAGIC
# MAGIC - **Same filter as Parseland, no copy.** The module and its hand-read list are parseland-lib commit
# MAGIC   `4245f4f` (`parseland_lib/not_abstract.py`, `data/not_abstracts.tsv`), stored unchanged in the
# MAGIC   Volume below and checked by sha256 before use. It needs only the standard library.
# MAGIC - **One-time build, like `abstracts_backfill_repair` (#807).** The backfill never changes, so the
# MAGIC   table only needs rebuilding when the filter does: upload the new commit's two files, change
# MAGIC   `FILTER_COMMIT` and the hashes, re-run.
# MAGIC - **Revert:** remove the anti-join from CreateWorksBase (one line), or `TRUNCATE` this table. Never
# MAGIC   DROP it while CreateWorksBase reads it: a missing table fails the nightly.
# MAGIC
# MAGIC MANUAL TRIGGER ONLY.

# COMMAND ----------

import hashlib
import os

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

FILTER_COMMIT = "4245f4f"
LIB_DIR = f"/Volumes/openalex/default/libraries/parseland_not_abstract/{FILTER_COMMIT}"
SHA256 = {
    "not_abstract.py": "85403be7e157e0bf47b23d95ccca0973a99799abf4e4c67a409728d1cf777c45",
    "data/not_abstracts.tsv": "3b8364f731da214c35fa0701c796f1ee7213593be6bceae42f2d2fa467115115",
}
SOURCE_TABLE = "openalex.abstracts.abstracts_backfill"
REPAIR_TABLE = "openalex.abstracts.abstracts_backfill_repair"
TARGET_TABLE = "openalex.abstracts.abstracts_backfill_not_abstract"
# Longest list text is 1,835 characters; templates need <= 1,500 and wordings <= 200 after cleaning.
# Anything longer than this cannot match, so it skips the Python UDF.
MAX_LEN = 6000

blobs = {}
for rel, want in SHA256.items():
    with open(os.path.join(LIB_DIR, rel), "rb") as f:
        blobs[rel] = f.read()
    got = hashlib.sha256(blobs[rel]).hexdigest()
    assert got == want, f"{rel}: sha256 {got} != {want} (wrong file in {LIB_DIR})"
SRC = blobs["not_abstract.py"].decode("utf-8")
TSV = blobs["data/not_abstracts.tsv"].decode("utf-8")
print(f"filter {FILTER_COMMIT}: {len(SRC):,} bytes of code, {sum(1 for l in TSV.splitlines() if l and not l.startswith('#')):,} list texts")

# COMMAND ----------

_state = {}


def _checker():
    """parseland-lib's not_abstract module, loaded from the pinned source, with its list from TSV."""
    if "reason" not in _state:
        ns = {"__name__": "not_abstract", "__file__": "not_abstract.py"}
        exec(compile(SRC, "not_abstract.py", "exec"), ns)
        known = frozenset(ns["key"](line.split("\t")[-1]) for line in TSV.splitlines() if line and not line.startswith("#"))
        ns["_known"] = lambda: known  # the module reads data/not_abstracts.tsv next to itself; same lines

        def reason(text):
            if not ns["is_not_abstract"](text):
                return None
            plain = ns["_plain"](text)
            k = ns["_key_of"](plain)
            if not k:
                return "empty"
            if k in known:
                return "list"
            if len(k) <= 200 and ns["_KEY_RE"].fullmatch(k):
                return "wording"
            return "template"

        _state["reason"] = reason
    return _state["reason"]


# Driver self-test: the decisions the module's own tests pin.
_r = _checker()
assert _r("No abstract available.") == "wording"
assert _r("This datasheet on Mesorchis covers Identity.") == "template"
assert _r("Established in 1964, the IUCN Red List of Threatened Species has evolved to become the world’s most "
          "comprehensive information source on the global conservation status of animal, fungi and plant species.") == "list"
assert _r("We measured the effect of nitrogen on maize yield in 40 field trials and found a 12% increase.") is None


@F.pandas_udf(T.StringType())
def not_abstract_reason(texts: pd.Series) -> pd.Series:
    r = _checker()
    return texts.map(lambda t: r(t) if isinstance(t, str) else None)


# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
  work_id BIGINT, reason STRING, text_len INT, filter_commit STRING, built_at TIMESTAMP
) COMMENT 'oxjob #1800: work_ids whose abstracts_backfill text (after the #807 repair overlay) is not an abstract per parseland-lib not_abstract.is_not_abstract; CreateWorksBase anti-joins it. Built by notebooks/maintenance/FlagBackfillNotAbstracts.'
""")

eff = spark.sql(f"""
SELECT bf.work_id,
       CASE WHEN r.work_id IS NOT NULL THEN r.abstract ELSE bf.abstract END AS abstract
FROM {SOURCE_TABLE} bf LEFT JOIN {REPAIR_TABLE} r ON bf.work_id = r.work_id
""").where(F.col("abstract").isNotNull() & (F.length("abstract") <= MAX_LEN))

flagged = (eff.withColumn("reason", not_abstract_reason("abstract"))
              .where(F.col("reason").isNotNull())
              .select("work_id", "reason", F.length("abstract").alias("text_len"),
                      F.lit(FILTER_COMMIT).alias("filter_commit"), F.current_timestamp().alias("built_at")))
flagged.createOrReplaceTempView("flagged_backfill")

# INSERT OVERWRITE replaces the rows atomically: CreateWorksBase never sees the table missing or half-built.
spark.sql(f"INSERT OVERWRITE {TARGET_TABLE} SELECT * FROM flagged_backfill")

# COMMAND ----------

display(spark.sql(f"SELECT reason, count(*) AS work_ids, max(filter_commit) AS filter_commit FROM {TARGET_TABLE} GROUP BY reason ORDER BY 2 DESC"))
