# Databricks notebook source
# MAGIC %md
# MAGIC # Flag abstract texts that are not abstracts, in every location source (oxjob #1800)
# MAGIC
# MAGIC Parseland stopped emitting placeholder and boilerplate abstracts on 24 Oct 2026 (parseland-lib
# MAGIC `not_abstract.is_not_abstract`), and `abstracts_backfill_not_abstract` keeps the same junk out of the
# MAGIC legacy backfill. The same texts also arrive in Crossref deposits ("No abstract is available for this
# MAGIC article.", "Résumé", "Extended abstract of a paper presented at ..."), repository and DataCite records
# MAGIC ("-", "Abstract", "N/A", a repository's own site description), PDF parses, and landing pages parsed
# MAGIC before the filter. On 26 Oct 2026 ~199K works still showed such a text from those sources.
# MAGIC
# MAGIC This notebook builds `openalex.abstracts.not_abstract_texts`: one row per distinct abstract text in
# MAGIC `openalex.works.locations_parsed` that fails `is_not_abstract`, keyed by `xxhash64(abstract)` and its
# MAGIC length. `CreateSuperLocations` joins it where it reads `locations_parsed` and NULLs those abstracts, so
# MAGIC selection falls through to the next source (a Crossref placeholder no longer hides the landing page's
# MAGIC real abstract) or to none.
# MAGIC
# MAGIC - **Same filter as Parseland, no copy.** Module and list are parseland-lib commit `FILTER_COMMIT`
# MAGIC   (`parseland_lib/not_abstract.py`, `parseland_lib/data/not_abstracts.tsv`), stored unchanged in the
# MAGIC   Volume below and checked by sha256.
# MAGIC - **Keyed by text, not by record**, so a new record carrying a known junk text is caught the night it
# MAGIC   arrives. A new *kind* of junk (or a per-record template on a new record) waits for the next build.
# MAGIC - **Owner guard lives in CreateSuperLocations**, on the record's own title: a hand-read list text is a
# MAGIC   book's or series' blurb, so it is kept on the record it describes (`is_list` marks those texts).
# MAGIC - **Revert:** remove the join in CreateSuperLocations, or `TRUNCATE` this table. Never DROP it while
# MAGIC   CreateSuperLocations reads it: a missing table fails the nightly.
# MAGIC
# MAGIC MANUAL TRIGGER ONLY (re-run when the filter changes; INSERT OVERWRITE keeps the table present).

# COMMAND ----------

import hashlib
import html
import os
import re

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

FILTER_COMMIT = "6b90a3b"
LIB_DIR = f"/Volumes/openalex/default/libraries/parseland_not_abstract/{FILTER_COMMIT}"
SHA256 = {
    "not_abstract.py": "aaf2b47fafc18d0d991cf9494c66382905673254d27dd86576beeeb0cf58c496",
    "data/not_abstracts.tsv": "3b8364f731da214c35fa0701c796f1ee7213593be6bceae42f2d2fa467115115",
}
SOURCE_TABLE = "openalex.works.locations_parsed"
TARGET_TABLE = "openalex.abstracts.not_abstract_texts"
# Longest list text is 1,835 characters of key; templates need <= 1,500 plain and wordings <= 200 of key.
# Raw text longer than this cannot match (markup inflates, so the cap is generous); it skips the Python UDF.
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

def _make_reason(src, tsv):
    """parseland-lib's not_abstract module, loaded from the pinned source, with its list from tsv.
    Returns reason(text): None for an abstract, else 'list' | 'wording' | 'template' | 'empty'."""
    ns = {"__name__": "not_abstract", "__file__": "not_abstract.py"}
    exec(compile(src, "not_abstract.py", "exec"), ns)
    known = frozenset(ns["key"](line.split("\t")[-1]) for line in tsv.splitlines() if line and not line.startswith("#"))
    ns["_known"] = lambda: known  # the module reads data/not_abstracts.tsv next to itself; same lines
    bounded_tag = re.compile(r"<[^<>]{0,300}>")

    def reason(text):
        if not ns["is_not_abstract"](text):
            return None
        plain = ns["_plain"](text)
        k = ns["_key_of"](plain)
        # The module strips any '<' ... '>' run as a tag, so a real abstract wrapped in '<< ... >>' collapses to nothing
        # ("<< ABSTRACT Starting with French Painting Today (1953) ..."; 5 texts on 26 Oct 2026). Keep a text whose
        # reading with bounded tags still has words the key lost. Errs toward keeping (inline tags split words here).
        bounded = bounded_tag.sub(" ", html.unescape(html.unescape(text)).lower())
        if len(ns["_WORD_RE"].findall(bounded)) > len(k.split()) + 5:
            return None
        if not k:
            return "empty"
        if k in known:
            return "list"
        if len(k) <= 200 and ns["_KEY_RE"].fullmatch(k):
            return "wording"
        return "template"

    return reason


# Driver self-test on its own instance: the UDF must not capture a loaded module (it doesn't pickle).
_r = _make_reason(SRC, TSV)
assert _r("No abstract available.") == "list"  # on the hand-read list, which is checked first
assert _r("<jats:p>No abstract is available for this paper.</jats:p>") == "wording"  # Crossref JATS markup
assert _r("—") == "empty"
assert _r("This datasheet on Mesorchis covers Identity.") == "template"
assert _r("Presenter: Nora Singh BA | Hartford Hospital") == "template"
assert _r("Established in 1964, the IUCN Red List of Threatened Species has evolved to become the world’s most "
          "comprehensive information source on the global conservation status of animal, fungi and plant species.") == "list"
assert _r("<jats:title>Abstract</jats:title><jats:p>We measured the effect of nitrogen on maize yield in 40 field "
          "trials and found a 12% increase.</jats:p>") is None
assert _r("Author: BARBARA SANTICH The French Australian Review 77 ... This article continues the story.") is None
assert _r("<< ABSTRACT Starting with French Painting Today (1953) this chapter considers developments in exhibitions "
          "from the 1950s to 1980s. This was a time when most cultural exchange between France and Australia ran "
          "through touring shows, and the chapter follows how curators, critics and dealers in Sydney and Melbourne "
          "framed contemporary French art for local audiences, and what those audiences made of it in turn. >>"
          ) is None  # the module alone calls this punctuation only
assert _r("<jats:title>Abstract</jats:title>") is not None  # a bare JATS label is still junk
del _r

_worker = {}  # empty on the driver, so the UDF pickles only code and two strings; filled once per worker


@F.pandas_udf(T.StringType())
def not_abstract_reason(texts: pd.Series) -> pd.Series:
    r = _worker.get("reason")
    if r is None:
        r = _worker["reason"] = _make_reason(SRC, TSV)
    return texts.map(lambda t: r(t) if isinstance(t, str) else None)


# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
  abstract_hash BIGINT, text_len INT, is_list BOOLEAN, reason STRING, n_rows BIGINT,
  provenances ARRAY<STRING>, sample_text STRING, filter_commit STRING, built_at TIMESTAMP
) COMMENT 'oxjob #1800: distinct abstract texts in openalex.works.locations_parsed that are not abstracts per parseland-lib not_abstract.is_not_abstract, keyed by xxhash64(abstract) + length(abstract). CreateSuperLocations NULLs them (owner guard on is_list texts). Built by notebooks/maintenance/FlagLocationNotAbstracts.'
""")

rows = (spark.table(SOURCE_TABLE)
        .where(F.col("abstract").isNotNull() & (F.length("abstract") <= MAX_LEN) & (F.trim("abstract") != ""))
        .select("provenance", "abstract"))
# Check every record, then group only the flagged ones: grouping all ~520M texts first would shuffle ~0.6 TB.
flagged = (rows.withColumn("reason", not_abstract_reason("abstract"))
               .where(F.col("reason").isNotNull())
               .groupBy(F.xxhash64("abstract").alias("abstract_hash"), F.length("abstract").alias("text_len"))
               .agg(F.first("abstract").alias("abstract"), F.first("reason").alias("reason"),
                    F.count("*").alias("n_rows"), F.collect_set("provenance").alias("provenances")))
flagged.createOrReplaceTempView("flagged_texts")

spark.sql(f"""
INSERT OVERWRITE {TARGET_TABLE}
SELECT abstract_hash, text_len, reason = 'list' AS is_list, reason, n_rows, provenances,
       left(abstract, 500) AS sample_text, '{FILTER_COMMIT}' AS filter_commit, current_timestamp() AS built_at
FROM flagged_texts
""")
# INSERT OVERWRITE replaces the rows atomically: CreateSuperLocations never sees the table missing or half-built.

# COMMAND ----------

display(spark.sql(f"""
SELECT reason, count(*) AS texts, sum(n_rows) AS records, max(filter_commit) AS filter_commit
FROM {TARGET_TABLE} GROUP BY reason ORDER BY 3 DESC"""))
