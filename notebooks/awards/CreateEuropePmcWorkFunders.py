# Databricks notebook source
# MAGIC %md
# MAGIC ### Create EuropePMC work-funder linkages (oxjob #477, Phase 1)
# MAGIC
# MAGIC OUTPUT-LIST / linkage-only pattern (mirrors `CreateHakaiWorkFunders`): resolve the
# MAGIC funder strings EuropePMC carries per work (Articles API `grantsList`) to OpenAlex
# MAGIC funder_ids, crosswalk the work's DOI to an OpenAlex `work_id`, and write the junction
# MAGIC `openalex.awards.europepmc_work_funders (work_id, funder_id, provenance)`.
# MAGIC
# MAGIC Funder resolution = the **existing** matcher (registry `display_name ∪ alternate_titles`,
# MAGIC unambiguous) **+** a verified NIH/HHS/VA acronym→funder_id map (the MEDLINE `X NIH HHS`
# MAGIC codes that dominate biomedical funding strings). No new entity-resolution layer.
# MAGIC Validated locally at ~81% occurrence-weighted recall (oxjob #477).
# MAGIC
# MAGIC **Source data:** harvested by `openalex-ingest/europepmc.py` →
# MAGIC `s3://openalex-ingest/awards/europepmc/`. Set widget `data_prefix=_bounded/` for the
# MAGIC bounded test run; empty for the full corpus.
# MAGIC
# MAGIC **Go-live (after review) — NOT done by this notebook:** UNION
# MAGIC `europepmc_work_funders` into `openalex.awards.funder_reported_work_funders` (the
# MAGIC `from_funder_reported` leg of CreateWorksEnriched already reads that shared table), then
# MAGIC run `walden_end2end`. Grant-level award rows (→ openalex_awards_raw) are a follow-on.

# COMMAND ----------

dbutils.widgets.text("data_prefix", "_bounded/", "S3 sub-prefix under awards/europepmc/ ('' = full)")
DATA_PREFIX = dbutils.widgets.get("data_prefix")
SRC = f"s3a://openalex-ingest/awards/europepmc/{DATA_PREFIX}"
NIH_MAP_PATH = "s3a://openalex-ingest/awards/europepmc/nih_funder_map.json"
print("reading:", SRC)

# COMMAND ----------

# MAGIC %md #### Step 1 — stage the harvested funding records

# COMMAND ----------

import re, html
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

raw = (spark.read.json(SRC)  # gzip auto-detected; nested grants -> array<struct>
       .where(F.col("doi").isNotNull() & (F.col("doi") != "")))
raw.createOrReplaceTempView("europepmc_funding_raw")
print("staged works:", raw.count())

# COMMAND ----------

# MAGIC %md #### Step 2 — build funder lookups (existing matcher + verified NIH map)

# COMMAND ----------

# 2a. unambiguous registry names (display_name ∪ alternate_titles), DataCite #268 pattern
fv = (spark.table("openalex.mid.funder")
      .where(F.col("display_name").isNotNull())
      .select("funder_id",
              F.explode(F.array_union(
                  F.array("display_name"),
                  F.coalesce(F.from_json("alternate_titles", "array<string>"), F.array())
              )).alias("nm")))
fv = fv.select(F.lower(F.trim(F.regexp_replace("nm", r"\s+", " "))).alias("nl"), "funder_id")
counts = (fv.groupBy("nl")
          .agg(F.countDistinct("funder_id").alias("c"), F.max("funder_id").alias("fid"))
          .where("c = 1"))
unambig = {r["nl"]: r["fid"] for r in counts.collect()}
print("unambiguous registry names:", len(unambig))

# 2b. verified NIH/HHS/VA acronym -> funder_id (built + checked locally, staged on S3)
nrow = spark.read.option("multiline", "true").json(NIH_MAP_PATH).collect()[0].asDict()
def _fid(v): return int(v["funder_id"].split("/")[-1].lstrip("F"))
NIH_ACR = {acr: _fid(v) for acr, v in nrow.items()}
NIH_NAME = {" ".join(v["display_name"].lower().split()): _fid(v) for v in nrow.values()}
print("NIH acronym entries:", len(NIH_ACR))

bc_un = spark.sparkContext.broadcast(unambig)
bc_acr = spark.sparkContext.broadcast(NIH_ACR)
bc_nm = spark.sparkContext.broadcast(NIH_NAME)

# COMMAND ----------

# MAGIC %md #### Step 3 — resolve agency strings to funder_ids (the validated v1 stack)

# COMMAND ----------

def _norm(s): return " ".join(s.lower().split()) if s else s

def resolve(agency):
    if not agency:
        return None
    un, acr, nm = bc_un.value, bc_acr.value, bc_nm.value
    s = html.unescape(html.unescape(agency))           # &amp;amp; -> &
    st = s.strip(); toks = st.split()
    if toks:                                            # MEDLINE 'X NIH HHS' etc. -> funder_id (+ parent fallback)
        first = toks[0]; spec = acr.get(first) or acr.get(first.upper())
        if st.endswith("NIH HHS"): return spec or acr.get("NIH")
        if st.endswith("CDC HHS"): return spec or acr.get("CDC")
        if st.endswith(" VA"):     return spec or acr.get("VA")
        if st.endswith("HHS") and spec: return spec
    base = re.sub(r"^the\s+", "", s, flags=re.I)
    def sp(x): return re.sub(r"\s*\([^()]*\)\s*$", "", x).strip()
    cands = [s, base, sp(base)]
    for seg in s.split("|"):
        cands.append(sp(re.sub(r"^the\s+", "", seg.strip(), flags=re.I)))
    m = re.search(r"\(([^()]*)\)\s*$", s)
    if m: cands.append(m.group(1))
    for c in cands:
        n = _norm(c)
        if not n: continue
        if n in nm: return nm[n]
        if n in un: return un[n]
    return None

resolve_udf = F.udf(resolve, LongType())

mentions = (spark.table("europepmc_funding_raw")
            .select(F.lower("doi").alias("doi_bare"), F.explode("grants").alias("g"))
            .select("doi_bare", F.col("g.agency").alias("agency"), F.col("g.grant_id").alias("grant_id")))
resolved = (mentions.withColumn("funder_id", resolve_udf("agency"))
            .where(F.col("funder_id").isNotNull()))

# COMMAND ----------

# MAGIC %md #### Step 4 — crosswalk DOI → work_id and build the junction

# COMMAND ----------

# 99.9% of EuropePMC funding records carry a DOI (oxjob #477 probe) -> DOI crosswalk suffices.
works = (spark.table("openalex.works.openalex_works")
         .where(F.col("doi").isNotNull())
         .select(F.col("id").alias("work_id"),
                 F.regexp_replace(F.lower("doi"), r"^https?://(dx\.)?doi\.org/", "").alias("wdoi_bare")))

joined = resolved.join(works, resolved.doi_bare == works.wdoi_bare, "inner")

junction = (joined.select("work_id", "funder_id").distinct()
            .withColumn("provenance", F.lit("europepmc_work_funders")))
junction.write.mode("overwrite").saveAsTable("openalex.awards.europepmc_work_funders")
print("junction rows (work,funder):", spark.table("openalex.awards.europepmc_work_funders").count())

# COMMAND ----------

# MAGIC %md #### Sanity checks + sample edges (for review)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*)               FROM europepmc_funding_raw)                         AS staged_works,
# MAGIC   (SELECT COUNT(*)               FROM openalex.awards.europepmc_work_funders)        AS junction_rows,
# MAGIC   (SELECT COUNT(DISTINCT work_id) FROM openalex.awards.europepmc_work_funders)       AS distinct_works,
# MAGIC   (SELECT COUNT(DISTINCT funder_id) FROM openalex.awards.europepmc_work_funders)     AS distinct_funders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- top resolved funders by edge count (eyeball: should be NIH institutes, NSFC, Wellcome, ...)
# MAGIC SELECT e.funder_id, f.display_name, COUNT(*) AS edges
# MAGIC FROM openalex.awards.europepmc_work_funders e
# MAGIC JOIN openalex.mid.funder f ON f.funder_id = e.funder_id
# MAGIC GROUP BY e.funder_id, f.display_name
# MAGIC ORDER BY edges DESC LIMIT 25;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 25 sample edges with the resolved funder name + the work, to spot-check correctness
# MAGIC SELECT e.work_id, w.doi, e.funder_id, f.display_name
# MAGIC FROM openalex.awards.europepmc_work_funders e
# MAGIC JOIN openalex.mid.funder f ON f.funder_id = e.funder_id
# MAGIC JOIN openalex.works.openalex_works w ON w.id = e.work_id
# MAGIC LIMIT 25;
