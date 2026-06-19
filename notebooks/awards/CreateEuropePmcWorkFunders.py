# Databricks notebook source
# MAGIC %md
# MAGIC ### Create EuropePMC work-funder + work-award linkages (oxjob #477, Phase 1)
# MAGIC
# MAGIC Linkage-only source (we resolve to *existing* OpenAlex works; no new works). Mirrors
# MAGIC the **DataCite/Crossref** pattern (junction with `award_ids` + minted award entities),
# MAGIC with an upfront DOI→`work_id` crosswalk because EuropePMC is not in `locations_mapped`.
# MAGIC
# MAGIC Source: `openalex-ingest/europepmc.py` → `s3://openalex-ingest/awards/europepmc/`
# MAGIC (Articles API `grantsList`: per work, `{grant_id, agency}` + crosswalk ids).
# MAGIC
# MAGIC Funder resolution = existing registry matcher (`display_name ∪ alternate_titles`,
# MAGIC unambiguous) **+** a verified NIH/HHS/VA acronym→funder_id map (the MEDLINE `X NIH HHS`
# MAGIC codes). Validated locally ~81% occurrence-weighted recall (oxjob #477).
# MAGIC
# MAGIC **Outputs (both funder AND grant linkages):**
# MAGIC 1. `openalex.awards.europepmc_work_funders (work_id, funder_id, award_ids)` — the junction.
# MAGIC 2. Award entities → `openalex.awards.openalex_awards_raw` (provenance `europepmc_work_funders`, priority 2).
# MAGIC
# MAGIC **Surfaces on /works via (go-live, after review):**
# MAGIC - funder edges: `funder_reported_work_funders` UNION (Hakai notebook) → `from_funder_reported` leg.
# MAGIC - award (grant) entities: `WorkAwards` `europepmc_work_funder_awards` leg (priority 4) → `from_work_awards`.
# MAGIC - Order: this notebook → RefreshWorkAwards (CreateAwards+WorkAwards) → Hakai → walden_end2end.
# MAGIC
# MAGIC Widget `data_prefix=_bounded/` for the bounded test; empty for the full corpus.

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

# MAGIC %md #### Step 3 — resolve agency→funder_id, crosswalk DOI→work_id, stage resolved mentions

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

# per-mention: agency -> funder_id ; keep raw grant_id string (split downstream in SQL)
mentions = (spark.table("europepmc_funding_raw")
            .select(F.lower("doi").alias("doi_bare"), F.explode("grants").alias("g"))
            .select("doi_bare", F.col("g.agency").alias("agency"), F.col("g.grant_id").alias("grant_id")))
resolved = (mentions.withColumn("funder_id", resolve_udf("agency"))
            .where(F.col("funder_id").isNotNull()))

# crosswalk DOI -> work_id (99.9% of EuropePMC funding records carry a DOI; oxjob #477 probe)
works = (spark.table("openalex.works.openalex_works")
         .where(F.col("doi").isNotNull())
         .select(F.col("id").alias("work_id"),
                 F.regexp_replace(F.lower("doi"), r"^https?://(dx\.)?doi\.org/", "").alias("wdoi_bare")))
epmc_resolved = (resolved.join(works, resolved.doi_bare == works.wdoi_bare, "inner")
                 .select("work_id", "funder_id", "grant_id"))
epmc_resolved.createOrReplaceTempView("epmc_resolved")
print("resolved (work,funder,grant) mentions:", epmc_resolved.count())

# COMMAND ----------

# MAGIC %md #### Step 4 — build the junction with award_ids (split multi-grant strings; keep funder-only)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE openalex.awards.europepmc_work_funders
# MAGIC USING delta AS
# MAGIC WITH split_awards AS (
# MAGIC   -- one grantId field can pack several ids ("3250170251, U23A20207"). Split on , or ;.
# MAGIC   -- OUTER EXPLODE keeps funder-only rows (grant_id NULL) so those funder edges survive.
# MAGIC   SELECT work_id, funder_id, trim(aid_raw) AS aid_raw
# MAGIC   FROM epmc_resolved
# MAGIC   LATERAL VIEW OUTER EXPLODE(split(grant_id, '[,;]')) AS aid_raw
# MAGIC ),
# MAGIC flattened AS (
# MAGIC   SELECT work_id, funder_id,
# MAGIC     CASE WHEN aid_raw IS NOT NULL AND aid_raw <> ''
# MAGIC               AND openalex.common.is_usable_award_id(aid_raw) THEN aid_raw END AS aid
# MAGIC   FROM split_awards
# MAGIC )
# MAGIC SELECT work_id, funder_id, ARRAY_DISTINCT(COLLECT_LIST(aid)) AS award_ids
# MAGIC FROM flattened
# MAGIC GROUP BY work_id, funder_id;

# COMMAND ----------

# MAGIC %md #### Step 5 — mint award entities into openalex_awards_raw (mirror DataCite)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW europepmc_work_funder_awards AS
# MAGIC WITH exploded AS (
# MAGIC   SELECT DISTINCT ewf.funder_id, LOWER(award_id) AS normalized_award_id, award_id AS funder_award_id
# MAGIC   FROM openalex.awards.europepmc_work_funders ewf
# MAGIC   LATERAL VIEW EXPLODE(award_ids) AS award_id
# MAGIC   WHERE SIZE(award_ids) > 0
# MAGIC ),
# MAGIC funders AS (SELECT funder_id, display_name, ror_id, doi FROM openalex.mid.funder)
# MAGIC SELECT
# MAGIC   ABS(XXHASH64(CONCAT(f.funder_id, ':', e.normalized_award_id))) % 9000000000 AS id,
# MAGIC   CAST(NULL AS STRING) AS display_name, CAST(NULL AS STRING) AS description,
# MAGIC   f.funder_id, e.funder_award_id,
# MAGIC   CAST(NULL AS DOUBLE) AS amount, CAST(NULL AS STRING) AS currency,
# MAGIC   STRUCT(CONCAT('https://openalex.org/F', f.funder_id) AS id, f.display_name, f.ror_id, f.doi) AS funder,
# MAGIC   CAST(NULL AS STRING) AS funding_type, CAST(NULL AS STRING) AS funder_scheme,
# MAGIC   'europepmc_work_funders' AS provenance,
# MAGIC   CAST(NULL AS DATE) AS start_date, CAST(NULL AS DATE) AS end_date,
# MAGIC   CAST(NULL AS INT) AS start_year, CAST(NULL AS INT) AS end_year,
# MAGIC   CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
# MAGIC     affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) AS lead_investigator,
# MAGIC   CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
# MAGIC     affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) AS co_lead_investigator,
# MAGIC   CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
# MAGIC     affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) AS investigators,
# MAGIC   CAST(NULL AS STRING) AS landing_page_url,
# MAGIC   openalex.common.extract_grant_doi(e.funder_award_id) AS doi,
# MAGIC   CONCAT('https://api.openalex.org/works?filter=awards.id:G', ABS(XXHASH64(CONCAT(f.funder_id, ':', e.normalized_award_id))) % 9000000000) AS works_api_url,
# MAGIC   CURRENT_TIMESTAMP() AS created_date, CURRENT_TIMESTAMP() AS updated_date
# MAGIC FROM exploded e JOIN funders f ON f.funder_id = e.funder_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM openalex.awards.openalex_awards_raw
# MAGIC WHERE provenance = 'europepmc_work_funders' AND priority = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO openalex.awards.openalex_awards_raw
# MAGIC SELECT *, 2 AS priority FROM europepmc_work_funder_awards;

# COMMAND ----------

# MAGIC %md #### Sanity checks + sample edges (for review)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*)                  FROM europepmc_funding_raw)                   AS staged_works,
# MAGIC   (SELECT COUNT(*)                  FROM openalex.awards.europepmc_work_funders)  AS junction_rows,
# MAGIC   (SELECT COUNT(DISTINCT work_id)   FROM openalex.awards.europepmc_work_funders)  AS distinct_works,
# MAGIC   (SELECT COUNT(DISTINCT funder_id) FROM openalex.awards.europepmc_work_funders)  AS distinct_funders,
# MAGIC   (SELECT COUNT(*) FROM openalex.awards.europepmc_work_funders WHERE SIZE(award_ids) > 0) AS rows_with_awards,
# MAGIC   (SELECT COUNT(*) FROM openalex.awards.openalex_awards_raw WHERE provenance='europepmc_work_funders') AS minted_award_rows;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- top resolved funders by edge count (eyeball: NIH institutes, NSFC, Wellcome, ...)
# MAGIC SELECT f.display_name, COUNT(*) AS edges, SUM(SIZE(e.award_ids)) AS grant_links
# MAGIC FROM openalex.awards.europepmc_work_funders e
# MAGIC JOIN openalex.mid.funder f ON f.funder_id = e.funder_id
# MAGIC GROUP BY f.display_name ORDER BY edges DESC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- sample edges WITH a grant id, to spot-check the award linkages
# MAGIC SELECT w.doi, f.display_name, e.award_ids
# MAGIC FROM openalex.awards.europepmc_work_funders e
# MAGIC JOIN openalex.mid.funder f ON f.funder_id = e.funder_id
# MAGIC JOIN openalex.works.openalex_works w ON w.id = e.work_id
# MAGIC WHERE SIZE(e.award_ids) > 0 LIMIT 25;
