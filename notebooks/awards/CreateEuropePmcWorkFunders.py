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

# 2c. qualifier-tolerant PREFIX matching for non-NIH funders whose EuropePMC agency strings
# append a sub-organization / program / country qualifier to the canonical name, e.g. AHA:
#   "American Heart Association-American Stroke Association"   (ASA is an AHA division)
#   "American Heart Association, United States"
#   "American Heart Association (AHA) Pre-doctoral Fellowship Award"
# The existing matcher is exact-only, so these fall through and the AHA grant is dropped
# (audit: 150 of 329 AHA gold-set recall misses are exactly this — EuropePMC has the grant,
# we never resolved it).
#
# This is OPT-IN and SAFE BY CONSTRUCTION, not open substring matching: a canonical name is
# enabled as a prefix only if NO OTHER funder's registry name (display_name or
# alternate_title) extends it token-wise. That guard REFUSES the dangerous case
# "National Science Foundation" (a token-prefix of "National Science Foundation of China" /
# NSFC) — so NSF would never swallow NSFC's works — while allowing AHA, which nothing extends.
def _toks(s):
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).split()

PREFIX_CANDIDATES = {
    "american heart association": 4320306230,   # AHA (F4320306230)
    # add more non-NIH funders here as they're verified; each is checked by the guard below.
}
_all_name_tuples = [(tuple(_toks(r["nl"])), r["funder_id"]) for r in fv.collect() if r["nl"]]
PREFIX_SAFE = {}
for _canon, _fid in PREFIX_CANDIDATES.items():
    _ct = tuple(_toks(_canon))
    _blockers = sorted({f for tk, f in _all_name_tuples
                        if f != _fid and len(tk) > len(_ct) and tk[:len(_ct)] == _ct})
    if _blockers:
        print(f"[prefix-guard] REFUSED {_canon!r} (F{_fid}): other funders extend it -> {_blockers}")
    else:
        PREFIX_SAFE[_ct] = _fid
print("prefix-safe funders enabled:", {" ".join(k): v for k, v in PREFIX_SAFE.items()})

bc_un = spark.sparkContext.broadcast(unambig)
bc_acr = spark.sparkContext.broadcast(NIH_ACR)
bc_nm = spark.sparkContext.broadcast(NIH_NAME)
bc_pfx = spark.sparkContext.broadcast(PREFIX_SAFE)

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
    # qualifier-tolerant prefix match, PREFIX-SAFE funders only (see 2c guard). Catches
    # agency strings that append a sub-org/program/country qualifier to the canonical name
    # (e.g. AHA '-American Stroke Association', ', United States', ' (AHA) ... Award').
    pfx = bc_pfx.value
    if pfx:
        at = re.sub(r"[^a-z0-9]+", " ", s.lower()).split()
        for ct, fid in pfx.items():
            k = len(ct)
            if len(at) >= k and tuple(at[:k]) == ct:
                return fid
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
# MAGIC cleaned_awards AS (
# MAGIC   -- EuropePMC grantIds often append a PI name after a spaced hyphen
# MAGIC   -- ("20SFRN35360185 - ANDREA BEATON") or trailing text in parens. Strip from the first
# MAGIC   -- " - " (space-hyphen-space) and drop a trailing "(...)". Real grant numbers never
# MAGIC   -- contain " - " or end in parens, so internal hyphens ("268201600003C-0-0-1") and
# MAGIC   -- mid-string content are untouched. Without this, the dirty id either fails
# MAGIC   -- is_usable_award_id (grant link lost) or is stored with the name glued on.
# MAGIC   SELECT work_id, funder_id,
# MAGIC     trim(regexp_replace(regexp_replace(aid_raw, '\\s+-\\s+.*$', ''), '\\s*\\([^()]*\\)\\s*$', '')) AS aid_raw
# MAGIC   FROM split_awards
# MAGIC ),
# MAGIC flattened AS (
# MAGIC   SELECT work_id, funder_id,
# MAGIC     CASE WHEN aid_raw IS NOT NULL AND aid_raw <> ''
# MAGIC               AND openalex.common.is_usable_award_id(aid_raw) THEN aid_raw END AS aid
# MAGIC   FROM cleaned_awards
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
# MAGIC WHERE provenance = 'europepmc_work_funders' AND priority IN (0, 2);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO openalex.awards.openalex_awards_raw
# MAGIC SELECT *, 0 AS priority FROM europepmc_work_funder_awards;

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

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AHA resolution fix validation (oxjob: non-NIH funder-name resolution).
# MAGIC -- Before the prefix-safe rule, AHA (F4320306230) edges were near-zero because its
# MAGIC -- EuropePMC agency strings ("American Heart Association-American Stroke Association",
# MAGIC -- ", United States", " (AHA) ... Award") never exact-matched. Expect a large jump.
# MAGIC SELECT
# MAGIC   COUNT(*)                              AS aha_work_funder_edges,
# MAGIC   COUNT(DISTINCT e.work_id)             AS aha_works,
# MAGIC   SUM(SIZE(e.award_ids))                AS aha_grant_links
# MAGIC FROM openalex.awards.europepmc_work_funders e
# MAGIC WHERE e.funder_id = 4320306230;

# COMMAND ----------

# MAGIC %md #### Diagnostic — top UNRESOLVED funder agencies (the non-NIH ~20%)
# MAGIC Ranks the agency strings the matcher misses and auto-triages each: the longest registry
# MAGIC funder-name that is a token-prefix of it, and whether that name is PREFIX-SAFE (no other
# MAGIC funder extends it). Prefix-safe hits are ready candidates for `PREFIX_CANDIDATES` (like
# MAGIC AHA); ambiguous hits need an exact alias or a manual call. Run once to build the worklist.

# COMMAND ----------

from collections import defaultdict

# 1) occurrence-weighted resolution rate + unresolved-agency frequencies
_mentions = (spark.table("europepmc_funding_raw")
             .select(F.explode("grants").alias("g")).select(F.col("g.agency").alias("agency"))
             .where(F.col("agency").isNotNull() & (F.trim("agency") != "")))
_mentions = _mentions.withColumn("fid", resolve_udf("agency")).cache()
_tot = _mentions.count(); _un = _mentions.where(F.col("fid").isNull()).count()
print(f"agency mentions: {_tot:,} | resolved: {_tot-_un:,} ({100*(_tot-_un)/_tot:.1f}%) | "
      f"unresolved: {_un:,} ({100*_un/_tot:.1f}%)")
top_un = (_mentions.where(F.col("fid").isNull())
          .groupBy("agency").count().orderBy(F.desc("count")).limit(200).collect())

# 2) auto-triage each top unresolved agency vs the registry (token-prefix, longest wins)
_by_first = defaultdict(list)
for _tk, _f in _all_name_tuples:
    if _tk: _by_first[_tk[0]].append((_tk, _f))

def _suggest(agency):
    at = tuple(_toks(agency))
    if not at: return None
    best = None
    for tk, fid in _by_first.get(at[0], []):
        if len(at) >= len(tk) and at[:len(tk)] == tk and (best is None or len(tk) > len(best[0])):
            best = (tk, fid)
    if not best: return None
    tk, fid = best
    safe = not any(f != fid and len(o) > len(tk) and o[:len(tk)] == tk for o, f in _all_name_tuples)
    return (" ".join(tk), fid, safe)

print(f"\n{'cnt':>8}  {'verdict':10}  agency  ->  suggested funder")
_safe_adds = []
for r in top_un:
    s = _suggest(r["agency"])
    if not s:
        print(f"{r['count']:>8}  {'no-prefix':10}  {r['agency'][:58]!r}")
        continue
    canon, fid, safe = s
    if safe: _safe_adds.append((canon, fid))
    print(f"{r['count']:>8}  {('SAFE-add' if safe else 'ambiguous'):10}  {r['agency'][:58]!r}  ->  F{fid} ({canon})")

# 3) ready-to-paste prefix-safe additions (review before adding)
print("\n# Prefix-safe PREFIX_CANDIDATES additions to review:")
for canon, fid in sorted(set(_safe_adds)):
    print(f'    "{canon}": {fid},')
