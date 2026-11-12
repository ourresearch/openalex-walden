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
# MAGIC - **Owner rows** (`openalex.abstracts.not_abstract_owner_rows`): a hand-read list text is a book's or
# MAGIC   series' blurb, so it stays on the record it describes: >= 2 of the record's title words (5+ letters),
# MAGIC   and at least half of them, appear in the text (the backfill filter's rule). Landing-page and PDF rows
# MAGIC   carry no title, so theirs comes from the Crossref record of their DOI, else the repository record of
# MAGIC   their pmh id. CreateSuperLocations leaves these records' abstracts alone.
# MAGIC - **Templates count on landing pages and Crossref only** (CreateSuperLocations applies that). They were
# MAGIC   built for landing pages; on repository, DataCite and PDF records the bar (26 Oct 2026) found real
# MAGIC   descriptions they match: "Presenter: ... Abstract: ..." slide decks, quoted library catalog notes,
# MAGIC   reports that open with "The views expressed in this report ..." and go on to a summary.
# MAGIC - **Short stand-ins** (oxjob #1800, 29 Oct 2026): short texts repeated across many works ("v", ":unav",
# MAGIC   "PhD", "Mode of access: Internet.") that only name the item, its kind, date or status. Judged one text at a
# MAGIC   time by Opus 5.5 under the charter's stand-in rule; the flagged texts live in `SHORT_TABLE`, and every
# MAGIC   rebuild re-derives their (hash, length) keys from `locations_parsed` with reason `short_stand_in`, which
# MAGIC   applies to every provenance. A length cut alone failed its bar (real photo and figure captions).
# MAGIC   Since 2 Nov 2026 the table also holds repeated texts of 30-299 chars ("Digital Specimen for the physical
# MAGIC   specimen hosted at ...") and one-off texts of 1-29 chars read by Jev, so keys are derived up to 1,000 chars.
# MAGIC - **Deposit notices** (oxjob #1800, 6 Nov 2026): DSpace repositories (Brazilian theses, Illinois IDEALS, EKT)
# MAGIC   expose dc.description.provenance as their first dc:description, so ~248K works showed "Submitted by NAME
# MAGIC   (email) on 2016-... No. of bitstreams: 1 file.pdf: N bytes, checksum: ... (MD5)" as their abstract. Each text
# MAGIC   is unique (checksums, timestamps), so they are found by rule `DEPOSIT_NOTICE` (reason `deposit_notice`, every
# MAGIC   provenance), not by a list. A '%%%%' join or text left after the file entries means a real abstract follows
# MAGIC   the notice: kept. Bar: 0 real of 250 random flagged works (Opus 5.5).
# MAGIC - **Citation lines** (oxjob #1800, 12 Nov 2026): OUP Academic pages with no abstract carry a 160-char meta
# MAGIC   description "<authors>; <title>, <journal>, Volume N, Issue N, <date>, Pages ..." that the Oxford parser
# MAGIC   took as the abstract (~395K works). Each text is unique, so rule `CITATION_LINE_KEYS` finds them: a
# MAGIC   landing-page or Crossref text <= 170 chars whose part after the first "; " starts like the record's title
# MAGIC   (its own, else its DOI's Crossref title), >= 10 letters+digits. Reason `template` (landing pages and
# MAGIC   Crossref only). Bar: 0 real of 250 + 0 of 100 weakest matches (Opus 5.5).
# MAGIC - **Held texts** (`HELD_TABLE`): texts a rule matches but a job holds back (e.g. its works would fall back to
# MAGIC   an unread text). Every rule's keys skip them; delete a row to release it at the next build.
# MAGIC - **Judged keys** (`JUDGED_TABLE`, oxjob #1800, 12 Nov 2026): keys a judged read made, which no rule here can
# MAGIC   re-derive (first: tfteaser-v1, T&F 200-char body teasers that Jev's opening-desc-v1 read as not describing the
# MAGIC   work, ~118K keys; Opus bar 0/150 book reviews, 1/150 other). Carried over unchanged on every build.
# MAGIC - **Revert:** remove the join in CreateSuperLocations, or `TRUNCATE` these tables. Never DROP them while
# MAGIC   CreateSuperLocations reads them: a missing table fails the nightly.
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

# a3e36f8 (26 Oct): + short Silverchair page furniture and Frontiers headers (#1857); '< word' is no longer a tag.
FILTER_COMMIT = "a3e36f8"
LIB_DIR = f"/Volumes/openalex/default/libraries/parseland_not_abstract/{FILTER_COMMIT}"
SHA256 = {
    "not_abstract.py": "2c30b1f05b641836d228c3af151b03aaf3cfd1713ad21629aab383670ed54f38",
    "data/not_abstracts.tsv": "3b8364f731da214c35fa0701c796f1ee7213593be6bceae42f2d2fa467115115",
}
SOURCE_TABLE = "openalex.works.locations_parsed"
TARGET_TABLE = "openalex.abstracts.not_abstract_texts"
OWNER_TABLE = "openalex.abstracts.not_abstract_owner_rows"
# oxjob #1800 (29 Oct 2026): judged short stand-in texts (trimmed), the source of truth for reason short_stand_in.
SHORT_TABLE = "openalex.abstracts.not_abstract_short_texts"
# oxjob #1800 (6 Nov 2026): rule dspace-v1, a SQL predicate on a text column (see the header). Keep byte-identical
# with FlagBackfillNotAbstracts. 'Submitted by' alone matches real texts (congressional reports), hence the bitstreams.
DEPOSIT_NOTICE_COMMIT = "dspace-v1: DSpace deposit notice (#1800, 6 Nov 2026)"
DEPOSIT_NOTICE = r"""(
  trim(COL) RLIKE '^(Submitted by|Made available in DSpace on|Approved for entry into archive by|Rejected by|Item withdrawn by) '
  AND instr(COL, 'No. of bitstreams') > 0 AND instr(COL, '%%%%') = 0
  AND length(regexp_replace(trim(COL), '(?i)[^\\n]{0,200}?: \\d+ bytes, checksum: [0-9a-f]{32} \\((MD5|SHA-1|SHA1)\\)', ' ')) <= 150)"""
# oxjob #1800 (12 Nov 2026): rule oupcite-v1 (see the header). Same SQL as the 12 Nov write
# (oxjobs working/parseland-swarm-abstracts/work/oupcite/rule.sql). OUP landing-page rows carry no title, hence
# the DOI's Crossref title. Over 170 chars the same shape is reference-work keyword lists and real blurbs.
CITATION_LINE_COMMIT = "oupcite-v1: OUP Academic citation line (#1800, 12 Nov 2026)"
CITATION_LINE_KEYS = r"""
  SELECT xxhash64(abstract) AS abstract_hash, length(abstract) AS text_len, first(abstract) AS abstract,
         count(*) AS n_rows, collect_set(provenance) AS provenances
  FROM (
    SELECT r.provenance, r.abstract,
           regexp_replace(lower(coalesce(r.title, cr.cr_title)), '[^\\p{L}\\p{N}]', '') AS nt,
           regexp_replace(lower(substring(r.abstract, r.p + 2)), '[^\\p{L}\\p{N}]', '') AS na
    FROM (
      SELECT provenance, title, abstract, instr(abstract, '; ') AS p,
             lower(get(filter(ids, x -> x.namespace = 'doi').id, 0)) AS doi
      FROM SOURCE_TABLE
      WHERE provenance IN ('landing_page', 'crossref') AND abstract IS NOT NULL AND length(abstract) <= 170
        AND instr(abstract, '; ') > 1
    ) r
    LEFT JOIN (SELECT lower(native_id) AS doi, max(title) AS cr_title FROM SOURCE_TABLE
               WHERE provenance = 'crossref' AND title IS NOT NULL GROUP BY 1) cr ON cr.doi = r.doi
  ) s
  WHERE nt IS NOT NULL AND least(length(na), length(nt)) >= 10
    AND left(na, least(length(na), length(nt))) = left(nt, least(length(na), length(nt)))
  GROUP BY 1, 2"""
HELD_TABLE = "openalex.abstracts.not_abstract_held_texts"
# oxjob #1800 (12 Nov 2026): keys a judged read made (Jev / Opus, e.g. tfteaser-v1), which no rule here re-derives.
JUDGED_TABLE = "openalex.abstracts.not_abstract_judged_keys"
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
assert _r("Neurology| June 01 2004 Pseudotumor Cerebri and Sickle Cell Disease https://doi.org/10.1542/gr.11-6-64 "
          "Views Icon Views Article contents Figures & tables Share Icon Share Cite Icon Cite") == "template"
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
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HELD_TABLE} (
  abstract_hash BIGINT, text_len INT, filter_commit STRING, why STRING, held_at TIMESTAMP
) COMMENT 'oxjob #1800: texts a not-abstract rule matches but a job holds back (keyed like not_abstract_texts). FlagLocationNotAbstracts leaves them out of not_abstract_texts on a rebuild. Delete a row to release it.'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {JUDGED_TABLE} (
  abstract_hash BIGINT, text_len INT, reason STRING, n_rows BIGINT, provenances ARRAY<STRING>, sample_text STRING,
  filter_commit STRING, built_at TIMESTAMP
) COMMENT 'oxjob #1800: not_abstract_texts keys that come from a judged read (Jev / Opus), not a rule the build can re-derive. FlagLocationNotAbstracts carries them over on a rebuild. Delete a row to drop the key at the next build (and from not_abstract_texts to drop it now).'
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
SELECT * FROM (
SELECT abstract_hash, text_len, reason = 'list' AS is_list, reason, n_rows, provenances,
       left(abstract, 500) AS sample_text, '{FILTER_COMMIT}' AS filter_commit, current_timestamp() AS built_at
FROM flagged_texts
UNION ALL
-- Short stand-ins (see the header): every record whose trimmed text is a judged one, keyed like the rest.
SELECT s.abstract_hash, s.text_len, FALSE AS is_list, 'short_stand_in' AS reason, s.n_rows, s.provenances,
       left(s.abstract, 500) AS sample_text, s.rubric AS filter_commit, current_timestamp() AS built_at
FROM (
  SELECT xxhash64(lp.abstract) AS abstract_hash, length(lp.abstract) AS text_len, first(lp.abstract) AS abstract,
         count(*) AS n_rows, collect_set(lp.provenance) AS provenances, first(st.rubric) AS rubric
  FROM {SOURCE_TABLE} lp JOIN {SHORT_TABLE} st ON st.text = trim(lp.abstract)
  WHERE lp.abstract IS NOT NULL AND length(lp.abstract) <= 1000  -- judged texts reach 299 chars trimmed (2 Nov 2026)
  GROUP BY 1, 2
) s LEFT ANTI JOIN flagged_texts f ON f.abstract_hash = s.abstract_hash AND f.text_len = s.text_len
UNION ALL
-- Deposit notices (see the header): every record whose text matches the rule, keyed like the rest, once per key.
SELECT d.abstract_hash, d.text_len, FALSE AS is_list, 'deposit_notice' AS reason, d.n_rows, d.provenances,
       left(d.abstract, 500) AS sample_text, '{DEPOSIT_NOTICE_COMMIT}' AS filter_commit, current_timestamp() AS built_at
FROM (
  SELECT xxhash64(lp.abstract) AS abstract_hash, length(lp.abstract) AS text_len, first(lp.abstract) AS abstract,
         count(*) AS n_rows, collect_set(lp.provenance) AS provenances
  FROM {SOURCE_TABLE} lp LEFT ANTI JOIN {SHORT_TABLE} st ON st.text = trim(lp.abstract)
  WHERE lp.abstract IS NOT NULL AND {DEPOSIT_NOTICE.replace('COL', 'lp.abstract')}
  GROUP BY 1, 2
) d LEFT ANTI JOIN flagged_texts f ON f.abstract_hash = d.abstract_hash AND f.text_len = d.text_len
UNION ALL
-- Citation lines (see the header): reason 'template', so CreateSuperLocations applies them to landing pages and
-- Crossref only. Skips texts another block keys (judged short texts by text, the UDF's by key).
SELECT c.abstract_hash, c.text_len, FALSE AS is_list, 'template' AS reason, c.n_rows, c.provenances,
       left(c.abstract, 500) AS sample_text, '{CITATION_LINE_COMMIT}' AS filter_commit, current_timestamp() AS built_at
FROM ({CITATION_LINE_KEYS.replace('SOURCE_TABLE', SOURCE_TABLE)}) c
LEFT ANTI JOIN flagged_texts f ON f.abstract_hash = c.abstract_hash AND f.text_len = c.text_len
LEFT ANTI JOIN {SHORT_TABLE} st ON st.text = trim(c.abstract)
UNION ALL
-- Judged keys (see the header): carried over as written; a key another block already makes is left to that block.
SELECT j.abstract_hash, j.text_len, FALSE AS is_list, j.reason, j.n_rows, j.provenances, j.sample_text, j.filter_commit,
       current_timestamp() AS built_at
FROM {JUDGED_TABLE} j
LEFT ANTI JOIN flagged_texts f ON f.abstract_hash = j.abstract_hash AND f.text_len = j.text_len
) k
-- Held texts (see the header) are keyed by no rule.
LEFT ANTI JOIN {HELD_TABLE} h ON h.abstract_hash = k.abstract_hash AND h.text_len = k.text_len
""")
# INSERT OVERWRITE replaces the rows atomically: CreateSuperLocations never sees the table missing or half-built.

# COMMAND ----------

# Owner rows: the records a hand-read list text describes (see the header). shortcut: word overlap, not meaning; a
# series blurb that repeats its series title on every issue is also kept (it stays junk there).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OWNER_TABLE} (
  provenance STRING, native_id STRING, abstract_hash BIGINT, title STRING, built_at TIMESTAMP
) COMMENT 'oxjob #1800: records whose abstract is a hand-read not-abstract list text that describes the record itself (its title words appear in the text). CreateSuperLocations keeps these abstracts. Built by notebooks/maintenance/FlagLocationNotAbstracts.'
""")
spark.sql(r"""
INSERT OVERWRITE OWNER_TABLE
WITH lt AS (SELECT abstract_hash, text_len FROM TARGET_TABLE WHERE is_list),
r AS (
  SELECT lp.provenance, lp.native_id, lp.abstract, lt.abstract_hash, lp.title,
         lower(get(filter(lp.ids, x -> x.namespace = 'doi').id, 0)) AS doi,
         get(filter(lp.ids, x -> x.namespace = 'pmh').id, 0) AS pmh
  FROM SOURCE_TABLE lp JOIN lt ON lt.abstract_hash = xxhash64(lp.abstract) AND lt.text_len = length(lp.abstract)
),
cr AS (
  SELECT lower(native_id) AS doi, max(title) AS title FROM SOURCE_TABLE
  WHERE provenance = 'crossref' AND title IS NOT NULL AND lower(native_id) IN (SELECT doi FROM r WHERE title IS NULL)
  GROUP BY 1
),
rp AS (
  SELECT native_id AS pmh, max(title) AS title FROM SOURCE_TABLE
  WHERE provenance IN ('repo', 'repo_backfill') AND title IS NOT NULL AND native_id IN (SELECT pmh FROM r WHERE title IS NULL)
  GROUP BY 1
),
t AS (
  SELECT r.provenance, r.native_id, r.abstract_hash, r.abstract, coalesce(r.title, cr.title, rp.title) AS title
  FROM r LEFT JOIN cr ON r.title IS NULL AND cr.doi = r.doi LEFT JOIN rp ON r.title IS NULL AND rp.pmh = r.pmh
),
w AS (
  SELECT *, filter(array_distinct(split(regexp_replace(lower(coalesce(title, '')), '[^\\p{L}\\p{N}]+', ' '), ' ')),
                   x -> length(x) >= 5) AS tw
  FROM t
),
h AS (SELECT *, size(filter(tw, x -> instr(lower(abstract), x) > 0)) AS hits FROM w)
SELECT provenance, native_id, abstract_hash, max(title) AS title, current_timestamp() AS built_at
FROM h WHERE hits >= 2 AND 2 * hits >= size(tw)
GROUP BY provenance, native_id, abstract_hash
""".replace("OWNER_TABLE", OWNER_TABLE).replace("TARGET_TABLE", TARGET_TABLE).replace("SOURCE_TABLE", SOURCE_TABLE))

# COMMAND ----------

display(spark.sql(f"""
SELECT reason, count(*) AS texts, sum(n_rows) AS records, max(filter_commit) AS filter_commit
FROM {TARGET_TABLE} GROUP BY reason ORDER BY 3 DESC"""))
display(spark.sql(f"SELECT provenance, count(*) AS owner_rows FROM {OWNER_TABLE} GROUP BY 1 ORDER BY 2 DESC"))
