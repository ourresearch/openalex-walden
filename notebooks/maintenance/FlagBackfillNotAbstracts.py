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
# MAGIC - **Short stand-ins** (oxjob #1800, 29 Oct 2026): a work whose backfill text (trimmed) is one of the judged
# MAGIC   short stand-in texts in `SHORT_TABLE` ("v", "PhD", "Mode of access: Internet.") is flagged with reason
# MAGIC   `short_stand_in`; FlagLocationNotAbstracts' header says how those texts were judged.
# MAGIC - **Peer reviews' borrowed abstracts** (oxjob #1790, 30 Oct 2026): a Crossref peer review whose backfill text is
# MAGIC   word for word the reviewed article's abstract is flagged with reason `peer_review_borrowed`. The work_ids are
# MAGIC   #1790's list `PEER_REVIEW_TABLE` (built once by #1790 `work/pr_borrowed_backfill_build.sql`; 64,769 on 30 Oct);
# MAGIC   this notebook copies them so a rebuild keeps them. Revert that part: drop the branch below.
# MAGIC - **Flagged PDF texts copied into the backfill** (oxjob #1800, 31 Oct 2026): works whose backfill text is word for
# MAGIC   word a PDF text flagged for the same work (`openalex.abstracts.not_abstract_pdf_texts`) kept showing that junk
# MAGIC   once CreateSuperLocations NULLed the PDF row (54,014 works after night 2026-10-31). Only works published before
# MAGIC   2010 passed the bar (old scanned issue PDFs: another article's text, page furniture; 0 real of 250 fresh), so
# MAGIC   #1800's list `PDF_BACKFILL_TABLE` (built once by #1800 `work/pdf3/backfill_same_write.sql`; 28,310 on 31 Oct)
# MAGIC   holds exactly those work_ids, reason `pdf_text`; this notebook copies them so a rebuild keeps them. Revert that
# MAGIC   part: drop the branch below.
# MAGIC - **Different-item pages** (oxjob #1790, 1 Nov 2026): a work whose stored landing page shows ANOTHER item (an
# MAGIC   OECD / UN chapter's DOI on its book's page, a neighbouring article, a data record's parent article) and whose
# MAGIC   backfill text is word for word that other item's abstract is flagged with reason `different_item`. The work_ids
# MAGIC   are #1790's list `DIFFERENT_ITEM_TABLE` (built once by #1790 `work/diff_item_backfill_build.sql` +
# MAGIC   `work/diff_item_publish.sql`; 10,251 on 1 Nov); this notebook copies them so a rebuild keeps them. Revert that
# MAGIC   part: drop the branch below.
# MAGIC - **Deposit notices** (oxjob #1800, 6 Nov 2026): a work whose backfill text is a DSpace deposit notice ("Submitted
# MAGIC   by NAME (email) on ... No. of bitstreams: 1 ... checksum: ... (MD5)") is flagged with reason `deposit_notice`,
# MAGIC   by the same rule `DEPOSIT_NOTICE` as FlagLocationNotAbstracts (its header says how it was checked).
# MAGIC - **Openings the landing page marks** (oxjob #2162, 11 Nov 2026): a work whose backfill text is word for word the
# MAGIC   opening its own landing page marks as such (Taylor & Francis books' product `textType` "first-paragraph",
# MAGIC   Cambridge Core's "extract" / "Extract"), which Parseland stopped emitting in parseland-lib #2162, is flagged with
# MAGIC   reason `page_marked_opening`. The work_ids are #2162's list `PAGE_OPENING_TABLE` (built once by #2162
# MAGIC   `work/opening_backfill_build.sql` after its re-parse wave); this notebook copies them so a rebuild keeps them.
# MAGIC   Revert that part: drop the branch below.
# MAGIC - **ACS page chrome** (oxjob #1432, 12 Nov 2026): a work whose backfill text is an old ACS Publications landing
# MAGIC   page's furniture and nothing else ("ADVERTISEMENT RETURN TO ISSUE ... Cite this: ... Article Views ... LEARN
# MAGIC   ABOUT THESE METRICS ... Get e-Alerts": title, authors, citation, metrics and menus, no abstract) is flagged with
# MAGIC   reason `page_chrome` by the SQL rule `ACS_CHROME` (634,659 works on 12 Nov; bar: Opus 5.5 read 100 of 100 random
# MAGIC   flagged texts as chrome only, #1432 README § Wave 3). Texts that say "abstract" anywhere but the export menu are
# MAGIC   left alone. Revert that part: drop the branch below.
# MAGIC - **Revert:** remove the anti-join from CreateWorksBase (one line), or `TRUNCATE` this table. Never
# MAGIC   DROP it while CreateWorksBase reads it: a missing table fails the nightly.
# MAGIC
# MAGIC MANUAL TRIGGER ONLY.

# COMMAND ----------

import hashlib
import os
import re

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

# a3e36f8 (26 Oct): adds Silverchair page furniture under 1,500 characters ("Search for other works by this author on
# ...", "Cite Icon Cite") and Frontiers headers, both from this backfill (#1857), plus #1794's meeting-abstract lines.
FILTER_COMMIT = "a3e36f8"
LIB_DIR = f"/Volumes/openalex/default/libraries/parseland_not_abstract/{FILTER_COMMIT}"
SHA256 = {
    "not_abstract.py": "2c30b1f05b641836d228c3af151b03aaf3cfd1713ad21629aab383670ed54f38",
    "data/not_abstracts.tsv": "3b8364f731da214c35fa0701c796f1ee7213593be6bceae42f2d2fa467115115",
}
SOURCE_TABLE = "openalex.abstracts.abstracts_backfill"
REPAIR_TABLE = "openalex.abstracts.abstracts_backfill_repair"
SHORT_TABLE = "openalex.abstracts.not_abstract_short_texts"  # oxjob #1800: judged short stand-in texts (trimmed)
PEER_REVIEW_TABLE = "openalex.abstracts.peer_review_borrowed_backfill"  # oxjob #1790: reviews showing the article's abstract
PDF_BACKFILL_TABLE = "openalex.abstracts.not_abstract_pdf_backfill"  # oxjob #1800: backfill copies of flagged PDF texts
DIFFERENT_ITEM_TABLE = "openalex.abstracts.different_item_backfill"  # oxjob #1790: works showing another item's abstract
PAGE_OPENING_TABLE = "openalex.abstracts.page_marked_opening_backfill"  # oxjob #2162: openings the page marks
# oxjob #1800 (6 Nov 2026): rule dspace-v1, a SQL predicate on a text column. Keep byte-identical with
# FlagLocationNotAbstracts.
DEPOSIT_NOTICE_COMMIT = "dspace-v1: DSpace deposit notice (#1800, 6 Nov 2026)"
DEPOSIT_NOTICE = r"""(
  trim(COL) RLIKE '^(Submitted by|Made available in DSpace on|Approved for entry into archive by|Rejected by|Item withdrawn by) '
  AND instr(COL, 'No. of bitstreams') > 0 AND instr(COL, '%%%%') = 0
  AND length(regexp_replace(trim(COL), '(?i)[^\\n]{0,200}?: \\d+ bytes, checksum: [0-9a-f]{32} \\((MD5|SHA-1|SHA1)\\)', ' ')) <= 150)"""
# oxjob #1432 (12 Nov 2026): rule acs-chrome-v1, a SQL predicate on a text column. Keep byte-identical with #1432
# work/acs_rule.sql.
ACS_CHROME_COMMIT = "oxjob1432-acs-chrome-v1"
ACS_CHROME = r"""(trim(COL) LIKE 'ADVERTISEMENT RETURN TO ISSUE%'
 AND instr(COL, 'LEARN ABOUT THESE METRICS') > 0
 AND instr(COL, 'Get e-Alerts') > 0
 AND length(trim(substring_index(COL, 'Get e-Alerts', -1))) = 0
 AND NOT replace(COL, 'Citation and abstract', '') RLIKE '(?i)abstract'
 AND length(COL) <= 6000)"""
# A dry run writes elsewhere first (job parameter target_table), to diff a new filter commit against the live table.
dbutils.widgets.text("target_table", "openalex.abstracts.abstracts_backfill_not_abstract")
TARGET_TABLE = dbutils.widgets.get("target_table")
# Longest list text is 1,835 characters; templates need <= 1,500 and wordings <= 200 after cleaning. Page furniture
# can sit anywhere, but texts over this length are left alone on purpose (they may be full text with a real abstract).
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
        # The meeting-abstract presenter line (#1794's template, built for landing pages) also opens real conference
        # abstracts in this backfill ("Presenter: Y. Muraki (...) Recent neutrino oscillation experiments limit ...":
        # 4 of 281 judged own, 26 Oct), so it doesn't count here.
        if presenter.match(plain):
            return None
        return "template"

    presenter = re.compile(r"(?:presenter|presenting author(?: contact details)?|author block|primary author)\s*:")
    return reason


# Driver self-test on its own instance: the UDF must not capture a loaded module (it doesn't pickle).
_r = _make_reason(SRC, TSV)
assert _r("No abstract available.") == "list"  # on the hand-read list, which is checked first
assert _r("No abstract is available for this paper.") == "wording"
assert _r("\u2014") == "empty"
assert _r("This datasheet on Mesorchis covers Identity.") == "template"
assert _r("Established in 1964, the IUCN Red List of Threatened Species has evolved to become the world’s most "
          "comprehensive information source on the global conservation status of animal, fungi and plant species.") == "list"
assert _r("We measured the effect of nitrogen on maize yield in 40 field trials and found a 12% increase.") is None
assert _r("Journal Article Yeardley Get access J. O. H.-P. Search for other works by this author on: Oxford Academic "
          "Google Scholar Notes and Queries, Volume s6-VIII, Issue 196, 6 October 1883, Page 268") == "template"
assert _r("EDITORIAL article Front. Neurol., 30 August 2023Sec. Epilepsy Volume 14 - 2023 | "
          "https://doi.org/10.3389/fneur.2023.1270702") == "template"
assert _r("Presenter: Nora Singh BA | Hartford Hospital") is None  # presenter lines don't count in the backfill
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
  work_id BIGINT, reason STRING, text_len INT, filter_commit STRING, built_at TIMESTAMP
) COMMENT 'oxjob #1800: work_ids whose abstracts_backfill text (after the #807 repair overlay) is not an abstract per parseland-lib not_abstract.is_not_abstract; CreateWorksBase anti-joins it. Built by notebooks/maintenance/FlagBackfillNotAbstracts.'
""")

eff = spark.sql(f"""
SELECT bf.work_id,
       CASE WHEN r.work_id IS NOT NULL THEN r.abstract ELSE bf.abstract END AS abstract
FROM {SOURCE_TABLE} bf LEFT JOIN {REPAIR_TABLE} r ON bf.work_id = r.work_id
""").where(F.col("abstract").isNotNull() & (F.length("abstract") <= MAX_LEN))

flagged = (eff.withColumn("reason", not_abstract_reason("abstract"))
              .where(F.col("reason").isNotNull()))
flagged.createOrReplaceTempView("flagged_backfill")
eff.createOrReplaceTempView("eff_backfill")

# Owner guard (list texts only). A list text is shared by 100+ works, so it is nobody's abstract... except the one
# item it describes: a book's blurb on its own record (it sits on every chapter too), or the paper behind a "featured
# abstract" a site shows on all its articles. Keep a list row out when the work's own title words appear in the
# text: >= 2 title words of 5+ letters, covering at least half of them (25 Oct 2026: 7,621 of 601,218 list rows, e.g.
# "Drugs Handbook 2012-2013", "Shakespeare and Animals"; chapters like "Oxybuprocaine." never match). Templates
# carry the title by construction, so the guard doesn't apply to them.
# shortcut: word overlap, not meaning; a series blurb that repeats a series title is also kept out (it stays junk).
spark.sql(r"""
INSERT OVERWRITE TARGET_TABLE
SELECT work_id, reason, text_len, filter_commit, built_at FROM (
  SELECT f.work_id, f.reason, length(f.abstract) AS text_len, 'FILTER_COMMIT' AS filter_commit, current_timestamp() AS built_at,
         size(coalesce(w.tw, array())) AS n_title,
         size(filter(coalesce(w.tw, array()), x -> instr(lower(f.abstract), x) > 0)) AS n_hit
  FROM flagged_backfill f
  LEFT JOIN (SELECT id, filter(array_distinct(split(regexp_replace(lower(title), '[^\\p{L}\\p{N}]+', ' '), ' ')), x -> length(x) >= 5) AS tw
             FROM openalex.works.openalex_works) w ON w.id = f.work_id)
WHERE NOT (reason = 'list' AND n_hit >= 2 AND n_hit * 2 >= n_title)
UNION ALL
-- Short stand-ins (see the header).
SELECT e.work_id, 'short_stand_in' AS reason, length(e.abstract) AS text_len, st.rubric AS filter_commit,
       current_timestamp() AS built_at
FROM eff_backfill e JOIN SHORT_TABLE st ON st.text = trim(e.abstract)
LEFT ANTI JOIN flagged_backfill f ON f.work_id = e.work_id
UNION ALL
-- Peer reviews' borrowed abstracts (see the header). One row per work: skip works another branch already flags.
SELECT p.work_id, 'peer_review_borrowed' AS reason, p.text_len, p.label_version AS filter_commit, current_timestamp() AS built_at
FROM PEER_REVIEW_TABLE p
LEFT ANTI JOIN flagged_backfill f ON f.work_id = p.work_id
LEFT ANTI JOIN (SELECT e.work_id FROM eff_backfill e JOIN SHORT_TABLE st ON st.text = trim(e.abstract)) s ON s.work_id = p.work_id
UNION ALL
-- Backfill copies of flagged PDF texts (see the header). One row per work: skip works another branch already flags.
SELECT d.work_id, 'pdf_text' AS reason, d.text_len, d.label_version AS filter_commit, current_timestamp() AS built_at
FROM PDF_BACKFILL_TABLE d
LEFT ANTI JOIN flagged_backfill f ON f.work_id = d.work_id
LEFT ANTI JOIN (SELECT e.work_id FROM eff_backfill e JOIN SHORT_TABLE st ON st.text = trim(e.abstract)) s ON s.work_id = d.work_id
LEFT ANTI JOIN PEER_REVIEW_TABLE p ON p.work_id = d.work_id
UNION ALL
-- Different-item pages (see the header). One row per work: skip works another branch already flags.
SELECT di.work_id, 'different_item' AS reason, di.text_len, di.label_version AS filter_commit, current_timestamp() AS built_at
FROM DIFFERENT_ITEM_TABLE di
LEFT ANTI JOIN flagged_backfill f ON f.work_id = di.work_id
LEFT ANTI JOIN (SELECT e.work_id FROM eff_backfill e JOIN SHORT_TABLE st ON st.text = trim(e.abstract)) s ON s.work_id = di.work_id
LEFT ANTI JOIN PEER_REVIEW_TABLE p ON p.work_id = di.work_id
LEFT ANTI JOIN PDF_BACKFILL_TABLE d ON d.work_id = di.work_id
UNION ALL
-- Openings the landing page marks (see the header). One row per work: skip works another branch already flags.
SELECT po.work_id, 'page_marked_opening' AS reason, po.text_len, po.label_version AS filter_commit, current_timestamp() AS built_at
FROM PAGE_OPENING_TABLE po
LEFT ANTI JOIN flagged_backfill f ON f.work_id = po.work_id
LEFT ANTI JOIN (SELECT e.work_id FROM eff_backfill e JOIN SHORT_TABLE st ON st.text = trim(e.abstract)) s ON s.work_id = po.work_id
LEFT ANTI JOIN PEER_REVIEW_TABLE p ON p.work_id = po.work_id
LEFT ANTI JOIN PDF_BACKFILL_TABLE d ON d.work_id = po.work_id
LEFT ANTI JOIN DIFFERENT_ITEM_TABLE di ON di.work_id = po.work_id
UNION ALL
-- Deposit notices (see the header), on the uncapped backfill text. One row per work: skip works another branch flags.
SELECT e.work_id, 'deposit_notice' AS reason, length(e.abstract) AS text_len, 'DEPOSIT_NOTICE_COMMIT' AS filter_commit,
       current_timestamp() AS built_at
FROM (SELECT bf.work_id, CASE WHEN r.work_id IS NOT NULL THEN r.abstract ELSE bf.abstract END AS abstract
      FROM SOURCE_TABLE bf LEFT JOIN REPAIR_TABLE r ON bf.work_id = r.work_id) e
LEFT ANTI JOIN flagged_backfill f ON f.work_id = e.work_id
LEFT ANTI JOIN (SELECT e.work_id FROM eff_backfill e JOIN SHORT_TABLE st ON st.text = trim(e.abstract)) s ON s.work_id = e.work_id
LEFT ANTI JOIN PEER_REVIEW_TABLE p ON p.work_id = e.work_id
LEFT ANTI JOIN PDF_BACKFILL_TABLE d ON d.work_id = e.work_id
LEFT ANTI JOIN DIFFERENT_ITEM_TABLE di ON di.work_id = e.work_id
LEFT ANTI JOIN PAGE_OPENING_TABLE po ON po.work_id = e.work_id
WHERE e.abstract IS NOT NULL AND DEPOSIT_NOTICE_SQL
UNION ALL
-- ACS page chrome (see the header). One row per work: skip works another branch flags (a deposit notice can't match).
SELECT e.work_id, 'page_chrome' AS reason, length(e.abstract) AS text_len, 'ACS_CHROME_COMMIT' AS filter_commit,
       current_timestamp() AS built_at
FROM eff_backfill e
LEFT ANTI JOIN flagged_backfill f ON f.work_id = e.work_id
LEFT ANTI JOIN (SELECT e.work_id FROM eff_backfill e JOIN SHORT_TABLE st ON st.text = trim(e.abstract)) s ON s.work_id = e.work_id
LEFT ANTI JOIN PEER_REVIEW_TABLE p ON p.work_id = e.work_id
LEFT ANTI JOIN PDF_BACKFILL_TABLE d ON d.work_id = e.work_id
LEFT ANTI JOIN DIFFERENT_ITEM_TABLE di ON di.work_id = e.work_id
LEFT ANTI JOIN PAGE_OPENING_TABLE po ON po.work_id = e.work_id
WHERE ACS_CHROME_SQL
""".replace("TARGET_TABLE", TARGET_TABLE).replace("FILTER_COMMIT", FILTER_COMMIT).replace("SHORT_TABLE", SHORT_TABLE)
   .replace("PEER_REVIEW_TABLE", PEER_REVIEW_TABLE).replace("PDF_BACKFILL_TABLE", PDF_BACKFILL_TABLE)
   .replace("DIFFERENT_ITEM_TABLE", DIFFERENT_ITEM_TABLE).replace("PAGE_OPENING_TABLE", PAGE_OPENING_TABLE)
   .replace("SOURCE_TABLE", SOURCE_TABLE)
   .replace("REPAIR_TABLE", REPAIR_TABLE).replace("DEPOSIT_NOTICE_COMMIT", DEPOSIT_NOTICE_COMMIT)
   .replace("DEPOSIT_NOTICE_SQL", DEPOSIT_NOTICE.replace("COL", "e.abstract"))
   .replace("ACS_CHROME_SQL", ACS_CHROME.replace("COL", "e.abstract")).replace("ACS_CHROME_COMMIT", ACS_CHROME_COMMIT))
# INSERT OVERWRITE replaces the rows atomically: CreateWorksBase never sees the table missing or half-built.

# COMMAND ----------

display(spark.sql(f"SELECT reason, count(*) AS work_ids, max(filter_commit) AS filter_commit FROM {TARGET_TABLE} GROUP BY reason ORDER BY 2 DESC"))
