# Databricks notebook source
# MAGIC %md
# MAGIC # MergeIdenticalKeyWorks — collapse works that share a byte-identical `title_author` key (oxjob #1256 step four)
# MAGIC
# MAGIC The identical-key defect class: a `title_author` key (> 20 chars) carried by the pinned records of 2–3
# MAGIC works where at most ONE of them carries a DOI. The keyless side(s) minted as fresh works because the
# MAGIC map did not hold the key for the DOI-bearing work (leak closed 2026-09-22, walden `3631b877`; stock
# MAGIC 3,985,433 keys / 4,084,352 excess works on 2026-09-24, 86 % uncited, 77 % repo-only). The two-DOI
# MAGIC groups are #880's policy class (versioned DOIs, same-titled items) and are NOT in scope.
# MAGIC
# MAGIC **Winner** = the DOI-bearing work, else the lowest id. **Losers** = the rest. The merge is the
# MAGIC registries' sanctioned correction path (`RepointWorkIds` header): DELETE the loser's registry pins and
# MAGIC its `work_id_map` rows, then let the nightly `MapWorkIds` re-resolve the loser's records — keyless, so
# MAGIC the title tier binds them to the winner (after the delete the key's only id is the winner). The loser
# MAGIC ends the night with no pins → `TrackDeletedWorks` ledgers it (404, deleted_ids.csv, ES delete). The
# MAGIC morning after, `repoint_citations` moves `work_references.cited_work_id` loser → winner by id pair.
# MAGIC
# MAGIC **Rule (from a 1,570-pair blind-labelled sample, 2026-09-24; precision weighted by stratum size):** a pair
# MAGIC merges when the two records agree on a known publication year (tier 1: full normalized titles identical,
# MAGIC ~100 %; tier 2: titles differ beyond the key but share >= `title_jaccard_min` of their tokens, ~99 %), volume /
# MAGIC first page do not contradict, and none of the failure shapes the sample found applies: same primary source
# MAGIC (letters and serial items sharing a page: 77 % same), preprint / conference / thesis / report vs article
# MAGIC (a version), a book vs its chapter or a review of it, a junk-typed side, two undated books. Tier 3 (a year
# MAGIC missing, 94 %) and tier 4 (years one apart, 84 %, mostly preprints becoming articles) are staged but off by
# MAGIC default (`tiers`). No size or citation caps: every cited loser is listed in `<target>_cited_review` for the
# MAGIC record; `chained` (the loser is a winner of another key) is a mechanical hold for the second pass.
# MAGIC
# MAGIC **Waves** are assigned per KEY (all losers of a key in one wave, so MIN(id) over the key is the winner
# MAGIC the next morning), uncited keys first, `wave_size` losers per wave. Nightly budget: TrackDeletedWorks
# MAGIC refuses > 0.5 % of live works deleted (~2.38M on 2026-09-24) and Guardrails > 7.5M stamps (losers +
# MAGIC winners + normal churn) → default 1,500,000.
# MAGIC
# MAGIC Modes (all keyed on `target_table`, one row per loser):
# MAGIC - `stage`     build the class from `locations_mapped` + `openalex_works`, apply holds, assign waves.
# MAGIC - `dry_run`   `wave = N`: losers, keys, winners, pins + map rows to delete, cited losers; samples.
# MAGIC - `execute`   `confirm = yes`, `wave = N`, outside 04:00-08:00 UTC, never while End 2 End runs: freeze
# MAGIC               `<target>_wave<N>_audit` (the pins and map rows deleted, in full), the two DELETEs, stamp.
# MAGIC               Undo: re-INSERT the audit rows (pins + map rows) — the nightly has not run yet.
# MAGIC - `verify`    the morning after: every audited pin re-pinned; landed on the winner / elsewhere / NULL;
# MAGIC               losers with 0 pins; losers ledgered.
# MAGIC
# MAGIC **`class_mode = exact_signature`** (2026-09-26): pairs whose title keys DIFFER but whose records agree exactly on
# MAGIC normalized title + year + abstract / author list / page fingerprint / arXiv id (see `exact_signature_class_sql`).
# MAGIC The unpin alone would make the loser's records mint (their keys are not the winner's), so `execute` also writes
# MAGIC `<target>_wave<N>_aliases` and inserts those keys into `work_id_map` with the winner's id.
# MAGIC
# MAGIC **`class_mode = feed_twin`** (oxjob #1427, 2026-09-29): an OJS journal's own OAI feed record and the journal's Crossref
# MAGIC record of the same article on two works. Scope = the feed endpoints `feed_scope_sql` returns (default: the
# MAGIC `is_journal_host` endpoints). The feed record's twin is the Crossref record with its DOI, else the one whose
# MAGIC `resource.primary.URL` is its `/article/view/<N>` page (key claimed by one DOI, same year, host not on the
# MAGIC renumbered-host blocklist, as MapWorkIds' url tier), else the one with its normalized title (>= 40 chars) + year
# MAGIC (one Crossref work). Winner = the Crossref record's work; loser = the feed record's work. 98 % of these pairs are
# MAGIC legacy pins MapWorkIds never re-resolves. Held: `default_oai_host` (native_id collisions, #1407),
# MAGIC `loser_has_primary` (the loser carries its own Crossref/DataCite record: that needs a record move, not a merge),
# MAGIC `year_differs` (the two works' publication years disagree: reprints, later editions, mis-pinned annual reports) and
# MAGIC `loser_mixed` (the loser also carries non-repo records, e.g. a MAG-era book the feed's book review was pinned onto;
# MAGIC the 2026-10-02 stage's wrong merges all sat in these two classes: 2,361 losers, 4 % of the wave, half its citations),
# MAGIC `winner_junk` (the Crossref winner has no title or is typed paratext: econjournals' 10.32479 stubs, 791 pairs carrying
# MAGIC 74 % of wave 2's citations on 2026-10-03; the feed side holds the real metadata, so a merge would bury it),
# MAGIC `winner_preprint` (the twin is an SSRN-style preprint record; the published side must survive, charter policy),
# MAGIC `title_type_differs` (a title twin whose two works carry different types: a book or chapter against an article with a
# MAGIC generic title was wrong in 5 of 15 sampled on 2026-10-03; same-type title twins were 25/25 the same article),
# MAGIC `junk_type` on the title twin, and the mechanical holds. **`class_mode = legacy_twin`** (oxjob #1427, 2026-10-05): a DOI-less
# MAGIC feed-only work beside a MAG-only legacy work on the same primary source with the same normalised title; the legacy work
# MAGIC wins (it carries the citations) and the feed record is re-keyed onto it; holds `not_one_to_one`, `title_short`, `year_differs`,
# MAGIC `type_differs`, `junk_type` and the mechanical ones. Keys differ (translated titles), so execute re-keys the
# MAGIC loser's record keys onto the winner like exact_signature; `ta` = 'twin:<winner id>'.
# MAGIC
# MAGIC **`class_mode = zenodo_twin`** (oxjob #1540, 2026-10-05): Zenodo registers a concept DOI `10.5281/zenodo.N` and a version DOI per
# MAGIC deposit, both as DataCite records with the same metadata; walden mints a work for each (OJS journals that DOI through Zenodo:
# MAGIC ~150K pairs on `ojs_coverage` sources; 6.9M corpus-wide, 2026-10-05 sizing in oxjobs #1427 `work/q42`). Every InvenioRDM
# MAGIC instance does the same (KTH Data Repository 10.71775, ZD 25672), so since 2026-10-06 the class takes any DataCite prefix except
# MAGIC arXiv (10.48550, the declared_version class). The pair is the version
# MAGIC record's own declaration: `ids[]` carries `IsVersionOf` exactly one concept DOI, and that concept DOI is a DataCite record on
# MAGIC exactly one other live work. **Winner = the version DOI's work** (what the article page prints and what the journal's feed record
# MAGIC attached to; Casey 2026-10-05), loser = the concept work; the concept DOI becomes an alias key on the winner (execute re-keys
# MAGIC like exact_signature; `ta` = 'zen:<winner id>'). Scope = `twin_scope_sql` (source ids; a pair is in scope when either side's
# MAGIC primary source is; empty = corpus-wide). Held: `multi_winner` (a concept with several versions: software / dataset releases,
# MAGIC re-deposited articles, not duplicates), `title_differs`, `year_differs`, `author_differs` (first authors share no name token:
# MAGIC 2 of 25 in the 2026-10-06 non-Zenodo blind sample were different papers under one title), `loser_has_primary` (the concept work also carries a
# MAGIC Crossref record or another DataCite record), `winner_junk`, `dataset_software` (either side typed dataset / software / supplementary-materials: a separate
# MAGIC decision), and the mechanical holds.
# MAGIC
# MAGIC **`class_mode = version_suffix`** (oxjob #1581, 2026-10-07): Figshare (10.6084 and the institutional portals 10.25384,
# MAGIC 10.25375, 10.25446, ...) and a few other DataCite registrants mint a version DOI as the concept DOI plus `.vN`
# MAGIC (`10.6084/m9.figshare.123` and `10.6084/m9.figshare.123.v1`) without declaring `IsVersionOf`, so `zenodo_twin` never
# MAGIC sees them (174 of 2.16M Figshare pairs staged there). The pair is a live DataCite record whose DOI ends `.v<N>` and the
# MAGIC DataCite record holding the DOI without the suffix on exactly one other live work. Same winner rule, holds and execute
# MAGIC re-keying as `zenodo_twin` (winner = the version work; `ta` = 'vsx:<winner id>'). Sized 2026-10-07 (oxjob #1577): ~2.9M
# MAGIC version works with a live concept twin; the 184 such pairs in the #1577 sample were 168/168 the same item to Opus 5.5.
# MAGIC - `repoint_citations`  `wave = N`, `confirm = yes`, after `verify` is clean: `<target>_wave<N>_refs_audit`
# MAGIC               (before-image, both sides) then UPDATE `work_references`: `cited_work_id` loser → winner (citations
# MAGIC               TO the loser) and `citing_work_id` loser → winner (the loser's OWN reference list comes along; rows keep
# MAGIC               their location + ref_ind so they never collide with the winner's).

# COMMAND ----------

dbutils.widgets.dropdown("mode", "stage", ["stage", "dry_run", "execute", "verify", "repoint_citations", "reexecute_resurrected", "record_merges", "follow_null_to_winner", "repoint_arrays", "release_held", "rehold_list"])
dbutils.widgets.text("target_table", "openalex.works.oxjob1256_identical_key_merge_target")
dbutils.widgets.text("wave_size", "1500000")
dbutils.widgets.text("wave", "1")
dbutils.widgets.dropdown("class_mode", "title_key", ["title_key", "same_doi", "exact_signature", "declared_version", "feed_twin", "zenodo_twin", "legacy_twin", "version_suffix"])
dbutils.widgets.text("feed_scope_sql", "SELECT endpoint_id FROM openalex.sources.endpoint_to_source WHERE is_journal_host")
dbutils.widgets.text("twin_scope_sql", "SELECT source_id FROM openalex_dev.sources.ojs_coverage")
dbutils.widgets.text("tiers", "1,2")
dbutils.widgets.text("title_jaccard_min", "0.9")
dbutils.widgets.text("abstract_jaccard_min", "0.6")
dbutils.widgets.text("preprint_is_same", "no")
dbutils.widgets.text("prior_targets", "")
dbutils.widgets.text("hold_cited_over", "")
dbutils.widgets.text("release_hold", "")
dbutils.widgets.text("keep_held_losers", "")
dbutils.widgets.text("release_list", "")
dbutils.widgets.text("confirm", "no")

MODE = dbutils.widgets.get("mode")
TARGET = dbutils.widgets.get("target_table")
WAVE_SIZE = int(dbutils.widgets.get("wave_size"))
WAVE = int(dbutils.widgets.get("wave"))
CLASS_MODE = dbutils.widgets.get("class_mode")
TIERS = {int(t) for t in dbutils.widgets.get("tiers").split(",") if t.strip()}
TITLE_JACCARD_MIN = float(dbutils.widgets.get("title_jaccard_min"))
ABSTRACT_JACCARD_MIN = float(dbutils.widgets.get("abstract_jaccard_min"))
PREPRINT_IS_SAME = dbutils.widgets.get("preprint_is_same") == "yes"
# feed_twin: the OAI feed endpoints in scope (a query returning endpoint_id)
FEED_SCOPE_SQL = dbutils.widgets.get("feed_scope_sql").strip()
# zenodo_twin / version_suffix: the sources in scope (a query returning source_id BIGINT); empty = corpus-wide
TWIN_SCOPE_SQL = dbutils.widgets.get("twin_scope_sql").strip()
# earlier targets whose executed losers must not be staged again (locations_mapped still shows them until the nightly rebuild)
PRIOR_TARGETS = [t.strip() for t in dbutils.widgets.get("prior_targets").split(",") if t.strip()]
# losers cited at least this often are held for a labelled review instead of merging (empty = no cap)
RELEASE_HOLD = dbutils.widgets.get("release_hold").strip()
# CSV (loser_work_id, winner_work_id, ...) of reviewed pairs to release regardless of their hold, e.g. a Jev-gated and blind-confirmed list
RELEASE_LIST = dbutils.widgets.get("release_list").strip()
# comma-separated loser ids that stay held when release_held runs (the labelled review's rejects)
KEEP_HELD = [int(x) for x in dbutils.widgets.get("keep_held_losers").replace(" ", "").split(",") if x]
HOLD_CITED_OVER = int(dbutils.widgets.get("hold_cited_over")) if dbutils.widgets.get("hold_cited_over").strip() else None
CONFIRM = dbutils.widgets.get("confirm") == "yes"
AUDIT = f"{TARGET}_wave{WAVE}_audit"
REFS_AUDIT = f"{TARGET}_wave{WAVE}_refs_audit"
REVIEW = f"{TARGET}_cited_review"

LM = "openalex.works.locations_mapped"
REGISTRY = "openalex.works.location_work_ids"
MAP = "openalex.works.work_id_map"
WORKS = "openalex.works.openalex_works"
REFS = "openalex.works.work_references"
LEDGER = "openalex.works.deleted_works"
END2END_JOB_ID = 616701029470182
PUBLISHED_TYPES = "('article', 'conference-paper', 'book-chapter')"   # the published side of a preprint pair (Casey 2026-09-28)
MERGED = "openalex.works.merged_work_ids"   # durable loser -> winner record; MapWorkIds redirects legacy adoption through it
CROSSREF_RAW = "openalex.crossref.crossref_deduplicated"   # feed_twin: resource.primary.URL never reaches locations_mapped

import datetime, json, time

SUMMARY = {"mode": MODE, "wave": WAVE}


def note(**kw):
    """Serverless notebook tasks return no stdout through the API; everything printed is also returned via notebook.exit."""
    SUMMARY.update(kw)
    print(kw)

print(dict(mode=MODE, class_mode=CLASS_MODE, target=TARGET, wave_size=WAVE_SIZE, wave=WAVE, tiers=sorted(TIERS), title_jaccard_min=TITLE_JACCARD_MIN,
           abstract_jaccard_min=ABSTRACT_JACCARD_MIN, preprint_is_same=PREPRINT_IS_SAME, prior_targets=PRIOR_TARGETS,
           hold_cited_over=HOLD_CITED_OVER, confirm=CONFIRM))


def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def one(sql):
    return rows(sql)[0]


def end2end_active():
    from databricks.sdk import WorkspaceClient
    return [r.run_id for r in WorkspaceClient().jobs.list_runs(job_id=END2END_JOB_ID, active_only=True)]


def class_sql():
    """One row per loser with its winner, the evidence features, the tier, and the hold (2026-09-24 labelled sample, oxjob #1256)."""
    if CLASS_MODE == "same_doi":
        return same_doi_class_sql()
    if CLASS_MODE == "exact_signature":
        return exact_signature_class_sql()
    if CLASS_MODE == "declared_version":
        return declared_version_class_sql()
    if CLASS_MODE == "feed_twin":
        return feed_twin_class_sql()
    if CLASS_MODE == "zenodo_twin":
        return zenodo_twin_class_sql()
    if CLASS_MODE == "legacy_twin":
        return legacy_twin_class_sql()
    if CLASS_MODE == "version_suffix":
        return version_suffix_class_sql()
    return f"""
    WITH k AS (
      SELECT merge_key.title_author AS ta, work_id,
             MAX(NULLIF(merge_key.doi, '') IS NOT NULL) AS has_doi,
             MAX(CASE WHEN provenance NOT IN ('repo', 'repo_backfill') THEN 1 ELSE 0 END) = 0 AS repo_only,
             COUNT(*) AS n_locations
      FROM {LM}
      WHERE work_id IS NOT NULL AND merge_key.title_author IS NOT NULL AND LENGTH(merge_key.title_author) > 20
        {prior_exclusion()}
      GROUP BY 1, 2
    ),
    g AS (
      SELECT ta, COUNT(*) AS n_ids, SUM(CASE WHEN has_doi THEN 1 ELSE 0 END) AS n_doi
      FROM k GROUP BY ta HAVING COUNT(*) BETWEEN 2 AND 3
    ),
    w AS (
      SELECT id, publication_year AS yr, type, title,
             regexp_replace(lower(title), '[^a-z0-9]', '') AS title_norm,
             array_distinct(filter(split(regexp_replace(lower(title), '[^a-z0-9 ]', ' '), ' +'), x -> x <> '')) AS title_tokens,
             primary_location.source.id AS src, biblio.volume AS vol, biblio.first_page AS fp,
             COALESCE(cited_by_count, 0) AS cites,
             array_distinct(filter(split(regexp_replace(lower(COALESCE(abstract, '')), '[^a-z0-9 ]', ' '), ' +'), x -> length(x) > 3)) AS abs_tokens
      FROM {WORKS}
    ),
    {title_key_sides()},
    winners AS (SELECT * FROM sides WHERE rn = 1),
    pairs AS (
      SELECT s.ta, s.n_ids, s.n_doi, x.work_id AS winner_work_id, x.yr AS winner_yr, x.has_doi AS winner_has_doi, x.type AS winner_type,
             s.work_id AS loser_work_id, s.yr AS loser_yr, s.cites AS loser_cites, s.repo_only AS loser_repo_only,
             s.n_locations AS loser_locations, LENGTH(s.ta) AS key_len, s.type AS loser_type,
             CASE WHEN s.yr IS NULL OR x.yr IS NULL THEN 'unknown' WHEN s.yr = x.yr THEN 'same'
                  WHEN ABS(s.yr - x.yr) = 1 THEN 'pm1' ELSE 'gt1' END AS year_cls,
             (s.title_norm = x.title_norm) AS full_title_same,
             CASE WHEN size(array_union(s.title_tokens, x.title_tokens)) = 0 THEN 0.0
                  ELSE size(array_intersect(s.title_tokens, x.title_tokens)) / size(array_union(s.title_tokens, x.title_tokens)) END AS title_jaccard,
             (s.vol IS NOT NULL AND x.vol IS NOT NULL AND (s.vol <> x.vol OR (s.fp IS NOT NULL AND x.fp IS NOT NULL AND s.fp <> x.fp))) AS biblio_differs,
             (s.src IS NOT NULL AND s.src = x.src) AS src_same,
             CASE WHEN size(s.abs_tokens) >= 20 AND size(x.abs_tokens) >= 20
                  THEN size(array_intersect(s.abs_tokens, x.abs_tokens)) / size(array_union(s.abs_tokens, x.abs_tokens)) END AS abs_jaccard
      FROM sides s JOIN winners x ON x.ta = s.ta WHERE s.rn > 1
    ),
    tiered AS (
      SELECT p.*,
             CASE WHEN p.biblio_differs OR p.year_cls = 'gt1' THEN NULL
                  WHEN p.year_cls = 'same' AND p.full_title_same THEN 1
                  WHEN p.year_cls = 'same' THEN 2
                  WHEN p.year_cls = 'unknown' THEN 3
                  WHEN p.year_cls = 'pm1' THEN 4 END AS tier,
             -- the failure shapes the labelled sample found: same venue (letters / serial items on one page),
             -- preprint-or-conference-vs-article (a version, not a duplicate), a book vs its chapter or a review of it
             CASE WHEN p.loser_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other')
                    OR p.winner_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other') THEN 'junk_type'
{preprint_pair_not_excluded('p')}                  WHEN (p.loser_type, p.winner_type) IN (('preprint', 'article'), ('article', 'preprint'), ('conference-paper', 'article'), ('article', 'conference-paper'),
                                                         ('dissertation', 'article'), ('article', 'dissertation'), ('report', 'article'), ('article', 'report')) THEN 'version_types'
                  WHEN (p.loser_type = 'book' AND p.winner_type IN ('book-chapter', 'article')) OR (p.winner_type = 'book' AND p.loser_type IN ('book-chapter', 'article')) THEN 'book_vs_part'
                  WHEN p.src_same THEN 'same_source'
                  WHEN p.loser_type = 'book' AND p.winner_type = 'book' AND p.year_cls = 'unknown' THEN 'undated_books' END AS exclusion
      FROM pairs p
    ),
    held AS (
      SELECT t.*,
             CASE WHEN t.exclusion IS NOT NULL THEN t.exclusion
                  WHEN t.tier IS NULL THEN CASE WHEN t.biblio_differs THEN 'biblio_differs' ELSE 'year_gap' END
                  WHEN t.tier NOT IN ({', '.join(str(x) for x in sorted(TIERS)) or 'NULL'}) THEN CONCAT('tier_', t.tier, '_off')
                  WHEN t.tier = 2 AND t.title_jaccard < {TITLE_JACCARD_MIN} THEN 'title_too_different'
                  END AS base_hold
      FROM tiered t
    )
    -- abstract rescue (2026-09-24 labelled sample, 1,103 held pairs): where both records carry an abstract and the
    -- token sets agree (Jaccard >= abstract_jaccard_min) the pair is the same work in every bucket below (42/42 each);
    -- version-type and +-1-year pairs are the same MANUSCRIPT or its preprint, so they join only under preprint_is_same
    SELECT h.*,
           CASE WHEN h.base_hold IN ('same_source', 'year_gap', 'tier_3_off', 'title_too_different') AND h.abs_jaccard >= {ABSTRACT_JACCARD_MIN} THEN 'abstract'
                WHEN h.base_hold IN ('version_types', 'tier_4_off') AND h.abs_jaccard >= {ABSTRACT_JACCARD_MIN} AND {preprint_rescue_pred('h')} THEN 'abstract_preprint'
                END AS rescue,
           CASE WHEN h.base_hold IN ('same_source', 'year_gap', 'tier_3_off', 'title_too_different') AND h.abs_jaccard >= {ABSTRACT_JACCARD_MIN} THEN NULL
                WHEN h.base_hold IN ('version_types', 'tier_4_off') AND h.abs_jaccard >= {ABSTRACT_JACCARD_MIN} AND {preprint_rescue_pred('h')} THEN NULL
                WHEN h.base_hold IS NOT NULL THEN h.base_hold
                WHEN EXISTS (SELECT 1 FROM winners x WHERE x.work_id = h.loser_work_id) THEN 'chained'
                END AS hold_reason
    FROM held h
    """


def same_doi_class_sql():
    """Same-DOI class: 2-3 live works whose pinned records carry one cleaned DOI. Winner = the work with a Crossref pin
    for it, else the lowest id (the DOI seed already made MIN(id) over the DOI the anchor). The loser's records re-resolve
    on the DOI tier. Tier 1: titles agree (Jaccard >= title_jaccard_min); tier 2: titles differ but abstracts agree. Held:
    year gap > 1 (`year_gap`), titles differ with disagreeing abstracts (`doi_misassigned`: the Spiegelhalter-discussion
    shape) or with no abstract (`title_differs_no_abstract`); junk / book-vs-part types as in the title class."""
    clean = "regexp_replace({c}, '[^a-zA-Z0-9\\./-]', '')"
    return f"""
    WITH k AS (
      SELECT lower(NULLIF({clean.format(c='merge_key.doi')}, '')) AS ta, work_id,
             MAX(CASE WHEN provenance = 'crossref' THEN 1 ELSE 0 END) = 1 AS has_doi,
             MAX(CASE WHEN provenance NOT IN ('repo', 'repo_backfill') THEN 1 ELSE 0 END) = 0 AS repo_only,
             COUNT(*) AS n_locations
      FROM {LM}
      WHERE work_id IS NOT NULL AND NULLIF(merge_key.doi, '') IS NOT NULL
        {prior_exclusion()}
      GROUP BY 1, 2
    ),
    live AS (SELECT DISTINCT work_id FROM {REGISTRY} WHERE work_id IS NOT NULL),
    kl AS (SELECT k.* FROM k JOIN live l ON l.work_id = k.work_id),
    g AS (SELECT ta, COUNT(*) AS n_ids, SUM(CASE WHEN has_doi THEN 1 ELSE 0 END) AS n_doi FROM kl GROUP BY ta HAVING COUNT(*) BETWEEN 2 AND 3),
    w AS (
      SELECT id, publication_year AS yr, type, title,
             regexp_replace(lower(title), '[^a-z0-9]', '') AS title_norm,
             array_distinct(filter(split(regexp_replace(lower(title), '[^a-z0-9 ]', ' '), ' +'), x -> x <> '')) AS title_tokens,
             primary_location.source.id AS src, biblio.volume AS vol, biblio.first_page AS fp,
             COALESCE(cited_by_count, 0) AS cites,
             array_distinct(filter(split(regexp_replace(lower(COALESCE(abstract, '')), '[^a-z0-9 ]', ' '), ' +'), x -> length(x) > 3)) AS abs_tokens
      FROM {WORKS}
    ),
    sides AS (
      SELECT kl.ta, g.n_ids, g.n_doi, kl.work_id, kl.has_doi, kl.repo_only, kl.n_locations, w.yr, w.type, w.title_norm, w.title_tokens, w.src, w.vol, w.fp, w.cites, w.abs_tokens,
             ROW_NUMBER() OVER (PARTITION BY kl.ta ORDER BY kl.has_doi DESC, kl.work_id) AS rn
      FROM g JOIN kl ON kl.ta = g.ta LEFT JOIN w ON w.id = kl.work_id
    ),
    winners AS (SELECT * FROM sides WHERE rn = 1),
    pairs AS (
      SELECT s.ta, s.n_ids, s.n_doi, x.work_id AS winner_work_id, x.yr AS winner_yr, x.has_doi AS winner_has_doi, x.type AS winner_type,
             s.work_id AS loser_work_id, s.yr AS loser_yr, s.cites AS loser_cites, s.repo_only AS loser_repo_only,
             s.n_locations AS loser_locations, LENGTH(s.ta) AS key_len, s.type AS loser_type,
             CASE WHEN s.yr IS NULL OR x.yr IS NULL THEN 'unknown' WHEN s.yr = x.yr THEN 'same'
                  WHEN ABS(s.yr - x.yr) = 1 THEN 'pm1' ELSE 'gt1' END AS year_cls,
             (s.title_norm = x.title_norm) AS full_title_same,
             CASE WHEN size(array_union(s.title_tokens, x.title_tokens)) = 0 THEN 0.0
                  ELSE size(array_intersect(s.title_tokens, x.title_tokens)) / size(array_union(s.title_tokens, x.title_tokens)) END AS title_jaccard,
             (s.vol IS NOT NULL AND x.vol IS NOT NULL AND (s.vol <> x.vol OR (s.fp IS NOT NULL AND x.fp IS NOT NULL AND s.fp <> x.fp))) AS biblio_differs,
             (s.src IS NOT NULL AND s.src = x.src) AS src_same,
             CASE WHEN size(s.abs_tokens) >= 20 AND size(x.abs_tokens) >= 20
                  THEN size(array_intersect(s.abs_tokens, x.abs_tokens)) / size(array_union(s.abs_tokens, x.abs_tokens)) END AS abs_jaccard
      FROM sides s JOIN winners x ON x.ta = s.ta WHERE s.rn > 1
    ),
    tiered AS (
      SELECT p.*,
             CASE WHEN p.year_cls = 'gt1' THEN NULL
                  WHEN p.title_jaccard >= {TITLE_JACCARD_MIN} THEN 1
                  WHEN p.abs_jaccard >= {ABSTRACT_JACCARD_MIN} THEN 2 END AS tier,
             CASE WHEN p.loser_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other')
                    OR p.winner_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other') THEN 'junk_type'
                  WHEN (p.loser_type = 'book' AND p.winner_type IN ('book-chapter', 'article')) OR (p.winner_type = 'book' AND p.loser_type IN ('book-chapter', 'article')) THEN 'book_vs_part' END AS exclusion
      FROM pairs p
    ),
    held AS (
      SELECT t.*,
             CASE WHEN t.exclusion IS NOT NULL THEN t.exclusion
                  WHEN t.year_cls = 'gt1' THEN 'year_gap'
                  WHEN t.tier IS NULL THEN CASE WHEN t.abs_jaccard IS NOT NULL THEN 'doi_misassigned' ELSE 'title_differs_no_abstract' END
                  WHEN t.tier NOT IN ({', '.join(str(x) for x in sorted(TIERS)) or 'NULL'}) THEN CONCAT('tier_', t.tier, '_off')
                  END AS base_hold
      FROM tiered t
    )
    SELECT h.*, CAST(NULL AS STRING) AS rescue,
           CASE WHEN h.base_hold IS NOT NULL THEN h.base_hold
                WHEN EXISTS (SELECT 1 FROM winners x WHERE x.work_id = h.loser_work_id) THEN 'chained'
                END AS hold_reason
    FROM held h
    """


def exact_signature_class_sql():
    """Exact-signature class (oxjob #1256, 2026-09-26): live works whose records carry DIFFERENT title keys but agree
    exactly on normalized full title + publication year + at least one of {abstract (>= 400 normalized chars), ordered
    author surnames (>= 2 authors), source + volume + issue + first page, arXiv id}; groups of 2-3 per signature; at most
    one distinct DOI across the pair; types equal or both journal-ish. Blind-labelled 198 + 130 pairs: 97.9 % same before
    the holds below, 99.91 % on a fresh draw after them. Winner = the DOI-bearing side, else the lower id.
    Holds: biblio_differs, biblio_only_authors_disjoint, other_type (the three failure shapes of the first sample),
    pmid_conflict, multi_winner (a loser matched to two winners), chained, loser_key_shared (a loser record key also
    held by a third work, which could route the records there). Keys differ, so execute also re-keys the loser's
    record keys onto the winner (`<target>_wave<N>_aliases`); `ta` = 'sig:<winner id>' groups a winner's losers."""
    norm = "regexp_replace(lower({c}), '[^\\\\p{{L}}\\\\p{{N}}]', '')"
    surnames = "transform(authorships, a -> lower(regexp_replace(element_at(split(trim(a.author.display_name), ' '), -1), '[^\\\\p{L}]', '')))"
    journalish = "('article', 'review', 'other', 'paratext', 'editorial', 'letter')"
    return f"""
    WITH live AS (SELECT w.* FROM {WORKS} w LEFT ANTI JOIN {MERGED} m ON m.loser_work_id = w.id),
    s AS (
      SELECT 'biblio' AS sn, id, concat_ws('|', primary_location.source.id, publication_year, lower(biblio.volume), lower(biblio.issue),
                                           lower(biblio.first_page), {norm.format(c='title')}) AS sig FROM live
        WHERE primary_location.source.id IS NOT NULL AND publication_year IS NOT NULL AND biblio.volume IS NOT NULL
          AND biblio.first_page IS NOT NULL AND length({norm.format(c='title')}) >= 15
      UNION ALL
      SELECT 'abstract', id, concat(publication_year, '|', sha2({norm.format(c='abstract')}, 256)) FROM live
        WHERE abstract IS NOT NULL AND publication_year IS NOT NULL AND length({norm.format(c='abstract')}) >= 400
      UNION ALL
      SELECT 'authors', id, concat_ws('|', publication_year, {norm.format(c='title')}, array_join({surnames}, ',')) FROM live
        WHERE publication_year IS NOT NULL AND size(authorships) >= 2 AND length({norm.format(c='title')}) >= 30
      UNION ALL
      SELECT 'arxiv', l.work_id, lower(regexp_replace(l.merge_key.arxiv, 'v[0-9]+$', '')) FROM {LM} l JOIN live ON live.id = l.work_id
        WHERE l.merge_key.arxiv IS NOT NULL AND l.work_id IS NOT NULL),
    g AS (SELECT sn, sig, collect_set(id) AS ids FROM s GROUP BY sn, sig HAVING size(collect_set(id)) BETWEEN 2 AND 3),
    p0 AS (SELECT sn, array_min(ids) AS a, b FROM g LATERAL VIEW explode(ids) e AS b WHERE b <> array_min(ids)),
    p AS (SELECT a, b, max(sn = 'biblio') AS s_biblio, max(sn = 'abstract') AS s_abstract, max(sn = 'authors') AS s_authors,
                 max(sn = 'arxiv') AS s_arxiv FROM p0 GROUP BY a, b),
    wf AS (SELECT id, regexp_replace(regexp_replace(lower(doi), '^https?://(dx\\\\.)?doi\\\\.org/', ''), '\\\\s', '') AS d,
                  publication_year AS yr, type, primary_location.source.id AS src, {norm.format(c='title')} AS tn,
                  biblio.volume AS vol, biblio.first_page AS fp, {surnames} AS fam, ids['pmid'] AS pmid,
                  COALESCE(cited_by_count, 0) AS cites FROM live),
    f AS (
      SELECT p.*, x.d AS da, y.d AS db, x.type AS ta_type, y.type AS tb_type, x.yr AS ya, y.yr AS yb, x.cites AS ca, y.cites AS cb,
             x.tn = y.tn AS title_same, x.yr = y.yr AS year_same, (x.src IS NOT NULL AND x.src = y.src) AS src_same,
             (x.type = y.type OR (x.type IN {journalish} AND y.type IN {journalish})) AS type_ok,
             (x.vol IS NOT NULL AND y.vol IS NOT NULL AND (x.vol <> y.vol OR (x.fp IS NOT NULL AND y.fp IS NOT NULL AND x.fp <> y.fp))) AS biblio_differs,
             (size(x.fam) > 0 AND size(y.fam) > 0 AND size(array_intersect(x.fam, y.fam)) = 0) AS authors_disjoint,
             (x.pmid IS NOT NULL AND y.pmid IS NOT NULL AND x.pmid <> y.pmid) AS pmid_conflict,
             CASE WHEN x.type IN {journalish} THEN 'journalish' WHEN x.type = 'dissertation' THEN 'thesis' WHEN x.type = 'preprint' THEN 'preprint'
                  WHEN x.type = 'conference-paper' THEN 'conference' WHEN x.type IN ('book', 'book-chapter') THEN 'book' ELSE 'other_type' END AS tg,
             regexp_replace(x.tn, '[^\\\\p{{L}}]', '') = '' AS digit_title
      FROM p JOIN wf x ON x.id = p.a JOIN wf y ON y.id = p.b),
    r1 AS (
      SELECT *,
             -- winner = the DOI-bearing side, else the lower id (a < b by construction)
             {exact_signature_winner()}
      FROM f
      WHERE title_same AND year_same AND NOT digit_title
        AND (da IS NULL OR db IS NULL OR da = db)),""" + pair_class_tail(EXACT_SIGNATURE_SIGNALS, exact_signature_hold())


def pair_class_tail(signals, hold):
    """Shared tail of the pair classes (exact_signature, declared_version): `r1` (one row per pair with a, b, da, db, ya, yb,
    ca, cb, ta_type, tb_type, biblio_differs, src_same, winner_work_id, loser_work_id) -> the target's columns, with the
    mechanical holds (multi_winner, chained, loser_key_shared) available to `hold` as mu / wn / sh."""
    return f"""
    lm AS (SELECT work_id, COUNT(*) AS n_locations,
                  MAX(CASE WHEN provenance NOT IN ('repo', 'repo_backfill') THEN 1 ELSE 0 END) = 0 AS repo_only
           FROM {LM} WHERE work_id IS NOT NULL GROUP BY work_id),
    -- the loser's record keys, as MapWorkIds will look them up after the unpin
    lkeys AS (
      SELECT DISTINCT x.loser_work_id, x.winner_work_id, kv.col, kv.k
      FROM (SELECT r.loser_work_id, r.winner_work_id, l.merge_key AS mk FROM r1 r JOIN {LM} l ON l.work_id = r.loser_work_id) x
      LATERAL VIEW explode(map('doi', NULLIF(x.mk.doi, ''), 'pmid', x.mk.pmid, 'arxiv', x.mk.arxiv,
                               'title_author', x.mk.title_author)) kv AS col, k
      WHERE kv.k IS NOT NULL),
    kin AS (SELECT DISTINCT col, k FROM lkeys),
    holders AS (
      SELECT 'doi' AS col, m.doi AS k, m.id FROM {MAP} m JOIN kin ON kin.col = 'doi' AND kin.k = m.doi
      UNION ALL SELECT 'pmid', m.pmid, m.id FROM {MAP} m JOIN kin ON kin.col = 'pmid' AND kin.k = m.pmid
      UNION ALL SELECT 'arxiv', m.arxiv, m.id FROM {MAP} m JOIN kin ON kin.col = 'arxiv' AND kin.k = m.arxiv
      UNION ALL SELECT 'title_author', m.title_author, m.id FROM {MAP} m JOIN kin ON kin.col = 'title_author' AND kin.k = m.title_author),
    kc AS (SELECT col, k, COUNT(DISTINCT id) AS n, MIN(id) AS mn, MAX(id) AS mx FROM holders GROUP BY col, k),
    shared AS (
      SELECT DISTINCT lk.loser_work_id FROM lkeys lk JOIN kc ON kc.col = lk.col AND kc.k = lk.k
      WHERE kc.n > 2
         OR (kc.n = 2 AND NOT (kc.mn IN (lk.loser_work_id, lk.winner_work_id) AND kc.mx IN (lk.loser_work_id, lk.winner_work_id)))
         OR (kc.n = 1 AND kc.mn NOT IN (lk.loser_work_id, lk.winner_work_id))),
    multi AS (SELECT loser_work_id FROM r1 GROUP BY loser_work_id HAVING COUNT(DISTINCT winner_work_id) > 1),
    winners AS (SELECT DISTINCT winner_work_id FROM r1)
    SELECT concat('sig:', r.winner_work_id) AS ta, CAST(2 AS BIGINT) AS n_ids,
           CAST((r.da IS NOT NULL) OR (r.db IS NOT NULL) AS BIGINT) AS n_doi,
           r.winner_work_id,
           CASE WHEN r.winner_work_id = r.a THEN r.ya ELSE r.yb END AS winner_yr,
           CASE WHEN r.winner_work_id = r.a THEN r.da ELSE r.db END IS NOT NULL AS winner_has_doi,
           CASE WHEN r.winner_work_id = r.a THEN r.ta_type ELSE r.tb_type END AS winner_type,
           r.loser_work_id,
           CASE WHEN r.loser_work_id = r.a THEN r.ya ELSE r.yb END AS loser_yr,
           CASE WHEN r.loser_work_id = r.a THEN r.ca ELSE r.cb END AS loser_cites,
           COALESCE(lm.repo_only, FALSE) AS loser_repo_only, COALESCE(lm.n_locations, 0) AS loser_locations,
           CAST(NULL AS INT) AS key_len,
           CASE WHEN r.loser_work_id = r.a THEN r.ta_type ELSE r.tb_type END AS loser_type,
           'same' AS year_cls, TRUE AS full_title_same, 1.0D AS title_jaccard, r.biblio_differs, r.src_same,
           1 AS tier, CAST(NULL AS STRING) AS exclusion,
           {signals} AS signals,
           CAST(NULL AS STRING) AS base_hold, CAST(NULL AS STRING) AS rescue,
           {hold} AS hold_reason
    FROM r1 r
    LEFT JOIN lm ON lm.work_id = r.loser_work_id
    LEFT JOIN multi mu ON mu.loser_work_id = r.loser_work_id
    LEFT JOIN winners wn ON wn.winner_work_id = r.loser_work_id
    LEFT JOIN shared sh ON sh.loser_work_id = r.loser_work_id
    """

EXACT_SIGNATURE_SIGNALS = """concat_ws('+', CASE WHEN r.s_abstract THEN 'abstract' END, CASE WHEN r.s_authors THEN 'authors' END,
                          CASE WHEN r.s_biblio THEN 'biblio' END, CASE WHEN r.s_arxiv THEN 'arxiv' END)"""
EXACT_SIGNATURE_HOLD = """CASE WHEN NOT r.type_ok THEN 'version_types'
                WHEN r.biblio_differs THEN 'biblio_differs'
                WHEN r.s_biblio AND NOT (r.s_abstract OR r.s_authors OR r.s_arxiv) AND r.authors_disjoint THEN 'biblio_only_authors_disjoint'
                WHEN r.tg = 'other_type' THEN 'other_type'
                WHEN r.pmid_conflict THEN 'pmid_conflict'
                WHEN mu.loser_work_id IS NOT NULL THEN 'multi_winner'
                WHEN wn.winner_work_id IS NOT NULL THEN 'chained'
                WHEN sh.loser_work_id IS NOT NULL THEN 'loser_key_shared'
                END"""


def declared_version_class_sql():
    """Declared-version class (oxjob #1256, 2026-09-28; Casey: a preprint and its published article are ONE work): an arXiv
    preprint work whose DataCite record declares `IsVersionOf` exactly one non-arXiv DOI, and the live work holding that DOI
    typed article / conference-paper / book-chapter. The published side always wins. Blind-labelled 150 pairs: normalized
    titles identical 100/100 same; titles differing 47/50 (one bad declaration: an author's unrelated earlier paper) ->
    held as `title_differs`. arXiv's own DOI is the loser's key, so execute re-keys it onto the winner like exact_signature;
    `ta` = 'ver:<winner id>'. Mechanical holds as exact_signature (a MAG-era arXiv record holding the same arXiv id makes
    a triple: loser_key_shared)."""
    norm = "regexp_replace(lower({c}), '[^\\\\p{{L}}\\\\p{{N}}]', '')"
    doi_clean = "regexp_replace(regexp_replace(lower(trim({c})), '^(https?://(dx\\\\.)?doi\\\\.org/|doi:)', ''), '[^a-z0-9./-]', '')"
    return f"""
    WITH live AS (SELECT w.* FROM {WORKS} w LEFT ANTI JOIN {MERGED} m ON m.loser_work_id = w.id),
    rel AS (
      SELECT DISTINCT l.work_id AS a, {doi_clean.format(c='i.id')} AS target_doi
      FROM {LM} l LATERAL VIEW explode(l.ids) e AS i
      WHERE l.work_id IS NOT NULL AND l.provenance = 'datacite' AND l.native_id LIKE '10.48550/%'
        AND i.relationship = 'IsVersionOf' AND lower(COALESCE(i.namespace, 'doi')) = 'doi' AND lower(i.id) NOT LIKE '%10.48550/%'),
    one_target AS (SELECT a, MIN(target_doi) AS target_doi FROM rel GROUP BY a HAVING COUNT(DISTINCT target_doi) = 1),
    wf AS (SELECT id, {doi_clean.format(c='doi')} AS d, publication_year AS yr, type, primary_location.source.id AS src,
                  {norm.format(c='title')} AS tn, ids['pmid'] AS pmid, COALESCE(cited_by_count, 0) AS cites FROM live),
    r1 AS (
      SELECT x.id AS a, y.id AS b, x.d AS da, y.d AS db, x.yr AS ya, y.yr AS yb, x.cites AS ca, y.cites AS cb,
             x.type AS ta_type, y.type AS tb_type, FALSE AS biblio_differs, (x.src IS NOT NULL AND x.src = y.src) AS src_same,
             x.tn = y.tn AS title_same, (x.pmid IS NOT NULL AND y.pmid IS NOT NULL AND x.pmid <> y.pmid) AS pmid_conflict,
             y.id AS winner_work_id, x.id AS loser_work_id
      FROM one_target o JOIN wf x ON x.id = o.a JOIN wf y ON y.d = o.target_doi
      WHERE x.type = 'preprint' AND y.type IN ('article', 'conference-paper', 'book-chapter') AND x.id <> y.id),""" + pair_class_tail(DECLARED_VERSION_SIGNALS, DECLARED_VERSION_HOLD).replace("concat('sig:', r.winner_work_id)", "concat('ver:', r.winner_work_id)")

DECLARED_VERSION_SIGNALS = "'declared_is_version_of'"
DECLARED_VERSION_HOLD = """CASE WHEN NOT r.title_same THEN 'title_differs'
                WHEN r.pmid_conflict THEN 'pmid_conflict'
                WHEN mu.loser_work_id IS NOT NULL THEN 'multi_winner'
                WHEN wn.winner_work_id IS NOT NULL THEN 'chained'
                WHEN sh.loser_work_id IS NOT NULL THEN 'loser_key_shared'
                END"""


def feed_twin_class_sql():
    """Feed-twin class (oxjob #1427, 2026-09-29): a feed record in scope whose Crossref twin (by DOI > article URL > title +
    year) sits on another live work. Measured on the #1404 OJS endpoints: 58K records; DOI twins 100 % the same article in
    samples, URL twins 99.06 % DOI-agreement on 268K pairs (49/50 blind), title twins 40/40. One row per (winner, loser)."""
    ukey = lambda c: (f"regexp_extract(regexp_replace(regexp_replace(lower({c}), '^https?://(www[.])?', ''), '/index[.]php/', '/'), "
                      "'^([^?#]*/article/view/[0-9]+)', 1)")
    dk = lambda c: f"NULLIF(regexp_replace(lower({c}), '[^a-z0-9./-]', ''), '')"
    return f"""
    WITH live AS (SELECT w.* FROM {WORKS} w LEFT ANTI JOIN {MERGED} m ON m.loser_work_id = w.id),
    scope AS ({FEED_SCOPE_SQL}),
    feed AS (
      SELECT l.native_id, l.work_id, YEAR(l.published_date) AS yr, l.normalized_title AS nt, {dk('l.merge_key.doi')} AS dk,
             array_distinct(filter(transform(l.urls, u -> {ukey('u.url')}), x -> x <> '')) AS ukeys,
             regexp_extract(l.native_id, '^oai:([^:]+):', 1) IN ('ojs.pkp.sfu.ca', 'localhost', 'ojs.localhost', 'generic.eprints.org') AS default_host
      FROM {LM} l JOIN scope s ON s.endpoint_id = l.endpoint_id
      WHERE l.provenance IN ('repo', 'repo_backfill') AND l.work_id IS NOT NULL {prior_exclusion()}),
    crl AS (SELECT native_id, work_id, {dk('merge_key.doi')} AS dk, YEAR(published_date) AS yr, normalized_title AS nt
            FROM {LM} WHERE provenance = 'crossref' AND work_id IS NOT NULL),
    by_doi AS (SELECT dk, MIN(work_id) AS w FROM crl WHERE dk IS NOT NULL GROUP BY dk HAVING COUNT(DISTINCT work_id) = 1),
    cru AS (SELECT c.native_id, {ukey('c.resource.primary.URL')} AS ukey FROM {CROSSREF_RAW} c WHERE c.resource.primary.URL LIKE '%/article/view/%'),
    keyed AS (  -- a URL key counts only when exactly one DOI claims it
      SELECT cru.ukey, MIN(crl.work_id) AS w, MIN(crl.dk) AS dk, MIN(crl.yr) AS yr
      FROM cru JOIN crl ON crl.native_id = cru.native_id WHERE cru.ukey <> ''
      GROUP BY cru.ukey HAVING COUNT(DISTINCT crl.dk) = 1),
    fk AS (SELECT x.native_id, x.dk, x.yr, e.k AS ukey FROM (SELECT native_id, dk, yr, ukeys FROM feed) x LATERAL VIEW explode(x.ukeys) e AS k),
    blocked AS (  -- hosts whose URL keys disagree with their records' own DOIs on > 2 % of >= 20 pairs (renumbered in a migration)
      SELECT split_part(fk.ukey, '/', 1) AS host FROM fk JOIN keyed k ON k.ukey = fk.ukey WHERE fk.dk IS NOT NULL
      GROUP BY 1 HAVING COUNT(*) >= 20 AND COUNT_IF(k.dk <> fk.dk) > 0.02 * COUNT(*)),
    by_url AS (
      SELECT fk.native_id, MIN(k.w) AS w FROM fk JOIN keyed k ON k.ukey = fk.ukey AND k.yr = fk.yr
      LEFT ANTI JOIN blocked b ON b.host = split_part(fk.ukey, '/', 1)
      WHERE fk.dk IS NULL GROUP BY fk.native_id HAVING COUNT(DISTINCT k.w) = 1),
    by_title AS (SELECT nt, yr, MIN(work_id) AS w FROM crl WHERE length(nt) >= 40 AND yr IS NOT NULL
                 GROUP BY nt, yr HAVING COUNT(DISTINCT work_id) = 1),
    tw AS (
      SELECT f.work_id, f.default_host,
             CASE WHEN d.w IS NOT NULL THEN 1 WHEN u.w IS NOT NULL THEN 2 WHEN t.w IS NOT NULL THEN 3 END AS twin_rank,
             COALESCE(d.w, u.w, t.w) AS target
      FROM feed f LEFT JOIN by_doi d ON d.dk = f.dk LEFT JOIN by_url u ON u.native_id = f.native_id
      LEFT JOIN by_title t ON f.dk IS NULL AND length(f.nt) >= 40 AND t.nt = f.nt AND t.yr = f.yr),
    pairs AS (SELECT target AS a, work_id AS b, MIN(twin_rank) AS twin_rank, MAX(default_host) AS default_host
              FROM tw WHERE target IS NOT NULL AND target <> work_id GROUP BY target, work_id),
    pr AS (SELECT DISTINCT work_id FROM {LM} WHERE provenance IN ('crossref', 'datacite') AND work_id IS NOT NULL),
    wf AS (SELECT id, lower(doi) AS d, publication_year AS yr, type, primary_location.source.id AS src, COALESCE(cited_by_count, 0) AS cites,
                  (title IS NULL OR trim(title) = '') AS no_title FROM live),
    r1 AS (
      SELECT p.a, p.b, x.d AS da, y.d AS db, x.yr AS ya, y.yr AS yb, x.cites AS ca, y.cites AS cb, x.type AS ta_type, y.type AS tb_type,
             FALSE AS biblio_differs, (x.src IS NOT NULL AND x.src = y.src) AS src_same,
             p.a AS winner_work_id, p.b AS loser_work_id, x.no_title AS winner_no_title,
             element_at(array('doi', 'url', 'title'), p.twin_rank) AS twin, p.default_host, (pr.work_id IS NOT NULL) AS loser_has_primary
      FROM pairs p JOIN wf x ON x.id = p.a JOIN wf y ON y.id = p.b LEFT JOIN pr ON pr.work_id = p.b),""" + pair_class_tail(FEED_TWIN_SIGNALS, FEED_TWIN_HOLD).replace("concat('sig:', r.winner_work_id)", "concat('twin:', r.winner_work_id)")

FEED_TWIN_SIGNALS = "r.twin"
FEED_TWIN_HOLD = """CASE WHEN r.default_host THEN 'default_oai_host'
                WHEN r.loser_has_primary THEN 'loser_has_primary'
                WHEN r.ya IS NOT NULL AND r.yb IS NOT NULL AND r.ya <> r.yb THEN 'year_differs'
                WHEN NOT COALESCE(lm.repo_only, FALSE) THEN 'loser_mixed'
                WHEN r.winner_no_title OR r.ta_type = 'paratext' THEN 'winner_junk'
                WHEN r.ta_type = 'preprint' THEN 'winner_preprint'
                WHEN r.twin = 'title' AND r.ta_type <> r.tb_type THEN 'title_type_differs'
                WHEN r.twin = 'title' AND (r.ta_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other')
                                           OR r.tb_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other')) THEN 'junk_type'
                WHEN mu.loser_work_id IS NOT NULL THEN 'multi_winner'
                WHEN wn.winner_work_id IS NOT NULL THEN 'chained'
                WHEN sh.loser_work_id IS NOT NULL THEN 'loser_key_shared'
                END"""


def zenodo_twin_class_sql(rel_sql=None, tag="zen", signals=None):
    """Zenodo-twin class (oxjob #1540, 2026-10-05; widened 2026-10-06 to every DataCite prefix): a live DataCite record whose `ids[]`
    declares `IsVersionOf` exactly one concept DOI, that concept DOI being a DataCite record on exactly one other live work. Zenodo
    and every other InvenioRDM instance (KTH 10.71775, ZD 25672) register the pair this way; arXiv records (10.48550) stay with the
    declared_version class. Winner = the version work, loser = the concept work (Casey 2026-10-05). One row per (winner, loser);
    a concept with several versions lists several winners and is held as multi_winner by the shared tail."""
    norm = "regexp_replace(lower({c}), '[^\\\\p{{L}}\\\\p{{N}}]', '')"
    doi_clean = "regexp_replace(regexp_replace(lower(trim({c})), '^(https?://(dx\\\\.)?doi\\\\.org/|doi:)', ''), '[^a-z0-9./-]', '')"
    scope = (f"(x.src IN (SELECT source_id FROM scope) OR y.src IN (SELECT source_id FROM scope))" if TWIN_SCOPE_SQL else "TRUE")
    scope_cte = f"scope AS ({TWIN_SCOPE_SQL})," if TWIN_SCOPE_SQL else ""
    if rel_sql is None:
        rel_sql = f"""SELECT DISTINCT d.work_id AS ver_work, {doi_clean.format(c='i.id')} AS concept_doi
            FROM dc d LATERAL VIEW explode(d.ids) e AS i
            WHERE i.relationship = 'IsVersionOf' AND lower(COALESCE(i.namespace, 'doi')) = 'doi' AND lower(i.id) NOT LIKE '%10.48550/%'"""
    return f"""
    WITH live AS (SELECT w.* FROM {WORKS} w LEFT ANTI JOIN {MERGED} m ON m.loser_work_id = w.id),
    {scope_cte}
    dc AS (SELECT l.work_id, lower(l.native_id) AS doi, l.ids FROM {LM} l
           WHERE l.provenance = 'datacite' AND l.work_id IS NOT NULL AND lower(l.native_id) NOT LIKE '10.48550/%' {prior_exclusion()}),
    rel AS ({rel_sql}),
    one AS (SELECT ver_work, MIN(concept_doi) AS concept_doi FROM rel GROUP BY ver_work HAVING COUNT(DISTINCT concept_doi) = 1),
    cw AS (SELECT doi, MIN(work_id) AS concept_work FROM dc GROUP BY doi HAVING COUNT(DISTINCT work_id) = 1),
    pairs AS (SELECT o.ver_work, cw.concept_work, o.concept_doi FROM one o JOIN cw ON cw.doi = o.concept_doi WHERE cw.concept_work <> o.ver_work),
    -- the concept work's other primary records: a Crossref record, or a DataCite record that is not the concept DOI itself
    lp AS (SELECT p.concept_work FROM pairs p JOIN {LM} l ON l.work_id = p.concept_work
           WHERE l.provenance = 'crossref' OR (l.provenance = 'datacite' AND lower(l.native_id) <> p.concept_doi) GROUP BY p.concept_work),
    wf AS (SELECT id, lower(doi) AS d, publication_year AS yr, type, CAST(regexp_extract(primary_location.source.id, '([0-9]+)$', 1) AS BIGINT) AS src,
                  {norm.format(c='title')} AS tn, (title IS NULL OR trim(title) = '') AS no_title, COALESCE(cited_by_count, 0) AS cites,
                  filter(split(lower(get(authorships, 0).author.display_name), '[^\\\\p{{L}}]+'), t -> length(t) >= 3) AS a1 FROM live),
    r1 AS (
      SELECT x.id AS a, y.id AS b, x.d AS da, y.d AS db, x.yr AS ya, y.yr AS yb, x.cites AS ca, y.cites AS cb,
             x.type AS ta_type, y.type AS tb_type, FALSE AS biblio_differs, (x.src IS NOT NULL AND x.src = y.src) AS src_same,
             x.tn = y.tn AS title_same, (x.yr IS NOT NULL AND y.yr IS NOT NULL AND x.yr <> y.yr) AS year_differs,
             (size(x.a1) > 0 AND size(y.a1) > 0 AND NOT arrays_overlap(x.a1, y.a1)) AS author_differs,
             (lp.concept_work IS NOT NULL) AS loser_has_primary, x.no_title AS winner_no_title,
             (x.type IN ('dataset', 'software', 'supplementary-materials') OR y.type IN ('dataset', 'software', 'supplementary-materials')) AS dataset_software,
             x.id AS winner_work_id, y.id AS loser_work_id
      FROM pairs p JOIN wf x ON x.id = p.ver_work JOIN wf y ON y.id = p.concept_work LEFT JOIN lp ON lp.concept_work = p.concept_work
      WHERE {scope}),""" + pair_class_tail(signals or ZENODO_TWIN_SIGNALS, ZENODO_TWIN_HOLD).replace("concat('sig:', r.winner_work_id)", f"concat('{tag}:', r.winner_work_id)")

ZENODO_TWIN_SIGNALS = "'declared_is_version_of'"
ZENODO_TWIN_HOLD = """CASE WHEN mu.loser_work_id IS NOT NULL THEN 'multi_winner'
                WHEN NOT r.title_same THEN 'title_differs'
                WHEN r.year_differs THEN 'year_differs'
                WHEN r.author_differs THEN 'author_differs'
                WHEN r.loser_has_primary THEN 'loser_has_primary'
                WHEN r.winner_no_title OR r.ta_type = 'paratext' THEN 'winner_junk'
                WHEN r.dataset_software THEN 'dataset_software'
                WHEN wn.winner_work_id IS NOT NULL THEN 'chained'
                WHEN sh.loser_work_id IS NOT NULL THEN 'loser_key_shared'
                END"""


def version_suffix_class_sql():
    """Version-suffix class (oxjob #1581, 2026-10-07): a live DataCite record whose DOI is another live DataCite record's DOI plus
    `.v<N>` (Figshare and its institutional portals register versions this way and declare no IsVersionOf). The zenodo_twin
    pipeline with the pair taken from the DOI string instead of the declaration: same winner (the version work), holds and
    re-keying; `ta` = 'vsx:<winner id>'. A concept with several `.vN` works is held as multi_winner, as in zenodo_twin."""
    rel = """SELECT DISTINCT d.work_id AS ver_work, regexp_replace(d.doi, '\\\\.v[0-9]+$', '') AS concept_doi
            FROM dc d WHERE d.doi RLIKE '\\\\.v[0-9]+$'"""
    return zenodo_twin_class_sql(rel_sql=rel, tag="vsx", signals="'doi_version_suffix'")


def legacy_twin_class_sql():
    """Legacy-twin class (oxjob #1427, 2026-10-05, from #1546's QA round 2): a DOI-less feed-only work (every record repo, at least
    one from a feed endpoint in `feed_scope_sql`) beside a MAG-only legacy work on the same primary source with the same normalised
    title. Winner = the legacy work (it carries the citations), loser = the feed work; execute re-keys the feed record onto the legacy
    work. Sized 2026-10-05: 43,709 clean pairs on 1,506 sources, 41,827 citations on the winners; blind 50 and the 15 most-cited
    winners all the same article (oxjobs #1427 work/q45–q47). Holds: not_one_to_one, title_short (< 40 chars), year_differs
    (or unknown), type_differs, junk_type, plus the mechanical holds; `ta` = 'leg:<winner id>'."""
    return f"""
    WITH live AS (SELECT w.* FROM {WORKS} w LEFT ANTI JOIN {MERGED} m ON m.loser_work_id = w.id),
    scope AS ({FEED_SCOPE_SQL}),
    feedw AS (SELECT DISTINCT l.work_id FROM {LM} l JOIN scope s ON s.endpoint_id = l.endpoint_id
              WHERE l.provenance IN ('repo', 'repo_backfill') AND l.work_id IS NOT NULL {prior_exclusion()}),
    lprov AS (SELECT l.work_id, MAX(CASE WHEN l.provenance NOT IN ('repo', 'repo_backfill') THEN 1 ELSE 0 END) AS has_nonrepo,
                     MAX(CASE WHEN NULLIF(l.merge_key.doi, '') IS NOT NULL THEN 1 ELSE 0 END) AS has_doi, MIN(l.normalized_title) AS nt
              FROM {LM} l LEFT SEMI JOIN feedw f ON f.work_id = l.work_id GROUP BY l.work_id),
    L AS (SELECT w.id, w.primary_location.source.id AS src, p.nt FROM live w JOIN lprov p ON p.work_id = w.id
          WHERE p.has_nonrepo = 0 AND p.has_doi = 0 AND w.primary_location.source.id IS NOT NULL AND p.nt IS NOT NULL AND p.nt <> ''),
    srcs AS (SELECT DISTINCT src FROM L),
    Wc AS (SELECT w.id FROM live w JOIN srcs ON srcs.src = w.primary_location.source.id),
    wprov AS (SELECT l.work_id, MAX(CASE WHEN l.provenance <> 'mag' THEN 1 ELSE 0 END) AS has_nonmag,
                     MIN(CASE WHEN l.provenance = 'mag' THEN l.normalized_title END) AS nt
              FROM {LM} l LEFT SEMI JOIN Wc ON Wc.id = l.work_id GROUP BY l.work_id),
    W AS (SELECT w.id, w.primary_location.source.id AS src, wp.nt FROM live w JOIN wprov wp ON wp.work_id = w.id
          WHERE wp.has_nonmag = 0 AND wp.nt IS NOT NULL AND wp.nt <> ''),
    pairs AS (SELECT W.id AS a, L.id AS b, length(L.nt) AS tl FROM L JOIN W ON W.src = L.src AND W.nt = L.nt),
    nl AS (SELECT b, COUNT(*) AS n FROM pairs GROUP BY b),
    nw AS (SELECT a, COUNT(*) AS n FROM pairs GROUP BY a),
    wf AS (SELECT id, lower(doi) AS d, publication_year AS yr, type, COALESCE(cited_by_count, 0) AS cites FROM live),
    r1 AS (
      SELECT p.a, p.b, x.d AS da, y.d AS db, x.yr AS ya, y.yr AS yb, x.cites AS ca, y.cites AS cb, x.type AS ta_type, y.type AS tb_type,
             FALSE AS biblio_differs, TRUE AS src_same,
             p.a AS winner_work_id, p.b AS loser_work_id,
             (p.tl < 40) AS title_short, (nl.n > 1 OR nw.n > 1) AS not_one_to_one
      FROM pairs p JOIN wf x ON x.id = p.a JOIN wf y ON y.id = p.b JOIN nl ON nl.b = p.b JOIN nw ON nw.a = p.a),""" + pair_class_tail(LEGACY_TWIN_SIGNALS, LEGACY_TWIN_HOLD).replace("concat('sig:', r.winner_work_id)", "concat('leg:', r.winner_work_id)")

LEGACY_TWIN_SIGNALS = "'legacy_mag'"
LEGACY_TWIN_HOLD = """CASE WHEN r.not_one_to_one THEN 'not_one_to_one'
                WHEN r.title_short THEN 'title_short'
                WHEN r.ya IS NULL OR r.yb IS NULL OR r.ya <> r.yb THEN 'year_differs'
                WHEN r.ta_type <> r.tb_type THEN 'type_differs'
                WHEN r.ta_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other')
                     OR r.tb_type IN ('book-review', 'letter', 'editorial', 'erratum', 'paratext', 'review', 'other') THEN 'junk_type'
                WHEN mu.loser_work_id IS NOT NULL THEN 'multi_winner'
                WHEN wn.winner_work_id IS NOT NULL THEN 'chained'
                WHEN sh.loser_work_id IS NOT NULL THEN 'loser_key_shared'
                END"""


def is_preprint_pair(a, b):
    return f"(({a} = 'preprint' AND {b} IN {PUBLISHED_TYPES}) OR ({b} = 'preprint' AND {a} IN {PUBLISHED_TYPES}))"


def title_key_sides():
    """Winner = the DOI-bearing work, else the lowest id; under preprint_is_same a key holding a preprint is won by its published side."""
    if not PREPRINT_IS_SAME:
        return """sides AS (
      SELECT k.ta, g.n_ids, g.n_doi, k.work_id, k.has_doi, k.repo_only, k.n_locations, w.yr, w.type, w.title_norm, w.title_tokens, w.src, w.vol, w.fp, w.cites, w.abs_tokens,
             ROW_NUMBER() OVER (PARTITION BY k.ta ORDER BY k.has_doi DESC, k.work_id) AS rn
      FROM g JOIN k ON k.ta = g.ta
      LEFT JOIN w ON w.id = k.work_id
      WHERE g.n_doi <= 1
    )"""
    return f"""sides0 AS (
      SELECT k.ta, g.n_ids, g.n_doi, k.work_id, k.has_doi, k.repo_only, k.n_locations, w.yr, w.type, w.title_norm, w.title_tokens, w.src, w.vol, w.fp, w.cites, w.abs_tokens,
             MAX(CASE WHEN w.type = 'preprint' THEN 1 ELSE 0 END) OVER (PARTITION BY k.ta) AS key_has_preprint
      FROM g JOIN k ON k.ta = g.ta
      LEFT JOIN w ON w.id = k.work_id
      WHERE g.n_doi <= 1
    ),
    sides AS (
      SELECT * EXCEPT (key_has_preprint),
             ROW_NUMBER() OVER (PARTITION BY ta ORDER BY CASE WHEN key_has_preprint = 1 AND type IN {PUBLISHED_TYPES} THEN 0 ELSE 1 END,
                                                        has_doi DESC, work_id) AS rn
      FROM sides0
    )"""


def preprint_pair_not_excluded(p):
    """under preprint_is_same a preprint + its published version is one work: no exclusion (the tiers still apply)"""
    return f"                  WHEN {is_preprint_pair(f'{p}.loser_type', f'{p}.winner_type')} THEN NULL\n" if PREPRINT_IS_SAME else ""


def preprint_rescue_pred(h):
    return is_preprint_pair(f"{h}.loser_type", f"{h}.winner_type") if PREPRINT_IS_SAME else "FALSE"


def exact_signature_winner():
    if not PREPRINT_IS_SAME:
        return """CASE WHEN db IS NOT NULL AND da IS NULL THEN b ELSE a END AS winner_work_id,
             CASE WHEN db IS NOT NULL AND da IS NULL THEN a ELSE b END AS loser_work_id"""
    pa = f"(ta_type = 'preprint' AND tb_type IN {PUBLISHED_TYPES})"
    pb = f"(tb_type = 'preprint' AND ta_type IN {PUBLISHED_TYPES})"
    return f"""CASE WHEN {pa} THEN b WHEN {pb} THEN a WHEN db IS NOT NULL AND da IS NULL THEN b ELSE a END AS winner_work_id,
             CASE WHEN {pa} THEN a WHEN {pb} THEN b WHEN db IS NOT NULL AND da IS NULL THEN a ELSE b END AS loser_work_id"""


def exact_signature_hold():
    if not PREPRINT_IS_SAME:
        return EXACT_SIGNATURE_HOLD
    return EXACT_SIGNATURE_HOLD.replace("CASE WHEN NOT r.type_ok THEN 'version_types'",
                                        f"CASE WHEN NOT r.type_ok AND NOT {is_preprint_pair('r.ta_type', 'r.tb_type')} THEN 'version_types'", 1)


def prior_exclusion():
    """anti-join every prior target's executed losers (and their winners' losers) so a re-stage never re-lists them"""
    return " ".join(f"AND NOT EXISTS (SELECT 1 FROM {t} p WHERE p.executed_at IS NOT NULL AND p.loser_work_id = work_id)" for t in PRIOR_TARGETS)


def wave_pred():
    return f"t.wave = {WAVE} AND t.hold_reason IS NULL AND t.executed_at IS NULL"


def print_waves(table):
    for r in rows(f"""SELECT wave, hold_reason, COUNT(*) AS losers, COUNT(DISTINCT ta) AS keys, COUNT(DISTINCT winner_work_id) AS winners,
                             SUM(loser_locations) AS locations, SUM(CASE WHEN loser_cites > 0 THEN 1 ELSE 0 END) AS cited_losers,
                             SUM(loser_cites) AS cites, SUM(CASE WHEN executed_at IS NOT NULL THEN 1 ELSE 0 END) AS executed
                      FROM {table} GROUP BY 1, 2 ORDER BY 1 NULLS LAST, 2 NULLS FIRST"""):
        print(r)

def record_merges(scope_sql):
    """Write (loser -> winner) into the durable merged_work_ids table (idempotent). MapWorkIds' legacy adoption
    (mag id / PMH crosswalk) redirects through it, so a merged loser can never be resurrected by its own legacy
    record (35,035 came back that way on 2026-09-25 before this existed)."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {MERGED} (loser_work_id BIGINT NOT NULL, winner_work_id BIGINT NOT NULL,
                  merged_at TIMESTAMP NOT NULL, source STRING) CLUSTER BY (loser_work_id)""")
    n = spark.sql(f"""MERGE INTO {MERGED} m
                      USING (SELECT DISTINCT t.loser_work_id, t.winner_work_id, MIN(t.executed_at) AS merged_at, '{TARGET}' AS source
                             FROM {scope_sql} GROUP BY t.loser_work_id, t.winner_work_id) s
                        ON m.loser_work_id = s.loser_work_id AND m.winner_work_id = s.winner_work_id
                      WHEN NOT MATCHED THEN INSERT *""").collect()[0].num_inserted_rows
    note(merged_work_ids_recorded=n)
    return n

# COMMAND ----------

if MODE == "stage":
    t0 = time.time()
    cited_hold = (f"CASE WHEN c0.hold_reason IS NULL AND c0.loser_cites >= {HOLD_CITED_OVER} THEN 'cited_over_{HOLD_CITED_OVER}' ELSE c0.hold_reason END"
                  if HOLD_CITED_OVER is not None else "c0.hold_reason")
    spark.sql(f"""CREATE OR REPLACE TABLE {TARGET} AS
                  WITH c0 AS ({class_sql()}),
                  c AS (SELECT c0.* EXCEPT (hold_reason), {cited_hold} AS hold_reason FROM c0),
                  keys AS (
                    SELECT ta, MAX(CASE WHEN hold_reason IS NOT NULL THEN 1 ELSE 0 END) AS held,
                           MAX(loser_cites) AS max_cites, COUNT(*) AS n_losers
                    FROM c GROUP BY ta),
                  ordered AS (
                    -- uncited keys first, then by citations; all losers of a key share a wave; held keys get no wave
                    SELECT ta, held, SUM(n_losers) OVER (ORDER BY max_cites, ta ROWS UNBOUNDED PRECEDING) AS cum_losers
                    FROM keys WHERE held = 0)
                  SELECT c.*, CASE WHEN o.ta IS NOT NULL THEN CAST(CEIL(o.cum_losers / {WAVE_SIZE}) AS INT) END AS wave,
                         current_timestamp() AS staged_at, CAST(NULL AS TIMESTAMP) AS executed_at
                  FROM c LEFT JOIN ordered o ON o.ta = c.ta""")
    spark.sql(f"ALTER TABLE {TARGET} CLUSTER BY (wave, ta)")
    spark.sql(f"""CREATE OR REPLACE TABLE {REVIEW} AS
                  SELECT t.wave, t.hold_reason, t.loser_work_id, t.loser_cites, t.winner_work_id, t.ta,
                         LEFT(wl.title, 120) AS loser_title, LEFT(ww.title, 120) AS winner_title, t.loser_yr, t.winner_yr
                  FROM {TARGET} t LEFT JOIN {WORKS} wl ON wl.id = t.loser_work_id LEFT JOIN {WORKS} ww ON ww.id = t.winner_work_id
                  WHERE t.loser_cites > 0 ORDER BY t.loser_cites DESC""")
    note(staged_seconds=int(time.time() - t0), target=TARGET, cited_review=REVIEW,
         totals=one(f"""SELECT COUNT(*) AS losers, COUNT(DISTINCT ta) AS keys, SUM(CASE WHEN hold_reason IS NULL THEN 1 ELSE 0 END) AS executable,
                               MAX(wave) AS waves FROM {TARGET}"""),
         holds=rows(f"SELECT hold_reason, COUNT(*) AS losers FROM {TARGET} WHERE hold_reason IS NOT NULL GROUP BY 1 ORDER BY 2 DESC"),
         waves=rows(f"SELECT wave, COUNT(*) AS losers, SUM(loser_cites) AS cites FROM {TARGET} WHERE wave IS NOT NULL GROUP BY 1 ORDER BY 1"),
         rescued=rows(f"SELECT rescue, base_hold, COUNT(*) AS losers FROM {TARGET} WHERE rescue IS NOT NULL GROUP BY 1, 2 ORDER BY 3 DESC"))
    print_waves(TARGET)

# COMMAND ----------

if MODE in ("dry_run", "execute"):
    plan = one(f"""SELECT COUNT(*) AS losers, COUNT(DISTINCT t.ta) AS keys, COUNT(DISTINCT t.winner_work_id) AS winners,
                          SUM(t.loser_locations) AS locations, SUM(CASE WHEN t.loser_cites > 0 THEN 1 ELSE 0 END) AS cited_losers,
                          SUM(t.loser_cites) AS cites_to_move, MAX(t.loser_cites) AS max_cites
                   FROM {TARGET} t WHERE {wave_pred()}""")
    dels = one(f"""SELECT (SELECT COUNT(*) FROM {REGISTRY} r WHERE EXISTS (SELECT 1 FROM {TARGET} t WHERE {wave_pred()} AND t.loser_work_id = r.work_id)) AS pins_to_delete,
                          (SELECT COUNT(*) FROM {MAP} m WHERE EXISTS (SELECT 1 FROM {TARGET} t WHERE {wave_pred()} AND t.loser_work_id = m.id)) AS map_rows_to_delete""")
    note(**plan, **dels)
    print_waves(TARGET)
    if plan["losers"] == 0:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": f"nothing to do for wave {WAVE}"}, default=str))
    print("sample merges:")
    for r in rows(f"""SELECT t.loser_work_id, LEFT(wl.title, 50) AS loser_title, t.loser_yr, t.loser_cites, t.loser_repo_only,
                             t.winner_work_id, LEFT(ww.title, 50) AS winner_title, t.winner_yr, t.winner_has_doi
                      FROM {TARGET} t LEFT JOIN {WORKS} wl ON wl.id = t.loser_work_id LEFT JOIN {WORKS} ww ON ww.id = t.winner_work_id
                      WHERE {wave_pred()} ORDER BY RAND(1) LIMIT 10"""):
        print("  ", r)

# COMMAND ----------

if MODE == "execute":
    hour = datetime.datetime.utcnow().hour
    if 4 <= hour < 8:
        raise Exception("MapWorkIds writes the registries 05:00-07:00 UTC; run execute outside 04:00-08:00 UTC")
    active = end2end_active()
    if active:
        raise Exception(f"Walden End 2 End is running (runs {active}); wait for it to finish")
    if not CONFIRM:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": "dry run only: pass confirm=yes to execute"}, default=str))
    if spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} exists: wave {WAVE} was already executed")

    t0 = time.time()
    spark.sql(f"""CREATE TABLE {AUDIT} AS
                  SELECT 'pin' AS kind, t.loser_work_id, t.winner_work_id, t.ta,
                         r.provenance, r.native_id_namespace, r.native_id, r.work_id_source, r.openalex_created_dt, r.openalex_updated_dt, r.seeded_dt, r.seeded_from,
                         CAST(NULL AS STRING) AS doi, CAST(NULL AS STRING) AS pmid, CAST(NULL AS STRING) AS arxiv, CAST(NULL AS STRING) AS title_author,
                         CAST(NULL AS DATE) AS created_date, CAST(NULL AS TIMESTAMP) AS updated_date, current_timestamp() AS audited_at
                  FROM {TARGET} t JOIN {REGISTRY} r ON r.work_id = t.loser_work_id WHERE {wave_pred()}
                  UNION ALL
                  SELECT 'map', t.loser_work_id, t.winner_work_id, t.ta,
                         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                         m.doi, m.pmid, m.arxiv, m.title_author, m.created_date, m.updated_date, current_timestamp()
                  FROM {TARGET} t JOIN {MAP} m ON m.id = t.loser_work_id WHERE {wave_pred()}""")
    # a loser under two keys is audited once per key; the DELETEs are by pin / id, so compare DISTINCT
    n = one(f"""SELECT COUNT(DISTINCT CASE WHEN kind = 'pin' THEN CONCAT_WS('|', provenance, native_id_namespace, native_id) END) AS pins,
                       COUNT(DISTINCT CASE WHEN kind = 'map' THEN CONCAT_WS('|', loser_work_id, doi, pmid, arxiv, title_author, created_date) END) AS map_rows
                FROM {AUDIT}""")
    # a record key can be pinned twice (legacy adoption); delete only the loser's pin, and expect exactly the rows that match
    expected_pins = one(f"""SELECT COUNT(*) AS n FROM {REGISTRY} r WHERE EXISTS (SELECT 1 FROM {AUDIT} a WHERE a.kind = 'pin'
                         AND a.provenance = r.provenance AND a.native_id_namespace = r.native_id_namespace AND a.native_id = r.native_id
                         AND a.loser_work_id = r.work_id)""")["n"]
    pins = spark.sql(f"""DELETE FROM {REGISTRY} r WHERE EXISTS (SELECT 1 FROM {AUDIT} a WHERE a.kind = 'pin'
                         AND a.provenance = r.provenance AND a.native_id_namespace = r.native_id_namespace AND a.native_id = r.native_id
                         AND a.loser_work_id = r.work_id)""").collect()[0].num_affected_rows
    maprows = spark.sql(f"""DELETE FROM {MAP} m WHERE EXISTS (SELECT 1 FROM {AUDIT} a WHERE a.kind = 'map' AND a.loser_work_id = m.id)""").collect()[0].num_affected_rows
    if CLASS_MODE in ("exact_signature", "declared_version", "feed_twin", "zenodo_twin", "legacy_twin", "version_suffix") or PREPRINT_IS_SAME:
        # the loser's records carry keys the winner does not hold; bind every key combination they carry to the winner
        # so the nightly MapWorkIds re-resolves them there instead of minting. Undo: DELETE the aliases table's rows from the map.
        ALIASES = f"{TARGET}_wave{WAVE}_aliases"
        spark.sql(f"""CREATE TABLE {ALIASES} AS
                      SELECT DISTINCT t.winner_work_id AS id, NULLIF(l.merge_key.doi, '') AS doi, l.merge_key.pmid AS pmid,
                             l.merge_key.arxiv AS arxiv, l.merge_key.title_author AS title_author, t.loser_work_id
                      FROM {TARGET} t JOIN {AUDIT} a ON a.kind = 'pin' AND a.loser_work_id = t.loser_work_id
                      JOIN {LM} l ON l.provenance = a.provenance AND l.native_id_namespace = a.native_id_namespace AND l.native_id = a.native_id
                      WHERE {wave_pred()}
                        AND COALESCE(NULLIF(l.merge_key.doi, ''), l.merge_key.pmid, l.merge_key.arxiv, l.merge_key.title_author) IS NOT NULL""")
        aliases = spark.sql(f"""INSERT INTO {MAP} (id, doi, pmid, arxiv, title_author, created_date, updated_date)
                                SELECT DISTINCT id, doi, pmid, arxiv, title_author, current_date(), current_timestamp() FROM {ALIASES} x
                                WHERE NOT EXISTS (SELECT 1 FROM {MAP} m WHERE m.id = x.id AND m.doi <=> x.doi AND m.pmid <=> x.pmid
                                                  AND m.arxiv <=> x.arxiv AND m.title_author <=> x.title_author)""").collect()[0].num_inserted_rows
        note(aliases_table=ALIASES, alias_rows_inserted=aliases)
    spark.sql(f"UPDATE {TARGET} t SET executed_at = current_timestamp() WHERE {wave_pred()}")
    record_merges(f"{TARGET} t WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL")
    note(executed_seconds=int(time.time() - t0), audit=AUDIT, audited_pins=n["pins"], audited_map_rows=n["map_rows"],
         pins_deleted=pins, map_rows_deleted=maprows)
    assert pins == expected_pins, f"pins deleted {pins} != matching audited pins {expected_pins}"
    print_waves(TARGET)

# COMMAND ----------

if MODE == "rehold_list":
    # put listed (loser, winner) pairs of an unexecuted wave back on hold as release_hold (e.g. a lower-id sibling on the key)
    if not (RELEASE_LIST and RELEASE_HOLD):
        raise Exception("rehold_list needs release_list (CSV of pairs) and release_hold (the hold_reason to set)")
    spark.read.option("header", True).csv(RELEASE_LIST).selectExpr(
        "CAST(loser_work_id AS BIGINT) AS loser_work_id", "CAST(winner_work_id AS BIGINT) AS winner_work_id"
    ).createOrReplaceTempView("rehold_list")
    n = spark.sql(f"""MERGE INTO {TARGET} t USING (SELECT DISTINCT * FROM rehold_list) r
                      ON t.loser_work_id = r.loser_work_id AND t.winner_work_id = r.winner_work_id
                      WHEN MATCHED AND t.executed_at IS NULL THEN UPDATE SET hold_reason = '{RELEASE_HOLD}', wave = NULL""").collect()[0].num_updated_rows
    note(rehold_list=RELEASE_LIST, hold_reason=RELEASE_HOLD, reheld=n)
    print_waves(TARGET)

# COMMAND ----------

if MODE == "release_held":
    # a labelled review cleared a held class: move its rows into wave N, except the listed rejects (which stay held)
    if not RELEASE_HOLD and not RELEASE_LIST:
        raise Exception("release_held needs release_hold (the hold_reason being cleared) or release_list (a CSV of pairs)")
    if spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} exists: wave {WAVE} was already executed; release into a new wave")
    keep = ", ".join(str(x) for x in KEEP_HELD) or "NULL"
    # `x NOT IN (NULL)` is never true, so an empty keep list must drop the clause rather than render NULL
    def not_kept(alias=""):
        return f"{alias}loser_work_id NOT IN ({keep})" if KEEP_HELD else "TRUE"
    if RELEASE_LIST:
        spark.read.option("header", True).csv(RELEASE_LIST).selectExpr(
            "CAST(loser_work_id AS BIGINT) AS loser_work_id", "CAST(winner_work_id AS BIGINT) AS winner_work_id"
        ).createOrReplaceTempView("release_list")
        listed = one(f"""SELECT COUNT(*) AS listed,
                                SUM(CASE WHEN t.loser_work_id IS NOT NULL THEN 1 ELSE 0 END) AS matched_held,
                                SUM(CASE WHEN m.loser_work_id IS NOT NULL THEN 1 ELSE 0 END) AS already_merged
                         FROM release_list r
                         LEFT JOIN (SELECT DISTINCT loser_work_id, winner_work_id FROM {TARGET}
                                    WHERE hold_reason IS NOT NULL AND executed_at IS NULL) t
                           ON t.loser_work_id = r.loser_work_id AND t.winner_work_id = r.winner_work_id
                         LEFT JOIN {MERGED} m ON m.loser_work_id = r.loser_work_id""")
        # Delta rejects multi-column IN inside UPDATE; MERGE on the pair instead
        n = spark.sql(f"""MERGE INTO {TARGET} t
                          USING (SELECT DISTINCT r.loser_work_id, r.winner_work_id FROM release_list r
                                 LEFT ANTI JOIN {MERGED} m ON m.loser_work_id = r.loser_work_id) r
                            ON t.loser_work_id = r.loser_work_id AND t.winner_work_id = r.winner_work_id
                          WHEN MATCHED AND t.hold_reason IS NOT NULL AND t.executed_at IS NULL AND {not_kept('t.')}
                            THEN UPDATE SET hold_reason = NULL, wave = {WAVE}""").collect()[0].num_updated_rows
        note(release_list=RELEASE_LIST, **listed, released_into_wave=n, wave_now=WAVE)
        print_waves(TARGET)
        dbutils.notebook.exit(json.dumps(SUMMARY, default=str))
    before = one(f"""SELECT COUNT(*) AS held, SUM(CASE WHEN loser_work_id IN ({keep}) THEN 1 ELSE 0 END) AS kept
                     FROM {TARGET} WHERE hold_reason = '{RELEASE_HOLD}' AND executed_at IS NULL""")
    n = spark.sql(f"""UPDATE {TARGET} SET hold_reason = NULL, wave = {WAVE}
                      WHERE hold_reason = '{RELEASE_HOLD}' AND executed_at IS NULL AND {not_kept()}""").collect()[0].num_affected_rows
    spark.sql(f"""UPDATE {TARGET} SET hold_reason = '{RELEASE_HOLD}_rejected'
                  WHERE hold_reason = '{RELEASE_HOLD}' AND executed_at IS NULL AND loser_work_id IN ({keep})""")
    note(release_hold=RELEASE_HOLD, held_before=before["held"], kept_held=before["kept"], released_into_wave=n, wave_now=WAVE)
    print_waves(TARGET)

# COMMAND ----------

if MODE == "record_merges":
    # backfill the durable table from every executed row of this target (all waves)
    record_merges(f"{TARGET} t WHERE t.executed_at IS NOT NULL")
    note(merged_rows_total=one(f"SELECT COUNT(*) AS n FROM {MERGED}")["n"])

# COMMAND ----------

if MODE == "reexecute_resurrected":
    # executed losers that hold registry pins again (legacy adoption re-pinned their own records before the
    # MapWorkIds redirect existed): audit + delete their pins and map rows once more so the nightly re-resolves
    # them through the redirect onto the winner. `wave` selects the wave; `confirm = yes` executes.
    hour = datetime.datetime.utcnow().hour
    if 4 <= hour < 8:
        raise Exception("run outside 04:00-08:00 UTC")
    active = end2end_active()
    if active:
        raise Exception(f"Walden End 2 End is running (runs {active}); wait for it to finish")
    REAUDIT = f"{TARGET}_wave{WAVE}_reexec_audit"
    back = f"""(SELECT DISTINCT t.loser_work_id, t.winner_work_id, t.ta FROM {TARGET} t
                WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL
                  AND EXISTS (SELECT 1 FROM {REGISTRY} r WHERE r.work_id = t.loser_work_id))"""
    plan = one(f"""SELECT COUNT(*) AS resurrected_losers,
                          (SELECT COUNT(*) FROM {REGISTRY} r WHERE EXISTS (SELECT 1 FROM {back} b WHERE b.loser_work_id = r.work_id)) AS pins_to_delete,
                          (SELECT COUNT(*) FROM {MAP} m WHERE EXISTS (SELECT 1 FROM {back} b WHERE b.loser_work_id = m.id)) AS map_rows_to_delete
                   FROM {back} b""")
    note(**plan)
    print("how the resurrected records were bound:")
    for r in rows(f"""SELECT r.work_id_source, r.provenance, COUNT(*) AS pins FROM {REGISTRY} r
                      WHERE EXISTS (SELECT 1 FROM {back} b WHERE b.loser_work_id = r.work_id) GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 8"""):
        print("  ", r)
    if plan["resurrected_losers"] == 0:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": "nothing resurrected"}, default=str))
    if not CONFIRM:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": "dry run only: pass confirm=yes to re-execute"}, default=str))
    if spark.catalog.tableExists(REAUDIT):
        raise Exception(f"{REAUDIT} exists: already re-executed once; drop it deliberately to run again")
    record_merges(f"{TARGET} t WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL")
    spark.sql(f"""CREATE TABLE {REAUDIT} AS
                  SELECT 'pin' AS kind, b.loser_work_id, b.winner_work_id, b.ta,
                         r.provenance, r.native_id_namespace, r.native_id, r.work_id_source, r.openalex_created_dt, r.openalex_updated_dt, r.seeded_dt, r.seeded_from,
                         CAST(NULL AS STRING) AS doi, CAST(NULL AS STRING) AS pmid, CAST(NULL AS STRING) AS arxiv, CAST(NULL AS STRING) AS title_author,
                         CAST(NULL AS DATE) AS created_date, CAST(NULL AS TIMESTAMP) AS updated_date, current_timestamp() AS audited_at
                  FROM {back} b JOIN {REGISTRY} r ON r.work_id = b.loser_work_id
                  UNION ALL
                  SELECT 'map', b.loser_work_id, b.winner_work_id, b.ta, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                         m.doi, m.pmid, m.arxiv, m.title_author, m.created_date, m.updated_date, current_timestamp()
                  FROM {back} b JOIN {MAP} m ON m.id = b.loser_work_id""")
    pins = spark.sql(f"""DELETE FROM {REGISTRY} r WHERE EXISTS (SELECT 1 FROM {REAUDIT} a WHERE a.kind = 'pin'
                         AND a.provenance = r.provenance AND a.native_id_namespace = r.native_id_namespace AND a.native_id = r.native_id
                         AND a.loser_work_id = r.work_id)""").collect()[0].num_affected_rows
    maprows = spark.sql(f"DELETE FROM {MAP} m WHERE EXISTS (SELECT 1 FROM {REAUDIT} a WHERE a.kind = 'map' AND a.loser_work_id = m.id)").collect()[0].num_affected_rows
    note(reexec_audit=REAUDIT, pins_deleted=pins, map_rows_deleted=maprows)

# COMMAND ----------

if MODE == "follow_null_to_winner":
    # A loser's records that the nightly could not resolve by any tier (registered NULL, retried nightly) were part of
    # the loser and belong with its winner: 237K such pins after the 2026-09-25 nightly. Pin them to the winner
    # directly (source 'merge_follow'), audited, and register their title keys to the winner so the map invariant holds.
    if not spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} does not exist: wave {WAVE} was not executed")
    FOLLOW_AUDIT = f"{TARGET}_wave{WAVE}_follow_audit"
    # one row per record (a record audited under two keys can name two winners): the lowest winner id
    scope = f"""(SELECT a.provenance, a.native_id_namespace, a.native_id, MIN(a.loser_work_id) AS loser_work_id, MIN(a.winner_work_id) AS winner_work_id
                 FROM {AUDIT} a JOIN {REGISTRY} r ON r.provenance = a.provenance AND r.native_id_namespace = a.native_id_namespace AND r.native_id = a.native_id
                 WHERE a.kind = 'pin' AND r.work_id IS NULL
                   AND NOT EXISTS (SELECT 1 FROM {REGISTRY} p WHERE p.work_id = a.loser_work_id)
                   AND EXISTS (SELECT 1 FROM {REGISTRY} p WHERE p.work_id = a.winner_work_id)
                 GROUP BY a.provenance, a.native_id_namespace, a.native_id)"""
    plan = one(f"SELECT COUNT(*) AS null_pins_to_follow, COUNT(DISTINCT winner_work_id) AS winners, COUNT(DISTINCT loser_work_id) AS losers FROM {scope} x")
    note(**plan)
    if plan["null_pins_to_follow"] == 0:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": "nothing to follow"}, default=str))
    if not CONFIRM:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": "dry run only: pass confirm=yes to follow"}, default=str))
    # idempotent: the MERGE only touches rows still NULL, so a re-run after a failed attempt is safe and the audit is
    # rebuilt from what is still to follow (a completed pass leaves nothing)
    spark.sql(f"CREATE OR REPLACE TABLE {FOLLOW_AUDIT} AS SELECT x.*, current_timestamp() AS audited_at FROM {scope} x")
    moved = spark.sql(f"""MERGE INTO {REGISTRY} r USING {FOLLOW_AUDIT} x
                          ON r.provenance = x.provenance AND r.native_id_namespace = x.native_id_namespace AND r.native_id = x.native_id
                          WHEN MATCHED AND r.work_id IS NULL THEN UPDATE SET r.work_id = x.winner_work_id, r.work_id_source = 'merge_follow', r.openalex_updated_dt = current_timestamp()""").collect()[0].num_affected_rows
    keys = spark.sql(f"""INSERT INTO {MAP} (id, doi, pmid, arxiv, title_author, created_date, updated_date)
                         SELECT DISTINCT x.winner_work_id, NULL, NULL, NULL, NULLIF(l.merge_key.title_author, ''), current_date(), current_timestamp()
                         FROM {FOLLOW_AUDIT} x JOIN openalex.works.locations_w_types l
                           ON l.provenance = x.provenance AND l.native_id_namespace = x.native_id_namespace AND l.native_id = x.native_id
                         WHERE NULLIF(l.merge_key.title_author, '') IS NOT NULL AND LENGTH(l.merge_key.title_author) > 20
                           AND NOT EXISTS (SELECT 1 FROM {MAP} m WHERE m.id = x.winner_work_id AND m.title_author = l.merge_key.title_author)""").collect()[0].num_inserted_rows
    note(follow_audit=FOLLOW_AUDIT, pins_followed_to_winner=moved, title_keys_registered=keys)

# COMMAND ----------

if MODE == "repoint_arrays":
    # cited_by_count is computed from the citing works' referenced_works ARRAYS (CreateWorksEnriched), and the nightly
    # MERGE only ever unions into them, so a merged loser id stays in every citing work's list and keeps collecting
    # citations. Rewrite the arrays of exactly the works known to cite this wave's losers: from the refs audit (parsed
    # graph) and openalex.mid.citation (legacy graph). Audited (before-image arrays); cited_by_count follows on the nightly.
    if not spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} does not exist: wave {WAVE} was not executed")
    ARR_AUDIT = f"{TARGET}_wave{WAVE}_arrays_audit"
    pairs = f"""(SELECT t.loser_work_id, MIN(t.winner_work_id) AS winner_work_id FROM {TARGET} t
                 WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL GROUP BY t.loser_work_id)"""
    citing = f"""(SELECT DISTINCT id FROM (
                    SELECT c.paper_id AS id FROM openalex.mid.citation c JOIN {pairs} p ON c.paper_reference_id = p.loser_work_id
                    UNION ALL
                    SELECT a.citing_work_id FROM {REFS_AUDIT} a JOIN {pairs} p ON a.old_id = p.loser_work_id WHERE a.side = 'cited'
                    UNION ALL
                    SELECT r.citing_work_id FROM {REFS} r JOIN {pairs} p ON r.cited_work_id = p.loser_work_id))"""
    fix = f"""(SELECT x.id, x.old_refs, array_sort(collect_set(COALESCE(p.winner_work_id, x.ref))) AS new_refs
               FROM (SELECT w.id, w.referenced_works AS old_refs, explode(w.referenced_works) AS ref
                     FROM {WORKS} w JOIN {citing} ci ON ci.id = w.id) x
               LEFT JOIN {pairs} p ON p.loser_work_id = x.ref
               GROUP BY x.id, x.old_refs
               HAVING MAX(CASE WHEN p.loser_work_id IS NOT NULL THEN 1 ELSE 0 END) = 1)"""
    plan = one(f"SELECT COUNT(*) AS citing_works_to_rewrite FROM {fix} f")
    note(**plan)
    if plan["citing_works_to_rewrite"] == 0:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": "no arrays hold this wave's losers"}, default=str))
    if not CONFIRM:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": "dry run only: pass confirm=yes to rewrite the arrays"}, default=str))
    spark.sql(f"CREATE OR REPLACE TABLE {ARR_AUDIT} AS SELECT f.id, f.old_refs, f.new_refs, current_timestamp() AS audited_at FROM {fix} f")
    n = spark.sql(f"""MERGE INTO {WORKS} w USING {ARR_AUDIT} a ON w.id = a.id
                      WHEN MATCHED THEN UPDATE SET w.referenced_works = slice(a.new_refs, 1, 5000),
                                                   w.referenced_works_count = least(size(a.new_refs), 5000),
                                                   w.updated_date = current_timestamp()""").collect()[0].num_affected_rows
    note(arrays_audit=ARR_AUDIT, citing_works_rewritten=n)

# COMMAND ----------

if MODE == "verify":
    if not spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} does not exist: wave {WAVE} was not executed")
    note(landed=rows(f"""
        SELECT CASE WHEN r.work_id IS NULL THEN 'not re-pinned yet / NULL'
                    WHEN r.work_id = a.winner_work_id THEN 'on the winner'
                    WHEN r.work_id = a.loser_work_id THEN 'BACK ON THE LOSER'
                    ELSE 'elsewhere' END AS landed, COUNT(*) AS pins
        FROM {AUDIT} a LEFT JOIN {REGISTRY} r ON r.provenance = a.provenance AND r.native_id_namespace = a.native_id_namespace AND r.native_id = a.native_id
        WHERE a.kind = 'pin' GROUP BY 1 ORDER BY 2 DESC"""))
    note(losers=one(f"""
        SELECT COUNT(DISTINCT t.loser_work_id) AS executed_losers,
               COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM {REGISTRY} p WHERE p.work_id = t.loser_work_id) THEN t.loser_work_id END) AS losers_still_pinned,
               COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM {LEDGER} d WHERE d.work_id = t.loser_work_id) THEN t.loser_work_id END) AS losers_ledgered
        FROM {TARGET} t WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL"""))
    print("elsewhere samples (a loser record that re-resolved to a third work):")
    for r in rows(f"""SELECT a.provenance, a.native_id, a.loser_work_id, a.winner_work_id, r.work_id AS landed_on, r.work_id_source
                      FROM {AUDIT} a JOIN {REGISTRY} r ON r.provenance = a.provenance AND r.native_id_namespace = a.native_id_namespace AND r.native_id = a.native_id
                      WHERE a.kind = 'pin' AND r.work_id IS NOT NULL AND r.work_id NOT IN (a.winner_work_id, a.loser_work_id) LIMIT 8"""):
        print("  ", r)

# COMMAND ----------

if MODE == "repoint_citations":
    if not spark.catalog.tableExists(AUDIT):
        raise Exception(f"{AUDIT} does not exist: wave {WAVE} was not executed")
    # losers that hold pins again (resurrected before the MapWorkIds redirect) are left for the re-execute pass.
    # ONE winner per loser (a loser under two keys can list two winners and MERGE needs a single source row per
    # target row): the lowest winner id, which is what MIN(id) resolution picks too.
    pairs = f"""(SELECT t.loser_work_id, MIN(t.winner_work_id) AS winner_work_id FROM {TARGET} t
                 WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL
                   AND NOT EXISTS (SELECT 1 FROM {REGISTRY} r WHERE r.work_id = t.loser_work_id)
                 GROUP BY t.loser_work_id)"""
    still = one(f"""SELECT COUNT(DISTINCT t.loser_work_id) AS n FROM {TARGET} t WHERE t.wave = {WAVE} AND t.executed_at IS NOT NULL
                    AND EXISTS (SELECT 1 FROM {REGISTRY} r WHERE r.work_id = t.loser_work_id)""")["n"]
    plan = one(f"""SELECT COUNT(*) AS edges, COUNT(DISTINCT r.citing_work_id) AS citing_works, COUNT(DISTINCT r.cited_work_id) AS losers_cited
                   FROM {REFS} r JOIN {pairs} p ON r.cited_work_id = p.loser_work_id""")
    note(losers_still_pinned_and_skipped=still, **plan)
    own = one(f"""SELECT COUNT(*) AS loser_reference_rows, COUNT(DISTINCT r.citing_work_id) AS losers_with_references
                  FROM {REFS} r JOIN {pairs} p ON r.citing_work_id = p.loser_work_id""")
    note(**own)
    if plan["edges"] == 0 and own["loser_reference_rows"] == 0:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": "nothing to repoint (already done, or nothing cited)"}, default=str))
    if not CONFIRM:
        dbutils.notebook.exit(json.dumps({**SUMMARY, "result": "dry run only: pass confirm=yes to repoint citations"}, default=str))
    # before-image of both directions: rows that CITE a loser (cited side) and the loser's OWN reference list (citing side).
    # Idempotent: the MERGEs only touch rows still keyed to a loser, so a re-run after a failed attempt is safe and the
    # audit is rebuilt from what is still to move (a completed pass leaves nothing, so nothing gets overwritten).
    spark.sql(f"""CREATE OR REPLACE TABLE {REFS_AUDIT} AS
                  SELECT 'cited' AS side, r.citing_work_id, r.native_id, r.native_id_namespace, r.ref_ind,
                         r.cited_work_id AS old_id, p.winner_work_id AS new_id, current_timestamp() AS audited_at
                  FROM {REFS} r JOIN {pairs} p ON r.cited_work_id = p.loser_work_id
                  UNION ALL
                  SELECT 'citing', r.citing_work_id, r.native_id, r.native_id_namespace, r.ref_ind,
                         r.citing_work_id, p.winner_work_id, current_timestamp()
                  FROM {REFS} r JOIN {pairs} p ON r.citing_work_id = p.loser_work_id""")
    moved = spark.sql(f"""MERGE INTO {REFS} r USING {pairs} p ON r.cited_work_id = p.loser_work_id
                          WHEN MATCHED THEN UPDATE SET r.cited_work_id = p.winner_work_id, r.updated_timestamp = current_timestamp()""").collect()[0].num_affected_rows
    # the loser's own reference list comes along: rows keep their (native_id, ref_ind) so they cannot collide with the winner's
    carried = spark.sql(f"""MERGE INTO {REFS} r USING {pairs} p ON r.citing_work_id = p.loser_work_id
                            WHEN MATCHED THEN UPDATE SET r.citing_work_id = p.winner_work_id, r.updated_timestamp = current_timestamp()""").collect()[0].num_affected_rows
    note(refs_audit=REFS_AUDIT, edges_repointed=moved, loser_reference_rows_carried=carried)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(SUMMARY, default=str))
