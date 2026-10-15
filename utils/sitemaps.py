"""Sitemaps for openalex.org record pages (oxjob #1689).

Pure Python, no Spark: the nightly notebook (notebooks/sitemaps/BuildSitemaps.py), the local sample
script (scripts/local/sitemaps_sample.py) and the tests (notebooks/sitemaps/tests/) all import this.

What lives here, once:
  * THE QUALITY RULE (which records get indexed). `WORK_INDEXABLE_SQL` / `AUTHOR_INDEXABLE_SQL` are the
    SQL predicates over the Walden tables; `is_indexable()` is the same rule over API records (the JSON the
    website renders). The website's `noindex` rule (track seo-content) must agree with both: the shared
    cases are in notebooks/sitemaps/quality_rule_cases.json and the tests run both forms against them.
  * The groups (one sitemap index per group), their tables, URL forms and page fingerprints.
  * The lastmod rule (true page-change time, from the fingerprint).
  * Sitemap XML writers, file packing, limits.

Published layout (all at the site root, so each sitemap may list any openalex.org URL without relying on
Search Console's cross-directory exception; Google: "unless you submit your sitemap through Search
Console, a sitemap affects only descendants of the parent directory"):
  /sitemap.xml                         flat index of EVERY sitemap file (Google rejects nested indexes)
  /sitemap-<group>.xml                 one index per group (submit each in Search Console, list in robots.txt)
  /sitemap-<group>-<NNNNN>.xml.gz      the URL files, <= 50,000 URLs and <= 50 MB uncompressed each
"""
from __future__ import annotations

import gzip
import hashlib
import io
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Iterator, Sequence
from xml.sax.saxutils import escape

BASE_URL = "https://openalex.org"
SITEMAP_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"
MAX_URLS_PER_FILE = 50_000            # sitemaps.org, Google and Bing limit
MAX_BYTES_UNCOMPRESSED = 50 * 1024 * 1024
MAX_FILES_PER_INDEX = 50_000
BUCKET_WIDTH = 10_000                 # id buckets for packing; densest range (authors) is ~6.3K rows per bucket

# ---------------------------------------------------------------------------------------------------------
# THE QUALITY RULE. Records below the bar keep their page (people and assistants use them) but carry
# <meta name="robots" content="noindex"> and stay out of sitemaps. RECOMMENDATION.md "Records with something
# to say": works with no abstract AND zero citations, paratext and xpac (extended corpus) are noindexed;
# authors with <= 1 work and no ORCID are noindexed; every source, institution, topic, funder and publisher
# is indexable.
#
# Delta columns: openalex.works.openalex_works.has_abstract == (abstract_inverted_index IS NOT NULL), the
# same test the ES sync uses for the API's has_abstract (checked: 0 disagreements). The API record has no
# has_abstract field, so is_indexable() tests abstract_inverted_index directly.
#
# ---------------------------------------------------------------------------------------------------------
# The website's copy of this rule is openalex-gui src/seo/indexable.mjs (noindexReason / isIndexable, track
# seo-content, commit 43c1e616). Both must pass notebooks/sitemaps/quality_rule_cases.json; change all three
# together. JS `hasAbstract` = abstract_inverted_index present with at least one key, hence the '{}' guard
# (0 such rows in Walden on 2026-10-15, but the rule is the rule). JS reads `orcid || ids.orcid`.
HAS_ABSTRACT_SQL = "(COALESCE(has_abstract, FALSE) AND COALESCE(abstract_inverted_index, '{}') <> '{}')"
HAS_ORCID_SQL = "(COALESCE(orcid, '') <> '' OR COALESCE(ids.orcid, '') <> '')"
WORK_INDEXABLE_SQL = (
    "(NOT COALESCE(is_xpac, FALSE) AND NOT COALESCE(is_paratext, FALSE)"
    f" AND ({HAS_ABSTRACT_SQL} OR COALESCE(cited_by_count, 0) > 0))"
)
AUTHOR_INDEXABLE_SQL = f"(COALESCE(works_count, 0) >= 2 OR {HAS_ORCID_SQL})"


def is_indexable(entity: str, record: dict) -> bool:
    """The quality rule over an API record (what the website has when it renders the page)."""
    if entity == "works":
        if record.get("is_xpac") or record.get("is_paratext"):
            return False
        return bool(record.get("abstract_inverted_index")) or (record.get("cited_by_count") or 0) > 0
    if entity == "authors":
        orcid = record.get("orcid") or (record.get("ids") or {}).get("orcid")
        return (record.get("works_count") or 0) >= 2 or bool(orcid)
    return entity in ENTITY_PATHS


# ---------------------------------------------------------------------------------------------------------
# Groups. `cohort_sql` splits an entity into its sitemap groups (Search Console reports index rates per
# submitted sitemap, so works with an abstract and works with only citations get separate numbers).
# `fingerprint_sql` hashes what the record page shows as its content: text, people, venue, links. Counters
# (citations, works counts, references count) and internal ids are left out on purpose: they change for
# 5-10% of records a night and Google uses lastmod only "if it's consistently and verifiably accurate"
# about "the last significant update to the page". Measured 2026-10-15 on 252K works (ids 2.000-2.001B):
# updated_date moved on 15% in one day and 30% in a week; this fingerprint on 5.6% and 10.7%. Authors
# (731K, ids 5.000-5.001B): updated_date 11% / 53% in six days; fingerprint 1.1% / 3.4%.
# ---------------------------------------------------------------------------------------------------------
WORK_FINGERPRINT_SQL = """xxhash64(to_json(named_struct(
  't', title, 'pd', CAST(publication_date AS STRING), 'ty', type, 'doi', doi, 'lang', language,
  'ret', is_retracted, 'abs', xxhash64(abstract_inverted_index),
  'au', transform(authorships, a -> named_struct('n', a.author.display_name, 'o', a.author.orcid,
                                                  'i', transform(a.institutions, i -> i.id))),
  'src', primary_location.source.id, 'srcn', primary_location.source.display_name,
  'lp', primary_location.landing_page_url, 'pdf', primary_location.pdf_url,
  'oa', open_access.is_oa, 'oau', open_access.oa_url, 'top', primary_topic.id, 'bib', biblio)))"""

AUTHOR_FINGERPRINT_SQL = """xxhash64(to_json(named_struct(
  'n', display_name, 'o', orcid, 'i', transform(last_known_institutions, i -> i.id))))"""


@dataclass(frozen=True)
class Entity:
    name: str                 # URL path segment: https://openalex.org/<name>/<prefix><id>
    prefix: str               # id letter
    table: str
    indexable_sql: str        # WHERE clause over `table`
    cohort_sql: str           # expression giving the group name
    fingerprint_sql: str
    groups: tuple             # every value cohort_sql can return, in publishing order


ENTITIES: tuple[Entity, ...] = (
    Entity("works", "W", "openalex.works.openalex_works", WORK_INDEXABLE_SQL,
           f"CASE WHEN {HAS_ABSTRACT_SQL} THEN 'works-abstract' ELSE 'works-cited' END",
           WORK_FINGERPRINT_SQL, ("works-abstract", "works-cited")),
    Entity("authors", "A", "openalex.authors.openalex_authors", AUTHOR_INDEXABLE_SQL,
           f"CASE WHEN {HAS_ORCID_SQL} THEN 'authors-orcid' ELSE 'authors-multi' END",
           AUTHOR_FINGERPRINT_SQL, ("authors-orcid", "authors-multi")),
    Entity("sources", "S", "openalex.sources.sources_api", "TRUE", "'sources'",
           """xxhash64(to_json(named_struct('n', display_name, 'issn', issn_l, 'h', host_organization_name,
              'u', homepage_url, 'ty', type, 'cc', country_code, 'oa', is_oa, 'doaj', is_in_doaj)))""",
           ("sources",)),
    Entity("institutions", "I", "openalex.institutions.institutions_api", "TRUE", "'institutions'",
           """xxhash64(to_json(named_struct('n', display_name, 'ror', ror, 'cc', country_code, 'ty', type,
              'u', homepage_url, 'st', status, 'l', lineage, 'alt', display_name_alternatives)))""",
           ("institutions",)),
    Entity("topics", "T", "openalex.common.topics_api", "TRUE", "'topics'",
           # keywords left out: their scores move nightly and the top-keyword set reshuffles (1,312 of 4,516
           # topics in six days by name alone, 2026-10-09 -> 10-15)
           """xxhash64(to_json(named_struct('n', display_name, 'd', description,
              'sf', subfield.id, 'f', field.id, 'dm', domain.id)))""",
           ("topics",)),
    Entity("funders", "F", "openalex.funders.funders_api", "TRUE", "'funders'",
           """xxhash64(to_json(named_struct('n', display_name, 'd', description, 'cc', country_code,
              'u', homepage_url, 'alt', alternate_titles)))""",
           ("funders",)),
    Entity("publishers", "P", "openalex.publishers.publishers_api", "TRUE", "'publishers'",
           """xxhash64(to_json(named_struct('n', display_name, 'u', homepage_url, 'hl', hierarchy_level,
              'pp', parent_publisher, 'cc', country_codes, 'alt', alternate_titles)))""",
           ("publishers",)),
)
ENTITY_PATHS = {e.name: e for e in ENTITIES}
CHANGED_GROUP = "changed"
ALL_GROUPS: tuple[str, ...] = tuple(g for e in ENTITIES for g in e.groups) + (CHANGED_GROUP,)


def current_records_sql(entity: Entity, where_extra: str = "", as_of: str | None = None) -> str:
    """SELECT of every indexable record of one entity: entity, id, grp, fp, updated_date, created_date.
    `as_of` (a Delta TIMESTAMP AS OF literal) is for the sample/backtest only."""
    tt = f" TIMESTAMP AS OF '{as_of}'" if as_of else ""
    extra = f" AND ({where_extra})" if where_extra else ""
    return (
        f"SELECT '{entity.name}' AS entity, CAST(id AS BIGINT) AS id, {entity.cohort_sql} AS grp,\n"
        f"  {entity.fingerprint_sql} AS fp,\n"
        f"  CAST(updated_date AS TIMESTAMP) AS updated_date, CAST(created_date AS TIMESTAMP) AS created_date\n"
        f"FROM {entity.table}{tt}\nWHERE id IS NOT NULL AND id > 0 AND {entity.indexable_sql}{extra}"
    )


# ---------------------------------------------------------------------------------------------------------
# lastmod: the time the page's content last changed, never the build time.
#   * record new to the sitemaps (new record, or newly over the bar): its own updated_date
#   * fingerprint changed: its updated_date if that moved past the previous lastmod (the pipeline's true
#     stamp for the change), else the run time (a change that rode a hash-rebaselined night unstamped)
#   * otherwise: unchanged
# Always capped at the run time (no future dates). `p` = previous state row, `c` = current record.
# ---------------------------------------------------------------------------------------------------------
def lastmod_sql(run_ts_literal: str) -> str:
    rt = f"TIMESTAMP'{run_ts_literal}'"
    upd = f"LEAST(COALESCE(c.updated_date, {rt}), {rt})"
    return (
        f"CASE WHEN p.id IS NULL THEN {upd}\n"
        f"     WHEN p.fp <> c.fp THEN CASE WHEN c.updated_date > p.lastmod THEN {upd} ELSE {rt} END\n"
        f"     ELSE p.lastmod END"
    )


def next_state_sql(current_view: str, prev_table: str | None, run_ts_literal: str) -> str:
    """New state rows (entity, id, grp, fp, lastmod, created_date, change) from the current records view
    and the previous state table. change: 'new' (not in previous state), 'updated' (fingerprint changed),
    '' (unchanged). Without a previous state (the seed run) every row is 'seed'."""
    if prev_table is None:
        rt = f"TIMESTAMP'{run_ts_literal}'"
        return (f"SELECT c.entity, c.id, c.grp, c.fp, LEAST(COALESCE(c.updated_date, {rt}), {rt}) AS lastmod,\n"
                f"  c.created_date, 'seed' AS change FROM {current_view} c")
    return (
        f"SELECT c.entity, c.id, c.grp, c.fp,\n  {lastmod_sql(run_ts_literal)} AS lastmod,\n"
        f"  c.created_date,\n"
        f"  CASE WHEN p.id IS NULL THEN 'new' WHEN p.fp <> c.fp THEN 'updated' ELSE '' END AS change\n"
        f"FROM {current_view} c LEFT JOIN {prev_table} p ON p.entity = c.entity AND p.id = c.id"
    )


def removed_sql(current_view: str, prev_table: str) -> str:
    """Records in the previous state that left the sitemaps (merged, deleted, or fell under the bar)."""
    return (f"SELECT p.entity, p.id FROM {prev_table} p "
            f"LEFT ANTI JOIN {current_view} c ON c.entity = p.entity AND c.id = p.id")


def changed_sql(state_view: str, run_ts_literal: str, days: int, max_urls: int) -> str:
    """The rolling 'changed' group: records whose lastmod is within `days` of the run, records created in
    that window first (newest first), then the rest by lastmod, newest first; capped at max_urls. On a wave
    night (tens of millions of re-stamps) the cap keeps the file a fast lane for the newest changes; every
    other changed record still carries its new lastmod in its own group's files."""
    rt = f"TIMESTAMP'{run_ts_literal}'"
    return (
        f"SELECT entity, id, lastmod,\n"
        f"  CASE WHEN created_date >= {rt} - INTERVAL {int(days)} DAYS THEN 0 ELSE 1 END AS prio\n"
        f"FROM {state_view}\nWHERE lastmod >= {rt} - INTERVAL {int(days)} DAYS\n"
        f"ORDER BY prio, lastmod DESC, entity, id\nLIMIT {int(max_urls)}"
    )


CHANGED_ORDER = ("prio", "lastmod DESC", "entity", "id")   # the same order, for numbering the rows


# ---------------------------------------------------------------------------------------------------------
# URLs, names, XML
# ---------------------------------------------------------------------------------------------------------
# Spark formats lastmod once, in SQL, so executors never touch time zones (session time zone set to UTC).
LASTMOD_W3C_SQL = "date_format(lastmod, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")"


def record_url(entity: str, record_id: int) -> str:
    """Canonical long form, e.g. https://openalex.org/works/W2741809807 (same as the page's canonical tag)."""
    return f"{BASE_URL}/{entity}/{ENTITY_PATHS[entity].prefix}{int(record_id)}"


def file_name(group: str, n: int) -> str:
    return f"sitemap-{group}-{n:05d}.xml.gz"


def group_index_name(group: str) -> str:
    return f"sitemap-{group}.xml"


TOP_INDEX_NAME = "sitemap.xml"


def w3c(ts) -> str:
    """W3C Datetime in UTC with seconds, e.g. 2026-10-14T05:12:33Z (Bing asks for date and time)."""
    if ts is None:
        raise ValueError("lastmod must not be None")
    if isinstance(ts, str) and len(ts) == 20 and ts[10] == "T" and ts[-1] == "Z":
        return ts  # already formatted by Spark (LASTMOD_W3C_SQL); the hot path in the nightly
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_URLSET_HEAD = f'<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="{SITEMAP_NS}">\n'.encode()
_URLSET_TAIL = b"</urlset>\n"
_INDEX_HEAD = f'<?xml version="1.0" encoding="UTF-8"?>\n<sitemapindex xmlns="{SITEMAP_NS}">\n'.encode()
_INDEX_TAIL = b"</sitemapindex>\n"


def urlset_bytes(rows: Iterable[tuple[str, object]]) -> tuple[bytes, int, str | None]:
    """rows: (url, lastmod) in output order. Returns (uncompressed XML, url count, max lastmod as W3C).
    Raises if a limit is broken, so a bad file is never published."""
    buf = io.BytesIO()
    buf.write(_URLSET_HEAD)
    n, mx = 0, None
    for url, lastmod in rows:
        lm = w3c(lastmod)
        mx = lm if mx is None or lm > mx else mx
        buf.write(f"<url><loc>{escape(url)}</loc><lastmod>{lm}</lastmod></url>\n".encode())
        n += 1
    buf.write(_URLSET_TAIL)
    data = buf.getvalue()
    if n > MAX_URLS_PER_FILE:
        raise ValueError(f"{n} URLs in one sitemap (limit {MAX_URLS_PER_FILE})")
    if len(data) > MAX_BYTES_UNCOMPRESSED:
        raise ValueError(f"{len(data)} bytes uncompressed (limit {MAX_BYTES_UNCOMPRESSED})")
    return data, n, mx


def sitemapindex_bytes(entries: Sequence[tuple[str, str | None]]) -> bytes:
    """entries: (absolute sitemap URL, lastmod W3C or None)."""
    if len(entries) > MAX_FILES_PER_INDEX:
        raise ValueError(f"{len(entries)} sitemaps in one index (limit {MAX_FILES_PER_INDEX})")
    out = [_INDEX_HEAD]
    for loc, lm in entries:
        lm_tag = f"<lastmod>{lm}</lastmod>" if lm else ""
        out.append(f"<sitemap><loc>{escape(loc)}</loc>{lm_tag}</sitemap>\n".encode())
    out.append(_INDEX_TAIL)
    data = b"".join(out)
    if len(data) > MAX_BYTES_UNCOMPRESSED:
        raise ValueError("sitemap index over 50 MB")
    return data


def gzip_deterministic(data: bytes) -> bytes:
    """mtime=0 and no file name, so the same content always gives the same bytes (and the same ETag/MD5):
    the uploader skips files whose bytes didn't change."""
    buf = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0, compresslevel=6) as gz:
        gz.write(data)
    return buf.getvalue()


def md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()  # S3/R2 single-part ETag


@dataclass
class FileStat:
    group: str
    n: int                      # file number within the group, from 1
    name: str
    urls: int
    lastmod: str | None         # max lastmod of its URLs (the index entry's lastmod)
    bytes_xml: int
    bytes_gz: int
    md5: str
    first_id: int | None = None
    last_id: int | None = None
    extra: dict = field(default_factory=dict)


def build_file(group: str, n: int, rows: Iterable[tuple[str, int, object]]) -> tuple[bytes, FileStat]:
    """rows: (entity, id, lastmod) already in output order. Returns (gzipped bytes, stats)."""
    first = last = None

    def gen():
        nonlocal first, last
        for entity, rid, lm in rows:
            if first is None:
                first = rid
            last = rid
            yield record_url(entity, rid), lm

    xml, count, mx = urlset_bytes(gen())
    gz = gzip_deterministic(xml)
    return gz, FileStat(group, n, file_name(group, n), count, mx, len(xml), len(gz), md5_hex(gz), first, last)


def build_indexes(stats: Sequence[FileStat], base_url: str = BASE_URL) -> dict[str, bytes]:
    """Group indexes plus the flat top index. Returns {published name: bytes}."""
    by_group: dict[str, list[FileStat]] = {}
    for s in sorted(stats, key=lambda s: (ALL_GROUPS.index(s.group) if s.group in ALL_GROUPS else 99, s.n)):
        by_group.setdefault(s.group, []).append(s)
    out = {}
    flat = []
    for g, files in by_group.items():
        entries = [(f"{base_url}/{s.name}", s.lastmod) for s in files]
        out[group_index_name(g)] = sitemapindex_bytes(entries)
        flat.extend(entries)
    out[TOP_INDEX_NAME] = sitemapindex_bytes(flat)
    return out


# ---------------------------------------------------------------------------------------------------------
# Packing: consecutive id buckets into files of <= max_per_file URLs, per group, in id order. Needs only a
# small (group, bucket, count) histogram on the driver; no global window over hundreds of millions of rows.
# ---------------------------------------------------------------------------------------------------------
def pack_buckets(hist: Iterable[tuple[str, int, int]], max_per_file: int = MAX_URLS_PER_FILE
                 ) -> dict[tuple[str, int], int]:
    """hist: (group, bucket, count). Returns {(group, bucket): file number from 1}. A single bucket larger
    than max_per_file is an error (shrink BUCKET_WIDTH)."""
    per_group: dict[str, list[tuple[int, int]]] = {}
    for g, b, c in hist:
        per_group.setdefault(g, []).append((int(b), int(c)))
    assign = {}
    for g, buckets in per_group.items():
        n, filled = 1, 0
        for b, c in sorted(buckets):
            if c > max_per_file:
                raise ValueError(f"bucket {b} of {g} holds {c} rows (> {max_per_file}); lower BUCKET_WIDTH")
            if filled + c > max_per_file:
                n, filled = n + 1, 0
            assign[(g, b)] = n
            filled += c
    return assign


def chunks(seq: Sequence, size: int) -> Iterator[Sequence]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def validate_published_set(stats: Sequence[FileStat], expected_counts: dict[str, int]) -> list[str]:
    """Checks before anything is published. Returns a list of problems (empty = OK)."""
    problems = []
    got: dict[str, int] = {}
    for s in stats:
        got[s.group] = got.get(s.group, 0) + s.urls
        if s.urls == 0:
            problems.append(f"{s.name}: empty file")
        if s.urls > MAX_URLS_PER_FILE or s.bytes_xml > MAX_BYTES_UNCOMPRESSED:
            problems.append(f"{s.name}: over the limits ({s.urls} URLs, {s.bytes_xml} bytes)")
    for g, want in expected_counts.items():
        if got.get(g, 0) != want:
            problems.append(f"group {g}: {got.get(g, 0)} URLs written, {want} expected")
    names = [s.name for s in stats]
    if len(names) != len(set(names)):
        problems.append("duplicate file names")
    if len(stats) > MAX_FILES_PER_INDEX:
        problems.append(f"{len(stats)} files: the flat /sitemap.xml index would exceed 50,000 entries")
    return problems
