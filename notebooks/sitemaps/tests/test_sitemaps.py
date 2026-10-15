"""Offline tests for utils/sitemaps.py and utils/indexnow.py (oxjob #1689).

Run: python3 -m pytest notebooks/sitemaps/tests -q   (needs lxml for the schema checks)
"""
import gzip
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO))

from utils import indexnow as ix  # noqa: E402
from utils import sitemaps as sm  # noqa: E402

CASES = json.loads((HERE.parent / "quality_rule_cases.json").read_text())["cases"]


# ------------------------------------------------------------------------------------------- quality rule
@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_quality_rule_python(case):
    assert sm.is_indexable(case["entity"], case["record"]) is case["expected"]


def _delta_row(entity, rec):
    """API record -> the Walden columns the SQL predicate reads."""
    if entity == "works":
        return {"is_xpac": rec.get("is_xpac"), "is_paratext": rec.get("is_paratext"),
                "has_abstract": rec.get("abstract_inverted_index") is not None,
                "cited_by_count": rec.get("cited_by_count")}
    return {"works_count": rec.get("works_count"), "orcid": rec.get("orcid")}


@pytest.mark.parametrize("case", [c for c in CASES if c["entity"] in ("works", "authors")],
                         ids=[c["name"] for c in CASES if c["entity"] in ("works", "authors")])
def test_quality_rule_sql_matches(case):
    """The exact SQL predicate string the nightly runs, evaluated in SQLite on the same cases."""
    row = _delta_row(case["entity"], case["record"])
    pred = sm.WORK_INDEXABLE_SQL if case["entity"] == "works" else sm.AUTHOR_INDEXABLE_SQL
    db = sqlite3.connect(":memory:")
    cols = list(row)
    db.execute(f"CREATE TABLE t ({', '.join(cols)})")
    db.execute(f"INSERT INTO t VALUES ({', '.join('?' * len(cols))})", [row[c] for c in cols])
    got = db.execute(f"SELECT {pred} FROM t").fetchone()[0]
    assert bool(got) is case["expected"]


def test_keywords_not_in_sitemaps():
    assert sm.is_indexable("keywords", {}) is False


def test_every_entity_has_its_groups_and_prefix():
    assert [e.prefix for e in sm.ENTITIES] == ["W", "A", "S", "I", "T", "F", "P"]
    assert len(sm.ALL_GROUPS) == len(set(sm.ALL_GROUPS)) == 10
    for e in sm.ENTITIES:
        sql = sm.current_records_sql(e)
        assert e.table in sql and e.indexable_sql in sql and "fp" in sql
        for g in e.groups:
            assert f"'{g}'" in e.cohort_sql


# ------------------------------------------------------------------------------------------------ URLs
def test_record_url_is_canonical_long_form():
    assert sm.record_url("works", 2741809807) == "https://openalex.org/works/W2741809807"
    assert sm.record_url("topics", 10001) == "https://openalex.org/topics/T10001"
    assert sm.file_name("works-abstract", 7) == "sitemap-works-abstract-00007.xml.gz"


def test_w3c_lastmod():
    assert sm.w3c(datetime(2026, 10, 14, 5, 12, 33, 999)) == "2026-10-14T05:12:33Z"
    assert sm.w3c("2026-10-14T05:12:33.123Z") == "2026-10-14T05:12:33Z"
    with pytest.raises(ValueError):
        sm.w3c(None)


# ------------------------------------------------------------------------------------- XML vs the schema
def _schema(name):
    lxml = pytest.importorskip("lxml.etree")
    return lxml.XMLSchema(lxml.parse(str(HERE / name)))


def test_urlset_validates_against_sitemaps_org_schema():
    lxml = pytest.importorskip("lxml.etree")
    t = datetime(2026, 10, 14, 5, 0, tzinfo=timezone.utc)
    gz, st = sm.build_file("works-abstract", 1, [("works", 2741809807, t), ("authors", 5023888391, t)])
    xml = gzip.decompress(gz)
    _schema("sitemap.xsd").assertValid(lxml.fromstring(xml))
    assert st.urls == 2 and st.lastmod == "2026-10-14T05:00:00Z" and st.first_id == 2741809807
    assert st.md5 == sm.md5_hex(gz)


def test_indexes_validate_and_flat_top_index_has_no_nesting():
    lxml = pytest.importorskip("lxml.etree")
    t = datetime(2026, 10, 14, tzinfo=timezone.utc)
    stats = [sm.build_file(g, n, [("works", 1000 + n, t)])[1]
             for g, n in [("works-abstract", 1), ("works-abstract", 2), ("sources", 1), ("changed", 1)]]
    out = sm.build_indexes(stats)
    assert set(out) == {"sitemap.xml", "sitemap-works-abstract.xml", "sitemap-sources.xml", "sitemap-changed.xml"}
    schema = _schema("siteindex.xsd")
    for data in out.values():
        schema.assertValid(lxml.fromstring(data))
    top = out["sitemap.xml"].decode()
    assert top.count("<sitemap>") == 4 and "sitemap-works-abstract.xml<" not in top  # lists files, not indexes
    assert top.index("works-abstract-00001") < top.index("sources-00001") < top.index("changed-00001")


def test_gzip_is_deterministic():
    assert sm.gzip_deterministic(b"abc") == sm.gzip_deterministic(b"abc")


def test_limits_enforced():
    t = datetime(2026, 1, 1)
    with pytest.raises(ValueError):
        sm.urlset_bytes((f"https://openalex.org/works/W{i}", t) for i in range(sm.MAX_URLS_PER_FILE + 1))
    with pytest.raises(ValueError):
        sm.sitemapindex_bytes([("https://openalex.org/x", None)] * (sm.MAX_FILES_PER_INDEX + 1))


def test_full_file_size_is_well_under_50mb():
    t = datetime(2026, 10, 14, 5, 12, 33)
    xml, n, _ = sm.urlset_bytes((sm.record_url("works", 7_221_846_771 - i), t) for i in range(50_000))
    assert n == 50_000 and len(xml) < 6 * 1024 * 1024


# ---------------------------------------------------------------------------------------------- packing
def test_pack_buckets_respects_limit_and_order():
    hist = [("a", 3, 30), ("a", 1, 30), ("a", 2, 30), ("a", 9, 50), ("b", 1, 10)]
    got = sm.pack_buckets(hist, max_per_file=60)
    assert got == {("a", 1): 1, ("a", 2): 1, ("a", 3): 2, ("a", 9): 3, ("b", 1): 1}
    with pytest.raises(ValueError):
        sm.pack_buckets([("a", 1, 61)], max_per_file=60)


def test_validate_published_set():
    t = datetime(2026, 10, 14)
    st = [sm.build_file("sources", 1, [("sources", 1, t), ("sources", 2, t)])[1]]
    assert sm.validate_published_set(st, {"sources": 2}) == []
    assert sm.validate_published_set(st, {"sources": 3})


# -------------------------------------------------------------------------------------- lastmod rule SQL
def test_lastmod_rule_in_sqlite():
    """The lastmod CASE, run on rows covering each branch (SQLite stands in for Spark SQL)."""
    rt = "2026-10-15 20:00:00"
    sql = sm.lastmod_sql(rt).replace(f"TIMESTAMP'{rt}'", f"'{rt}'").replace("LEAST", "MIN")
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE c (id, fp, updated_date)")
    db.execute("CREATE TABLE p (id, fp, lastmod)")
    db.executemany("INSERT INTO c VALUES (?,?,?)", [
        (1, 10, "2026-10-15 05:30:00"),   # new to the sitemap -> its updated_date
        (2, 20, "2026-10-15 05:30:00"),   # fp changed, stamped -> updated_date
        (3, 30, "2026-10-01 00:00:00"),   # fp changed, not stamped (rebaselined night) -> run time
        (4, 40, "2026-10-15 05:30:00"),   # fp same, re-stamped by a counter change -> keep old lastmod
        (5, 50, "2026-10-16 00:00:00"),   # new, future stamp -> capped at run time
    ])
    db.executemany("INSERT INTO p VALUES (?,?,?)", [
        (2, 21, "2026-09-01 00:00:00"), (3, 31, "2026-10-02 00:00:00"), (4, 40, "2026-08-01 00:00:00")])
    got = dict(db.execute(f"SELECT c.id, {sql} FROM c LEFT JOIN p ON p.id = c.id").fetchall())
    assert got == {1: "2026-10-15 05:30:00", 2: "2026-10-15 05:30:00", 3: rt, 4: "2026-08-01 00:00:00", 5: rt}


# --------------------------------------------------------------------------------------------- IndexNow
def test_indexnow_key_and_payload():
    assert ix.KEY_RE.match(ix.KEY)
    body = ix.payload(["https://openalex.org/works/W1"])
    assert body == {"host": "openalex.org", "key": ix.KEY, "keyLocation": f"https://openalex.org/{ix.KEY}.txt",
                    "urlList": ["https://openalex.org/works/W1"]}
    with pytest.raises(ValueError):
        ix.payload(["https://example.com/works/W1"])
    with pytest.raises(ValueError):
        ix.payload([f"https://openalex.org/works/W{i}" for i in range(10_001)])


def test_indexnow_batches_dedup_and_split():
    urls = [f"https://openalex.org/works/W{i}" for i in range(25_000)] + ["https://openalex.org/works/W0"]
    bs = list(ix.batches(urls))
    assert [len(b) for b in bs] == [10_000, 10_000, 5_000]


def test_indexnow_dry_run_sends_nothing():
    calls = []
    res = ix.ping([f"https://openalex.org/works/W{i}" for i in range(12_000)], send=False,
                  poster=lambda *a: calls.append(a) or 200, log=lambda m: None, sleep=lambda s: None)
    assert calls == [] and res.dry_run and res.sent_urls == 12_000 and res.sent_batches == 2


def test_indexnow_cap_retry_and_stop():
    statuses = iter([429, 200, 200, 403])
    calls = []

    def poster(url, body):
        calls.append(len(body["urlList"]))
        return next(statuses)

    waits = []
    res = ix.ping([f"https://openalex.org/works/W{i}" for i in range(35_000)], send=True, max_urls=25_000,
                  poster=poster, log=lambda m: None, sleep=waits.append)
    # batch 1: 429 then 200; batch 2: 200; batch 3 trimmed to the cap (5,000): 403 -> stop
    assert calls == [10_000, 10_000, 10_000, 5_000]
    assert res.sent_urls == 20_000 and res.stopped_reason == "HTTP 403 on batch 3"
    assert waits[0] == 30
