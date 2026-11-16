"""DataCite container-ISSN tier (CreateLocationsWithSources cell 1), run on local Spark (oxjob #693).

Runs the notebook's own datacite_issn_journal_map / datacite_issn_exploded / datacite_issn_candidates SQL, cut from the
notebook by name, over small temp views. Needs a real pyspark + JVM (a dev venv, a cluster); elsewhere it skips. CI does not run
pytest.
"""
import json
import re
from datetime import date
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
if not hasattr(pyspark, "__version__"):  # a pyspark stub, not real Spark
    pytest.skip("pyspark stub, not real Spark", allow_module_level=True)

NB = Path(__file__).resolve().parents[1] / "CreateLocationsWithSources.ipynb"
SQL = "".join(json.loads(NB.read_text())["cells"][1]["source"])


def cte(name):
    """The body of CTE `name` in cell 1: from its '(' to the matching ')', skipping strings and -- comments."""
    m = re.search(rf"(?:\bwith |\n){name} as \(", SQL, flags=re.I)
    assert m, name
    depth, quote, i = 0, False, m.end() - 1
    while True:
        ch = SQL[i]
        if quote:
            quote = ch != "'" or SQL[i - 1] == "\\"
        elif SQL.startswith("--", i):
            i = SQL.index("\n", i)
            continue
        elif ch == "'":
            quote = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return SQL[m.end():i]
        i += 1


FIX_TABLE = "openalex.works.datacite_container_issn_fix"  # oxjob #693 backlog table, a temp view here
TIER = ",\n".join(f"{n} as ({cte(n)})" for n in
                   ("datacite_issn_journal_map", "datacite_issn_exploded", "datacite_issn_candidates")
                   ).replace(FIX_TABLE, "datacite_container_issn_fix")

J1, J2 = 101, 202  # J1 holds ISSN 1111-1111 and e-ISSN 2222-2222; J2 holds 3333-3333
SOURCES = [(J1, "journal", ["1111-1111", "2222-2222"], True), (J2, "journal", ["3333-3333"], False),
           (303, "repository", ["4444-4444"], False)]


DAY = date(2026, 1, 9)


def loc(doi, *ids, raw_type="JournalArticle"):
    return ("datacite", "doi", doi, raw_type, DAY, [{"id": i, "namespace": ns, "relationship": rel} for ns, i, rel in ids])


LOCATIONS = [
    loc("10/issn", ("issn", "1111-1111", "IsPartOf")),
    loc("10/eissn-only", ("eissn", "2222-2222", "IsPublishedIn")),  # e-ISSN only: was dropped before #693
    loc("10/lissn-only", ("lissn", "1111-1111", "IsPartOf")),
    loc("10/print-and-e", ("issn", "1111-1111", "IsPartOf"), ("eissn", "2222-2222", "IsPartOf")),  # one journal: attach
    loc("10/two-journals", ("issn", "1111-1111", "IsPartOf"), ("eissn", "3333-3333", "IsPartOf")),  # ambiguous: none
    loc("10/not-container", ("eissn", "2222-2222", "References")),  # a citation, not the container
    loc("10/dataset", ("eissn", "2222-2222", "IsPartOf"), raw_type="Dataset"),  # genre guard
    loc("10/repository-issn", ("issn", "4444-4444", "IsPartOf")),  # ISSN of a non-journal source
    loc("10/unhyphenated", ("eissn", "22222222", "ispublishedin")),
    loc("10/other-namespaces", ("isbn", "2222-2222", "IsPartOf"), ("url", "2222-2222", "IsPartOf"),
        (None, "2222-2222", "IsPartOf"), ("eissn", None, "IsPartOf"), ("eissn", "2222-2222", None)),
]

# oxjob #693 backlog: records parsed before 6022b344 carry the container ISSN only in the fix table
LOCATIONS += [
    loc("10/fix-only"),  # Journal of Nanostructure in Chemistry's case: nothing in ids
    loc("10/fix-and-ids-same-journal", ("issn", "1111-1111", "IsPartOf")),
    loc("10/fix-other-journal", ("issn", "1111-1111", "IsPartOf")),  # ids say J1, the fix row says J2: ambiguous, none
    loc("10/fix-stale"),  # re-sent since the fix was built: its updated_date moved on, the row no longer counts
    loc("10/fix-dataset", raw_type="Dataset"),  # genre guard still applies
]
FIXES = [("doi", "10/fix-only", "2222-2222", DAY), ("doi", "10/fix-and-ids-same-journal", "2222-2222", DAY),
         ("doi", "10/fix-other-journal", "3333-3333", DAY), ("doi", "10/fix-stale", "2222-2222", date(2025, 1, 1)),
         ("doi", "10/fix-dataset", "2222-2222", DAY)]


@pytest.fixture(scope="module")
def candidates():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import ArrayType, BooleanType, DateType, LongType, StringType, StructField, StructType
    try:
        spark = SparkSession.builder.master("local[1]").appName("datacite_issn_tier").getOrCreate()
    except Exception as e:  # pragma: no cover - no JVM
        pytest.skip(f"no local Spark: {e}")
    spark.createDataFrame(SOURCES, StructType([
        StructField("id", LongType()), StructField("type", StringType()),
        StructField("issns", ArrayType(StringType())), StructField("is_oa", BooleanType())])
    ).createOrReplaceTempView("sources_filtered")
    ids_type = ArrayType(StructType([StructField(n, StringType()) for n in ("id", "namespace", "relationship")]))
    spark.createDataFrame(LOCATIONS, StructType([
        StructField("provenance", StringType()), StructField("native_id_namespace", StringType()),
        StructField("native_id", StringType()), StructField("raw_type", StringType()),
        StructField("updated_date", DateType()), StructField("ids", ids_type)])
    ).createOrReplaceTempView("all_locations")
    spark.createDataFrame(FIXES, StructType([
        StructField("native_id_namespace", StringType()), StructField("native_id", StringType()),
        StructField("issn", StringType()), StructField("fix_updated_date", DateType())])
    ).createOrReplaceTempView("datacite_container_issn_fix")
    rows = spark.sql(f"with {TIER} select native_id, issn_journal_id from datacite_issn_candidates").collect()
    yield {r.native_id: r.issn_journal_id for r in rows}
    spark.stop()


def test_issn_still_attaches(candidates):
    assert candidates["10/issn"] == J1


def test_eissn_and_lissn_attach_like_crossref(candidates):
    assert candidates["10/eissn-only"] == J1
    assert candidates["10/lissn-only"] == J1
    assert candidates["10/unhyphenated"] == J1


def test_print_and_e_issn_of_one_journal_attach(candidates):
    assert candidates["10/print-and-e"] == J1


def test_issns_of_two_journals_match_nothing(candidates):
    assert "10/two-journals" not in candidates


def test_relation_genre_and_source_type_guards_hold(candidates):
    for doi in ("10/not-container", "10/dataset", "10/repository-issn", "10/other-namespaces"):
        assert doi not in candidates, doi


def test_backlog_fix_rows_attach_like_ids(candidates):
    assert candidates["10/fix-only"] == J1
    assert candidates["10/fix-and-ids-same-journal"] == J1


def test_backlog_fix_rows_keep_the_tier_guards(candidates):
    for doi in ("10/fix-other-journal", "10/fix-stale", "10/fix-dataset"):
        assert doi not in candidates, doi
