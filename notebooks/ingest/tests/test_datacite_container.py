"""DataCite.py's `ids` column with the container ISSN (oxjob #693), run on local Spark over DataCite-shaped records.

The `ids` expression of datacite_parsed and its helper are cut from the notebook with ast and run with real pyspark, so
the test covers the concat, its types and the dedup as the pipeline runs them. Needs a real pyspark + JVM (a dev venv,
a cluster); elsewhere it skips. CI does not run pytest.
"""
import ast
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
if not hasattr(pyspark, "__version__"):  # a pyspark stub, not real Spark
    pytest.skip("pyspark stub, not real Spark", allow_module_level=True)

SRC = (Path(__file__).resolve().parents[1] / "DataCite.py").read_text()


def _ids_column():
    """datacite_parsed's .withColumn("ids", <expr>) as a Column, with the notebook's own helper."""
    import pyspark.sql.functions as F
    tree = ast.parse(SRC)
    helper = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_container_issn_ids"]
    parsed = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "datacite_parsed")
    expr = [c.args[1] for c in ast.walk(parsed) if isinstance(c, ast.Call) and getattr(c.func, "attr", "") == "withColumn"
            and isinstance(c.args[0], ast.Constant) and c.args[0].value == "ids"]
    assert len(helper) == 1 and len(expr) == 1
    ns = {"F": F}
    exec(compile(ast.Module(body=helper, type_ignores=[]), "DataCite.py", "exec"), ns)
    return eval(compile(ast.Expression(expr[0]), "DataCite.py", "eval"), ns)


def rel(issn, kind="ISSN", relation="IsPartOf"):
    return {"relatedIdentifier": issn, "relatedIdentifierType": kind, "relationType": relation}


def rec(doi, container, related=()):
    return {"id": doi, "attributes": {"container": container, "relatedIdentifiers": list(related)},
            "relationships": {"client": {"data": {"id": "client.x"}}}}


TGDK = {"type": "Series", "identifier": "2942-7517", "identifierType": "ISSN", "volume": "3", "issue": "3",
        "firstPage": "5:1", "lastPage": "5:22", "title": "Transactions on Graph Data and Knowledge (TGDK)"}
JNSC = {"type": "Series", "identifier": "2193-8865", "identifierType": "EISSN", "volume": "16", "issue": "5",
        "title": "Journal of Nanostructure in Chemistry"}
RECORDS = [
    rec("tgdk", TGDK, [rel("2942-7517")]),  # 10.4230/tgdk.3.3.5: an IsPartOf relatedIdentifier carries the ISSN already
    rec("tgdk-unhyphenated", TGDK, [rel("29427517")]),
    rec("jnsc", JNSC),  # 10.57647/jnsc.2026.1605.26: the e-ISSN is only in container (relatedItems)
    rec("jnsc-cited", JNSC, [rel("2193-8865", relation="References")]),  # a citation is not the container
    rec("lissn", {"identifier": "0035-2411", "identifierType": "LISSN"}, [rel("10.1/x", "DOI", "IsSupplementTo")]),
    rec("issn-typed-junk", {"identifier": "", "identifierType": "ISSN"}),
    # 10.11588/xxi.2020.1.73101 as first deposited: a comma-split seriesInformation named by DOI
    rec("series-doi", {"identifier": "10.11588/xxi.2020.1", "identifierType": "DOI", "volume": "History",
                       "title": "21: Inquiries into Art"}),
    rec("no-container", None),
]


@pytest.fixture(scope="module")
def out():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType
    try:
        spark = SparkSession.builder.master("local[1]").appName("datacite_container").getOrCreate()
    except Exception as e:  # pragma: no cover - no JVM
        pytest.skip(f"no local Spark: {e}")
    s = lambda *names: StructType([StructField(n, StringType()) for n in names])  # noqa: E731
    schema = StructType([
        StructField("id", StringType()),
        StructField("attributes", StructType([
            StructField("container", s("type", "identifier", "identifierType", "title", "volume", "issue", "firstPage",
                                       "lastPage", "number", "edition")),
            StructField("relatedIdentifiers", ArrayType(s("relatedIdentifier", "relatedIdentifierType", "relationType"))),
        ])),
        StructField("relationships", StructType([StructField("client", StructType([
            StructField("data", s("id"))]))])),
    ])
    df = spark.createDataFrame(RECORDS, schema).select("id", _ids_column().alias("ids"))
    yield {r.id: [(x.id, x.namespace, x.relationship) for x in r.ids] for r in df.collect()}
    spark.stop()


def container_entries(ids):
    return [x for x in ids if x[2] == "IsPublishedIn"]


def test_container_issn_joins_ids(out):
    assert out["jnsc"] == [("jnsc", "doi", "self"), ("client.x", "datacite_client", "self"),
                           ("2193-8865", "eissn", "IsPublishedIn")]
    assert container_entries(out["lissn"]) == [("0035-2411", "lissn", "IsPublishedIn")]
    assert container_entries(out["jnsc-cited"]) == [("2193-8865", "eissn", "IsPublishedIn")]


def test_no_second_copy_of_an_ispartof_issn(out):
    assert out["tgdk"] == [("tgdk", "doi", "self"), ("2942-7517", "issn", "IsPartOf"),
                           ("client.x", "datacite_client", "self")]
    assert container_entries(out["tgdk-unhyphenated"]) == []


def test_non_issn_or_junk_container_adds_nothing(out):
    for doi in ("issn-typed-junk", "series-doi", "no-container"):
        assert container_entries(out[doi]) == [], doi
        assert out[doi][0] == (doi, "doi", "self"), doi
