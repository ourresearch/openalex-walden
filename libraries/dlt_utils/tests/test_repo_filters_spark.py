"""Real-Spark behaviour test for apply_endpoint_filters (oxjob #1000; the #881 follow-up).

The contract tests in test_repo_filters.py run on the pyspark stub and cannot catch analysis
errors -- #881's first refresh died on a lambda arity that only Spark's analyzer sees. This
file runs only where a real pyspark + JVM are available (a dev venv, a cluster); elsewhere it
skips. CI does not run pytest.
"""
import pytest

pyspark = pytest.importorskip("pyspark")
if not hasattr(pyspark, "__version__"):  # the conftest stub
    pytest.skip("pyspark stub, not real Spark", allow_module_level=True)

import pyspark.sql.functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType, StringType, StructField, StructType)

from openalex.dlt.repo_filters import apply_endpoint_filters  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    try:
        s = SparkSession.builder.master("local[1]").appName("repo_filters").getOrCreate()
    except Exception as e:  # pragma: no cover - no JVM
        pytest.skip(f"no local Spark: {e}")
    yield s
    s.stop()


SCHEMA = StructType([
    StructField("native_id", StringType()), StructField("endpoint_id", StringType()),
    StructField("set_spec", ArrayType(StringType())),
    StructField("ids", ArrayType(StructType([
        StructField("id", StringType()), StructField("namespace", StringType()),
        StructField("relationship", StringType())]))),
    StructField("_change_type", StringType()),
])


def _doi(*dois):
    return [(d, "doi", None) for d in dois]


def test_figshare_component_carve(spark):
    rows = [
        # dropped: publisher components mirrored on figshare
        ("oai:figshare.com:article/1", "e1", None, _doi("10.1371/journal.pone.0274801.s001"), "upsert"),
        ("oai:figshare.com:article/2", "e1", None, _doi("10.1371/journal.ppat.1001173.t003"), "upsert"),
        ("oai:figshare.com:article/3", "e1", None, _doi("10.1371/journal.pgen.1011542.g001"), "upsert"),
        ("oai:figshare.com:article/4", "e1", None, _doi("10.1021/ACSABM.0C01427.S001"), "upsert"),
        # kept: figshare's own content and the parent article itself
        ("oai:figshare.com:article/5", "e1", None, _doi("10.6084/m9.figshare.12345678"), "upsert"),
        ("oai:figshare.com:article/6", "e1", None, _doi("10.1371/journal.pone.0274801"), "upsert"),
        ("oai:figshare.com:article/7", "e1", None, None, "upsert"),
        ("oai:figshare.com:article/8", "e1", None, [], "upsert"),
        ("oai:figshare.com:article/9", "e1", None, [("PMC123", "pmcid", None)], "upsert"),
        # kept: a delete event carries the pre-image and must bypass every rule
        ("oai:figshare.com:article/10", "e1", None, _doi("10.1371/journal.pone.0274801.s001"), "delete"),
        # kept: the same suffix off figshare -- OJS galleys, Technometrics
        ("oai:ojs.example.org:article/11", "e2", None, _doi("10.1234/abc.v8i2.1385.g599"), "upsert"),
        ("oai:tandfonline.example:12", "e3", None, _doi("10.1198/tech.2005.s303"), "upsert"),
        # kept: NULL native_id / NULL endpoint_id are "nothing to match", never a drop
        (None, "e1", None, _doi("10.1371/journal.pone.0274801.s001"), "upsert"),
        ("oai:other.org:13", None, None, _doi("10.1371/journal.pone.0274801.s001"), "upsert"),
        # dropped: an existing setSpec carve still fires alongside the new rule
        ("oai:figshare.com:article/14", "b6f3a90f96528af2baa", ["gallica:typedoc:objets"],
         _doi("10.6084/m9.figshare.1"), "upsert"),
    ]
    out = apply_endpoint_filters(spark.createDataFrame(rows, SCHEMA),
                                 keep_when=F.col("_change_type") == "delete")
    kept = sorted(r.native_id or "NULL" for r in out.collect())
    assert kept == sorted(
        [f"oai:figshare.com:article/{i}" for i in (5, 6, 7, 8, 9, 10)]
        + ["oai:ojs.example.org:article/11", "oai:tandfonline.example:12", "NULL", "oai:other.org:13"])
