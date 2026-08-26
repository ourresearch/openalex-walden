"""Tests for deterministic record ordering (oxjob #837 / #881).

The end-to-end property -- "two runs pick the same winner" -- needs a real SparkSession and
belongs in a cluster check. What is covered here is the pure logic a future edit could silently
break: the content hash must not depend on schema column order, and it must be the LAST ordering
term so it only ever breaks genuine ties.
"""

from openalex.dlt.sequencing import content_hash, dedupe_by_sequence

from conftest import FakeCol, FakeWindow


class FakeDF:
    """Minimal DataFrame stand-in: enough surface for dedupe_by_sequence's call chain."""

    def __init__(self, columns):
        self.columns = columns

    def withColumn(self, name, expr):
        return self

    def filter(self, cond):
        return self

    def drop(self, *cols):
        return self


def test_content_hash_is_independent_of_column_order():
    """A select reorder must not silently change which duplicate wins."""
    a = content_hash(FakeDF(["native_id", "title", "updated_date"]))
    b = content_hash(FakeDF(["updated_date", "native_id", "title"]))
    assert a == b


def test_content_hash_changes_with_the_column_set():
    # dropping a column from the hash genuinely changes row identity -- not something to do quietly
    assert content_hash(FakeDF(["native_id", "title"])) != content_hash(FakeDF(["native_id"]))


def test_content_hash_honours_an_explicit_column_list():
    df = FakeDF(["native_id", "title", "abstract"])
    assert content_hash(df, ["native_id"]) == content_hash(FakeDF(["native_id"]))


def test_hash_is_the_last_ordering_term():
    """The hash must break ties only AFTER the real ordering columns."""
    df = FakeDF(["native_id", "updated_date"])
    dedupe_by_sequence(df, keys=["native_id"],
                       order_by=[FakeCol("updated_date").desc_nulls_last()])
    order = FakeWindow.last.order
    assert order[0] == FakeCol("updated_date DESC NULLS LAST")
    assert order[-1].name.startswith("xxhash64(")
    assert len(order) == 2


def test_partitions_by_the_supplied_keys():
    df = FakeDF(["native_id", "updated_date"])
    dedupe_by_sequence(df, keys=["native_id"], order_by=[FakeCol("updated_date").desc()])
    assert FakeWindow.last.keys == ("native_id",)
