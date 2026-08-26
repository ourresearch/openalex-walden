"""Shared pyspark stub for the dlt_utils unit tests.

Several modules in openalex.dlt import pyspark at module scope for UDF wrappers and DataFrame
helpers, but the logic worth unit-testing (type vocabulary, hashing, filter wiring) is pure
Python. Outside Databricks pyspark is normally absent, so this installs a stub good enough to
import those modules and inspect the expressions they build.

When real pyspark IS importable (e.g. running these on a cluster) the stub is skipped entirely
and the real library is used.
"""
import sys
import types


class FakeCol:
    """A recorded column expression -- lets tests assert on what was built."""

    def __init__(self, name):
        self.name = name

    def desc(self):
        return FakeCol(f"{self.name} DESC")

    def asc(self):
        return FakeCol(f"{self.name} ASC")

    def desc_nulls_last(self):
        return FakeCol(f"{self.name} DESC NULLS LAST")

    def over(self, window):
        return FakeCol(f"{self.name} OVER {window!r}")

    def __eq__(self, other):
        return isinstance(other, FakeCol) and other.name == self.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return f"<{self.name}>"


class FakeWindow:
    last = None

    def __init__(self):
        self.keys = None
        self.order = None

    @classmethod
    def partitionBy(cls, *keys):
        w = cls()
        w.keys = keys
        FakeWindow.last = w
        return w

    def orderBy(self, *order):
        self.order = order
        return self


def _install():
    def mk(name):
        m = types.ModuleType(name)
        sys.modules[name] = m
        return m

    ps, sql = mk("pyspark"), mk("pyspark.sql")
    fn = mk("pyspark.sql.functions")
    ty = mk("pyspark.sql.types")
    win = mk("pyspark.sql.window")
    ps.sql = sql
    sql.functions, sql.types, sql.window = fn, ty, win

    fn.col = FakeCol
    fn.lit = FakeCol
    fn.xxhash64 = lambda *cols: FakeCol("xxhash64(" + ",".join(c.name for c in cols) + ")")
    fn.row_number = lambda: FakeCol("row_number")
    fn.lower = lambda c: FakeCol(f"lower({c.name})")
    fn.trim = lambda c: FakeCol(f"trim({c.name})")
    fn.length = lambda c: FakeCol(f"length({c.name})")
    fn.pandas_udf = lambda *a, **k: (lambda f: f)
    fn.udf = lambda f, *a, **k: f
    for n in ("StringType", "StructField", "StructType", "ArrayType", "BooleanType",
              "TimestampType", "DateType"):
        setattr(ty, n, type(n, (), {"__init__": lambda self, *a, **k: None}))
    win.Window = FakeWindow


try:  # pragma: no cover - depends on the environment, not the code
    import pyspark  # noqa: F401
except ImportError:
    _install()
