"""Deterministic record ordering for the ingest pipelines (oxjob #837).

The problem this solves: `df.sort(...).dropDuplicates(keys)` looks ordered but is not.
`dropDuplicates` gives NO guarantee it keeps the first row of a preceding sort -- Spark is free
to keep any row of the group, and which one it keeps varies between runs and between cluster
shapes. Any rerun can therefore pick a different winner for the same key.

Repo.py solved this inline for the DLT union with a `_sequence` struct whose last element is a
content hash, giving a TOTAL order over rows that are otherwise tied. This module makes that
reusable so the batch ingests get the same guarantee -- RepoBackfill.py first, where
`repo_works_backfill` holds 82,121,583 duplicate native_ids over 228,564,455 distinct keys, so
the choice of winner is not hypothetical.
"""

import pyspark.sql.functions as F
from pyspark.sql.window import Window


def content_hash(df, columns=None):
    """A stable 64-bit hash of a row's content, for use as a final tiebreak.

    Column names are sorted so the hash does not silently change when someone reorders a
    select. (Repo.py's inline version hashes in schema order; sorting is strictly safer and
    these are separate tables, so they need not agree.)
    """
    cols = sorted(columns if columns is not None else df.columns)
    return F.xxhash64(*[F.col(c) for c in cols])


def dedupe_by_sequence(df, keys, order_by, hash_columns=None):
    """Keep exactly one row per `keys`, chosen by a TOTAL order.

    Args:
        df:      input DataFrame
        keys:    list of column names identifying a record (e.g. ["native_id"])
        order_by: list of Column expressions, highest priority first. Put explicit
                  null handling on each (`desc_nulls_last()`), because a NULL in the leading
                  sort column is exactly the tie this function exists to break.
        hash_columns: columns to feed the content-hash tiebreak; defaults to all of them.

    The content hash is appended as the last ordering term, so two rows can only tie if they
    are byte-identical -- in which case it does not matter which survives.
    """
    order = list(order_by) + [content_hash(df, hash_columns).desc()]
    w = Window.partitionBy(*keys).orderBy(*order)
    return (
        df.withColumn("_seq_rn", F.row_number().over(w))
          .filter(F.col("_seq_rn") == 1)
          .drop("_seq_rn")
    )
