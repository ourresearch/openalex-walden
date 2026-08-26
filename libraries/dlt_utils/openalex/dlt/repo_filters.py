"""Ingest policy filters for the repo streams.

Records that must never become works. Split from repo_types.py (oxjob #881) because the
vocabulary is a large, stable data asset while these rules are small and expected to change --
#881 phase 4 and the non-scholarly detector both add gates here.

Previously these lived inside Repo.py's repo_parsed() only, so repo_parsed_backfill() and
repo_parsed_irdb() -- raw CDF passthroughs -- bypassed them entirely: 20,874,419 records reached
repo_works past rules we had already agreed to, 20,872,994 of them (99.99%) from backfill.
"""

import pyspark.sql.functions as F

from .repo_types import TYPES_TO_DELETE

# Repo.py used the literal 5 inline; RepoBackfill.py had no title rule at all. Named so the two
# call sites cannot drift again.
MIN_TITLE_LENGTH = 5


def apply_repo_policy_filters(df, title_col="title", type_col="raw_native_type",
                              native_id_col="native_id", keep_when=None):
    """Drop records that must never become works, for EVERY repo stream.

    Three rules, previously implemented only inside Repo.py's repo_parsed() and therefore
    bypassed entirely by repo_parsed_backfill() and repo_parsed_irdb(), which are raw CDF
    passthroughs (oxjob #881):

      1. raw_native_type on TYPES_TO_DELETE  -- object formats, not works
      2. null raw_native_type from archive.org -- untyped archive.org items
      3. title shorter than MIN_TITLE_LENGTH -- nothing to key a work on

    Idempotent, so it is safe to call early (per stream, to keep intermediate tables lean) and
    again on the union (to catch every stream). Call it on the union at minimum -- that is the
    only place all three streams are covered.

    keep_when: a Column that forces a row to be RETAINED regardless of the rules. This exists for
    delete events (oxjob #881): a CDF delete carries the pre-image of the row being removed, so a
    junk record's delete event fails these very rules and would be dropped -- meaning the deletion
    silently never propagates. Deletes must bypass every filter between the source and
    apply_changes, or they do nothing.
    """
    passes_policy = (
        (
            (~F.col(type_col).isNull() & ~F.lower(F.col(type_col)).isin(TYPES_TO_DELETE))
            |
            (F.col(type_col).isNull() & ~F.col(native_id_col).startswith("oai:archive.org"))
        )
        & F.col(title_col).isNotNull()
        & (F.length(F.trim(F.col(title_col))) >= MIN_TITLE_LENGTH)
    )
    if keep_when is not None:
        passes_policy = keep_when | passes_policy
    return df.filter(passes_policy)
