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

# oxjob #881 round 2: whole endpoints whose records must never become works. Decided per
# endpoint against the contribution docket (works held ONLY by the endpoint, and how many are
# cited) -- the decision record with per-endpoint rationale is ENDPOINT_VERDICTS.md in the oxjob;
# sizing evidence is q81/q82. Tier A (>=90% of records carry no DOI, no author, no abstract)
# plus pt.cision.com (2b57dfbd...), EDP's Cision media-monitoring feed: press clippings behind a
# login-walled portal, blanket-typed InteractiveResource, "authors" are monitoring-account
# usernames.
ENDPOINTS_TO_DELETE = frozenset({
    "01a614fc4fa7a41b87d", "037ede0fcc87a84b5fe", "05b312337ed8746c780",
    "080dded3c8e30a1e675", "0c5211d0dbeb0177d8c", "0e90d1eb894861ae118",
    "15b936a5ee1d4645a1b", "1d16bcb60d5d5dc7c36", "215bbea87561a3ab940",
    "298981a79babf762b08", "2b57dfbd43207095dbc", "3011b5743bde16b4c5a",
    "34ee764b8e12c72f2cc", "378455584dbf2965434", "3a7d479c12b0620630f",
    "4771924336b7485fe2a", "489e064af1c2c0c5763", "51975d88bd61f005b89",
    "57a02a7d90caa0cfebe", "61e455fc63a484a2b76", "64603e8bebf27a33f12",
    "6786254ce6399a04bef", "68f8160496d4d5cc41f", "75d3ee26c800f79073d",
    "7e54e265f411a728f68", "7ec41b5c396442eee8ed", "83221bfba1246982016",
    "8b5ef34d37ce72c504a", "93e3039e515300a2b19", "94ec77bde3bf6a8b6ba",
    "977908284ce1e2da4d8", "97ba749ea9efda757bc", "9825926b46bdf50993b",
    "a2d9757da3a086efb6d", "a48b4f972ebd0480221", "a5c3f1aa83b208d36cb",
    "ac4e02c839086707b56", "ad2a6600530d1fcbc41", "ad81c5c39e8cab08e03",
    "b41b2a9570ec55cc5fe", "b488924f4e136b92266", "bb71b9a37dd16a26f17",
    "bcd317b203074ea182f", "bea6ddeca26aa91ea09", "c1bf8cf438d5520c462",
    "c83a9fca22860782238", "cc64232c0526080d0a3", "cgeek9gu5ssdw98ymjbx",
    "d35e9c456857e1eb0c1", "d5b80c5dd75cc146463", "d61f497953ea5ae0b5e",
    "db7d86c1ad782e15c55", "dd7ed831818e42d5a20", "df67293ae2e390e5731",
    "e09c0b6f22c4fbf5c45", "e4932d97855bbecf5ef", "ec6b677fc6dc330ae0d",
    "ee663efa9663af969cc", "efcc7095eda9a819ed1", "f5245d6fc6afb853e94",
    "f636af0063b01d813c2", "f7339d14c2862b00456", "f7f46bc748b76f00e5d",
    "f9aaacddd8597430f3c", "fb5960cc394837b481e", "fbee8323c69f9efc06c",
    "fc368edc6e0086c0c48", "ff62abe687c515a86f7",
})

# oxjob #881 round 2: sub-endpoint carves -- the endpoint stays, records in these setSpec
# classes go. A record is removed when ANY of its set_spec elements starts with one of the
# prefixes; records with no set_spec are kept. Gallica (oai.bnf.fr): periodical ISSUES,
# museum objects, images, sheet music, A/V and maps are removed; monographies (digitized books,
# 84% authored), manuscrits and periodiques:titres are kept.
ENDPOINT_SETSPEC_DELETE = {
    "b6f3a90f96528af2baa": (
        "gallica:typedoc:periodiques:fascicules",
        "gallica:typedoc:objets",
        "gallica:typedoc:images",
        "gallica:typedoc:partitions",
        "gallica:typedoc:audio",
        "gallica:typedoc:videos",
        "gallica:typedoc:cartes",
    ),
}


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


def apply_endpoint_filters(df, endpoint_col="endpoint_id", set_spec_col="set_spec",
                           keep_when=None):
    """Drop records from denylisted endpoints and carved setSpec classes (oxjob #881 round 2).

    Call on the union in repo_enriched(), where every stream carries endpoint_id and set_spec.
    NOT folded into apply_repo_policy_filters because that one also runs inside repo_parsed(),
    before the repository_id -> endpoint_id rename.

    A NULL endpoint_id is kept (nothing to match). A carve removes a record when any set_spec
    element starts with one of that endpoint's prefixes; NULL/empty set_spec is kept.

    keep_when: same contract as apply_repo_policy_filters -- delete events carry the pre-image
    of the row being removed and must bypass every filter or the deletion never propagates.
    """
    denylisted = F.col(endpoint_col).isin(*sorted(ENDPOINTS_TO_DELETE))

    # SQL-side lambda (the Repo.py filter idiom) -- a pyspark-side lambda with extra bound
    # params fails DLT analysis (2026-08-27, update e0fd1ae6).
    carved = F.lit(False)
    for endpoint_id, prefixes in ENDPOINT_SETSPEC_DELETE.items():
        likes = " OR ".join(f"s LIKE '{p}%'" for p in prefixes)
        in_carved_class = F.expr(
            f"exists(coalesce({set_spec_col}, array()), s -> {likes})")
        carved = carved | ((F.col(endpoint_col) == endpoint_id) & in_carved_class)

    passes_policy = ~(F.coalesce(denylisted, F.lit(False)) | carved)
    if keep_when is not None:
        passes_policy = keep_when | passes_policy
    return df.filter(passes_policy)
