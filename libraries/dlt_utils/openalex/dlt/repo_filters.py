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
    # oxjob #880 round 3 (2026-09-04, Casey): item-level digitization and periodical-issue
    # endpoints found by profiling the digit-key fan-out batch (KEY_LEDGER_PLAN.md s 9, q77/q78):
    # 86-97% of their records are folder/box scan pages, permit cards, per-sherd excavation
    # context records, pamphlet pages, newspaper and official-bulletin issues.
    "3e821e5524e99c846c7",  # Open Context (opencontext.org/oai/request): per-sherd context records
    "4cf3b25bdbf192a382e",  # Open Context duplicate endpoint (retired in the registry 2026-08-31)
    "424791a2d217cbac04a",  # Open Context (opencontext.org/oai/oai2.php)
    "33c353ac55f2fe86ca4",  # Hennepin County Library ContentDM: permit cards, yearbook pages, property records
    "56a335c85759891850c",  # Plano Public Library ContentDM (cdm15915): minutes/diary/program pages
    "2464b7588f6c599ab7c",  # Biblioteca Virtual de Andalucia: newspaper issues, graphic material, video
    "e11d60e7cd398991490",  # Biblioteca Virtual de Madrid: Boletin oficial de la provincia issues
    "d81af6bd4b339ff4510",  # Wroclaw University digital library (bibliotekacyfrowa.pl): newspaper issues, index pages, weather sheets
    "4c6759f5f7aa2e0f771",  # Baltycka Biblioteka Cyfrowa (bibliotekacyfrowa.eu): newspaper issues, museum objects, house files
})

# oxjob #881 round 2: sub-endpoint carves -- the endpoint stays, records in these setSpec
# classes go. A record is removed when ANY of its set_spec elements starts with one of the
# prefixes; records with no set_spec are kept. Gallica (oai.bnf.fr): periodical ISSUES,
# museum objects, images, sheet music, A/V and maps are removed; monographies (digitized books,
# 84% authored), manuscrits and periodiques:titres are kept.
ENDPOINT_SETSPEC_DELETE = {
    # Hispana (oai:hispana.mcu.es, Spanish heritage aggregator; oxjob #880/#881, 2026-09-03):
    # the heritage collections go -- coins, prints, manuscripts, newspaper issues, civil
    # registries (0-5% scholarly dc:type, no DOIs, uncited). The university-IR sets (gredos,
    # riunet, idus, uji, ...) and the no-setSpec bucket (carries the DOI copies) are kept.
    "0ef9aa4cd18142685bb": (
        "eseceres", "bdmmadrid", "gal2", "iaph", "laguna", "hdmurcia", "bvdefensa", "simurg",
        "bancoespana", "digicarmel", "jable", "bdcomunidadmadrid", "ciconia", "ccbae",
        "historico_valencia",
    ),
    "b6f3a90f96528af2baa": (
        "gallica:typedoc:periodiques:fascicules",
        "gallica:typedoc:objets",
        "gallica:typedoc:images",
        "gallica:typedoc:partitions",
        "gallica:typedoc:audio",
        "gallica:typedoc:videos",
        "gallica:typedoc:cartes",
    ),
    # oxjob #880 round 3 (2026-09-04): newspaper-issue and image sets on otherwise-kept endpoints.
    # Galiciana: the hemeroteca (newspaper issues; 'hemeroteca' also covers 'hemeroteca_1') and
    # the objdigitales set go; the 'duplicados' set (digitized books, bookParts) stays.
    "82fda151e563b16f718": ("hemeroteca", "objdigitales"),
    # University of Hawaii eVols: Austin's Hawaiian Weekly, Marianas Variety, Honolulu Weekly issues.
    "00eb30b3c421604fb0a": ("com_10524_22", "com_10524_48717", "com_10524_55438"),
    # Murray State Digital Commons: four newspaper title sets + the image collection.
    "257ceb94a3e9008c0fd": ("publication:mlt", "publication:dm", "publication:tn", "publication:tml",
                            "publication:digital_coll"),
    # Ball State: student and local newspaper issues.
    "1d1d13c412643d1f98b": ("BSUDlyNws", "PostDemNews", "GrnRchOrNgh"),
    # UNH Scholars: The New Hampshire student paper issues + aerial photograph frames.
    "7bc56f87fb0e7f4ae9b": ("publication:tnh_archive", "publication:aerial"),
    # Portal to Texas History: the CAH map-sheet partner set.
    "4c3c0d4c422b7f8c3fc": ("partner:CAH",),
}


# oxjob #880 round 3b (2026-09-05): the INVERSE of ENDPOINT_SETSPEC_DELETE -- for these endpoints a
# record is removed UNLESS its setSpec is on the keep list. Use it when an endpoint is a digitization
# platform with a thin scholarly tail: listing the junk sets would mean any set the repository adds
# later defaults to "keep", which is the wrong prior here.
#
# A record with NO setSpec is REMOVED on a keep-list endpoint (the opposite of the delete list, where
# a missing setSpec is kept). Safe for UNC: all 1,381,527 records carry exactly one setSpec.
#
# UNC Wilson Library (dc.lib.unc.edu) was a whole-endpoint denial on 2026-09-04 and that was too broad:
# 71 of the 102 cited works deleted that night were the *Journal of the North Carolina Academy of
# Science* (`jncas`, 208 citations), hosted on the same OAI endpoint as 1.38M folder/box/audiocassette
# scans. 64 of its 72 sets are 90-100% item-level with no authors; the keep list is the bibliographic
# tail (9,076 records, 0.66%): the journal, the health-history monographs, the Rare Book Room, the
# Minipage news articles, Vesalius, the Bunker manuscripts, the Attic vase corpus.
# NOT kept, deliberately: `sohp` (7,918 Southern Oral History Program interviews titled `A-0001` --
# authored but degenerate-titled), `powell`/`unctshirts`/`vir_museum`/`keepsakes` (advertisements,
# t-shirts, museum objects).
ENDPOINT_SETSPEC_KEEP = {
    "7ccc21dda876bd4e680": (
        "jncas", "nchh", "dmisc", "rbr", "vesalius", "minipage", "bunkers", "attic",
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

    ENDPOINT_SETSPEC_KEEP inverts that for endpoints that are junk by default: a record survives
    only if its set_spec matches a keep prefix, so a NULL/empty set_spec is REMOVED there.

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

    # keep-list endpoints: everything EXCEPT the listed sets goes (oxjob #880 round 3b)
    for endpoint_id, prefixes in ENDPOINT_SETSPEC_KEEP.items():
        likes = " OR ".join(f"s LIKE '{p}%'" for p in prefixes)
        in_kept_class = F.expr(
            f"exists(coalesce({set_spec_col}, array()), s -> {likes})")
        carved = carved | ((F.col(endpoint_col) == endpoint_id) & ~in_kept_class)

    passes_policy = ~(F.coalesce(denylisted, F.lit(False)) | carved)
    if keep_when is not None:
        passes_policy = keep_when | passes_policy
    return df.filter(passes_policy)
