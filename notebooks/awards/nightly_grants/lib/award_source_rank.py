"""The one rank table for grant sources (Kyle + Rohan, 19 Nov 2026). Rank 1 is the highest. A higher rank overwrites a lower one.

  1  funder's own data   a loader that reads the funder's own grant list, for that funder's grants only
  2  grant DOI           a grant registered with a DOI, Crossref and DataCite equal
  3  aggregator          somebody else's collection of many funders' grants (USAspending, IATI, ...)
  4  mention-only grant  a grant we know only from a paper's funding note (was "shell")

Before this file the rule was spread over magic numbers in `openalex_awards_raw.priority` (0 to 703 on 19 Nov 2026):
0 = mention-only, 1 = grant DOI, 3 and up = a loader, and among loaders the HIGHER number won (flipped from lower-wins on
2026-06-20, #500), so an aggregator at 703 beat a funder's own feed at 3. The number said nothing about whose data it was.

The old number stays in the `priority` column and is used only as the order inside one rank (higher first), so no election
inside a rank changes with this file. Do not read meaning into it; do not add new numbers. New sources get a rank here.
This file is the only place in the grant builder that reads the old number: every other file asks it (rank_sql, is_grant_doi_sql,
is_loader_sql, is_direct_sql, namespace_sql). test_award_source_rank.py fails if a `priority>=3` or `priority=1` comes back elsewhere.

Legacy Gateway to Research publication records (`gtr_awards`, not in raw) are given rank 3 with old number -1 in
build_awards.py. That keeps them exactly where they were: below every loader, grant DOI and aggregator, above mention-only.

Own-funder rule: rank 1 is "the funder's own data for that funder". With a `source_owners` relation (provenance, funder_id;
the Awards job's sources.csv) a loader row for a funder the source is not registered for is rank 3, not rank 1. The input is not
configured yet: on 19 Nov no registered source wrote a row for another funder (0 of 4,350,829 rows), and 63 loader sources
(5.4M rows) have no registered funder list, so for them the rule can't be checked.

What a higher rank wins, and what the merged grant keeps (the funder's own record against its own grant DOIs): registry_twin.py.
"""
NAMES = {1: "funder's own data", 2: "grant DOI", 3: "aggregator", 4: "mention-only grant"}
HIGHEST, LOWEST = 1, 4

GRANT_DOI = {"crossref_work": "crossref_grant", "datacite": "datacite_award"}   # grant-DOI provenance -> its NATIVE key namespace
# Somebody else's collection: (how to match the provenance, text, why). Reviewed list: add a row, with the reason, to add one.
AGGREGATORS = (
    ("prefix", "usaspending", "USAspending bulk downloads, all agencies"),
    ("prefix", "iati_", "IATI registry, generic ingester"),
    ("suffix", "_iati", "IATI registry, one publisher"),
    ("exact", "hhs_taggs", "HHS TAGGS, department-wide"),
    ("prefix", "openaire_", "OpenAIRE copy of a funder's grants"),
    ("prefix", "dimensions_", "Dimensions copy of a funder's grants"),
)
# Not decided (19 Nov), left at rank 1 until Rohan or Kyle says otherwise; the dev run measures what moving each to rank 3 changes:
UNDECIDED = (
    ("suffix", "_europepmc_grist", "a funder's grant list held by Europe PMC (BHF, Diabetes UK, India Alliance)"),
    ("suffix", "_360giving", "a 360Giving file the funder publishes itself"),
    ("exact", "agaur_raisc_socrata", "the RAISC portal"),
    ("exact", "gateway_to_research", "Gateway to Research, which also holds Innovate UK's grants (Innovate UK's own list: innovate_uk)"),
)


def _old(x):
    return f"{x}.priority" if x else "priority"


def _prov(x):
    return f"{x}.provenance" if x else "provenance"


def is_grant_doi_sql(x="x"):
    """A grant-DOI record: the Crossref grant and DataCite award slices of raw (old number 1)."""
    return f"({_old(x)}=1 AND {_prov(x)} IN ({','.join(repr(p) for p in GRANT_DOI)}))"


def is_loader_sql(x="x"):
    """A row a grant loader wrote (old numbers 3 and up): the funder's own data, or an aggregator's."""
    return f"{_old(x)}>=3"


def is_direct_sql(x="x"):
    """Ranks 1 to 3: a record of the grant itself, not a mention of it in a paper."""
    return f"({is_loader_sql(x)} OR {is_grant_doi_sql(x)})"


def namespace_sql(x="x"):
    """The NATIVE key namespace of a grant-DOI record (its DOI is the key), else NULL."""
    whens = " ".join(f"WHEN {_prov(x)}='{p}' THEN '{ns}'" for p, ns in GRANT_DOI.items())
    return f"CASE WHEN {is_grant_doi_sql(x)} THEN CASE {whens} END END"


def rank(provenance, legacy_priority, funder_id=None, owners=None):
    """Python twin of rank_sql, for tests and for reading. owners: {provenance: set(funder_id)} or None."""
    if legacy_priority is None:
        return None
    if legacy_priority == 1 and provenance in GRANT_DOI:
        return 2
    if legacy_priority < 3:
        return 4
    if any(_is(provenance or "", kind, text) for kind, text, _ in AGGREGATORS):
        return 3
    if owners and provenance in owners and funder_id not in owners[provenance]:
        return 3
    return 1


def _is(value, kind, text):
    return {"prefix": value.startswith(text), "suffix": value.endswith(text), "exact": value == text}[kind]


def _is_sql(col, kind, text):
    n = len(text)
    return {"prefix": f"substr({col},1,{n})='{text}'", "suffix": f"right({col},{n})='{text}'", "exact": f"{col}='{text}'"}[kind]


def matches_sql(col, rows):
    """SQL that is true when `col` matches one of (kind, text, why) rows, e.g. AGGREGATORS or UNDECIDED."""
    return "(" + " OR ".join(_is_sql(col, kind, text) for kind, text, _ in rows) + ")"


def rank_sql(x="x", not_own=None):
    """SQL for the rank of a raw awards row aliased `x`. not_own: a boolean SQL expression that is true when the row's
    source is registered for other funders only (see owner_join_sql); leave None when there is no owner list."""
    own = f" WHEN {not_own} THEN 3" if not_own else ""
    # a row with no number has no rank (NULL): its observation then fails INVALID_OBSERVATION (is_direct NULL), as on main
    return (f"CASE WHEN {_old(x)} IS NULL THEN NULL WHEN {is_grant_doi_sql(x)} THEN 2"
            f" WHEN NOT {is_loader_sql(x)} THEN 4"
            f" WHEN {matches_sql(_prov(x), AGGREGATORS)} THEN 3{own} ELSE 1 END")


def owner_join_sql(x="x", owners="source_owners_v"):
    """(joins, not_own) for the own-funder rule: a source with registered funders that writes a row for another funder."""
    joins = (f" LEFT JOIN (SELECT DISTINCT provenance own_source FROM {owners}) so ON so.own_source={x}.provenance"
             f" LEFT JOIN (SELECT DISTINCT provenance own_source,funder_id own_funder FROM {owners}) sf"
             f" ON sf.own_source={x}.provenance AND sf.own_funder={x}.funder_id")
    return joins, "so.own_source IS NOT NULL AND sf.own_funder IS NULL"


def order_sql(a=""):
    """ORDER BY head for every election: best rank first, then the old number inside the rank."""
    a = a + "." if a else ""
    return f"{a}source_rank,{a}priority DESC"
