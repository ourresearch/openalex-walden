"""Author ids on award investigators (oxjob #1847: grants on author profiles).

Optional input `investigator_authors` (config extra_inputs): openalex.awards.award_investigator_authors, written by the Award
Investigator Authors job (one row per served grant slot; which layers it serves is the job's decision). It needs
`investigator_profiles` (openalex.authors.authors_api) for the author's display name. Not configured = every `author` is
NULL; to_json drops NULL fields, so no content hash, updated_date or search document moves.

api_payload.sql attaches an author by slot, position AND the names and ORCID as printed (names lower-cased and trimmed, the ORCID
cleaned as the job cleans it): a reordered list or an edited name or ORCID leaves the slot without one, never gives it someone else's.
A profile that is gone attaches nothing.
"""

EMPTY_LINKS = ("SELECT CAST(NULL AS BIGINT) award_id,CAST(NULL AS STRING) slot,CAST(NULL AS INT) position,CAST(NULL AS STRING) given_name,"
               "CAST(NULL AS STRING) family_name,CAST(NULL AS STRING) author_id,CAST(NULL AS STRING) display_name,"
               "CAST(NULL AS STRING) grant_orcid WHERE false")


def links_relation(c):
    """{INVESTIGATOR_LINKS}: the served slot -> author rows, with the author's id URL and display name."""
    if not c.has_extra("investigator_authors"):
        return EMPTY_LINKS
    c.require(c.has_extra("investigator_profiles"), "INVESTIGATOR_PROFILES_MISSING")
    c.zero("INVESTIGATOR_AUTHORS_SLOT", "SELECT slot FROM investigator_authors_v WHERE slot IS NULL OR slot NOT IN "
           "('lead_investigator','co_lead_investigator','investigators')")
    return ("SELECT l.award_id,l.slot,l.position,l.given_name,l.family_name,p.id author_id,p.display_name,l.orcid grant_orcid "
            "FROM investigator_authors_v l JOIN investigator_profiles_v p ON p.id=concat('https://openalex.org/A',CAST(l.author_id AS STRING))")
