"""The contributor roles vocabulary: CRediT's 14 roles, plus `editor` and `translator`, as OpenAlex entities (oxjob #2075).

Each role is an entity at https://openalex.org/contributor-roles/<slug>. For CRediT's roles the slug is the last part of
CRediT's own role URL (https://credit.niso.org/contributor-roles/<slug>/); display_name is CRediT's term and description
CRediT's definition, quoted from https://credit.niso.org/contributor-roles-defined/ (CRediT, ANSI/NISO Z39.104-2022,
licensed CC BY 4.0). Works carry them per author as authorships[].contributor_roles = [{id, display_name}], sorted by
slug (CRediT's own order is alphabetical).

`editor` (D11 round one, Jason 22 Nov 2026) is the person who edited the work, as Crossref, DataCite, JATS and library
catalogues record editors apart from authors. It is not CRediT's writing-review-editing (commenting on or revising a
draft). It comes from Crossref's editor list on books whose record lists no authors (CreateWorksBase's
crossref_book_contributors). An empty contributor_roles means unspecified, never "author".

`translator` (oxjob #2512, Jason 22 Nov 2026) is the person who translated the work, as Crossref, DataCite, JATS and
library catalogues record translators apart from authors. It comes from Crossref's translator list on any work
(CreateWorksBase's crossref_translators): a translator the byline doesn't show gets a seat after the authors.

STANDARD_TERMS gives each role's equivalents in other vocabularies ([{vocabulary, term, url}], the shape /relationships
and /author-types use); `ids.credit` stays the identity of a CRediT role.

The CRediT slugs themselves live in utils.pmc_jats.CREDIT_ROLES, shared by both readers (PMC JATS, Crossref deposits).
CreateWorksEnriched (SQL warehouse, no Python) spells the same slug -> display_name map out in SQL: keep them in step.
"""
from utils.pmc_jats import CREDIT_ROLES

ID_PREFIX = "https://openalex.org/contributor-roles/"

# Every role the entity serves: CRediT's 14, then the roles other standards define (oxjob #2075 D11, #2512).
ROLES = list(CREDIT_ROLES) + ["editor", "translator"]

DISPLAY_NAME = {
    "conceptualization": "Conceptualization",
    "data-curation": "Data curation",
    "formal-analysis": "Formal analysis",
    "funding-acquisition": "Funding acquisition",
    "investigation": "Investigation",
    "methodology": "Methodology",
    "project-administration": "Project administration",
    "resources": "Resources",
    "software": "Software",
    "supervision": "Supervision",
    "validation": "Validation",
    "visualization": "Visualization",
    "writing-original-draft": "Writing – original draft",
    "writing-review-editing": "Writing – review & editing",
    "editor": "Editor",
    "translator": "Translator",
}

DESCRIPTION = {
    "conceptualization": "Ideas, formulation or evolution of overarching research goals and aims.",
    "data-curation": "Management activities to annotate (produce metadata), scrub data and maintain research data "
                     "(including software code, where it is necessary for interpreting the data itself) for initial "
                     "use and later re-use.",
    "formal-analysis": "Application of statistical, mathematical, computational, or other formal techniques to analyze "
                       "or synthesize study data.",
    "funding-acquisition": "Acquisition of the financial support for the project leading to this publication.",
    "investigation": "Conducting a research and investigation process, specifically performing the experiments, or "
                     "data/evidence collection.",
    "methodology": "Development or design of methodology; creation of models.",
    "project-administration": "Management and coordination responsibility for the research activity planning and "
                              "execution.",
    "resources": "Provision of study materials, reagents, materials, patients, laboratory samples, animals, "
                 "instrumentation, computing resources, or other analysis tools.",
    "software": "Programming, software development; designing computer programs; implementation of the computer code "
                "and supporting algorithms; testing of existing code components.",
    "supervision": "Oversight and leadership responsibility for the research activity planning and execution, "
                   "including mentorship external to the core team.",
    "validation": "Verification, whether as a part of the activity or separate, of the overall "
                  "replication/reproducibility of results/experiments and other research outputs.",
    "visualization": "Preparation, creation and/or presentation of the published work, specifically "
                     "visualization/data presentation.",
    "writing-original-draft": "Preparation, creation and/or presentation of the published work, specifically writing "
                              "the initial draft (including substantive translation).",
    "writing-review-editing": "Preparation, creation and/or presentation of the published work by those from the "
                              "original research group, specifically critical review, commentary or revision – "
                              "including pre- or post-publication stages.",
    "editor": "Edited the work: chose, arranged or revised what it contains, as the editor of an edited book or a "
              "proceedings volume. Publishers and catalogues list editors apart from authors. Not CRediT's "
              "Writing – review & editing, which is reviewing or revising a draft.",
    "translator": "Translated the work into the language it is published in. Publishers and catalogues list "
                  "translators apart from authors. Not CRediT's Writing – original draft, which counts substantive "
                  "translation as part of writing the first draft.",
}

CROSSREF_CONTRIBUTORS = "https://www.crossref.org/documentation/schema-library/markup-guide-metadata-segments/contributors/"

STANDARD_TERMS = {
    # Crossref deposits CRediT as <role vocab="credit" type="<slug>"> (REST API: role {vocabulary: credit, role: <slug>});
    # JATS4R's recommendation names each role by its https URI; ORCID's API 3.0 accepts only the http form.
    **{slug: [
        ("Crossref", slug, CROSSREF_CONTRIBUTORS),
        ("JATS4R", f"https://credit.niso.org/contributor-roles/{slug}/", "https://jats4r.niso.org/credit-taxonomy/"),
        ("ORCID", f"http://credit.niso.org/contributor-roles/{slug}/",
         "https://github.com/ORCID/orcid-model/blob/master/src/main/resources/common_3.0/common-3.0.xsd"),
    ] for slug in CREDIT_ROLES},
    "editor": [
        ("Crossref", "editor", CROSSREF_CONTRIBUTORS),
        ("DataCite", "Editor",
         "https://datacite-metadata-schema.readthedocs.io/en/4.7/appendices/appendix-1/contributorType/#editor"),
        ("JATS", "editor", "https://jats.nlm.nih.gov/publishing/tag-library/1.4/attribute/contrib-type.html"),
        ("MARC relators", "edt", "http://id.loc.gov/vocabulary/relators/edt"),
        ("MARC relators", "edc", "http://id.loc.gov/vocabulary/relators/edc"),
        ("CSL", "editor", "https://docs.citationstyles.org/en/stable/specification.html#name-variables"),
        ("schema.org", "editor", "https://schema.org/editor"),
        ("Wikidata", "P98", "https://www.wikidata.org/wiki/Property:P98"),
    ],
    "translator": [
        ("Crossref", "translator", CROSSREF_CONTRIBUTORS),
        ("DataCite", "Translator",
         "https://datacite-metadata-schema.readthedocs.io/en/4.7/appendices/appendix-1/contributorType/#translator"),
        ("JATS", "translator", "https://jats.nlm.nih.gov/publishing/tag-library/1.4/attribute/contrib-type.html"),
        ("MARC relators", "trl", "http://id.loc.gov/vocabulary/relators/trl"),
        ("CSL", "translator", "https://docs.citationstyles.org/en/stable/specification.html#name-variables"),
        ("schema.org", "translator", "https://schema.org/translator"),
        ("Wikidata", "P655", "https://www.wikidata.org/wiki/Property:P655"),
    ],
}

assert set(DISPLAY_NAME) == set(DESCRIPTION) == set(STANDARD_TERMS) == set(ROLES), "contributor roles vocabulary out of step"


def credit_url(slug):
    """CRediT's URL for one of its roles; None for a role CRediT doesn't define (editor, translator)."""
    return f"https://credit.niso.org/contributor-roles/{slug}/" if slug in CREDIT_ROLES else None
