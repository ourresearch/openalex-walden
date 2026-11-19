"""The contributor roles vocabulary: CRediT's 14 roles as OpenAlex entities (oxjob #2075).

Each role is an entity at https://openalex.org/contributor-roles/<slug>, the slug being the last part of CRediT's own
role URL (https://credit.niso.org/contributor-roles/<slug>/). Works carry them per author as
authorships[].contributor_roles = [{id, display_name}], in CRediT's (alphabetical) order. display_name is CRediT's term
and description CRediT's definition, quoted from https://credit.niso.org/contributor-roles-defined/
(CRediT, ANSI/NISO Z39.104-2022, licensed CC BY 4.0).

The slugs themselves live in utils.pmc_jats.CREDIT_ROLES, shared by both readers (PMC JATS, Crossref deposits).
CreateWorksEnriched (SQL warehouse, no Python) spells the same slug -> display_name map out in SQL: keep them in step.
"""
from utils.pmc_jats import CREDIT_ROLES

ID_PREFIX = "https://openalex.org/contributor-roles/"

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
}

assert set(DISPLAY_NAME) == set(DESCRIPTION) == set(CREDIT_ROLES), "contributor roles vocabulary out of step"


def credit_url(slug):
    return f"https://credit.niso.org/contributor-roles/{slug}/"
