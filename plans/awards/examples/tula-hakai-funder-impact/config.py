"""Central config for the Tula/Hakai funder impact case study.

All OpenAlex IDs and search strings were verified against the live API on 2026-06-06.
"""
import os
from pathlib import Path

# --- paths ---------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
FIGURES = ROOT / "figures"
TABLES = ROOT / "tables"
CACHE = ROOT / ".cache"
BLOG = ROOT / "blog"
for _d in (DATA, FIGURES, TABLES, CACHE, BLOG):
    _d.mkdir(parents=True, exist_ok=True)

# --- OpenAlex API ---------------------------------------------------------
# Authenticate with your OpenAlex API key (see
# https://docs.openalex.org/how-to-use-the-api/api-keys). Replace "XXX" with your
# key, or export OPENALEX_API_KEY in your environment.
API_KEY = os.environ.get("OPENALEX_API_KEY", "XXX")
API = "https://api.openalex.org"

# --- entity IDs ----------------------------------------------------------
FUNDER_HAKAI = "F4320334031"   # Hakai Institute (funder)
FUNDER_TULA = "F4320315065"    # Tula Foundation (funder)
INST_HAKAI = "I4387155864"     # Hakai Institute (institution)
INST_TULA = "I4210087486"      # Tula Foundation (institution)

# raw-affiliation "Tula" is NOT used: 6557 hits, dominated by Russian universities
# (Tula State University, etc.). We rely on the Tula institution ID instead.

# --- candidate search strategies ----------------------------------------
# Each entry: (provenance_flag_column, openalex_filter_expression)
STRATEGIES = [
    ("prov_funder_hakai", f"funders.id:{FUNDER_HAKAI}"),
    ("prov_funder_tula",  f"funders.id:{FUNDER_TULA}"),
    ("prov_inst_hakai",   f"authorships.institutions.id:{INST_HAKAI}"),
    ("prov_inst_tula",    f"authorships.institutions.id:{INST_TULA}"),
    ("prov_rawaff_hakai", "raw_affiliation_strings.search:Hakai"),
    ("prov_ft_hakai_inst", 'fulltext.search:"Hakai Institute"'),
    ("prov_ft_hakai_net",  'fulltext.search:"Hakai Network"'),
    ("prov_ft_tula",       'fulltext.search:"Tula Foundation"'),
    ("prov_ft_calvert",    'fulltext.search:"Calvert Island"'),  # lower-confidence
]
PROV_FLAGS = [s[0] for s in STRATEGIES]
# Strong provenance = a direct, structured Tula/Hakai link. Weak = fulltext/raw-string only.
STRONG_PROV = ["prov_funder_hakai", "prov_funder_tula", "prov_inst_hakai", "prov_inst_tula"]
WEAK_PROV = [p for p in PROV_FLAGS if p not in STRONG_PROV]

# --- analysis parameters (refined during execution) ----------------------
# Treatment year for before/after analyses. Hakai Beach Institute opened on
# Calvert Island in 2009; the SFU-run Hakai Network grants pre-date the field
# stations. Revisit in 07/08 with sensitivity around 2009-2011.
TREATMENT_YEAR = 2010

# Major BC universities (IDs verified against the live API 2026-06-06).
BC_UNIVERSITIES = {
    "UBC": "I141945490",          # University of British Columbia
    "UBC-O": "I4405260628",       # UBC Okanagan campus
    "SFU": "I18014758",           # Simon Fraser University
    "UVic": "I212119943",         # University of Victoria
    "UNBC": "I151934421",         # University of Northern British Columbia
}
# BC universities for the pairwise collaboration network (08).
BC_CORE = {
    "UBC": "I141945490",
    "SFU": "I18014758",
    "UVic": "I212119943",
    "UNBC": "I151934421",
    "VIU": "I28638086",
}

# Hakai-focus marine/coastal topics (primary-topic IDs from the corpus) for the
# field-level DiD (07).
HAKAI_TOPICS = {
    "T10643": "Marine and coastal plant biology",
    "T10230": "Marine and fisheries research",
    "T10032": "Marine and coastal ecosystems",
    "T10302": "Fish Ecology and Management Studies",
    "T12806": "Ocean Acidification Effects and Responses",
    "T10765": "Marine Biology and Ecology Research",
    "T12640": "Environmental DNA in Biodiversity Studies",
}
