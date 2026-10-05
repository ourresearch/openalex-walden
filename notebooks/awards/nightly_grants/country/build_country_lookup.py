"""Drafts award_country_lookup.csv (run by hand; the CSV is the reviewed source of truth, this script only regenerates it).

    uv run --with pycountry python build_country_lookup.py observed_values.tsv > award_country_lookup.csv

observed_values.tsv: provenance<TAB>role<TAB>country<TAB>awards<TAB>occurrences, one row per distinct
`affiliation.country` value in openalex.awards.openalex_awards.

Columns
  value             lower(trim(country)); '*' = the row that describes the source itself (one per provenance)
  provenance_scope  '*' (any source) or one provenance
  iso2              ISO 3166-1 alpha-2; empty on source rows and on values that are not one country
  confidence        high / medium = usable, low = not usable
  meaning           what the country field holds:
                      organisation      the country of the named organisation, as recorded by the source
                      assumed_domestic  one constant for every award, written by the ingest notebook; usable because the
                                        funder's recipients are domestic (contradicted matches were wrong matches)
                      assumed           one constant, and organisations abroad are named next to it: not usable
                      project_country   where the project takes place (IDRC's recipient country, NSF's place of
                                        performance), not where the organisation is: not usable
                      person_country    the investigator's own country (nationality, or where the person is listed): not usable
                      mixed             neither reliably: not usable
                      us_state          a US state or territory code (resolves to US)
                      not_a_country     placeholder text, a region, two countries
  note              evidence
How a (provenance, value) resolves: a row for that provenance and value decides on its own. Otherwise the provenance's
source row must be usable, and the code comes from the '*'-scoped row for the value. A provenance with no source row
gives no code: a new source is not trusted until someone has looked at what its country field means.
"""
import csv
import re
import sys
import unicodedata

import pycountry

ISO2 = {c.alpha_2 for c in pycountry.countries}

# value (any case) -> (iso2 or '', confidence, note). Hand-reviewed against the observed values of 2026-10-02.
ALIASES = {
    "uk": ("GB", "high", "common code for the United Kingdom (not ISO)"),
    "el": ("GR", "high", "European Commission code for Greece"),
    "xk": ("XK", "high", "Kosovo: user-assigned code, the one OpenAlex institutions use"),
    "kosovo": ("XK", "high", "Kosovo: user-assigned code"),
    "usa": ("US", "high", ""), "u.s.a.": ("US", "high", ""), "u.s.": ("US", "high", ""),
    "united states of america": ("US", "high", ""), "eua": ("US", "high", "Portuguese abbreviation"),
    "états-unis d'amérique": ("US", "high", "French"), "etats-unis": ("US", "high", "French"),
    "great britain": ("GB", "high", ""), "great britain and northern ireland": ("GB", "high", ""),
    "england": ("GB", "high", "constituent country"), "scotland": ("GB", "high", "constituent country"),
    "wales": ("GB", "high", "constituent country"), "northern ireland": ("GB", "high", "constituent country"),
    "royaume-uni de grande-bretagne et d'irlande du nord": ("GB", "high", "French"), "royaume-uni": ("GB", "high", "French"),
    "storbritannia": ("GB", "high", "Norwegian"),
    "deutschland": ("DE", "high", "German"), "allemagne": ("DE", "high", "French"), "tyskland": ("DE", "high", "Norwegian"),
    "espagne": ("ES", "high", "French"), "spania": ("ES", "high", "Norwegian"), "italie": ("IT", "high", "French"),
    "belgique": ("BE", "high", "French"), "pays-bas": ("NL", "high", "French"), "the netherlands": ("NL", "high", ""),
    "suisse": ("CH", "high", "French"), "swizerland": ("CH", "high", "typo"), "suède": ("SE", "high", "French"),
    "sverige": ("SE", "high", "Swedish"), "autriche": ("AT", "high", "French"), "japon": ("JP", "high", "French"),
    "japão": ("JP", "high", "Portuguese"), "norvège": ("NO", "high", "French"), "norge": ("NO", "high", "Norwegian"),
    "pologne": ("PL", "high", "French"), "brésil": ("BR", "high", "French"), "bazil": ("BR", "medium", "typo for Brazil"),
    "finlande": ("FI", "high", "French"), "roumanie": ("RO", "high", "French"), "grèce": ("GR", "high", "French"),
    "irlande": ("IE", "high", "French"), "chine": ("CN", "high", "French"), "turquie": ("TR", "high", "French"),
    "turkey": ("TR", "high", "former English name"), "danemark": ("DK", "high", "French"), "danmark": ("DK", "high", "Danish"),
    "maroc": ("MA", "high", "French"), "nouvelle-calédonie": ("NC", "high", "French"), "hongrie": ("HU", "high", "French"),
    "hungria": ("HU", "high", "Portuguese"), "mexique": ("MX", "high", "French"), "singapour": ("SG", "high", "French"),
    "tunisie": ("TN", "high", "French"), "polynésie française": ("PF", "high", "French"), "inde": ("IN", "high", "French"),
    "liban": ("LB", "high", "French"), "australie": ("AU", "high", "French"), "afrique du sud": ("ZA", "high", "French"),
    "sør-afrika": ("ZA", "high", "Norwegian"), "égypte": ("EG", "high", "French"), "tchéquie": ("CZ", "high", "French"),
    "república checa": ("CZ", "high", "Portuguese"), "czech republic": ("CZ", "high", ""),
    "guyane française": ("GF", "high", "French"), "russie": ("RU", "high", "French"), "russia": ("RU", "high", ""),
    "thaïlande": ("TH", "high", "French"), "algérie": ("DZ", "high", "French"), "slovénie": ("SI", "high", "French"),
    "eslovénia": ("SI", "high", "Portuguese"), "chili": ("CL", "high", "French"), "argentine": ("AR", "high", "French"),
    "lituanie": ("LT", "high", "French"), "lituânia": ("LT", "high", "Portuguese"), "croatie": ("HR", "high", "French"),
    "nouvelle-zélande": ("NZ", "high", "French"), "nova zelândia": ("NZ", "high", "Portuguese"),
    "corée (république de)": ("KR", "high", "French"), "arabie saoudite": ("SA", "high", "French"),
    "malaisie": ("MY", "high", "French"), "cambodge": ("KH", "high", "French"), "cameroun": ("CM", "high", "French"),
    "islande": ("IS", "high", "French"), "island": ("IS", "medium", "Norwegian for Iceland (holberg_wp_rest)"),
    "colombie": ("CO", "high", "French"), "malte": ("MT", "high", "French"), "slovaquie": ("SK", "high", "French"),
    "lettonie": ("LV", "high", "French"), "albanie": ("AL", "high", "French"), "frankrike": ("FR", "high", "Norwegian"),
    "bulgarie": ("BG", "high", "French"), "serbie": ("RS", "high", "French"), "serbien": ("RS", "high", "German"),
    "cook (îles)": ("CK", "high", "French"), "ouganda": ("UG", "high", "French"), "koweït": ("KW", "high", "French"),
    "équateur": ("EC", "high", "French"), "kirghizistan": ("KG", "high", "French"), "indonésie": ("ID", "high", "French"),
    "bolivie": ("BO", "high", "French"), "éthiopie": ("ET", "high", "French"), "guinée": ("GN", "high", "French"),
    "chypre": ("CY", "high", "French"), "jordanie": ("JO", "high", "French"),
    "korea, south": ("KR", "high", ""), "korea (south)": ("KR", "high", ""), "south korea": ("KR", "high", ""),
    "korea rep of": ("KR", "high", "NIH spelling"), "korean republic (south korea)": ("KR", "high", ""),
    "korea (republic of)": ("KR", "high", ""), "republic of korea": ("KR", "high", ""),
    "korea": ("KR", "medium", "assumed South Korea"),
    "tanzania u rep": ("TZ", "high", "NIH spelling"), "tanzania": ("TZ", "high", ""),
    "fed micronesia": ("FM", "high", "NIH spelling"), "micronesia": ("FM", "high", ""),
    "trinidad/toba": ("TT", "high", "NIH spelling"), "trinidad & tobago": ("TT", "high", ""),
    "congo dem rep": ("CD", "high", "NIH spelling"), "democratic republic of the congo": ("CD", "high", ""),
    "congo (kinshasa)": ("CD", "high", ""), "congo (dem. republic)": ("CD", "high", ""),
    "congo (république démocratique du)": ("CD", "high", "French"), "congo, democratic republic of": ("CD", "high", ""),
    "congo democratic republic (formerly zaire)": ("CD", "high", ""),
    "congo (brazzaville)": ("CG", "high", ""), "congo, republic": ("CG", "high", ""),
    "congo": ("CG", "medium", "ISO short name of the Republic of the Congo; some sources mean the DRC"),
    "dominican rep": ("DO", "high", "NIH spelling"), "st kitts/nevis": ("KN", "high", "NIH spelling"),
    "st lucia": ("LC", "high", "NIH spelling"), "papua n guinea": ("PG", "high", "NIH spelling"),
    "papuanew guinea": ("PG", "high", "typo"), "burma": ("MM", "high", "former name"),
    "hongkong": ("HK", "high", ""), "macau": ("MO", "high", ""), "vatican state": ("VA", "high", ""),
    "bosnia-hercegovina": ("BA", "high", ""), "bosnia-herzegovina": ("BA", "high", ""),
    "ivory coast": ("CI", "high", ""), "côte divoire": ("CI", "high", "typo"),
    "cote d'ivoire (ivory coast)": ("CI", "high", ""), "cote d'ivoire": ("CI", "high", ""),
    "kazakstan": ("KZ", "high", "old spelling"), "tajikstan": ("TJ", "high", "typo"),
    "macedonia": ("MK", "high", ""), "macedonia,former yugoslav rep.": ("MK", "high", ""),
    "palestinian territories": ("PS", "high", ""), "cape verde": ("CV", "high", ""), "swaziland": ("SZ", "high", "former name"),
    "falkland islands (islas malvinas)": ("FK", "high", ""), "iran isl. rep.": ("IR", "high", ""),
    "taiwan, china": ("TW", "high", ""), "taiwan": ("TW", "high", ""), "vietnam": ("VN", "high", ""),
    "laos": ("LA", "high", ""), "syria": ("SY", "high", ""), "moldova": ("MD", "high", ""), "bolivia": ("BO", "high", ""),
    "venezuela": ("VE", "high", ""), "iran": ("IR", "high", ""), "brunei": ("BN", "high", ""),
    # resolvable to a country only by assumption, or not at all
    "serbia and montenegro": ("", "low", "dissolved state (2006): Serbia or Montenegro"),
    "yugoslavia": ("", "low", "dissolved state"), "netherlands antilles": ("", "low", "dissolved (2010): CW, SX or BQ"),
    "usa/finland": ("", "low", "two countries"), "mexico and egypt": ("", "low", "two countries"),
    "saudi arabia (nationality: tunisia)": ("SA", "medium", "country of work; nationality in brackets"),
    "howland island": ("UM", "medium", "US Minor Outlying Islands"),
    "ontario": ("CA", "medium", "Canadian province in a country field"),
    "california": ("US", "medium", "US state in a country field"), "north carolina": ("US", "medium", "US state in a country field"),
    "massachusetts": ("US", "medium", "US state in a country field"),
    "award does not have an oda downstream partner": ("", "low", "NIHR placeholder, not a country"),
    "ri required": ("", "low", "NSF placeholder, not a country"), "unknown": ("", "low", "placeholder"),
    "worldwide": ("", "low", "not a country"), "southeast asia": ("", "low", "region, not a country"),
}

# RWJF stores the grantee's US state or territory code in the country field ('MA' is Massachusetts, not Morocco).
US_STATE_SOURCES = {"rwjf_grants_explorer": "RWJF: US state or territory code in the country field (CreateRWJFAwards cell 9); foreign grantees carry a country name"}
# Sources whose country field is not usable: provenance -> (meaning, evidence of 2026-10-02).
ABROAD = "one constant on every award; organisations abroad are named next to it"
UNUSABLE_SOURCES = {
    "idrc_iati": ("project_country", "IDRC: `a.recipient_country as country` (CreateIDRCAwards cell 9); University of Alberta carries ZA, TH, CU, MW"),
    "nsf_award_search": ("project_country", "NSF: the award's place of performance (scripts/local/nsf_awards_to_s3.py fills inst_* from perf_inst; the awardee block is not kept). US universities carry the country of their field site: University of Washington with Antarctica, Emory with Tanzania; 45 of 50 sampled removals of a US institution under a foreign place were correct matches"),
    "nihr": ("project_country", "NIHR: the ODA partner country; the only value today is the sentinel 'Award does not have an ODA Downstream Partner' (CreateNIHRAwards cell 7)"),
    "kavli_nextdata": ("person_country", "Kavli Prize: the laureate's country (`element_at(s.countries, 1)`, CreateKavliPrizeAwards cell 7): Caltech with NL, Stanford with RU, MIT with CA"),
    "cifar_wp_rest": ("person_country", "CIFAR: the fellow's country term, next to an institution elsewhere: DeepMind, Stanford, Yale with CA; Johns Hopkins with JP"),
    "bbrf_narsad": ("mixed", "BBRF: the directory's country does not follow the institution: Yale and Caltech with TR, KAIST with US, Harvard Medical School with NL"),
    "villum_veluxfonden": ("mixed", "Villum: non-Danish values sit next to Danish institutions (Aarhus University with US, DTU with SE): all 5 contradicted matches were correct"),
    "snsf": ("mixed", "SNSF: non-Swiss values sit next to Swiss institutions (University of Berne with DE, RU, GB)"),
    "fct": ("mixed", "FCT: Instituto Superior Tecnico carries Italy on 1,527 awards; Portugal sits next to Vigo, Utrecht, Amsterdam"),
    "humboldt": ("assumed", "`'Germany' as country` (CreateHumboldtAwards cell 6); the name is the fellow's institution abroad, 74% of legacy matches contradicted"),
    "cihr_opendata": ("assumed", "`'Canada' as country` (CreateCIHRAwards cell 7); " + ABROAD + " (Harvard, Stanford, Witwatersrand)"),
    "nwopen": ("assumed", "`'Netherlands' as country` (CreateNWOAwards cell 6); " + ABROAD + " (Harvard, KU Leuven, Oxford, ETH)"),
    "nihr_ods_dhsc": ("assumed", ABROAD + " (Malawi-Liverpool-Wellcome Programme)"),
    "frqsc": ("assumed", ABROAD + " (KU Leuven, NYU, LSE)"),
    "frqnt": ("assumed", ABROAD + " (KU Leuven)"),
    "frqs": ("assumed", ABROAD + " (NYU)"),
    "hewlett_facetwp": ("assumed", ABROAD + " (TrustAfrica, Senegal)"),
    "helmsley_grants": ("assumed", ABROAD + " (Medizinische Hochschule Hannover)"),
    "telethon": ("assumed", ABROAD + " (University College London)"),
    "mott_grants": ("assumed", ABROAD + " (Southern Africa Trust, CIVICUS, University of the Western Cape)"),
    "doe_sc": ("assumed", ABROAD + " (Monash, Reading, Juelich)"),
    "czi_grants": ("assumed", ABROAD + " (ANU, TU Delft, Leiden)"),
    "pew_biomed": ("assumed", ABROAD + " (Cambridge, ETH Zurich, UBC)"),
    "lcrf": ("assumed", ABROAD + " (Manchester, Ottawa Hospital Research Institute)"),
    "royal_society_grants": ("assumed", ABROAD + " (University of Cape Town, Tsinghua, Stellenbosch)"),
    "rj_jubileumsfond_grants": ("assumed", ABROAD + " (Oslo, Copenhagen, Helsinki)"),
    "hrb_ireland": ("assumed", ABROAD + " (Queen's University Belfast, Ulster)"),
    "lister": ("assumed", ABROAD + " (Aarhus, ANU, Caltech)"),
    "tsc_alliance": ("assumed", ABROAD + " (Cardiff, UCL, King's College London)"),
    "fritz_thyssen_fundings": ("assumed", ABROAD + " (Weizmann, Hebrew University, Central European University)"),
    "brain_tumour_charity": ("assumed", ABROAD + " (Harvard, Mayo Clinic, UCSF)"),
    "inca": ("assumed", ABROAD + " (CIRMF Gabon, Hopital Charles Nicolle Tunis, Cancer Research UK)"),
}
# value kept usable inside an unusable source: (provenance, value) -> (iso2, confidence, meaning, evidence)
EXCEPTIONS = {
    ("snsf", "switzerland"): ("CH", "high", "organisation", "SNSF: Switzerland next to Swiss institutions is reliable (contradicted matches were wrong matches)"),
    ("villum_veluxfonden", "denmark"): ("DK", "high", "organisation", "Villum: Denmark next to Danish institutions is reliable (971 of 978 awards)"),
    ("nsf_award_search", "united states"): ("US", "medium", "project_country", "NSF: a place of performance in the US. Assumption: an organisation performing in the US is a US organisation. 50 of 50 sampled matches to an institution abroad under a US place were wrong matches (Princeton to The Princes Trust, Lincoln University to New Zealand's)"),
}
US_SUBDIVISIONS = {s.code.split("-")[1] for s in pycountry.subdivisions.get(country_code="US")}   # 50 states, DC, territories


def fold(s):
    s = unicodedata.normalize("NFKD", s)
    return re.sub(r"\s+", " ", "".join(ch for ch in s if not unicodedata.combining(ch)).strip().casefold())


def main(observed_path):
    rows = {}                                              # (value, scope) -> (iso2, confidence, meaning, note)

    def put(value, scope, iso2, confidence, note, meaning=None):
        meaning = meaning or ("organisation" if iso2 else "not_a_country")
        rows.setdefault((value.strip().lower(), scope), (iso2, confidence, meaning, note))

    for value, (iso2, confidence, note) in ALIASES.items():
        put(value, "*", iso2, confidence, note)
    names = {}
    for c in pycountry.countries:
        put(c.alpha_2, "*", c.alpha_2, "high", "ISO 3166-1 alpha-2")
        put(c.alpha_3, "*", c.alpha_2, "high", "ISO 3166-1 alpha-3")
        for attr in ("name", "common_name", "official_name"):
            if hasattr(c, attr):
                put(getattr(c, attr), "*", c.alpha_2, "high", "ISO 3166-1 name")
                names[fold(getattr(c, attr))] = c.alpha_2
    for scope, why in US_STATE_SOURCES.items():
        put("*", scope, "", "high", why, "us_state")
        for code in sorted(US_SUBDIVISIONS):
            put(code, scope, "US", "high", why, "us_state")
    for scope, (meaning, why) in UNUSABLE_SOURCES.items():
        put("*", scope, "", "low", why, meaning)
    for (scope, value), (iso2, confidence, meaning, note) in EXCEPTIONS.items():
        put(value, scope, iso2, confidence, note, meaning)
    unresolved, codes, awards = [], {}, {}                 # per provenance: resolved codes seen, awards carrying a value
    with open(observed_path) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            value, prov = r["country"].strip().lower(), r["provenance"]
            if not value:
                continue
            if (value, "*") not in rows and fold(value) in names:      # accent / spacing variant of an ISO name
                put(value, "*", names[fold(value)], "high", "ISO 3166-1 name (variant spelling)")
            if (value, "*") not in rows and (value, prov) not in rows:
                unresolved.append((prov, value, int(r["awards"])))
            iso2 = rows.get((value, prov), rows.get((value, "*"), ("",)))[0]
            if iso2:
                codes.setdefault(prov, set()).add(iso2)
            if r["role"] == "lead":
                awards[prov] = awards.get(prov, 0) + int(r["awards"])
    for prov in sorted(codes):                             # every other source that carries a country gets its source row
        if len(codes[prov]) > 1:
            put("*", prov, "", "high", f"{len(codes[prov])} countries recorded on {awards.get(prov, 0)} awards", "organisation")
        else:
            put("*", prov, "", "medium", f"one constant ({min(codes[prov])}) on {awards.get(prov, 0)} awards; no organisation abroad seen next to it", "assumed_domestic")
    out = csv.writer(sys.stdout, lineterminator="\n")
    out.writerow(["value", "provenance_scope", "iso2", "confidence", "meaning", "note"])
    for (value, scope), (iso2, confidence, meaning, note) in sorted(rows.items(), key=lambda kv: (kv[0][1] != "*", kv[0][1], kv[0][0] != "*", kv[0][0])):
        out.writerow([value, scope, iso2, confidence, meaning, note])
    print(f"{len(rows)} rows; {len(unresolved)} observed (provenance, value) pairs unresolved, "
          f"{sum(n for _, _, n in unresolved)} awards", file=sys.stderr)
    for prov, value, n in sorted(unresolved, key=lambda x: -x[2]):
        if prov != "hfsp_awards_listing":
            print(f"  unresolved\t{prov}\t{value}\t{n}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1])
