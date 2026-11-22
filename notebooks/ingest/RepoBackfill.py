# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.41-py3-none-any.whl

# COMMAND ----------

import re
import unicodedata
import html
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

# oxjob #881: the work-type vocabulary and the policy filters used to be copy-pasted here from
# Repo.py (~1,240 lines). The maps stayed in sync; the filters built on them did not.
from openalex.dlt.repo_types import best_type_udf
from openalex.dlt.repo_filters import apply_repo_policy_filters
from openalex.dlt.sequencing import dedupe_by_sequence
from openalex.dlt.repo_ids import extract_ids_udf
from openalex.dlt.oai_ids import read_endpoint_hosts, with_rekeyed_native_id

# oxjob #933: same trusted-host rule as notebooks/ingest/Repo.py -- keep byte-identical.
# oxjob #1407: both expressions read oai_identifier (the RAW header id), not the re-keyed native_id.
OSTI_ORIGIN_DOI_PREFIXES = "2172|25582|17188|11578|5439|18429|15121|21947|25585|17190|17182|15485|18141|15473|34664|7910"
TRUSTED_HOST_IS_OA_EXPR = f"""
    size(split(oai_identifier, ':')) >= 2 AND (
      lower(split(oai_identifier, ':')[1]) RLIKE 'arxiv|pubmedcentral|europepmc|biorxiv|medrxiv|zenodo|open-science\\\\.canada'
      OR (
        lower(split(oai_identifier, ':')[1]) RLIKE 'osti'
        AND NOT exists(ids, x -> x.namespace = 'doi'
                             AND NOT lower(x.id) RLIKE '(^|doi\\\\.org/)10\\\\.({OSTI_ORIGIN_DOI_PREFIXES})/')
      )
    )
"""
# oxjob #1344: PubMed Central / Europe PMC version from the record's own OAI setSpec. PMC oai_dc
# never states a version, so the regex in detect_version_* fell through to submittedVersion for
# ~2.9M works. The set names the kind of deposit: funder author-manuscript sets (>=99.6% carry a
# PubMed NIHMS/UKMS/CAMS/HALMS manuscript id; gatesmanu 63/110), preprint sets, or a journal set
# (publisher deposit). NULL for non-PMC records and PMC records with no setSpec -> the regex
# decides, as before. Keep byte-identical with the other ingest notebook.
PMC_VERSION_FROM_SET_SPEC_EXPR = """
    CASE WHEN (lower(oai_identifier) LIKE 'oai:pubmedcentral.nih.gov:%' OR lower(oai_identifier) LIKE 'oai:europepmc.org:%')
              AND size(set_spec) > 0 THEN
      CASE WHEN arrays_overlap(set_spec, array('nihpa','wtpa','hhspa','epapa','nistpa','vapa','capmc','nasapa',
                                                'hhmipa','hal','hrams','dhspa','asms','gatesmanu','manusctipt'))
             THEN 'acceptedVersion'
           WHEN arrays_overlap(set_spec, array('biorxiv','medrxiv','ressq','arxiv','ssrn','chemrxiv'))
             THEN 'submittedVersion'
           ELSE 'publishedVersion'
      END
    END
"""
# oxjob #880: the title normalizer had drifted into a local copy here; one definition, in the wheel.
from openalex.dlt.normalize import normalize_title_udf

# COMMAND ----------

# oxjob #881 -- ONE-TIME REBUILD SWITCH. Default false; leave it false.
#
# false (normal): MERGE with whenNotMatchedBySourceDelete. Incremental, deletes flow through CDF
#   to repo_works, no DLT refresh needed. This is the steady state.
#
# true (exceptional): overwrite the table outright. Needed once, to clear damage the MERGE cannot
#   reach -- as of 2026-08-26 that is 82,121,583 duplicate native_ids left by write paths that
#   predate dedupe_by_sequence. The MERGE cannot remove them: whenNotMatchedBySourceDelete only
#   deletes rows whose key is ABSENT from the source, and a duplicate's key is present, so every
#   copy matches and is updated in place.
#
#   Deleting them by hand is NOT a safe alternative now that the delete path is live: a DELETE of
#   duplicate rows emits delete CDF for keys that should REMAIN, and apply_changes would take the
#   last event per key and drop the record from repo_works entirely.
#
#   AFTER a rebuild run you MUST full-refresh the Repo DLT pipeline. An overwrite is not a
#   row-level change feed, so repo_parsed_backfill's readChangeFeed stream fails with "Detected a
#   data update/delete in the source table" until it is refreshed. Set the widget back to false
#   immediately afterwards.
# Text, not dropdown: a dropdown widget raises if a job supplies a value outside its choice list,
# and this is driven by the job's base_parameters (jobs/repo_backfill.yaml).
dbutils.widgets.text("rebuild", "false", "Full rebuild (overwrite)")
REBUILD = dbutils.widgets.get("rebuild").strip().lower() in ("true", "1", "yes")
print(f"RepoBackfill mode: {'REBUILD (overwrite)' if REBUILD else 'MERGE (incremental)'}")
# oxjob #2516: dry_run=true parses as usual, then writes the parse to DRY_RUN_TABLE and prints what a MERGE would change
# (per column, licence and is_oa transitions, inserts, deletes) instead of writing repo_works_backfill. Size a parser
# change's re-stamps this way before a real run: the MERGE's change feed reaches repo_works at the next Repo run.
dbutils.widgets.text("dry_run", "false", "Dry run: scratch table + diff, no write")
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() in ("true", "1", "yes")
DRY_RUN_TABLE = "openalex.transient.repo_backfill_dryrun"
assert not (DRY_RUN and REBUILD), "dry_run and rebuild are exclusive"

# Licence fallbacks, same rule as Repo.py (4afa5d56). Keep the two constants below byte-identical with Repo.py.
# CC0 named as text in dc:rights: the normaliser knows the CC0 URL and the words "public domain"
# only. Whole-value forms seen in the corpus (whitespace collapsed, lower-cased, trailing period
# stripped); a bare token would also match revocations such as "In Copyright - The CC0 1.0
# Universal license was removed from this item". The value is checked as a whole, so a
# sentence that merely mentions CC0 does not count.
CC0_TEXT_PATTERNS = (
    r"^(creative commons |cc |licen[cs]e:? ?)?cc0([- ]1\.0)?( universal| universell| uniwersalna| παγκόσμια| public domain dedication)?( \(cc0([- ]1\.0)?\))?$"
    r"|^cc-?zero$|^creative commons \(cc0\)$|^creative commons zero v?1\.0 universal( \(cc0[- ]1\.0\))?$"
    r"|^cc0:? creative commons zero( 1\.0)?( universal)?$|^cc0[- ]?(1\.0 )?no rights reserved$|^no rights reserved \(cc0( 1\.0)?\)$"
    r"|^https?://(spdx\.org|choosealicense\.com)/licenses/cc0-1\.0(\.html)?/?$|^https?://api\.github\.com/licenses/cc0-1\.0$"
    r"|^https?://(www\.)?opendefinition\.org/licenses/cc-zero/?$"
    r"|^dedicaci[óo]n de dominio p[úu]blico 1\.0 universal\.? \(cc0\)$|^uznanie cc0 1\.0 universal \(cc0 1\.0\) przekazanie do domeny publicznej$"
)
# Whole values only. The full CC0 legal-code text, the deed sentence ("To the extent possible under
# law, X has waived ...") and per-item statements ("Items in this record are available as ...") are
# deliberately not matched: an open-ended prefix would also accept a negated or qualified sentence.
# A dc:rights value that restricts access blocks every fallback below (not the dc:rights pick
# itself, whose behaviour is unchanged). Tested on the normalised form (spaces/hyphens removed).
RIGHTS_RESTRICTED_PATTERN = r"restrictedaccess|closedaccess|embargoedaccess|rightsstatements\.org/vocab/inc|incopyright"
# Licence carried outside dc:rights. Repo.py reads the harvester's re-serialised prefix (ns4:accessRights / ns4:license);
# the backfill XML keeps each repository's own prefix, so the same dcterms elements appear as dcterms:, dct: or
# unprefixed. Exact element names only (not licenseCondition, not dc:rightsHolder), text content only, one element.
ALT_RIGHTS_ELEMENT_PATTERN = r"<(?:(?:dcterms|dct):)?(?:accessRights|license)(?:\s[^>]*)?>([^<]*)</"
# For the fallbacks, dc:rights is read with or without attributes (Persee: <dc:rights xml:lang=...>), text content
# only. The existing raw_license pick is NOT changed: it still reads attribute-free dc:rights only. The fallbacks are
# not applied when (a) a dc:rights element cannot be read as text (CDATA, nested markup), (b) a dc:rights value
# restricts access, or (c) any dc:rights value already names a licence the normaliser recognises (that record
# keeps today's result: an alternative field never overrides a licence stated in dc:rights). The joined check is
# repeated per value for public domain, because the normaliser's 'works not in the public domain' exception would
# otherwise let one value cancel another.
DC_RIGHTS_TEXT_PATTERN = r"<dc:rights(?:\s[^>]*)?>([^<]*)</dc:rights>"
DC_RIGHTS_OPEN_TAG_PATTERN = r"<dc:rights(?:\s[^>]*)?(?<!/)>"   # every non-self-closing opening tag
# Used below as fallbacks when the dc:rights pick normalises to nothing (e.g. a copyright line): the dcterms fields with
# the same creativecommons.org-first selection rule, then CC0 named as text; both blocked by a restricting dc:rights
# value. The patterns are passed with lit(), so they reach the regex engine unescaped. (No comments inside the
# backslash-continued chain: a comment line would end the continuation.)

# --- cc licence block (oxjob #2516): start. Repo.py holds the same block, byte for byte ---
# oxjob #2516: a Creative Commons licence a dc:rights value states without a creativecommons.org URL. EPrints exports
# its licence codes (cc_by_4, cc_by_nc_nd, cc_by_nc_sa_4_0: underscores, which normalize_license never matched), and
# many repositories write the licence in words ("Creative Commons Attribution 4.0 International", "Atribución-
# NoComercial-SinDerivadas 3.0 España", "Creative Commons - Namensnennung 4.0"), or after a copyright line. Pure Python;
# notebooks/ingest/tests/test_repo_cc_licence.py runs it and checks RepoBackfill.py holds the same block.
# Read in this order: a CC code or abbreviation (cc by-nc-nd, cc_by_4, CCBYNC, spdx CC-BY-SA-4.0); the EPrints CC0 codes;
# the licence's name in words. The name must follow "Creative Commons" or "CC" in the value, or be the whole value
# ("Attribution-NonCommercial 4.0 International"). Its elements are read only as a chain right after the Attribution
# element, so prose after the name ("... Attribution License, which permits commercial and non-commercial use") adds
# nothing. No Attribution element, or ND with SA, names no licence ("Creative Commons", "Title 17 ... unless on a
# Creative Commons license"). Licences that reuse the BY-NC-ND letters (KOGL BY-NC-ND) never match: the letters must
# follow "CC" or "Creative Commons".
# A dc:rights value that restricts access keeps a licence read here off the record: RIGHTS_RESTRICTED_PATTERN plus the
# COAR closed / restricted / embargoed URIs and "metadata only", tested on the same form (lower case, no spaces/hyphens).
CC_RIGHTS_BLOCK_PATTERN = RIGHTS_RESTRICTED_PATTERN + r"|coar/access_right/c_(?:14cb|16ec|f1cf)|metadataonly"
# "cc_by_og" is not CC BY: DERA's EPrints code for the UK Open Government Licence (its pages say so).
CC_CODE_RE = re.compile(r"(?<![a-z0-9])cc[\s:-]*by((?:[\s/-]*(?:nc|nd|sa)(?![a-z]))*)(?![a-z])(?!\s*og(?![a-z]))")
CC_NAMED_BY_RE = re.compile(r"creative[\s-]*commons?[\s:-]*(?:licen[cs]e[\s:-]*)?(?:«\s*)?by((?:[\s/-]*(?:nc|nd|sa)(?![a-z]))*)(?![a-z])")
CC_ZERO_RE = re.compile(r"^cc[\s-]*(?:zero|public[\s-]*domain)$")
# A whole value that is only the letters: "by-nc-nd", "BY-SA 4.0".
CC_BARE_LETTERS_RE = re.compile(r"^by((?:[\s/-]+(?:nc|nd|sa))+)(?:\s*v?\d(?:\.\d)?)?$")
CC_CONTEXT_RE = re.compile(r"creative[\s-]*commons?|(?<![a-z])(?<!data )commons attribution|クリエイティブ・?コモンズ|(?<![a-z0-9])cc(?![a-z0-9])")
_CC_BY = (r"attribution|atribuci[oó]n|reconocimiento|atribui[cç][aã]o|reconeixement|namensnennung|uznanie autorstwa"
          r"|naamsvermelding|attribuzione|paternit[eé]|nimeä|erkännande|navngivelse|uznání autorství")
_CC_NC = (r"non[\s-]?commerc|no[\s-]?comercial|n[ãa]o[\s-]?comercial|nicht[\s-]?kommerz|keine kommerzielle nutzung"
          r"|niekomercyjn|niet[\s-]?commercieel|pas d.utilisation commerciale|非営利|epäkaupallinen|ickekommersiell"
          r"|ikke[\s-]?kommerciel|nekomerční")
_CC_ND = (r"no[\s-]?deriv|sin[\s-]?(?:obras?[\s-]?)?deriva|no[\s-]?derivadas|sem[\s-]?(?:obras?[\s-]?)?deriva"
          r"|vedada a cria[cç][aã]o de obras derivadas|sense[\s-]?obr\w*[\s-]?deriva|keine[\s-]?bearbeitung"
          r"|bez utworów zależnych|geen[\s-]?afgeleide|non[\s-]?opere[\s-]?derivate|pas de modification|改変禁止"
          r"|ei muutoksia|inga bearbetningar|ingen bearbejdelse")
_CC_SA = (r"share[\s-]?alike|compartir[\s-]?igual|compartilha[\s-]?igual|weitergabe unter gleichen(?: bedingungen)?"
          r"|na tych samych warunkach|gelijk[\s-]?delen|condividi allo stesso modo|partage dans les m[êe]mes|継承"
          r"|jaa samoin|dela lika")
CC_PARTS = (("nc", re.compile(_CC_NC)), ("nd", re.compile(_CC_ND)), ("sa", re.compile(_CC_SA)))
_CC_CHAIN = (r"(?:" + _CC_BY + r"|(?<=コモンズ)\s*表示|(?<=コモンズ・)表示)((?:[\s,.:;–—‐・/-]*(?:uso\s+|u[żz]ycie\s+)?(?:"
             + _CC_NC + "|" + _CC_ND + "|" + _CC_SA + r")\w*)*)")
CC_CHAIN_RE = re.compile(_CC_CHAIN)
# The whole value is the licence name: "Attribution", "Attribution-NonCommercial-NoDerivatives 4.0 International",
# "Atribución-NoComercial-SinDerivadas 3.0 España", "Reconocimiento – NoComercial – SinObraDerivada".
CC_WHOLE_NAME_RE = re.compile(r"^(?:licen[cs]e:?\s*|udzielam licencji\.?\s*)?" + _CC_CHAIN.replace("((?:", "(?:(?:", 1)
                              + r"(?:\s*v?\d(?:\.\d)?(?:\s+[^\W\d]+){0,3}"
                              r"|\s+(?:international|internacional|internationale?|unported|generic|licen[cs]e))?\.?$")


def _cc_flavour(parts):
    """{'nc', 'nd', 'sa'} -> 'cc-by-nc-nd' etc. (every licence here has BY); None for ND with SA."""
    if "nd" in parts and "sa" in parts:
        return None
    return "-".join(["cc-by"] + [p for p in ("nc", "nd", "sa") if p in parts])


def cc_licence_from_text(value):
    """The Creative Commons licence one dc:rights value names in a code or in words, else None."""
    if not value:
        return None
    t = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value).lower().replace("_", " ")).strip()
    m = CC_CODE_RE.search(t)
    if m:
        return _cc_flavour(set(re.findall(r"nc|nd|sa", m.group(1))))
    if CC_ZERO_RE.match(t):
        return "public-domain"
    m = CC_BARE_LETTERS_RE.match(t)
    if m:
        return _cc_flavour(set(re.findall(r"nc|nd|sa", m.group(1))))
    if CC_CONTEXT_RE.search(t):
        m = CC_NAMED_BY_RE.search(t)
        if m:
            return _cc_flavour(set(re.findall(r"nc|nd|sa", m.group(1))))
        m = CC_CHAIN_RE.search(t)
    elif CC_WHOLE_NAME_RE.match(t):
        m = CC_CHAIN_RE.search(t)
    else:
        return None
    if not m:
        return None
    return _cc_flavour({p for p, rx in CC_PARTS if rx.search(m.group(1))})


def cc_licence_from_rights(values):
    """The licence of the first dc:rights value that names a Creative Commons licence (see above), else None."""
    for v in values or []:
        lic = cc_licence_from_text(v)
        if lic:
            return lic
    return None
# --- cc licence block (oxjob #2516): end ---


# oxjob #2516: as in Repo.py, the CC licence a record states in a code or in words when the pick found none or only
# an open-access statement, unless a value restricts access. The backfill reads dc:rights out of the raw XML, so
# entities (&amp;, &#243;) are decoded first. It is applied after the dedupe (below), so the tiebreak hash never sees
# it (#1800), and is_oa still reads the pick: a licence read here never opens a copy by itself.
@pandas_udf(StringType())
def cc_licence_from_rights_udf(rights: pd.Series) -> pd.Series:
    return rights.apply(lambda r: None if r is None else cc_licence_from_rights([html.unescape(v) for v in r]))

# COMMAND ----------

# first run
# df = (
#     spark.read
#     .parquet("s3a://openalex-ingest/parquet_output/")
# )

# save to table so we don't need to query s3 next time
# df.write.format("delta").mode("overwrite").saveAsTable("openalex.repo.repo_items_backfill")

# second run
df = spark.table("openalex.repo.repo_items_backfill")

# COMMAND ----------

clean_df = df.withColumn("cleaned_xml",
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(col("api_raw"), 
                        r'^"{3}', ''
                    ),
                    r'"{3}$', ''
                ),
                r'\\\"', '"'
            ),
            r'""', '"'
        )
    )
).drop("api_raw")

# COMMAND ----------

# udfs
def normalize_language_code(lang_code):
    """
    Normalize language codes to ISO 639-1 two-letter lowercase format.
    Handles ISO 639-1 (two-letter), ISO 639-2/T (three-letter), and common variations.
    """
    if not lang_code or not isinstance(lang_code, str):
        return None

    lang_code = lang_code.strip().lower()
    
    # remove any [[iso]] prefix
    if "[[iso]]" in lang_code:
        lang_code = lang_code.replace("[[iso]]", "")
        
    # handle special cases
    if lang_code in ["null", "und", "other"]:
        return None
    
    # handle codes with regional variants
    if "_" in lang_code:
        lang_code = lang_code.split("_")[0]
        
    # handle multiple codes (e.g., "tr; en")
    if ";" in lang_code:
        lang_code = lang_code.split(";")[0].strip()

    # full names to codes mapping
    names_to_codes = {
        "english": "en",
        "spanish": "es",
        "french": "fr",
        "german": "de",
        "chinese": "zh",
        "russian": "ru",
        "japanese": "ja",
        "arabic": "ar",
        "portuguese": "pt",
        "italian": "it",
    }

    # ISO 639-2 to ISO 639-1 mapping
    three_to_two = {
        # most common languages first for quick matches
        "eng": "en", "fra": "fr", "spa": "es", "deu": "de", "rus": "ru",
        "zho": "zh", "jpn": "ja", "ara": "ar", "por": "pt", "ita": "it",
        # additional languages
        "abk": "ab", "aar": "aa", "afr": "af", "aka": "ak", "alb": "sq",
        "amh": "am", "arg": "an", "arm": "hy", "asm": "as", "ava": "av",
        "ave": "ae", "aym": "ay", "aze": "az", "bam": "bm", "bak": "ba",
        "baq": "eu", "bel": "be", "ben": "bn", "bih": "bh", "bis": "bi",
        "bos": "bs", "bre": "br", "bul": "bg", "bur": "my", "cat": "ca",
        "cha": "ch", "che": "ce", "nya": "ny", "chi": "zh", "chu": "cu",
        "chv": "cv", "cor": "kw", "cos": "co", "cre": "cr", "hrv": "hr",
        "cze": "cs", "dan": "da", "div": "dv", "dut": "nl", "dzo": "dz",
        "epo": "eo", "est": "et", "ewe": "ee", "fao": "fo", "fij": "fj",
        "fin": "fi", "fre": "fr", "fry": "fy", "ful": "ff", "geo": "ka",
        "ger": "de", "gla": "gd", "gle": "ga", "glg": "gl", "glv": "gv",
        "gre": "el", "grn": "gn", "guj": "gu", "hat": "ht", "hau": "ha",
        "heb": "he", "her": "hz", "hin": "hi", "hmo": "ho", "hun": "hu",
        "ice": "is", "ido": "io", "iii": "ii", "iku": "iu", "ile": "ie",
        "ina": "ia", "ind": "id", "ipk": "ik", "isl": "is", "jav": "jv",
        "kan": "kn", "kau": "kr", "kas": "ks", "kaz": "kk", "khm": "km",
        "kik": "ki", "kin": "rw", "kir": "ky", "kom": "kv", "kon": "kg",
        "kor": "ko", "kua": "kj", "kur": "ku", "lao": "lo", "lat": "la",
        "lav": "lv", "lim": "li", "lin": "ln", "lit": "lt", "ltz": "lb",
        "lub": "lu", "lug": "lg", "mac": "mk", "mah": "mh", "mal": "ml",
        "mao": "mi", "mar": "mr", "may": "ms", "mlg": "mg", "mlt": "mt",
        "mon": "mn", "nau": "na", "nav": "nv", "nbl": "nr", "nde": "nd",
        "ndo": "ng", "nep": "ne", "nno": "nn", "nob": "nb", "nor": "no",
        "oji": "oj", "ori": "or", "orm": "om", "oss": "os", "pan": "pa",
        "per": "fa", "pli": "pi", "pol": "pl", "pus": "ps", "que": "qu",
        "roh": "rm", "rum": "ro", "run": "rn", "sag": "sg", "san": "sa",
        "sin": "si", "slo": "sk", "slv": "sl", "sme": "se", "smo": "sm",
        "sna": "sn", "snd": "sd", "som": "so", "sot": "st", "srd": "sc",
        "srp": "sr", "ssw": "ss", "sun": "su", "swa": "sw", "swe": "sv",
        "tah": "ty", "tam": "ta", "tat": "tt", "tel": "te", "tgk": "tg",
        "tgl": "tl", "tha": "th", "tib": "bo", "tir": "ti", "ton": "to",
        "tsn": "tn", "tso": "ts", "tuk": "tk", "tur": "tr", "twi": "tw",
        "uig": "ug", "ukr": "uk", "urd": "ur", "uzb": "uz", "ven": "ve",
        "vie": "vi", "vol": "vo", "wel": "cy", "wln": "wa", "wol": "wo",
        "xho": "xh", "yid": "yi", "yor": "yo", "zha": "za", "zul": "zu"
    }
    
    # check if it's already a valid two-letter code
    if len(lang_code) == 2:
        return lang_code
        
    # check full names
    if lang_code in names_to_codes:
        return names_to_codes[lang_code]
        
    # check three-letter codes
    if len(lang_code) == 3:
        return three_to_two.get(lang_code)
        
    return None

url_pattern = r"(https?://\S+|www\.\S+)"

   
def normalize_license(text):
    if not text:
        return None

    normalized_text = text.replace(" ", "").replace("-", "").lower()

    license_lookups = [
        # open Access patterns
        ("infoeureposematicsaccess", "other-oa"),
        ("openaccess", "other-oa"),
        
        # publisher-specific
        ("elsevier.com/openaccess/userlicense", "other-oa"),
        ("pubs.acs.org/page/policy/authorchoice_termsofuse.html", "other-oa"),
        # oxjob #1228: a publisher's own licence for its paid open access option, registered only on OA articles,
        # counts as open (help.openalex.org Licenses: publisher-specific open licences, like ACS AuthorChoice above).
        # IEEE OAPA.html (IEEE.html, the copyright transfer, stays unmapped); Optica #VOR-OA (#VOR, #AM, #GOV and
        # #OPG-OO stay unmapped). Keep in step with the copies in PDF.py and RepoBackfill.py (oxjob #991).
        ("licenseinformation/oapa.html", "other-oa"),
        ("10.1364/oa_license_v1#voroa", "other-oa"),
        ("10.1364/oa_license_v2#voroa", "other-oa"),
        ("arxiv.orgperpetual", "other-oa"),
        ("arxiv.orgnonexclusive", "other-oa"),
        # oxjob #1169: the UK Open Government Licence (nationalarchives.gov.uk/doc/open-government-licence) and the
        # Open Government Licence - Canada (open.canada.ca/en/open-government-licence-canada) are open licences like
        # CC BY; Crown-copyright articles and Natural Resources Canada's GEOSCAN reports (10.4095) carry them.
        ("opengovernmentlicence", "other-oa"),
        ("opengovernmentlicense", "other-oa"),
        
        # creative Commons licenses
        # Text form ("CC BY-NC-ND 4.0") and URL form
        # ("creativecommons.org/licenses/by-nc-nd/4.0") differ once hyphens are
        # stripped, so each flavor needs both keys. Longest-first.
        ("ccbyncnd", "cc-by-nc-nd"),
        ("licenses/byncnd", "cc-by-nc-nd"),
        ("ccbyncsa", "cc-by-nc-sa"),
        ("licenses/byncsa", "cc-by-nc-sa"),
        ("ccbynd", "cc-by-nd"),
        ("licenses/bynd", "cc-by-nd"),
        ("ccbysa", "cc-by-sa"),
        ("licenses/bysa", "cc-by-sa"),
        ("ccbync", "cc-by-nc"),
        ("licenses/bync", "cc-by-nc"),
        ("ccby", "cc-by"),
        # plain BY needs a trailing boundary, or a malformed flavor
        # (licenses/by-n345667c-nd) resolves to the most permissive answer.
        ("creativecommons.org/licenses/by/", "cc-by"),
        ("licenses/by/", "cc-by"),
        ("licenses/by4", "cc-by"),
        ("licenses/by3", "cc-by"),
        ("licenses/by2", "cc-by"),
        ("licenses/by1", "cc-by"),
        
        # public domain
        ("publicdomain", "public-domain"),
        
        # software/Dataset licenses
        ("mit ", "mit"),
        ("gpl3", "gpl-3"),
        ("gpl2", "gpl-2"),
        ("gpl", "gpl"),
        ("apache2", "apache-2.0")
    ]

    for lookup, license in license_lookups:
        if lookup in normalized_text:
            if license == "public-domain" and "worksnotinthepublicdomain" in normalized_text:
                continue
            return license

    return None

def detect_version_from_xml(cleaned_xml, native_id):
    """
    Detect version from XML content and native_id based on regex patterns
    Returns 'acceptedVersion', 'publishedVersion', or 'submittedVersion'
    """
    
    ACCEPTED_VERSION_REPOS = [
        "oai:catalog.lib.kyushu-u.ac.jp",
        "oai:cronfa.swan.ac.uk",
        "oai:dora",
        "oai:e-space.mmu.ac.uk",
        "oai:hrcak.srce.hr",
        "oai:infocom.co.jp",
        "oai:library.wur.nl",
        "oai:lirias2repo.kuleuven.be",
        "oai:mro.massey.ac.nz",
        "oai:raumplan.iaus.ac.rs",
        "oai:repository.arizona.edu",
        "oai:repository.cardiffmet.ac.uk",
        "oai:researchbank.swinburne.edu.au",
        "oai:researchonline.gcu.ac.uk",
        "oai:rke.abertay.ac.uk",
        "oai:shura.shu.ac.uk",
        "oai:taju.uniarts.fi"
    ]
    
    # Check if native_id starts with any of the accepted repo prefixes
    if native_id:
        native_id_str = str(native_id)
        for repo in ACCEPTED_VERSION_REPOS:
            if native_id_str.startswith(repo + ":"):
                return "acceptedVersion"
    
    if not cleaned_xml:
        return "submittedVersion"
    
    search_text = str(cleaned_xml).lower()
    
    accepted_patterns = [
        r"accepted.?version",
        r"version.?accepted", 
        r"accepted.?manuscript",
        r"peer.?reviewed",
        r"refereed/peer-reviewed"
    ]
    
    for pattern in accepted_patterns:
        if re.search(pattern, search_text, re.IGNORECASE | re.MULTILINE | re.DOTALL):
            return "acceptedVersion"
    
    published_patterns = [
        r"publishedversion",
        r"published.*version",
        r"version.*published"
    ]
    
    for pattern in published_patterns:
        if re.search(pattern, search_text, re.IGNORECASE | re.MULTILINE | re.DOTALL):
            return "publishedVersion"
    
    return "submittedVersion"


@pandas_udf(StringType())
def normalize_license_udf(license_series: pd.Series) -> pd.Series:
    return license_series.apply(normalize_license)

@pandas_udf(StringType())
def normalize_language_code_udf(language_code_series: pd.Series) -> pd.Series:
    return language_code_series.apply(normalize_language_code)

@pandas_udf(StringType())
def detect_version_udf(metadata_series: pd.Series, native_id_series: pd.Series) -> pd.Series:
    return pd.Series([
        detect_version_from_xml(metadata, native_id) 
        for metadata, native_id in zip(metadata_series, native_id_series)
    ])


# COMMAND ----------

MAX_TITLE_LENGTH = 5000
MIN_ABSTRACT_LENGTH = 100
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500
MAX_AFFILIATION_STRING_LENGTH = 1000

# oxjob #1800: which dc:description is the abstract. Many repositories put a short note first ("Orientador: ...",
# "107 p.", "Thesis (M. Eng.)--MIT, 1998.", a DSpace deposit notice) and the abstract in a later dc:description, and
# this parser took only the first (and dropped it under 100 characters). On the endpoints listed in DESC_CHOOSER_TABLE
# (each judged on a sample: the later description was the work's own abstract), a weak first description (blank, under
# 100 characters after trim, or a deposit notice) gives way to the first later one of 200+ characters that does not
# open like a notice and is not a Zenodo/Plazi "Published as part of ..." citation line. Used below only where this
# parser's own pick yields nothing (gain-only). Rule desc-v2; its bar passed for this path (95 of 100 works that showed
# no abstract now show their own; oxjobs #1800 EXPLORE § 38). Repo.py's live path does NOT use it: there the same rule
# failed its bar on works that show no abstract (79 of 88). Revert: empty the table and re-run this job, or git revert.
DESC_CHOOSER_TABLE = "openalex.repo.desc_chooser_endpoints"


def read_desc_chooser_endpoints(spark, table=DESC_CHOOSER_TABLE):
    """Endpoint ids where the description chooser is on, read once (a few hundred rows), sorted for a stable plan.
    Fails loudly if the table can't be read, like read_endpoint_hosts."""
    return sorted(r.endpoint_id for r in spark.read.table(table).select("endpoint_id").distinct().collect())


def desc_chooser_sql(descs, endpoint, endpoints):
    """SQL for the abstract among `descs` (array<string>, document order) of a record from `endpoint` (a column);
    `endpoints` = the ids where the chooser is on, inlined as a literal list (no join, no stateful-plan change)."""
    opens_like_notice = ("rlike(trim({x}), '^(Submitted by |Made available in DSpace on |Approved for entry into archive "
                         "by |Rejected by |Item withdrawn by )')")
    first = f"get({descs}, 0)"
    weak = (f"(coalesce(length(trim({first})), 0) < 100"
            f" or ({opens_like_notice.format(x=first)} and {first} like '%No. of bitstreams%'))")
    good = (f"x -> length(trim(x)) >= 200 and not {opens_like_notice.format(x='x')}"
            " and not trim(x) like 'Published as part of %'")
    later = f"get(filter(slice({descs}, 2, greatest(size({descs}) - 1, 1)), {good}), 0)"
    if not endpoints:
        return first
    on = f"{endpoint} in ({', '.join(repr(e) for e in endpoints)})"
    return f"case when {on} and {weak} then coalesce({later}, {first}) else {first} end"


DESC_CHOOSER_ENDPOINTS = read_desc_chooser_endpoints(spark)

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")
# oxjob #881: whenMatchedUpdateAll does NOT evolve the target schema on its own, and the
# mergeSchema option below only applies to the first-run branch. Without this the new
# set_spec/dc_format columns are silently dropped on every MERGE.
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# oxjob #881: set_spec / dc_format below are the repository's OWN classification of the record
# ("photographs", "theses", "datasets") and its MIME type. Repo.py reads them from its parsed
# struct; here the raw XML is regexed, and <setSpec> is confirmed BARE -- zero namespaced
# occurrences across a 2M-row sample of repo_items_backfill.api_raw (oxjob #881 evidence/q60).
# Both are dropped again before repo_works via except_column_list in Repo.py, so they never
# enter the cross-source union where repo_works is the canonical schema donor.

# oxjob #1407: native_id is the header id with a placeholder host replaced by the endpoint's own
# host -- the same shared function as Repo.py, so live and backfill copies of one record keep one
# key. The raw id stays in oai_identifier for the host-parsing expressions; it is not selected below.
# oxjob #1800: the abstract. abstract_raw (the first attribute-free <dc:description>, kept at 100+ chars) stays the
# abstract whenever it yields one, byte for byte. Only when it yields nothing, on the description-chooser endpoints
# (desc_chooser_sql above), a later description is taken (`_abstract_desc`, filled in after the dedupe below);
# descriptions with attributes
# (<dc:description xml:lang="pt">, matched after a space) count as candidates there. Gain-only: no record that had an
# abstract on this path gets a different one. (Reading attributed descriptions on every endpoint failed its bar,
# 66 of 100: library catalogue notes; #1800 EXPLORE § 38.)
parsed_df = clean_df \
    .withColumn("oai_identifier", regexp_extract(col("cleaned_xml"), r"<identifier>(.*?)</identifier>", 1)) \
    .transform(lambda d: with_rekeyed_native_id(d, read_endpoint_hosts(spark), "endpoint_id")) \
    .withColumn("native_id_namespace", lit("pmh")) \
    .withColumn("title", substring(regexp_extract(col("cleaned_xml"), r"<dc:title.*?>(.*?)</dc:title>", 1), 0, MAX_TITLE_LENGTH)) \
    .withColumn("normalized_title", normalize_title_udf(col("title"))) \
    .withColumn("authors", 
        expr(f"""
            transform(
                regexp_extract_all(cleaned_xml, '<dc:creator>(.*?)</dc:creator>'),
                x -> struct(
                    cast(null as string) as given,
                    cast(null as string) as family,
                    substring(x, 0, {MAX_AUTHOR_NAME_LENGTH}) as name,
                    cast(null as string) as orcid,
                    array(struct(
                        cast(null as string) as name,
                        cast(null as string) as department,
                        cast(null as string) as ror_id
                    )) as affiliations
                )
            )
        """)) \
    .withColumn("raw_native_types", expr("regexp_extract_all(cleaned_xml, '<dc:type.*?>(.*?)</dc:type>', 1)")) \
    .withColumn("set_spec", expr("regexp_extract_all(cleaned_xml, '<setSpec>(.*?)</setSpec>', 1)")) \
    .withColumn("dc_format", expr("regexp_extract_all(cleaned_xml, '<dc:format.*?>(.*?)</dc:format>', 1)")) \
    .withColumn("_best", best_type_udf(col("raw_native_types"))) \
    .withColumn("raw_native_type", col("_best.raw_native_type")) \
    .withColumn("type", lit(None).cast("string")) \
    .drop("_best") \
    .transform(apply_repo_policy_filters) \
    .withColumn("identifiers", 
        expr("""
            transform(
                regexp_extract_all(cleaned_xml, '<dc:identifier>(.*?)</dc:identifier>'),
                x -> trim(x)
            )
        """)) \
    .withColumn("ids",
        extract_ids_udf(
            col("identifiers"),
            col("native_id")
        )) \
    .withColumn("version", coalesce(
        expr(PMC_VERSION_FROM_SET_SPEC_EXPR),
        detect_version_udf(col("cleaned_xml"), col("oai_identifier")))) \
    .withColumn("language", normalize_language_code_udf(regexp_extract(col("cleaned_xml"), r"<dc:language.*?>(.*?)</dc:language>", 1))) \
    .withColumn("published_date",
        expr("""
            array_min(
                filter(
                    transform(
                        regexp_extract_all(cleaned_xml, '<dc:date.*?>(.*?)</dc:date>'),
                        date_str -> coalesce(
                            -- ISO format with timezone
                            to_date(to_timestamp(date_str, "yyyy-MM-dd'T'HH:mm:ss'Z'")),
                            -- ISO format without timezone
                            to_date(to_timestamp(date_str, "yyyy-MM-dd'T'HH:mm:ss")),
                            -- Regular date
                            to_date(date_str, "yyyy-MM-dd"),
                            -- Month and year
                            to_date(date_str, "yyyy-MM"),
                            -- Period-separated format
                            to_date(regexp_replace(date_str, '\\.', '-'), "yyyy-MM-dd"),
                            -- Year only
                            to_date(
                                if(length(trim(date_str)) = 4, concat(date_str, "-01-01"), null),
                                "yyyy-MM-dd"
                            )
                        )
                    ),
                    d -> d is not null and year(d) >= 1900
                )
            )
        """)) \
    .withColumn("created_date", col("published_date")) \
    .withColumn("updated_date", to_date(
        regexp_extract(col("cleaned_xml"), r"<datestamp>(.*?)</datestamp>", 1))) \
    .withColumn("abstract_raw", 
        element_at(expr("regexp_extract_all(cleaned_xml, '<dc:description>(.*?)</dc:description>')"), 1)) \
    .withColumn("_descs", expr("regexp_extract_all(cleaned_xml, '<dc:description(?: [^>]*)?>(.*?)</dc:description>', 1)")) \
    .withColumn("_desc_first", expr("get(_descs, 0)")) \
    .withColumn("_desc_chosen", expr(desc_chooser_sql("_descs", "endpoint_id", DESC_CHOOSER_ENDPOINTS))) \
    .withColumn("abstract",
        when(length(col("abstract_raw")) >= MIN_ABSTRACT_LENGTH, 
            substring(col("abstract_raw"), 0, MAX_ABSTRACT_LENGTH))
        .otherwise(lit(None))) \
    .withColumn("_abstract_desc",
        when(col("abstract").isNull() & (col("_desc_chosen") != col("_desc_first")),
            substring(col("_desc_chosen"), 0, MAX_ABSTRACT_LENGTH))) \
    .drop("_descs", "_desc_first", "_desc_chosen") \
    .withColumn("source_name", 
        regexp_extract(col("cleaned_xml"), r"<dc:source.*?>(.*?)</dc:source>", 1)) \
    .withColumn("publisher", 
        regexp_extract(col("cleaned_xml"), r"<dc:publisher.*?>(.*?)</dc:publisher>", 1)) \
    .withColumn("urls",
        expr("""
            transform(
                case
                    when size(regexp_extract_all(cleaned_xml, '<dc:identifier>(http.*?)</dc:identifier>')) > 0
                    then regexp_extract_all(cleaned_xml, '<dc:identifier>(http.*?)</dc:identifier>')
                    else regexp_extract_all(cleaned_xml, '<dc:relation>(http.*?)</dc:relation>')
                end,
                x -> struct(
                    x as url,
                    case when lower(x) like '%pdf%' then 'pdf' else 'html' end as `content-type`,
                    case when lower(x) like '%pdf%' then 'pdf' else 'html' end as `content_type`
                )
            )
        """)) \
    .withColumn("raw_license",
        expr("""
            case 
                when size(regexp_extract_all(cleaned_xml, '<dc:rights>(.*?creativecommons.org.*?)</dc:rights>')) > 0 
                then element_at(regexp_extract_all(cleaned_xml, '<dc:rights>(.*?creativecommons.org.*?)</dc:rights>'), 1)
                else element_at(regexp_extract_all(cleaned_xml, '<dc:rights>(.*?)</dc:rights>'), 1)
            end
        """)) \
    .withColumn("_alt_rights", regexp_extract_all(col("cleaned_xml"), lit(ALT_RIGHTS_ELEMENT_PATTERN), lit(1))) \
    .withColumn("raw_alt_license",
        expr("""
            case
                when size(filter(_alt_rights, x -> x like '%creativecommons.org%')) > 0
                then filter(_alt_rights, x -> x like '%creativecommons.org%')[0]
                else get(_alt_rights, 0)
            end
        """)) \
    .withColumn("_dc_rights_all", regexp_extract_all(col("cleaned_xml"), lit(DC_RIGHTS_TEXT_PATTERN), lit(1))) \
    .withColumn("_rights_blocked",
        (size(regexp_extract_all(col("cleaned_xml"), lit(DC_RIGHTS_OPEN_TAG_PATTERN), lit(0))) > size(col("_dc_rights_all")))
        | exists(col("_dc_rights_all"),
            lambda x: coalesce(regexp_replace(lower(x), "[ -]", "").rlike(RIGHTS_RESTRICTED_PATTERN), lit(False)))
        | normalize_license_udf(when(size(col("_dc_rights_all")) > 0, concat_ws(" | ", col("_dc_rights_all")))).isNotNull()
        | exists(col("_dc_rights_all"),
            lambda x: coalesce(regexp_replace(lower(x), "[ -]", "").contains("publicdomain")
                               & ~regexp_replace(lower(x), "[ -]", "").contains("worksnotinthepublicdomain"), lit(False)))) \
    .withColumn("_cc0_text",
        exists(col("_dc_rights_all"),
            lambda x: coalesce(regexp_replace(regexp_replace(lower(trim(x)), r"\s+", " "), r"\.$", "").rlike(CC0_TEXT_PATTERNS), lit(False)))) \
    .withColumn("license",
        coalesce(
            normalize_license_udf(col("raw_license")),
            when(~col("_rights_blocked"),
                coalesce(
                    normalize_license_udf(col("raw_alt_license")),
                    when(col("_cc0_text"), lit("public-domain")))))) \
    .withColumn("_cc_licence",
        cc_licence_from_rights_udf(when(
            (col("license").isNull() | (col("license") == "other-oa"))
            & ~exists(col("_dc_rights_all"),
                lambda x: coalesce(regexp_replace(lower(x), "[ -]", "").rlike(CC_RIGHTS_BLOCK_PATTERN), lit(False))),
            col("_dc_rights_all")))) \
    .drop("_alt_rights", "raw_alt_license", "_dc_rights_all", "_rights_blocked", "_cc0_text") \
    .withColumn("issue", lit(None).cast("string")) \
    .withColumn("volume", lit(None).cast("string")) \
    .withColumn("first_page", lit(None).cast("string")) \
    .withColumn("last_page", lit(None).cast("string")) \
    .withColumn("is_retracted", lit(None).cast("boolean")) \
    .withColumn("funders", array(
        struct(
            lit(None).cast("string").alias("doi"),
            lit(None).cast("string").alias("ror"),
            lit(None).cast("string").alias("name"),
            array(lit(None).cast("string")).alias("awards")
        )
    )) \
    .withColumn("references", lit(None).cast(ArrayType(StructType([  # oxjob #1700: NULL, as Repo.py and IRDB.py
        StructField(n, StringType(), True) for n in ("doi", "pmid", "arxiv", "title", "authors", "year", "raw")])))) \
    .withColumn("mesh", lit(None).cast("string")) \
    .withColumn(
    "is_oa",
        when(
            lower(col("license")).startswith("cc") |
            lower(col("license")).contains("other-oa") |
            lower(col("license")).contains("public-domain") |
            expr(TRUSTED_HOST_IS_OA_EXPR),
            lit(True)
        ).otherwise(lit(False))
    ) \
    \
    .filter(size(col("urls")) > 0) \
    .filter(size(filter(col("urls"), lambda x: ~x.url.contains("doi.org"))) > 0)

# Select final columns in the same order as DLT
parsed_df = parsed_df.select(
    "native_id",
    "native_id_namespace",
    "title",
    "normalized_title",
    "authors",
    "ids",
    "raw_native_type",
    "type",
    "version",
    "license",
    "language",
    "published_date",
    "created_date",
    "updated_date",
    "issue",
    "volume",
    "first_page",
    "last_page",
    "is_retracted",
    "abstract",
    "source_name",
    "publisher",
    "funders",
    "references",
    "urls",
    "mesh",
    "is_oa",
    "endpoint_id",
    "set_spec",
    "dc_format",
    "_abstract_desc",
    "_cc_licence"
)

# oxjob #911: the whole backfill corpus arrived in S3 as one parquet_output export
# (607 chunks, all mtime 2025-01-16 -- verified via s3 ls). ingested_at means "when the
# source file arrived in our S3", so every backfill row carries that batch timestamp;
# stamped physically on the table 2026-08-28, re-stamped here so MERGE/rebuild runs
# keep the column populated.
parsed_df = parsed_df.withColumn(
    "ingested_at", lit("2025-01-16 15:52:00").cast("timestamp"))

# Keep the newest record per native_id. oxjob #837/#881: this WAS
#   parsed_df.sort(col("updated_date").desc()).dropDuplicates(["native_id"])
# which only looks deterministic -- dropDuplicates does not promise to keep the first row of a
# preceding sort, so reruns could pick a different winner for the same key. That matters here:
# repo_works_backfill holds 82,121,583 duplicate native_ids over 228,564,455 distinct keys, so
# which row survives is decided for 82M keys. 3,932,463 rows are additionally tied on a NULL
# updated_date (though 99.99% of those carry no usable URL and never reach repo_works anyway).
# dedupe_by_sequence appends a content hash, giving a total order.
# oxjob #1800: the content-hash tiebreak reads the abstract the old rule gave (`abstract`), never the chooser's
# (`_abstract_desc`), so every native_id keeps the copy it kept before. On 7 Nov the first run hashed the new abstract and
# 174,783 tied duplicates switched to another endpoint's copy (endpoint, URLs, licence, OA). The chooser's text fills
# in after the dedupe, only where the kept copy had no abstract.
parsed_df = dedupe_by_sequence(
    parsed_df,
    keys=["native_id"],
    order_by=[col("updated_date").desc_nulls_last()],
    hash_columns=[c for c in parsed_df.columns if c not in ("_abstract_desc", "_cc_licence")],
)
parsed_df = parsed_df.withColumn("abstract", coalesce(col("abstract"), col("_abstract_desc"))).drop("_abstract_desc")
# oxjob #2516: the CC licence read from every dc:rights value (cc_licence_from_rights_udf above) fills in after the dedupe
# too, for the same reason; is_oa was decided before it from the pick. Revert: drop these lines and _cc_licence above.
parsed_df = parsed_df.withColumn("license", coalesce(col("_cc_licence"), col("license"))).drop("_cc_licence")

# oxjob #881: MERGE that also DELETES (Casey, 2026-08-26).
#
# This used to be a MERGE with whenMatchedUpdateAll/whenNotMatchedInsertAll and NO delete clause,
# so a record the parse stopped producing was never removed from the target. That is why
# tightening the filters upstream did nothing to what was already stored: 14,945,709 untyped
# archive.org rows, ~16.4M denylisted types, ~11.5M short titles and 3,932,145 rows with no usable
# URL all survived filters meant to exclude them, plus 82,121,583 duplicate native_ids.
#
# whenNotMatchedBySourceDelete makes the target a true mirror of the parse. Deletions emit
# row-level CDF, which Repo.py's repo_parsed_backfill now forwards, and dlt.apply_changes turns
# into real deletes in repo_works via apply_as_deletes.
#
# A rebuild (mode overwrite) was tried first and reverted: an overwrite is not a row-level change
# feed, so it breaks the readChangeFeed stream and forces a DLT full refresh on EVERY run. This
# keeps the pipeline incremental.
target_table = "openalex.repo.repo_works_backfill"

from delta.tables import DeltaTable

# GUARD -- whenNotMatchedBySourceDelete deletes every target row the source does not contain, so
# a short or partial parse would silently wipe the corpus. If the source is unexpectedly small
# relative to the target, stop rather than delete.
#
# Compare against the target's DISTINCT native_id count, not its row count. parsed_df is already
# deduped on native_id (dedupe_by_sequence above), while the target still carries duplicates from
# write paths that predate that dedup -- 82,121,583 of them as of 2026-08-26. Comparing a deduped
# source against a duplicated target understates the ratio badly and trips this guard on a
# perfectly healthy parse: on the 2026-08-26 run it read 60.3% against rows where the real figure
# was 81.9% against distinct ids. native_id is the MERGE key, so distinct is the honest baseline.
# After the first successful run the target is unique and the two measures converge.
MIN_KEEP_RATIO = 0.75

# The guard applies to BOTH modes -- an overwrite with a short parse destroys just as much as a
# delete-by-absence does.
# Persist before the guard: its count() materializes the full parse, and without a cache Spark
# recomputes the entire 310M-row regex parse a second time for the write. On the 4-worker run the
# parse dominates wall-clock, so this is close to a 2x on the whole job.
from pyspark import StorageLevel
parsed_df = parsed_df.persist(StorageLevel.MEMORY_AND_DISK)

if spark.catalog.tableExists(target_table):
    _src = parsed_df.count()
    _tgt_rows = spark.table(target_table).count()
    _tgt_keys = spark.table(target_table).select("native_id").distinct().count()
    _ratio = (_src / _tgt_keys) if _tgt_keys else 1.0
    if _tgt_keys > 0 and _ratio < MIN_KEEP_RATIO:
        raise ValueError(
            f"RepoBackfill aborted: source has {_src:,} rows vs {_tgt_keys:,} distinct native_ids "
            f"in the target ({_ratio:.1%}), below MIN_KEEP_RATIO={MIN_KEEP_RATIO:.0%}. "
            f"(Target holds {_tgt_rows:,} rows, so {_tgt_rows - _tgt_keys:,} are duplicate keys.) "
            "The difference would be destroyed -- deleted by whenNotMatchedBySourceDelete in "
            "MERGE mode, or simply not written in REBUILD mode. Investigate the parse before "
            "re-running; do not lower the ratio to get past this."
        )
    _dropped_keys = _tgt_keys - _src if _tgt_keys > _src else 0
    print(f"RepoBackfill: source {_src:,} rows vs {_tgt_keys:,} distinct target keys "
          f"({_ratio:.1%}); target holds {_tgt_rows:,} rows across those keys.")
    if REBUILD:
        # the overwrite is the only path that collapses duplicates -- see the switch comment
        print(f"  REBUILD: table becomes exactly the {_src:,} parsed rows. "
              f"{_tgt_rows - _src:,} rows go away: {_tgt_rows - _tgt_keys:,} duplicate keys "
              f"collapsed plus {_dropped_keys:,} keys filtered out.")
    else:
        # MERGE does NOT collapse duplicate keys: whenNotMatchedBySourceDelete removes only rows
        # whose native_id is ABSENT from the source, and a duplicate's key is present, so every
        # copy matches and is updated in place. They stay as CDF noise (~82M redundant update
        # events per run) -- not a correctness problem, since repo_works keys on native_id via
        # apply_changes and collapses them anyway.
        print(f"  MERGE: {_dropped_keys:,} keys are absent from the source and their rows will "
              f"be DELETED. Remaining keys are updated in place, duplicates included -- "
              f"{_tgt_rows - _tgt_keys:,} duplicate rows survive (use REBUILD to clear them).")

if DRY_RUN:
    parsed_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(DRY_RUN_TABLE)
    _new = spark.table(DRY_RUN_TABLE)
    # The MERGE's update test, per column, against one target row per key (duplicate target rows all take the source's).
    _old = spark.table(target_table).dropDuplicates(["native_id"])
    _cols = [c for c in _new.columns if c != "native_id"]
    _j = (_new.withColumn("_in_s", lit(True)).alias("s")
          .join(_old.withColumn("_in_t", lit(True)).alias("t"), "native_id", "full_outer"))
    _upd = (col("s.updated_date").isNull() | col("t.updated_date").isNull() | (col("s.updated_date") >= col("t.updated_date")))
    _d = _j.select(
        "native_id",
        col("s._in_s").isNull().alias("_no_src"), col("t._in_t").isNull().alias("_no_tgt"),
        _upd.alias("_newer"),
        array(*[when(~col(f"s.{c}").eqNullSafe(col(f"t.{c}")), lit(c)) for c in _cols]).alias("_cc"),
        col("t.license").alias("old_license"), col("s.license").alias("license"),
        col("t.is_oa").alias("old_is_oa"), col("s.is_oa").alias("is_oa"),
    ).withColumn("_changed", filter(col("_cc"), lambda x: x.isNotNull())).drop("_cc")
    _d.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(DRY_RUN_TABLE + "_diff")
    _d = spark.table(DRY_RUN_TABLE + "_diff")
    print("rows: source only (insert), target only (delete), would update, unchanged:")
    _d.select(
        count_if(col("_no_tgt") & ~col("_no_src")).alias("insert"),
        count_if(col("_no_src") & ~col("_no_tgt")).alias("delete"),
        count_if(~col("_no_src") & ~col("_no_tgt") & col("_newer") & (size("_changed") > 0)).alias("update"),
        count_if(~col("_no_src") & ~col("_no_tgt") & (size("_changed") == 0)).alias("same"),
    ).show()
    _u = _d.filter(~col("_no_src") & ~col("_no_tgt") & col("_newer") & (size("_changed") > 0))
    print("changed columns (update rows):")
    _u.select(explode("_changed").alias("c")).groupBy("c").count().orderBy(desc("count")).show(40, truncate=False)
    print("licence and is_oa transitions (update rows):")
    _u.groupBy("old_license", "license", "old_is_oa", "is_oa").count().orderBy(desc("count")).show(40, truncate=False)
    dbutils.notebook.exit(f"dry run: {DRY_RUN_TABLE}, {DRY_RUN_TABLE}_diff")

if not spark.catalog.tableExists(target_table):
    (parsed_df.write.format("delta")
        .option("mergeSchema", "true").mode("overwrite").saveAsTable(target_table))

elif REBUILD:
    print(f"REBUILD: overwriting {target_table} -- collapsing duplicate keys and applying "
          f"filters in one pass. FULL-REFRESH the Repo DLT pipeline afterwards, then set the "
          f"rebuild widget back to false.")
    (parsed_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table))

else:
    # CHANGED-ONLY MERGE: a matched row is updated only when some column differs. The updated_date
    # test alone is true for every unchanged row (>= holds on equal dates), so each run rewrote the
    # whole table into the change feed -- history v86 (09-03) and v89 (09-10): 179,509,487 of
    # 179,513,507 rows updated -- and repo_works/Union re-processed every one. A re-emitted unchanged
    # row reaches repo_works with the same content; only its _commit_version (a _sequence tiebreak) moves.
    # Side effect: this job no longer replays unchanged backfill rows through Repo.py's repo_enriched, so
    # a change there (filters, enrichment, the current_date()+30 date cut-off) reaches them only through
    # a Repo pipeline full refresh, as it already does for live rows.
    # <=> is null-safe and compares arrays/structs element by element (nested NULLs included).
    # If any source column is new to the target or has a different type (schema evolution), fall
    # back to updating every matched row, as before, so the new/changed column is filled everywhere.
    _tgt_types = {f.name.lower(): f.dataType.simpleString() for f in spark.table(target_table).schema.fields}
    _same_schema = all(_tgt_types.get(f.name.lower()) == f.dataType.simpleString() for f in parsed_df.schema.fields)
    _changed = " OR ".join(f"NOT (target.`{c}` <=> source.`{c}`)" for c in parsed_df.columns
                           if c != "native_id") if _same_schema else "true"
    print("  MERGE updates " + ("only matched rows whose content changed." if _same_schema else
          "EVERY matched row: the source schema differs from the target (new or retyped column)."))
    (DeltaTable.forName(spark, target_table).alias("target")
        .merge(parsed_df.alias("source"), "target.native_id = source.native_id")
        # NULL-safe: 3,932,463 rows carry a NULL updated_date and NULL >= NULL is NULL, which
        # MERGE treats as false -- those rows could never be refreshed (oxjob #881 REPOBACKFILL
        # CHANGES section C).
        .whenMatchedUpdateAll(
            condition="(source.updated_date IS NULL OR target.updated_date IS NULL "
                      "OR source.updated_date >= target.updated_date) AND (" + _changed + ")")
        .whenNotMatchedInsertAll()
        # the whole point: rows the parse no longer produces are removed, not stranded
        .whenNotMatchedBySourceDelete()
        .execute())
