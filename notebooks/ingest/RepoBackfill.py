# Databricks notebook source
import re
import unicodedata
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
def clean_html(raw_html):
    cleanr = re.compile(r'<\w+.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext

def remove_everything_but_alphas(input_string):
    if input_string:
        return "".join(e for e in input_string if e.isalpha())
    return ""

def remove_accents(text):
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')

def normalize_title(title):
    if not title:
        return ""

    if isinstance(title, bytes):
        title = str(title, 'ascii')

    text = title[0:500]

    text = text.lower()

    # handle unicode characters
    text = remove_accents(text)

    # remove HTML tags
    text = clean_html(text)

    # remove articles and common prepositions
    text = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", text)

    # remove everything except alphabetic characters
    text = remove_everything_but_alphas(text)

    return text.strip()

# Types to filter out (DELETE from CSV mapping) - lowercase normalized
TYPES_TO_DELETE = [
    "person", "image", "newspaper", "info:eu-repo/semantics/lecture", "photograph",
    "bildband", "dvd-video", "video", "fotografia", "cd", "sound recording",
    "text and image", "moving image", "photographs", "cd-rom",
    "blu-ray-disc", "stillimage", "image; text", "image;stillimage", "still image",
    "image;", "ilustraciones y fotos", "fotografie", "fotografía"
]

# Comprehensive type mappings - combines original logic + CSV mappings (all lowercase keys)
REPO_TYPE_MAPPING = {
    # Articles and journal content
    "article": "article",
    "journal article": "article",
    "journal articles": "article",
    "publishedversion": "article",
    "contributiontoperiodical": "article",
    "conference papers": "article",
    "conferencepaper": "article",
    "conferenceobject": "article",
    "conferenceposter": "article",
    "conferenceproceedings": "article",
    "conferencecontribution": "article",
    "conferenceitem": "article",
    "text": "article",
    "manuscript": "article",
    "peerreviewed": "article",  # CSV
    "peer reviewed": "article",  # CSV
    "submittedversion": "article",  # CSV (extracted from URI)
    "acceptedversion": "article",  # CSV
    "review": "article",  # CSV (extracted from URI)
    "zeitschrift": "article",  # CSV
    "conference paper": "article",  # CSV
    "konferenzschrift": "article",  # CSV
    "article / letter to editor": "article",  # CSV
    "inproceedings": "article",  # CSV
    "text (article)": "article",  # CSV
    "info:ulb-repo/semantics/openurl/article": "article",  # CSV (different prefix)
    "vor": "article",  # CSV
    "journal contribution": "article",  # CSV
    "artigo de jornal": "article",  # CSV
    "doc-type:article": "article",  # CSV
    "letter": "article",  # CSV
    
    # Books and book content
    "book": "book",
    "books": "book",
    "book sections": "book-chapter",
    "bookpart": "book-chapter",
    "monografische reihe": "book",  # CSV
    "volume": "book",  # CSV
    "monograph": "book",  # CSV
    "book article": "book-chapter",  # CSV
    "book part": "book-chapter",  # CSV
    "chapter, part of book": "book-chapter",  # CSV
    "part of book or chapter of book": "book-chapter",  # CSV
    "libros": "book",  # CSV
    "doc-type:bookpart": "book-chapter",  # CSV
    
    # Theses and dissertations
    "thesis": "dissertation",
    "theses": "dissertation",
    "masterthesis": "dissertation",
    "bachelorthesis": "dissertation",
    "doctoralthesis": "dissertation",
    "doctor of philosophy": "dissertation",
    "phd": "dissertation",
    "dissertation": "dissertation",
    "undergraduate senior honors thesis": "dissertation",
    "hochschulschrift": "dissertation",  # CSV
    "phdthesis": "dissertation",  # CSV
    "master thesis": "dissertation",  # CSV
    "diplomová práce": "dissertation",  # CSV
    "doctoral thesis": "dissertation",  # CSV
    "doc-type:doctoralthesis": "dissertation",  # CSV
    "masters paper": "dissertation",  # CSV
    "masters thesis": "dissertation",  # CSV
    "dissertação": "dissertation",  # CSV
    "doctoral_dissertation": "dissertation",  # CSV
    "tesi doctoral": "dissertation",  # CSV
    "thesis or dissertation": "dissertation",  # CSV
    "thèse": "dissertation",  # CSV
    "electronic dissertation": "dissertation",  # CSV
    "dissertation-reproduction (electronic)": "dissertation",  # CSV
    "thesis-reproduction (electronic)": "dissertation",  # CSV
    
    # Reports
    "report": "report",
    "reports": "report",
    "technical report": "report",
    "workingpaper": "report",
    "working paper": "report",
    "reportpart": "report",
    "technical documentation": "report",
    
    # Reviews
    "review": "review",
    "peer-review": "peer-review",
    
    # Preprints
    "preprint": "preprint",
    "preprints, working papers, ...": "preprint",
    
    # Datasets
    "dataset": "dataset",
    "dataset/mass spectrometry": "dataset",  # CSV
    
    # Awards
    "award/grant": "award",  # CSV
    
    # Software
    "software": "software",  # CSV
    
    # Images (mapped to other)
    "image": "other",
    "chemical structures": "other",
    
    # Other types
    "lecture": "other",
    "patent": "other",
    "creative project": "other",
    "other": "other",
    "null": "other",
}

def get_openalex_type_from_repo(input_type):
    """
    Convert various publication type formats to OpenAlex types.
    """
    if not input_type or not isinstance(input_type, str):
        return "other"

    input_type = input_type.strip()
    input_type_lower = input_type.lower()
    
    # Handle URI formats first
    if "info:eu-repo/semantics/" in input_type_lower:
        input_type_lower = input_type_lower.split("info:eu-repo/semantics/")[-1]
    elif "purl.org/coar/resource_type/" in input_type_lower:
        coar_to_type = {
            "c_93fc": "article",    # contribution to journal
            "c_6501": "article",    # journal article
            "c_bdcc": "article",    # research article
            "c_db06": "dissertation",    # dissertation (corrected from CSV)
            "r60j-j5bd": "article", # article
            "c_5794": "article",    # journal article
            "c_2f33": "article",    # article
        }
        coar_id = input_type_lower.split("/")[-1]
        return coar_to_type.get(coar_id, "other")
    elif "purl.org/coar/version/" in input_type_lower:
        return "article"
    
    # Check comprehensive mapping dictionary
    return REPO_TYPE_MAPPING.get(input_type_lower, "other")

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

id_struct_type = StructType([
    StructField("id", StringType(), True),
    StructField("namespace", StringType(), True),
    StructField("relationship", StringType(), True)
])

def extract_ids(identifiers, native_id):
   try:
       if identifiers is None:
           return []
       if not isinstance(identifiers, list):
           identifiers = [identifiers]
       if native_id is None:
           native_id = ""
           
       patterns = {
           'arxiv': (r"https?://arxiv\.org/abs/([0-9]{4}\.[0-9]{4,5}|[a-z\-]+/\d+)", 1),
           'arxiv_native': (r"oai:arXiv\.org:([^/\s]+/\d+|\d+\.\d+)", 1),
           'doi': (r"\b10\.\d{4,9}/\S+\b", 0),
           'issn': (r"\b\d{4}-\d{3}[0-9X]\b", 0),
           'hal': (r"\bhal-\d+\b", 0),
           'handle': (r"https?://hdl\.handle\.net/([^/\s]+/[^/\s]+)", 1)
       }
       
       results = []
       arxiv_id_from_native = None
       
       # extract arxiv ID from native_id and normalize it
       try:
           if isinstance(native_id, str):
               match = re.search(patterns['arxiv_native'][0], native_id)
               if match:
                   arxiv_id_from_native = match.group(1)
       except Exception:
           pass
       
       # process each identifier
       for identifier in identifiers:
           if not identifier or not isinstance(identifier, str):
               continue
               
           try:
               for namespace, (pattern, group) in patterns.items():
                   match = re.search(pattern, identifier)
                   if match:
                       try:
                           relationship = None
                           
                           if namespace.startswith('arxiv'):
                               id_value = "arXiv:" + match.group(group)  # prepend arXiv:
                               
                               # check if this is an arxiv ID and compare with native_id
                               if arxiv_id_from_native:
                                   if id_value == f"arXiv:{arxiv_id_from_native}" or f"oai:arXiv.org:{match.group(group)}" == native_id:
                                       relationship = 'self'
                           else:
                               id_value = match.group(group)
                           
                           results.append({
                               "id": id_value,
                               "namespace": namespace.split('_')[0],
                               "relationship": relationship
                           })
                           break
                       except Exception:
                           continue
           except Exception:
               continue
       
       # add native_id
       if native_id:
           results.append({
               "id": native_id,
               "namespace": "pmh",
               "relationship": "self"
           })
       
       # deduplicate
       seen = set()
       unique_results = []
       for r in results:
           try:
               key = (r['id'], r['namespace'], r['relationship'])
               if key not in seen:
                   seen.add(key)
                   unique_results.append(r)
           except Exception:
               continue
       
       return unique_results
       
   except Exception as e:
       print(f"Error in extract_ids: {str(e)}")
       return []
   
def normalize_license(text):
    if not text:
        return None

    normalized_text = text.replace(" ", "").replace("-", "").lower()

    license_lookups = [
        # open Access patterns
        ("infoeureposematicsaccess", "other-oa"),
        ("openaccess", "other-oa"),
        
        # publisher-specific
        ("elsevier.com/openaccess/userlicense", "publisher-specific-oa"),
        ("pubs.acs.org/page/policy/authorchoice_termsofuse.html", "publisher-specific-oa"),
        ("arxiv.orgperpetual", "publisher-specific-oa"),
        ("arxiv.orgnonexclusive", "publisher-specific-oa"),
        
        # creative Commons licenses
        ("ccbyncnd", "cc-by-nc-nd"),
        ("ccbyncsa", "cc-by-nc-sa"),
        ("ccbynd", "cc-by-nd"),
        ("ccbysa", "cc-by-sa"),
        ("ccbync", "cc-by-nc"),
        ("ccby", "cc-by"),
        ("creativecommons.org/licenses/by/", "cc-by"),
        
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

def has_oa_domain(native_id):
    oa_domains = ["arxiv", "osti", "pubmedcentral", "biorxiv", "medrxiv", "zenodo", "figshare"]
    if native_id is None:
        return False
    
    parts = native_id.lower().split(":")
    if len(parts) >= 2:
        domain_part = parts[1]
        for domain in oa_domains:
            if domain in domain_part:
                return True
    return False

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

extract_ids_udf = udf(extract_ids, ArrayType(id_struct_type))

@pandas_udf(StringType())
def normalize_license_udf(license_series: pd.Series) -> pd.Series:
    return license_series.apply(normalize_license)

@pandas_udf(StringType())
def normalize_title_udf(title_series: pd.Series) -> pd.Series:
    return title_series.apply(normalize_title)

@pandas_udf(StringType())
def get_openalex_type_from_repo_udf(repo_type_series: pd.Series) -> pd.Series:
    return repo_type_series.apply(get_openalex_type_from_repo)

@pandas_udf(StringType())
def normalize_language_code_udf(language_code_series: pd.Series) -> pd.Series:
    return language_code_series.apply(normalize_language_code)

@pandas_udf(BooleanType())
def has_oa_domain_udf(url_series: pd.Series) -> pd.Series:
    return url_series.apply(has_oa_domain)

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

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

parsed_df = clean_df \
    .withColumn("native_id", regexp_extract(col("cleaned_xml"), r"<identifier>(.*?)</identifier>", 1)) \
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
    .withColumn("raw_native_type", regexp_extract(col("cleaned_xml"), r"<dc:type.*?>(.*?)</dc:type>", 1)) \
    .withColumn("type", get_openalex_type_from_repo_udf(col("raw_native_type"))) \
    .filter(~lower(col("raw_native_type")).isin(TYPES_TO_DELETE)) \
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
    .withColumn("version", detect_version_udf(col("cleaned_xml"), col("native_id"))) \
    .withColumn("language", normalize_language_code_udf(regexp_extract(col("cleaned_xml"), r"<dc:language.*?>(.*?)</dc:language>", 1))) \
    .withColumn("published_date_raw", 
        regexp_extract(col("cleaned_xml"), r"<dc:date.*?>(.*?)</dc:date>", 1)) \
    .withColumn("published_date",
        coalesce(
            # Try original formats
            to_date(to_timestamp(col("published_date_raw"), "yyyy-MM-dd'T'HH:mm:ss'Z'")),
            to_date(to_timestamp(col("published_date_raw"), "yyyy-MM-dd'T'HH:mm:ss")),
            to_date(col("published_date_raw"), "yyyy-MM-dd"),
            to_date(col("published_date_raw"), "yyyy-MM"),
            # Add period-separated format
            to_date(regexp_replace(col("published_date_raw"), r'\.', '-'), "yyyy-MM-dd"),
            # Year only
            to_date(
                when(length(trim(col("published_date_raw"))) == 4,
                    concat(col("published_date_raw"), lit("-01-01"))),
                "yyyy-MM-dd"
            )
        )) \
    .withColumn("created_date", col("published_date")) \
    .withColumn("updated_date", to_date(
        regexp_extract(col("cleaned_xml"), r"<datestamp>(.*?)</datestamp>", 1))) \
    .withColumn("abstract_raw", 
        element_at(expr("regexp_extract_all(cleaned_xml, '<dc:description>(.*?)</dc:description>')"), 1)) \
    .withColumn("abstract",
        when(length(col("abstract_raw")) >= MIN_ABSTRACT_LENGTH, 
            substring(col("abstract_raw"), 0, MAX_ABSTRACT_LENGTH))
        .otherwise(lit(None))) \
    .withColumn("source_name", 
        regexp_extract(col("cleaned_xml"), r"<dc:source.*?>(.*?)</dc:source>", 1)) \
    .withColumn("publisher", 
        regexp_extract(col("cleaned_xml"), r"<dc:publisher.*?>(.*?)</dc:publisher>", 1)) \
    .withColumn("urls",
        expr("""
            transform(
                regexp_extract_all(cleaned_xml, '<dc:identifier>(http.*?)</dc:identifier>'),
                x -> struct(
                    x as url,
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
    .withColumn("license", normalize_license_udf(col("raw_license"))) \
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
    .withColumn("references", array(
        struct(
            lit(None).cast("string").alias("doi"),
            lit(None).cast("string").alias("pmid"),
            lit(None).cast("string").alias("arxiv"),
            lit(None).cast("string").alias("title"),
            lit(None).cast("string").alias("authors"),
            lit(None).cast("string").alias("year"),
            lit(None).cast("string").alias("raw")
        )
    )) \
    .withColumn("mesh", lit(None).cast("string")) \
    .withColumn(
    "is_oa",
        when(
            lower(col("license")).startswith("cc") | 
            lower(col("license")).contains("other-oa") |
            lower(col("license")).contains("public-domain") |
            has_oa_domain_udf(col("native_id")),
            lit(True)
        ).otherwise(lit(False))
    )

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
    "is_oa"
)

# deduplicate based on native_id and most recent updated date
parsed_df = parsed_df.sort(col("updated_date").desc()).dropDuplicates(["native_id"])

parsed_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("openalex.repo.repo_works_backfill")
