# Databricks notebook source
# MAGIC %md
# MAGIC # HAL in xml-tei (oxjob #1588)
# MAGIC
# MAGIC All of HAL, harvested in `xml-tei` by the OAI-PMH harvester (openalex-ingest `repositories.py`, endpoint
# MAGIC `hal_tei`) into its own S3 folder `s3://openalex-ingest/hal-tei/`, NOT `repositories/`: `Repo.py` reads
# MAGIC that with a fixed oai_dc schema, and TEI rows there would collide with today's oai_dc HAL rows (same OAI
# MAGIC ids and datestamps).
# MAGIC
# MAGIC - `hal_tei_items` (bronze): one row per harvested record, raw TEI kept.
# MAGIC - `hal_tei_records`: the latest version and datestamp per HAL id.
# MAGIC - `hal_tei_authors`: one row per (HAL id, author position): idHAL, ORCID, IdRef, HAL author-form id,
# MAGIC   attached structure ids and their ROR ids.
# MAGIC
# MAGIC - `hal_tei_parsed` (oxjob #1786): per harvested record, the oai_dc record HAL would serve for it, rebuilt from the
# MAGIC   TEI, plus the TEI's abstract, volume, issue and pages. **Repo.py reads it** (`repo_parsed_hal_tei` ->
# MAGIC   `repo_enriched_hal_tei` -> a second CDC flow into `repo_works`): HAL's repo locations come from here.
# MAGIC
# MAGIC Authors, the affiliation matcher and works' author ids don't read these tables. HAL never overrides or fills
# MAGIC affiliations (Jason, 30 Sep 2026, #1467); idHAL / ORCID in author matching and HAL funding are separate
# MAGIC decisions with their own evals. HAL's OAI feed reports no deletions (`deletedRecord=no`).
# MAGIC Parser reference + checks: oxjobs `working/hal-tei-harvest/work/hal_tei_parse.py`.

# COMMAND ----------

import datetime
import gzip
import html
import io
import re
import xml.etree.ElementTree as ET

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import (ArrayType, BooleanType, IntegerType, MapType, StringType, StructField,
                               StructType)

# COMMAND ----------

# Parser: stdlib only (ElementTree is namespace-aware by URI, so the harvester's ns0/ns1 re-serialised
# prefixes don't matter; it doesn't reject the struct-N xml:ids that repeat across records in one file).
OAI = "{http://www.openarchives.org/OAI/2.0/}"
TEI = "{http://www.tei-c.org/ns/1.0}"
XML_ID = "{http://www.w3.org/XML/1998/namespace}id"
ROR_RE = re.compile(r"(?:https?://)?(?:www\.)?(?:ror\.org/)?(0[a-hj-km-np-tv-z0-9]{6}[0-9]{2})\s*$", re.I)


def _text(el):
    return (el.text or "").strip() if el is not None else None


def parse_record(rec):
    """One OAI <record> element -> dict (raw TEI kept as a string)."""
    h = rec.find(OAI + "header")
    out = {"oai_id": _text(h.find(OAI + "identifier")), "datestamp": _text(h.find(OAI + "datestamp")),
           "is_deleted": h.get("status") == "deleted", "hal_id": None, "hal_version": None, "doi": None,
           "tei": None, "authors": []}
    m = re.search(r"v(\d+)$", out["oai_id"] or "")  # oai:HAL:hal-01269578v1; the TEI has no version idno
    out["hal_version"] = int(m.group(1)) if m else None
    md = rec.find(OAI + "metadata")
    tei = md[0] if md is not None and len(md) else None
    if out["is_deleted"] or tei is None:
        return out
    out["tei"] = ET.tostring(tei, encoding="unicode")
    bf = tei.find(f".//{TEI}biblFull")
    if bf is None:
        return out
    for i in bf.findall(f"{TEI}publicationStmt/{TEI}idno"):
        if i.get("type") == "halId": out["hal_id"] = _text(i)
    for i in bf.iterfind(f".//{TEI}sourceDesc//{TEI}idno"):
        if i.get("type") == "doi":
            out["doi"] = _text(i); break
    rors = {}  # struct id -> ROR ids, from the back-matter structure list
    for org in tei.iterfind(f".//{TEI}back/{TEI}listOrg[@type='structures']/{TEI}org"):
        ids = [ROR_RE.search(_text(i) or "") for i in org.findall(TEI + "idno") if i.get("type") == "ROR"]
        rors[org.get(XML_ID)] = sorted({m.group(1).lower() for m in ids if m})
    for pos, a in enumerate(bf.findall(f"{TEI}titleStmt/{TEI}author")):
        ids = {}
        for i in a.findall(TEI + "idno"):
            key = (i.get("type") or "") + (":" + i.get("notation") if i.get("notation") else "")
            ids.setdefault(key, []).append(_text(i))
        structs = [r.get("ref", "").lstrip("#") for r in a.findall(TEI + "affiliation")]
        out["authors"].append({
            "position": pos, "role": a.get("role"),
            "forename": " ".join(_text(f) or "" for f in a.findall(f"{TEI}persName/{TEI}forename")).strip() or None,
            "surname": _text(a.find(f"{TEI}persName/{TEI}surname")),
            "idhal_string": (ids.get("idhal:string") or [None])[0],
            "idhal_numeric": (ids.get("idhal:numeric") or [None])[0],
            "halauthorid": (ids.get("halauthorid") or ids.get("halauthorid:string") or [None])[0],
            "orcid": (ids.get("ORCID") or [None])[0],
            "idref": (ids.get("IDREF") or [None])[0],
            "other_ids": {k: v for k, v in ids.items() if k not in
                          ("idhal:string", "idhal:numeric", "halauthorid", "halauthorid:string", "ORCID", "IDREF")},
            "struct_ids": structs,
            "struct_rors": sorted({r for s in structs for r in rors.get(s, [])}),
        })
    return out


def parse_file(data):
    """Harvester file bytes (gzip or not) -> list of record dicts.

    Streams record by record and frees each one: a backfill file is ~28 MB of XML, and building its whole
    tree took ~300 MB per file, which OOMed the Python workers on the first backfill update (9 Oct)."""
    stream = gzip.GzipFile(fileobj=io.BytesIO(data)) if data[:2] == b"\x1f\x8b" else io.BytesIO(data)
    out = []
    for _, el in ET.iterparse(stream, events=("end",)):
        if el.tag == OAI + "record":
            out.append(parse_record(el))
            el.clear()
    return out


# COMMAND ----------

author_type = StructType([
    StructField("position", IntegerType()),
    StructField("role", StringType()),
    StructField("forename", StringType()),
    StructField("surname", StringType()),
    StructField("idhal_string", StringType()),
    StructField("idhal_numeric", StringType()),
    StructField("halauthorid", StringType()),
    StructField("orcid", StringType()),
    StructField("idref", StringType()),
    StructField("other_ids", MapType(StringType(), ArrayType(StringType()))),
    StructField("struct_ids", ArrayType(StringType())),
    StructField("struct_rors", ArrayType(StringType())),
])
record_type = StructType([
    StructField("oai_id", StringType()),
    StructField("datestamp", StringType()),
    StructField("is_deleted", BooleanType()),
    StructField("hal_id", StringType()),
    StructField("hal_version", IntegerType()),
    StructField("doi", StringType()),
    StructField("tei", StringType()),
    StructField("authors", ArrayType(author_type)),
    StructField("parse_error", StringType()),
])


def parse_file_safe(data):
    """A file that fails to parse becomes one error row (counted by the expectation below), not a failed update."""
    try:
        return parse_file(data)
    except Exception as e:  # noqa: BLE001 -- surfaced as a row, never swallowed
        return [{"is_deleted": False, "authors": [], "parse_error": f"{type(e).__name__}: {e}"[:1000]}]


parse_file_udf = F.udf(parse_file_safe, ArrayType(record_type))

# COMMAND ----------

@dlt.table(
    name="hal_tei_items",
    table_properties={"quality": "bronze"},
    comment="HAL xml-tei records as harvested (oxjob #1588), one row per record per harvested file",
)
@dlt.expect("file_parsed", "parse_error IS NULL")
def hal_tei_items():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.xml.gz")
        .option("cloudFiles.schemaLocation", "dbfs:/pipelines/hal_tei/schema")
        # Discovery via UC managed file events on the openalex-ingest external location, as Repo.py / IRDB.py
        .option("cloudFiles.useManagedFileEvents", "true")
        # Backfill files hold 1,000 records (~28 MB of XML); keep micro-batches small
        .option("cloudFiles.maxFilesPerTrigger", "500")
        .load("s3a://openalex-ingest/hal-tei/")
        .select(
            F.explode(parse_file_udf(F.col("content"))).alias("r"),
            F.col("path").alias("file_path"),
            # S3 object mtime = when the harvester delivered the file (oxjob #911 semantics)
            F.col("modificationTime").alias("ingested_at"),
        )
        .select("r.*", "file_path", "ingested_at")
        .withColumn("datestamp", F.to_date("datestamp"))
    )

# COMMAND ----------

dlt.create_streaming_table(
    name="hal_tei_records",
    comment="Latest HAL xml-tei record per HAL id (highest version, then latest datestamp, then latest file)",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
    cluster_by=["hal_id"],
)

@dlt.view(name="hal_tei_items_keyed")
def hal_tei_items_keyed():
    return (
        dlt.read_stream("hal_tei_items")
        .filter(F.col("hal_id").isNotNull() & ~F.col("is_deleted"))
        .withColumn("_sequence", F.struct("hal_version", "datestamp", "ingested_at", "file_path"))
    )

dlt.apply_changes(
    target="hal_tei_records",
    source="hal_tei_items_keyed",
    keys=["hal_id"],
    sequence_by="_sequence",
    except_column_list=["_sequence"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `hal_tei_parsed` (oxjob #1786): HAL's oai_dc record, rebuilt from the TEI
# MAGIC
# MAGIC Repo.py derives ids, type, version, licence, dates and URLs from `dc:*` with rules tuned over years. Handing it the
# MAGIC `dc:*` struct HAL itself would serve keeps all of those identical by construction (checked on 1,806 records with the
# MAGIC same datestamp in both formats: authors, dates, version, language 100%; type, source 99.9%). What changes, on
# MAGIC purpose: the title (the document language's, not the last one Spark kept), the abstract (the real one, never HAL's
# MAGIC 'International audience' flag or a comment, #1635), volume/issue/pages, and the licence order (a free file under
# MAGIC 'HAL Authorization' is other-oa). The block between the markers is copied verbatim from oxjobs
# MAGIC `working/hal-works-from-tei/scratch/hal_tei_dc.py`, where it is tested; edit both.

# COMMAND ----------

# --- hal_tei_dc: begin (verbatim from oxjob #1786 scratch/hal_tei_dc.py) ---
TEI_NS = "{http://www.tei-c.org/ns/1.0}"
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"

# HAL typology code -> the eu-repo type HAL's oai_dc pairs with it (repo_items, 20-23 Oct 2026, every code seen).
HAL_EUREPO_TYPE = {
    "ART": "article", "COMM": "conferenceObject", "POSTER": "conferenceObject", "PRESCONF": "conferenceObject",
    "COUV": "bookPart", "OUV": "book", "THESE": "doctoralThesis", "MEM": "masterThesis", "REPORT": "report",
    "UNDEFINED": "preprint", "PATENT": "patent", "LECTURE": "lecture",
}  # every other code (OTHER, IMG, ISSUE, BLOG, NOTICE, PROCEEDINGS, HDR, VIDEO, TRAD, SOFTWARE, SON, MAP, CREPORT) -> other
AUDIENCE_LABEL = {"1": "National audience", "2": "International audience", "3": "No audience"}
# sourceDesc/biblStruct idno types -> the dc:identifier prefix HAL writes ("DOI: 10.1016/...")
MONOGR_ID_PREFIX = {"isbn": "ISBN", "nnt": "NNT", "patentNumber": "Patent N°", "reportNumber": "Report N°"}
SKIP_SOURCE_IDS = {"hdl"}  # HAL puts handles in dc:relation only


def _t(el):
    return "".join(el.itertext()).strip() if el is not None else None


def _clean(s):
    """HAL's TEI escapes markup twice ('κ &amp;gt; 0', 'du&lt;br /&gt;consentement') and keeps no-break spaces;
    its oai_dc serves the plain text. Unescape, turn <br> into a space, collapse whitespace."""
    if not s:
        return s
    s = re.sub(r"<br\s*/?>", " ", html.unescape(s), flags=re.I)
    return re.sub(r"[\s  ]+", " ", s).strip() or None


def _first(xs):
    return xs[0] if xs else None


def _pages(pp):
    """'60-70' -> ('60', '70'); '25' -> ('25', None); 'pp. 1 - 8' -> ('1', '8')."""
    if not pp:
        return None, None
    s = re.sub(r"^\s*(pp?\.?)\s*", "", pp.strip(), flags=re.I)
    m = re.match(r"^([^\s\-–—]+)\s*[\-–—]+\s*([^\s\-–—]+)$", s)
    if m:
        return m.group(1)[:100], m.group(2)[:100]
    return (s[:100] or None), None


def tei_to_dc(tei_xml):
    """One HAL TEI document (the <TEI> element as a string) -> dict; None when it has no biblFull."""
    bf = ET.fromstring(tei_xml).find(f".//{TEI_NS}biblFull")
    if bf is None:
        return None
    lang = _first([x.get("ident") for x in bf.findall(f"{TEI_NS}profileDesc/{TEI_NS}langUsage/{TEI_NS}language")])

    # title: main title in the document language, else the first; subtitle likewise
    titles = [(t.get(XML_LANG), t.get("type"), _clean(_t(t))) for t in bf.findall(f"{TEI_NS}titleStmt/{TEI_NS}title")]
    mains = [t for t in titles if t[1] != "sub" and t[2]]
    subs = [t for t in titles if t[1] == "sub" and t[2]]
    main = next((t for t in mains if t[0] == lang), _first(mains))
    sub = next((t for t in subs if main and t[0] == main[0]), _first(subs))
    title = (main[2] if main else None)
    if sub:
        title = f"{title}: {sub[2]}" if title else sub[2]

    # authors as HAL's dc:creator writes them: "Surname, Forename[, Middle]", every role, collective names as is
    creators = []
    for a in bf.findall(f"{TEI_NS}titleStmt/{TEI_NS}author"):
        sn = _t(a.find(f"{TEI_NS}persName/{TEI_NS}surname"))
        fns = [x for x in (_t(f) for f in a.findall(f"{TEI_NS}persName/{TEI_NS}forename")) if x]
        if sn:
            creators.append(", ".join([sn] + fns) if fns else sn + ",")  # HAL writes "Damien," when there is no forename
        elif fns:
            creators.append(", ".join(fns))
        else:
            org = _t(a.find(f"{TEI_NS}orgName"))
            if org:
                creators.append(org)

    # abstracts, notes
    abstracts = [(a.get(XML_LANG), html.unescape("\n".join(_t(p) for p in a.findall(f"{TEI_NS}p") if _t(p)) or _t(a) or "").strip())
                 for a in bf.findall(f"{TEI_NS}profileDesc/{TEI_NS}abstract")]
    abstracts = [a for a in abstracts if a[1]]
    best_abs = next((a for a in abstracts if a[0] == lang), _first(abstracts))
    notes = [(n.get("type"), n.get("n"), _t(n)) for n in bf.findall(f"{TEI_NS}notesStmt/{TEI_NS}note")]
    audience = _first([n[1] for n in notes if n[0] == "audience"])
    description = [n[2] for n in notes if n[0] in ("commentary", "description") and n[2]]
    description += [AUDIENCE_LABEL.get(n[1], n[2]) for n in notes if n[0] == "audience" and (n[1] in AUDIENCE_LABEL or n[2])]
    description += [a[1] for a in abstracts]

    # type
    codes = {c.get("scheme"): (c.get("n"), _t(c)) for c in bf.findall(f"{TEI_NS}profileDesc/{TEI_NS}textClass/{TEI_NS}classCode")}
    code, label = codes.get("halTypology", (None, None))
    dc_type = [f"info:eu-repo/semantics/{HAL_EUREPO_TYPE.get(code, 'other')}"] + ([label] if label else [])

    # subjects: author keywords, then "[CODE]Domain label" (feeds only the version regex, as in oai_dc)
    subject = [_t(k) for k in bf.findall(f"{TEI_NS}profileDesc/{TEI_NS}textClass/{TEI_NS}keywords/{TEI_NS}term") if _t(k)]
    subject += [f"[{(c.get('n') or '').upper()}]{_t(c)}" for c in bf.findall(f"{TEI_NS}profileDesc/{TEI_NS}textClass/{TEI_NS}classCode")
                if c.get("scheme") == "halDomain"]

    # identifiers in HAL's oai_dc order: monogr ids, halId, halUri, file URLs, then the record's own ids
    pub = {}
    for i in bf.findall(f"{TEI_NS}publicationStmt/{TEI_NS}idno"):
        pub.setdefault(i.get("type"), _t(i))
    edition = bf.find(f"{TEI_NS}editionStmt/{TEI_NS}edition[@type='current']")
    if edition is None:
        edition = bf.find(f"{TEI_NS}editionStmt/{TEI_NS}edition")
    files = [r.get("target") for r in (edition.findall(f"{TEI_NS}ref") if edition is not None else [])
             if r.get("type") == "file" and r.get("target")]
    bs = bf.find(f"{TEI_NS}sourceDesc/{TEI_NS}biblStruct")
    monogr = bs.find(f"{TEI_NS}monogr") if bs is not None else None
    identifier = []
    for i in (monogr.findall(f"{TEI_NS}idno") if monogr is not None else []):
        if i.get("type") in MONOGR_ID_PREFIX and _t(i):
            identifier.append(f"{MONOGR_ID_PREFIX[i.get('type')]}: {_t(i)}")
    identifier += [x for x in (pub.get("halId"), pub.get("halUri")) if x] + files
    relation = []
    for i in (bs.findall(f"{TEI_NS}idno") if bs is not None else []):
        typ, val = i.get("type") or "", _t(i)
        if not val:
            continue
        if typ.lower() == "doi":
            val = val.lower()  # HAL's oai_dc lowercases DOIs; the TEI keeps the depositor's case
        if typ.lower() in ("doi", "pubmed", "arxiv"):
            kind = "pmid" if typ.lower() == "pubmed" else typ.lower()
            relation.append(f"info:eu-repo/semantics/altIdentifier/{kind}/{val}")
        if typ in SKIP_SOURCE_IDS:
            relation.append(f"info:eu-repo/semantics/altIdentifier/hdl/{val}")
            continue
        identifier.append(f"{typ.upper()}: {val}")

    # rights, ordered so Repo.py's pick (first creativecommons.org value, else the first value) means: the CC licence
    # if there is one, else open access when a file is out of embargo, else the licence URL (normalises to nothing).
    # HAL's oai_dc lists the licence URL first, so "HAL Authorization" + a free PDF came out as no licence and not OA
    # on records harvested after HAL reordered dc:rights, and as other-oa before: a coin flip on harvest date.
    lic = [l.get("target") for l in bf.findall(f"{TEI_NS}publicationStmt/{TEI_NS}availability/{TEI_NS}licence") if l.get("target")]
    today = datetime.date.today().isoformat()
    embargo_ends = [d.get("notBefore") for r in (edition.findall(f"{TEI_NS}ref") if edition is not None else [])
                    if r.get("type") == "file" for d in r.findall(f"{TEI_NS}date") if d.get("notBefore")]
    file_open = bool(files) and all(e[:10] <= today for e in embargo_ends)
    rights = [x for x in lic if "creativecommons.org" in x]
    rights += ["info:eu-repo/semantics/OpenAccess"] if file_open else []
    rights += [x for x in lic if "creativecommons.org" not in x]

    # source (Spark keeps the last dc:source: HAL's citation string, or a publisher-link ref after it)
    src = [pub.get("halRef")] + [_t(r) for r in (bs.findall(f"{TEI_NS}ref") if bs is not None else []) if r.get("type") == "publisher" and _t(r)]
    src = [s for s in src if s]
    imprint = monogr.find(f"{TEI_NS}imprint") if monogr is not None else None
    publishers = [_t(p) for p in (imprint.findall(f"{TEI_NS}publisher") if imprint is not None else []) if _t(p)]
    scope = {}
    for b in (imprint.findall(f"{TEI_NS}biblScope") if imprint is not None else []):
        scope.setdefault(b.get("unit"), _t(b))
    first_page, last_page = _pages(scope.get("pp"))
    produced = _first([d.text.strip() for d in (edition.findall(f"{TEI_NS}date") if edition is not None else [])
                       if d.get("type") == "whenProduced" and d.text])

    set_spec = [f"type:{code}"] if code else []
    set_spec += ["collection:" + s.get("n") for s in bf.findall(f"{TEI_NS}seriesStmt/{TEI_NS}idno")
                 if s.get("type") == "stamp" and s.get("n")]

    return {
        "dc": {
            "dc:title": title,
            "dc:creator": creators,
            "dc:contributor": [],
            "dc:subject": subject,
            "dc:description": description,
            "dc:source": src[-1] if src else None,
            "dc:date": [produced] if produced else [],
            "dc:type": dc_type,
            "dc:identifier": identifier,
            "dc:language": lang,
            "dc:format": None,
            "dc:publisher": publishers[-1] if publishers else "CCSD",
            "dc:rights": rights or None,
            "ns4:accessRights": None,
            "ns4:license": None,
            "dc:relation": relation or None,
        },
        "set_spec": set_spec,
        "abstract": best_abs[1] if best_abs else None,
        "abstract_lang": best_abs[0] if best_abs else None,
        "abstracts": [{"lang": a[0], "text": a[1]} for a in abstracts],
        "audience": audience,
        "volume": (scope.get("volume") or None) and scope["volume"][:100],
        "issue": (scope.get("issue") or None) and scope["issue"][:100],
        "first_page": first_page,
        "last_page": last_page,
    }
# --- hal_tei_dc: end ---


_dc_struct = StructType([StructField(name, typ) for name, typ in [
    ("dc:title", StringType()), ("dc:creator", ArrayType(StringType())), ("dc:contributor", ArrayType(StringType())),
    ("dc:subject", ArrayType(StringType())), ("dc:description", ArrayType(StringType())), ("dc:source", StringType()),
    ("dc:date", ArrayType(StringType())), ("dc:type", ArrayType(StringType())), ("dc:identifier", ArrayType(StringType())),
    ("dc:language", StringType()), ("dc:format", ArrayType(StringType())), ("dc:publisher", StringType()),
    ("dc:rights", ArrayType(StringType())), ("ns4:accessRights", ArrayType(StringType())),
    ("ns4:license", ArrayType(StringType())), ("dc:relation", ArrayType(StringType())),
]])  # same names and types as Repo.py's repository_schema `ns1:dc`
_tei_dc_type = StructType([
    StructField("dc", _dc_struct),
    StructField("set_spec", ArrayType(StringType())),
    StructField("abstract", StringType()),
    StructField("abstract_lang", StringType()),
    StructField("abstracts", ArrayType(StructType([StructField("lang", StringType()), StructField("text", StringType())]))),
    StructField("audience", StringType()),
    StructField("volume", StringType()),
    StructField("issue", StringType()),
    StructField("first_page", StringType()),
    StructField("last_page", StringType()),
    StructField("error", StringType()),
])


def tei_to_dc_safe(tei_xml):
    """A record that fails to parse becomes an error row (counted by the expectation), never a failed update."""
    if not tei_xml:
        return None
    try:
        return tei_to_dc(tei_xml)
    except Exception as e:  # noqa: BLE001 -- surfaced as a row, never swallowed
        return {"error": f"{type(e).__name__}: {e}"[:1000]}


tei_to_dc_udf = F.udf(tei_to_dc_safe, _tei_dc_type)

# endpoint_id of each HAL record = the oai_dc endpoint that delivered it last (frozen at the cutover), so the record's
# source (INRIA, HAL AMU, ... or HAL) and every location hash stay put. endpoint_id sits in the location and work hashes
# and keys same-URL dedup. New records: their earlier version's endpoint, else the portal of the three whose oai_dc
# feed is exactly one collection, else hal_tei (bound to the HAL source).
HAL_ENDPOINT_MAP_TABLE = "openalex.repo.hal_oai_dc_endpoints"
HAL_TEI_ENDPOINT_ID = "hal_tei"
PORTAL_COLLECTION_ENDPOINTS = [
    ("collection:UNIV-AMU", "b9bb6f966cfb6627303"),    # https://api.archives-ouvertes.fr/oai/amu/ -> HAL AMU
    ("collection:CNAM", "pueedsaajjaysri6bbvs"),       # /oai/cnam/ -> HAL - CNAM
    ("collection:SCIENCESPO", "2146a6a382bebe0e6f4"),  # /oai/sciencespo -> SPIRE (Sciences Po)
]


def build_hal_tei_parsed(items, endpoint_map):
    """hal_tei_items rows (stream or batch) + the endpoint map -> hal_tei_parsed rows, in repo_items' oai_dc shape."""
    by_native = endpoint_map.select(F.col("native_id").alias("oai_id"), F.col("endpoint_id").alias("_ep_native"))
    by_hal = endpoint_map.groupBy("hal_id").agg(F.max_by("endpoint_id", "hal_version").alias("_ep_hal"))
    portal = None
    for coll, ep in PORTAL_COLLECTION_ENDPOINTS:
        cond = F.array_contains(F.col("p.set_spec"), coll)
        portal = F.when(cond, F.lit(ep)) if portal is None else portal.when(cond, F.lit(ep))
    return (
        items
        .filter(F.col("hal_id").isNotNull() & ~F.col("is_deleted") & F.col("tei").isNotNull())
        .withColumn("p", tei_to_dc_udf(F.col("tei")))
        .join(by_native, "oai_id", "left")
        .join(by_hal, "hal_id", "left")
        .select(
            F.struct(
                F.col("oai_id").alias("ns0:identifier"),
                F.col("datestamp").cast("timestamp").alias("ns0:datestamp"),
                F.col("p.set_spec").alias("ns0:setSpec"),
            ).alias("ns0:header"),
            F.when(F.col("p.dc").isNotNull(), F.struct(F.col("p.dc").alias("ns1:dc"))).alias("ns0:metadata"),
            F.coalesce("_ep_native", "_ep_hal", portal, F.lit(HAL_TEI_ENDPOINT_ID)).alias("repository_id"),
            "ingested_at",
            "oai_id",
            "hal_id",
            "hal_version",
            F.col("p.abstract").alias("tei_abstract"),
            F.col("p.abstract_lang").alias("tei_abstract_lang"),
            F.col("p.abstracts").alias("tei_abstracts"),
            F.col("p.audience").alias("tei_audience"),
            F.col("p.volume").alias("tei_volume"),
            F.col("p.issue").alias("tei_issue"),
            F.col("p.first_page").alias("tei_first_page"),
            F.col("p.last_page").alias("tei_last_page"),
            F.col("p.error").alias("tei_error"),
            "file_path",
        )
    )


@dlt.table(
    name="hal_tei_parsed",
    comment="HAL records as the oai_dc HAL would serve, rebuilt from TEI, + abstract/biblio (oxjob #1786); read by Repo.py",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
@dlt.expect("tei_parsed", "tei_error IS NULL")
def hal_tei_parsed():
    return build_hal_tei_parsed(dlt.read_stream("hal_tei_items"), spark.read.table(HAL_ENDPOINT_MAP_TABLE))

# COMMAND ----------

@dlt.table(
    name="hal_tei_authors",
    comment="One row per (HAL id, author position) from hal_tei_records: HAL person ids (idHAL), ORCID, IdRef, attached structures",
)
def hal_tei_authors():
    return (
        dlt.read("hal_tei_records")
        .select("hal_id", "hal_version", "oai_id", "datestamp", "doi", F.explode("authors").alias("a"))
        .select("hal_id", "hal_version", "oai_id", "datestamp", "doi", "a.*")
    )
