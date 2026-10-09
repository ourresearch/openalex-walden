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
# MAGIC **Feeds nothing yet.** Works, authors and the affiliation matcher don't read these tables. HAL never
# MAGIC overrides or fills affiliations (Jason, 30 Sep 2026, #1467); using idHAL / ORCID in author matching is a
# MAGIC separate decision with its own eval. HAL's OAI feed reports no deletions (`deletedRecord=no`).
# MAGIC Parser reference + checks: oxjobs `working/hal-tei-harvest/work/hal_tei_parse.py`.

# COMMAND ----------

import gzip
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
