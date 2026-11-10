"""Stored PDF / GROBID XML files per work location, and which one is each work's primary (oxjob #1951, #1958).

One row per location in `locations_mapped` that has a file. Used by the partner manifest
(notebooks/elastic/export_content_manifest_to_r2) and, as one pair per work, by the content API's D1
index (notebooks/elastic/sync_content_index_to_d1), so the manifest's `is_primary` row is the PDF
content.openalex.org serves.

Primary: a location with a PDF over an XML-only one, then publishedVersion > acceptedVersion >
submittedVersion, then a PDF that has its XML, then the lowest UUID as the last tiebreak. In the
manifest the XML always comes from the same location as the PDF. The content API serves the primary
row's XML, else the XML of the work's best row that has one: 37,206 works (2026-11-09) had their only
XML on a non-primary row. Locations whose PDF failed the byte check
(`openalex.pdf.manifest_excluded_pdfs`) are left out.
"""

LOCATIONS_MAPPED = "openalex.works.locations_mapped"

VERSION_RANK = (
    "CASE version WHEN 'publishedVersion' THEN 1 WHEN 'acceptedVersion' THEN 2 "
    "WHEN 'submittedVersion' THEN 3 ELSE 4 END"
)
PRIMARY_ORDER = (
    f"pdf_s3_id IS NULL, {VERSION_RANK}, grobid_s3_id IS NULL, "
    "pdf_s3_id, grobid_s3_id, native_id_namespace, native_id"
)


def content_files_sql(extra_where: str = "", table: str = LOCATIONS_MAPPED) -> str:
    """SELECT over `table` (locations_mapped, or a VERSION AS OF of it); `extra_where` must start with AND
    and keep whole works."""
    return f"""
        SELECT
          work_id,
          REPLACE(pdf_s3_id, '.pdf', '') AS pdf_uuid,
          REPLACE(grobid_s3_id, '.xml.gz', '') AS grobid_xml_id,
          native_id,
          native_id_namespace,
          version,
          updated_date,
          ROW_NUMBER() OVER (PARTITION BY work_id ORDER BY {PRIMARY_ORDER}) = 1 AS is_primary,
          ROW_NUMBER() OVER (
            PARTITION BY work_id ORDER BY grobid_s3_id IS NULL, {PRIMARY_ORDER}
          ) = 1 AS is_primary_xml
        FROM {table} lm
        WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
          AND work_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM openalex.pdf.manifest_excluded_pdfs x
            WHERE x.pdf_uuid = REPLACE(lm.pdf_s3_id, '.pdf', '')
          )
          {extra_where}
    """


def primary_pair_sql(extra_where: str = "", table: str = LOCATIONS_MAPPED) -> str:
    """One row per work with a file: the (pdf_uuid, grobid_uuid) pair the content API serves."""
    return f"""
        SELECT
          work_id,
          MAX(CASE WHEN is_primary THEN pdf_uuid END) AS pdf_uuid,
          MAX(CASE WHEN is_primary_xml THEN grobid_xml_id END) AS grobid_uuid
        FROM ({content_files_sql(extra_where, table)})
        GROUP BY work_id
    """
