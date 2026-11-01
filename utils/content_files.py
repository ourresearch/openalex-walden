"""Stored PDF / GROBID XML files per work location, and which one is each work's primary (oxjob #1951).

One row per location in `locations_mapped` that has a file. Used by the partner manifest
(notebooks/elastic/export_content_manifest_to_r2); the content API's D1 index moves to the same
rule on the switch date, so the manifest's `is_primary` row is the file content.openalex.org serves.

Primary: a location with a PDF over an XML-only one, then publishedVersion > acceptedVersion >
submittedVersion, then a PDF that has its XML, then the lowest UUID as the last tiebreak. The XML
always comes from the same location as the PDF. Locations whose PDF failed the byte check
(`openalex.pdf.manifest_excluded_pdfs`) are left out.
"""

VERSION_RANK = (
    "CASE version WHEN 'publishedVersion' THEN 1 WHEN 'acceptedVersion' THEN 2 "
    "WHEN 'submittedVersion' THEN 3 ELSE 4 END"
)


def content_files_sql(extra_where: str = "") -> str:
    """SELECT over locations_mapped; `extra_where` must start with AND and keep whole works."""
    return f"""
        SELECT
          work_id,
          REPLACE(pdf_s3_id, '.pdf', '') AS pdf_uuid,
          REPLACE(grobid_s3_id, '.xml.gz', '') AS grobid_xml_id,
          native_id,
          native_id_namespace,
          version,
          updated_date,
          ROW_NUMBER() OVER (
            PARTITION BY work_id
            ORDER BY pdf_s3_id IS NULL, {VERSION_RANK}, grobid_s3_id IS NULL,
                     pdf_s3_id, grobid_s3_id, native_id_namespace, native_id
          ) = 1 AS is_primary
        FROM openalex.works.locations_mapped lm
        WHERE (pdf_s3_id IS NOT NULL OR grobid_s3_id IS NOT NULL)
          AND work_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM openalex.pdf.manifest_excluded_pdfs x
            WHERE x.pdf_uuid = REPLACE(lm.pdf_s3_id, '.pdf', '')
          )
          {extra_where}
    """
