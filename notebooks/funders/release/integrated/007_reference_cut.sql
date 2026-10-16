-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- datacite OR
CREATE OR REPLACE TEMP VIEW datacite_funder_references AS
WITH locations AS (
  SELECT DISTINCT work_id, lower(native_id) AS native_id
  FROM openalex.works.locations_mapped
  WHERE provenance='datacite' AND work_id IS NOT NULL
),
raw_ranked AS (
  SELECT lower(id) AS native_id, attributes.fundingReferences AS refs,
         attributes.publisher AS publisher, ingested_at, attributes.updated AS raw_updated,
         sha2(to_json(attributes),256) AS raw_hash,
         row_number() OVER (PARTITION BY lower(id) ORDER BY ingested_at DESC,
           attributes.updated DESC, sha2(to_json(attributes.fundingReferences),256) DESC,
           sha2(to_json(attributes),256) DESC) AS rn
  FROM openalex.datacite.datacite_items
),
raw_refs AS (
  SELECT l.work_id, l.native_id, r.ingested_at, r.raw_updated, r.raw_hash,
         pos AS reference_index, to_json(ref) AS raw_reference,
         ref.funderIdentifier AS identifier, ref.funderIdentifierType AS declared_type,
         ref.funderName AS funder_name, ref.awardNumber AS award_number,
         ref.awardUri AS award_uri,
         lower(regexp_extract(trim(ref.funderIdentifier), r'(?i)^(?:https?://ror\.org/)?([0-9a-z]{9})/?$',1)) AS rk,
         CASE WHEN upper(trim(r.publisher.publisherIdentifierScheme))='ROR'
           THEN nullif(lower(regexp_extract(trim(r.publisher.publisherIdentifier), r'(?i)^(?:https?://ror\.org/)?([0-9a-z]{9})/?$',1)),'') END AS publisher_ror
  FROM locations l JOIN raw_ranked r ON r.native_id=l.native_id AND r.rn=1
  LATERAL VIEW posexplode(r.refs) e AS pos, ref
)
SELECT *,sha2(to_json(named_struct('native_id',native_id,'name',funder_name,'ror',rk,'award_number',award_number,'award_uri',award_uri)),256) reference_key FROM raw_refs;
