-- sql-template: rendered by lib/sql_templates.py, which fills every parameter before Spark runs it.
-- Piece 2b: one row per work of wt_work_set with every pack input (worktype-student-v2, 2026-11-04). Read-only temp view.
-- 11-09 re-cut: the work set is every live work (02_queue_keys.sql: the input_key of each) or tonight's candidates (the packs).
-- The statements are TEST-2/3/4's (student/out/v3/code/c_packs.py works/records/tags, b_pull.py B3), unchanged except
-- the record list: TEST-4 read the 10-22 pool's frozen location list (wtm1022_pool_locs = posexplode(openalex_works.
-- locations), freeze/sql/03_pool_locs.sql); the nightly reads the same posexplode live. B1/B2 (pages, reference
-- counts) are the records statement's own fields, so they aren't read twice (lib/pack_assembly.py bmap_from_records).
-- Today's type is never read here (blinding; evidence_packs.gate refuses a pack that carries it). No join hints: the
-- work set is every live work (~315M) for the keys but ~120K candidates a night for the packs, so Spark's adaptive plan picks the join (11-08).
CREATE OR REPLACE TEMP VIEW wt_pack_rows AS
WITH w AS (
  SELECT w.id AS work_id, w.doi, w.title, substr(w.abstract, 1, 1500) AS abstract, w.publication_year,
         w.language, w.publisher, w.authors_count, w.referenced_works_count, to_json(w.biblio) AS biblio,
         to_json(w.ids) AS ids, to_json(w.indexed_in) AS indexed_in, w.is_retracted, w.locations
  FROM openalex.works.openalex_works w
  JOIN wt_work_set c ON c.work_id = w.id
),
k AS (
  SELECT DISTINCT w.work_id, x.pos, x.loc.provenance AS provenance, x.loc.native_id AS native_id
  FROM w LATERAL VIEW posexplode(w.locations) x AS pos, loc
  WHERE x.loc.provenance IS NOT NULL AND x.loc.native_id IS NOT NULL
),
kk AS (SELECT DISTINCT provenance, native_id FROM k),
t AS (
  SELECT l.provenance, l.native_id, count(*) AS n_rows,
         max_by(named_struct('title', substr(l.title, 1, 2000),
                  'raw_type', CASE WHEN l.provenance = 'pubmed' THEN NULL ELSE l.raw_type END,
                  'version', l.version, 'published_date', CAST(l.published_date AS STRING), 'issue', l.issue,
                  'volume', l.volume, 'first_page', l.first_page, 'last_page', l.last_page,
                  'n_references', size(l.`references`.arxiv), 'ids', l.ids, 'source_name', l.source_name,
                  'publisher', l.publisher, 'landing_page_url', substr(l.landing_page_url, 1, 2000),
                  'source_id', l.source_id),
                coalesce(l.ingested_at, TIMESTAMP '1970-01-01 00:00:00')) AS r
  FROM openalex.works.locations_w_types l
  JOIN kk ON kk.provenance = l.provenance AND kk.native_id = l.native_id
  GROUP BY l.provenance, l.native_id
),
cs AS (
  SELECT native_id, max(subtype) AS cr_subtype FROM openalex.crossref.crossref_exploded
  WHERE native_id IN (SELECT native_id FROM kk WHERE provenance = 'crossref') GROUP BY native_id
),
rec AS (
  SELECT k.work_id,
         to_json(named_struct('work_id', k.work_id, 'pos', k.pos, 'provenance', k.provenance, 'native_id', k.native_id,
           'n_rows', t.n_rows, 'r', t.r, 'cr_subtype', cs.cr_subtype, 'src_type', s.type)) AS j
  FROM k
  JOIN t ON t.provenance = k.provenance AND t.native_id = k.native_id
  LEFT JOIN cs ON cs.native_id = k.native_id AND k.provenance = 'crossref'
  LEFT JOIN openalex.sources.sources s ON s.id = t.r.source_id
),
kq AS (
  SELECT DISTINCT work_id,
         CASE WHEN provenance IN ('crossref', 'datacite') THEN lower(native_id) ELSE native_id END AS m_key,
         CASE provenance WHEN 'crossref' THEN 'doi' WHEN 'datacite' THEN 'doi' WHEN 'pubmed' THEN 'pmid'
                         WHEN 'repo' THEN 'pmh' WHEN 'mag' THEN 'mag' END AS m_ns
  FROM k
),
g AS (
  SELECT m.native_id AS k, m.native_id_namespace AS ns,
         max_by(named_struct('page_title', m.tx_page_title, 'tx_meta', to_json(m.tx_meta)),
                named_struct('d', m.derived_at, 'f', m.file_key)) AS mm
  FROM openalex.landing_page.meta_tags_for_work_type m
  JOIN (SELECT DISTINCT m_key, m_ns FROM kq) q ON m.native_id = q.m_key AND m.native_id_namespace = q.m_ns
  GROUP BY m.native_id, m.native_id_namespace
),
tags AS (
  SELECT kq.work_id,
         to_json(named_struct('k', g.k, 'ns', g.ns, 'page_title', g.mm.page_title, 'tx_meta_json', g.mm.tx_meta)) AS tj
  FROM kq JOIN g ON g.k = kq.m_key AND g.ns = kq.m_ns
),
pm AS (
  SELECT CAST(p.pmid AS STRING) AS pmid,
         max_by(p.MedlineCitation.Article.PublicationTypeList.PublicationType._VALUE,
                concat(coalesce(CAST(p.revised_date AS STRING), ''), '|',
                       md5(coalesce(to_json(p.MedlineCitation.Article.PublicationTypeList), '')))) AS pts
  FROM openalex.pubmed.pubmed_exploded p
  WHERE p.pmid IN (SELECT CAST(native_id AS BIGINT) FROM kk WHERE provenance = 'pubmed' AND native_id RLIKE '^[0-9]+$')
  GROUP BY p.pmid
),
dc AS (
  SELECT lower(d.id) AS doi,
         max(lower(trim(d.attributes.types.resourceTypeGeneral))) AS dc_type_general,
         max(left(lower(trim(d.attributes.types.resourceType)), 60)) AS dc_resource_type
  FROM openalex.datacite.datacite_items d
  WHERE lower(d.id) IN (SELECT lower(native_id) FROM kk WHERE provenance = 'datacite')
  GROUP BY lower(d.id)
),
b3 AS (
  SELECT DISTINCT k.work_id, to_json(named_struct('provenance', 'pubmed', 'native_id', pm.pmid,
           'medline_types', to_json(pm.pts))) AS bj
  FROM k JOIN pm ON k.provenance = 'pubmed' AND pm.pmid = k.native_id
  UNION ALL
  SELECT DISTINCT k.work_id, to_json(named_struct('provenance', 'datacite', 'native_id', dc.doi,
           'dc_type_general', dc.dc_type_general, 'dc_resource_type', dc.dc_resource_type))
  FROM k JOIN dc ON k.provenance = 'datacite' AND dc.doi = lower(k.native_id)
)
SELECT w.work_id,
       to_json(named_struct('work_id', w.work_id, 'doi', w.doi, 'title', w.title, 'abstract', w.abstract,
         'publication_year', w.publication_year, 'language', w.language, 'publisher', w.publisher,
         'authors_count', w.authors_count, 'referenced_works_count', w.referenced_works_count, 'biblio', w.biblio,
         'ids', w.ids, 'indexed_in', w.indexed_in, 'is_retracted', w.is_retracted)) AS work_j,
       coalesce(r.recs, array()) AS recs,
       coalesce(tg.tags, array()) AS tags,
       coalesce(b.b3, array()) AS b3
FROM w
LEFT JOIN (SELECT work_id, collect_list(j) AS recs FROM rec GROUP BY work_id) r ON r.work_id = w.work_id
LEFT JOIN (SELECT work_id, collect_list(tj) AS tags FROM tags GROUP BY work_id) tg ON tg.work_id = w.work_id
LEFT JOIN (SELECT work_id, collect_list(bj) AS b3 FROM b3 GROUP BY work_id) b ON b.work_id = w.work_id;
