-- Databricks notebook source
-- CreateLocationTypes — work-type classifier at LOCATION level (Casey/Jason design
-- 2026-07-14). New step between Locations_with_Sources and Locations_Mapped.
-- Rulings: n_refs + has_journal = merge_key group windows; new table (rebuild idiom);
-- BOTH columns written: `type` passes through UNCHANGED (day-one inert) and
-- `classified_type` carries the cascade's answer — the flip is `type = classified_type`
-- in this one step, later, deliberately. Full daily recompute, no hash (Jason).
-- Classifier: frozen post-T5 CASE (163 rules; parity 52,383/52,383, synth 117/117,
-- escape PASS, gold 85.65 ratified at WORK grain — location-grain re-validation tracked).

-- COMMAND ----------

CREATE OR REPLACE TABLE identifier('openalex' || :env_suffix || '.works.locations_with_types')
CLUSTER BY (best_doi, provenance, native_id)
AS (

WITH loc AS (
  SELECT
    l.provenance, l.native_id, l.native_id_namespace, l.source_id, l.type AS existing_type,
    concat_ws('|', coalesce(l.merge_key.doi,''), coalesce(l.merge_key.pmid,''),
              coalesce(l.merge_key.arxiv,''), coalesce(l.merge_key.title_author,'')) AS work_group,
    l.title AS oa_title,
    l.raw_type AS oa_raw_type,
    CASE WHEN l.provenance = 'crossref' THEN l.raw_type END AS cr_type,
    CASE WHEN l.provenance = 'crossref' THEN l.source_name END AS cr_container,
    false AS cr_isbn,
    l.source_name AS oa_source_name,
    l.issue AS oa_issue, l.first_page AS oa_first_page,
    (l.first_page IS NOT NULL AND l.first_page <> '' AND l.first_page = l.last_page) AS oa_single_page,
    coalesce(size(l.references), 0) AS rec_n_refs,
    (l.abstract IS NOT NULL AND l.abstract <> '') AS oa_has_abstract,
    coalesce(l.is_retracted, false) AS oa_is_retracted,
    CAST(NULL AS STRING) AS oa_type,        -- recovery rule deliberately dormant (types being nulled)
    l.abstract AS oa_abstract,
    coalesce(l.best_doi, CASE WHEN l.native_id_namespace = 'doi' THEN l.native_id END) AS doi,
    l.landing_page_url AS tx_resolved_url
  FROM identifier('openalex' || :env_suffix || '.works.locations_w_sources') l
  
),
cr_sub AS (  -- subtype exists only in crossref_exploded (1 rule); crossref rows only
  SELECT native_id, max(subtype) AS cr_subtype
  FROM openalex.crossref.crossref_exploded
  WHERE native_id IN (SELECT native_id FROM loc WHERE provenance = 'crossref')
  GROUP BY native_id
),
src AS (
  SELECT id AS source_id, type AS src_type FROM openalex.sources.sources
),
works AS (
  SELECT
    concat_ws('~', loc.provenance, loc.native_id_namespace, loc.native_id) AS work_id,
    loc.provenance, loc.native_id, loc.native_id_namespace, loc.existing_type, loc.work_group,
    loc.oa_title, loc.oa_raw_type, loc.cr_type,
    CASE WHEN loc.provenance = 'crossref' THEN cs.cr_subtype END AS cr_subtype,
    loc.cr_container, loc.cr_isbn, loc.oa_source_name,
    s.src_type AS oa_source_type,
    loc.oa_issue, loc.oa_first_page,
    max(coalesce(loc.rec_n_refs, 0)) OVER (PARTITION BY loc.work_group) AS oa_n_refs,
    loc.oa_single_page, loc.oa_has_abstract, loc.oa_is_retracted, loc.oa_type, loc.oa_abstract,
    m.tx_meta AS tx_meta, m.tx_page_title AS tx_page_title,
    loc.tx_resolved_url, loc.doi,
    max(CASE WHEN lower(coalesce(s.src_type, '')) = 'journal' THEN 1 ELSE 0 END)
      OVER (PARTITION BY loc.work_group) = 1 AS oa_has_journal_location
  FROM loc
  LEFT JOIN cr_sub cs ON cs.native_id = loc.native_id AND loc.provenance = 'crossref'
  LEFT JOIN src s ON s.source_id = loc.source_id
  LEFT JOIN openalex.landing_page.meta_tags_for_work_type m ON m.native_id = loc.native_id
    AND m.native_id_namespace = loc.native_id_namespace
),
feat AS (
  SELECT work_id,
    lower(coalesce(oa_title, '')) AS f_title,
    lower(coalesce(nullif(oa_raw_type, ''), nullif(cr_type, ''), '')) AS f_raw,
    lower(coalesce(cr_type, '')) AS f_crtype,
    lower(coalesce(cr_subtype, '')) AS f_sub,
    lower(coalesce(oa_source_name, '')) AS f_src,
    lower(coalesce(cr_container, '')) AS f_cont,
    lower(coalesce(cast(oa_issue AS string), '')) AS f_issue,
    CASE WHEN oa_first_page IS NULL THEN '' ELSE lower(trim(split(cast(oa_first_page AS string), '-')[0])) END AS f_fp,
    coalesce(oa_n_refs, 0) AS f_nrefs,
    coalesce(oa_single_page, false) AS f_single,
    coalesce(oa_has_abstract, false) AS f_hasabs,
    coalesce(oa_is_retracted, false) AS f_retr,
    lower(coalesce(oa_type, '')) AS f_oatype,
    lower(coalesce(tx_page_title, '')) AS f_ptl,
    lower(coalesce(oa_abstract, '')) AS f_abs,
    coalesce(regexp_extract(lower(coalesce(tx_resolved_url, '')), '^[a-z][a-z0-9+.\\-]*://([^/?#]*)', 1), '') AS f_host,
    CASE WHEN lower(coalesce(tx_resolved_url, '')) RLIKE '^[a-z][a-z0-9+.\\-]*://' THEN coalesce(regexp_extract(lower(coalesce(tx_resolved_url, '')), '^[a-z][a-z0-9+.\\-]*://[^/?#]*([^?#]*)', 1), '') ELSE lower(coalesce(tx_resolved_url, '')) END AS f_path,
    transform(flatten(transform(coalesce(tx_meta, array()), m -> regexp_extract_all(lower(m), '(?:dc\\.type(?:\\.articletype)?|article-type|articletype|dcterms\\.type|prism\\.contenttype|citation_article_type)"?\\s*(?:content=)?[":=]?\\s*"?\\s*([a-zA-Z][\\p{L}\\p{N}_ .\\-/]{1,40})', 1))), v -> trim(trim(TRAILING '"/' FROM trim(v)))) AS f_dc,
    transform(flatten(transform(coalesce(tx_meta, array()), m -> regexp_extract_all(lower(m), 'og:type"?\\s*(?:content=)?"?\\s*([a-zA-Z][\\p{L}\\p{N}_ .\\-/]{1,30})', 1))), v -> trim(trim(TRAILING '"/' FROM trim(v)))) AS f_og,
    exists(coalesce(tx_meta, array()), m -> lower(m) RLIKE '(?:name|property)\\s*=\\s*"(?:citation_conference_date|citation_conference_location)"') AS k_confabs,
    exists(coalesce(tx_meta, array()), m -> lower(m) RLIKE '(?:name|property)\\s*=\\s*"(?:citation_conference_abbrev|citation_conference_abbreviation|citation_conference_identifier|citation_conference_series_id)"') AS k_confpap,
    exists(coalesce(tx_meta, array()), m -> lower(m) RLIKE '(?:name|property)\\s*=\\s*"(?:bepress_citation_dissertation_institution|bepress_citation_dissertation_name|citation_dissertation_institution|citation_dissertation_name)"') AS k_diss,
    exists(coalesce(tx_meta, array()), m -> lower(m) LIKE '%citation_dissertation_%' AND lower(m) RLIKE 'content\\s*=\\s*"[^"]') AS k_diss_content,
    lower(coalesce(doi, '')) AS f_doi,
    lower(coalesce(tx_resolved_url, '')) AS f_url,
    coalesce(cr_isbn, false) AS f_isbn,
    lower(coalesce(oa_source_type, '')) AS f_srctype,
    coalesce(oa_has_journal_location, false) AS f_hasjournal,
    lower(array_join(coalesce(tx_meta, array()), ' ')) AS f_meta,
    exists(coalesce(tx_meta, array()), m -> lower(m) RLIKE '(?:name|property)\\s*=\\s*"citation_isbn"') AS k_isbn
  FROM works),
feat2 AS (
  SELECT *,
    concat(f_src, ' ', f_cont) AS f_sc,
    regexp_extract_all(f_path, '[a-z]{3,}', 0) AS f_urltok,
    replace(replace(replace(replace(f_raw, '-', ''), '_', ''), ' ', ''), ':', '') AS f_rawnorm
  FROM feat),
scored AS (
  SELECT work_id,
  CASE
      WHEN (f_title LIKE 'retraction%' OR f_title LIKE 'statement of retraction%') OR (f_retr AND f_title LIKE 'withdrawn%') OR (f_abs LIKE '%this retracts%' OR f_abs LIKE '%retracts the article%') THEN 'retraction'
      WHEN (f_title LIKE '%erratum%' OR f_title LIKE '%corrigendum%' OR f_title LIKE '%correction to%' OR f_title LIKE '%author correction%' OR f_title LIKE '%publisher correction%') OR f_title LIKE 'correction%' OR (f_abs LIKE '%this corrects the article%' OR f_abs LIKE '%corrects the article%') THEN 'erratum'
      WHEN f_rawnorm = 'peerreview' OR (f_title LIKE 'review for%' OR f_title LIKE 'decision letter%' OR f_title LIKE 'author response%' OR f_title LIKE 'reply on%' OR f_title LIKE 'peer review of%' OR f_title LIKE 'reviewer public%' OR f_title LIKE 'comment on egusphere%') THEN 'peer-review'
      WHEN f_crtype = 'dissertation' THEN 'dissertation'
      WHEN f_crtype IN ('reference-entry','reference-book') THEN 'reference-entry'
      WHEN f_crtype = 'standard' THEN 'standard'
      WHEN f_crtype = 'report-component' THEN 'report'
      WHEN f_sub = 'preprint' THEN 'preprint'
      WHEN f_host IN ('osf.io', 'www.researchsquare.com') THEN 'preprint'
      WHEN f_host IN ('www.encodeproject.org', 'www.rcsb.org', 'www.wwpdb.org') THEN 'dataset'
      WHEN f_host IN ('www.softxjournal.com') THEN 'software-paper'
      WHEN (f_host IN ('cran.r-project.org', 'demonstrations.wolfram.com')) AND f_raw <> 'dataset' THEN 'software'
      WHEN f_host IN ('facultyopinions.com', 'publons.com', 'www.webofscience.com') THEN 'peer-review'
      WHEN f_host IN ('theses.fr', 'theses.hal.science') THEN 'dissertation'
      WHEN f_host IN ('materials.springer.com', 'referenceworks.brill.com', 'www.cabidigitallibrary.org', 'www.oed.com', 'www.oxfordartonline.com', 'www.ukwhoswho.com') THEN 'reference-entry'
      WHEN f_host IN ('meetingorganizer.copernicus.org', 'www.morressier.com') THEN 'conference-abstract'
      WHEN f_host IN ('goodreads.com', 'www.goodreads.com') THEN 'book'
      WHEN f_host IN ('picryl.com', 'www.picryl.com') THEN 'other'
      WHEN f_src IN ('abstracts', 'abstracts with programs - geological society of america', 'academy of management proceedings', 'endocrine abstracts', 'the proceedings of the annual convention of the japanese psychological association') THEN 'conference-abstract'
      WHEN f_src IN ('brill’s new pauly', 'definitions', 'der neue pauly', 'encyclopédie de l’islam', 'iucn red list of threatened species', 'lexikon des gesamten buchwesens online', 'radiopaedia.org', 'religion in geschichte und gegenwart', 'springerreference', 'supplementum epigraphicum graecum', 'the shafr guide online', 'who was who', 'who\'s who') THEN 'reference-entry'
      WHEN f_src IN ('psyctests dataset') THEN 'dataset'
      WHEN f_src IN ('research square', 'ssrn electronic journal') THEN 'preprint'
      WHEN f_src IN ('data in brief') THEN 'data-paper'
      WHEN f_src IN ('softwarex', 'the journal of open source software') THEN 'software-paper'
      WHEN f_src IN ('acta horticulturae', 'ecs transactions', 'iceri proceedings', 'ifac proceedings volumes', 'materials today proceedings', 'procedia engineering') THEN 'conference-paper'
      WHEN f_src IN ('faculty opinions – post-publication peer review of the biomedical literature') THEN 'peer-review'
      WHEN f_src IN ('apress ebooks', 'jaypee brothers medical publishers (p) ltd. ebooks') THEN 'book-chapter'
      WHEN f_src IN ('bulletin of the center for children\'s books', 'choice reviews online') THEN 'book-review'
      WHEN f_src IN ('electronic enlightenment scholarly edition of correspondence') THEN 'other'
      WHEN f_src IN ('national bureau of economic research') THEN 'report'
      WHEN f_src IN ('synfacts') THEN 'editorial'
      WHEN f_sc LIKE '%datasets%' THEN 'dataset'
      WHEN f_sc LIKE '%web of conferences%' THEN 'conference-paper'
      WHEN f_sc LIKE '%rxiv%' THEN 'preprint'
      WHEN f_sc LIKE '%preprint%' THEN 'preprint'
      WHEN f_sc LIKE '%dictionary%' THEN 'reference-entry'
      WHEN f_sc LIKE '%encyclopedia%' THEN 'reference-entry'
      WHEN f_sc LIKE '%lexicon%' THEN 'reference-entry'
      WHEN f_sc LIKE '%meeting abstracts%' THEN 'conference-abstract'
      WHEN f_src IN ('e3s web of conferences', 'lecture notes on data engineering and communications technologies', 'procedia - social and behavioral sciences') THEN 'conference-paper'
      WHEN f_src IN ('european urology supplements') THEN 'conference-abstract'
      WHEN f_src IN ('gisaid') THEN 'dataset'
      WHEN (f_src LIKE '%encode%' OR f_cont LIKE '%encode%') THEN 'dataset'
      WHEN (f_src LIKE '%spie proceedings%' OR f_cont LIKE '%spie proceedings%') THEN 'conference-paper'
      WHEN (f_src LIKE '%worldwide protein data bank%' OR f_cont LIKE '%worldwide protein data bank%') THEN 'dataset'
      WHEN (f_src LIKE '%sae technical paper series%' OR f_cont LIKE '%sae technical paper series%') THEN 'conference-paper'
      WHEN (f_src LIKE '%advances in social science, education and humanities research%' OR f_cont LIKE '%advances in social science, education and humanities research%') THEN 'conference-paper'
      WHEN (f_src LIKE '%conference on lasers and electro-optics%' OR f_cont LIKE '%conference on lasers and electro-optics%') THEN 'conference-paper'
      WHEN (f_src LIKE '%ifmbe proceedings%' OR f_cont LIKE '%ifmbe proceedings%') THEN 'conference-paper'
      WHEN (f_src LIKE '%morphosource%' OR f_cont LIKE '%morphosource%') THEN 'dataset'
      WHEN (f_src LIKE '%sgem international multidisciplinary scientific geoconference%' OR f_cont LIKE '%sgem international multidisciplinary scientific geoconference%') THEN 'conference-paper'
      WHEN f_doi LIKE '%meetingabstracts%' OR f_doi LIKE '%meeting-abstracts%' OR f_url LIKE '%meetingabstracts%' OR f_url LIKE '%meeting-abstracts%' THEN 'conference-abstract'
      WHEN f_title LIKE 'editorial board%' THEN 'paratext'
      WHEN f_title LIKE 'front matter%' THEN 'paratext'
      WHEN (f_title LIKE 'preface%' OR f_title LIKE 'appendix%' OR f_title LIKE 'proofs of%') AND (f_raw IN ('book-chapter','book-part','chapter','book-section') OR f_crtype IN ('book-chapter','monograph','edited-book')) THEN 'paratext'
      WHEN array_contains(f_urltok, 'referenceworkentry') THEN 'reference-entry'
      WHEN array_contains(f_urltok, 'meetingabstracts') THEN 'conference-abstract'
      WHEN (array_contains(f_urltok, 'thesis') OR array_contains(f_urltok, 'theses') OR array_contains(f_urltok, 'dissertations')) AND f_crtype = '' AND f_srctype <> 'journal' THEN 'dissertation'
      WHEN k_confabs THEN 'conference-abstract'
      WHEN k_confpap THEN 'conference-paper'
      WHEN array_contains(f_dc, 'book-review') THEN 'book-review'
      WHEN array_contains(f_dc, 'bookreview') THEN 'book-review'
      WHEN array_contains(f_dc, 'book reviews') THEN 'book-review'
      WHEN array_contains(f_dc, 'book review') THEN 'book-review'
      WHEN array_contains(f_dc, 'reseñas') THEN 'book-review'
      WHEN array_contains(f_dc, 'thesis') THEN 'dissertation'
      WHEN array_contains(f_dc, 'dissertação') THEN 'dissertation'
      WHEN array_contains(f_dc, 'doctoral dissertation') THEN 'dissertation'
      WHEN array_contains(f_dc, 'pg_thesis') THEN 'dissertation'
      WHEN array_contains(f_dc, 'editorial') THEN 'editorial'
      WHEN array_contains(f_dc, 'editorialnotes') THEN 'editorial'
      WHEN array_contains(f_dc, 'article-commentary') THEN 'editorial'
      WHEN array_contains(f_dc, 'meeting-report') THEN 'conference-abstract'
      WHEN array_contains(f_dc, 'congress-abstract') THEN 'conference-abstract'
      WHEN array_contains(f_dc, 'oxan-executive-summary') THEN 'report'
      WHEN array_contains(f_dc, 'news') THEN 'other'
      WHEN array_contains(f_dc, 'chapter') THEN 'book-chapter'
      WHEN f_ptl LIKE 'reply%' THEN 'letter'
      WHEN (f_title LIKE 'supplementary%' OR f_title LIKE 'supplemental%' OR f_title LIKE 'figure from%') OR (f_title LIKE '%supplementary figure%' OR f_title LIKE '%supplementary table%' OR f_title LIKE '%supplemental material%' OR f_title LIKE '%figure from%') THEN 'supplementary-materials'
      WHEN (f_title LIKE 'table of contents%' OR f_title LIKE 'contents%' OR f_title LIKE 'front matter%' OR f_title LIKE 'back matter%' OR f_title LIKE 'frontmatter%' OR f_title LIKE 'front cover%' OR f_title LIKE 'editorial board%' OR f_title LIKE 'subject index%' OR f_title LIKE 'author index%' OR f_title LIKE 'name index%' OR f_title LIKE 'list of figures%' OR f_title LIKE 'list of tables%' OR f_title LIKE 'list of contributors%' OR f_title LIKE 'list of abbreviations%' OR f_title LIKE 'list of illustrations%' OR f_title LIKE 'list of plates%' OR f_title LIKE 'bibliography%' OR f_title LIKE 'abbreviations%' OR f_title LIKE 'abbreviation%' OR f_title LIKE 'acknowledgment%' OR f_title LIKE 'acknowledgments%' OR f_title LIKE 'acknowledgement%' OR f_title LIKE 'acknowledgements%' OR f_title LIKE 'dedication%' OR f_title LIKE 'contributors%' OR f_title LIKE 'about the author%' OR f_title LIKE 'about the editor%' OR f_title LIKE 'copyright%' OR f_title LIKE 'title page%' OR f_title LIKE 'masthead%' OR f_title LIKE 'frontispiece%' OR f_title LIKE 'titelei%' OR f_title LIKE 'inhaltsverzeichnis%' OR f_title LIKE 'sachregister%' OR f_title LIKE 'literaturverzeichnis%' OR f_title LIKE 'inhalt%' OR f_title LIKE 'session details%' OR f_title LIKE 'forthcoming%' OR f_title LIKE 'calendar%' OR f_title LIKE 'general index%' OR f_title LIKE 'back cover%' OR f_title LIKE 'inside front cover%' OR f_title LIKE 'prelims%' OR f_title LIKE 'preliminary material%' OR f_title LIKE 'backmatter%' OR f_title LIKE 'books received%' OR f_title LIKE 'works cited%' OR f_title LIKE 'about the contributors%' OR f_title LIKE 'author biograph%' OR f_title LIKE 'expediente%' OR f_title LIKE 'table des mati%' OR f_title LIKE 'remerciements%') THEN 'paratext'
      WHEN (f_title LIKE '%issue information%' OR f_title LIKE '%masthead%' OR f_title LIKE '%editorial board%' OR f_title LIKE '%instructions for authors%' OR f_title LIKE '%list of reviewers%' OR f_title LIKE '%acknowledgment of reviewers%' OR f_title LIKE '%acknowledgement of reviewers%' OR f_title LIKE '%cover image%' OR f_title LIKE '%information for authors%' OR f_title LIKE '%society information%' OR f_title LIKE '%information for contributors%' OR f_title LIKE '%information for readers%' OR f_title LIKE '%notes for contributors%' OR f_title LIKE '%notes on contributors%' OR f_title LIKE '%call for papers%' OR f_title LIKE '%call for submissions%' OR f_title LIKE '%call for abstracts%' OR f_title LIKE '%guide for authors%' OR f_title LIKE '%impressum%' OR f_title LIKE '%publication information%' OR f_title LIKE '%reviewer acknowledgement%') THEN 'paratext'
      WHEN trim(f_title) = 'notes' THEN 'paratext'
      WHEN trim(f_title) = 'peer review statement' THEN 'paratext'
      WHEN (f_title LIKE 'program committee%' OR f_title LIKE 'organizing committee%' OR f_title LIKE 'workshop committee%' OR f_title LIKE 'conference committee%' OR f_title LIKE 'scientific committee%' OR f_title LIKE 'technical program committee%' OR f_title LIKE 'steering committee%') OR trim(f_title) RLIKE '^(program |organizing |scientific |technical |workshop |conference |steering )?committee(s)?( members| list(ing)?s?)?$' THEN 'paratext'
      WHEN f_title LIKE 'index%' OR ((f_title LIKE 'references%' OR f_title LIKE 'list of%') AND (f_fp IN ('i','ii','iii','iv','ix','v','vi','vii','viii','x','xi','xii','xiii','xiv','xv') OR f_nrefs = 0 OR NOT f_hasabs)) THEN 'paratext'
      WHEN (f_title LIKE '%python package%') THEN 'software-paper'
      WHEN (f_title LIKE 'din en%' OR f_title LIKE 'specification for%' OR f_title LIKE 'test method%') OR (f_title LIKE '%englische fassung%') THEN 'standard'
      WHEN (f_title LIKE 'encsr%') THEN 'dataset'
      WHEN (f_title LIKE 'book review%' OR f_title LIKE 'review of the book%' OR f_title LIKE 'reseña del libro%') OR (f_title LIKE '% isbn%' OR f_title LIKE '%edited by%') OR array_contains(f_dc, 'book-review') OR (f_title LIKE '%pp.%' AND (f_title LIKE '%isbn%' OR f_title LIKE '%press%' OR f_title LIKE '%£%')) THEN 'book-review'
      WHEN (f_title LIKE 'guest editorial%' OR f_title LIKE 'editorial comment%' OR f_title LIKE 'guest editor%' OR f_title LIKE 'commentary on%' OR f_title LIKE 'message from%' OR f_title LIKE 'editorial board is%' OR f_title LIKE 'editorial:%' OR f_title LIKE 'preface:%' OR f_title LIKE 'préambule%' OR f_title LIKE 'éditorial%' OR f_title LIKE 'editors\' note%' OR f_title LIKE 'editors note%' OR f_title LIKE 'special thanks%' OR f_title LIKE 'nota de la directora%' OR f_title LIKE 'note from the editor%' OR f_title LIKE 'interview with%' OR f_title LIKE 'interview:%' OR f_title LIKE 'entrevista%') OR (f_title LIKE '%from the editor%' OR f_title LIKE '%special issue on%' OR f_title LIKE '%to the special issue%' OR f_title LIKE '%commentary:%') OR (f_title LIKE 'editorial%' AND f_title NOT LIKE '%board%') THEN 'editorial'
      WHEN (f_title LIKE 'letter to the%' OR f_title LIKE 'reply to%' OR f_title LIKE 'in reply%' OR f_title LIKE 'reader response%' OR f_title LIKE 'comments on the article%') OR (f_title LIKE '%to the editor%' OR f_title LIKE '%authors\' reply%' OR f_title LIKE '%reply to comment%') OR ((f_title LIKE 'reply%' OR f_title LIKE 'comment on%') AND f_single) OR f_title LIKE 'correspondence%' THEN 'letter'
      WHEN (f_title LIKE '%narrative review%' OR f_title LIKE '%mini-review%' OR f_title LIKE '%meta-analysis of%') THEN 'review'
      WHEN (f_title LIKE 'libguides%' OR f_title LIKE 'all guides%' OR f_title LIKE 'research guides%') THEN 'libguides'
      WHEN (f_title LIKE 're:%' OR f_title LIKE 'the authors reply%' OR f_title LIKE 'comment on:%') THEN 'letter'
      WHEN f_title LIKE 'discussion of%' THEN 'editorial'
      WHEN f_title LIKE 'data for %' THEN 'dataset'
      WHEN f_title LIKE '%systematic literature review%' AND NOT (f_title LIKE '%case report%' OR f_title LIKE '%case study%') THEN 'review'
      WHEN (f_title LIKE '%in memoriam%' OR f_title LIKE '%autograph letter%' OR f_title LIKE '%obituary%') THEN 'other'
      WHEN f_title LIKE 'abstract%' THEN 'conference-abstract'
      WHEN (f_src LIKE '%abstract%' OR f_cont LIKE '%abstract%') AND (f_single OR (f_nrefs = 0 AND f_hasabs)) THEN 'conference-abstract'
      WHEN f_src LIKE '%supplement%' AND f_single AND f_nrefs = 0 THEN 'conference-abstract'
      WHEN f_issue LIKE '%suppl%' AND f_single THEN 'conference-abstract'
      WHEN f_raw = 'journal-article' AND f_nrefs = 0 AND f_single AND (f_issue RLIKE '^s[0-9]' OR f_issue RLIKE '^[0-9]+s$') THEN 'conference-abstract'
      WHEN (f_abs LIKE '%abstracts of presentations%' OR f_abs LIKE '%searchable abstracts%') THEN 'conference-abstract'
      WHEN ltrim(f_abs) LIKE 'reviewed by%' THEN 'book-review'
      WHEN (f_abs LIKE '%this data article%') THEN 'data-paper'
      WHEN (f_abs LIKE '%this editorial%' OR f_abs LIKE '%in this editorial%') THEN 'editorial'
      WHEN f_src IN ('communications in computer and information science', 'energy procedia', 'lecture notes in civil engineering', 'lecture notes in computer science', 'procedia computer science') AND (f_nrefs = 0 AND f_single AND f_hasabs) THEN 'conference-abstract'
      WHEN f_src IN ('communications in computer and information science', 'energy procedia', 'lecture notes in civil engineering', 'lecture notes in computer science', 'procedia computer science') THEN 'conference-paper'
      WHEN f_src IN ('scientific data') THEN 'data-paper'
      WHEN (f_src LIKE '%journal of physics: conference series%' OR f_cont LIKE '%journal of physics: conference series%') AND (f_nrefs = 0 AND f_single AND f_hasabs) THEN 'conference-abstract'
      WHEN (f_src LIKE '%journal of physics: conference series%' OR f_cont LIKE '%journal of physics: conference series%') THEN 'conference-paper'
      WHEN f_title RLIKE '^[a-z]{1,3}-?[0-9]{2,5}[.:\\s\\p{Z}]' AND f_nrefs = 0 AND f_raw NOT IN ('dataset','database') THEN 'conference-abstract'
      WHEN f_title LIKE '%systematic review%' AND f_nrefs > 0 THEN 'review'
      WHEN f_oatype = 'review' AND f_nrefs >= 25 AND f_hasabs THEN 'review'
      WHEN f_sc LIKE '%conference%' AND (f_nrefs = 0 AND f_single AND f_hasabs) THEN 'conference-abstract'
      WHEN f_sc LIKE '%conference%' THEN 'conference-paper'
      WHEN f_sc LIKE '%symposium%' AND (f_nrefs = 0 AND f_single AND f_hasabs) THEN 'conference-abstract'
      WHEN f_sc LIKE '%symposium%' THEN 'conference-paper'
      WHEN f_sc LIKE '%workshop%' AND (f_nrefs = 0 AND f_single AND f_hasabs) THEN 'conference-abstract'
      WHEN f_sc LIKE '%workshop%' THEN 'conference-paper'
      WHEN f_raw = 'proceedings-article' AND (f_nrefs = 0 AND f_single AND f_hasabs) THEN 'conference-abstract'
      WHEN f_raw = 'proceedings-article' THEN 'conference-paper'
      WHEN f_raw = 'proceedings' AND f_crtype = '' AND f_title NOT LIKE 'proceedings%' AND (f_nrefs = 0 AND f_single AND f_hasabs) THEN 'conference-abstract'
      WHEN f_raw = 'proceedings' AND f_crtype = '' AND f_title NOT LIKE 'proceedings%' THEN 'conference-paper'
      WHEN f_crtype = 'journal-issue' THEN 'paratext'
      WHEN f_crtype IN ('edited-book','monograph') THEN 'book'
      WHEN f_raw = 'reference-entry' THEN 'reference-entry'
      WHEN f_raw = 'dissertation' THEN 'dissertation'
      WHEN f_nrefs >= 20 AND (rtrim(f_title, ' .') LIKE '%a review' OR rtrim(f_title, ' .') LIKE '%a literature review' OR f_title LIKE '%scientometric review%') THEN 'review'
      WHEN f_title LIKE '%a meta-analysis%' AND f_nrefs >= 20 THEN 'review'
      WHEN f_raw LIKE '%eu-repo/semantics/%' AND trim(f_raw) LIKE '%/conferenceobject' THEN 'conference-paper'
      WHEN f_raw LIKE '%eu-repo/semantics/%' AND trim(f_raw) LIKE '%/bookpart' THEN 'book-chapter'
      WHEN f_raw LIKE '%eu-repo/semantics/%' AND trim(f_raw) LIKE '%/doctoralthesis' THEN 'dissertation'
      WHEN f_raw LIKE '%eu-repo/semantics/%' AND trim(f_raw) LIKE '%/masterthesis' THEN 'dissertation'
      WHEN f_raw LIKE '%eu-repo/semantics/%' AND trim(f_raw) LIKE '%/article' THEN 'article'
      WHEN f_raw LIKE '%eu-repo/semantics/%' AND trim(f_raw) LIKE '%/report' THEN 'report'
      WHEN f_raw LIKE '%eu-repo/semantics/%' AND trim(f_raw) LIKE '%/other' THEN 'other'
      WHEN f_raw LIKE '%thesis%' THEN 'dissertation'
      WHEN f_raw LIKE '%väitöskirja%' THEN 'dissertation'
      WHEN f_raw LIKE '%hochschulschrift%' THEN 'dissertation'
      WHEN (f_raw LIKE 'tesis%' OR f_raw LIKE '%bakalářská práce%') THEN 'dissertation'
      WHEN f_raw LIKE '%final year project%' THEN 'report'
      WHEN f_rawnorm IN ('chapter','bookpart') THEN 'book-chapter'
      WHEN f_rawnorm LIKE '%conferencepaper' THEN 'conference-paper'
      WHEN f_rawnorm = 'researchreport' THEN 'report'
      WHEN f_raw = 'figure' THEN 'supplementary-materials'
      WHEN f_rawnorm = 'software,multimedia' THEN 'other'
      WHEN f_raw = 'software' THEN 'software'
      WHEN f_raw LIKE '%printed serial%' THEN 'other'
      WHEN f_rawnorm IN ('image','physicalobject') THEN 'other'
      WHEN f_rawnorm IN ('audiovisual','sound') THEN 'other'
      WHEN (f_raw LIKE '%monograf%' OR f_raw LIKE '%monograph%') THEN 'book'
      WHEN f_rawnorm LIKE '%book' AND f_raw NOT IN ('book','edited-book','monograph','book-set') THEN 'book'
      WHEN f_raw LIKE '%preprint%' AND NOT (f_raw LIKE '%eu-repo%' AND NOT trim(f_raw) LIKE '%/preprint') AND NOT (f_srctype = 'journal' AND NOT (f_src LIKE '%rxiv%' OR f_src LIKE '%preprint%' OR f_src LIKE '%repec%' OR f_src LIKE '%ssrn%' OR f_src LIKE '%zenodo%' OR f_src LIKE '%research square%' OR f_src LIKE '%osf%')) AND NOT f_hasjournal THEN 'preprint'
      WHEN f_raw IN ('book-chapter','book-part') THEN 'book-chapter'
      WHEN f_raw = 'book-section' THEN 'reference-entry'
      WHEN f_raw IN ('book','edited-book','monograph','book-set') THEN 'book'
      WHEN f_raw = 'report' THEN 'report'
      WHEN f_raw = 'posted-content' THEN 'other'
      WHEN f_raw IN ('dataset','database') THEN 'dataset'
      WHEN f_raw = 'proceedings' THEN 'paratext'
      WHEN f_raw = 'other' THEN 'other'
      ELSE 'article' END AS classified_type
  FROM feat2
)
SELECT l.*, sc.classified_type
FROM identifier('openalex' || :env_suffix || '.works.locations_w_sources') l
JOIN scored sc ON sc.work_id = concat_ws('~', l.provenance, l.native_id_namespace, l.native_id)
);

-- COMMAND ----------

SELECT classified_type, count(*) AS n
FROM identifier('openalex' || :env_suffix || '.works.locations_with_types')
GROUP BY 1 ORDER BY n DESC LIMIT 30;
