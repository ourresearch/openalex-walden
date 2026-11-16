-- Copied from oxjobs junk-entries-in-reference-lists work/10_rules_v2.sql (oxjob #2188), unchanged; the reference matcher skips
-- the classes MatchReferences.py names in SKIP (the same skip #2000's corpus run used).
-- #2188 v2 of #2000's junk rules (reference-matcher/work/21_ref_junk_rules.sql), tightened on 2,300 judged rows (12 Nov).
-- Same CTE contract: plug in after a `src` CTE with provenance, title, authors, text, from_fields. First match wins.
-- Changes from v1: headings take an enumerator prefix and more languages; more link labels; a TOC line that starts with a
-- dated reference (initials, URL or "retrieved") is not a TOC line; boilerplate is only a publisher's note, an author
-- biography or an e-mail line, none carrying a dated reference (copyright and acknowledgement openers caught real ones); index entries split into glued entries (index_entry) and a single entry ending in page numbers
-- (index_or_short_cite, kept: humanities short-form footnotes look the same); an MDPI disclaimer glued to a reference is
-- flagged for trimming (f_trim_disclaimer) instead of filed as boilerplate.
WITH src AS (SELECT 1),
b AS (
  SELECT s.*,
    trim(coalesce(text, '')) AS t,
    length(regexp_replace(coalesce(text, ''), '[^\\p{L}]', '')) AS letters,
    lower(trim(regexp_replace(coalesce(text, ''), '[^\\p{L}\\p{N}]+', ' '))) AS tn,
    length(regexp_replace(regexp_replace(regexp_replace(coalesce(text, ''), '(?i)(https?:?//?|www\\.)\\S+', ' '), '10\\.\\d{4,9}/\\S+', ' '), '[^\\p{L}]', '')) AS letters_nourl,
    size(regexp_extract_all(coalesce(text, ''), '(?<![0-9])(1[5-9][0-9]{2}|20[0-2][0-9])(?![0-9])', 0)) AS n_years,
    size(regexp_extract_all(coalesce(text, ''), '(?<![0-9])[0-9]+(?![0-9])', 0)) AS n_numgroups,
    length(regexp_replace(coalesce(text, ''), '(?i)(https?:?//?|www\\.)\\S+', '')) AS len_nourl,
    size(regexp_extract_all(coalesce(text, ''), '\\p{Lu}[\\p{L}''-]+,?\\s+(\\p{Lu}\\.\\s?-?){1,3}', 0)) AS n_initials,
    regexp_extract(coalesce(text, ''), '^(.*?)((\\.\\s*){6,}|(…\\s*){3,}|(·\\s*){6,})', 1) AS toc_head
  FROM src s
), c AS (
  SELECT b.*, CASE
    WHEN provenance = 'datacite' AND letters_nourl < 10 THEN 'datacite_url_only'
    WHEN from_fields AND nullif(trim(title), '') IS NULL AND nullif(trim(authors), '') IS NULL THEN 'crossref_fields_year_only'
    WHEN from_fields AND nullif(trim(title), '') IS NULL THEN 'crossref_fields_author_year'
    WHEN letters = 0 THEN 'no_letters'
    WHEN tn RLIKE '^(([0-9]+|[ivx]+|[a-z])\\s)?(references?|referencias?|referências|referências bibliográficas|referencias bibliográficas|referencias bibliograficas|references cited|reference list|list of references|references and notes|notes and references|bibliographic references|bibliography|bibliographie|bibliografia|bibliografía|bibliograficas|bibliográficas|bibliografía consultada|bibliographies|literatur|literaturverzeichnis|literatura|literature|literature cited|literatuur|literatuurlijst|works cited|other works cited|cited literature|sources|primary sources|secondary sources|archival sources|other sources|online sources|internet sources|web sources|electronic sources|notes|endnotes|footnotes|fuentes|quellen|daftar|daftar pustaka|daftar rujukan|daftar isi|pustaka|lähteet|kirjallisuus|sisällys|kaynakça|kaynaklar|çevrimiçi kaynaklar|internet kaynakları|litteratur|litteraturliste|referenser|källförteckning|källor|kilder|kildeliste|heimildaskrá|heimildir|riferimenti|riferimenti bibliografici|bibliografie|źródła|zdroje|список литературы|литература|список использованной литературы|spisok literatury|література|список літератури|参考文献|引用文献|外国語文献|日本語文献|欧文文献|문헌|참고문헌|sitography|sitografia|webgrafia|webography|further reading|suggested reading|appendix|annex|index|índice|indice|sommaire|inhaltsverzeichnis|contents|table of contents|books|articles|websites|legislation|cases|statutes)$' THEN 'section_heading'
    WHEN tn RLIKE '^((google scholar|crossref|cross ref|pubmed|pubmed central|pmc free article|pmc|cas|web of science|wos|isi|scopus|medline|full text|free full text|publisher full text|view article|view at publisher|publisher site|find this article|search google scholar|open url|openurl|link|links|article|abstract|pdf|html|doi|ref|google|ads|nasa ads|inspire|mathscinet|zbmath|chemport|jstor|europe pmc|semantic scholar|n a|na|none|null|nil|unknown|tbd|xxx)\\s*)+$' THEN 'link_label_placeholder'
    WHEN t RLIKE '(\\.\\s*){6,}|(…\\s*){3,}|(·\\s*){6,}'
      AND NOT (toc_head RLIKE '(?<![0-9])(1[5-9][0-9]{2}|20[0-2][0-9])(?![0-9])'
               AND (toc_head RLIKE '\\p{Lu}[\\p{L}''-]+,?\\s+(\\p{Lu}\\.\\s?-?){1,3}' OR toc_head RLIKE '(?i)https?:|www\\.|retrieved|accessed|available'))
      THEN 'toc_dot_leaders'
    WHEN (tn RLIKE '^(disclaimer publisher s note|publisher s note|author biograph)'
          OR t RLIKE '(?i)^\\s*(e-?\\s?mail|email address)[^:]{0,20}:\\s*\\S*[\\w.+-]+@[\\w-]+\\.[\\w.]+'
          OR (t RLIKE '[\\w.+-]+@[\\w-]+\\.[A-Za-z]{2,}' AND NOT t RLIKE '@/' AND letters < 40 AND n_years = 0))
      AND NOT (n_years >= 1 AND n_initials >= 1) THEN 'boilerplate_bio_email'
    WHEN provenance <> 'datacite' AND letters_nourl < 10 AND letters >= 10 THEN 'url_only'
    WHEN (tn RLIKE '^(ibid|ibidem|ebd|ebenda|ivi|tamže|там же|同上|id|idem|eadem|ders|dies)( |$)' AND letters < 40)
      OR (t RLIKE '(?i)\\b(op\\.? ?cit|loc\\.? ?cit|art\\.? ?cit|ob\\.? ?cit|supra,? (note|n\\.)|a\\.\\s?a\\.\\s?O\\.|wie Anm)' AND letters < 60) THEN 'ibid_back_reference'
    WHEN tn RLIKE '\\b(personal communication|pers comm|private communication|oral communication|personal correspondence|unpublished (data|results|observations?)|data not shown|interview(ed)? (with|by)|entrevista (com|con|a)|com pers)\\b' THEN 'personal_comm_unpublished'
    WHEN n_years = 0 AND letters < 80 AND t RLIKE '[0-9]{1,4},?\\s+\\p{Lu}[\\p{L}''-]+,\\s+\\p{Lu}[\\p{L}.]+,?\\s+[0-9]'
      AND NOT t RLIKE '(?i)\\b(vol|no|pp?|j|journal|rev|proc|phys|chem|ann|lett|acta|bull|in)\\b\\.?' THEN 'index_entry'
    WHEN n_years = 0 AND letters < 80 AND t RLIKE '(,\\s*[0-9]{1,4}[a-zA-Z]?){3,}\\s*[.;]?\\s*$'
      AND NOT t RLIKE '(?i)\\b(vol|no|pp?|j|journal|rev|proc|phys|chem|ann|lett|acta|bull|in)\\b\\.?' THEN 'index_or_short_cite'
    WHEN len_nourl > 600 AND n_years <= 2 AND n_initials >= 10 THEN 'looks_like_reference'
    WHEN len_nourl > 600 AND n_years >= 3 AND n_initials >= 6 THEN 'long_glued_refs'
    WHEN len_nourl > 600 THEN 'long_prose_or_table'
    WHEN letters < 20 AND n_numgroups - n_years < 2 THEN 'too_short'
    ELSE 'looks_like_reference' END AS cls,
    t RLIKE '(?i)Disclaimer/Publisher.s Note:' AND NOT tn RLIKE '^disclaimer publisher s note' AS f_trim_disclaimer,
    t RLIKE '\\x{FFFD}|\\(cid:[0-9]+\\)|Ã[\\x{80}-\\x{BF}]|â€' AS f_garbled,
    t RLIKE '<\\s*/?\\s*[a-zA-Z][a-zA-Z0-9:_-]*[^>]*>|&(amp|lt|gt|quot|apos|nbsp|#[0-9]+|#x[0-9a-fA-F]+);' AS f_markup
  FROM b
)
SELECT 1
