-- Investigator authors (oxjob #1847): the one definition of "same name" (same-person rule 2; cases in the tests), used by every step.
-- Profile side: openalex.authors.author_names (the parser's readings: both orders, particles, suffixes, romanisations). Grant side:
-- name_tokens below (grants print given and family apart, so no order is guessed). names_agree is stricter than
-- openalex.authors.names_compatible on purpose: Ken is not Kenji, John is not James, Smith is not Smithson, Müller is Mueller.
CREATE OR REPLACE FUNCTION __P__name_tokens(s STRING) RETURNS ARRAY<STRING>
COMMENT 'oxjob #1847: name -> folded a-z tokens (marks, accents, ss/ae/oe, ae/oe/ue -> a/o/u), titles and suffixes dropped'
RETURN transform(filter(split(trim(regexp_replace(translate(replace(replace(replace(regexp_replace(regexp_replace(lower(coalesce(s, '')),
    '\\b(a|em|assoc|adj|clin)/(pr|prof|professor)\\b', ' '), '[\\p{M}\\p{Cf}]+', ''), 'ß', 'ss'), 'æ', 'ae'), 'œ', 'oe'),
    'áàäâãåāăąéèëêēėęěíìïîīįıóòöôõøōőúùüûūůűųñńňçćčšśşșžźżýÿłđðřťțďľĺŕğ',
    'aaaaaaaaaeeeeeeeeiiiiiiioooooooouuuuuuuunnncccsssszzzyylddrttdllrg'), '[^a-z]+', ' ')), ' '),
  t -> t <> '' AND NOT array_contains(array('prof', 'professor', 'dr', 'mr', 'mrs', 'ms', 'miss', 'mx', 'sir', 'dame', 'rev', 'hon',
    'honorary', 'assoc', 'associate', 'asst', 'assistant', 'adj', 'adjunct', 'emeritus', 'jr', 'sr', 'ii', 'iii', 'iv', 'phd', 'md'), t)),
  t -> replace(replace(replace(t, 'ae', 'a'), 'oe', 'o'), 'ue', 'u'))
;
CREATE OR REPLACE FUNCTION __P__family_keys(s STRING) RETURNS ARRAY<STRING>
COMMENT 'oxjob #1847: family name -> its keys: all tokens joined (garcialopez, vanderberg) and each token, 2+ letters, no particles'
RETURN filter(array_distinct(concat(array(array_join(__P__name_tokens(s), '')), __P__name_tokens(s))),
  k -> length(k) >= 2 AND NOT array_contains(array('de', 'da', 'do', 'dos', 'das', 'del', 'della', 'di', 'du', 'van', 'von', 'der',
    'den', 'ter', 'zu', 'la', 'le', 'el', 'al', 'bin', 'ben', 'mac'), k))
;
CREATE OR REPLACE FUNCTION __P__names_agree(a_fam ARRAY<STRING>, a_given STRING, b_fam ARRAY<STRING>, b_given STRING) RETURNS BOOLEAN
COMMENT 'oxjob #1847 same-person rule 2: a family key in common, and first given names equal or one the initial of the other (Ken <> Kenji)'
RETURN arrays_overlap(a_fam, b_fam) AND (coalesce(a_given, '') = '' OR coalesce(b_given, '') = '' OR a_given = b_given
  OR (length(a_given) = 1 AND left(b_given, 1) = a_given) OR (length(b_given) = 1 AND left(a_given, 1) = b_given))
;
CREATE OR REPLACE FUNCTION __P__romaji_fold(s STRING) RETURNS STRING
COMMENT 'oxjob #1847: a romaji token folded so Hepburn, Kunrei and long vowels meet (Shimpei = Shinpei, Kohji = Koji; Yohei stays)'
RETURN replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
  replace(replace(replace(replace(replace(coalesce(s, ''), 'tsu', 'tu'), 'shi', 'si'), 'chi', 'ti'), 'sh', 'sy'), 'ch', 'ty'), 'fu', 'hu'),
  'ji', 'zi'), 'j', 'zy'), 'ou', 'o'), 'oo', 'o'), 'uu', 'u'),
  'oha', 'o#a'), 'ohe', 'o#e'), 'ohi', 'o#i'), 'oho', 'o#o'), 'ohu', 'o#u'), 'oh', 'o'), '#', 'h'),
  'mb', 'nb'), 'mm', 'nm'), 'mp', 'np')
;
-- Every live profile's name readings as keys: display name + up to 10 alternatives, each through author_names (the parse and
-- every alternate reading), one row per (author, family key, first given token).
CREATE OR REPLACE TABLE __P__profile_names CLUSTER BY (last_key) AS
WITH v AS (
  SELECT DISTINCT try_cast(regexp_extract(a.id, '([0-9]+)$', 1) AS BIGINT) AS author_id, trim(n) AS raw
  FROM openalex.authors.authors_api a
  LATERAL VIEW explode(concat(array(a.display_name), slice(coalesce(a.display_name_alternatives, array()), 1, 10))) t AS n
  WHERE a.works_count > 0 AND trim(coalesce(n, '')) <> ''
),
r AS (
  SELECT v.author_id, get(__P__name_tokens(x.first), 0) AS first_key, __P__family_keys(x.last) AS ks
  FROM v JOIN openalex.authors.author_names an ON an.raw_author_name = v.raw
  LATERAL VIEW explode(concat(array(named_struct('first', an.match_first, 'last', an.match_last)),
    transform(coalesce(an.alt_parses, array()), p -> named_struct('first', p.first, 'last', p.last)))) t AS x
)
SELECT DISTINCT author_id, k AS last_key, first_key FROM r LATERAL VIEW explode(ks) t AS k
