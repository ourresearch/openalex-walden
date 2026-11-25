-- Databricks notebook source
-- Title second pass, daily (oxjob #2596 fix 2). The reference matcher (#2000/#2296) declines a printed line whose record
-- is another version of the cited work (an "In ICLR 2018" line whose only record is the arXiv preprint). This pass links
-- such lines on works with an arXiv location when the work's full title is printed, the line starts with its first author
-- and a printed year is within one; rule v7, judged per type on fresh draws: conference papers 100/100, preprints 100/100,
-- articles 395/400, books 100/100 (#2596 EXPLORE § Fix 1). The 24 Nov backlog ran once by hand (run title-2596-2026-11-24).
--
-- Each run: lines of lists filed since 3 days before the last registered run (at most 14 days back), already seen by the
-- nightly matcher, still unlinked and with no link pending in another unapplied run, go into
-- openalex.works.reference_matcher_links under a run id of their own (title-daily-<UTC timestamp>), registered only after
-- its links are written. The next End 2 End's parse_referenced_works applies it like a matcher run: a link fills its line
-- only if the line is still unlinked, still prints the same text (text hash) and the cited work is live. A line an applied
-- run left unlinked is proposed again while its list is in the window.
-- Scope is by citing work: a newly filed list brings the work's other unlinked lines too (same rule, same answer).
-- A run never appends to an earlier run, so a retry or a second run in a day mints a new id. Links of an attempt that
-- failed before registering are deleted at the start of the next attempt: the parse step never applies unregistered links,
-- but the matcher's same-text reuse reads every link.
-- Revert: pause this job (Jobs UI, then jobs/title_second_pass.yaml), make sure neither it nor End 2 End is running, then
-- run notebooks/references/title_second_pass/title_daily_revert.sql.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE tp_run STRING DEFAULT concat('title-daily-', date_format(current_timestamp(), 'yyyyMMdd-HHmmss'))

-- COMMAND ----------

-- Lists filed after the nightly matcher's latest applied run have not been seen by it yet: they wait for the next run.
DECLARE OR REPLACE VARIABLE tp_to TIMESTAMP

-- COMMAND ----------

SET VAR tp_to = (SELECT max(applied_at) FROM openalex.works.reference_matcher_runs WHERE run_id LIKE 'live-%')

-- COMMAND ----------

-- From 3 days before the last registered daily run (so missed days are caught up), never more than 14 days back.
DECLARE OR REPLACE VARIABLE tp_from TIMESTAMP

-- COMMAND ----------

SET VAR tp_from = (
  SELECT greatest(current_timestamp() - INTERVAL 14 DAYS,
                  least(current_timestamp() - INTERVAL 3 DAYS,
                        coalesce(max(to_timestamp(substring(run_id, 13), 'yyyyMMdd-HHmmss')), current_timestamp()) - INTERVAL 3 DAYS))
  FROM openalex.works.reference_matcher_runs WHERE run_id LIKE 'title-daily-%')

-- COMMAND ----------

-- Links of an earlier attempt that failed before registering its run.
DELETE FROM openalex.works.reference_matcher_links
WHERE run_id LIKE 'title-daily-%'
  AND run_id NOT IN (SELECT run_id FROM openalex.works.reference_matcher_runs WHERE run_id LIKE 'title-daily-%')

-- COMMAND ----------

-- The matcher's runs are named live-<date> (#2296); if none applied in 3 days, stop loudly rather than link nothing.
BEGIN
  IF tp_to IS NULL OR tp_to < current_timestamp() - INTERVAL 3 DAYS THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'NO_RECENT_MATCHER_RUN: no live-% matcher run applied in 3 days; nothing written';
  END IF;
END

-- COMMAND ----------

INSERT INTO openalex.works.reference_matcher_links
  (citing_work_id, native_id, ref_ind, text_hash, cited_work_id, score, in_band, linked_by, run_id, decided_at)
WITH scope AS (
  SELECT DISTINCT l.citing_work_id, w.publication_year AS citing_year
  FROM openalex.works.work_reference_lists l
  JOIN openalex.works.openalex_works w ON w.id = l.citing_work_id
  WHERE l.filed_at >= tp_from AND l.filed_at <= tp_to
    AND exists(w.locations, x -> x.source.id = 'https://openalex.org/S4306400194')
),
lines AS (
  SELECT wr.citing_work_id, s.citing_year, wr.native_id, wr.ref_ind, wr.raw,
         -- the matcher's text key, from the same row the match reads (the same expression as parse_referenced_works)
         xxhash64(left(trim(regexp_replace(regexp_replace(coalesce(nullif(trim(wr.raw), ''), concat_ws('. ', wr.authors, wr.year, wr.title)), '(?i)(https?://)?(dx\\.)?(doi\\.org/|doi:?\\s*)?10\\.\\d{4,9}/\\S+', ' '), '(?i)https?://\\S+|www\\.\\S+', ' ')), 1000)) AS text_hash,
         -- PDF line breaks split words as "bottle- neck": join them for matching (backreferences via chr(36): a job run
         -- blanks a literal dollar, oxjob #1932)
         regexp_replace(wr.raw, '(\\p{L})- (\\p{L})', concat(chr(36), '1', chr(36), '2')) AS raw_j
  FROM openalex.works.work_references wr
  JOIN scope s ON s.citing_work_id = wr.citing_work_id
  WHERE wr.cited_work_id IS NULL AND nullif(trim(wr.raw), '') IS NOT NULL
    AND nullif(trim(wr.doi), '') IS NULL AND wr.parsed_doi IS NULL AND wr.pmid IS NULL AND wr.arxiv IS NULL
    -- a printed DOI or arXiv id the parser missed: the id path owns the line, not a title match
    AND NOT wr.raw RLIKE '(?i)10\\.\\d{4,9}/|arxiv[:\\s/]*([a-z\\-]+/)?\\d{4}\\.?\\d{3,5}'
    -- an erratum or corrigendum cites the correction notice, not the work its title names
    AND NOT wr.raw RLIKE '(?i)erratum|corrigendum|correction to|addendum'
    -- a thesis: its same-titled article or preprint is a different work, and dissertations aren't in the ship set
    AND NOT wr.raw RLIKE '(?i)thesis|dissertation|ph\\.?\\s?d\\b|doctoral|habilitation'
),
lines2 AS (
  SELECT *,
         trim(regexp_replace(lower(raw_j), '[^\\p{L}\\p{N}]+', ' ')) AS words,
         -- the first author's name: the text before the first comma, semicolon, " and " or " & " (at most 60 letters)
         trim(regexp_replace(lower(regexp_extract(raw_j, '^\\s*([^,;]{2,60}?)(,|;|\\s+and\\s|\\s&\\s)', 1)),
                             '[^\\p{L}\\p{N}]+', ' ')) AS first_chunk,
         -- journal evidence: a volume and page printed ("2, 011008", "vol. 34", "34(10)", "118:8207")
         raw_j RLIKE '(?i)\\bvol\\.?\\s*\\d+|\\b\\d{1,4}\\s*\\(\\d{1,4}\\)|\\b\\d{1,4}\\s*[,:]\\s*[A-Z]?\\d{2,6}\\b' AS vol_page,
         -- venue evidence comes from the line without its URLs (the title is removed per candidate below)
         trim(regexp_replace(lower(regexp_replace(raw_j, '(?i)https?://\\S+|www\\.\\S+', ' ')), '[^\\p{L}\\p{N}]+', ' ')) AS words_nourl
  FROM lines
),
segs AS (  -- title-sized pieces: text between sentence marks or quotes, and each piece joined with the next
  SELECT citing_work_id, native_id, ref_ind, posexplode(split(raw_j, '[.?!]\\s+|["“”«»]')) AS (pos, seg) FROM lines2
),
segs2 AS (
  SELECT citing_work_id, native_id, ref_ind, seg FROM segs
  UNION ALL
  SELECT a.citing_work_id, a.native_id, a.ref_ind, concat(a.seg, ' ', b.seg)
  FROM segs a JOIN segs b ON a.citing_work_id = b.citing_work_id AND a.native_id = b.native_id
    AND a.ref_ind = b.ref_ind AND b.pos = a.pos + 1
),
keys AS (
  SELECT DISTINCT citing_work_id, native_id, ref_ind, regexp_replace(lower(seg), '[^\\p{L}\\p{N}]', '') AS k
  FROM segs2
  WHERE length(regexp_replace(lower(seg), '[^\\p{L}\\p{N}]', '')) >= 25
),
works AS (
  SELECT id, title, publication_year, type, cited_by_count,
         try_cast(biblio.first_page AS INT) AS fp, try_cast(biblio.last_page AS INT) AS lp, biblio.volume AS vol,
         regexp_replace(lower(title), '[^\\p{L}\\p{N}]', '') AS k,
         trim(regexp_replace(lower(title), '[^\\p{L}\\p{N}]+', ' ')) AS title_words,
         lower(element_at(split(trim(regexp_replace(coalesce(get(authorships, 0).author.display_name,
                                                             get(authorships, 0).raw_author_name, ''),
                                                    '[^\\p{L}\\p{N} ]+', ' ')), '\\s+'), -1)) AS surname
  FROM openalex.works.openalex_works
  WHERE title IS NOT NULL AND NOT coalesce(is_paratext, false)
    AND type NOT IN ('erratum', 'retraction', 'paratext', 'book-review', 'peer-review', 'supplementary-materials')
),
cand AS (
  SELECT l.citing_work_id, l.native_id, l.ref_ind, l.text_hash, l.raw, l.raw_j, l.words, l.vol_page, w.id AS cand,
         w.k AS title_key, w.type, w.publication_year, w.cited_by_count, w.title_words,
         -- the line's venue words: URLs and this candidate's title removed
         trim(replace(concat(' ', l.words_nourl, ' '), concat(' ', w.title_words, ' '), ' ')) AS venue_words
  FROM keys k
  JOIN works w ON w.k = k.k
  JOIN lines2 l ON l.citing_work_id = k.citing_work_id AND l.native_id = k.native_id AND l.ref_ind = k.ref_ind
  WHERE size(split(w.title_words, ' ')) >= 4 AND length(w.k) >= 25 AND w.id <> l.citing_work_id
    AND length(w.surname) >= 2
    -- never a work two or more years newer than the citing work (eligibility, before ranking)
    AND w.publication_year <= coalesce(l.citing_year, 9999) + 1
    -- the line starts with the first author: the surname is in the line's first name chunk (else its first four words)
    AND array_contains(split(CASE WHEN l.first_chunk <> '' THEN l.first_chunk
                                  ELSE array_join(slice(split(l.words, ' '), 1, 4), ' ') END, ' '), w.surname)
    -- a printed year within one of the work's year
    AND exists(regexp_extract_all(l.raw_j, '(?<![0-9])(1[5-9][0-9][0-9]|20[0-9][0-9])(?![0-9])', 1),
               y -> abs(cast(y AS INT) - w.publication_year) <= 1)
    -- the title is the container the cited chapter sits in: not this work
    AND NOT concat(' ', l.words, ' ') RLIKE concat(
          '\\s(in|ed|eds|editor|editors|edited\\sby)\\s(the\\sbook\\s|collect\\s|collected\\s|collection\\s|proceedings\\sof\\s)?',
          w.title_words, '\\s')
    -- a journal item (review, notice) carrying a book's title, when the line cites a published book
    AND NOT (w.type IN ('article', 'review', 'letter', 'editorial', 'other')
             AND lower(l.raw_j) RLIKE '(university press|\\bpress\\b|publish|verlag|springer|wiley|elsevier|dekker|pearson|prentice|addison|mcgraw|clarendon|birkh|routledge|crc |taylor (and|&) francis|world scientific|dover|north.holland|kluwer|\\bed\\.|edition|isbn|penguin|random house|harper|norton|basic books|sagepub|/book)'
             AND NOT lower(l.raw_j) RLIKE '(journal|\\bj\\.|transactions|proceedings|letters|review|\\bvol|\\d+\\s*\\(\\d+\\))')
    -- a same-titled chapter when the line cites a whole book (no container, pages or proceedings printed)
    AND NOT (w.type = 'book-chapter'
             AND NOT lower(l.raw_j) RLIKE '(\\bin\\b|\\bpp\\b|pages|chapter|proceedings|conference|workshop|symposium|lecture notes|\\bvol)')
    -- a one-page journal item (a book review or notice) when the line prints neither its page nor its volume
    AND NOT (w.type IN ('article', 'review', 'letter', 'editorial', 'other') AND w.fp IS NOT NULL
             AND coalesce(w.lp, w.fp) >= w.fp AND coalesce(w.lp, w.fp) - w.fp <= 1
             AND NOT array_contains(split(l.words, ' '), cast(w.fp AS STRING))
             AND NOT (length(w.vol) >= 2 AND array_contains(split(l.words, ' '), lower(w.vol))))
    -- a data deposit carrying an article's title, unless the line cites data
    AND NOT (w.type = 'dataset' AND NOT lower(l.raw_j) RLIKE '(data|zenodo|figshare|dryad|repository|github|software)')
),
classed AS (
  SELECT c.*,
    CASE WHEN venue_words RLIKE '\\b(arxiv|preprint|corr)\\b' THEN 'arxiv'
         WHEN venue_words RLIKE '\\b(proceedings|proc|conference|conf|workshop|symposium|symp|neural information processing|neurips|nips|icml|iclr|cvpr|iccv|eccv|aaai|ijcai|acl|emnlp|naacl|kdd|sigir|uai|aistats|colt|interspeech|icassp|stoc|focs|soda|icra|iros|miccai|osdi|sosp|pldi|eurocrypt|crypto)\\b' THEN 'conference'
         WHEN venue_words RLIKE '\\b(journal|transactions|letters|review|reviews|annals|bulletin)\\b' OR vol_page THEN 'journal'
         ELSE 'none' END AS venue
  FROM cand c
),
eligible AS (
  -- a line naming a conference never links a journal article: the later journal version is an extended, different work
  -- (v5) and a line citing a journal never links a conference paper of the same title
  SELECT * FROM classed WHERE NOT (venue = 'conference' AND type IN ('article', 'review', 'letter'))
                          AND NOT (venue = 'journal' AND type = 'conference-paper')
    -- (v7) a line naming no venue links an article only in its exact year (a book review comes out the year after)
    AND NOT (type IN ('article', 'review', 'letter') AND venue = 'none'
             AND NOT exists(regexp_extract_all(raw_j, '(?<![0-9])(1[5-9][0-9][0-9]|20[0-9][0-9])(?![0-9])', 1),
                            y -> cast(y AS INT) = publication_year))
    -- (v6) a book only for a line that names no journal or conference
    AND NOT (type = 'book' AND venue IN ('journal', 'conference'))
    -- (v6) one volume, part or tome of a multi-volume work, when the record's title doesn't name that part
    AND NOT (type IN ('book', 'article') AND venue <> 'journal'
             AND venue_words RLIKE '\\b(vol|volume|tome|part|fasc|band|teil)\\s+([ivx]+|\\d+)\\b'
             AND NOT title_words RLIKE '\\b(vol|volume|tome|part|fasc|band|teil)\\s+([ivx]+|\\d+)\\b')
),
picked AS (
  SELECT e.*, size(collect_set(title_key) OVER (PARTITION BY e.citing_work_id, e.native_id, e.ref_ind)) AS n_titles,
         row_number() OVER (PARTITION BY e.citing_work_id, e.native_id, e.ref_ind ORDER BY
           -- Q3: the record the line describes
           CASE WHEN venue = 'arxiv' THEN (CASE WHEN type = 'preprint' THEN 0 ELSE 1 END)
                WHEN venue = 'conference' THEN (CASE WHEN type IN ('conference-paper', 'book-chapter') THEN 0 ELSE 1 END)
                WHEN venue = 'journal' THEN (CASE WHEN type IN ('article', 'review', 'letter') THEN 0
                                                  WHEN type = 'preprint' THEN 2 ELSE 1 END)
                ELSE (CASE WHEN type = 'preprint' THEN 1 ELSE 0 END) END,
           e.cited_by_count DESC, e.cand) AS rn
  FROM eligible e
),
out AS (
  SELECT p.*, count(*) OVER () AS total_n
  FROM picked p
  WHERE p.rn = 1 AND p.n_titles = 1
    -- ship set: the cited types judged >= 97% on a fresh draw (filtered after the pick, so an excluded winner abstains)
    AND p.type IN ('conference-paper', 'preprint', 'article', 'book')
    -- a link already pending for this line and text in a registered, unapplied run (any matcher or title run): leave it.
    -- An applied run that left the line unlinked doesn't block a new proposal.
    AND NOT EXISTS (SELECT 1 FROM openalex.works.reference_matcher_links x
                    JOIN openalex.works.reference_matcher_runs r ON r.run_id = x.run_id AND r.applied_at IS NULL
                    WHERE x.native_id = p.native_id AND x.ref_ind = p.ref_ind AND x.text_hash = p.text_hash)
)
SELECT citing_work_id, native_id, ref_ind, text_hash, cand, CAST(NULL AS DOUBLE), false, 'title_rule', tp_run,
       current_timestamp()
FROM out
-- a runaway run (a re-harvest, a broken scope) writes nothing and fails loudly. A normal day is a few thousand lines;
-- the 24 Nov backlog of every arXiv work was 3.95M.
WHERE CASE WHEN total_n > 300000 THEN raise_error('TOO_MANY_LINKS: over 300,000 lines in one daily run; nothing written')
           ELSE true END

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE tp_n BIGINT

-- COMMAND ----------

SET VAR tp_n = (SELECT count(*) FROM openalex.works.reference_matcher_links WHERE run_id = tp_run)

-- COMMAND ----------

-- Register the run only now that its links are written.
BEGIN
  IF tp_n > 0 THEN
    INSERT INTO openalex.works.reference_matcher_runs (run_id, applied_at) VALUES (tp_run, CAST(NULL AS TIMESTAMP));
  END IF;
END

-- COMMAND ----------

SELECT tp_run AS run_id, tp_n AS links_written, tp_from AS lists_filed_from, tp_to AS lists_filed_to
