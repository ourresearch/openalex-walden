-- #1348 Vector_Pool: tonight's seats with their vectors (vector_match_night) and the candidate pool (vector_match_pool): CAP most-recent stored
-- seats of every profile in the touched small + medium blocks, from openalex.authors.seat_embeddings_live (#1342's 09-24 store + nightly vectors), tonight's works excluded.
-- Runs on the serverless warehouse (69a583ace3bdc8d0) after Vector_Embed; ~5-6 min for an 800K-seat night (104M pool rows).
-- Block tier = profiles in the block per authors_for_matching (small <= 249, medium <= 5000, mega beyond: cascade only).
CREATE OR REPLACE TABLE openalex.authors.vector_match_night CLUSTER BY (block_key) AS
WITH bs AS (SELECT block_key, COUNT(*) block_size FROM openalex.authors.authors_for_matching GROUP BY 1),
pn AS (SELECT c.work_id, c.author_sequence, c.raw_name, p.parsed_name.first AS pn_first,
              CASE WHEN p.parsed_name.last IS NULL THEN NULL WHEN COALESCE(SUBSTRING(p.parsed_name.first, 1, 1), '') = '' THEN p.parsed_name.last
                   ELSE CONCAT(SUBSTRING(p.parsed_name.first, 1, 1), ' ', p.parsed_name.last) END AS block_key
       FROM openalex.authors.vector_match_cards c LEFT JOIN openalex.authors.author_names p ON TRIM(c.raw_name) = p.raw_author_name)
SELECT pn.work_id, pn.author_sequence, pn.block_key, bs.block_size, CASE WHEN bs.block_size <= 249 THEN 'small' WHEN bs.block_size <= 5000 THEN 'medium' ELSE 'mega' END AS tier,
       pn.raw_name, pn.pn_first, CAST(NULL AS STRING) AS match_outcome, CAST(NULL AS BIGINT) AS name_author_id, CAST(NULL AS BIGINT) AS orcid_author_id, CAST(NULL AS BIGINT) AS existing_author_id,
       e.ft_q, e.ft_s, e.st_q, e.st_s
FROM pn JOIN openalex.authors.seat_embeddings_daily e ON e.run_date = current_date() AND e.work_id = pn.work_id AND e.author_sequence = pn.author_sequence
LEFT JOIN bs USING (block_key) WHERE pn.block_key IS NOT NULL;
CREATE OR REPLACE TABLE openalex.authors.vector_match_pool CLUSTER BY (block_key) AS
WITH nb AS (SELECT DISTINCT block_key FROM openalex.authors.vector_match_night WHERE tier <> 'mega'),
p AS (SELECT a.author_id, a.block_key, a.first AS prof_first, a.works_count FROM openalex.authors.authors_for_matching a JOIN nb USING (block_key)),
nw AS (SELECT DISTINCT work_id FROM openalex.authors.vector_match_cards),
k AS (SELECT p.*, wa.work_id, CAST(wa.author_sequence AS INT) AS author_sequence, w.publication_year
      FROM p JOIN openalex.works.work_authors wa ON wa.author_id = p.author_id LEFT ANTI JOIN nw ON nw.work_id = wa.work_id
      JOIN openalex.works.openalex_works w ON w.id = wa.work_id),
kr AS (SELECT * FROM (SELECT k.*, ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY publication_year DESC NULLS LAST, work_id) rn FROM k) WHERE rn <= 20)
SELECT kr.block_key, kr.author_id, kr.prof_first, kr.works_count, kr.work_id, kr.author_sequence, kr.publication_year, e.ft_q, e.ft_s, e.st_q, e.st_s
FROM kr JOIN openalex.authors.seat_embeddings_live e ON e.work_id = kr.work_id AND e.author_sequence = kr.author_sequence;
