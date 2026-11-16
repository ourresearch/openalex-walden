-- #2000 reference text key: the ONE definition of the text each reference is embedded and matched by (12 Nov 2026).
-- The backfill and the nightly matcher both read this file (Python: ref_text_key.py parses it), so the hash can never
-- differ between them (Casey, 12 Nov: the hash is computed on the fly; work_references gets no new column).
--
-- text = the form the tested matcher embedded on real unlinked references (18_rd_sample.sql: the reader's training draw
-- and the review bench): the printed string, or for an entry with no printed string its fields as "authors. year. title";
-- DOIs and URLs removed; trimmed; the first 1,000 characters. Columns raw, authors, year, title of work_references.
-- text_hash = xxhash64(text). openalex.vector_search.reference_embeddings_qwen3 stores text without the instruction;
-- the instruction is prepended at embed time only (the tested stack's 'api' query variant).
-- Changing either expression changes every key: every reference would be embedded again.
--
-- @text
left(trim(regexp_replace(regexp_replace(coalesce(nullif(trim(raw), ''), concat_ws('. ', authors, year, title)), '(?i)(https?://)?(dx\\.)?(doi\\.org/|doi:?\\s*)?10\\.\\d{4,9}/\\S+', ' '), '(?i)https?://\\S+|www\\.\\S+', ' ')), 1000)
-- @instruction
'Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: '
