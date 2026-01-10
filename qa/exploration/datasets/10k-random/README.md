# 10k Random OpenAlex Sample

This directory contains data for evaluating metadata extraction from 10,000 randomly sampled OpenAlex works.

## Seed File
- `openalex_random_ids.txt` - 10,000 random OpenAlex work IDs

## Generated TSV Files
- `openalex.tsv` - OpenAlex metadata (openalex_id, year, doi, data)
- `parseland.tsv` - Parseland parsed data (openalex_id, html_uuid, data)
- `openai.tsv` - OpenAI extracted data (openalex_id, tokens_up, tokens_down, data)

## How to Generate

Run from the `scripts/` directory:

```bash
# Step 1: Fetch OpenAlex data
python get_openalex.py ../10k_random/openalex_random_ids.txt ../10k_random/openalex.tsv

# Step 2: Get Parseland data (DOI → Taxicab → html_uuid → Parseland)
python get_parseland.py ../10k_random/openalex.tsv ../10k_random/parseland.tsv

# Step 3: Get OpenAI responses (html_uuid → Taxicab HTML → OpenAI)
python get_openai.py ../10k_random/parseland.tsv ../10k_random/openai.tsv --workers 10
```

All scripts support checkpointing - you can stop and resume at any time.
