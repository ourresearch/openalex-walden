OpenAlex daily snapshot
=======================
The complete OpenAlex database as downloadable files, rebuilt and published
every day into its own dated folder (full/YYYY-MM-DD/).

Data:      jsonl/ and parquet/  (two complete copies, partitioned by updated_date)
Manifests: jsonl/manifest.json, parquet/manifest.json  (written after the data files; present = this day's data files are complete)
Deletions: jsonl/works/deleted_ids.csv.gz, parquet/works/deleted_ids.csv.gz  (gzip-compressed CSV; cumulative log of deleted work IDs)
Merges:    {jsonl,parquet}/{works,authors,sources}/merged_ids.csv.gz  (merged-away ID -> live ID; written up to ~10 min after the manifests)
History:   RELEASE_NOTES.txt
License:   CC0, same as the public snapshot (https://openalex.s3.amazonaws.com/LICENSE.txt)

The free public snapshot at s3://openalex/data/ is a quarterly release of the same layout.

Docs: https://help.openalex.org/access/snapshot/#the-daily-snapshot-bucket-member-and-partner
Sync: https://help.openalex.org/access/sync/  (which partitions to download each day)
