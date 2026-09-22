OpenAlex daily snapshot
=======================
The complete OpenAlex database as downloadable files, rebuilt and published
every day into its own dated folder (full/YYYY-MM-DD/).

Data:      jsonl/ and parquet/  (two complete copies, partitioned by updated_date)
Manifests: jsonl/manifest.json, parquet/manifest.json  (written last; present = this day's snapshot is complete)
Deletions: jsonl/works/deleted_ids.csv.gz, parquet/works/deleted_ids.csv.gz  (gzip-compressed CSV; cumulative log of deleted work IDs)
           deleted_ids.csv remains alongside each compressed file for compatibility.
History:   RELEASE_NOTES.txt
License:   CC0, same as the public snapshot (https://openalex.s3.amazonaws.com/LICENSE.txt)

The free public snapshot at s3://openalex/data/ is a quarterly release of the same layout.

Docs: https://help.openalex.org/access/snapshot/#the-daily-snapshot-bucket-paid-plans
Sync: https://help.openalex.org/access/sync/
