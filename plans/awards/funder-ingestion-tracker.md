# Funder Ingestion Tracker

This document tracks the progress of adding new funders to OpenAlex.

## Status Key

- **Step 0**: Look up funder_id in OpenAlex
- **Step 1**: Download data and upload to S3
- **Step 2**: Create Databricks notebook
- **Step 3**: Add to CreateAwards.ipynb
- **Step 4**: Commit and push
- **Step 5**: Human runs notebook
- **Step 6**: Verify the data
- **Step 7**: Final human approval
- **Complete**: Fully integrated into OpenAlex

---

## Funders

| Funder Name | Status | Notes |
|-------------|--------|-------|
| Gateway to Research (GTR) Project Awards | Complete | Priority 0 - Authoritative for UK grants with full metadata |
| Crossref Awards | Complete | Priority 1 - Rich metadata from Crossref |
| Backfill Awards | Complete | Priority 2 - Extracted from publication funding acknowledgements |
| NIH (National Institutes of Health) | Complete | Priority 3 - US NIH grants with full metadata |
| NSF (National Science Foundation) | Complete | Priority 3 - US NSF grants with full metadata |
| NSERC (Natural Sciences and Engineering Research Council) | Complete | Priority 3 - Canadian NSERC grants with full metadata |
| Gateway to Research (GTR) Awards (legacy) | Complete | Priority 3 - Publication-based, for work linkage only |
| Gates Foundation | Complete | Priority 4 - Bill & Melinda Gates Foundation committed grants |
| SSHRC (Social Sciences and Humanities Research Council) | Complete | Priority 5 - Canadian SSHRC grants |
| NWO (Netherlands Organisation for Scientific Research) | Step 6 | Has notebook (CreateNWOAwards.ipynb) and script (nwo_to_s3.py), not yet in CreateAwards.ipynb |
| NHMRC (National Health and Medical Research Council) | Step 6 | Has notebook (CreateNHMRCAwards.ipynb) and script (nhmrc_to_s3.py), not yet in CreateAwards.ipynb |
| KAKEN (Japan Grant-in-Aid for Scientific Research) | Step 6 | Has notebook (CreateKAKENAwards.ipynb) and script (kaken_to_s3.py), not yet in CreateAwards.ipynb |

