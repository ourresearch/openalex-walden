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
| Gateway to Research (GTR) Project Awards | Complete | Priority 0 - Authoritative for UK grants with full metadata. 163,638 grants |
| Crossref Awards | Complete | Priority 1 - Rich metadata from Crossref. 185,447 grants |
| Backfill Awards | Complete | Priority 2 - Extracted from publication funding acknowledgements. 6,832,722 grants |
| NIH (National Institutes of Health) | Complete | Priority 3 - US NIH grants with full metadata. 2,281,454 grants |
| NSF (National Science Foundation) | Complete | Priority 3 - US NSF grants with full metadata. 644,453 grants |
| NSERC (Natural Sciences and Engineering Research Council) | Complete | Priority 3 - Canadian NSERC grants with full metadata. 200,886 grants |
| Gateway to Research (GTR) Awards (legacy) | Complete | Priority 3 - Publication-based, for work linkage only. 1,320,206 grants |
| Gates Foundation | Complete | Priority 4 - Bill & Melinda Gates Foundation committed grants. 40,221 grants |
| SSHRC (Social Sciences and Humanities Research Council) | Complete | Priority 5 - Canadian SSHRC grants. 114,403 grants |
| NWO (Netherlands Organisation for Scientific Research) | Step 6 | Has notebook (CreateNWOAwards.ipynb) and script (nwo_to_s3.py), not yet in CreateAwards.ipynb |
| NHMRC (National Health and Medical Research Council) | Step 6 | Has notebook (CreateNHMRCAwards.ipynb) and script (nhmrc_to_s3.py), not yet in CreateAwards.ipynb |
| KAKEN (Japan Grant-in-Aid for Scientific Research) | Step 1 | Currently downloading, expected to take days to complete |
| ANR (Agence Nationale de la Recherche) | Step 5 | Priority 6 - French research agency. 34,435 grants. Data uploaded to S3, notebook created. Waiting for human to run. |
| CIHR (Canadian Institutes of Health Research) | Step 1 | Canadian health research funder. Data source: https://open.canada.ca/data/en/dataset/49edb1d7-5cb4-4fa7-897c-515d1aad5da3. Claimed for ingestion. |
| Canadian Foundation for Innovation | Step 0 | ~14k grants. Data source: https://www.innovation.ca/projects-results/funded-projects-dashboard#!list-view |
| Swedish Research Council (Vinnova) | Step 0 | Swedish research funder. Data source: https://www.vinnova.se/en/about-us/about-the-website/open-data/ |
| Armasuisse | Step 0 | Swiss defense research funding. ~43k grants. Data source: https://docs.google.com/spreadsheets/d/1FH7sLNJGV8Jp988INbrQXCQO7JpYRPhgFE0yawWWjL4/edit?usp=sharing |
| NIHR (National Institute for Health and Care Research) | Step 0 | UK health research funder. Data source: https://fundingawards.nihr.ac.uk/ |
| DFG (Deutsche Forschungsgemeinschaft) | Step 0 | German Research Foundation. Data source: TBD |
| FAPESP (São Paulo Research Foundation) | Step 0 | Brazilian research funder. Data source: https://bv.fapesp.br/en/ |
| ARC (Australian Research Council) | Step 0 | Australian research funder. Data source: https://dataportal.arc.gov.au/RGS/Web/Grants |
| Swedish Research Council | Step 0 | Swedish research funder. Data source: https://www.vr.se/english/swecris.html |
| ERC (European Research Council) | Step 0 | European research funder. Data source: https://erc.europa.eu/projects-statistics/erc-dashboard |
| SNSF (Swiss National Science Foundation) | Step 0 | Swiss research funder. Data source: https://data.snf.ch/grants/documentation |

