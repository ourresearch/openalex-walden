# NEES Dataset

Known problem DOIs from the National Electronic Extraction Service (NEES) that have affiliation extraction issues.

## Purpose

This dataset contains ~316 DOIs that are known to have problems with affiliation extraction. It's useful for:
- Targeted parser testing
- Measuring improvement on hard cases
- Regression testing after parser changes

## Files

- `dois.txt` - List of problematic DOIs (one per line)

## Origin

These DOIs were identified through quality analysis that found specific publishers and document types where affiliation extraction consistently fails.

## Usage

```bash
# Test Parseland on these DOIs
python qa/exploration/scripts/verify_nees_elsevier.py

# Use as input to other scripts
cat qa/exploration/datasets/nees/dois.txt | head -10
```

## Notes

- Many of these DOIs may not have HTML available (404 from Taxicab)
- Some publishers in this list have structural HTML issues requiring parser updates
- This is an intentionally "hard" dataset - low success rates are expected
