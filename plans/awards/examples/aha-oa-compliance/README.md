# American Heart Association — open-access compliance dashboard

A single-page **compliance-monitoring dashboard** for the **American Heart Association (AHA**,
OpenAlex funder [`F4320306230`](https://openalex.org/F4320306230)), powered entirely by the live
OpenAlex public API (client-side fetch, no key, no backend).

AHA's open-science policy requires funded researchers to deposit a free copy of each publication in
PubMed Central within **12 months** of publication. This dashboard surfaces AHA-funded works and
flags those at risk of falling out of that policy.

This is the second entry in the collection of **funder-intelligence examples** built on open
OpenAlex metadata (see [`../tula-hakai-funder-impact/`](../tula-hakai-funder-impact/)). It is the
front end for an ongoing collaboration with AHA, who are moving their compliance monitoring and
funding intelligence off Dimensions onto open data. More work to come — see *Roadmap* below.

## What it shows
- **KPIs:** total AHA-funded publications, % open access, % with a PubMed record, and a count of
  works **overdue** (not OA and published >12 months ago).
- **Charts:** publications over time, OA-status breakdown, top subfields, top topics — each click
  through to the matching OpenAlex result.
- **Table:** per-work OA status, PubMed presence, and a compliance flag (Compliant / Within window
  / Overdue), with filters for year, type, OA status, and compliance.

## Current compliance heuristic (and its limits)
With today's OpenAlex fields, "overdue" is approximated as **not open access AND published more than
12 months ago**. This is a *lagging* signal — a work is only flagged once the deadline has already
passed. The roadmap replaces this with PubMed/PMC embargo metadata so breaches can be predicted
*before* the deadline, and adds true grant↔output linkage checks.

## Run locally
Static file — open `index.html` in a browser, or run the bundled server:
```bash
npm start        # serves on http://localhost:3000 (or $PORT)
```
All data is fetched live from `api.openalex.org` in the browser; there is no backend or database.

## Layout
- `index.html` — the entire dashboard (markup, styles, Chart.js, and the OpenAlex client).
- `server.js` — tiny static file server (Node, listens on `$PORT`).
- `package.json` — `npm start` entry point.

## Roadmap (AHA collaboration)
1. **Precision/recall against AHA's gold set** — AHA is supplying a verified list of their supported
   publications (Excel, one tab per month, DOI per row, colour-coded verification status). Match by
   DOI into OpenAlex and measure precision/recall of AHA funder + specific-grant linkage, mirroring
   the Tula/Hakai method, then diagnose the misses to improve the funding pipeline.
2. **PMC grant↔output linkages** — wire PubMed Central's researcher-deposited grant-to-publication
   and funder-to-publication links into OpenAlex (we ingest PMC works but not these linkages today).
3. **PMC embargo metadata (scoping)** — assess layering PMC/PubMed embargo dates on top, to flag
   policy breaches *in advance* rather than after the 12-month window lapses.
4. **Non-compliance targeting** — let AHA find grantees whose works lack any grant linkage in
   OpenAlex and/or are not OA in time, to feed their Proposal Central outreach.

Deployment target: `openalex.org/examples/AHA-dashboard`.
