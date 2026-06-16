# American Heart Association — open-access compliance dashboard

A single-page **compliance-monitoring dashboard** for the **American Heart Association**
(AHA, OpenAlex funder [`F4320306230`](https://openalex.org/F4320306230)), powered entirely
by the live OpenAlex public API (client-side fetch, no key, no backend).

AHA's open-science policy requires funded researchers to deposit a free copy of each
publication in PubMed Central within **12 months** of publication. This dashboard surfaces
AHA-funded works and flags those at risk of falling out of that policy.

Part of the collection of **funder-intelligence examples** built on open OpenAlex
metadata (see [`../tula-hakai-funder-impact/`](../tula-hakai-funder-impact/)).

## What it shows
- **KPIs:** total AHA-funded publications, % open access, % with a PubMed record, and a
  count of works **overdue** (not OA and published >12 months ago).
- **Charts:** publications over time, OA-status breakdown, top subfields, top topics —
  each clicks through to the matching OpenAlex result.
- **Table:** per-work OA status, PubMed presence, and a compliance flag (Compliant /
  Within window / Overdue), with filters for year, type, OA status, and compliance.

## Run locally
Static file — open `index.html` in a browser, or run the bundled server:
```bash
npm start        # serves on http://localhost:3000 (or $PORT)
```
All data is fetched live from `api.openalex.org` in the browser; there is no backend.

## Layout
- `index.html` — the entire dashboard (markup, styles, Chart.js, and the OpenAlex client).
- `server.js` — tiny static file server (Node, listens on `$PORT`).
- `package.json` — `npm start` entry point.

Deployment target: `openalex.org/examples/AHA-dashboard`.
